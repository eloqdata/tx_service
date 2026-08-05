/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */
#include "cc/page_fetch.h"

#include <butil/logging.h>

#include <cassert>
#include <memory>

#include "cc/cc_entry.h"
#include "cc/cc_map.h"
#include "cc/cc_shard.h"
#include "cc/non_blocking_lock.h"
#include "fault/fault_inject.h"

namespace txservice
{
bool PageFetch::Execute(CcShard &ccs)
{
    // Test hook: hold this completion in the shard queue for a bounded window
    // so a test can swap or retire the payload while this fetch is genuinely
    // in flight — the §7 path that is otherwise impossible to hit
    // deterministically from a client. Self-clearing; compiled out unless
    // WITH_FAULT_INJECT.
    CODE_FAULT_INJECTOR("stall_page_fetch", {
        uint64_t now_ms = ccs.NowInMilliseconds();
        if (stall_until_ == 0)
        {
            stall_until_ = now_ms + 3000;
        }
        if (now_ms < stall_until_)
        {
            // EVERY_N, not FIRST_N: FIRST_N stops logging after its budget
            // is spent for the life of the process, which silently blinds any
            // later test that checks whether the stall fired.
            LOG_EVERY_N(INFO, 50)
                << "FAULTLOG stall_page_fetch page=" << page_id_;
            ccs.Enqueue(ccs.LocalCoreId(), this);
            return false;
        }
    });

    // Runs on the shard core after the store handler filled the result and
    // enqueued this request back (FetchRecordCc::SetFinish). The entry pin
    // taken at issuance keeps cce_ and its lock structure alive until here.
    KeyGapLockAndExtraData *lke = cce_->GetKeyGapLockAndExtraData();
    assert(lke != nullptr);
    FetchHub *hub = lke->FetchHubPtr();
    assert(hub != nullptr);

    // Pop ourselves from whichever collection owns us: `self` destroys this
    // object when it leaves scope at the end of this function. That is why we
    // MUST return false below -- returning true would have CcShard::
    // ProcessRequests call Free() on an object that no longer exists.
    std::unique_ptr<PageFetch> self =
        orphaned_ ? hub->TakeOrphan(this) : hub->TakeLive(page_id_);
    assert(self.get() == this);

    // Diagnostic-only natural completion marker. Integration tests compare
    // deltas around one operation to distinguish a metadata-only read from a
    // read that actually completed page fetches. DLOG keeps the per-page
    // volume out of optimized production builds.
    DLOG(INFO) << "PAGELOG page_fetch_complete page=" << page_id_
               << " error=" << error_code_
               << " status=" << static_cast<int>(rec_status_)
               << " orphan=" << orphaned_;

    // An orphaned fetch is pure teardown (docs/08 §7): its bytes may
    // describe a page id of a superseded incarnation, and its waiters were
    // eagerly woken at the swap. A dead term likewise discards — the entry's
    // contents are being torn down wholesale.
    // Test hook: make this completion look like a store failure, so the
    // error path (§4) can be exercised. Everything downstream keys off
    // error_code_, so setting it here is exactly what a failed store read
    // delivers.
    CODE_FAULT_INJECTOR("fail_page_fetch", {
        LOG(INFO) << "FAULTLOG fail_page_fetch page=" << page_id_;
        error_code_ = static_cast<int>(CcErrorCode::DATA_STORE_ERR);
    });

    // Test hook: simulate an EXTERNAL abort of the commands parked on this
    // fetch — a deadlock victim, tx recovery, or a term change. Those paths
    // know nothing about page fetches, so they abort the request without
    // touching the hub, which is exactly the case this must survive.
    CODE_FAULT_INJECTOR("abort_parked_waiters", {
        for (TxNumber waiter : waiter_txns_)
        {
            CcRequestBase *parked = hub->PeekParked(waiter);
            if (parked != nullptr)
            {
                LOG(INFO) << "FAULTLOG abort_parked_waiters txn=" << waiter;
                parked->AbortCcRequest(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            }
        }
    });

    // Test hooks for the two corruption classes a client cannot produce
    // (docs/08 §5): a LIVE page whose store row vanished, and a live page
    // whose bytes are garbage. Both must surface as a deterministic error to
    // the waiters — never a successful wake that refaults the same page
    // forever.
    CODE_FAULT_INJECTOR("page_fetch_missing_row", {
        LOG(INFO) << "FAULTLOG page_fetch_missing_row page=" << page_id_;
        rec_status_ = RecordStatus::Deleted;
        rec_str_.clear();
    });
    CODE_FAULT_INJECTOR("page_fetch_corrupt_bytes", {
        if (rec_status_ == RecordStatus::Normal && !rec_str_.empty())
        {
            LOG(INFO) << "FAULTLOG page_fetch_corrupt_bytes page=" << page_id_;
            for (size_t i = 0; i < rec_str_.size() && i < 128; ++i)
            {
                rec_str_[i] ^= 0x5A;
            }
        }
    });

    bool term_ok = ValidTermCheck();
    if (!orphaned_ && term_ok)
    {
        CcMap *ccm = lke->GetCcMap();
        assert(ccm != nullptr);
        ccm->BackFillPage(*this);
    }
    else if (!orphaned_)
    {
        // This fetch is over but nothing will install it, so the §8
        // reservation it carried must be released here — BackFillPage, the
        // only other place that consumes or drops it, is skipped on this
        // path.
        lke->GetCcMap()->ReleasePageReservation(cce_, page_id_);

        // The node group's term moved between issue and completion, so this
        // result cannot be installed. The waiters must still be released:
        // BackFillPage is the only thing that would have resolved them, and
        // the orphan branch below does not run for a fetch that was never
        // superseded, so without this they wait forever. That is a hang for a
        // parked command and a permanently stuck buffered-command list for the
        // reserved drain context.
        //
        // false marks each waiter errored, so a woken command aborts rather
        // than re-running and faulting on the same page under a term that no
        // longer serves it.
        for (TxNumber waiter : waiter_txns_)
        {
            CcRequestBase *parked = hub->ResolveWaiter(waiter, false);
            if (parked != nullptr)
            {
                ccs.Enqueue(ccs.LocalCoreId(), parked);
            }
        }
    }

    // An orphaned fetch installs nothing, but its waiters must still be
    // released: the payload they faulted on is superseded, and the wake record
    // lives on the entry precisely so they can be resumed to observe that.
    // Requests the swap rule already took are cleared, so this cannot enqueue
    // one twice.
    if (orphaned_)
    {
        for (uint64_t waiter : waiter_txns_)
        {
            // true: discarding a superseded result is not an error for the
            // waiter — it re-runs against the new payload.
            CcRequestBase *parked = hub->ResolveWaiter(waiter, true);
            if (parked != nullptr)
            {
                ccs.Enqueue(ccs.LocalCoreId(), parked);
            }
        }
    }

    lke->ReleasePin();
    cce_->RecycleKeyLock(ccs);

    // FALSE, not true: `self` above owns this object and destroys it as this
    // function returns. A true return means "finished, caller please Free()",
    // and the caller would dereference freed memory -- the crash this fixes.
    // FetchCatalogCc does the same thing for the same reason: it erases itself
    // from the shard's fetch-request map and returns false.
    return false;
}
}  // namespace txservice
