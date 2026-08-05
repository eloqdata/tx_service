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
#pragma once

// Page fetch machinery for paged large objects (eloqkv
// docs/08-paged-objects.md §4/§7; engine change inventory §13). Page fetches
// bypass the shard's fetch_record_reqs_ map entirely — that map coalesces
// whole-record fetches per LruEntry, whereas page fetches are per
// (object, page id). The FetchHub on the entry's KeyGapLockAndExtraData is
// their single home: I/O requests are ENTRY-scoped — they outlive payload
// swaps — while page state is payload-scoped and dies with its block (§7).

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cc_req_misc.h"
#include "page_key_codec.h"
#include "tx_key.h"

namespace txservice
{
/**
 * @brief The §5 composite page key <object key, page id> as an engine key
 * type: its serialized bytes are the full encoded page-row key, while its
 * Hash() is the OBJECT key's hash — the mechanism that makes page-row
 * co-location structural rather than a rule to remember. One key type serves
 * the fetch and the flush, so the read and write paths cannot disagree about
 * placement.
 *
 * Never enters a CcMap and takes part in no concurrency control (§5), so the
 * comparison operators are plain byte order.
 */
class PageKey
{
public:
    PageKey() = default;

    PageKey(std::string encoded_bytes, size_t object_key_hash)
        : bytes_(std::move(encoded_bytes)), object_key_hash_(object_key_hash)
    {
        assert(HasPageKeyMagic(bytes_));
    }

    PageKey(const PageKey &) = default;
    PageKey(PageKey &&) = default;

    bool operator==(const PageKey &rhs) const
    {
        return bytes_ == rhs.bytes_;
    }

    bool operator<(const PageKey &rhs) const
    {
        return bytes_ < rhs.bytes_;
    }

    /**
     * @brief The object key's hash, not a hash of the page-key bytes (§5):
     * the page id decides which row, never which shard.
     */
    size_t Hash() const
    {
        return object_key_hash_;
    }

    void Serialize(std::string &str) const
    {
        str.append(bytes_);
    }

    std::string_view KVSerialize() const
    {
        return std::string_view(bytes_.data(), bytes_.size());
    }

    size_t SerializedLength() const
    {
        return bytes_.size();
    }

    TxKey CloneTxKey() const
    {
        return TxKey(std::make_unique<PageKey>(*this));
    }

    void Copy(const PageKey &rhs)
    {
        bytes_ = rhs.bytes_;
        object_key_hash_ = rhs.object_key_hash_;
    }

    size_t MemUsage() const
    {
        return sizeof(PageKey) + bytes_.capacity();
    }

    void SetPackedKey(const char *data, size_t size)
    {
        bytes_.assign(data, size);
    }

    const char *Data() const
    {
        return bytes_.data();
    }

    size_t Size() const
    {
        return bytes_.size();
    }

    KeyType Type() const
    {
        return KeyType::Normal;
    }

    bool NeedsDefrag(mi_heap_t *heap)
    {
        return false;
    }

    std::string ToString() const
    {
        PageKeyParts parts;
        if (DecodePageKey(bytes_, parts))
        {
            return "PageKey(kind=" +
                   std::to_string(static_cast<int>(parts.kind_)) +
                   ", page_id=" + std::to_string(parts.page_id_) + ")";
        }
        return "PageKey(malformed)";
    }

    static const TxKeyInterface *TxKeyImpl()
    {
        // The temporary exists only for template-argument deduction in the
        // TxKeyInterface constructor; no static PageKey is kept.
        static const TxKeyInterface tx_key_impl{PageKey()};
        return &tx_key_impl;
    }

private:
    std::string bytes_;
    size_t object_key_hash_{0};
};

/**
 * @brief One outstanding page fetch — the request ITSELF, not a wrapper
 * around one (docs/08 §4). Deriving from FetchRecordCc lets the completion
 * (this request's own Execute(), run on the shard core once the store
 * handler enqueues it back) read its own orphaned_ flag directly, and keeps
 * the whole-record completion path byte-for-byte untouched. The store
 * handler needs no changes either: it consumes tx_key_ bytes,
 * kv_table_name_, and partition_id_, all of which the constructor fills.
 *
 * waiter_txns_ holds TX NUMBERS, never request pointers. A completion resolves
 * each txn against the hub's own wake records (FetchHub::ResolveWaiter), which
 * live on the ENTRY and therefore survive a payload swap, deletion or expiry;
 * the payload is consulted only to pin the fetched page for that txn
 * (PagedTxObject::NotePageFetched). A txn with no wake record resolves to
 * nothing, so no deregistration protocol is needed on any teardown path (§7).
 */
struct PageFetch : public FetchRecordCc
{
public:
    PageFetch(const TableName *tbl_name,
              const TableSchema *tbl_schema,
              TxKey page_key,
              uint32_t page_id,
              LruEntry *cce,
              CcShard &ccs,
              NodeGroupId cc_ng_id,
              int64_t cc_ng_term,
              int32_t partition_id)
        : FetchRecordCc(tbl_name,
                        tbl_schema,
                        std::move(page_key),
                        cce,
                        ccs,
                        cc_ng_id,
                        cc_ng_term,
                        partition_id),
          page_id_(page_id)
    {
    }

    /**
     * @brief The completion, on the shard core: pops itself from the hub,
     * routes a live result through CcMap::BackFillPage (install + waiter
     * resolution), discards an orphaned one, and releases the entry pin the
     * issuance took. Defined in src/cc/page_fetch.cpp.
     *
     * @return Always false, meaning "not finished, do not Free() me". This
     *         request takes ownership of itself out of the hub, so it is
     *         destroyed as this returns. Returning true would make
     *         CcShard::ProcessRequests call Free() on freed memory.
     *         FetchCatalogCc returns false for the same reason.
     */
    bool Execute(CcShard &ccs) override;

    uint32_t page_id_{0};
    // Deduplicated on insert by linear search, not by a hash set: a command
    // recomputes its fault set from residency each round, so a re-run
    // re-registers for pages still in flight, and coalescing must not record
    // the same txn twice — one waiter, one resolution. The list is small in
    // practice (the transactions concurrently awaiting one page of one
    // object), and a scan over a handful of contiguous 64-bit values beats a
    // hash set's allocation and indirection. On the replay/standby/migration
    // paths this holds kDrainTxnNumber (docs/08 §10).
    std::vector<TxNumber> waiter_txns_;
    // Set at the §7 swap splice; carries the incarnation boundary — a fetch
    // issued for one incarnation's page id must never install into a
    // successor's.
    bool orphaned_{false};
    // Test hook only (WITH_FAULT_INJECT): deadline until which this completion
    // is held in the shard queue, so a test can swap or retire the payload
    // while the fetch is genuinely in flight (docs/08 §7).
    uint64_t stall_until_{0};
};

/**
 * @brief The single home for every outstanding page fetch of one entry
 * (docs/08 §7). Hangs off KeyGapLockAndExtraData as a lazily allocated
 * unique_ptr — 8 bytes when absent, allocated on the first page fetch, which
 * also takes the entry pin the structure exists for. Its emptiness gates
 * KeyGapLockAndExtraData::IsEmpty(), the recycle gate: recycling while the
 * store handler still holds pointers into a PageFetch is a use-after-free.
 */
struct FetchHub
{
public:
    /**
     * @brief Per-transaction wake record, held on the ENTRY (docs/08 §4:
     * "requests with the entry, state with the payload").
     *
     * It lived inside the paged payload until a measured hang proved that
     * wrong: DEL drops the committed payload, destroying the parked request
     * pointer with it, so the fetch completion had nothing to wake and the
     * client waited forever. Page-scoped state (pins, residency) still belongs
     * to the payload — it is meaningless once that payload is gone — but the
     * record of WHO IS WAITING must outlive payload replacement or removal.
     */
    struct TxWake
    {
        CcRequestBase *parked_req_{nullptr};
        // Outstanding waiter entries for this txn, across every fetch in the
        // hub. Exact rather than merely balanced, because waiter_txns_ is
        // deduplicated: one entry per (fetch, txn), incremented where the
        // entry is appended and decremented where it is consumed. The command
        // wakes when it reaches zero, so an N-page fault set costs one
        // re-execution instead of N.
        uint32_t awaited_{0};
        // A fetch this txn waited on failed; the woken command must error out
        // rather than re-run and fault on the same missing page (§4).
        bool errored_{false};
    };

    /**
     * @brief Per-transaction wake records, keyed by tx number — see TxWake
     * above for why they live on the entry rather than in the payload.
     */
    std::unordered_map<TxNumber, TxWake> tx_wakes_;

    /**
     * @brief Live fetches, at most one per page id — simultaneously the
     * coalescing index ("is a fetch for P in flight?") and the owner. The
     * unique_ptr keeps each request's address stable for the store handler
     * across rehashes.
     */
    std::unordered_map<uint32_t, std::unique_ptr<PageFetch>> live_;

    /**
     * @brief Fetches superseded by a payload swap, flagged
     * discard-on-complete. A vector, not a map: the same page id can recur
     * across chained swaps, and completions erase by address.
     */
    std::vector<std::unique_ptr<PageFetch>> orphans_;

    /**
     * @return true iff no fetch is outstanding, live or orphaned.
     *
     * KeyGapLockAndExtraData::IsEmpty() consults this before recycling the
     * lock structure. Recycling it while the store handler still holds a
     * PageFetch pointer is a use-after-free.
     */
    bool Empty() const
    {
        return live_.empty() && orphans_.empty();
    }


    /**
     * @brief Records `req` as the request to wake for `txn`; nullptr
     * deregisters, which every path that re-enqueues a parked request by
     * other means must do, so a later completion cannot enqueue it twice.
     */
    void RegisterWaiter(TxNumber txn, CcRequestBase *req)
    {
        if (req == nullptr)
        {
            auto it = tx_wakes_.find(txn);
            if (it != tx_wakes_.end())
            {
                it->second.parked_req_ = nullptr;
            }
            return;
        }
        tx_wakes_.try_emplace(txn).first->second.parked_req_ = req;
    }

    /**
     * @brief Records that a waiter entry for `txn` was appended to some
     * fetch's list. Pairs 1:1 with the ResolveWaiter that consumes it.
     */
    void NoteAwaited(TxNumber txn)
    {
        ++tx_wakes_.try_emplace(txn).first->second.awaited_;
    }

    /**
     * @brief Consumes one waiter entry for `txn`.
     *
     * @param success false marks the txn errored. The woken command then
     *        fails with the store error instead of re-running, which would
     *        fault on the same missing page (§4). An orphaned fetch passes
     *        true: discarding a superseded result is not an error for the
     *        waiter. That command simply re-runs against the new payload.
     * @param reached_zero optional out-param, set true iff this call was the
     *        one that brought the txn's outstanding count to zero. The return
     *        value alone cannot say so for kDrainTxnNumber, whose parked
     *        request is legitimately null — "nullptr" there would be
     *        indistinguishable from "still waiting".
     * @return The parked request once this txn has nothing outstanding, else
     *         nullptr. The slot is cleared as the request is handed back, so
     *         no later consumer can enqueue it a second time.
     */
    CcRequestBase *ResolveWaiter(TxNumber txn,
                                 bool success,
                                 bool *reached_zero = nullptr)
    {
        if (reached_zero != nullptr)
        {
            *reached_zero = false;
        }
        auto it = tx_wakes_.find(txn);
        if (it == tx_wakes_.end())
        {
            // Forgotten at commit/abort/lock release: a stale entry resolves
            // to nothing by design (§7).
            return nullptr;
        }
        if (!success)
        {
            it->second.errored_ = true;
        }
        if (it->second.awaited_ > 0)
        {
            --it->second.awaited_;
        }
        if (it->second.awaited_ != 0)
        {
            return nullptr;
        }
        if (reached_zero != nullptr)
        {
            *reached_zero = true;
        }
        CcRequestBase *parked = it->second.parked_req_;
        it->second.parked_req_ = nullptr;
        return parked;
    }

    /**
     * @brief Reports and clears `txn`'s fetch-error flag, dropping the record
     * when nothing is left to remember.
     *
     * @return true iff a page fetch this txn was waiting on FAILED since the
     *         last call, meaning the woken command must abort with the store
     *         error rather than re-run (§4). The flag is consumed, so an
     *         immediate second call returns false. false also when the txn has
     *         no wake record at all — nothing failed, nothing to report.
     */
    bool ConsumeError(TxNumber txn)
    {
        auto it = tx_wakes_.find(txn);
        if (it == tx_wakes_.end())
        {
            return false;
        }
        bool errored = it->second.errored_;
        it->second.errored_ = false;
        if (it->second.parked_req_ == nullptr)
        {
            tx_wakes_.erase(it);
        }
        return errored;
    }

    /**
     * @brief The request parked for `txn`, without deregistering it.
     * @return The parked request, or nullptr if this txn has none.
     */
    CcRequestBase *PeekParked(TxNumber txn) const
    {
        auto it = tx_wakes_.find(txn);
        return it == tx_wakes_.end() ? nullptr : it->second.parked_req_;
    }

    /**
     * @brief Drops `txn`'s wake record entirely (commit, abort, lock release).
     */
    void ForgetWaiter(TxNumber txn)
    {
        tx_wakes_.erase(txn);
    }

    /**
     * @brief Takes every live parked request, for teardown paths that must
     * wake them all at once.
     *
     * @return One request per txn that had one parked. Each is deregistered
     *         as it is taken, so a second call returns an empty vector and no
     *         request can be handed out twice. The caller must enqueue every
     *         request it receives.
     */
    std::vector<CcRequestBase *> TakeAllParked()
    {
        std::vector<CcRequestBase *> out;
        for (auto &[txn, wake] : tx_wakes_)
        {
            if (wake.parked_req_ != nullptr)
            {
                out.push_back(wake.parked_req_);
                wake.parked_req_ = nullptr;
            }
        }
        return out;
    }

    /**
     * @brief The §7 swap rule's fetch half: flag every live fetch
     * discard-on-complete and splice it into orphans_ — a local move within
     * one structure; request addresses stay stable. The caller (the payload
     * swap) separately erases the superseded block's tx contexts.
     */
    void SpliceAllToOrphans()
    {
        for (auto &[page_id, fetch] : live_)
        {
            fetch->orphaned_ = true;
            orphans_.emplace_back(std::move(fetch));
        }
        live_.clear();
    }

    /**
     * @brief Removes the live fetch for `page_id` from the coalescing index.
     * @return The fetch, or nullptr if no live fetch has this page id. A
     *         completion gets nullptr when a payload swap moved its fetch to
     *         orphans_ while the I/O was in flight.
     */
    std::unique_ptr<PageFetch> TakeLive(uint32_t page_id)
    {
        auto it = live_.find(page_id);
        if (it == live_.end())
        {
            return nullptr;
        }
        std::unique_ptr<PageFetch> fetch = std::move(it->second);
        live_.erase(it);
        return fetch;
    }

    /**
     * @brief Removes the orphaned fetch with this address. Address, not page
     * id: chained swaps can leave several orphans with the same page id.
     * @return The fetch, or nullptr if it is not in orphans_.
     */
    std::unique_ptr<PageFetch> TakeOrphan(const PageFetch *fetch)
    {
        for (auto it = orphans_.begin(); it != orphans_.end(); ++it)
        {
            if (it->get() == fetch)
            {
                std::unique_ptr<PageFetch> out = std::move(*it);
                orphans_.erase(it);
                return out;
            }
        }
        return nullptr;
    }
};
}  // namespace txservice
