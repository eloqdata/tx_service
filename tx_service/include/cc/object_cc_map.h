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

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog_factory.h"
#include "cc_entry.h"
#include "cc_map.h"
#include "cc_req_base.h"
#include "cc_req_misc.h"
#include "cc_request.pb.h"
#include "cc_shard.h"
#include "error_messages.h"
#include "non_blocking_lock.h"
#include "page_fetch.h"
#include "paged_tx_object.h"
#include "sharder.h"
#include "standby.h"
#include "template_cc_map.h"
#include "tx_command.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_service_common.h"

namespace txservice
{
// whether skip accessing KV when cc map cache misses.
extern bool txservice_skip_kv;

template <typename KeyT, typename ValueT>
class ObjectCcMap : public TemplateCcMap<KeyT, ValueT, false, false>
{
public:
    ObjectCcMap(const ObjectCcMap &rhs) = delete;
    ~ObjectCcMap() = default;

    /**
     * @brief Constructs a new object cc map object. The object cc map has no
     * schema, so the schema's timestamp is set to 1 (the beginning of history).
     *
     * @param shard
     */
    ObjectCcMap(CcShard *shard,
                NodeGroupId cc_ng_id,
                const TableName &table_name,
                uint64_t schema_ts,
                const TableSchema *table_schema = nullptr,
                bool ccm_has_full_entries = false)
        : TemplateCcMap<KeyT, ValueT, false, false>(shard,
                                                    cc_ng_id,
                                                    table_name,
                                                    schema_ts,
                                                    table_schema,
                                                    ccm_has_full_entries)
    {
        DLOG(INFO) << "creating ObjectCcmap on shard: " << shard_->core_id_
                   << ", table name: " << table_name.StringView()
                   << ", table_schema: " << table_schema_
                   << ", schema_ts: " << schema_ts_;
    }

    using CcMap::AcquireCceKeyLock;
    using CcMap::cc_ng_id_;
    using CcMap::ccm_has_full_entries_;
    using CcMap::last_dirty_commit_ts_;
    using CcMap::LockHandleForResumedRequest;
    using CcMap::MoveRequest;
    using CcMap::ReleaseCceLock;
    using CcMap::schema_ts_;
    using CcMap::shard_;
    using CcMap::table_name_;
    using CcMap::table_schema_;
    using TemplateCcMap<KeyT, ValueT, false, false>::ccmp_;
    using TemplateCcMap<KeyT, ValueT, false, false>::Find;
    using TemplateCcMap<KeyT, ValueT, false, false>::FindEmplace;
    using TemplateCcMap<KeyT, ValueT, false, false>::End;
    using typename TemplateCcMap<KeyT, ValueT, false, false>::Iterator;
    using TemplateCcMap<KeyT, ValueT, false, false>::KeySchema;
    using TemplateCcMap<KeyT, ValueT, false, false>::RecordSchema;
    using TemplateCcMap<KeyT, ValueT, false, false>::Type;
    using TemplateCcMap<KeyT, ValueT, false, false>::CleanEntry;
    using TemplateCcMap<KeyT, ValueT, false, false>::TryUpdatePageKey;
    using TemplateCcMap<KeyT, ValueT, false, false>::
        EnsureLargeObjOccupyPageAlone;

    bool Execute(ApplyCc &req) override
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"template_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append(std::to_string(req.TxTerm()));
            });
        TX_TRACE_DUMP(&req);

        CcHandlerResult<ObjectCommandResult> *hd_res = req.Result();

        if (req.SchemaVersion() != 0 && req.SchemaVersion() != schema_ts_)
        {
            hd_res->SetError(CcErrorCode::REQUESTED_TABLE_SCHEMA_MISMATCH);
            return true;
        }

        ObjectCommandResult &obj_result = hd_res->Value();
        CcEntryAddr &cce_addr = obj_result.cce_addr_;
        bool &object_modified = obj_result.object_modified_;
        bool &object_deleted = obj_result.object_deleted_;
        CcEntry<KeyT, ValueT, false, false> *cce = nullptr;
        CcPage<KeyT, ValueT, false, false> *ccp = nullptr;
        const KeyT *look_key = nullptr;
        KeyT decoded_key;

        uint32_t ng_id = req.NodeGroupId();
        TxNumber txn = req.Txn();
        int64_t is_standby_tx = IsStandbyTx(req.TxTerm());
        int64_t ng_term = -1;
        if (is_standby_tx)
        {
            ng_term = Sharder::Instance().StandbyNodeTerm();
            if (ng_term < 0 || ng_term != req.TxTerm())
            {
                LOG(INFO) << "ApplyCc, the standby node of node_group(#"
                          << ng_id << "), standby node term: " << ng_term
                          << ", standby tx term: " << req.TxTerm()
                          << ", txn: " << txn;
                hd_res->SetError(CcErrorCode::DATA_NOT_ON_LOCAL_NODE);
                return true;
            }

            if (!req.IsReadOnly())
            {
                hd_res->SetError(CcErrorCode::DATA_NOT_ON_LOCAL_NODE);
                return true;
            }
        }
        else
        {
            ng_term = Sharder::Instance().LeaderTerm(ng_id);
            CODE_FAULT_INJECTOR("term_TemplateCcMap_Execute_ApplyCc", {
                LOG(INFO) << "FaultInject  term_TemplateCcMap_Execute_ApplyCc";
                ng_term = -1;
            });
            if (ng_term < 0)
            {
                LOG(INFO) << "ApplyCc, node_group(#" << ng_id
                          << ") term < 0, tx:" << txn;
                hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                return true;
            }
        }

        LockType acquired_lock = LockType::NoLock;
        CcErrorCode err_code = CcErrorCode::NO_ERROR;

        // Should create command before calling req.IsReadOnly().
        TxCommand *cmd = nullptr;
        if (req.IsLocal())
        {
            cmd = req.CommandPtr();
        }
        else
        {
            if (req.HasCommand())
            {
                cmd = req.remote_input_.cmd_;
            }
            else
            {
                std::unique_ptr<TxCommand> cmd_uptr =
                    CreateTxCommand(*req.CommandImage());
                cmd = cmd_uptr.get();
                req.SetCommand(cmd_uptr.release());
            }
        }

        auto need_fetch_kv = [this](CcEntry<KeyT, ValueT, false, false> *cce,
                                    CcOperation cc_op,
                                    TxNumber txn,
                                    TxCommand *cmd)
        {
            // Check if this cce does not exist in ccmap at all. We need to
            // double-check that there is no dirty payload status on the cce
            // since a previous cmd might ignore old payload value and directly
            // applied dirty payload status.

            assert(cc_op == CcOperation::Read ||
                   cc_op == CcOperation::ReadForWrite);
            if (cce->PayloadStatus() != RecordStatus::Unknown)
            {
                return false;
            }
            if (ccm_has_full_entries_ || txservice_skip_kv)
            {
                cce->SetCommitTsPayloadStatus(1U, RecordStatus::Deleted);
                cce->SetCkptTs(1U);
                return false;
            }

            NonBlockingLock *lk = cce->GetKeyLock();

            // Whether the read/write request will just read/write its own dirty
            // payload. If true, no need to FetchRecord if RecordStatus is
            // unknown.
            auto read_write_dirty_payload = [&]
            {
                // If write lock has been acquired by this txn, this read/write
                // just read/write the dirty payload which must exist (Normal or
                // Deleted or Uncreated).
                if (lk != nullptr && lk->HasWriteLock())
                {
                    assert(cce->DirtyPayloadStatus() !=
                           RecordStatus::NonExistent);
                    if (lk->WriteLockTx() == txn)
                    {
                        return true;
                    }
                }
                return false;
            };

            // When do we need to FetchRecord from KV?
            // First, the payload status must be unknown, then:
            // If the dirty payload doesn't exist, or it exists but this command
            // only reads the committed status and doesn't check the dirty
            // status. Which means, the dirty status could be set by another txn
            // and this txn only reads the committed payload (under OCC read)
            // and cannot read the uncommitted dirty status, should FetchRecord.
            // In summary, FetchRecord if the cce's payload status is unknown
            // and the command don't check_dirty_status or the dirty status does
            // not exist;

            if (cc_op == CcOperation::Read && read_write_dirty_payload())
            {
                // This is a read operation that will read its own dirty
                // payload. No need to fetch record from KV.
                return false;
            }
            if (cc_op == CcOperation::ReadForWrite &&
                (read_write_dirty_payload() || cmd->IgnoreOldValue()))
            {
                // This is a write operation which write its own dirty payload,
                // or ignores Kv value.
                return false;
            }
            // This a normal write operation or a read operation that must check
            // the original payload.
            return true;
        };

        // Set by the pre-acquisition probe when its run is authoritative —
        // the lock was grantable and ExecuteOn completed — so the normal
        // path below reuses that result instead of executing a second time
        // (§6).
        bool probe_authoritative = false;
        ExecResult probe_exec_rst = ExecResult::Fail;

        // Handles ExecResult::Yield (eloqkv docs/08-paged-objects.md §6): the
        // command found part of its fault set non-resident and executed
        // nothing. Drains the page ids the object recorded, issues one fetch
        // each, and parks the request to be re-enqueued when they land.
        //
        // The object supplies the ids but not the key or the shard — a
        // TxObject has neither (§4) — so issuance necessarily happens here.
        // Returns false if the fetch set could not be issued, in which case
        // the caller must not park.
        // Decodes the request's key into look_key, idempotently. The
        // surrounding code assigns look_key only on a request's FIRST pass,
        // because the monolithic path works from `cce` alone after a resume.
        // The paged fault path needs the key on EVERY pass: a command may fault
        // again on a resumed round (its fault set is recomputed each time,
        // docs/08 §6) and page row keys are derived from the object key.
        auto ensure_look_key = [&look_key, &decoded_key, &req, this]()
        {
            if (look_key != nullptr)
            {
                return;
            }
            const TxKey *req_key = req.Key();
            if (req_key != nullptr)
            {
                look_key = req_key->GetKey<KeyT>();
            }
            else
            {
                const std::string *key_str = req.KeyImage();
                assert(key_str != nullptr);
                size_t offset = 0;
                decoded_key.Deserialize(key_str->data(), offset, KeySchema());
                look_key = &decoded_key;
            }
        };

        auto park_on_page_faults = [this, &req, &look_key](
                                       CcEntry<KeyT, ValueT, false, false> *cce,
                                       const KeyT &key,
                                       TxObject *object,
                                       int64_t ng_term) -> bool
        {
            // Every call site passes *look_key. Catch a missing decode here
            // rather than as a null dereference inside FetchPage.
            assert(look_key != nullptr &&
                   "the paged fault path needs the object key on every pass");
            PagedTxObject *paged =
                object != nullptr ? object->AsPaged() : nullptr;
            assert(paged != nullptr &&
                   "only a paged object may return ExecResult::Yield");
            std::vector<uint32_t> faults;
            bool has_faults = paged->TakePendingFaults(faults);
            // A yield with an empty fault set would spin: the command would
            // re-run, find nothing missing to fetch, and yield again.
            assert(has_faults &&
                   "ExecuteOn yielded without recording a missing page");
            if (!has_faults)
            {
                return false;
            }

            // ADMISSION (docs/08 §8): claim the buffers this fault set will
            // need BEFORE issuing any fetch, so page memory cannot overshoot
            // the shard budget between the decision to fetch and the arrival
            // of the bytes. Admission IS the allocation -- there is no
            // reservation counter to drift -- so two concurrent faulters
            // cannot both pass and jointly overshoot: the second one's
            // allocation simply fails.
            //
            // Refusal PARKS the request on the shard's memory wait list
            // and starts a reclamation campaign; it does not re-enqueue and
            // retry. Re-enqueueing was measured at ~130 k refusals/s and
            // ~200 % CPU, burning the very core the clean pass needs. The
            // wait list demands a request carrying no per-attempt state,
            // which the block below establishes before parking.
            if (!paged->ReserveFaultBuffers(faults))
            {
                if (!paged->FaultSetCanEverFit(faults.size()))
                {
                    // Larger than the shard could hold even when empty:
                    // retrying would spin forever, so this is an error.
                    req.AbortCcRequest(CcErrorCode::OUT_OF_MEMORY);
                    return false;
                }
                // Termination is the wait list's job (below), not a
                // per-request deadline: AbortRequestsAfterMemoryFree fails
                // the parked requests when the cleaner reaches the tail
                // without freeing anything, which is the same escape hatch
                // entry allocation already relies on.
                // Drop EVERY trace of this attempt, then PARK — do not
                // re-enqueue. Three defects came from doing otherwise
                // (reported):
                //
                //  * The speculative probe created the entry's key-lock
                //    structure. Refusing without recycling it left an EMPTY
                //    lock behind, and the very clean pass this refusal asks
                //    for then asserts in IsFree() that an empty lock was
                //    already recycled — a Debug abort under sustained
                //    pressure, and in Release an entry that can never be
                //    evicted, i.e. the memory is never returned.
                //  * Keeping the raw CcePtr while asking for eviction is
                //    self-contradictory: recycling the lock makes the entry
                //    evictable, which can free the very entry the pointer
                //    names. Clearing it is what makes a from-scratch re-run
                //    honest — the retry re-looks-up the key.
                //  * Re-enqueueing is a hot loop: ~3.3M admission checks in
                //    under 4 s were measured. The shard's memory wait list
                //    is the mechanism for exactly this, and it demands a
                //    request carrying no per-attempt state, which is now
                //    true. The cleaner wakes the list when memory frees
                //    (DequeueWaitListAfterMemoryFree) and aborts it when it
                //    cannot (AbortRequestsAfterMemoryFree) — liveness and
                //    termination both come from machinery that already
                //    exists, instead of from a spin plus a deadline.
                // Ask for a reclamation campaign. The cleaner's own
                // Full() trigger is NOT enough: admission refuses at
                // `allocated + requested > limit`, so in the gap below the
                // hard limit the cleaner would reclaim nothing and wake this
                // request straight back into the same refusal.
                shard_->RequestCleanCampaign();
                cce->RecycleKeyLock(*shard_);
                req.SetCcePtr(nullptr);
                req.block_type_ = ApplyCc::ApplyBlockType::NoBlocking;
                shard_->WakeUpShardCleanCc();
                shard_->EnqueueWaitListIfMemoryFull(&req);
                return false;
            }
            // The pin keeps the entry — and the lock structure that owns the
            // fetch hub — alive while this request is parked. Each PageFetch
            // holds its own separate pin for the duration of its I/O, and
            // releases it in its own Execute; this one is released by the
            // resume path above.
            cce->GetOrCreateKeyLock(shard_, this, cce->GetCcPage());
            cce->GetKeyGapLockAndExtraData()->AddPin();

            for (uint32_t page_id : faults)
            {
                auto res = shard_->FetchPage(
                    this->table_name_,
                    this->GetTableSchema(),
                    TxKey(&key),
                    PageRowKind::HashPage,
                    page_id,
                    cce,
                    this->cc_ng_id_,
                    ng_term,
                    req.Txn(),
                    Sharder::MapKeyHashToHashPartitionId(key.Hash()));
                if (res == store::DataStoreHandler::DataStoreOpStatus::Retry)
                {
                    // The store is busy. Whatever was issued stays in flight
                    // and will back-fill harmlessly; this request re-runs and
                    // recomputes its fault set, exactly as the whole-record
                    // path does on a Retry.
                    //
                    // Deregister first: this request goes straight back on the
                    // shard queue, so a completion from an earlier round must
                    // not enqueue it a second time.
                    cce->GetKeyGapLockAndExtraData()
                        ->GetOrCreateFetchHub()
                        .RegisterWaiter(req.Txn(), nullptr);
                    cce->GetKeyGapLockAndExtraData()->ReleasePin();
                    cce->RecycleKeyLock(*shard_);
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
                }
            }

            // Register the wake. Without this the fetches complete, find no
            // waiter context, and the command parks forever -- which is exactly
            // what happened until partial eviction made faults reachable at
            // all. Safe to do after issuing: completions run as cc requests on
            // this same shard, so none can fire before this Execute returns.
            // The wake record goes on the ENTRY, which survives payload
            // replacement or removal; the payload only needs a context to hold
            // this txn's pins (docs/08 §4).
            paged->EnsureTxFaultContext(req.Txn());
            cce->GetKeyGapLockAndExtraData()
                ->GetOrCreateFetchHub()
                .RegisterWaiter(req.Txn(), &req);
            req.block_type_ = ApplyCc::ApplyBlockType::BlockOnPageFault;
            return true;
        };

        // Always read the cce first to check if the object exists.
        CcOperation cc_op =
            req.IsReadOnly() ? CcOperation::Read : CcOperation::ReadForWrite;

        if (req.CcePtr() != nullptr)
        {
            // A resumed request skips the first-pass block below that would
            // have set look_key, so decode it here: a resumed paged command can
            // fault again and the fault path needs the key.
            ensure_look_key();

            // The request was blocked and is now unblocked.
            cce = static_cast<CcEntry<KeyT, ValueT, false, false> *>(
                req.CcePtr());
            ccp = static_cast<CcPage<KeyT, ValueT, false, false> *>(
                cce->GetCcPage());
            assert(cce->GetKeyGapLockAndExtraData() != nullptr);
            assert(ccp != nullptr);

            if (req.block_type_ == ApplyCc::ApplyBlockType::BlockOnRead ||
                req.block_type_ == ApplyCc::ApplyBlockType::BlockOnWriteLock ||
                req.block_type_ == ApplyCc::ApplyBlockType::BlockOnCondition)
            {
                if (req.block_type_ ==
                    ApplyCc::ApplyBlockType::BlockOnCondition)
                {
                    shard_->RemoveExpiredActiveBlockingTxs();
                    if (shard_->RemoveActiveBlockingTx(req.Txn()))
                    {
                        // remove succeeds, means the txn is expired
                        hd_res->SetError(CcErrorCode::TASK_EXPIRED);
                        return true;
                    }
                }

                // FetchRecord (if need to) happens before lock acquisition. So
                // if the request resumes from lock block, the PayloadStatus
                // must not be Unknown, or this request doesn't need to
                // FetchRecord.
                assert(cce->PayloadStatus() != RecordStatus::Unknown ||
                       !need_fetch_kv(cce, cc_op, txn, cmd));

                if (req.block_type_ ==
                        ApplyCc::ApplyBlockType::BlockOnWriteLock ||
                    req.block_type_ ==
                        ApplyCc::ApplyBlockType::BlockOnCondition)
                {
                    cc_op = CcOperation::Write;
                }

                // For ON_KEY_OBJECT, we add lock regardless of whether the
                // record is deleted, so just pass RecordStatus::Normal.
                std::tie(acquired_lock, err_code) =
                    LockHandleForResumedRequest(cce,
                                                cce->CommitTs(),
                                                RecordStatus::Normal,
                                                &req,
                                                req.NodeGroupId(),
                                                ng_term,
                                                req.TxTerm(),
                                                cc_op,
                                                req.Isolation(),
                                                req.Protocol(),
                                                0,
                                                false);
                req.block_type_ = ApplyCc::ApplyBlockType::NoBlocking;
            }
            else
            {
                if (req.block_type_ == ApplyCc::ApplyBlockType::BlockOnMemory)
                {
                    // A §8 admission retry: nothing was claimed, so there is
                    // no pin to release and no fetch error to consume. Just
                    // clear the marker and fall through to re-run from the
                    // top, which recomputes the fault set and re-attempts the
                    // allocation.
                    req.block_type_ = ApplyCc::ApplyBlockType::NoBlocking;
                }
                else
                {
                    // Both fetch-parked states release the pin the request took
                    // before parking and then re-run from the top. They differ
                    // in what "the top" means, which is handled below rather
                    // than here: a BlockOnFetch resume knows the record is now
                    // present and proceeds to acquire, while a BlockOnPageFault
                    // resume re-runs ExecuteOn — possibly to yield again on the
                    // next page of a multi-round fault set (docs/08 §6).
                    assert(req.block_type_ ==
                               ApplyCc::ApplyBlockType::BlockOnFetch ||
                           req.block_type_ ==
                               ApplyCc::ApplyBlockType::BlockOnPageFault);
                    // Read the fetch-error flag BEFORE releasing the pin. The
                    // flag lives in the entry's FetchHub, and RecycleKeyLock
                    // below destroys that structure once this was the last pin
                    // — after which the error is unreadable, the command
                    // re-runs, faults on the same missing page, and parks again
                    // forever (§4).
                    bool fetch_errored = false;
                    if (req.block_type_ ==
                        ApplyCc::ApplyBlockType::BlockOnPageFault)
                    {
                        FetchHub *err_hub =
                            cce->GetKeyGapLockAndExtraData()->FetchHubPtr();
                        fetch_errored =
                            err_hub != nullptr && err_hub->ConsumeError(txn);
                    }

                    cce->GetKeyGapLockAndExtraData()->ReleasePin();
                    cce->RecycleKeyLock(*shard_);
                    if (req.block_type_ ==
                        ApplyCc::ApplyBlockType::BlockOnPageFault)
                    {
                        // Cleared only on the paged path: a page fault can
                        // recur on the next round of the same command, and the
                        // state must not look stale to that round. BlockOnFetch
                        // is left exactly as it was, so the monolithic path is
                        // unchanged.
                        req.block_type_ = ApplyCc::ApplyBlockType::NoBlocking;
                        if (fetch_errored)
                        {
                            // A failed fetch fails the command rather than
                            // re-running it into the same missing page (§4).
                            hd_res->SetError(CcErrorCode::DATA_STORE_ERR);
                            return true;
                        }
                    }
                }
            }
        }

        if (cce_addr.ExtractCce() == nullptr)
        {
            // Lock hasn't been acquired. For blocking commands, the lock is
            // released before blocking and needs to be acquired after the
            // resume.

            // First time the request is processed. Find the cce and object.
            if (cce == nullptr)
            {
                ensure_look_key();

                Iterator it = End();
                // If all data is in memory and deleted objects should be
                // skipped, use Find instead of Emplace to avoid inserting a
                // deleted CCE that would need removal.
                // ReadIntent needs to be acquired even the object does not
                // exist under RepeatableRead isolation level (WATCH command).
                if (ccm_has_full_entries_ &&
                    req.Isolation() != IsolationLevel::RepeatableRead &&
                    (req.IsReadOnly() || !cmd->ProceedOnNonExistentObject()))
                {
                    it = Find(*look_key);
                    if (it == End())
                    {
                        obj_result.rec_status_ = RecordStatus::Deleted;
                        obj_result.commit_ts_ = 1;
                        obj_result.ttl_ = UINT64_MAX;
                        hd_res->SetFinished();
                        return true;
                    }
                }
                else
                {
                    // DEL existing keys only decreases memory utilization
                    // therefore considered readonly here.
                    it = FindEmplace(
                        *look_key, false, req.IsReadOnly() || req.IsDelete());
                }
                cce = it->second;
                ccp = it.GetPage();

                if (cmd->GetBlockOperationType() == BlockOperation::Discard)
                {
                    assert(!req.apply_and_commit_);
                    if (cce != nullptr)
                    {
                        bool succeed = cce->AbortBlockCmdRequest(
                            txn, CcErrorCode::TASK_EXPIRED, shard_);
                        if (!succeed)
                        {
                            DLOG(WARNING)
                                << "AbortBlockCmdRequest fail to find "
                                   "tx in queue_block_cmds_ and "
                                   "blocking_queue_, tx: "
                                << req.Txn() << "; req: " << &req;

                            shard_->UpsertActiveBlockingTx(req.Txn(),
                                                           shard_->Now());
                        }
                    }

                    if (req.is_local_)
                    {
                        // Only local command need to call SetFinished to avoid
                        // visit freed memory.
                        hd_res->SetError(CcErrorCode::TASK_EXPIRED);
                    }

                    return true;
                }

                if (cce == nullptr)
                {
                    // The apply request needs a new cc entry but the cc map has
                    // reached the maximal capacity.
                    if (txservice_skip_kv || ccm_has_full_entries_)
                    {
                        // If skip_kv or cache replacement is disabled, all data
                        // is cached in memory. Return DELETED if this is a
                        // readonly request, error out otherwise
                        if (req.IsReadOnly())
                        {
                            obj_result.rec_status_ = RecordStatus::Deleted;
                            obj_result.commit_ts_ = 1;
                            hd_res->SetFinished();
                            return true;
                        }
                        else
                        {
                            hd_res->SetError(CcErrorCode::OUT_OF_MEMORY);
                            return true;
                        }
                    }
                    // Otherwise, block the request by putting it into wait list
                    // util capacity is available.
                    shard_->EnqueueWaitListIfMemoryFull(&req);
                    return false;
                }

                req.SetCcePtr(cce);
                if (need_fetch_kv(cce, cc_op, txn, cmd))
                {
                    CODE_FAULT_INJECTOR("disable_fetch_record_from_kv", {
                        if (is_standby_tx)
                        {
                            LOG(INFO) << "FaultInject  "
                                         "disable_fetch_record_from_kv";

                            if (cmd->IsReadOnly())
                            {
                                assert(acquired_lock == LockType::NoLock);
                                obj_result.rec_status_ = RecordStatus::Deleted;
                                hd_res->SetFinished();
                                return true;
                            }
                        }
                    });

                    // Create key lock and extra struct for the cce. Fetch
                    // record will pin the cce to prevent it from being recycled
                    // before fetch record returns.
                    cce->GetOrCreateKeyLock(shard_, this, ccp);

                    // Fetch record from storage
                    int32_t part_id =
                        Sharder::MapKeyHashToHashPartitionId(look_key->Hash());
                    auto fetch_ret_status = shard_->FetchRecord(table_name_,
                                                                table_schema_,
                                                                TxKey(look_key),
                                                                cce,
                                                                cc_ng_id_,
                                                                ng_term,
                                                                &req,
                                                                part_id);

                    if (fetch_ret_status ==
                        store::DataStoreHandler::DataStoreOpStatus::Retry)
                    {
                        // Yield and retry
                        req.SetCcePtr(nullptr);
                        shard_->Enqueue(shard_->core_id_, &req);
                    }
                    else
                    {
                        req.block_type_ = ApplyCc::ApplyBlockType::BlockOnFetch;
                    }

                    if (metrics::enable_cache_hit_rate &&
                        !req.cache_hit_miss_collected_)
                    {
                        shard_->CollectCacheMiss();
                        req.cache_hit_miss_collected_ = true;
                    }
                    return false;
                }

                if (metrics::enable_cache_hit_rate &&
                    !req.cache_hit_miss_collected_)
                {
                    shard_->CollectCacheHit();
                    req.cache_hit_miss_collected_ = true;
                }

                if (cce->HasBufferedCommandList() && !is_standby_tx &&
                    cce->PayloadStatus() != RecordStatus::Unknown)
                {
                    LOG(ERROR) << "Buffered cmds found on leader node"
                               << ", cce key: " << cce->KeyString()
                               << ", cce CommitTs: " << cce->CommitTs() << "\n"
                               << cce->BufferedCommandList();
                    assert(false);
                }
            }

            // --- Deferred acquisition for paged objects (§6) ---
            //
            // Probe the command's fault set BEFORE taking any lock. This is a
            // correctness/simplicity requirement, not an optimization: if the
            // lock were taken first and the page fetch then failed, the error
            // path would have to release exactly the lock this command took
            // while leaving alone a lock an earlier command of the same
            // transaction established — the txm knows nothing until
            // SetFinished(). Probing first means a faulting command holds
            // either nothing (single command) or a lock the transaction
            // already recorded (2nd+ command of a MULTI), and in both cases
            // an I/O error needs no release at all.
            //
            // Scoped narrowly, per §6: an existing, paged, not-fully-resident
            // payload whose TTL has not expired. That excludes monolithic and
            // non-existent objects (which cannot fault), fully resident ones
            // (which cannot yield), and expired ones (whose expiry must be
            // evaluated by the normal path below, before any page loads).
            //
            // Side-effect-free by construction: a yielding ExecuteOn records
            // its missing page ids and returns without building a reply or
            // touching the CcEntry. If it *completes*, the result is
            // discarded and control falls through to the untouched
            // acquire-then-execute sequence, which re-runs it under the lock
            // — the reply must reflect state at the command's own
            // serialization point, not from before the wait. Every paged
            // result assigns rather than accumulates, so the re-run replaces
            // it cleanly.
            //
            // Also skipped when a dirty payload exists. Two reasons, and
            // either alone is sufficient: the normal path would execute
            // against the DIRTY object, so probing the committed one computes
            // a fault set for the wrong payload; and a dirty payload means
            // this is the 2nd+ command of a transaction that ALREADY holds
            // the lock on this key, which is exactly the sanctioned
            // fault-under-a-retained-lock case (§6) — there is no lock to
            // avoid taking, so the probe would buy nothing.
            if (cce != nullptr && ccp != nullptr &&
                cce->PayloadStatus() == RecordStatus::Normal &&
                cce->payload_.cur_payload_ != nullptr &&
                // DirtyPayloadStatus() asserts the lock structure exists, and
                // here — before any acquisition — it may not. No lock
                // structure means no dirty payload, which satisfies the
                // condition, so short-circuit instead of calling it.
                (cce->GetKeyGapLockAndExtraData() == nullptr ||
                 cce->DirtyPayloadStatus() != RecordStatus::Normal))
            {
                TxObject *probe_obj = cce->payload_.cur_payload_.get();
                PagedTxObject *paged = probe_obj->AsPaged();
                bool ttl_expired =
                    probe_obj->HasTTL() &&
                    probe_obj->GetTTL() < shard_->NowInMilliseconds();
                if (paged != nullptr && !paged->IsFullyResident() &&
                    !ttl_expired)
                {
                    // Grantability verdict, taken BEFORE the probe and in
                    // place of the conventional acquisition: it reports what
                    // AcquireCceKeyLock would return, without acquiring and
                    // without enqueuing (§6). Consumed after execution.
                    LockType probe_want = LockTypeUtil::DeduceLockType(
                        cc_op, req.Isolation(), req.Protocol(), false);
                    NonBlockingLock &probe_lk =
                        cce->GetOrCreateKeyLock(shard_, this, ccp);
                    bool probe_grantable =
                        probe_lk.WouldAcquireLock(
                            txn, req.Protocol(), probe_want) ==
                        LockOpStatus::Successful;

                    ExecResult probe_rst = cmd->ExecuteOn(*probe_obj);
                    if (probe_rst == ExecResult::Yield)
                    {
                        park_on_page_faults(cce, *look_key, probe_obj, ng_term);
                        return false;
                    }

                    // Uncontended: nothing can interleave between the verdict
                    // and the acquisition below — one shard core, and a
                    // non-yielding ExecuteOn has no suspension point — so that
                    // acquisition cannot fail and no concurrent commit
                    // intervened. The probe's run is therefore the
                    // authoritative one, and re-executing would be pure waste
                    // (§6 "test-then-acquire is atomic on the uncontended
                    // path"). Contended: the probe was only a fault-set
                    // discovery pass, its reply predates the wait, so it is
                    // discarded and the normal path re-runs under the lock.
                    //
                    // A TTL'd object is excluded from REUSE but not from the
                    // probe: the normal path evaluates expiry after
                    // acquisition, against a wall clock that could cross the
                    // deadline in between, and reusing a run made against a
                    // then-live object would bypass that verdict. Those keep
                    // the probe's correctness benefit and only pay the second
                    // execution.
                    if (probe_grantable && !probe_obj->HasTTL())
                    {
                        probe_authoritative = true;
                        probe_exec_rst = probe_rst;
                    }
                }
            }

            // For ON_KEY_OBJECT, we add lock regardless of whether the record
            // is deleted, so just pass RecordStatus::Normal.
            assert(cce != nullptr);
            assert(ccp != nullptr);
            std::tie(acquired_lock, err_code) =
                AcquireCceKeyLock(cce,
                                  cce->CommitTs(),
                                  ccp,
                                  RecordStatus::Normal,
                                  &req,
                                  req.NodeGroupId(),
                                  ng_term,
                                  req.TxTerm(),
                                  cc_op,
                                  req.Isolation(),
                                  req.Protocol(),
                                  0,
                                  false);
        }

        switch (err_code)
        {
        case CcErrorCode::NO_ERROR:
        {
            // Lock acquired, set the result.
            obj_result.lock_acquired_ = acquired_lock;
            if (acquired_lock != LockType::NoLock)
            {
                assert(cce != nullptr);
                cce_addr.SetCceLock(reinterpret_cast<uint64_t>(
                                        cce->GetKeyGapLockAndExtraData()),
                                    ng_term,
                                    shard_->core_id_);
            }
            break;
        }
        case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
        {
            // If the read request comes from a remote node, sends
            // acknowledgement to the sender when the request is
            // blocked.
            assert(cce != nullptr &&
                   cce->GetKeyGapLockAndExtraData() != nullptr);
            cce_addr.SetCceLock(
                reinterpret_cast<uint64_t>(cce->GetKeyGapLockAndExtraData()),
                ng_term,
                shard_->core_id_);
            if (!req.IsLocal())
            {
                static_cast<remote::RemoteApplyCc *>(&req)->Acknowledge();
            }
            req.block_type_ = ApplyCc::ApplyBlockType::BlockOnRead;
            // Acquire lock fail should stop the execution of current
            // ApplyCc request since it's already in blocking queue.
            return false;
        }
        default:
        {
            // lock confilct: back off and retry.
            req.Result()->SetError(err_code);
            return true;
        }
        }
        if (cmd->IgnoreOldValue())
        {
            // cmd that ignores kv value should be applied
            // regardless of current value.
            assert(cmd->ProceedOnNonExistentObject() &&
                   cmd->ProceedOnExistentObject() &&
                   acquired_lock == LockType::WriteIntent);
            // We will pretend that there's a delete on this cce
            // just before this cmd to ignore value in kv.
            cce->SetDirtyPayloadStatus(RecordStatus::Deleted);
            cce->SetCkptTs(1);
        }

        // Process ttl expire
        // Get ttl from dirty payload at first, then from payload
        obj_result.ttl_expired_ = false;
        uint64_t ttl = UINT64_MAX;

        NonBlockingLock *lk = cce->GetKeyLock();
        bool check_dirty_status =
            cc_op != CcOperation::Read ||
            (cc_op == CcOperation::Read && lk != nullptr &&
             lk->HasWriteLock() && lk->WriteLockTx() == txn);

        assert(cc_op == CcOperation::Read ||
               acquired_lock >= LockType::WriteIntent);

        // Create the dirty object only when we have to access it.
        if (check_dirty_status &&
            cce->DirtyPayloadStatus() == RecordStatus::Uncreated)
        {
            CreateDirtyPayloadFromPendingCommand(cce);
        }

        // Whether the payload the TTL was read from is PAGED — recorded
        // here because the §16 suppression below must examine the SAME
        // payload. In a multi-command transaction the dirty payload can
        // already be a paged twin (conversion runs in CommitOn as commands
        // apply) while the committed payload stays monolithic until the
        // final swap; guarding on the committed one alone re-opened the
        // recover-image hazard for exactly that window (a review
        // follow-up).
        bool ttl_src_paged = false;
        if (check_dirty_status && cce->GetKeyLock() != nullptr &&
            cce->DirtyPayloadStatus() == RecordStatus::Normal)
        {
            std::unique_ptr<ValueT> dirty_payload = cce->DirtyPayload();
            TxObject *obj = static_cast<TxObject *>(dirty_payload.get());
            if (obj != nullptr && obj->HasTTL())
            {
                ttl = obj->GetTTL();
            }
            ttl_src_paged = obj != nullptr && obj->AsPaged() != nullptr;
            cce->SetDirtyPayload(std::move(dirty_payload));
        }
        else
        {
            TxObject *obj =
                static_cast<TxObject *>(cce->payload_.cur_payload_.get());
            if (obj != nullptr && obj->HasTTL())
            {
                ttl = obj->GetTTL();
            }
            ttl_src_paged = obj != nullptr && obj->AsPaged() != nullptr;
        }

        // if ttl is expired
        if (ttl < shard_->NowInMilliseconds())
        {
            if (req.IsReadOnly())
            {
                // early return if ttl expired when cmd is read only
                obj_result.rec_status_ = RecordStatus::Deleted;
                obj_result.commit_ts_ = ttl;
                hd_res->SetFinished();
                return true;
            }
            obj_result.ttl_expired_ = true;
        }
        // if ttl exist, not expired, cmd will not overwrite object values and
        // the cmd will reset ttl
        else if (ttl < UINT64_MAX && cmd->WillSetTTL() && !cmd->IsOverwrite())
        {
            // NEVER for a PAGED payload (eloqkv docs/08 §16): ttl_reset_
            // makes the WAL and the standby forward carry the command's
            // RecoverObjectCommand — a full-object snapshot logged as an
            // overwrite. A paged object's serialization is METADATA ONLY,
            // so that image would prune the preceding page-writes from
            // replay, and the recovery factory has no paged arms by design
            // (a reported bug: the standby aborted on the TTLPagedHash tag).
            // The plain TTL command replays through CommitOn — the virtual
            // AddTTL/RemoveTTL twin swap — like every other paged mutation.
            // The guard examines the payload the TTL CAME FROM (dirty-first,
            // the same selection as above): the command's ExecuteOn runs on
            // that payload and would serialize IT into the recover image.
            if (!ttl_src_paged)
            {
                obj_result.ttl_reset_ = true;
            }
        }

        bool object_not_exist;
        bool s_obj_exist = (cce->PayloadStatus() == RecordStatus::Normal);

        // If reaches here and the payload status is still unknown, there must
        // be a command that ignores the KV value, either this command or a
        // previous command of this txn. Because only commands that ignore KV
        // value skip FetchRecord and leave the payload status unknown.
        if (cce->PayloadStatus() == RecordStatus::Unknown &&
            !cmd->IgnoreOldValue())
        {
            assert(cce->DirtyPayloadStatus() != RecordStatus::Uncreated);
            assert(lk != nullptr);
            assert(lk->HasWriteLock());
            assert(lk->WriteLockTx() == txn);
        }
        assert(cce->PayloadStatus() != RecordStatus::Unknown ||
               cce->DirtyPayloadStatus() != RecordStatus::Uncreated ||
               cmd->IgnoreOldValue());

        object_not_exist =
            check_dirty_status
                // If dirty payload exists, use dirty_payload_status. Use
                // payload status only if dirty payload doesn't exist.
                ? cce->DirtyPayloadStatus() == RecordStatus::Deleted ||
                      (cce->DirtyPayloadStatus() == RecordStatus::NonExistent &&
                       cce->PayloadStatus() == RecordStatus::Deleted)
                : cce->PayloadStatus() == RecordStatus::Deleted;

        // This branch processes and returns the results for all read-only
        // commands.
        if (cmd->IsReadOnly())
        {
            // Early return logic for read-only command.

            if (object_not_exist)
            {
                assert(!cmd->ProceedOnNonExistentObject());

                obj_result.rec_status_ = RecordStatus::Deleted;
            }
            else if (!cmd->ProceedOnExistentObject())
            {
                obj_result.rec_status_ = RecordStatus::Normal;
            }
            // Object exists and proceeds
            else if (check_dirty_status)
            {
                RecordStatus dirty_payload_status = cce->DirtyPayloadStatus();
                if (dirty_payload_status == RecordStatus::Normal)
                {
                    std::unique_ptr<ValueT> dirty_payload = cce->DirtyPayload();
                    assert(dirty_payload != nullptr);

                    // Temporary object exists, execute and commit the command
                    // on the temporary object.
                    ValueT &dirty_object = *dirty_payload;
                    ExecResult ro_rst = cmd->ExecuteOn(dirty_object);
                    if (ro_rst == ExecResult::Yield)
                    {
                        // Put the payload back before parking: the object
                        // must stay reachable for the fetch completion to
                        // back-fill into, and for the re-run to execute on.
                        TxObject *paged_obj = dirty_payload.get();
                        cce->SetDirtyPayload(std::move(dirty_payload));
                        cce->SetDirtyPayloadStatus(dirty_payload_status);
                        if (park_on_page_faults(
                                cce, *look_key, paged_obj, ng_term))
                        {
                            return false;
                        }
                        return false;
                    }
                    cce->SetDirtyPayload(std::move(dirty_payload));
                    cce->SetDirtyPayloadStatus(dirty_payload_status);
                    obj_result.rec_status_ = dirty_payload_status;
                }
                else
                {
                    assert(cce->PayloadStatus() == RecordStatus::Normal);
                    assert(cce->IsNullPendingCmd());
                    ValueT &object = *cce->payload_.cur_payload_;
                    // Reuse the probe's run when it was authoritative (§6).
                    if (!probe_authoritative &&
                        cmd->ExecuteOn(object) == ExecResult::Yield)
                    {
                        park_on_page_faults(cce, *look_key, &object, ng_term);
                        return false;
                    }
                    obj_result.rec_status_ = cce->PayloadStatus();
                }
            }
            else
            {
                assert(cce->PayloadStatus() == RecordStatus::Normal);
                assert(cce->payload_.cur_payload_ != nullptr);
                ValueT &object = *cce->payload_.cur_payload_;
                if (!probe_authoritative &&
                    cmd->ExecuteOn(object) == ExecResult::Yield)
                {
                    park_on_page_faults(cce, *look_key, &object, ng_term);
                    return false;
                }
                obj_result.rec_status_ = cce->PayloadStatus();
            }

            if (req.apply_and_commit_)
            {
                // Release and try to recycle the lock.
                if (acquired_lock != LockType::NoLock)
                {
                    assert(req.Isolation() > IsolationLevel::ReadCommitted);
                    ReleaseCceLock(
                        cce->GetKeyLock(), cce, txn, ng_id, acquired_lock);
                }
                // The command is done, so its page pins must go (docs/08 §6:
                // pins bridge bounded I/O gaps only and must never outlive the
                // command). ReleaseCceLock above is the ONLY other caller of
                // ReleaseTxPagePins, and it is skipped whenever no lock was
                // taken — the normal case for a ReadCommitted read. Without
                // this, every faulting read leaks its pins, no page is ever
                // evictable, the shard heap never reclaims and the whole shard
                // wedges with everything clean. Idempotent: a second call for a
                // txn with no context does nothing.
                ReleaseTxPagePins(cce, txn);
                obj_result.lock_acquired_ = LockType::NoLock;
            }

            assert(obj_result.rec_status_ != RecordStatus::Unknown);
            obj_result.commit_ts_ = cce->CommitTs();
            obj_result.lock_ts_ = shard_->Now();
            hd_res->SetFinished();
            return true;
        }

        // This is a write command.
        assert(acquired_lock >= LockType::WriteIntent);

        // 1. Upgrade to the write lock if the write command proceeds.
        if (acquired_lock != LockType::WriteLock)
        {
            bool need_write_lock =
                (!object_not_exist && cmd->ProceedOnExistentObject()) ||
                (object_not_exist && cmd->ProceedOnNonExistentObject()) ||
                (obj_result.ttl_expired_ || obj_result.ttl_reset_);

            // acquire write lock if need futher process
            if (need_write_lock)
            {
                // Upgrade to write lock
                std::tie(acquired_lock, err_code) =
                    AcquireCceKeyLock(cce,
                                      cce->CommitTs(),
                                      ccp,
                                      RecordStatus::Normal,
                                      &req,
                                      req.NodeGroupId(),
                                      ng_term,
                                      req.TxTerm(),
                                      CcOperation::Write,
                                      req.Isolation(),
                                      req.Protocol(),
                                      0,
                                      false);
            }
            else
            {
                // Early return logic for read-write command.
                if (req.apply_and_commit_)
                {
                    // Release and try to recycle the lock.
                    assert(acquired_lock != LockType::NoLock);
                    ReleaseCceLock(
                        cce->GetKeyLock(), cce, txn, ng_id, acquired_lock);
                    obj_result.lock_acquired_ = LockType::NoLock;
                }

                obj_result.rec_status_ = object_not_exist
                                             ? RecordStatus::Deleted
                                             : RecordStatus::Normal;

                obj_result.commit_ts_ = cce->CommitTs();
                obj_result.lock_ts_ = shard_->Now();
                obj_result.ttl_ = ttl;
                hd_res->SetFinished();
                return true;
            }

            switch (err_code)
            {
            case CcErrorCode::NO_ERROR:
            {
                // lock acquired
                assert(acquired_lock == LockType::WriteLock);
                obj_result.lock_acquired_ = acquired_lock;
                break;
            }
            case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
            {
                // If the read request comes from a remote node, sends
                // acknowledgement to the sender when the request is
                // blocked.

                assert(cce != nullptr &&
                       cce->GetKeyGapLockAndExtraData() != nullptr);
                cce_addr.SetCceLock(reinterpret_cast<uint64_t>(
                                        cce->GetKeyGapLockAndExtraData()),
                                    ng_term,
                                    shard_->core_id_);
                if (!req.IsLocal())
                {
                    static_cast<remote::RemoteApplyCc *>(&req)->Acknowledge();
                }
                req.block_type_ = ApplyCc::ApplyBlockType::BlockOnWriteLock;
                // Acquire lock fail should stop the execution of current
                // ApplyCc request since it's already in blocking queue.
                return false;
            }
            default:
            {
                // lock confilct: back off and retry.
                req.Result()->SetError(err_code);
                return true;
            }
            }
            assert(ccp != nullptr);
        }

        assert(obj_result.lock_acquired_ == LockType::WriteLock);

        // 2. Execute the command on dirty object or the real object.

        StandbyForwardEntry *forward_entry = nullptr;
        remote::KeyObjectStandbyForwardRequest *forward_req = nullptr;
        if (!shard_->GetSubscribedStandbys().empty())
        {
            forward_entry = cce->ForwardEntry();
            if (!forward_entry)
            {
                auto forward_entry_ptr =
                    std::make_unique<StandbyForwardEntry>();
                forward_entry = forward_entry_ptr.get();
                cce->SetForwardEntry(std::move(forward_entry_ptr));
                forward_req = &forward_entry->Request();
                forward_req->set_primary_leader_term(ng_term);
                forward_req->set_tx_number(req.Txn());
                forward_req->set_table_name(table_name_.String());
                forward_req->set_table_type(
                    remote::ToRemoteType::ConvertTableType(table_name_.Type()));
                forward_req->set_table_engine(
                    remote::ToRemoteType::ConvertTableEngine(
                        table_name_.Engine()));
                forward_req->set_key_shard_code(req.key_shard_code_);
                if (cce->PayloadStatus() == RecordStatus::Deleted)
                {
                    forward_req->set_has_overwrite(true);
                }
                std::string key_str;

                if (req.Key() == nullptr)
                {
                    assert(req.KeyImage() != nullptr &&
                           !req.KeyImage()->empty());
                    key_str = *req.KeyImage();
                }
                else
                {
                    req.Key()->Serialize(key_str);
                }

                forward_req->set_key(std::move(key_str));
            }
            else
            {
                forward_req = &forward_entry->Request();
                assert(forward_req->tx_number() == req.Txn());
            }
        }

        // if cce is already expired
        if (obj_result.ttl_expired_)
        {
            cce->SetDirtyPayload(nullptr);
            cce->SetDirtyPayloadStatus(RecordStatus::Deleted);
            cce->SetPendingCmd(nullptr);
            object_not_exist = true;
            if (forward_entry)
            {
                // Forward retire command to standby node to clear the object
                auto retire_command = cmd->RetireExpiredTTLObjectCommand();
                forward_entry->AddOverWriteCommand(retire_command.get());
            }
            // Object not exist due to ttl expired,
            // for command on exist object, return early
            if (cmd->ProceedOnExistentObject() &&
                !cmd->ProceedOnNonExistentObject())
            {
                // Early return logic for read-write command.
                if (req.apply_and_commit_)
                {
                    if (s_obj_exist)
                    {
                        --TemplateCcMap<KeyT, ValueT, false, false>::
                            normal_obj_sz_;
                    }
                    // The object is being dropped, so a paged payload's
                    // in-flight fetches must be orphaned and its parked
                    // readers re-enqueued first — otherwise their contexts
                    // die with the object and they wait forever (docs/08 §7).
                    ApplyPayloadSwapRule(cce);
                    cce->payload_.cur_payload_ = nullptr;
                    const uint64_t commit_ts = std::max(
                        {cce->CommitTs() + 1, req.TxTs(), shard_->Now()});
                    if (forward_entry)
                    {
                        // Set commit ts and send the msg to standby node
                        forward_req->set_commit_ts(commit_ts);
                        if (cce->PayloadStatus() == RecordStatus::Unknown)
                        {
                            assert(cmd->IgnoreOldValue());
                            forward_req->set_object_version(1);
                        }
                        else
                        {
                            assert(cce->CommitTs() > 0);
                            forward_req->set_object_version(cce->CommitTs());
                        }
                        forward_entry->Request().set_schema_version(schema_ts_);
                        std::unique_ptr<StandbyForwardEntry> entry_ptr =
                            cce->ReleaseForwardEntry();
                        shard_->ForwardStandbyMessage(entry_ptr.release());
                    }
                    bool was_dirty = cce->IsDirty();
                    cce->SetCommitTsPayloadStatus(commit_ts,
                                                  RecordStatus::Deleted);
                    this->OnCommittedUpdate(cce, was_dirty);
                    // Release and try to recycle the lock.
                    assert(acquired_lock != LockType::NoLock);
                    ReleaseCceLock(
                        cce->GetKeyLock(), cce, txn, ng_id, acquired_lock);
                    obj_result.lock_acquired_ = LockType::NoLock;
                }
                obj_result.rec_status_ = RecordStatus::Deleted;
                obj_result.commit_ts_ = cce->CommitTs();
                obj_result.lock_ts_ = shard_->Now();
                obj_result.ttl_ = UINT64_MAX;
                hd_res->SetFinished();
                return true;
            }
        }
        else if (obj_result.ttl_reset_)
        {
            // cmd will be processed as usual, but a recover obj cmd log will be
            // written
        }

        RecordStatus dirty_payload_status = cce->DirtyPayloadStatus();
        if (object_not_exist)
        {
            // The object does not exist but the write lock is acquired.
            assert(cmd->ProceedOnNonExistentObject());
            // Create an empty temporary object to process the commands, the
            // dirty payload will be uploaded to payload in PostWriteCc if
            // the txn commits.
            std::unique_ptr<ValueT> dirty_payload = cce->DirtyPayload();
            std::tie(dirty_payload, dirty_payload_status) =
                CreateDirtyPayloadFromCommand(cmd);
            cce->SetDirtyPayload(std::move(dirty_payload));
            cce->SetDirtyPayloadStatus(dirty_payload_status);
            cce->SetPendingCmd(nullptr);
            if (forward_req)
            {
                // command will be added below if dirty payload status is
                // not deleted.
                if (cce->PayloadStatus() == RecordStatus::Unknown)
                {
                    forward_req->set_object_version(1);
                }
                else
                {
                    assert(cce->CommitTs() > 0);
                    forward_req->set_object_version(cce->CommitTs());
                }
            }
        }

        ExecResult exec_rst = ExecResult::Fail;
        if (dirty_payload_status == RecordStatus::Normal)
        {
            std::unique_ptr<ValueT> dirty_payload = cce->DirtyPayload();
            assert(dirty_payload != nullptr);

            // Temporary object exists, execute and commit the command on
            // the temporary object.
            ValueT &dirty_object = *dirty_payload;
            exec_rst = cmd->ExecuteOn(dirty_object);
            if (exec_rst == ExecResult::Yield)
            {
                // The write lock is already held here and stays held across
                // the fault — the §6 case where a fault necessarily happens
                // under a retained lock. It cannot deadlock: this request
                // waits on store I/O, never on another transaction.
                TxObject *paged_obj = dirty_payload.get();
                cce->SetDirtyPayload(std::move(dirty_payload));
                cce->SetDirtyPayloadStatus(dirty_payload_status);
                park_on_page_faults(cce, *look_key, paged_obj, ng_term);
                return false;
            }
            object_deleted = exec_rst == ExecResult::Delete;
            object_modified = object_deleted || exec_rst == ExecResult::Write;

            if (object_modified)
            {
                if (forward_entry)
                {
                    if (object_deleted)
                    {
                        // If the command modifies the object into delete state,
                        // like rpop, zrem, add a delete command.
                        auto retire_command =
                            cmd->RetireExpiredTTLObjectCommand();
                        forward_entry->AddOverWriteCommand(
                            retire_command.get());
                    }
                    else
                    {
                        forward_entry->AddTxCommand(req);
                    }
                }
                bool applied = CommitCommandOnDirtyPayload(
                    dirty_payload, dirty_payload_status, *cmd);
                // A page fault is impossible on this path: ExecuteOn ran
                // earlier in this transaction and pinned every page it
                // touched, and the shed policy skips pinned pages. A false
                // here means a pin was released too early, which would
                // otherwise drop the write silently.
                assert(applied && "CommitOn faulted on a pinned paged object");
                (void) applied;
            }
            // if cmd.ExecuteOn() telling ttl reset is not going to happen
            else if (obj_result.ttl_reset_ == true)
            {
                obj_result.ttl_reset_ = false;
            }

            cce->SetDirtyPayload(std::move(dirty_payload));
            cce->SetDirtyPayloadStatus(dirty_payload_status);
        }
        else if (cce->PayloadStatus() == RecordStatus::Normal)
        {
            // The dirty payload does not exist. This is the first command.
            // Execute and copy the command. The command will be committed
            // in PostWriteCc if the txn commits.
            assert(cce->IsNullPendingCmd());
            assert(cce->payload_.cur_payload_ != nullptr);
            ValueT &object = *cce->payload_.cur_payload_;
            // Reuse the probe's run when it was authoritative (§6): the
            // verdict said grantable and nothing could interleave, so that
            // run is the command's real execution.
            exec_rst =
                probe_authoritative ? probe_exec_rst : cmd->ExecuteOn(object);
            if (exec_rst == ExecResult::Yield)
            {
                park_on_page_faults(cce, *look_key, &object, ng_term);
                return false;
            }
            object_deleted = exec_rst == ExecResult::Delete;
            object_modified = object_deleted || exec_rst == ExecResult::Write;

            if (object_modified)
            {
                if (forward_entry)
                {
                    forward_req->set_object_version(cce->CommitTs());
                    if (obj_result.ttl_reset_)
                    {
                        // Forward recover command to standby node to recover
                        // the object in case the object is removed from old
                        // node due to ttl expired.
                        auto recover_command = cmd->RecoverTTLObjectCommand();
                        forward_entry->AddOverWriteCommand(recover_command);
                    }

                    if (object_deleted)
                    {
                        // If the command modifies the object into delete state,
                        // like rpop, zrem, add a delete command.
                        auto retire_command =
                            cmd->RetireExpiredTTLObjectCommand();
                        forward_entry->AddOverWriteCommand(
                            retire_command.get());
                    }
                    else
                    {
                        forward_entry->AddTxCommand(req);
                    }
                }
                if (!req.apply_and_commit_)
                {
                    // Copy the command to be committed in PostWriteCc or when
                    // executing subsequent commands of the same txn.
                    if (req.IsLocal())
                    {
                        if (cmd->IsVolatile())
                        {
                            // If this command is volatile, it will need to
                            // clone a new instance to ensure it can be commit
                            // in PostWriteCc.
                            cce->SetPendingCmd(cmd->Clone());
                        }
                        else
                        {
                            // If the command is exist until transaction
                            // committed, it does not need to clone a new
                            // instance and use original cmd in PostWriteCc.
                            cce->SetPendingCmd(cmd);
                        }
                    }
                    else
                    {
                        // For remote ApplyCC, it will transfer the ownership
                        // from ApplyCC into pending cmd, so ApplyCC does not
                        // need to release this command.
                        cce->SetPendingCmd(std::unique_ptr<TxCommand>(cmd));
                        req.RemoveOwnership();
                    }

                    // The object is being modified, set dirty_payload_status_
                    // to Uncreated so that a temporary object will be created
                    // when processing subsequent commands of the same txn. In
                    // PostWriteCc, the original object will be replaced by the
                    // temporary object if the txn commits.
                    cce->SetDirtyPayloadStatus(RecordStatus::Uncreated);
                }
            }

            // if cmd.ExecuteOn() telling ttl reset is not going to happen
            if (!object_modified && obj_result.ttl_reset_ == true)
            {
                obj_result.ttl_reset_ = false;
            }
        }

        if (exec_rst == ExecResult::Block)
        {
            assert(!req.apply_and_commit_);
            if (forward_entry)
            {
                // Release forward entry (will be automatically freed)
                cce->ReleaseForwardEntry();
            }
            cce->PushBlockCmdRequest(&req);
            cce->SetDirtyPayload(nullptr);
            cce->SetDirtyPayloadStatus(RecordStatus::NonExistent);
            assert(acquired_lock != LockType::NoLock);
            ReleaseCceLock(cce->GetKeyLock(), cce, txn, ng_id, acquired_lock);
            // TODO(zkl): acquire ReadIntent before blocking?
            obj_result.lock_acquired_ = LockType::NoLock;
            req.block_type_ = ApplyCc::ApplyBlockType::BlockOnCondition;
            return false;
        }

        if (exec_rst == ExecResult::Unlock)
        {
            assert(!req.apply_and_commit_);
            if (forward_entry)
            {
                // Release forward entry (will be automatically freed)
                cce->ReleaseForwardEntry();
            }
            cce->SetDirtyPayload(nullptr);
            cce->SetDirtyPayloadStatus(RecordStatus::NonExistent);
            assert(acquired_lock != LockType::NoLock);
            ReleaseCceLock(cce->GetKeyLock(), cce, txn, ng_id, acquired_lock);
            obj_result.lock_acquired_ = LockType::NoLock;
            obj_result.commit_ts_ = 1;
            obj_result.lock_ts_ = shard_->Now();
            obj_result.rec_status_ = RecordStatus::Deleted;
            obj_result.ttl_ = UINT64_MAX;
            hd_res->SetFinished();
            return true;
        }

        if (req.apply_and_commit_)
        {
            if (object_modified)
            {
                // Skipping writing log, do the PostWrite and release the
                // lock.
                assert(acquired_lock == LockType::WriteLock);
                RecordStatus status = cce->PayloadStatus();
                if (dirty_payload_status == RecordStatus::Normal ||
                    dirty_payload_status == RecordStatus::Deleted)
                {
                    // Dirty payload exists. Use it to replace payload.
                    ApplyPayloadSwapRule(cce);
                    cce->payload_.PassInCurrentPayload(cce->DirtyPayload());
                    status = dirty_payload_status;
                }
                else
                {
                    bool applied = CommitCommandOnPayload(
                        cce, cce->payload_.cur_payload_, status, *cmd);
                    // A page fault is impossible on this path: ExecuteOn ran
                    // earlier in this transaction and pinned every page it
                    // touched, and the shed policy skips pinned pages. A false
                    // here means a pin was released too early, which would
                    // otherwise drop the write silently.
                    assert(applied &&
                           "CommitOn faulted on a pinned paged object");
                    (void) applied;
                }

                // Reset the dirty status.
                cce->SetDirtyPayload(nullptr);
                cce->SetDirtyPayloadStatus(RecordStatus::NonExistent);
                cce->SetPendingCmd(nullptr);
                // It's possible that the cce HasBufferedCommandList and is
                // still in unknown status (because FetchRecord fails) and
                // this command ignores kv value. Need to clear the buffered
                // commands.
                cce->BufferedCommandList().Clear();

                // Set commit ts based on the TxTs since there is no
                // PostWriteCc if apply_and_commit_.
                const uint64_t commit_ts =
                    std::max({cce->CommitTs() + 1, req.TxTs(), shard_->Now()});
                bool was_dirty = cce->IsDirty();
                StampPagedWrites(cce, commit_ts);
                cce->SetCommitTsPayloadStatus(commit_ts, status);
                this->OnCommittedUpdate(cce, was_dirty);

                if (forward_entry)
                {
                    // Set commit ts and send the msg to standby node
                    forward_req->set_commit_ts(commit_ts);
                    forward_entry->Request().set_schema_version(schema_ts_);
                    std::unique_ptr<StandbyForwardEntry> entry_ptr =
                        cce->ReleaseForwardEntry();
                    shard_->ForwardStandbyMessage(entry_ptr.release());
                }

                if (last_dirty_commit_ts_ < commit_ts)
                {
                    last_dirty_commit_ts_ = commit_ts;
                }
                if (commit_ts > ccp->last_dirty_commit_ts_)
                {
                    ccp->last_dirty_commit_ts_ = commit_ts;
                }

                if (ccp->smallest_ttl_ != 0)
                {
                    if (status == RecordStatus::Normal)
                    {
                        if (cce->payload_.cur_payload_ &&
                            cce->payload_.cur_payload_->HasTTL() &&
                            ccp->smallest_ttl_ >
                                cce->payload_.cur_payload_->GetTTL())
                        {
                            ccp->smallest_ttl_ =
                                cce->payload_.cur_payload_->GetTTL();
                        }
                    }
                    else
                    {
                        assert(cce->PayloadStatus() == RecordStatus::Deleted);
                        ccp->smallest_ttl_ = 0;
                    }
                }

                if (shard_->GetCacheEvictPolicy() == CacheEvictPolicy::LO_LRU)
                {
                    EnsureLargeObjOccupyPageAlone(ccp, cce);
                }
            }
            else
            {
                cce->SetDirtyPayload(nullptr);
                cce->SetDirtyPayloadStatus(RecordStatus::NonExistent);
                if (forward_entry)
                {
                    // Release forward entry (will be automatically freed)
                    cce->ReleaseForwardEntry();
                }
            }

            // Release and try to recycle the lock.
            assert(acquired_lock != LockType::NoLock);
            ReleaseCceLock(
                cce->GetKeyLock(),
                cce,
                txn,
                ng_id,
                acquired_lock,
                true,
                object_modified ? cce->payload_.cur_payload_.get() : nullptr);
            obj_result.lock_acquired_ = LockType::NoLock;

            if (s_obj_exist && cce->PayloadStatus() != RecordStatus::Normal)
            {
                TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_--;
            }
            else if (!s_obj_exist &&
                     cce->PayloadStatus() == RecordStatus::Normal)
            {
                TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_++;
            }
        }

        // Updates last_vali_ts after successfully acquiring the write
        // lock such that it is not smaller than the current time of
        // the shard. The net effect is that the tx acquiring the write
        // lock is forced not to commit at a time earlier than the
        // clock of this cc node, even if the clock of the tx's
        // coordinator node drifts and falls behind. Checkpointing
        // relies on this property to avoid picking a checkpoint ts in
        // this shard that may overlap with the ongoing tx.
        obj_result.last_vali_ts_ =
            std::max(shard_->LastReadTs(), shard_->Now());

        if (cce->PayloadStatus() == RecordStatus::Unknown)
        {
            // If this command ignores the old kv value, just pass
            // in as deleted and current ts so that the tx will
            // commit at a larger commit ts.
            obj_result.commit_ts_ = 1;
            obj_result.lock_ts_ = shard_->Now();
            obj_result.rec_status_ = RecordStatus::Deleted;
        }
        else
        {
            obj_result.commit_ts_ = cce->CommitTs();
            obj_result.lock_ts_ = shard_->Now();
            obj_result.rec_status_ = cce->PayloadStatus();
        }

        obj_result.ttl_ = ComputeReportedTtl(obj_result.ttl_reset_,
                                             obj_result.ttl_expired_,
                                             cmd->IsOverwrite(),
                                             ttl);

        if (obj_result.ttl_reset_)
        {
            // The command resets a live TTL: WAL replay must not depend on a
            // base object that may be gone, so ship the full-object snapshot
            // in the result. Single capture point for local and remote
            // coordinators alike — the coordinator always logs the image from
            // the result, never from its own (possibly never-executed)
            // command.
            TxCommand *recover_cmd = cmd->RecoverTTLObjectCommand();
            assert(recover_cmd != nullptr);
            obj_result.recover_cmd_image_.clear();
            recover_cmd->Serialize(obj_result.recover_cmd_image_);
        }

        hd_res->SetFinished();
        return true;
    }

    bool Execute(PostWriteCc &req) override
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"template_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append("0");
            });
        TX_TRACE_DUMP(&req);

        TxNumber txn = req.Txn();
        uint64_t commit_ts = req.CommitTs();
        OperationType op_type = req.GetOperationType();
        assert(op_type == OperationType::CommitCommands);
        (void) op_type;

        const CcEntryAddr *cce_addr = req.CceAddr();

        CcEntry<KeyT, ValueT, false, false> *cce =
            reinterpret_cast<CcEntry<KeyT, ValueT, false, false> *>(
                cce_addr->ExtractCce());

        // check that this txn is lock owner
        NonBlockingLock *lk = cce->GetKeyLock();
        if (lk == nullptr || !lk->HasWriteLock() || lk->WriteLockTx() != txn)
        {
            req.Result()->SetFinished();
            return true;
        }

        CcPage<KeyT, ValueT, false, false> *ccp =
            static_cast<CcPage<KeyT, ValueT, false, false> *>(cce->GetCcPage());
        assert(ccp != nullptr);
        bool s_obj_exist = (cce->PayloadStatus() == RecordStatus::Normal);

        auto subscribed_standbys = shard_->GetSubscribedStandbys();
        bool has_subscribed_standby = !subscribed_standbys.empty();
        StandbyForwardEntry *forward_entry = cce->ForwardEntry();
        LOG_IF(WARNING, has_subscribed_standby && forward_entry == nullptr)
            << "Subscribed standbys exist, but forward_entry is null. "
               "Data loss may occur.";

        // The §8 write-side memory park (eloqkv docs/08). A paged commit
        // allocates — COW copies, split pages — and none of it can be
        // refused once the mutation starts: the transaction is durable in
        // the WAL, so commit may stall but never fail. So the stall happens
        // HERE, before anything is touched: while the shard heap is over
        // budget, kick the clean pass and re-enqueue, holding the write
        // lock. The lock must be held for the duration — released early, a
        // queued writer could commit against the pre-image of a durable
        // transaction and fork the version chain. It cannot deadlock through
        // the lock: reclaim sheds other, clean pages and never needs this
        // entry. Termination is the §8 axiom's job (after a clean pass the
        // budget holds the largest dirty set plus one command's working
        // allocations); a deployment that violates it stalls visibly here
        // rather than corrupting. Aborts (commit_ts == 0) never park —
        // rolling back frees memory.
        //
        // Scope: the committed payload's representation decides. A
        // conversion commit (monolithic current, paged dirty) passes
        // ungated; its allocation is bounded by the conversion threshold.
        bool commit_over_budget = !AdmitPageBytes(0);
        // Test hook (Debug builds): force the park while armed, so the park +
        // resume + lock-retention behaviour is testable deterministically —
        // filling a real shard heap on demand is neither fast nor reliable.
        CODE_FAULT_INJECTOR("force_paged_commit_park", {
            LOG_EVERY_N(INFO, 100) << "FAULTLOG force_paged_commit_park";
            commit_over_budget = true;
        });
        if (commit_ts > 0 && cce->payload_.cur_payload_ != nullptr &&
            static_cast<const TxObject &>(*cce->payload_.cur_payload_)
                    .AsPaged() != nullptr &&
            commit_over_budget)
        {
            LOG_EVERY_N(WARNING, 1000)
                << "paged commit parked on memory (shard heap over budget), "
                   "key: "
                << cce->KeyString();
            shard_->WakeUpShardCleanCc();
            shard_->Enqueue(shard_->LocalCoreId(), &req);
            return false;
        }

        if (commit_ts > 0)
        {
            RecordStatus dirty_payload_status = cce->DirtyPayloadStatus();
            RecordStatus payload_status = cce->PayloadStatus();
            // The txn commits. Upload the change.
            if (dirty_payload_status == RecordStatus::Normal ||
                dirty_payload_status == RecordStatus::Deleted)
            {
                // Dirty payload exists. Use it to replace payload.
                payload_status = dirty_payload_status;
                ApplyPayloadSwapRule(cce);
                cce->payload_.PassInCurrentPayload(cce->DirtyPayload());
            }
            else
            {
                // Commit the pending command.
                auto var_cmd = cce->PendingCmd();
                TxCommand *pending_cmd = nullptr;
                if (std::holds_alternative<TxCommand *>(var_cmd))
                {
                    pending_cmd = std::get<TxCommand *>(var_cmd);
                }
                else
                {
                    pending_cmd =
                        std::get<std::unique_ptr<TxCommand>>(var_cmd).get();
                }

                if (pending_cmd != nullptr)
                {
                    assert(cce->payload_.cur_payload_ != nullptr);
                    bool applied =
                        CommitCommandOnPayload(cce,
                                               cce->payload_.cur_payload_,
                                               payload_status,
                                               *pending_cmd);
                    // A page fault is impossible on this path: ExecuteOn ran
                    // earlier in this transaction and pinned every page it
                    // touched, and the shed policy skips pinned pages. A false
                    // here means a pin was released too early, which would
                    // otherwise drop the write silently.
                    assert(applied &&
                           "CommitOn faulted on a pinned paged object");
                    (void) applied;
                }
                else
                {
                    assert(false);
                }
            }
            if (forward_entry)
            {
                if (has_subscribed_standby)
                {
                    // Set commit ts and send the msg to standby node.
                    forward_entry->Request().set_commit_ts(commit_ts);
                    forward_entry->Request().set_schema_version(schema_ts_);
                    std::unique_ptr<StandbyForwardEntry> entry_ptr =
                        cce->ReleaseForwardEntry();
                    shard_->ForwardStandbyMessage(entry_ptr.release());
                }
                else
                {
                    // No standby needs this entry anymore.
                    cce->ReleaseForwardEntry();
                }
            }
            bool was_dirty = cce->IsDirty();
            StampPagedWrites(cce, commit_ts);
            cce->SetCommitTsPayloadStatus(commit_ts, payload_status);
            this->OnCommittedUpdate(cce, was_dirty);
            // It's possible that the cce HasBufferedCommandList and is still in
            // unknown status (because FetchRecord fails) and this command
            // ignores kv value. Need to clear the buffered commands when a new
            // txn commits on the cce.
            cce->BufferedCommandList().Clear();

            if (last_dirty_commit_ts_ < commit_ts)
            {
                last_dirty_commit_ts_ = commit_ts;
            }

            if (commit_ts > ccp->last_dirty_commit_ts_)
            {
                ccp->last_dirty_commit_ts_ = commit_ts;
            }

            if (ccp->smallest_ttl_ != 0)
            {
                if (payload_status == RecordStatus::Normal)
                {
                    if (cce->payload_.cur_payload_ &&
                        cce->payload_.cur_payload_->HasTTL() &&
                        ccp->smallest_ttl_ >
                            cce->payload_.cur_payload_->GetTTL())
                    {
                        ccp->smallest_ttl_ =
                            cce->payload_.cur_payload_->GetTTL();
                    }
                }
                else
                {
                    assert(cce->PayloadStatus() == RecordStatus::Deleted);
                    ccp->smallest_ttl_ = 0;
                }
            }
        }
        else if (forward_entry)
        {
            // tx aborts, release forward entry (will be automatically freed)
            cce->ReleaseForwardEntry();
        }

        // Reset the dirty status.
        cce->SetDirtyPayload(nullptr);
        cce->SetDirtyPayloadStatus(RecordStatus::NonExistent);
        cce->SetPendingCmd(nullptr);

        if (s_obj_exist && cce->PayloadStatus() != RecordStatus::Normal)
        {
            TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_--;
        }
        else if (!s_obj_exist && cce->PayloadStatus() == RecordStatus::Normal)
        {
            TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_++;
        }

        ReleaseCceLock(lk,
                       cce,
                       txn,
                       req.NodeGroupId(),
                       LockType::WriteLock,
                       true,
                       cce->payload_.cur_payload_.get());

        if (shard_->GetCacheEvictPolicy() == CacheEvictPolicy::LO_LRU)
        {
            EnsureLargeObjOccupyPageAlone(ccp, cce);
        }

        if (cce->PayloadStatus() == RecordStatus::Unknown && cce->IsFree())
        {
            // If the finished cmd ignores kv value and the tx aborts, we will
            // end up with a cce with unknown status after dirty payload is
            // cleared. Remove the unused cce.
            CleanEntry(cce, ccp);
        }
        req.Result()->SetFinished();
        return true;
    }

    bool Execute(UploadBatchCc &req) override
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"template_cc_map\"")
                    .append(",\"term\":")
                    .append(std::to_string(req.CcNgTerm()));
            });
        TX_TRACE_DUMP(&req);

        if (!shard_->IsBucketsMigrating())
        {
            return req.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }
        auto entry_tuples = req.EntryTuple();
        size_t batch_size = req.BatchSize();

        const KeyT *key = nullptr;
        KeyT decoded_key;
        ValueT decoded_rec;
        TxRecord::Uptr object_uptr = nullptr;
        uint64_t commit_ts = 0;
        RecordStatus rec_status = RecordStatus::Normal;

        // object cc map only handles remote upload batch cc reqeust for now.
        auto &resume_pos = req.GetPausedPosition(shard_->core_id_);
        size_t key_pos = std::get<0>(resume_pos);
        size_t key_offset = std::get<1>(resume_pos);
        size_t rec_offset = std::get<2>(resume_pos);
        size_t ts_offset = std::get<3>(resume_pos);
        size_t status_offset = std::get<4>(resume_pos);
        size_t hash = 0;

        CcEntry<KeyT, ValueT, false, false> *cce;
        CcPage<KeyT, ValueT, false, false> *cc_page = nullptr;
        size_t next_key_offset = 0;
        size_t next_rec_offset = 0;
        size_t next_ts_offset = 0;
        size_t next_status_offset = 0;
        for (size_t cnt = 0;
             key_pos < batch_size && cnt < UploadBatchCc::UploadBatchBatchSize;
             ++key_pos, ++cnt)
        {
            next_key_offset = key_offset;
            next_rec_offset = rec_offset;
            next_ts_offset = ts_offset;
            next_status_offset = status_offset;

            auto [key_str, rec_str, ts_str, status_str, flags_str] =
                *entry_tuples;
            // deserialize key
            decoded_key.Deserialize(
                key_str.data(), next_key_offset, KeySchema());
            key = &decoded_key;
            // deserialize record status
            rec_status =
                *((RecordStatus *) (status_str.data() + next_status_offset));
            next_status_offset += sizeof(RecordStatus);
            if (rec_status == RecordStatus::Normal)
            {
                // deserialize rec
                object_uptr = decoded_rec.DeserializeObject(rec_str.data(),
                                                            next_rec_offset);
            }

            // deserialize commit ts
            commit_ts = *((uint64_t *) (ts_str.data() + next_ts_offset));
            next_ts_offset += sizeof(uint64_t);

            hash = key->Hash();
            uint16_t bucket_id = Sharder::MapKeyHashToBucketId(hash);
            size_t core_idx = (hash & 0x3FF) % shard_->core_cnt_;
            if (!(core_idx == shard_->core_id_) || commit_ts <= 1 ||
                !shard_->GetBucketInfo(bucket_id, cc_ng_id_)
                     ->AcceptsUploadBatch())
            {
                // Skip the key if
                // 1) key does not land on this core
                // 2) commit ts is invalid
                // 3) bucket stops accepting upload batch reqeust
                // Move to next key.
                key_offset = next_key_offset;
                rec_offset = next_rec_offset;
                ts_offset = next_ts_offset;
                status_offset = next_status_offset;
                continue;
            }

            auto it = FindEmplace(*key);
            cce = it->second;
            cc_page = it.GetPage();
            if (cce == nullptr)
            {
                DLOG(WARNING) << "!!!WARNING!!! UploadBatchCc OOM on core: "
                              << shard_->core_id_ << ". Txn: " << req.Txn()
                              << ", table name: " << this->table_name_.Trace();
                // This cc shard has reached max memory limit. Currently upload
                // batch for object cc map is only used for sending cache to new
                // data owner during migration. This is a best effort try and
                // does not need to be successful. Just return immediately.
                return req.SetError(CcErrorCode::OUT_OF_MEMORY);
            }

            assert(commit_ts > 1);
            if (cce->CommitTs() >= commit_ts)
            {
                // Concurrent upsert_tx has write the latest value, so discard
                // the old value directly. For example, during add index
                // transaction, we will write the packed sk data that generate
                // from old pk records into the new sk ccmap, and before this
                // post write request, we do not acquire the write lock on this
                // TxKey, so this value has been updated by a concurrent
                // transaction.
                key_offset = next_key_offset;
                rec_offset = next_rec_offset;
                ts_offset = next_ts_offset;
                status_offset = next_status_offset;
                continue;
            }

            uint64_t ttl = UINT64_MAX;
            if (rec_status == RecordStatus::Normal)
            {
                if (cce->PayloadStatus() != RecordStatus::Normal)
                {
                    TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_++;
                }
                if (object_uptr->HasTTL())
                {
                    ttl = object_uptr->GetTTL();
                }
                ApplyPayloadSwapRule(cce);
                cce->payload_.PassInCurrentPayload(std::move(object_uptr));
                object_uptr = nullptr;
            }
            else
            {
                if (cce->PayloadStatus() == RecordStatus::Normal)
                {
                    TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_--;
                }
                ApplyPayloadSwapRule(cce);
                cce->payload_.SetCurrentPayload(nullptr);
                ttl = 0;
            }

            bool was_dirty = cce->IsDirty();
            cce->SetCommitTsPayloadStatus(commit_ts, rec_status);
            if (req.Kind() == UploadBatchType::DirtyBucketData)
            {
                cce->SetCkptTs(commit_ts);
            }

            if (cce->HasBufferedCommandList())
            {
                BufferedTxnCmdList &buffered_cmd_list =
                    cce->BufferedCommandList();
                auto &cmd_list = buffered_cmd_list.txn_cmd_list_;
                int64_t buffered_cmd_cnt_old = buffered_cmd_list.Size();
                // Clear cmds with smaller commit_ts than uploaded version.
                auto it = cmd_list.begin();
                while (it != cmd_list.end() && it->new_version_ <= commit_ts)
                {
                    ++it;
                }
                cmd_list.erase(cmd_list.begin(), it);

                bool drained = cce->TryCommitBufferedCommands(
                    shard_, commit_ts, shard_->NowInMilliseconds());
                int64_t buffered_cmd_cnt_new = buffered_cmd_list.Size();
                shard_->UpdateBufferedCommandCnt(buffered_cmd_cnt_new -
                                                 buffered_cmd_cnt_old);
                // A false return means a paged CommitOn stalled on a page
                // that is not resident. Fetch it and re-drive when it lands;
                // until then the commands stay buffered and the version stops
                // where the drain stopped (docs/08 §10).
                if (!drained)
                {
                    IssueDrainFetches(cce, DrainFetchTerm());
                }
            }

            cce->SetCommitTsPayloadStatus(commit_ts,
                                          cce->DrainedPayloadStatus());
            // Since we have updated both ckpt ts and commit ts, we need to call
            // OnFlushed to update the dirty size.
            this->OnFlushed(cce, was_dirty);
            this->OnCommittedUpdate(cce, was_dirty);
            DLOG_IF(INFO, TRACE_OCC_ERR)
                << "UploadBatchCc, txn:" << req.Txn() << " ,cce: " << cce
                << " ,commit_ts: " << commit_ts;

            if (commit_ts > last_dirty_commit_ts_)
            {
                last_dirty_commit_ts_ = commit_ts;
            }
            if (commit_ts > cc_page->last_dirty_commit_ts_)
            {
                cc_page->last_dirty_commit_ts_ = commit_ts;
            }
            if (ttl < cc_page->smallest_ttl_)
            {
                cc_page->smallest_ttl_ = ttl;
            }

            // update the key offset
            key_offset = next_key_offset;
            rec_offset = next_rec_offset;
            ts_offset = next_ts_offset;
            status_offset = next_status_offset;
        }
        if (key_pos < batch_size)
        {
            // Only insert UploadBatchBatchSize keys in one round.  set the
            // paused key to mark resume position and put the request into cc
            // queue again.
            req.SetPausedPosition(shard_->core_id_,
                                  key_pos,
                                  key_offset,
                                  rec_offset,
                                  ts_offset,
                                  status_offset,
                                  0);
            shard_->Enqueue(shard_->LocalCoreId(), &req);
            return false;
        }

        return req.SetFinish();
    }

    bool Execute(UploadTxCommandsCc &req) override
    {
        TxNumber txn = req.Txn();
        uint64_t obj_version = req.ObjectVersion();
        uint64_t commit_ts = req.CommitTs();
        bool has_overwrite = req.HasOverWrite();
        const std::vector<std::string> *cmd_str_list = req.CommandList();

        const CcEntryAddr *cce_addr = req.CceAddr();

        CcEntry<KeyT, ValueT, false, false> *cce =
            reinterpret_cast<CcEntry<KeyT, ValueT, false, false> *>(
                cce_addr->ExtractCce());

        // check that this txn is lock owner
        NonBlockingLock *lk = cce->GetKeyLock();
        if (lk == nullptr || !lk->HasWriteLock() || lk->WriteLockTx() != txn)
        {
            assert(false);
            req.Result()->SetFinished();
            return true;
        }

        // Discard cmds that applies on an older version
        if (commit_ts > 0 && cce->CommitTs() <= obj_version)
        {
            CcPage<KeyT, ValueT, false, false> *ccp =
                static_cast<CcPage<KeyT, ValueT, false, false> *>(
                    cce->GetCcPage());

            std::vector<std::unique_ptr<TxCommand>> cmd_list;
            cmd_list.reserve(cmd_str_list->size());
            for (const std::string &cmd_str : *cmd_str_list)
            {
                std::unique_ptr<TxCommand> tx_cmd = CreateTxCommand(cmd_str);
                cmd_list.emplace_back(std::move(tx_cmd));
            }

            TxnCmd txn_cmd(obj_version,
                           commit_ts,
                           has_overwrite,
                           UINT64_MAX,
                           std::move(cmd_list));

            BufferedTxnCmdList &buffered_cmd_list = cce->BufferedCommandList();

            // Emplace txn_cmd and try to commit all pending commands.
            RecordStatus payload_status = cce->PayloadStatus();
            bool s_obj_exist = (payload_status == RecordStatus::Normal);

            assert(txn_cmd.new_version_ > cce->CommitTs());
            int64_t buffered_cmd_cnt_old = buffered_cmd_list.Size();
            bool was_dirty = cce->IsDirty();
            cce->EmplaceAndCommitBufferedTxnCommand(
                shard_, txn_cmd, shard_->NowInMilliseconds());
            this->OnCommittedUpdate(cce, was_dirty);
            int64_t buffered_cmd_cnt_new = buffered_cmd_list.Size();
            shard_->UpdateBufferedCommandCnt(buffered_cmd_cnt_new -
                                             buffered_cmd_cnt_old);
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
            if (!buffered_cmd_list.Empty())
            {
                const KeyT *key_ptr = ccp->KeyOfEntry(cce);
                int32_t part_id =
                    Sharder::MapKeyHashToHashPartitionId(key_ptr->Hash());
                int64_t ng_term = Sharder::Instance().StandbyNodeTerm();
                shard_->FetchRecord(this->table_name_,
                                    this->GetTableSchema(),
                                    TxKey(key_ptr),
                                    cce,
                                    this->cc_ng_id_,
                                    ng_term,
                                    nullptr,
                                    part_id);
            }
#endif
            // update payload status
            payload_status = cce->PayloadStatus();
            if (s_obj_exist && payload_status != RecordStatus::Normal)
            {
                TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_--;
            }
            else if (!s_obj_exist && payload_status == RecordStatus::Normal)
            {
                TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_++;
            }

            // if replay_cmd_list is null, key_lock_extra_data will be recycled
            // when release lock.

            // Must update dirty_commit_ts. Otherwise, this entry may be
            // skipped by checkpointer.
            if (commit_ts > last_dirty_commit_ts_)
            {
                last_dirty_commit_ts_ = commit_ts;
            }
            if (commit_ts > last_dirty_commit_ts_)
            {
                last_dirty_commit_ts_ = commit_ts;
            }
            if (commit_ts > ccp->last_dirty_commit_ts_)
            {
                ccp->last_dirty_commit_ts_ = commit_ts;
            }
            if (ccp->smallest_ttl_ != 0)
            {
                if (payload_status == RecordStatus::Normal)
                {
                    if (cce->payload_.cur_payload_ &&
                        cce->payload_.cur_payload_->HasTTL())
                    {
                        ccp->smallest_ttl_ =
                            std::min(cce->payload_.cur_payload_->GetTTL(),
                                     ccp->smallest_ttl_);
                    }
                }
                else if (payload_status == RecordStatus::Deleted)
                {
                    ccp->smallest_ttl_ = 0;
                }
            }

            if (shard_->GetCacheEvictPolicy() == CacheEvictPolicy::LO_LRU)
            {
                EnsureLargeObjOccupyPageAlone(ccp, cce);
            }
        }

        ReleaseCceLock(lk, cce, txn, req.NodeGroupId(), LockType::WriteLock);
        req.Result()->SetFinished();
        return true;
    }

    bool Execute(KeyObjectStandbyForwardCc &req) override
    {
        uint64_t schema_version = req.SchemaVersion();
        if (schema_version < schema_ts_)
        {
            // Discard message since it expired.
            return req.SetFinish(*shard_);
        }
        else if (schema_version > schema_ts_)
        {
            // Wait for DDL operation clearring this ccm.
            shard_->EnqueueWaitListIfSchemaMismatch(&req);
            return false;
        }

        uint64_t obj_version = req.ObjectVersion();
        uint64_t commit_ts = req.CommitTs();
        bool has_overwrite = req.HasOverWrite();
        const std::vector<std::string_view> *cmd_str_list = req.CommandList();
        assert(commit_ts > 0);

        CcEntry<KeyT, ValueT, false, false> *cce = nullptr;
        CcPage<KeyT, ValueT, false, false> *ccp = nullptr;
        KeyT decoded_key;
        const std::string *key_str = req.KeyImage();
        assert(key_str != nullptr);
        size_t offset = 0;
        decoded_key.Deserialize(key_str->data(), offset, KeySchema());
        const KeyT *look_key = &decoded_key;

        // In skip_kv mode there is no data store handler and nothing is
        // checkpointed to shared storage, so the forward message must be
        // applied.
        if (!txservice_skip_kv && Sharder::Instance().StandbyNodeTerm() >= 0 &&
            Sharder::Instance().GetDataStoreHandler()->IsSharedStorage() &&
            commit_ts < Sharder::Instance().NativeNodeGroupCkptTs())
        {
            auto it = Find(*look_key);
            if (it == End())
            {
                // Discard the forward message since it has already been
                // checkpointed. And the checkpointed data will be fetched when
                // a forward message with bigger commit_ts than ckpt_ts is
                // received.
                return req.SetFinish(*shard_);
            }
        }

        // first time the request is processed
        auto it = FindEmplace(*look_key);
        cce = it->second;
        if (cce == nullptr)
        {
            shard_->EnqueueWaitListIfMemoryFull(&req);
            return false;
        }
        assert(cce);
        ccp = it.GetPage();

        const int32_t part_id =
            Sharder::MapKeyHashToHashPartitionId(look_key->Hash());
        // Loads the payload asynchronously. Passes null as the requester cc
        // since the commands are buffered in the cce's buffered command list,
        // so there is no need to put this req back in the queue after the
        // record is fetched.
        auto fetch_record = [&]()
        {
            shard_->FetchRecord(table_name_,
                                table_schema_,
                                TxKey(look_key),
                                cce,
                                cc_ng_id_,
                                req.StandbyNodeTerm(),
                                nullptr,
                                part_id);
        };

        if (commit_ts <= cce->CommitTs())
        {
            // Discard message since cce has a newer version.
            return req.SetFinish(*shard_);
        }
        else
        {
            if (cce->PayloadStatus() == RecordStatus::Unknown)
            {
                if (!has_overwrite && obj_version != 1 &&
                    !ccm_has_full_entries_)
                {
                    if (Sharder::Instance().StandbyNodeTerm() > 0)
                    {
                        // Cannot find a cached version in memory. Fetch
                        // it from kv store if kv is synced with primary.
                        cce->GetOrCreateKeyLock(shard_, this, ccp);
                        fetch_record();
                    }
                }
                else
                {
                    // ver == 1 means this key does not exist on primary node.
                    assert(cce->PayloadStatus() == RecordStatus::Unknown ||
                           cce->CommitTs() == 1);
                    cce->SetCommitTsPayloadStatus(1, RecordStatus::Deleted);
                }
            }
            bool s_obj_exist = (cce->PayloadStatus() == RecordStatus::Normal);

            // A PAGED payload never takes the direct-apply fast path here
            // (docs/08 §6, phase 6b). Per #509 a standby-forwarded command
            // reaches CommitOn without ever running ExecuteOn, so no page is
            // pinned and CommitOn can fault partway through the batch -- and a
            // command that has already run cannot be taken back. The choice
            // therefore has to be made BEFORE the first command is applied,
            // which rules out reacting to a fault once one happens.
            //
            // The buffered branch below handles the fault properly: the drain
            // stops on the missing page, the caller issues the fetches, and the
            // completion re-drives it. Sending every paged command that way
            // costs the fast path for large objects, which is the right trade
            // against applying half a batch and having no way to undo it.
            // Deliberately conservative: it diverts whether or not this
            // particular batch would have faulted, because that is not knowable
            // without running the commands.
            TxObject *fast_path_payload = cce->payload_.cur_payload_.get();
            bool paged_payload = fast_path_payload != nullptr &&
                                 fast_path_payload->AsPaged() != nullptr;
            if ((obj_version == cce->CommitTs() || has_overwrite) &&
                !cce->HasBufferedCommandList() && !paged_payload)
            {
                // directly apply the command
                for (const std::string_view &cmd_str : *cmd_str_list)
                {
                    std::unique_ptr<TxCommand> tx_cmd =
                        CreateTxCommand(cmd_str);
                    if (cce->payload_.cur_payload_ == nullptr)
                    {
                        std::unique_ptr<TxRecord> obj_ptr =
                            tx_cmd->CreateObject(nullptr);
                        cce->payload_.PassInCurrentPayload(std::move(obj_ptr));
                    }
                    TxObject *obj_ptr = cce->payload_.cur_payload_.get();
                    // Reaching here means the payload is NOT paged (see the
                    // divert above), so CommitOn cannot fault and applying the
                    // batch in place is safe.
                    TxObject *new_obj_ptr = tx_cmd->CommitOn(obj_ptr);
                    if (new_obj_ptr != obj_ptr)
                    {
                        // FIXME(lzx): should we use "new_obj_ptr->Clone()" ?
                        std::unique_ptr<TxRecord> new_obj_ptr_uptr;
                        new_obj_ptr_uptr.reset(
                            static_cast<TxRecord *>(new_obj_ptr));
                        cce->payload_.PassInCurrentPayload(
                            std::move(new_obj_ptr_uptr));
                    }
                }
                RecordStatus payload_status =
                    cce->payload_.cur_payload_ == nullptr
                        ? RecordStatus::Deleted
                        : RecordStatus::Normal;
                bool was_dirty = cce->IsDirty();
                cce->SetCommitTsPayloadStatus(commit_ts, payload_status);
                this->OnCommittedUpdate(cce, was_dirty);
                if (s_obj_exist && payload_status != RecordStatus::Normal)
                {
                    TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_--;
                }
                else if (!s_obj_exist && payload_status == RecordStatus::Normal)
                {
                    TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_++;
                }
            }
            else
            {
                // Emplace the cmds as buffered cmds and try to commit them.
                cce->GetOrCreateKeyLock(shard_, this, ccp);
                std::vector<std::unique_ptr<TxCommand>> cmd_list;
                cmd_list.reserve(cmd_str_list->size());
                for (const std::string_view &cmd_str : *cmd_str_list)
                {
                    std::unique_ptr<TxCommand> tx_cmd =
                        CreateTxCommand(cmd_str);
                    cmd_list.emplace_back(std::move(tx_cmd));
                }

                TxnCmd txn_cmd(obj_version,
                               commit_ts,
                               has_overwrite,
                               UINT64_MAX,
                               std::move(cmd_list));

                BufferedTxnCmdList &buffered_cmd_list =
                    cce->BufferedCommandList();

                // Emplace txn_cmd and try to commit all pending commands.
                int64_t buffered_cmd_cnt_old = buffered_cmd_list.Size();
                bool was_dirty = cce->IsDirty();
                bool standby_drained = cce->EmplaceAndCommitBufferedTxnCommand(
                    shard_, txn_cmd, shard_->NowInMilliseconds());
                if (!standby_drained)
                {
                    // A paged CommitOn stopped on a page that is not resident.
                    // Nothing else on the standby path issues this fetch, so
                    // without it the fault is detected and then dropped and the
                    // commands stay buffered forever -- the same defect that
                    // stalled log replay until the ReplayLogCc sites were
                    // given this call.
                    IssueDrainFetches(cce, DrainFetchTerm());
                }
                this->OnCommittedUpdate(cce, was_dirty);
                RecordStatus new_status = cce->PayloadStatus();
                if (s_obj_exist && new_status != RecordStatus::Normal)
                {
                    TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_--;
                }
                else if (!s_obj_exist && new_status == RecordStatus::Normal)
                {
                    TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_++;
                }
                int64_t buffered_cmd_cnt_new = buffered_cmd_list.Size();
                shard_->UpdateBufferedCommandCnt(buffered_cmd_cnt_new -
                                                 buffered_cmd_cnt_old);
                // Resubscribe to the leader if standby node has fallen behind
                // too much.
                shard_->CheckLagAndResubscribe();

                if (buffered_cmd_list.Empty())
                {
                    // Recycles the lock if this and prior commands have been
                    // applied and there is no pending command.
                    cce->RecycleKeyLock(*shard_);
                }
                else
                {
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
                    fetch_record();
#endif
                }
            }
        }

        // Must update dirty_commit_ts. Otherwise, this entry may be
        // skipped by checkpointer.
        // Update dirty_commit_ts with the req.CommitTs().
        commit_ts = std::max(commit_ts, cce->CommitTs());
        if (commit_ts > last_dirty_commit_ts_)
        {
            last_dirty_commit_ts_ = commit_ts;
        }
        assert(ccp != nullptr);
        if (commit_ts > ccp->last_dirty_commit_ts_)
        {
            ccp->last_dirty_commit_ts_ = commit_ts;
        }

        if (ccp->smallest_ttl_ != 0)
        {
            if (cce->PayloadStatus() == RecordStatus::Normal)
            {
                if (cce->payload_.cur_payload_ &&
                    cce->payload_.cur_payload_->HasTTL() &&
                    ccp->smallest_ttl_ > cce->payload_.cur_payload_->GetTTL())
                {
                    ccp->smallest_ttl_ = cce->payload_.cur_payload_->GetTTL();
                }
            }
            else
            {
                ccp->smallest_ttl_ = 0;
            }
        }

        if (shard_->GetCacheEvictPolicy() == CacheEvictPolicy::LO_LRU)
        {
            EnsureLargeObjOccupyPageAlone(ccp, cce);
        }

        return req.SetFinish(*shard_);
    }

    bool Execute(RestoreCcMapCc &req) override
    {
        uint16_t core_id = shard_->core_id_;
        if (req.data_item_decoded_[core_id] == 0)
        {
            size_t index = req.NextIndex(core_id);
            auto &slice_data = req.SliceData(core_id);
            for (size_t i = 0; i < FillStoreSliceCc::MaxScanBatchSize &&
                               index < slice_data.size();
                 i++)
            {
                RawSliceDataItem &data_item = slice_data[index];
                std::string key_str = std::move(data_item.key_str_);
                std::string val_str = std::move(data_item.rec_str_);
                std::unique_ptr<KeyT> key = std::make_unique<KeyT>();
                key->KVDeserialize(key_str.data(), key_str.size());
                // tx_key is owner now
                TxKey tx_key(std::move(key));
                ValueT val;
                size_t offset = 0;
                std::unique_ptr<TxRecord> rec =
                    val.DeserializeObject(val_str.data(), offset);
                if (data_item.is_deleted_ ||
                    (rec->HasTTL() &&
                     rec->GetTTL() < shard_->NowInMilliseconds()))
                {
                    // skip expired keys.
                    index++;
                    continue;
                }

                req.DecodedDataItem(core_id,
                                    std::move(tx_key),
                                    std::move(rec),
                                    data_item.version_ts_,
                                    data_item.is_deleted_);
                index++;
            }

            if (index < slice_data.size())
            {
                req.SetNextIndex(core_id, index);
            }
            else
            {
                req.data_item_decoded_[core_id] = 1;
                req.SetNextIndex(core_id, 0);
            }

            shard_->Enqueue(core_id, &req);
        }
        else
        {
            std::deque<SliceDataItem> &slice_vec =
                req.DecodedSliceData(core_id);

            size_t index = req.NextIndex(shard_->core_id_);
            size_t last_index = std::min(
                index + FillStoreSliceCc::MaxScanBatchSize, slice_vec.size());
            bool success =
                this->BatchFillSlice(slice_vec, true, index, last_index);
            req.total_cnt_ += last_index - index;

            if (!success)
            {
                // This check makes sure only one line of log printed
                if (req.cancel_data_loading_on_error_->load(
                        std::memory_order_relaxed) == CcErrorCode::NO_ERROR)
                {
                    int64_t alloc, commit;
                    CcShardHeap *shard_heap = shard_->GetShardHeap();
                    shard_heap->Full(&alloc, &commit);
                    LOG(ERROR) << "Restore Tx cache failed due to out of "
                                  "memory, core: "
                               << core_id << " allocated: " << alloc
                               << " ,committed: " << commit;
                }
                req.SetFinished(CcErrorCode::OUT_OF_MEMORY);
                return true;
            }

            index = last_index;
            if (index == slice_vec.size())
            {
                req.SetFinished();
            }
            else
            {
                req.SetNextIndex(shard_->core_id_, index);
                shard_->Enqueue(core_id, &req);
            }
        }
        return false;
    }

    bool Execute(ReplayLogCc &req) override
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"template_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append("0");
            });
        TX_TRACE_DUMP(&req);

        // If the log record's commit ts is smaller than that of the cc map,
        // this record is generated before the latest schema of the table
        // and hence should skip the replay process.
        uint64_t commit_ts = req.CommitTs();
        if (commit_ts < schema_ts_)
        {
            DLOG(INFO) << "discard log, commit_ts: " << commit_ts
                       << ", schema_ts: " << schema_ts_;
            req.SetFinish();
            return true;
        }

        KeyT key;
        size_t offset = req.Offset();
        const std::string_view &log_blob = req.LogContentView();
        uint16_t next_core = req.NextCore();
        req.SetNextCore(UINT16_MAX);

        while (offset < log_blob.size())
        {
            size_t prev_offset = offset;
            // the format of log_blob is: key_str, object_version, valid scope
            // (ttl), commands str length, commands str
            key.Deserialize(log_blob.data(), offset, KeySchema());
            const uint64_t obj_version =
                *reinterpret_cast<decltype(obj_version) *>(log_blob.data() +
                                                           offset);
            offset += sizeof(obj_version);
            const uint64_t valid_scope =
                *reinterpret_cast<const uint64_t *>(log_blob.data() + offset);
            offset += sizeof(valid_scope);
            const uint32_t cmds_len = *reinterpret_cast<decltype(cmds_len) *>(
                log_blob.data() + offset);
            offset += sizeof(cmds_len);

            // If key not belongs to current ng, skip it.
            uint64_t key_hash = key.Hash();
            uint16_t bucket_id =
                Sharder::Instance().MapKeyHashToBucketId(key_hash);
            const BucketInfo *bucket_info =
                shard_->GetBucketInfo(bucket_id, cc_ng_id_);
            if (bucket_info->BucketOwner() != cc_ng_id_ &&
                bucket_info->DirtyBucketOwner() != cc_ng_id_)
            {
                offset += cmds_len;
                continue;
            }

            uint16_t core_id = (key_hash & 0x3FF) % shard_->core_cnt_;
            if (core_id != shard_->core_id_)
            {
                // Skips the key in the log record that is not sharded to this
                // core.
                offset += cmds_len;
                if (shard_->core_id_ == req.FirstCore() ||
                    (core_id != req.FirstCore() && core_id > shard_->core_id_))
                {
                    // Move to the smallest unvisited core id
                    next_core = std::min(core_id, next_core);
                }
                continue;
            }

            auto it = FindEmplace(key);
            CcEntry<KeyT, ValueT, false, false> *cce = it->second;
            CcPage<KeyT, ValueT, false, false> *ccp = it.GetPage();

            // Loads the payload asynchronously. Passes null as the requester
            // cc since the commands are buffered in the cce's buffered
            // command list, so there is no need to put this req back in the
            // queue after the record is fetched.
            auto fetch_record = [&](int64_t ng_term)
            {
                shard_->FetchRecord(
                    table_name_,
                    table_schema_,
                    TxKey(&key),
                    cce,
                    cc_ng_id_,
                    ng_term,
                    nullptr,
                    Sharder::MapKeyHashToHashPartitionId(key_hash));
            };

            // For orphan lock recovery, verify if the transaction still holds
            // the lock on this CC entry.
            if (req.IsLockRecovery())
            {
                if (const NonBlockingLock *key_lock =
                        cce != nullptr ? cce->GetKeyLock() : nullptr;
                    key_lock == nullptr ||
                    !key_lock->HasWriteLockOrWriteIntent(req.Txn()))
                {
                    offset += cmds_len;
                    continue;
                }
            }

            if (cce == nullptr)
            {
                // The cc map has
                // reached the maximal capacity. Blocks the request by putting
                // it into wait list until capacity is avaliable.
                req.SetOffset(prev_offset);
                req.SetNextCore(next_core);
                shard_->EnqueueWaitListIfMemoryFull(&req);
                return false;
            }

            bool txn_expired = valid_scope < shard_->NowInMilliseconds();
            uint64_t current_version = cce->CommitTs();
            RecordStatus payload_status = cce->PayloadStatus();
            bool s_obj_exist = (payload_status == RecordStatus::Normal);
            bool was_dirty = cce->IsDirty();
            if (commit_ts <= current_version)
            {
                // If the log record's commit ts is smaller than or equal to the
                // current version, we can skip the replay process.
                offset += cmds_len;
                continue;
            }
            bool acquired_extra_data = false;
            BufferedTxnCmdList *buffered_cmd_list = nullptr;
            if (cce->GetKeyLock() == nullptr)
            {
                cce->GetOrCreateKeyLock(shard_, this, ccp);
                assert(cce->GetKeyLock() != nullptr);
                acquired_extra_data = true;
            }
            if (txn_expired)
            {
                offset += cmds_len;
                DLOG(INFO) << "replay log key: " << key.ToString()
                           << "txn expired, commit_ts: " << commit_ts
                           << ", valid scope: " << valid_scope;

                // Skip commands before this tx since they are already
                // expired.
                if (cce->HasBufferedCommandList())
                {
                    // Create a txn command. We do not care about the actual
                    // commands since they have already expired.
                    std::vector<std::unique_ptr<TxCommand>> cmd_list;
                    TxnCmd txn_cmd(1,  // we do not care about previous version
                                   commit_ts,
                                   true,
                                   valid_scope,
                                   std::move(cmd_list));
                    buffered_cmd_list = &cce->BufferedCommandList();
                    int64_t buffered_cmd_cnt_old = buffered_cmd_list->Size();
                    bool replay_drained =
                        cce->EmplaceAndCommitBufferedTxnCommand(
                            shard_, txn_cmd, shard_->NowInMilliseconds());
                    if (!replay_drained)
                    {
                        // A paged CommitOn stopped on a page that is not
                        // resident. Nothing else on the replay path will issue
                        // the fetch -- BackFill only runs on a record fetch,
                        // which does not recur once the record is loaded -- so
                        // it must happen here or the commands stay buffered
                        // forever.
                        IssueDrainFetches(cce, DrainFetchTerm());
                    }
                    int64_t buffered_cmd_cnt_new = buffered_cmd_list->Size();
                    shard_->UpdateBufferedCommandCnt(buffered_cmd_cnt_new -
                                                     buffered_cmd_cnt_old);
                }
                else
                {
                    // No buffered commands, directly set cce commit ts.
                    // Same as above: drop the paged payload only after
                    // orphaning its fetches and waking its parked readers.
                    ApplyPayloadSwapRule(cce);
                    cce->payload_.cur_payload_ = nullptr;
                    cce->SetCommitTsPayloadStatus(commit_ts,
                                                  RecordStatus::Deleted);
                }
            }
            else
            {
                bool ignore_previous_version =
                    *reinterpret_cast<const uint8_t *>(log_blob.data() +
                                                       offset);
                offset += sizeof(uint8_t);

                DLOG(INFO) << "replay log key: " << key.ToString()
                           << ", obj_ver: " << obj_version
                           << ", commit ts: " << commit_ts
                           << ", cmds len: " << cmds_len << ", cmds str: "
                           << std::string_view(log_blob.data() + offset,
                                               cmds_len)
                           << " has_overwrite: " << ignore_previous_version
                           << ", valid scope: " << valid_scope
                           << ", expired: " << txn_expired
                           << ", cce version: " << cce->CommitTs();

                // load payload from kvstore before committing pending
                // commands. If there's already read intent on cce, that
                // means a previous replay cc has already sent fetch record.
                if (!ignore_previous_version &&
                    cce->PayloadStatus() == RecordStatus::Unknown &&
                    (!cce->GetKeyLock() || cce->GetKeyLock()->IsEmpty()))
                {
                    int64_t cc_ng_candid_term =
                        Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
                    int64_t cc_ng_term =
                        Sharder::Instance().LeaderTerm(cc_ng_id_);
                    int64_t ng_term = std::max(cc_ng_candid_term, cc_ng_term);
                    if (ng_term < 0)
                    {
                        req.SetFinish();
                        return true;
                    }

                    // If kv is skipped then log should always be skipped
                    // too.
                    assert(!txservice_skip_kv);
                    // Create key lock and extra struct for the cce. Fetch
                    // record will pin the cce to prevent it from being
                    // recycled before fetch record returns.
                    cce->GetOrCreateKeyLock(shard_, this, ccp);
                    fetch_record(ng_term);
                }
                // extract command list
                const uint16_t cmd_cnt = *reinterpret_cast<decltype(cmd_cnt) *>(
                    log_blob.data() + offset);
                offset += sizeof(cmd_cnt);
                std::vector<std::unique_ptr<TxCommand>> cmd_list;
                for (size_t i = 0; i < cmd_cnt; i++)
                {
                    const uint32_t cmd_len =
                        *reinterpret_cast<decltype(cmd_len) *>(log_blob.data() +
                                                               offset);
                    offset += sizeof(cmd_len);
                    std::unique_ptr<TxCommand> tx_cmd = CreateTxCommand(
                        std::string_view(log_blob.data() + offset, cmd_len));
                    offset += cmd_len;
                    cmd_list.emplace_back(std::move(tx_cmd));
                }

                // Emplace txn_cmd and try to commit all pending commands.
                TxnCmd txn_cmd(obj_version,
                               commit_ts,
                               ignore_previous_version,
                               valid_scope,
                               std::move(cmd_list));

                buffered_cmd_list = &cce->BufferedCommandList();
                int64_t buffered_cmd_cnt_old = buffered_cmd_list->Size();
                bool replay_drained = cce->EmplaceAndCommitBufferedTxnCommand(
                    shard_, txn_cmd, shard_->NowInMilliseconds());
                if (!replay_drained)
                {
                    // A paged CommitOn stopped on a page that is not resident.
                    // Nothing else on the replay path issues this fetch --
                    // BackFill only runs on a record fetch, which does not
                    // recur once the record is loaded -- so without this the
                    // fault is detected and then dropped, and the drain spins
                    // forever while the buffered list grows.
                    IssueDrainFetches(cce, DrainFetchTerm());
                }
                int64_t buffered_cmd_cnt_new = buffered_cmd_list->Size();
                shard_->UpdateBufferedCommandCnt(buffered_cmd_cnt_new -
                                                 buffered_cmd_cnt_old);
            }

            if (buffered_cmd_list != nullptr && buffered_cmd_list->Empty())
            {
                // Recycles the lock if this and prior commands have been
                // applied and there is no pending command.
                bool lock_recycled = cce->RecycleKeyLock(*shard_);
                if (acquired_extra_data)
                {
                    // The lock is newly assigned, recycle must succeed.
                    assert(lock_recycled);
                }
                (void) lock_recycled;
            }
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
            else if (buffered_cmd_list != nullptr)
            {
                fetch_record(Sharder::Instance().StandbyNodeTerm());
            }
#endif

            payload_status = cce->PayloadStatus();

            if (s_obj_exist && payload_status != RecordStatus::Normal)
            {
                --TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_;
            }
            else if (!s_obj_exist && payload_status == RecordStatus::Normal)
            {
                ++TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_;
            }

            this->OnCommittedUpdate(cce, was_dirty);

            // Must update dirty_commit_ts. Otherwise, this entry may be
            // skipped by checkpointer.
            if (commit_ts > last_dirty_commit_ts_)
            {
                last_dirty_commit_ts_ = commit_ts;
            }
            if (commit_ts > ccp->last_dirty_commit_ts_)
            {
                ccp->last_dirty_commit_ts_ = commit_ts;
            }

            if (ccp->smallest_ttl_ != 0)
            {
                if (payload_status == RecordStatus::Normal)
                {
                    if (cce->payload_.cur_payload_ &&
                        cce->payload_.cur_payload_->HasTTL() &&
                        ccp->smallest_ttl_ >
                            cce->payload_.cur_payload_->GetTTL())
                    {
                        ccp->smallest_ttl_ =
                            cce->payload_.cur_payload_->GetTTL();
                    }
                }
                else if (payload_status == RecordStatus::Deleted)
                {
                    ccp->smallest_ttl_ = 0;
                }
            }

            NonBlockingLock *lk = cce->GetKeyLock();
            if (lk != nullptr && lk->HasWriteLock())
            {
                // If the record in the log has a commit ts greater than
                // that of the cc entry and the cc entry has a write
                // lock, the lock's owner must be the tx that commits
                // the log record.
                // TODO: it is safer if we ship the tx ID with the
                // recovering message and match it against the lock holder.

                // Reset the dirty status since the committed commands are
                // already committed on the object.
                cce->SetDirtyPayload(nullptr);
                cce->SetDirtyPayloadStatus(RecordStatus::NonExistent);
                cce->SetPendingCmd(nullptr);

                // Forward the update to standby node.
                if (!shard_->GetSubscribedStandbys().empty() &&
                    cce->ForwardEntry())
                {
                    auto forward_entry = cce->ForwardEntry();
                    forward_entry->Request().set_commit_ts(commit_ts);
                    forward_entry->Request().set_schema_version(schema_ts_);
                    std::unique_ptr<StandbyForwardEntry> entry_ptr =
                        cce->ReleaseForwardEntry();
                    shard_->ForwardStandbyMessage(entry_ptr.release());
                }

                TxNumber txn = lk->WriteLockTx();
                ReleaseCceLock(lk,
                               cce,
                               txn,
                               req.NodeGroupId(),
                               LockType::WriteLock,
                               true,
                               cce->payload_.cur_payload_.get());
            }

            if (shard_->GetCacheEvictPolicy() == CacheEvictPolicy::LO_LRU &&
                cce->PayloadStatus() != RecordStatus::Unknown)
            {
                // Skip when payload is not yet backfilled from KV store.
                // BackFill() will call EnsureLargeObjOccupyPageAlone()
                // after the record is fetched and buffered commands are
                // committed.
                EnsureLargeObjOccupyPageAlone(ccp, cce);
            }
        }

        if (next_core != UINT16_MAX)
        {
            req.ResetCcm();
            MoveRequest(&req, next_core);
            return false;
        }
        else
        {
            req.SetFinish();
            return true;
        }
    }

    /**
     * @brief The page mode of back-fill (eloqkv docs/08-paged-objects.md
     * §13): installs one fetched page into the current paged payload and
     * resolves the fetch's waiter txns. Invoked by PageFetch::Execute on the
     * shard core; the entry pin and hub bookkeeping stay with the caller.
     *
     * Deliberately unlike BackFill below: never touches the entry's commit
     * ts or record status (the metadata row owns those), and a missing store
     * row for a LIVE page id is corruption rather than
     * RecordStatus::Deleted — §4's id lifecycle guarantees every live id a
     * checkpoint flushed has a row.
     */
    /**
     * @brief Applies the §7 swap rule to the payload about to be superseded:
     * orphan its in-flight page fetches (flagged discard-on-complete, which
     * also carries the incarnation boundary — a fetch for one incarnation's
     * page id must never install into a successor's), erase its fault
     * contexts, and eagerly re-enqueue whatever was parked on a fetch.
     *
     * Must be called BEFORE the new payload is installed, while the old one
     * is still reachable. A no-op unless the current payload is paged, so
     * every monolithic install path is unaffected.
     */
    /**
     * @brief Assigns the commit ts to a just-committed paged payload's dirty
     * pages (docs/08 §4). No-op for monolithic objects.
     */
    void StampPagedWrites(CcEntry<KeyT, ValueT, false, false> *cce,
                          uint64_t commit_ts)
    {
        if (cce->payload_.cur_payload_ == nullptr)
        {
            return;
        }
        PagedTxObject *paged = cce->payload_.cur_payload_->AsPaged();
        if (paged != nullptr)
        {
            paged->StampWrites(commit_ts);
        }
    }

    /**
     * @brief Runs the §7 rules that must precede ANY replacement or removal of
     * a committed paged payload — the dirty-for-committed swap at commit, and
     * the TTL-expiry retire paths that null the payload outright.
     *
     * Two things must happen while the OLD object is still installed: its
     * in-flight page fetches are orphaned (their completions then discard
     * instead of installing into a successor), and every request parked on one
     * of its pages is re-enqueued, because those parked pointers live in the
     * object's tx_contexts_ and would otherwise be destroyed with it, leaving
     * the commands waiting forever. Per-page pin counts need no unwinding:
     * they belong to the superseded object's slots and die with them.
     */
    void ApplyPayloadSwapRule(CcEntry<KeyT, ValueT, false, false> *cce)
    {
        if (cce->payload_.cur_payload_ == nullptr)
        {
            return;
        }
        PagedTxObject *paged = cce->payload_.cur_payload_->AsPaged();
        if (paged == nullptr)
        {
            return;
        }

        KeyGapLockAndExtraData *lke = cce->GetKeyGapLockAndExtraData();
        if (lke != nullptr)
        {
            FetchHub *hub = lke->FetchHubPtr();
            if (hub != nullptr)
            {
                hub->SpliceAllToOrphans();
            }
        }

        // Wake everyone parked on this payload's pages. The two halves of the
        // §7 rule live on different owners by design: the wake records are on
        // the ENTRY (FetchHub), so they survive the payload being replaced,
        // while the per-page state (pins) is in the payload and dies with it
        // via AbandonAllTxContexts.
        std::vector<CcRequestBase *> parked;
        if (lke != nullptr)
        {
            FetchHub *hub = lke->FetchHubPtr();
            if (hub != nullptr)
            {
                parked = hub->TakeAllParked();
            }
        }
        paged->AbandonAllTxContexts();
        for (CcRequestBase *req : parked)
        {
            shard_->Enqueue(shard_->LocalCoreId(), req);
        }
    }

    /**
     * @brief Clears everything a page-fault park left on this entry for
     * `tx_number` (docs/08 §6): the wake record, the entry pin the park took,
     * and the transaction's page pins.
     */
    void ReleasePageReservation(LruEntry *entry, uint32_t page_id) override
    {
        auto *typed = static_cast<CcEntry<KeyT, ValueT, false, false> *>(entry);
        TxObject *obj = typed->payload_.cur_payload_.get();
        PagedTxObject *paged = obj != nullptr ? obj->AsPaged() : nullptr;
        if (paged != nullptr)
        {
            paged->DropPageReservation(page_id);
        }
        KeyGapLockAndExtraData *lke = typed->GetKeyGapLockAndExtraData();
        if (lke != nullptr)
        {
            TxObject *dirty_obj = lke->PeekDirtyPayload();
            PagedTxObject *dirty_paged =
                dirty_obj != nullptr ? dirty_obj->AsPaged() : nullptr;
            if (dirty_paged != nullptr)
            {
                dirty_paged->DropPageReservation(page_id);
            }
        }
    }

    void ClearPageFaultParking(LruEntry *entry, TxNumber tx_number) override
    {
        auto *typed = static_cast<CcEntry<KeyT, ValueT, false, false> *>(entry);
        KeyGapLockAndExtraData *lke = typed->GetKeyGapLockAndExtraData();
        if (lke == nullptr)
        {
            return;
        }
        FetchHub *hub = lke->FetchHubPtr();
        if (hub != nullptr)
        {
            // The request is about to go back to its pool; a completion must
            // not resolve this txn to it afterwards.
            hub->ForgetWaiter(tx_number);
        }
        ReleaseTxPagePins(entry, tx_number);
        // The pin taken when the command parked. Its counterpart in the normal
        // flow is the release in the BlockOnPageFault resume path.
        lke->ReleasePin();
        typed->RecycleKeyLock(*shard_);
    }

    void ReleaseTxPagePins(LruEntry *cce, TxNumber tx_number) const override
    {
        auto *typed = static_cast<CcEntry<KeyT, ValueT, false, false> *>(cce);

        // BOTH payloads. A multi-command transaction can leave pins in the
        // committed object (from a command that ran before the dirty object
        // existed) and in the dirty object (from every command after). Pins
        // are per (object, txn), so releasing only the committed side strands
        // the dirty side's pins and those pages never become evictable.
        if (typed->payload_.cur_payload_ != nullptr)
        {
            PagedTxObject *paged = typed->payload_.cur_payload_->AsPaged();
            if (paged != nullptr)
            {
                paged->ReleaseTxPins(tx_number);
            }
        }
        KeyGapLockAndExtraData *lke = typed->GetKeyGapLockAndExtraData();
        if (lke != nullptr)
        {
            TxObject *dirty_obj = lke->PeekDirtyPayload();
            PagedTxObject *dirty_paged =
                dirty_obj != nullptr ? dirty_obj->AsPaged() : nullptr;
            if (dirty_paged != nullptr)
            {
                dirty_paged->ReleaseTxPins(tx_number);
            }
        }
    }

    /**
     * @brief Partial eviction of a paged large object (docs/08 §8): shed a
     * fraction of its clean, unpinned pages instead of freeing the whole entry.
     *
     * Unconditional -- every visit sheds, with no pressure test of its own.
     * Reaching a paged object in the LRU sweep is already the signal: entries
     * are chained by recency, so being visited means the object is not being
     * accessed frequently, and it is big, so taking 10 % of it is worthwhile.
     * If that relieves the pressure, the sweep stops coming. If the same object
     * is revisited, that says both that it is still cold and that pressure is
     * high enough to keep triggering cleans -- so it keeps shrinking, and once
     * no page is left resident the ordinary whole-entry path reclaims the
     * metadata.
     *
     * @return true if pages were shed and the entry must survive this pass.
     */
    bool ShedPagesForEviction(LruEntry *cce) override
    {
        auto *typed = static_cast<CcEntry<KeyT, ValueT, false, false> *>(cce);

        // Shed only from the committed payload. A dirty payload belongs to an
        // in-flight transaction: it is not durable, so by the per-page rule
        // nothing in it is shed-able anyway.
        if (typed->payload_.cur_payload_ == nullptr ||
            typed->PayloadStatus() != RecordStatus::Normal)
        {
            return false;
        }
        PagedTxObject *paged = typed->payload_.cur_payload_->AsPaged();
        if (paged == nullptr)
        {
            return false;
        }

        // Test hook: strip every evictable page and KEEP the entry, parking the
        // object in the "metadata resident, pages shed" state. That state is
        // reachable in production but not on demand, and it is the precondition
        // for a read to page-fault — without it a cold object is evicted whole
        // and the read becomes a record fetch instead, which is why the §7
        // swap-with-fetch-in-flight tests could not reach their own path.
        // Compiled out unless WITH_FAULT_INJECT.
        CODE_FAULT_INJECTOR("shed_all_pages", {
            size_t shed = 0;
            size_t n;
            while ((n = paged->ShedCleanPages()) > 0)
            {
                shed += n;
            }
            LOG_IF(INFO, shed > 0)
                << "FAULTLOG shed_all_pages key=" << typed->KeyString()
                << " shed=" << shed;
            return true;
        });

        // Terminal state: metadata only. Fall through to the ordinary
        // whole-entry path, where the unchanged IsFree() gate applies.
        if (paged->ResidentPageCount() == 0)
        {
            return false;
        }

        return paged->ShedCleanPages() > 0;
    }

    /**
     * @brief Issues the page fetches a stalled buffered-command drain needs
     * and arranges for the drain to run again once they land (docs/08 §10).
     *
     * The drain has no transaction of its own — replay, standby apply and
     * migration all reach CommitOn without one — so it registers under the
     * reserved kDrainTxnNumber. That context owns the arriving pages' pins
     * exactly as a command's context would, which is what keeps the eviction
     * pass from shedding the pages back out before the re-drive reads them.
     *
     * @return True if at least one fetch is in flight for the drain, so a
     * completion will re-drive it. False if nothing could be issued — the
     * commands simply stay buffered, and the next drain attempt rediscovers
     * the same faults, so no state is stranded.
     */
    /**
     * @brief Is a buffered-command drain on this entry waiting for a page?
     *
     * A paged drain stops on a page that is not resident, issues the fetch and
     * leaves its commands buffered until the page lands. While that fetch is
     * outstanding, "commands still buffered" is a normal, transient state, not
     * the missing-version inconsistency the end-of-replay check exists for.
     *
     * @return True while the entry's fetch hub still owns any page fetch,
     * live or orphaned by a payload swap. Both count: an orphaned fetch still
     * completes and still resolves the drain, which is what re-drives it.
     */
    bool DrainFetchPending(CcEntry<KeyT, ValueT, false, false> *cce) const
    {
        KeyGapLockAndExtraData *lke = cce->GetKeyGapLockAndExtraData();
        if (lke == nullptr)
        {
            return false;
        }
        FetchHub *hub = lke->FetchHubPtr();
        return hub != nullptr && !hub->Empty();
    }

    /**
     * @brief The term a drain-issued page fetch must be stamped with.
     *
     * During log replay the node group's leader is only a CANDIDATE: the real
     * term is still -1 and the node is not yet serving, and it becomes real
     * only once replay finishes. Stamping a fetch with the bare LeaderTerm()
     * therefore records -1, while FetchRecordCc::ValidTermCheck at completion
     * compares against the MAXIMUM of the candidate, leader and standby terms
     * -- so the completion mismatches, is discarded, and the drain is never
     * woken. This mirrors that same maximum so issue and completion agree.
     *
     * @return The term to stamp, negative if this node group serves in no
     * capacity at all, in which case there is no point issuing a fetch.
     */
    int64_t DrainFetchTerm() const
    {
        return std::max(
            {Sharder::Instance().CandidateLeaderTerm(this->cc_ng_id_),
             Sharder::Instance().LeaderTerm(this->cc_ng_id_),
             Sharder::Instance().StandbyNodeTerm()});
    }

    bool IssueDrainFetches(CcEntry<KeyT, ValueT, false, false> *cce,
                           int64_t ng_term)
    {
        // Invariant maintained by this function: on return, the entry is
        // marked as blocking recovery IFF a page fetch is outstanding for it.
        // Every path that leaves nothing in flight must clear the mark, or the
        // promotion gate waits on an entry nothing will ever un-mark.
        TxObject *obj = cce->payload_.cur_payload_.get();
        PagedTxObject *paged = obj != nullptr ? obj->AsPaged() : nullptr;
        if (paged == nullptr)
        {
            // The payload is gone or is no longer paged; these buffered
            // commands are some other path's problem now.
            shard_->NoteDrainUnblocked(this->cc_ng_id_, cce);
            return false;
        }
        std::vector<uint32_t> faults;
        bool has_faults = paged->TakePendingFaults(faults);
        // Re-driving with nothing to fetch would stall on the same command
        // forever, so a stall must always name the page it is waiting for.
        assert(has_faults && "the drain stalled without recording a page");
        if (!has_faults)
        {
            shard_->NoteDrainUnblocked(this->cc_ng_id_, cce);
            return false;
        }

        CcPage<KeyT, ValueT, false, false> *ccp =
            static_cast<CcPage<KeyT, ValueT, false, false> *>(cce->GetCcPage());
        const KeyT *key = ccp->KeyOfEntry(cce);
        if (key == nullptr)
        {
            shard_->NoteDrainUnblocked(this->cc_ng_id_, cce);
            return false;
        }

        // The pin holds the entry, and the lock structure that owns the fetch
        // hub, alive until the re-drive releases it.
        cce->GetOrCreateKeyLock(shard_, this, ccp);
        cce->GetKeyGapLockAndExtraData()->AddPin();
        paged->EnsureTxFaultContext(kDrainTxnNumber);

        // Observability, and the precondition a replay test asserts on: a
        // crash-restart run that never prints this drained without ever
        // faulting, so it proved nothing about this path.
        DLOG(INFO) << "FAULTLOG drain_page_stall key: " << cce->KeyString()
                   << ", pages: " << faults.size() << ", ng_term: " << ng_term;

        size_t issued = 0;
        for (uint32_t page_id : faults)
        {
            auto res = shard_->FetchPage(
                this->table_name_,
                this->GetTableSchema(),
                TxKey(key),
                PageRowKind::HashPage,
                page_id,
                cce,
                this->cc_ng_id_,
                ng_term,
                kDrainTxnNumber,
                Sharder::MapKeyHashToHashPartitionId(key->Hash()));
            if (res == store::DataStoreHandler::DataStoreOpStatus::Retry)
            {
                // The store is busy. Stop issuing: the fetches already in
                // flight still resolve this drain, and the re-drive they
                // trigger recomputes the remaining fault set and asks again.
                // Progress is preserved without a second pin.
                break;
            }
            ++issued;
        }

        if (issued == 0)
        {
            // The store refused every page (busy). Nothing is in flight, yet
            // the buffered commands are still unapplied, so the mark STAYS:
            // promoting here would serve a key missing its replayed tail.
            // Nothing re-drives this on its own today, so the promotion
            // deadline is what breaks the tie, failing recovery loudly rather
            // than serving stale data. A retry nudge from the promotion poller
            // would be the better answer and is not built yet.
            LOG(WARNING) << "drain fetch could not be issued, recovery still "
                            "gated on key "
                         << cce->KeyString();
            cce->GetKeyGapLockAndExtraData()->ReleasePin();
            cce->RecycleKeyLock(*shard_);
            return false;
        }

        // Recovery must not declare itself finished while this drain is
        // waiting: promotion from the candidate term to the real term gates on
        // this count reaching zero, so that the node never serves a key whose
        // replayed tail has not been applied yet (docs/08 §10).
        shard_->NoteDrainBlocked(this->cc_ng_id_, cce, cce->KeyString());
        return true;
    }

    /**
     * @brief Runs the buffered-command drain again after the pages it stalled
     * on have arrived, and re-issues if it stalls on a further page.
     *
     * Called only from the fetch completion, once the reserved drain context
     * has no fetch outstanding.
     */
    void RedriveBufferedDrain(CcEntry<KeyT, ValueT, false, false> *cce)
    {
        // The candidate term counts here too: a re-drive can run while the
        // node group is still replaying, when LeaderTerm() alone is -1.
        int64_t ng_term = DrainFetchTerm();

        bool drained = true;
        if (cce->HasBufferedCommandList())
        {
            BufferedTxnCmdList &buffered_cmd_list = cce->BufferedCommandList();
            int64_t buffered_cmd_cnt_old = buffered_cmd_list.Size();
            uint64_t commit_version = cce->CommitTs();
            bool was_dirty = cce->IsDirty();

            drained = cce->TryCommitBufferedCommands(
                shard_, commit_version, shard_->NowInMilliseconds());

            shard_->UpdateBufferedCommandCnt(buffered_cmd_list.Size() -
                                             buffered_cmd_cnt_old);
            cce->SetCommitTsPayloadStatus(commit_version,
                                          cce->DrainedPayloadStatus());
            this->OnCommittedUpdate(cce, was_dirty);
        }

        // Release the stall's pin before deciding what comes next: a fresh
        // stall takes its own, so releasing after would leave the entry pinned
        // once per stall and never freed.
        KeyGapLockAndExtraData *lke = cce->GetKeyGapLockAndExtraData();
        if (lke != nullptr)
        {
            lke->ReleasePin();
        }

        if (!drained && ng_term > 0)
        {
            // Still short of a page. IssueDrainFetches owns the mark either
            // way: it re-marks if it issues, and clears it if there is nothing
            // left to wait for.
            IssueDrainFetches(cce, ng_term);
        }
        else
        {
            // Either the drain applied everything, or the term moved and these
            // commands are moot. Both mean this entry no longer holds up
            // recovery; leaving it marked would stall promotion forever.
            shard_->NoteDrainUnblocked(this->cc_ng_id_, cce);
            cce->RecycleKeyLock(*shard_);
        }
    }

    void BackFillPage(PageFetch &fetch) override
    {
        CcEntry<KeyT, ValueT, false, false> *cce =
            static_cast<CcEntry<KeyT, ValueT, false, false> *>(fetch.cce_);

        // The current payload may no longer be paged: a replacement or
        // deletion can commit while the fetch is in flight (docs/08 §7). The
        // §7 swap rule orphans in-flight fetches in that case, so reaching
        // here non-orphaned with a non-paged payload only happens when the
        // swap raced the completion into the same queue drain; the result is
        // discarded and stale waiter txns resolve to nothing.
        // Two DIFFERENT questions, and conflating them stranded waiters:
        //
        //   who owns the waiters  -> whichever paged object holds their
        //                            contexts, whatever the record status;
        //   may we install bytes  -> only into a payload that is still Normal.
        //
        // A DEL or an expiry leaves the payload in place with a non-Normal
        // status. Gating BOTH on Normal meant a fetch completing after the
        // deletion resolved NO waiter, so every command parked on one of that
        // object's pages waited forever. They must still be woken: the re-run
        // sees the deleted status and finishes normally.
        TxObject *obj = cce->payload_.cur_payload_.get();
        PagedTxObject *paged = obj != nullptr ? obj->AsPaged() : nullptr;
        const bool payload_installable =
            paged != nullptr && cce->PayloadStatus() == RecordStatus::Normal;

        // Installing the fetched bytes.
        //
        // The COMMITTED payload always takes them when the id is still live
        // there: its page ids ARE the durable rows, and a waiter-less fetch
        // (the reopen path, §10) exists precisely to back-fill it.
        // Build the fetched bytes ONCE. Both the committed payload and a
        // transaction's dirty copy may want this page, and they want the same
        // durable bytes, so they share one buffer; copy-on-write splits it the
        // first time either side writes the page (§7). Safe only because COW
        // exists — sharing without it would let one object's write rewrite the
        // other's page.
        // The DIRTY payload is identified early because reservation cleanup
        // must reach it on every outcome, not only the install path.
        PagedTxObject *dirty_paged_early = nullptr;
        {
            KeyGapLockAndExtraData *lke_early =
                cce->GetKeyGapLockAndExtraData();
            if (lke_early != nullptr)
            {
                TxObject *dirty_obj = lke_early->PeekDirtyPayload();
                dirty_paged_early =
                    dirty_obj != nullptr ? dirty_obj->AsPaged() : nullptr;
            }
        }

        // The completion distinguishes FOUR outcomes per payload (a review
        // finding — the old code folded the last three into "not installed"
        // and then woke every waiter with the TRANSPORT flag alone, so a
        // missing or malformed row on a LIVE page woke its waiters as
        // SUCCESS: the command re-ran, found the page still absent, and
        // faulted it again, forever):
        //
        //   kBenign     the payload was superseded, is not installable, or
        //               freed the id while the fetch flew — the waiter
        //               re-runs successfully and recomputes against current
        //               state, which no longer needs this page;
        //   kInstalled  the bytes are in;
        //   kCorrupt    the page is STILL LIVE here but the store has no row
        //               for it, the row's size is wrong, or its image fails
        //               validation — no re-run can succeed, so the waiter
        //               must get a deterministic error, not a retry loop.
        //   (transport errors — fetch.error_code_ != 0 — error every waiter
        //   below, as before.)
        //
        // The buffer is allocated ONLY after the size is validated against
        // the target's page size (the old code allocated rec_str_.size()
        // bytes first, an unbounded store-driven allocation), and the
        // §8-admitted buffer is reused as the canonical fetched buffer.
        enum class PageOutcome : uint8_t
        {
            kBenign,
            kInstalled,
            kCorrupt
        };
        std::shared_ptr<uint8_t[]> fetched;
        auto classify_and_install =
            [&fetch, &fetched, cce, paged, dirty_paged_early](
                PagedTxObject *target, bool installable) -> PageOutcome
        {
            if (target == nullptr || !installable)
            {
                return PageOutcome::kBenign;
            }
            if (fetch.error_code_ != 0)
            {
                // Transport/store error: reported per waiter below via the
                // error flag; no install and nothing to classify.
                return PageOutcome::kCorrupt;
            }
            bool live = target->IsPageLive(fetch.page_id_);
            if (fetch.rec_status_ != RecordStatus::Normal)
            {
                if (live)
                {
                    LOG(ERROR)
                        << "Paged object corruption: no store row "
                           "for live page id "
                        << fetch.page_id_ << " of key " << cce->KeyString()
                        << "; its waiters get a deterministic error.";
                    return PageOutcome::kCorrupt;
                }
                return PageOutcome::kBenign;
            }
            if (!live)
            {
                // Freed while the fetch was in flight — a benign discard.
                return PageOutcome::kBenign;
            }
            if (fetch.rec_str_.size() != target->PageSizeBytes())
            {
                LOG(ERROR) << "Paged object corruption: page row size "
                           << fetch.rec_str_.size() << " != page size "
                           << target->PageSizeBytes() << " for live page id "
                           << fetch.page_id_ << " of key " << cce->KeyString()
                           << "; its waiters get a deterministic error.";
                return PageOutcome::kCorrupt;
            }
            if (fetched == nullptr)
            {
                // Size validated; NOW build the canonical buffer, preferring
                // the §8-admitted one — from WHICHEVER payload claimed it,
                // not merely the current target. A dirty-payload fault (a
                // multi-command transaction) reserves on the DIRTY payload,
                // while the COMMITTED payload is classified first: asking
                // only the target re-introduced an unchecked allocation on
                // exactly that path (review follow-up), leaving the admitted
                // buffer idle beside a fresh one. The other payload's page
                // size is checked independently — the two payloads of one
                // entry share a layout in every legal state, but a mismatch
                // must fall through to a plain allocation, never install
                // wrong-sized bytes.
                fetched = target->TakeReservedBuffer(fetch.page_id_);
                if (fetched == nullptr)
                {
                    PagedTxObject *other =
                        target == paged ? dirty_paged_early : paged;
                    if (other != nullptr &&
                        other->PageSizeBytes() == fetch.rec_str_.size())
                    {
                        fetched = other->TakeReservedBuffer(fetch.page_id_);
                    }
                }
                if (fetched == nullptr)
                {
                    fetched = std::shared_ptr<uint8_t[]>(
                        new uint8_t[fetch.rec_str_.size()]);
                }
                std::memcpy(fetched.get(),
                            fetch.rec_str_.data(),
                            fetch.rec_str_.size());
            }
            // Live, right-sized: a refusal here is a rejected image.
            if (target->InstallPageShared(fetch.page_id_,
                                          fetched,
                                          fetch.rec_str_.size(),
                                          fetch.rec_ts_))
            {
                return PageOutcome::kInstalled;
            }
            LOG(ERROR) << "Paged object corruption: page image rejected for "
                          "live page id "
                       << fetch.page_id_ << " of key " << cce->KeyString()
                       << "; its waiters get a deterministic error.";
            return PageOutcome::kCorrupt;
        };

        PageOutcome committed_outcome =
            classify_and_install(paged, payload_installable);

        // The DIRTY payload is a different matter. It is a copy-on-write copy
        // that can free and reallocate page ids, and InstallPage only checks
        // IsLive(id) — true for a REALLOCATED id — so writing durable bytes
        // into it blindly could overwrite unrelated content. It therefore
        // takes the bytes only when one of THIS fetch's waiters is registered
        // in it, which means that object actually faulted on this exact id.
        PagedTxObject *dirty_paged = dirty_paged_early;
        KeyGapLockAndExtraData *lke = cce->GetKeyGapLockAndExtraData();
        bool dirty_classified = false;
        PageOutcome dirty_outcome = PageOutcome::kBenign;
        FetchHub *hub_for_wake = lke != nullptr ? lke->FetchHubPtr() : nullptr;

        for (uint64_t txn : fetch.waiter_txns_)
        {
            // Route each waiter to the payload that holds ITS context — the
            // object that faulted for that txn. A waiter no object claims is
            // stale (its payload was replaced, aborted, or promoted) and
            // resolves to nothing, which is the §7 rule applied uniformly
            // instead of only to the committed payload.
            // DIRTY FIRST. A multi-command transaction can hold a context in
            // BOTH objects: an early command faults on the committed object,
            // a later one creates the dirty object and faults there. Once the
            // dirty object exists every subsequent command of that tx runs on
            // it, so it is the authoritative target. Checking the committed
            // object first would let the older, already-resolved context
            // shadow the live one — ResolvePageWaiter would return its
            // now-null parked_req_ and the waiting command would never wake.
            PagedTxObject *target = nullptr;
            PageOutcome target_outcome = PageOutcome::kBenign;
            bool waiter_has_owner = false;
            if (dirty_paged != nullptr && dirty_paged->HasPageWaiter(txn))
            {
                target = dirty_paged;
                waiter_has_owner = true;
                if (!dirty_classified)
                {
                    dirty_outcome = classify_and_install(dirty_paged, true);
                    dirty_classified = true;
                }
                target_outcome = dirty_outcome;
            }
            else if (paged != nullptr && paged->HasPageWaiter(txn))
            {
                target = paged;
                waiter_has_owner = true;
                target_outcome = committed_outcome;
            }

            if (target != nullptr)
            {
                target->NotePageFetched(
                    txn,
                    fetch.page_id_,
                    target_outcome == PageOutcome::kInstalled);
            }
            // Wake from the ENTRY, unconditionally: the payload this txn
            // faulted on may already be gone (DEL, expiry), and the command
            // must still be resumed so it can observe that and finish.
            //
            // The success flag is the OWNING payload's outcome, not the
            // install result and not the transport flag alone. kBenign wakes
            // successfully — the payload vanished or freed the page, and the
            // re-run observes that (failing a read of a DELETED key with a
            // storage error instead of nil is the bug this distinction
            // avoids). kCorrupt — a live page with no row, a wrong-size row,
            // or a rejected image — errors the waiter, because a re-run
            // would just fault the same page into the same corruption
            // forever. An ownerless waiter is stale (§7) and keeps the
            // transport flag.
            bool waiter_ok =
                fetch.error_code_ == 0 &&
                (!waiter_has_owner || target_outcome != PageOutcome::kCorrupt);
            bool reached_zero = false;
            CcRequestBase *req =
                hub_for_wake != nullptr
                    ? hub_for_wake->ResolveWaiter(txn, waiter_ok, &reached_zero)
                    : nullptr;
            if (req != nullptr)
            {
                shard_->Enqueue(shard_->core_id_, req);
            }
            if (txn == kDrainTxnNumber && reached_zero)
            {
                // The drain has no request to enqueue -- reaching zero IS the
                // signal to run it again (docs/08 §10). Deferring this to the
                // shard queue would need a request type of its own; the
                // completion already runs on this shard's core, which is the
                // only context the drain may touch the entry from.
                RedriveBufferedDrain(cce);
            }
        }

        // Whatever the outcomes, the fetch this §8 reservation was claimed
        // for is over: release anything an install did not consume, on both
        // payloads. Idempotent, and a no-op on the ungated paths.
        if (paged != nullptr)
        {
            paged->DropPageReservation(fetch.page_id_);
        }
        if (dirty_paged != nullptr)
        {
            dirty_paged->DropPageReservation(fetch.page_id_);
        }
    }

    /**
     * @brief Routes the post-flush signal to the current paged payload
     * (docs/08 §9). No identity check against the exported payload is
     * needed: the ts guard inside OnPagedFlushApplied makes marking safe
     * across §7 swaps and replacement incarnations — anything written,
     * freed, or created after the export carries a newer ts and fails the
     * guard.
     */
    void OnPagedFlushApplied(LruEntry *entry,
                             uint64_t flushed_commit_ts) override
    {
        CcEntry<KeyT, ValueT, false, false> *cce =
            static_cast<CcEntry<KeyT, ValueT, false, false> *>(entry);
        TxObject *obj = cce->payload_.cur_payload_.get();
        if (obj != nullptr)
        {
            if (PagedTxObject *paged = obj->AsPaged())
            {
                ACTION_FAULT_INJECTOR("paged_flush_before_apply_callback");
                paged->OnPagedFlushApplied(flushed_commit_ts);
                ACTION_FAULT_INJECTOR("paged_flush_after_apply_callback");

                // The RELEASE half of the deletion fan-out (§9, §16). A
                // deleted paged object is kept alive past the commit ONLY so
                // this flush could carry its page-id list; once that
                // deletion is durable the block has no further purpose, and
                // holding it would trade the storage leak it fixes for a
                // memory one. The ts guard is the same one the callback
                // itself uses: release only when the flush that just landed
                // is at least as new as the entry's commit, so a deletion
                // re-dirtied after the export is not dropped early.
                if (cce->PayloadStatus() == RecordStatus::Deleted &&
                    cce->CommitTs() <= flushed_commit_ts)
                {
                    cce->payload_.cur_payload_ = nullptr;
                }
            }
        }
    }

    bool BackFill(LruEntry *entry,
                  uint64_t commit_ts,
                  RecordStatus status,
                  const std::string &rec_str,
                  bool *corrupt) override
    {
        if (commit_ts > 1 && commit_ts < schema_ts_)
        {
            DLOG(INFO) << "BackFill: discard, commit_ts: " << commit_ts
                       << ", schema_ts: " << schema_ts_;
            return true;
        }

        CcEntry<KeyT, ValueT, false, false> *cce =
            static_cast<CcEntry<KeyT, ValueT, false, false> *>(entry);
        CcPage<KeyT, ValueT, false, false> *ccp =
            static_cast<CcPage<KeyT, ValueT, false, false> *>(cce->GetCcPage());

        cce->GetKeyGapLockAndExtraData()->ReleasePin();
        cce->RecycleKeyLock(*shard_);

        if (status == RecordStatus::Unknown)
        {
            // fetch record fails.
            if (cce->PayloadStatus() == RecordStatus::Unknown && cce->IsFree())
            {
                // Remove cce if it is not referenced by anyone.
                CleanEntry(entry, ccp);
            }
            return true;
        }
        // It's possible that first ReplayLogCc/StandbyForwardCc triggers
        // FetchRecord and the second ReplayLogCc/StandbyForwardCc has_overwrite
        // and overrides the cce. Overrides the cce if the BackFilled version is
        // newer.
        bool s_obj_exist = (cce->PayloadStatus() == RecordStatus::Normal);
        if (cce->PayloadStatus() == RecordStatus::Unknown ||
            cce->CommitTs() < commit_ts)
        {
            // Length-bounded and FALLIBLE (docs/08 §5): the row came from
            // the store and is not trusted, so it is parsed BEFORE the
            // entry's commit ts / payload status are touched. A corrupt row
            // therefore leaves the entry exactly as it was — a later fetch
            // retries and errors deterministically instead of installing
            // garbage — and the CALLER, not a retry loop, surfaces the
            // error to the requesters.
            if (status == RecordStatus::Normal && rec_str.empty())
            {
                // Every valid object row carries at least its type tag, so
                // an EMPTY Normal row is corruption — and it must be caught
                // HERE, because the parse below only runs on non-empty rows
                // (a review follow-up: this bypass stamped the entry Normal
                // in Release, leaving a null payload or an old payload at
                // the newer version). Same contract as a failed parse: the
                // entry is left untouched and the requesters get a
                // deterministic error.
                LOG(ERROR) << "Corrupt store row for key " << cce->KeyString()
                           << ": status Normal with an EMPTY payload; "
                              "requesters get a deterministic error.";
                if (corrupt != nullptr)
                {
                    *corrupt = true;
                }
                return true;
            }
            if (!rec_str.empty())
            {
                size_t offset = 0;
                if (!cce->payload_.DeserializeCurrentPayload(
                        rec_str.data(), rec_str.size(), offset))
                {
                    LOG(ERROR)
                        << "Corrupt store row for key " << cce->KeyString()
                        << " (size " << rec_str.size()
                        << "): metadata failed validation; requesters get "
                           "a deterministic error.";
                    if (corrupt != nullptr)
                    {
                        *corrupt = true;
                    }
                    return true;
                }
            }
            else
            {
                assert(cce->payload_.cur_payload_ == nullptr);
            }

            cce->SetCommitTsPayloadStatus(commit_ts, status);
            cce->SetCkptTs(commit_ts);
            DLOG(INFO) << "BackFill key: " << cce->KeyString()
                       << ", status: " << int(status)
                       << ", commit_ts: " << commit_ts;

            // Check if there's any buffered replay cmds, and try to
            // commit them.
            if (cce->HasBufferedCommandList())
            {
                BufferedTxnCmdList &buffered_cmd_list =
                    cce->BufferedCommandList();
                int64_t buffered_cmd_cnt_old = buffered_cmd_list.Size();
                // Clear cmds with smaller version than kv version.
                for (auto it = buffered_cmd_list.txn_cmd_list_.begin();
                     it != buffered_cmd_list.txn_cmd_list_.end();)
                {
                    if (it->obj_version_ >= commit_ts)
                    {
                        break;
                    }
                    it = buffered_cmd_list.txn_cmd_list_.erase(it);
                }

                uint64_t commit_version = commit_ts;
                bool was_dirty = cce->IsDirty();
                bool drained = cce->TryCommitBufferedCommands(
                    shard_, commit_version, shard_->NowInMilliseconds());
                int64_t buffered_cmd_cnt_new = buffered_cmd_list.Size();
                shard_->UpdateBufferedCommandCnt(buffered_cmd_cnt_new -
                                                 buffered_cmd_cnt_old);
                cce->SetCommitTsPayloadStatus(commit_version,
                                              cce->DrainedPayloadStatus());
                this->OnCommittedUpdate(cce, was_dirty);

                // A false return means a paged CommitOn stalled on a page
                // that is not resident. Fetch it and re-drive when it lands;
                // until then the commands stay buffered and the version stops
                // where the drain stopped (docs/08 §10).
                if (!drained)
                {
                    IssueDrainFetches(cce, DrainFetchTerm());
                }

                if (buffered_cmd_list.Empty())
                {
                    // Recycles the lock if all the replay commands have been
                    // applied.
                    cce->RecycleKeyLock(*shard_);
                }
                else if (DrainFetchPending(cce))
                {
                    // A paged drain is still waiting on a page fetch. Leave the
                    // buffered commands exactly where they are: the completion
                    // resolves the reserved drain context and re-drives them.
                    // Falling through to the branch below would clear an
                    // acknowledged write, and its assert would abort a node
                    // that is merely mid-fetch -- log replay finishes in far
                    // less time than a page read from the store takes.
                }
                else if (Sharder::Instance().LeaderTerm(cc_ng_id_) > 0)
                {
                    if (txservice_skip_wal)
                    {
                        // If the kv version cannot fill the gap between
                        // buffered cmd versions, and the node is now the ng
                        // leader, it must be that the missing object version
                        // were not flushed into kv in the previous term and
                        // this node has missed the forwarded standby message.
                        // In this case, clear the buffered cmd and use the
                        // newest version we can find. This should only happen
                        // if this node is a candidate leader(previously a
                        // standby) and the wal log is disabled(we should not
                        // have missing version if log is enabled).
                        assert(Sharder::Instance().NativeNodeGroup() ==
                               cc_ng_id_);
                    }
                    else
                    {
                        // If a node is escalated from standby to leader, it
                        // may have some buffered commands sent by previous
                        // leader that are not applied yet. In this case we have
                        // to clear the buffered cmd since the missing messages
                        // will never be received since the previous leader is
                        // dead.
                        LOG(ERROR)
                            << "The data log all processed, but there "
                               "are still some commands in buffered cmd list.\n"
                            << "cce payload status: "
                            << int(cce->PayloadStatus())
                            << ", cce CommitTs: " << cce->CommitTs() << "\n"
                            << buffered_cmd_list;
                        assert(false);
                    }
                    int64_t buffered_cmd_cnt_old = buffered_cmd_list.Size();
                    buffered_cmd_list.Clear();
                    shard_->UpdateBufferedCommandCnt(-buffered_cmd_cnt_old);
                    cce->RecycleKeyLock(*shard_);
                }
            }

            if (cce->CommitTs() > commit_ts)
            {
                // cce is on a newer version after buffered cmds are applied.
                // Update last dirty commit ts.
                if (last_dirty_commit_ts_ < cce->CommitTs())
                {
                    last_dirty_commit_ts_ = cce->CommitTs();
                }
                if (cce->CommitTs() > ccp->last_dirty_commit_ts_)
                {
                    ccp->last_dirty_commit_ts_ = cce->CommitTs();
                }
            }
            if (cce->PayloadStatus() == RecordStatus::Normal)
            {
                if (!s_obj_exist)
                {
                    TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_++;
                }
                if (cce->payload_.cur_payload_ &&
                    cce->payload_.cur_payload_->HasTTL() &&
                    ccp->smallest_ttl_ > cce->payload_.cur_payload_->GetTTL())
                {
                    ccp->smallest_ttl_ = cce->payload_.cur_payload_->GetTTL();
                }
            }
            else
            {
                assert(cce->PayloadStatus() == RecordStatus::Deleted);
                if (s_obj_exist)
                {
                    TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_--;
                }
                ccp->smallest_ttl_ = 0;
            }

            if (shard_->GetCacheEvictPolicy() == CacheEvictPolicy::LO_LRU)
            {
                EnsureLargeObjOccupyPageAlone(ccp, cce);
            }
        }

        return true;
    }

    size_t NormalObjectSize() override
    {
        return TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_;
    }

private:
    std::unique_ptr<TxCommand> CreateTxCommand(std::string_view cmd_image)
    {
        assert(table_schema_ != nullptr);
        auto cmd_uptr = table_schema_->CreateTxCommand(cmd_image);
        assert(cmd_uptr != nullptr);
        return cmd_uptr;
    }

    std::pair<std::unique_ptr<ValueT>, RecordStatus>
    CreateDirtyPayloadFromExistingPayload(ValueT *payload)
    {
        assert(payload != nullptr);
        ValueT &object = *payload;
        std::unique_ptr<TxRecord> tx_rec_uptr = object.Clone();
        auto *obj_ptr = static_cast<ValueT *>(tx_rec_uptr.release());
        return {std::unique_ptr<ValueT>(obj_ptr), RecordStatus::Normal};
    }

    std::pair<std::unique_ptr<ValueT>, RecordStatus>
    CreateDirtyPayloadFromCommand(TxCommand *cmd)
    {
        auto *obj_ptr =
            static_cast<ValueT *>(cmd->CreateObject(nullptr).release());
        return {std::unique_ptr<ValueT>(obj_ptr), RecordStatus::Normal};
    }

    void CreateDirtyPayloadFromPendingCommand(
        CcEntry<KeyT, ValueT, false, false> *cce) override
    {
        assert(cce->DirtyPayloadStatus() == RecordStatus::Uncreated);
        auto var_cmd = cce->PendingCmd();
        TxCommand *pending_cmd = nullptr;
        if (std::holds_alternative<TxCommand *>(var_cmd))
        {
            pending_cmd = std::get<TxCommand *>(var_cmd);
        }
        else
        {
            pending_cmd = std::get<std::unique_ptr<TxCommand>>(var_cmd).get();
        }

        std::unique_ptr<ValueT> dirty_payload = cce->DirtyPayload();
        RecordStatus dirty_payload_status;
        // Since pending_cmd_ exists, the payload must also exist.
        // Otherwise, the dirty payload should have already been
        // created by the last command.
        assert(pending_cmd != nullptr);
        assert(cce->PayloadStatus() == RecordStatus::Normal &&
               cce->payload_.cur_payload_ != nullptr);

        // If the pending cmd is DEL command, just create Deleted dirty
        // payload.
        if (pending_cmd->IsDelete())
        {
            cce->SetDirtyPayload(nullptr);
            cce->SetDirtyPayloadStatus(RecordStatus::Deleted);
            cce->SetPendingCmd(nullptr);
        }
        else
        {
            std::tie(dirty_payload, dirty_payload_status) =
                CreateDirtyPayloadFromExistingPayload(
                    cce->payload_.cur_payload_.get());
            assert(dirty_payload_status == RecordStatus::Normal);

            // Commit the pending command.
            bool applied = CommitCommandOnDirtyPayload(
                dirty_payload, dirty_payload_status, *pending_cmd);
            // A page fault is impossible on this path: ExecuteOn ran
            // earlier in this transaction and pinned every page it
            // touched, and the shed policy skips pinned pages. A false
            // here means a pin was released too early, which would
            // otherwise drop the write silently.
            assert(applied && "CommitOn faulted on a pinned paged object");
            (void) applied;

            cce->SetDirtyPayload(std::move(dirty_payload));
            cce->SetDirtyPayloadStatus(dirty_payload_status);
            cce->SetPendingCmd(nullptr);
        }

        if (shard_->GetCacheEvictPolicy() == CacheEvictPolicy::LO_LRU)
        {
            CcPage<KeyT, ValueT, false, false> *ccp =
                static_cast<CcPage<KeyT, ValueT, false, false> *>(
                    cce->GetCcPage());
            EnsureLargeObjOccupyPageAlone(ccp, cce);
        }
    }

    /**
     * @brief Applies a command to the committed payload at commit time.
     *
     * @return True if the command was applied. False only when the payload is
     * a paged object whose CommitOn hit a non-resident page: CommitOn is
     * discover-then-mutate, so the payload is then left exactly as it was and
     * the caller must fetch the missing pages and re-drive the command rather
     * than treat it as done.
     */
    bool CommitCommandOnPayload(CcEntry<KeyT, ValueT, false, false> *cce,
                                std::unique_ptr<ValueT> &payload,
                                RecordStatus &payload_status,
                                TxCommand &cmd)
    {
        assert(payload != nullptr && payload_status == RecordStatus::Normal);
        // The deletion branch below applies the §7 swap rule, which acts on
        // the entry's CURRENT payload — so this helper must only ever be
        // handed that payload.
        assert(cce == nullptr || cce->payload_.cur_payload_ == payload);
        TxObject *obj_ptr = payload.get();
        // The 2-arg overload applies the paged-deletion rule at the point
        // the deletion is DECIDED (docs/08 §9, §16): a deleted paged block
        // is retired — fetches orphaned, parked readers woken, pins
        // dropped, buffers released — and RETAINED, tagged, same pointer
        // back. The tag, not payload nullness, is the deletion signal.
        TxObject *new_obj_ptr = cmd.CommitOn(
            obj_ptr,
            PagedCommitContext{
                shard_,
                cce != nullptr ? cce->GetKeyGapLockAndExtraData() : nullptr});
        if (!PagedCommitApplied(obj_ptr, new_obj_ptr))
        {
            return false;
        }
        if (new_obj_ptr == obj_ptr && obj_ptr != nullptr &&
            obj_ptr->AsPaged() != nullptr &&
            obj_ptr->AsPaged()->IsDeletionRetained())
        {
            // A retired paged deletion: block kept for the fan-out flush,
            // released by the post-flush callback once durable.
            payload_status = RecordStatus::Deleted;
        }
        else if (new_obj_ptr != obj_ptr)
        {
            if (new_obj_ptr == nullptr)
            {
                // This is a DEL command and the object is deleted (the
                // paged case never reaches here — it is retired above).
                payload_status = RecordStatus::Deleted;
                if (cmd.IsLazyDelete())
                {
                    shard_->EnqueueLazyFree(
                        std::unique_ptr<TxObject>(std::move(payload)));
                }
                else
                {
                    payload = nullptr;
                }
            }
            else
            {
                // The object has been changed by cmd.
                payload_status = RecordStatus::Normal;
                payload =
                    std::unique_ptr<ValueT>(static_cast<ValueT *>(new_obj_ptr));
            }
        }
        return true;
    }

    /**
     * @brief Applies a command to the dirty payload at commit time.
     *
     * @return True if the command was applied, false on a paged page fault.
     * Same contract as CommitCommandOnPayload.
     */
    bool CommitCommandOnDirtyPayload(std::unique_ptr<ValueT> &dirty_payload,
                                     RecordStatus &dirty_payload_status,
                                     TxCommand &cmd)
    {
        assert(dirty_payload != nullptr &&
               dirty_payload_status == RecordStatus::Normal);
        TxObject *old_obj_ptr = dirty_payload.get();
        // Central paged-deletion rule; lke is null ON PURPOSE — this block
        // is not yet the entry's current payload, so the fetch-orphan /
        // parked-wake half runs when it is installed (the dirty→committed
        // swap sites call the full swap rule). The retire still abandons
        // the dead block's contexts and releases its buffers; a later
        // ReleaseTxPins for the committing txn is a no-op on the erased
        // context.
        TxObject *new_obj_ptr =
            cmd.CommitOn(old_obj_ptr, PagedCommitContext{shard_, nullptr});
        if (!PagedCommitApplied(old_obj_ptr, new_obj_ptr))
        {
            return false;
        }
        if (new_obj_ptr == old_obj_ptr && old_obj_ptr != nullptr &&
            old_obj_ptr->AsPaged() != nullptr &&
            old_obj_ptr->AsPaged()->IsDeletionRetained())
        {
            dirty_payload_status = RecordStatus::Deleted;
        }
        else if (new_obj_ptr != old_obj_ptr)
        {
            if (new_obj_ptr == nullptr)
            {
                // This is a DEL command and the object is deleted (paged
                // deletions are retired above, not nulled).
                dirty_payload_status = RecordStatus::Deleted;
                if (cmd.IsLazyDelete())
                {
                    shard_->EnqueueLazyFree(
                        std::unique_ptr<TxObject>(std::move(dirty_payload)));
                }
                else
                {
                    dirty_payload = nullptr;
                }
            }
            else
            {
                // The object has been changed by cmd.
                dirty_payload =
                    std::unique_ptr<ValueT>(static_cast<ValueT *>(new_obj_ptr));
                dirty_payload_status = RecordStatus::Normal;
            }
        }
        return true;
    }

    /**
     * If the a record is according to the conditions, return true, or return
     * false to neglect this record.
     */
    bool FilterRecord(const KeyT *key,
                      const CcEntry<KeyT, ValueT, false, false> *cce,
                      int32_t obj_type,
                      const std::string_view &scan_pattern) override
    {
        if (cce->PayloadStatus() == RecordStatus::Deleted &&
            (!cce->NeedCkpt() || txservice_skip_kv) &&
            (cce->GetKeyLock() == nullptr ||
             cce->DirtyPayloadStatus() == RecordStatus::NonExistent))
        {
            return false;
        }
        if (obj_type >= 0 && cce->payload_.cur_payload_ != nullptr &&
            !cce->payload_.cur_payload_->IsMatchType(obj_type))
        {
            return false;
        }
        if (scan_pattern.size() > 0 && !key->IsMatch(scan_pattern))
        {
            return false;
        }
        else
        {
            // if ttl is expired
            TxObject *obj =
                static_cast<TxObject *>(cce->payload_.cur_payload_.get());
            if (obj != nullptr && obj->HasTTL())
            {
                if (obj->GetTTL() < shard_->NowInMilliseconds())
                {
                    return false;
                }
            }
        }

        return true;
    }

    int32_t GetObjectType(CcEntry<KeyT, ValueT, false, false> *cce) override
    {
        return cce->payload_.cur_payload_ != nullptr
                   ? cce->payload_.cur_payload_->GetObjectType()
                   : -1;
    }
};
}  // namespace txservice
