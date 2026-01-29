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
    using TemplateCcMap<KeyT, ValueT, false, false>::Find;
    using TemplateCcMap<KeyT, ValueT, false, false>::FindEmplace;
    using TemplateCcMap<KeyT, ValueT, false, false>::End;
    using typename TemplateCcMap<KeyT, ValueT, false, false>::Iterator;
    using TemplateCcMap<KeyT, ValueT, false, false>::KeySchema;
    using TemplateCcMap<KeyT, ValueT, false, false>::RecordSchema;
    using TemplateCcMap<KeyT, ValueT, false, false>::Type;
    using TemplateCcMap<KeyT, ValueT, false, false>::CleanEntry;

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

        // Always read the cce first to check if the object exists.
        CcOperation cc_op =
            req.IsReadOnly() ? CcOperation::Read : CcOperation::ReadForWrite;

        if (req.CcePtr() != nullptr)
        {
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
                assert(req.block_type_ ==
                       ApplyCc::ApplyBlockType::BlockOnFetch);
                cce->GetKeyGapLockAndExtraData()->ReleasePin();
                cce->RecycleKeyLock(*shard_);
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
                    decoded_key.Deserialize(
                        key_str->data(), offset, KeySchema());
                    look_key = &decoded_key;
                }

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

        if (check_dirty_status && cce->GetKeyLock() != nullptr &&
            cce->DirtyPayloadStatus() == RecordStatus::Normal)
        {
            std::unique_ptr<ValueT> dirty_payload = cce->DirtyPayload();
            TxObject *obj = static_cast<TxObject *>(dirty_payload.get());
            if (obj != nullptr && obj->HasTTL())
            {
                ttl = obj->GetTTL();
            }
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
            obj_result.ttl_reset_ = true;
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
                    cmd->ExecuteOn(dirty_object);
                    cce->SetDirtyPayload(std::move(dirty_payload));
                    cce->SetDirtyPayloadStatus(dirty_payload_status);
                    obj_result.rec_status_ = dirty_payload_status;
                }
                else
                {
                    assert(cce->PayloadStatus() == RecordStatus::Normal);
                    assert(cce->IsNullPendingCmd());
                    ValueT &object = *cce->payload_.cur_payload_;
                    cmd->ExecuteOn(object);
                    obj_result.rec_status_ = cce->PayloadStatus();
                }
            }
            else
            {
                assert(cce->PayloadStatus() == RecordStatus::Normal);
                assert(cce->payload_.cur_payload_ != nullptr);
                ValueT &object = *cce->payload_.cur_payload_;
                cmd->ExecuteOn(object);
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
                CommitCommandOnDirtyPayload(
                    dirty_payload, dirty_payload_status, *cmd);
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
            exec_rst = cmd->ExecuteOn(object);
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
            obj_result.commit_ts_ = shard_->Now();
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
                    cce->payload_.PassInCurrentPayload(cce->DirtyPayload());
                    status = dirty_payload_status;
                }
                else
                {
                    CommitCommandOnPayload(
                        cce->payload_.cur_payload_, status, *cmd);
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
            obj_result.commit_ts_ = shard_->Now();
            obj_result.rec_status_ = RecordStatus::Deleted;
        }
        else
        {
            obj_result.commit_ts_ = cce->CommitTs();
            obj_result.lock_ts_ = shard_->Now();
            obj_result.rec_status_ = cce->PayloadStatus();
        }

        if (obj_result.ttl_reset_ || cmd->IsOverwrite())
        {
            // If this command reset ttl or overwrite the object, the previous
            // ttl on key might be changed. Just set ttl to UINT64_MAX since
            // overwrite command and ttl reset command can always be
            // successfully replayed.
            obj_result.ttl_ = UINT64_MAX;
        }
        else
        {
            // ttl has not been changed, just used the current ttl value.
            obj_result.ttl_ = ttl;
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

        StandbyForwardEntry *forward_entry = nullptr;
        if (!shard_->GetSubscribedStandbys().empty())
        {
            forward_entry = cce->ForwardEntry();
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
                    CommitCommandOnPayload(cce->payload_.cur_payload_,
                                           payload_status,
                                           *pending_cmd);
                }
                else
                {
                    assert(false);
                }
            }
            if (forward_entry)
            {
                // Set commit ts and send the msg to standby node
                forward_entry->Request().set_commit_ts(commit_ts);
                forward_entry->Request().set_schema_version(schema_ts_);
                std::unique_ptr<StandbyForwardEntry> entry_ptr =
                    cce->ReleaseForwardEntry();
                shard_->ForwardStandbyMessage(entry_ptr.release());
            }
            bool was_dirty = cce->IsDirty();
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

            auto [key_str, rec_str, ts_str, status_str] = *entry_tuples;
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
                cce->payload_.PassInCurrentPayload(std::move(object_uptr));
                object_uptr = nullptr;
            }
            else
            {
                if (cce->PayloadStatus() == RecordStatus::Normal)
                {
                    TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_--;
                }
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

                cce->TryCommitBufferedCommands(commit_ts,
                                               shard_->NowInMilliseconds());
                int64_t buffered_cmd_cnt_new = buffered_cmd_list.Size();
                shard_->UpdateBufferedCommandCnt(buffered_cmd_cnt_new -
                                                 buffered_cmd_cnt_old);
            }

            if (cce->payload_.cur_payload_)
            {
                cce->SetCommitTsPayloadStatus(commit_ts, RecordStatus::Normal);
            }
            else
            {
                cce->SetCommitTsPayloadStatus(commit_ts, RecordStatus::Deleted);
            }
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
                                  status_offset);
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
                txn_cmd, shard_->NowInMilliseconds());
            this->OnCommittedUpdate(cce, was_dirty);
            int64_t buffered_cmd_cnt_new = buffered_cmd_list.Size();
            shard_->UpdateBufferedCommandCnt(buffered_cmd_cnt_new -
                                             buffered_cmd_cnt_old);
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

        if (commit_ts <= cce->CommitTs() ||
            commit_ts <= Sharder::Instance().NativeNodeGroupCkptTs())
        {
            // Discard message since cce has a newer version or has been
            // checkpointed by leader node.
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
                        int32_t part_id = Sharder::MapKeyHashToHashPartitionId(
                            look_key->Hash());
                        shard_->FetchRecord(table_name_,
                                            table_schema_,
                                            TxKey(look_key),
                                            cce,
                                            cc_ng_id_,
                                            req.StandbyNodeTerm(),
                                            nullptr,
                                            part_id);
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
            if ((obj_version == cce->CommitTs() || has_overwrite) &&
                !cce->HasBufferedCommandList())
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
                bool was_dirty = (cce->CommitTs() > 1 && !cce->IsPersistent());
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
                bool was_dirty = (cce->CommitTs() > 1 && !cce->IsPersistent());
                cce->EmplaceAndCommitBufferedTxnCommand(
                    txn_cmd, shard_->NowInMilliseconds());
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
                    cce->EmplaceAndCommitBufferedTxnCommand(
                        txn_cmd, shard_->NowInMilliseconds());
                    int64_t buffered_cmd_cnt_new = buffered_cmd_list->Size();
                    shard_->UpdateBufferedCommandCnt(buffered_cmd_cnt_new -
                                                     buffered_cmd_cnt_old);
                }
                else
                {
                    // No buffered commands, directly set cce commit ts.
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
                    // load payload asynchronously, pass in null as
                    // requester cc since we will buffer the cmd in replay
                    // cmd list so there's no need to put this req back in
                    // queue after record is fetched.

                    int32_t part_id =
                        Sharder::MapKeyHashToHashPartitionId(key.Hash());
                    shard_->FetchRecord(table_name_,
                                        table_schema_,
                                        TxKey(&key),
                                        cce,
                                        cc_ng_id_,
                                        ng_term,
                                        nullptr,
                                        part_id);
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
                cce->EmplaceAndCommitBufferedTxnCommand(
                    txn_cmd, shard_->NowInMilliseconds());
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

    bool BackFill(LruEntry *entry,
                  uint64_t commit_ts,
                  RecordStatus status,
                  const std::string &rec_str) override
    {
        if (commit_ts > 1 && commit_ts < schema_ts_)
        {
            DLOG(INFO) << "BackFill: discard, commit_ts: " << commit_ts
                       << ", schema_ts: " << schema_ts_;
            return true;
        }

        CcEntry<KeyT, ValueT, false, false> *cce =
            static_cast<CcEntry<KeyT, ValueT, false, false> *>(entry);
        LruPage *ccp = cce->GetCcPage();

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
        // It's possible that first ReplayLogCc triggers FetchRecord and the
        // second ReplayLogCc has_overwrite and overrides the cce.
        if (cce->PayloadStatus() == RecordStatus::Unknown)
        {
            cce->SetCommitTsPayloadStatus(commit_ts, status);
            cce->SetCkptTs(commit_ts);
            DLOG(INFO) << "BackFill key: " << cce->KeyString()
                       << ", status: " << int(status)
                       << ", commit_ts: " << commit_ts;

            if (!rec_str.empty())
            {
                size_t offset = 0;
                cce->payload_.DeserializeCurrentPayload(rec_str.data(), offset);
            }
            else
            {
                assert(cce->payload_.cur_payload_ == nullptr);
            }

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
                cce->TryCommitBufferedCommands(commit_version,
                                               shard_->NowInMilliseconds());
                int64_t buffered_cmd_cnt_new = buffered_cmd_list.Size();
                shard_->UpdateBufferedCommandCnt(buffered_cmd_cnt_new -
                                                 buffered_cmd_cnt_old);
                RecordStatus commit_status =
                    cce->payload_.cur_payload_ == nullptr
                        ? RecordStatus::Deleted
                        : RecordStatus::Normal;
                cce->SetCommitTsPayloadStatus(commit_version, commit_status);
                this->OnCommittedUpdate(cce, was_dirty);

                if (buffered_cmd_list.Empty())
                {
                    // Recycles the lock if all the replay commands have been
                    // applied.
                    cce->RecycleKeyLock(*shard_);
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
                TemplateCcMap<KeyT, ValueT, false, false>::normal_obj_sz_++;
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
                ccp->smallest_ttl_ = 0;
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
            CommitCommandOnDirtyPayload(
                dirty_payload, dirty_payload_status, *pending_cmd);
            cce->SetDirtyPayload(std::move(dirty_payload));
            cce->SetDirtyPayloadStatus(dirty_payload_status);
            cce->SetPendingCmd(nullptr);
        }
    }

    void CommitCommandOnPayload(std::unique_ptr<ValueT> &payload,
                                RecordStatus &payload_status,
                                TxCommand &cmd)
    {
        assert(payload != nullptr && payload_status == RecordStatus::Normal);
        TxObject *obj_ptr = payload.get();
        TxObject *new_obj_ptr = cmd.CommitOn(obj_ptr);
        if (new_obj_ptr != obj_ptr)
        {
            if (new_obj_ptr == nullptr)
            {
                // This is a DEL command and the object is deleted.
                payload_status = RecordStatus::Deleted;
                payload = nullptr;
            }
            else
            {
                // The object has been changed by cmd.
                payload_status = RecordStatus::Normal;
                payload =
                    std::unique_ptr<ValueT>(static_cast<ValueT *>(new_obj_ptr));
            }
        }
    }

    void CommitCommandOnDirtyPayload(std::unique_ptr<ValueT> &dirty_payload,
                                     RecordStatus &dirty_payload_status,
                                     TxCommand &cmd)
    {
        assert(dirty_payload != nullptr &&
               dirty_payload_status == RecordStatus::Normal);
        TxObject *old_obj_ptr = dirty_payload.get();
        TxObject *new_obj_ptr = cmd.CommitOn(old_obj_ptr);
        if (new_obj_ptr != old_obj_ptr)
        {
            if (new_obj_ptr == nullptr)
            {
                // This is a DEL command and the object is deleted.
                dirty_payload = nullptr;
                dirty_payload_status = RecordStatus::Deleted;
            }
            else
            {
                // The object has been changed by cmd.
                dirty_payload =
                    std::unique_ptr<ValueT>(static_cast<ValueT *>(new_obj_ptr));
                dirty_payload_status = RecordStatus::Normal;
            }
        }
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
