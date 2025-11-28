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

#include <absl/container/btree_map.h>
#include <butil/time.h>

#include <algorithm>  // std::max
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "cc_entry.h"
#include "cc_map.h"
#include "cc_page_clean_guard.h"
#include "cc_protocol.h"
#include "cc_req_misc.h"
#include "cc_request.h"
#include "cc_shard.h"
#include "error_messages.h"  //CcErrorCode
#include "fault/fault_inject.h"
#include "local_cc_shards.h"
#include "mimalloc.h"
#include "non_blocking_lock.h"
#include "proto/cc_request.pb.h"
#include "range_slice.h"
#include "remote/remote_cc_handler.h"  //RemoteCcHandler
#include "remote/remote_cc_request.h"
#include "remote/remote_type.h"
#include "scan.h"
#include "sharder.h"
#include "store/data_store_handler.h"
#include "table_statistics.h"
#include "tx_id.h"
#include "tx_key.h"
#include "tx_object.h"
#include "tx_operation_result.h"
#include "tx_record.h"
#include "tx_service.h"
#include "tx_service_common.h"
#include "tx_service_metrics.h"
#include "tx_trace.h"
#include "type.h"

namespace txservice
{
template <typename KeyT, typename ValueT>
void BackfillSnapshotForScanSlice(FetchSnapshotCc *fetch_cc,
                                  CcRequestBase *requester);

template <typename KeyT, typename ValueT, bool VersionedRecord>

void BackfillForScanNextBatch(FetchBucketDataCc *fetch_cc,
                              CcRequestBase *requester);

template <typename KeyT,
          typename ValueT,
          bool VersionedRecord,
          bool RangePartitioned>
class TemplateCcMap : public CcMap
{
    using BtreeMapIterator = typename absl::btree_map<
        KeyT,
        std::unique_ptr<
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>>>::iterator;

public:
    TemplateCcMap() = delete;
    TemplateCcMap(const TemplateCcMap &rhs) = delete;
    explicit TemplateCcMap(CcMap &&rhs) = delete;

    TemplateCcMap(CcShard *shard,
                  NodeGroupId cc_ng_id,
                  const TableName &table_name,
                  uint64_t schema_ts,
                  const TableSchema *table_schema = nullptr,
                  bool ccm_has_full_entries = false)
        : CcMap(shard,
                cc_ng_id,
                table_name,
                table_schema,
                schema_ts,
                ccm_has_full_entries),
          neg_inf_page_(this),
          pos_inf_page_(this),
          sample_pool_(nullptr)
    {
        neg_inf_page_.prev_page_ = nullptr;
        neg_inf_page_.next_page_ = &pos_inf_page_;
        pos_inf_page_.prev_page_ = &neg_inf_page_;
        pos_inf_page_.next_page_ = nullptr;

        if (shard->realtime_sampling_ && table_schema && !table_name.IsMeta() &&
            table_name != sequence_table_name)
        {
            TableStatistics<KeyT> *statistics =
                static_cast<TableStatistics<KeyT> *>(
                    table_schema->StatisticsObject().get());
            assert(statistics != nullptr);

            sample_pool_ = statistics->MutableSamplePool(table_name, cc_ng_id);
            assert(sample_pool_ != nullptr);
        }

        TX_TRACE_ASSOCIATE_WITH_CONTEXT(
            (txservice::CcMap *) this,
            (txservice::LruEntry *) &neg_inf_,
            [this]() -> std::string
            {
                return std::string("\"associate\":\"neg_inf_\", \"cce_ptr_\":")
                    .append(std::to_string(
                        reinterpret_cast<uint64_t>(&this->neg_inf_)))
                    .append("\"key_\":")
                    .append(std::to_string(
                        reinterpret_cast<uint64_t>(&this->neg_inf_.Key())))
                    .append("\"table_name_\":")
                    .append(this->table_name_.StringView());
            });

        TX_TRACE_ASSOCIATE_WITH_CONTEXT(
            (txservice::CcMap *) this,
            (txservice::LruEntry *) &pos_inf_,
            [this]() -> std::string
            {
                return std::string("\"associate\":\"neg_inf_\", \"cce_ptr_\":")
                    .append(std::to_string(
                        reinterpret_cast<uint64_t>(&this->pos_inf_)))
                    .append("\"key_\":")
                    .append(std::to_string(
                        reinterpret_cast<uint64_t>(&this->pos_inf_.key_)))
                    .append("\"table_name_\":")
                    .append(this->table_name_.StringView());
            });
    }

    virtual ~TemplateCcMap()
    {
        Clean();
        neg_inf_.ClearLocks(*shard_, cc_ng_id_);
    }

    bool Execute(AcquireCc &req) override
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

        CcHandlerResult<std::vector<AcquireKeyResult>> *hd_res = req.Result();
        AcquireKeyResult &acquire_key_result =
            req.IsLocal() ? hd_res->Value()[req.HandlerResultIndex()]
                          : hd_res->Value()[0];
        CcEntryAddr &cce_addr = acquire_key_result.cce_addr_;
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce_ptr =
            nullptr;
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp = nullptr;
        bool block_by_lock = req.BlockedByLock();
        const KeyT *target_key = nullptr;
        KeyT decoded_key;

        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        CODE_FAULT_INJECTOR("term_TemplateCcMap_Execute_AcquireCc", {
            LOG(INFO) << "FaultInject  term_TemplateCcMap_Execute_AcquireCc";
            ng_term = -1;
        });
        if (ng_term < 0)
        {
            hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        if (req.SchemaVersion() != 0 && req.SchemaVersion() != schema_ts_)
        {
            hd_res->SetError(CcErrorCode::REQUESTED_TABLE_SCHEMA_MISMATCH);
            return true;
        }

        LockType acquired_lock = LockType::NoLock;
        CcErrorCode err_code = CcErrorCode::NO_ERROR;
        if (req.CcePtr() != nullptr)
        {
            // The request was blocked before and is now unblocked.
            cce_ptr = static_cast<
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                req.CcePtr());
            if (block_by_lock)
            {
                std::tie(acquired_lock, err_code) =
                    LockHandleForResumedRequest(cce_ptr,
                                                cce_ptr->CommitTs(),
                                                cce_ptr->PayloadStatus(),
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
                // Blocked by key cache add.
                assert(table_name_.IsBase() && txservice_enable_key_cache);
                ccp = static_cast<
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                    cce_ptr->GetCcPage());
                auto it = Iterator(cce_ptr, ccp, &neg_inf_);
                target_key = it->first;
                auto res = shard_->local_shards_.AddKeyToKeyCache(
                    table_name_, cc_ng_id_, shard_->core_id_, *target_key);
                if (res == RangeSliceOpStatus::Retry)
                {
                    // If the insert fails due to key cache is being
                    // initialized, retry later.
                    req.SetBlockedByLock(false);
                    shard_->Enqueue(&req);
                    return false;
                }
                else if (res == RangeSliceOpStatus::Successful)
                {
                    // If the insert is successful, we need to set the
                    // status to DELETED. We only delete from key cache
                    // when a key is evicted from cc map and its status
                    // is DELETED. So if the transaction aborts later
                    // and we didn't update the payload status to
                    // DELETED here, the status will remain as Unknown
                    // until its kicked out from memory, and the key
                    // will never be removed from key cache.
                    cce_ptr->SetCommitTsPayloadStatus(1U,
                                                      RecordStatus::Deleted);
                    cce_ptr->SetCkptTs(1U);
                }
                hd_res->SetFinished();
                return true;
            }
        }
        else
        {
            // First time the request is processed.

            if (req.Key() != nullptr)
            {
                target_key = static_cast<const KeyT *>(req.Key());
            }
            else
            {
                const std::string *key_str = req.KeyStr();

                assert(key_str != nullptr);

                size_t offset = 0;
                decoded_key.Deserialize(key_str->data(), offset, KeySchema());
                target_key = &decoded_key;
            }

            if (req.IsInsert())
            {
                Iterator it = Floor(*target_key);
                const KeyT *key_ptr = it->first;
                cce_ptr = it->second;
                ccp = it.GetPage();

                if (cce_ptr != &neg_inf_ && *key_ptr == *target_key)
                {
                    // The floor entry's key is equal to the insert key. If the
                    // key is deleted, the insert becomes an update. Or the
                    // insert is aborted due to the duplidate key conflict.
                    if (cce_ptr->PayloadStatus() == RecordStatus::Deleted)
                    {
                        cce_addr.SetCceLock(
                            reinterpret_cast<uint64_t>(
                                cce_ptr->GetKeyGapLockAndExtraData()),
                            ng_term,
                            req.NodeGroupId(),
                            shard_->LocalCoreId());
                    }
                    else
                    {
                        // Inserts a duplicate key.
                        hd_res->SetError(CcErrorCode::DUPLICATE_INSERT_ERR);
                        return true;
                    }
                }

                req.SetCcePtr(cce_ptr);
            }
            else
            {
                Iterator it = FindEmplace(*target_key);
                cce_ptr = it->second;
                ccp = it.GetPage();

                if (cce_ptr == nullptr)
                {
                    // The acquire request needs a new cc entry but the cc map
                    // has reached the maximal capacity.
                    shard_->EnqueueWaitListIfMemoryFull(&req);
                    return false;
                }

                assert(cce_ptr != nullptr);
                req.SetCcePtr(cce_ptr);
            }
        }

        // Cce ptr either points to the cc entry whose gap will accommodate the
        // new insert, or the cc entry whose key will be updated/deleted.
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> &cc_entry =
            *cce_ptr;

        {
            if (!block_by_lock)
            {
                std::tie(acquired_lock, err_code) =
                    AcquireCceKeyLock(&cc_entry,
                                      cc_entry.CommitTs(),
                                      ccp,
                                      cc_entry.PayloadStatus(),
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

            cce_addr.SetCceLock(reinterpret_cast<uint64_t>(
                                    cce_ptr->GetKeyGapLockAndExtraData()),
                                ng_term,
                                req.NodeGroupId(),
                                shard_->LocalCoreId());

            if (err_code == CcErrorCode::NO_ERROR)
            {
                assert(acquired_lock == LockType::WriteLock);
                // for mvcc
                uint64_t lock_ts = std::max(req.Ts(), shard_->Now());
                cc_entry.GetKeyLock()->SetWLockTs(lock_ts);

                // Updates last_vali_ts after successfully acquiring the write
                // lock such that it is no smaller than the current time of
                // the shard. The net effect is that the tx acquiring the write
                // lock is forced not to commit at a time earlier than the
                // clock of this cc node, even if the clock of the tx's
                // coordinator node drifts and falls behind. Checkpointing
                // relies on this property to avoid picking a checkpoint ts in
                // this shard that may overlap with the ongoing tx.
                acquire_key_result.last_vali_ts_ =
                    std::max(shard_->LastReadTs(), lock_ts);

                uint64_t curr_version_ts = cc_entry.CommitTs();
                // If a write is an update, delete or insert w/o duplicate, the
                // write must be preceded by a read, which acquires the write
                // intent and sets the payload status. The fact that the commit
                // ts is 0 means that this write disregards the existing value,
                // if there is any, and overwrites it. In such a case, we return
                // the current time via acquire_key_result.commit_ts_ so that
                // the tx's commit timestamp is bigger than the prior version
                // (if there is any).
                curr_version_ts =
                    curr_version_ts > 0 ? curr_version_ts : shard_->Now();
                acquire_key_result.commit_ts_ = curr_version_ts;
                if (cce_ptr->PayloadStatus() == RecordStatus::Unknown)
                {
                    if (txservice_enable_key_cache && table_name_.IsBase())
                    {
                        // cce is just created by find emplace, add this key to
                        // key cache. Usually a pk insert always does a read
                        // first to check pk duplicate, which creates the cce
                        // first and inserts the key into key cache. But for
                        // cases that disregards current value, like hidden pk
                        // or auto incr pk insert, the ReadCc is skipped and we
                        // need to update key cache here.
                        auto res = shard_->local_shards_.AddKeyToKeyCache(
                            table_name_,
                            cc_ng_id_,
                            shard_->core_id_,
                            *target_key);
                        if (res == RangeSliceOpStatus::Retry)
                        {
                            // If the insert fails due to key cache is being
                            // initialized, retry later.
                            req.SetBlockedByLock(false);
                            shard_->Enqueue(&req);
                            return false;
                        }

                        // Set the status as deleted with ts == 1 since we know
                        // fore sure this key never existed.
                        // Since the key is added to key cache, cce status
                        // cannot stay as Unknown otherwise this key will not be
                        // eviected from key cache if the cce is kicked out of
                        // memory as Unknwon status. To keep the key cache and
                        // status consistent, set the cce status as deleted to
                        // indicate does not exist.
                        cce_ptr->SetCommitTsPayloadStatus(
                            1U, RecordStatus::Deleted);
                        cce_ptr->SetCkptTs(1U);
                    }
                }

                hd_res->SetFinished();
            }
            else if (err_code == CcErrorCode::ACQUIRE_LOCK_BLOCKED)
            {
                // For 2PL, a conflict blocks the tx by putting it into the
                // lock's blocking queue.

                uint32_t tx_node = (req.Txn() >> 32L) >> 10;
                if (tx_node != req.NodeGroupId())
                {
                    // If the acquire request comes from a remote node,
                    // sends acknowledgement to the sender when the request
                    // is blocked.
                    remote::RemoteAcquire &remote_req =
                        static_cast<remote::RemoteAcquire &>(req);
                    remote_req.Acknowledge();
                }
                req.SetBlockedByLock(true);

                return false;
            }
            else
            {
                // lock confilct: back off and retry.
                req.Result()->SetError(err_code);
                return true;
            }
        }

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

        CODE_FAULT_INJECTOR("delay_release_write_lock_on_pk", {
            if (table_name_.Type() == TableType::Primary)
            {
                // throw back to cc_queue
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
            }
        });

        const CcEntryAddr *cce_addr = req.CceAddr();

        CODE_FAULT_INJECTOR("term_TemplateCcMap_Execute_PostWriteCc", {
            if (table_name_.Type() == TableType::Primary)
            {
                LOG(INFO) << "FaultInject  "
                             "term_TemplateCcMap_Execute_PostWriteCc";
                req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                return true;
            }
        });

        if (!Sharder::Instance().CheckLeaderTerm(cce_addr->NodeGroupId(),
                                                 cce_addr->Term()))
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        const ValueT *commit_val = static_cast<const ValueT *>(req.Payload());
        TxNumber txn = req.Txn();
        uint64_t commit_ts = req.CommitTs();
        const std::string *payload_str = req.PayloadStr();
        OperationType op_type = req.GetOperationType();
        bool is_del = op_type == OperationType::Delete;

        if (cce_addr->InsertPtr() != 0)
        {
            assert("Unsupported insert for phantom reads.");
            return true;
        }
        else
        {
            // upsert and delete branch.
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce;
            const KeyT *write_key = nullptr;
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *cc_page =
                nullptr;

            cce = reinterpret_cast<
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                cce_addr->ExtractCce());

            NonBlockingLock *lk = cce->GetKeyLock();
            if (lk == nullptr || !lk->HasWriteLock() ||
                lk->WriteLockTx() != txn)
            {
                req.Result()->SetFinished();
                return true;
            }

            cc_page = static_cast<
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                cce->GetCcPage());
            assert(cc_page != nullptr);
            write_key = cc_page->KeyOfEntry(cce);

            if (commit_ts > 0)
            {
                if (RangePartitioned)
                {
                    const uint64_t cce_version = cce->CommitTs();
                    if (cce_version == 1 ||
                        (cce_version == 0 && op_type == OperationType::Insert))
                    {
                        // If commit ts is 1 (entry does not exist and has no
                        // previous version), that means it does not exist in
                        // data store at all. If commit ts is 0 (entry does not
                        // exist in memory), that means it does not known
                        // whether it exists in data store. But for Insert
                        // operation, that means in data store it does not exist
                        // at all, or there is an entry with the Deleted status.
                        // In either case, the data store size of this entry can
                        // be set to 0.
                        cce->entry_info_.SetDataStoreSize(0);
                    }
                }

                // for mvcc
                if (shard_->EnableMvcc())
                {
                    // Archives whose version are bigger than ccentry's ckpt_ts_
                    // may be in use by checkpointer and must not be deleted
                    // here. These expired archives will be deleted at next
                    // checkpoint.
                    uint64_t recycle_ts = shard_->GlobalMinSiTxStartTs();
                    cce->KickOutArchiveRecords(recycle_ts);
                    cce->ArchiveBeforeUpdate();
                }

                if (is_del)
                {
                    cce->payload_.SetCurrentPayload(nullptr);
                }
                else if (payload_str == nullptr)
                {
                    cce->payload_.SetCurrentPayload(commit_val);
                }
                else
                {
                    size_t offset = 0;
                    cce->payload_.DeserializeCurrentPayload(payload_str->data(),
                                                            offset);
                }

                RecordStatus cce_old_status = cce->PayloadStatus();
                RecordStatus new_status =
                    is_del ? RecordStatus::Deleted : RecordStatus::Normal;
                cce->SetCommitTsPayloadStatus(commit_ts, new_status);

                if (req.IsInitialInsert())
                {
                    // Updates the ckpt ts after commit ts is set.
                    cce->SetCkptTs(1U);
                }

                DLOG_IF(INFO, TRACE_OCC_ERR)
                    << "PostWriteCc, txn:" << txn << " ,cce: " << cce
                    << " ,commit_ts: " << commit_ts;

                if (commit_ts > last_dirty_commit_ts_)
                {
                    last_dirty_commit_ts_ = commit_ts;
                }
                if (commit_ts > cc_page->last_dirty_commit_ts_)
                {
                    cc_page->last_dirty_commit_ts_ = commit_ts;
                }
                if (sample_pool_)
                {
                    assert(write_key != nullptr);

                    if (op_type == OperationType::Insert ||
                        op_type == OperationType::Upsert)
                    {
                        sample_pool_->OnInsert(
                            *shard_, *write_key, KeySchema());
                    }
                    else if (op_type == OperationType::Delete)
                    {
                        if (cce_old_status == RecordStatus::Normal)
                        {
                            sample_pool_->OnDelete(
                                *shard_, *write_key, KeySchema());
                        }
                    }
                }
            }

            ReleaseCceLock(cce->GetKeyLock(),
                           cce,
                           txn,
                           req.NodeGroupId(),
                           LockType::WriteLock);
            if (cce->PayloadStatus() == RecordStatus::Unknown && cce->IsFree())
            {
                // An unknown cce occurs when a transaction that tries to insert
                // a new key is aborted due to a conflict/dead lock or some
                // internal failure. Remove the unused cce if there is no read
                // intent on it.
                CleanEntry(cce, cc_page);
            }
            req.Result()->SetFinished();
            return true;
        }
    }

    bool Execute(AcquireAllCc &req) override
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

        if (table_name_.Type() == TableType::Secondary ||
            table_name_.Type() == TableType::UniqueSecondary)
        {
            assert(false);
            return false;
        }

        CcHandlerResult<AcquireAllResult> *hd_res = req.Result();
        AcquireAllResult &acquire_all_result = hd_res->Value();
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce_ptr =
            nullptr;
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp = nullptr;
        bool resume = false;
        const KeyT *target_key = nullptr;
        bool will_insert = false;

        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        if (ng_term < 0)
        {
            return hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }

        uint16_t tx_core_id = ((req.Txn() >> 32L) & 0x3FF) % shard_->core_cnt_;

        LockType acquired_lock = LockType::NoLock;
        CcErrorCode err_code = CcErrorCode::NO_ERROR;
        if (req.CcePtr(shard_->core_id_) != nullptr)
        {
            // The request was blocked before and is now unblocked.
            resume = true;
            cce_ptr = static_cast<
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                req.CcePtr(shard_->core_id_));
            std::tie(acquired_lock, err_code) =
                LockHandleForResumedRequest(cce_ptr,
                                            cce_ptr->CommitTs(),
                                            cce_ptr->PayloadStatus(),
                                            &req,
                                            ng_id,
                                            ng_term,
                                            req.TxTerm(),
                                            req.CcOp(),
                                            req.Isolation(),
                                            req.Protocol(),
                                            0,
                                            false);
            DLOG(INFO) << "AcquireAllCc from txn: " << req.Txn()
                       << ", on table: " << table_name_.StringView()
                       << ", key: " << cce_ptr->KeyString()
                       << " is resumed from blocking. acquired_lock: "
                       << int(acquired_lock) << ", err_code: " << int(err_code);
        }
        else
        {
            // First time the request is processed in this shard. Or the request
            // is blocked previously because the cc map is full.

            if (req.Key() != nullptr)
            {
                target_key = static_cast<const KeyT *>(req.Key());
            }
            else
            {
                assert(shard_->core_id_ == 0);
                switch (*req.KeyStrType())
                {
                case KeyType::NegativeInf:
                    target_key = KeyT::NegativeInfinity();
                    req.SetTxKey(target_key);
                    break;
                case KeyType::PositiveInf:
                    target_key = KeyT::PositiveInfinity();
                    req.SetTxKey(target_key);
                    break;
                case KeyType::Normal:
                    const std::string *key_str = req.KeyStr();
                    assert(key_str != nullptr);
                    std::unique_ptr<KeyT> decoded_key =
                        std::make_unique<KeyT>();
                    size_t offset = 0;
                    decoded_key->Deserialize(
                        key_str->data(), offset, KeySchema());
                    target_key = decoded_key.get();
                    req.SetDecodedKey(TxKey(std::move(decoded_key)));
                    break;
                }
            }

            if (hd_res->RefCnt() == 0)
            {
                // This is the first time req is processed on any core,
                // send the req to rest of the cores.
                hd_res->SetRefCnt(shard_->core_cnt_);
                for (uint32_t core_id = 1; core_id < shard_->core_cnt_;
                     core_id++)
                {
                    MoveRequest(&req, core_id);
                }

                if (!req.IsLocal() && req.Protocol() == CcProtocol::Locking)
                {
                    remote::RemoteAcquireAll &remote_req =
                        static_cast<remote::RemoteAcquireAll &>(req);
                    remote_req.SetCoreCnt(shard_->core_cnt_);
                }
            }

            if (req.IsInsert())
            {
                // For insert requests, finds a cc entry whose gap will
                // accommodate the insert key.
                Iterator it = Floor(*target_key);
                const KeyT *key_ptr = it->first;
                cce_ptr = it->second;
                ccp = it.GetPage();

                if (cce_ptr != &neg_inf_ && *key_ptr == *target_key)
                {
                    // The floor entry's key is equal to the insert key. If the
                    // key is deleted, the insert becomes an update. Or the
                    // insert is aborted due to the duplidate key conflict.
                    if (cce_ptr->PayloadStatus() != RecordStatus::Deleted)
                    {
                        // Inserts a duplicate key.
                        return hd_res->SetError(
                            CcErrorCode::DUPLICATE_INSERT_ERR);
                    }
                    else
                    {
                        if (shard_->core_id_ == tx_core_id)
                        {
                            acquire_all_result.local_cce_addr_.SetCceLock(
                                reinterpret_cast<uint64_t>(
                                    cce_ptr->GetKeyGapLockAndExtraData()),
                                ng_term,
                                req.NodeGroupId(),
                                shard_->LocalCoreId());
                        }

                        req.SetCcePtr(cce_ptr, shard_->core_id_);
                    }
                }
                else
                {
                    will_insert = true;
                }
            }
            else
            {
                Iterator it = FindEmplace(*target_key);
                cce_ptr = it->second;
                ccp = it.GetPage();

                if (cce_ptr == nullptr)
                {
                    // The acquire request needs a new cc entry but the cc map
                    // has reached the maximal capacity. Blocks the request by
                    // putting it into the wait list.
                    shard_->EnqueueWaitListIfMemoryFull(&req);
                    return false;
                }

                req.SetCcePtr(cce_ptr, shard_->core_id_);
            }
        }

        // Cce ptr either points to the cc entry whose gap will accommodate the
        // new insert, or the cc entry whose key will be updated/deleted.
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> &cc_entry =
            *cce_ptr;

        if (will_insert)
        {
            assert("Unsupported insert for phantom reads.");
        }
        else
        {
            int64_t tx_term = req.TxTerm();
            IsolationLevel iso_lvl = req.Isolation();
            CcProtocol cc_proto = req.Protocol();
            CcOperation cc_op = req.CcOp();

            auto try_ack_for_remote_req = [&req](int64_t term = -1,
                                                 uint32_t node_group_id = 0,
                                                 uint32_t core_id = 0,
                                                 uint64_t cce_lock_addr = 0)
            {
                if (!req.IsLocal() && req.Protocol() == CcProtocol::Locking)
                {
                    remote::RemoteAcquireAll &remote_req =
                        static_cast<remote::RemoteAcquireAll &>(req);
                    remote_req.TryAcknowledge(
                        term, node_group_id, core_id, cce_lock_addr);
                }
            };

            // On execution resumption, the write lock has been acquired when
            // being unblocked.
            if (!resume)
            {
                std::tie(acquired_lock, err_code) =
                    AcquireCceKeyLock(&cc_entry,
                                      cc_entry.CommitTs(),
                                      ccp,
                                      cc_entry.PayloadStatus(),
                                      &req,
                                      req.NodeGroupId(),
                                      ng_term,
                                      tx_term,
                                      cc_op,
                                      iso_lvl,
                                      cc_proto,
                                      0,
                                      false);
            }

            switch (err_code)
            {
            case CcErrorCode::NO_ERROR:
            {
                if (cc_entry.PayloadStatus() != RecordStatus::Deleted)
                {
                    assert(acquired_lock == LockType::WriteIntent ||
                           acquired_lock == LockType::WriteLock);
                }

                // Updates last_vali_ts such that it is no smaller than (1) all
                // read transactions that have read the item in all shards, and
                // (2) the local time.
                req.SetLastValidTs(shard_->LastReadTs());

                if (shard_->core_id_ == tx_core_id)
                {
                    acquire_all_result.local_cce_addr_.SetCceLock(
                        reinterpret_cast<uint64_t>(
                            cce_ptr->GetKeyGapLockAndExtraData()),
                        ng_term,
                        req.NodeGroupId(),
                        shard_->LocalCoreId());
                    acquire_all_result.commit_ts_ = cc_entry.CommitTs();
                    acquire_all_result.node_term_ = ng_term;
                }

                if (!resume)
                {
                    try_ack_for_remote_req();
                }

                CODE_FAULT_INJECTOR("term_TemplateCcMap_Execute_AcquireAllCc", {
                    LOG(INFO) << "FaultInject  "
                                 "term_TemplateCcMap_Execute_AcquireAllCc";
                    return hd_res->SetError(
                        CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                });

                return hd_res->SetFinished();
            }
            case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
            {
                DLOG(INFO) << "AcquireAllCc from txn: " << req.Txn()
                           << ", on table: " << table_name_.StringView()
                           << ", key: " << cce_ptr->KeyString()
                           << " is blocked.";
                // If the request comes from a remote node, sends
                // acknowledgement to the sender when the request is
                // blocked.
                if (!resume)
                {
                    try_ack_for_remote_req(
                        ng_term,
                        ng_id,
                        shard_->core_id_,
                        reinterpret_cast<uint64_t>(cce_ptr->GetLockAddr()));
                }

                return false;
            }
            default:
            {
                // lock confilct: back off and retry.
                return hd_res->SetError(err_code);
            }
            }  //-- end: switch
        }  //-- end: acquire lock

        return true;
    }

    bool Execute(PostWriteAllCc &req) override
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

        if (table_name_.Type() == TableType::Secondary ||
            table_name_.Type() == TableType::UniqueSecondary)
        {
            assert(false);
            return false;
        }

        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        const KeyT *target_key = nullptr;
        if (req.Key() != nullptr)
        {
            target_key = static_cast<const KeyT *>(req.Key());
        }
        else
        {
            switch (*req.KeyStrType())
            {
            case KeyType::NegativeInf:
                target_key = KeyT::NegativeInfinity();
                req.SetTxKey(target_key);
                break;
            case KeyType::PositiveInf:
                target_key = KeyT::PositiveInfinity();
                req.SetTxKey(target_key);
                break;
            case KeyType::Normal:
                const std::string *key_str = req.KeyStr();
                assert(key_str != nullptr);
                std::unique_ptr<KeyT> decoded_key = std::make_unique<KeyT>();
                size_t offset = 0;
                decoded_key->Deserialize(key_str->data(), offset, KeySchema());
                target_key = decoded_key.get();
                req.SetDecodedKey(TxKey(std::move(decoded_key)));
                break;
            }
        }

        const ValueT *payload = nullptr;
        if (req.Payload() != nullptr)
        {
            payload = static_cast<const ValueT *>(req.Payload());
        }
        // commit_ts = 0 means transaction failed (e.g. failed at prepare
        // phase), we have nothing to upload, only need to release write intent.
        else if (req.CommitTs() > 0 &&
                 req.CommitType() != PostWriteType::DowngradeLock)
        {
            if (req.DecodedPayload() == nullptr)
            {
                const std::string *payload_str = req.PayloadStr();
                assert(payload_str != nullptr);
                std::unique_ptr<ValueT> decoded_rec =
                    std::make_unique<ValueT>();
                size_t offset = 0;
                decoded_rec->Deserialize(payload_str->data(), offset);
                payload = decoded_rec.get();
                req.SetDecodedPayload(std::move(decoded_rec));
            }
            else
            {
                payload = static_cast<const ValueT *>(req.DecodedPayload());
            }
        }

        const KeyT *key_ptr = nullptr;
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce_ptr =
            nullptr;
        if (req.OpType() == OperationType::Insert)
        {
            Iterator it = Floor(*target_key);
            key_ptr = it->first;
            cce_ptr = it->second;
        }
        else
        {
            Iterator it = FindEmplace(*target_key);
            key_ptr = it->first;
            cce_ptr = it->second;
        }

        if (cce_ptr == nullptr)
        {
            shard_->EnqueueWaitListIfMemoryFull(&req);
            return false;
        }

        TxNumber txn = req.Txn();
        uint64_t commit_ts = req.CommitTs();

        if (req.OpType() == OperationType::Insert && *key_ptr != *target_key)
        {
            assert("Unsupported insert for phantom reads.");
        }
        else
        {
            LockType lk_type = LockType::NoLock;
            if (cce_ptr->GetKeyLock() != nullptr)
            {
                // AcquireAllCc only acquire WriteIntent or WriteLock
                auto [w_tx, w_type] = cce_ptr->GetKeyLock()->WriteTx();

                if (w_tx == txn)
                {
                    if (w_type == NonBlockingLock::WriteLockType::WriteLock)
                    {
                        lk_type = LockType::WriteLock;
                    }
                    else if (w_type ==
                             NonBlockingLock::WriteLockType::WriteIntent)
                    {
                        lk_type = LockType::WriteIntent;
                    }
                }
            }

            if (lk_type != LockType::NoLock)
            {
                if (commit_ts > 0 &&
                    req.CommitType() != PostWriteType::DowngradeLock)
                {
                    cce_ptr->payload_.SetCurrentPayload(payload);
                    // A prepare commit request only installs the dirty value,
                    // and does not change the record status and commit_ts.
                    if (req.CommitType() == PostWriteType::Commit ||
                        req.CommitType() == PostWriteType::PostCommit)
                    {
                        RecordStatus status =
                            (req.OpType() == OperationType::Delete ||
                             req.OpType() == OperationType::DropTable)
                                ? RecordStatus::Deleted
                                : RecordStatus::Normal;

                        cce_ptr->SetCommitTsPayloadStatus(commit_ts, status);
                    }
                }

                // When commit_ts = 0, the request removes the write lock
                // without installing a new value.

                if (req.CommitType() == PostWriteType::PrepareCommit ||
                    req.CommitType() == PostWriteType::DowngradeLock)
                {
                    // For PrepareCommit and DowngradeLock, the
                    // post-write-all request keeps write intent or downgrades
                    // the write lock to the write intent.
                    if (lk_type == LockType::WriteLock)
                    {
                        DowngradeCceKeyWriteLock(cce_ptr, txn);
                    }
                }
                else if (req.CommitType() == PostWriteType::Commit ||
                         req.CommitType() == PostWriteType::PostCommit)
                {
                    // For PostCommit or Commit, the post-write-all request
                    // releases the write lock.

                    // Do not recycle the lock on Catalog since it's frequently
                    // accessed.
                    bool recycle_lock =
                        table_name_.Type() != TableType::Catalog;
                    ReleaseCceLock(cce_ptr->GetKeyLock(),
                                   cce_ptr,
                                   txn,
                                   req.NodeGroupId(),
                                   lk_type,
                                   recycle_lock);
                }
                else
                {
                    assert(req.CommitType() == PostWriteType::UpdateDirty &&
                           req.OpType() == OperationType::AddIndex);
                }
            }
        }

        if (shard_->core_id_ == shard_->core_cnt_ - 1)
        {
            req.Result()->SetFinished();
            req.SetDecodedPayload(nullptr);
            return true;
        }
        else
        {
            req.ResetCcm();
            MoveRequest(&req, shard_->core_id_ + 1);
            return false;
        }
    }

    bool Execute(PostReadCc &req) override
    {
        const CcEntryAddr &cce_addr = *req.CceAddr();
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> &cc_entry =
            *reinterpret_cast<
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                cce_addr.ExtractCce());

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

        CODE_FAULT_INJECTOR("before_post_read", {
            NodeGroupId ng = (req.Txn() >> 32L) >> 10;
            if (ng != cc_ng_id_)
            {
                LOG(INFO) << "FaultInject before_post_read: skip executing "
                             "PostReadCc to timeout transaction  tx: "
                          << req.Txn();
                return true;
            }
        });

        auto hd_res = req.Result();
        CODE_FAULT_INJECTOR(
            "term_TemplateCcMap_Execute_PostReadCc", {
                if (strstr(typeid(*this).name(), "CatalogCcMap") == nullptr &&
                    table_name_.Type() == TableType::Primary)
                {
                    LOG(INFO)
                        << "FaultInject  term_TemplateCcMap_Execute_PostReadCc";
                    hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                    return true;
                }
            });

        int64_t standby_node_term = Sharder::Instance().StandbyNodeTerm();

        if (!Sharder::Instance().CheckLeaderTerm(cce_addr.NodeGroupId(),
                                                 cce_addr.Term()) &&
            (standby_node_term < 0 || standby_node_term != cce_addr.Term()))
        {
            LOG(INFO) << "PostReadCc, node_group(#" << cce_addr.NodeGroupId()
                      << ") term < 0, tx:" << req.Txn()
                      << " ,cce: " << cce_addr.ExtractCce();
            hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        uint64_t key_ts = req.KeyTs();
        uint64_t gap_ts = req.GapTs();
        uint64_t commit_ts = req.CommitTs();
        TxNumber txn = req.Txn();

        bool recycle_lock = true;
        // Do not recycle the lock on Catalog CcEntries since it's frequently
        // accessed.
        recycle_lock = table_name_.Type() != TableType::Catalog;

        // FIXME(lzx): Now, we don't backfill for "Unkown" entry when scanning.
        // So, Validate operation fails if another tx backfilled it. Temporary
        // fix is that we don't validate for "Unkown" status results.
        if (cc_entry.PayloadStatus() != RecordStatus::Unknown && key_ts > 0 &&
            key_ts != cc_entry.CommitTs())
        {
            ReleaseCceLock(cc_entry.GetKeyLock(),
                           &cc_entry,
                           txn,
                           req.NodeGroupId(),
                           LockType::NoLock,
                           recycle_lock);
            // broken repeatable read, set error.
            DLOG(INFO) << "txn: " << req.Txn()
                       << " validate fail on key: " << cc_entry.KeyString()
                       << ", key_ts: " << key_ts
                       << ", cce commit ts: " << cc_entry.CommitTs()
                       << ", table: " << table_name_.String();
            hd_res->SetError(
                CcErrorCode::VALIDATION_FAILED_FOR_VERSION_MISMATCH);
            DLOG_IF(INFO, TRACE_OCC_ERR)
                << "PostReadCc, occ_err, txn:" << txn << " ,cce: " << &cc_entry
                << " ,payload_status: "
                << static_cast<int>(cc_entry.PayloadStatus())
                << " ,key_ts: " << key_ts
                << " ,cc_entry.commit_ts_: " << cc_entry.CommitTs();
        }
        else
        {
            // For 2PL, read validation is equivalent to releasing the read
            // lock. In contrast to the conventional 2PL where read locks
            // are released after logging, our protocol releases the read
            // lock before the log is persisted. This difference demands
            // that future write transactions modifying this key cannot commit
            // prior to this read tx. This is achieved via updating the
            // last_read_ts field of the cc entry, which pushes future
            // transactions' commit timestamps larger than the largest commit
            // timestamp of all read transactions that have released the read
            // lock on the key.
            PostProcessResult &conflicting_txs = hd_res->Value();

            if (gap_ts > 0)
            {
                assert("Unsupported phantom reads.");
            }

            NonBlockingLock *key_lock = cc_entry.GetKeyLock();
            if (key_ts > 0)
            {
                shard_->UpdateLastReadTs(commit_ts);

                // Using locking protocol, this never happens.
                if (key_lock != nullptr && key_lock->HasWriteLock() &&
                    key_lock->WriteLockTx() != txn)
                {
                    DLOG(INFO)
                        << "txn: " << req.Txn()
                        << " validate fail on key: " << cc_entry.KeyString()
                        << ", conflicting write lock txn: "
                        << key_lock->WriteLockTx()
                        << ", table: " << table_name_.String();
                    int64_t ng_term =
                        Sharder::Instance().LeaderTerm(req.NodeGroupId());
                    shard_->CheckRecoverTx(
                        key_lock->WriteLockTx(), req.NodeGroupId(), ng_term);
                    conflicting_txs.IncrConflictingTx();

                    DLOG_IF(INFO, TRACE_OCC_ERR)
                        << "PostReadCc, occ_err, txn:" << txn
                        << " ,cce: " << &cc_entry
                        << " ,key conflict tx: " << key_lock->WriteLockTx();
                }
            }

            if (req.GetPostReadType() == PostReadType::Release)
            {
                ReleaseCceLock(key_lock,
                               &cc_entry,
                               txn,
                               req.NodeGroupId(),
                               LockType::NoLock,
                               recycle_lock);
            }
            else
            {
                DecrReadIntent(key_lock, &cc_entry, txn, req.NodeGroupId());
            }

            if (conflicting_txs.Size() > 0)
            {
                // Does not perform tx negotiations so far.
                DLOG(INFO)
                    << "Set CcErrorCode::VALIDATION_FAILED_FOR_CONFILICTED_TXS "
                       "to PostRead sent from txn: "
                    << req.Txn();
                hd_res->SetError(
                    CcErrorCode::VALIDATION_FAILED_FOR_CONFILICTED_TXS);
            }
            else
            {
                hd_res->SetFinished();
            }
        }

        return true;
    }

    bool Execute(ReadCc &req) override
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

        auto hd_res = req.Result();
        CODE_FAULT_INJECTOR("term_TemplateCcMap_Execute_ReadCc", {
            if (strstr(typeid(*this).name(), "CatalogCcMap") == nullptr)
            {
                LOG(INFO) << "FaultInject  term_TemplateCcMap_Execute_ReadCc";
                hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                return true;
            }
        });

        // To avoid lock the record and simulate message missed.
        CODE_FAULT_INJECTOR("remote_read_msg_missed", {
            if (req.CcePtr() != nullptr)
            {
                LOG(INFO) << "FaultInject  remote_read_msg_missed"
                          << "txID: " << req.Txn();
                hd_res->SetFinished();

                return true;
            }
        });

        // To avoid lock the record and simulate term changed.
        CODE_FAULT_INJECTOR("block_req_term_changed", {
            if (req.CcePtr() != nullptr)
            {
                LOG(INFO) << "FaultInject  block_req_term_changed";
                hd_res->SetFinished();
                return true;
            }
        });

        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = -1;
        if (req.IsInRecovering())
        {
            ng_term = Sharder::Instance().CandidateLeaderTerm(ng_id);
        }
        else
        {
            ng_term = Sharder::Instance().LeaderTerm(ng_id);
            ng_term = std::max(ng_term, Sharder::Instance().StandbyNodeTerm());
        }

        if (ng_term < 0)
        {
            LOG(INFO) << "ReadCc, node_group(#" << ng_id
                      << ") term < 0, tx:" << req.Txn();
            hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        if (req.SchemaVersion() != 0 && req.SchemaVersion() != schema_ts_)
        {
            hd_res->SetError(CcErrorCode::REQUESTED_TABLE_SCHEMA_MISMATCH);
            return true;
        }

        IsolationLevel iso_lvl = req.Isolation();
        CcProtocol cc_proto = req.Protocol();
        bool is_read_snapshot;
        CcOperation cc_op;
        if (table_name_.Type() == TableType::Secondary)
        {
            assert(false);
            cc_op = CcOperation::ReadSkIndex;
            is_read_snapshot = (iso_lvl == IsolationLevel::Snapshot);
        }
        else if (table_name_.Type() == TableType::UniqueSecondary)
        {
            cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::ReadSkIndex;
            is_read_snapshot =
                (iso_lvl == IsolationLevel::Snapshot && !req.IsForWrite());
        }
        else
        {
            cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::Read;
            is_read_snapshot =
                (iso_lvl == IsolationLevel::Snapshot && !req.IsForWrite());
        }

        CcEntryAddr &cce_addr = hd_res->Value().cce_addr_;
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce = nullptr;
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp = nullptr;
        const KeyT *look_key = nullptr;
        KeyT decoded_key;

        if (req.Type() == ReadType::Inside)
        {
            LockType acquired_lock = LockType::NoLock;
            CcErrorCode err_code = CcErrorCode::NO_ERROR;

            if (req.CcePtr() != nullptr)
            {
                // The request was blocked before. This is execution resumption
                // after the request is unblocked. The read lock/intention must
                // have been acquired.
                cce = static_cast<
                    CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                    req.CcePtr());
                ccp = static_cast<
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                    cce->GetCcPage());
                look_key = ccp->KeyOfEntry(cce);

                if (req.BlockedBy() == ReadCc::BlockByPostWrite)
                {
                    // Since when we are waiting for PostWrite, the ReadCc
                    // request is put into the blocking queue with ReadLock.
                    // After PostWrite finished, this ReadLock should be
                    // released.
                    cce->GetKeyLock()->ReleaseReadLock(req.Txn(), shard_);
                }
                else if (req.BlockedBy() == ReadCc::BlockByLock)
                {
                    std::tie(acquired_lock, err_code) =
                        LockHandleForResumedRequest(cce,
                                                    cce->CommitTs(),
                                                    cce->PayloadStatus(),
                                                    &req,
                                                    ng_id,
                                                    ng_term,
                                                    req.TxTerm(),
                                                    cc_op,
                                                    iso_lvl,
                                                    cc_proto,
                                                    req.ReadTimestamp(),
                                                    req.IsCoveringKeys());
                }
                else if (req.BlockedBy() == ReadCc::BlockByFetchBaseRecord ||
                         req.BlockedBy() == ReadCc::BlockByFetchArchiveRecord)
                {
                    cce->GetKeyGapLockAndExtraData()->ReleasePin();
                    cce->RecycleKeyLock(*shard_);

                    if (req.BlockedBy() == ReadCc::BlockByFetchBaseRecord)
                    {
                        if (req.Isolation() == IsolationLevel::Snapshot)
                        {
                            // MVCC update last_read_ts_ of lastest ccentry to
                            // tell later writer's commit_ts must be higher than
                            // MVCC reader's ts. Or it will break the REPEATABLE
                            // READ since the next MVCC read in the same
                            // transaction will read the new updated ccentry.
                            shard_->UpdateLastReadTs(req.ReadTimestamp());
                        }
                        std::tie(acquired_lock, err_code) =
                            AcquireCceKeyLock(cce,
                                              cce->CommitTs(),
                                              ccp,
                                              cce->PayloadStatus(),
                                              &req,
                                              ng_id,
                                              ng_term,
                                              req.TxTerm(),
                                              cc_op,
                                              iso_lvl,
                                              cc_proto,
                                              req.ReadTimestamp(),
                                              req.IsCoveringKeys());

                        cce_addr.SetCceLock(
                            reinterpret_cast<uint64_t>(
                                cce->GetKeyGapLockAndExtraData()),
                            ng_term,
                            req.NodeGroupId(),
                            shard_->LocalCoreId());
                    }
                }
                else
                {
                    assert(false);
                }

                req.SetBlockType(ReadCc::NotBlocked);
            }
            else
            {
                if (req.Key() != nullptr)
                {
                    look_key = static_cast<const KeyT *>(req.Key());
                }
                else
                {
                    assert(req.KeyBlob() != nullptr);
                    size_t offset = 0;
                    decoded_key.Deserialize(
                        req.KeyBlob()->data(), offset, KeySchema());
                    look_key = &decoded_key;
                }

                CODE_FAULT_INJECTOR("remote_read_msg_missed", {
                    LOG(INFO) << "FaultInject  remote_read_msg_missed"
                              << "txID: " << req.Txn();
                    if (!req.IsLocal())
                    {
                        remote::RemoteRead &remote_req =
                            static_cast<remote::RemoteRead &>(req);
                        remote_req.Acknowledge();
                    }

                    return false;
                });
                if (RangePartitioned)
                {
                    Iterator it = Find(*look_key);
                    cce = it->second;
                    ccp = it.GetPage();
                    if (cce == nullptr ||
                        cce->PayloadStatus() == RecordStatus::Unknown)
                    {
                        if (Type() == TableType::Primary ||
                            Type() == TableType::UniqueSecondary)
                        {
                            RangeSliceOpStatus pin_status;
                            RangeSliceId slice_id =
                                shard_->local_shards_.PinRangeSlice(
                                    table_name_,
                                    cc_ng_id_,
                                    ng_term,
                                    KeySchema(),
                                    RecordSchema(),
                                    schema_ts_,
                                    table_schema_->GetKVCatalogInfo(),
                                    req.PartitionId(),
                                    *look_key,
                                    true,
                                    &req,
                                    shard_,
                                    pin_status,
                                    false,
                                    0,
                                    txservice_enable_key_cache &&
                                        Type() == TableType::Primary,
                                    req.PointReadOnCacheMiss());

                            if (metrics::enable_cache_hit_rate &&
                                !req.CacheHitMissCollected())
                            {
                                if (pin_status ==
                                        RangeSliceOpStatus::Successful ||
                                    pin_status ==
                                        RangeSliceOpStatus::KeyNotExists)
                                {
                                    shard_->CollectCacheHit();
                                }
                                else
                                {
                                    shard_->CollectCacheMiss();
                                }

                                req.SetCacheHitMissCollected();
                            }

                            if (pin_status == RangeSliceOpStatus::Successful ||
                                pin_status == RangeSliceOpStatus::KeyNotExists)
                            {
                                if (cc_op != CcOperation::ReadForWrite)
                                {
                                    if (pin_status ==
                                        RangeSliceOpStatus::Successful)
                                    {
                                        slice_id.Unpin();
                                    }
                                    // If the read is not for update, there's no
                                    // need for any lock on it.
                                    hd_res->Value().ts_ = 1;
                                    hd_res->Value().rec_status_ =
                                        RecordStatus::Deleted;
                                    hd_res->SetFinished();
                                    return true;
                                }
                                // Inserting a new key to ccm, add to key cache
                                // too
                                if (txservice_enable_key_cache &&
                                    table_name_.IsBase())
                                {
                                    TemplateStoreRange<KeyT> *range =
                                        static_cast<TemplateStoreRange<KeyT> *>(
                                            slice_id.Range());
                                    auto res = range->AddKey(*look_key,
                                                             shard_->core_id_,
                                                             slice_id.Slice());
                                    if (res == RangeSliceOpStatus::Error)
                                    {
                                        // Key cache is invalidated due to
                                        // collision. Try to add the cached
                                        // slices back to the key cache.
                                        range->InitKeyCache(shard_,
                                                            &table_name_,
                                                            cc_ng_id_,
                                                            ng_term);
                                    }
                                    else if (res == RangeSliceOpStatus::Retry)
                                    {
                                        if (pin_status ==
                                            RangeSliceOpStatus::Successful)
                                        {
                                            slice_id.Unpin();
                                        }

                                        shard_->Enqueue(shard_->LocalCoreId(),
                                                        &req);
                                        return false;
                                    }
                                }
                                if (cce == nullptr)
                                {
                                    Iterator it = FindEmplace(*look_key);
                                    cce = it->second;
                                    if (cce == nullptr)
                                    {
                                        if (pin_status ==
                                            RangeSliceOpStatus::Successful)
                                        {
                                            slice_id.Unpin();
                                        }
                                        shard_->EnqueueWaitListIfMemoryFull(
                                            &req);
                                        return false;
                                    }
                                    ccp = it.GetPage();
                                }
                                // Key does not exist after quering slice info.
                                // Mark the key as deleted.
                                cce->SetCommitTsPayloadStatus(
                                    1U, RecordStatus::Deleted);
                                cce->SetCkptTs(1U);
                                cce->entry_info_.SetDataStoreSize(0);

                                if (pin_status ==
                                    RangeSliceOpStatus::Successful)
                                {
                                    // The slice is unpinned immediately. This
                                    // is because the prior pin operation brings
                                    // all records in the slice into memory,
                                    // including the target record sharded to
                                    // this core. Since cache cleaning is done
                                    // by the tx processor associated with this
                                    // core, the target record cannot be kicked
                                    // out before this read request finishes.
                                    slice_id.Unpin();
                                }
                            }
                            else if (pin_status ==
                                     RangeSliceOpStatus::BlockedOnLoad)
                            {
                                return false;
                            }
                            else if (pin_status == RangeSliceOpStatus::Retry)
                            {
                                shard_->Enqueue(shard_->LocalCoreId(), &req);
                                return false;
                            }
                            else if (pin_status == RangeSliceOpStatus::Delay)
                            {
                                if (slice_id.Range()->HasLock())
                                {
                                    hd_res->SetError(
                                        CcErrorCode::OUT_OF_MEMORY);
                                    return true;
                                }
                                else
                                {
                                    shard_->Enqueue(shard_->LocalCoreId(),
                                                    &req);
                                    return false;
                                }
                            }
                            else if (pin_status ==
                                     RangeSliceOpStatus::NotPinned)
                            {
                                assert(req.PointReadOnCacheMiss());
                                if (cce == nullptr)
                                {
                                    Iterator it = FindEmplace(*look_key);
                                    cce = it->second;
                                    if (cce == nullptr)
                                    {
                                        shard_->EnqueueWaitListIfMemoryFull(
                                            &req);
                                        return false;
                                    }
                                    ccp = it.GetPage();
                                }

                                // Create key lock and extra struct for the cce.
                                // Fetch record will pin the cce to prevent it
                                // from being recycled before fetch record
                                // returns.
                                cce->GetOrCreateKeyLock(shard_, this, ccp);
                                auto fetch_ret_status = shard_->FetchRecord(
                                    this->table_name_,
                                    this->table_schema_,
                                    TxKey(look_key),
                                    cce,
                                    this->cc_ng_id_,
                                    ng_term,
                                    &req,
                                    req.PartitionId(),
                                    false,
                                    0,
                                    is_read_snapshot ? req.ReadTimestamp() : 0,
                                    false);

                                if (fetch_ret_status ==
                                    store::DataStoreHandler::DataStoreOpStatus::
                                        Retry)
                                {
                                    // Yield and retry
                                    shard_->Enqueue(shard_->core_id_, &req);
                                }
                                else
                                {
                                    req.SetBlockType(
                                        ReadCc::BlockByFetchBaseRecord);
                                    req.SetCcePtr(cce);
                                }

                                return false;
                            }
                            else
                            {
                                // If the pin operation returns an error, the
                                // data store is inaccessible.
                                hd_res->SetError(
                                    CcErrorCode::PIN_RANGE_SLICE_FAILED);
                                return true;
                            }
                        }
                        else
                        {
                            assert(Type() == TableType::Catalog);
                        }
                    }
                    // collect metrics: slice cache hits
                    if (metrics::enable_cache_hit_rate &&
                        !req.CacheHitMissCollected())
                    {
                        shard_->CollectCacheHit();
                        req.SetCacheHitMissCollected();
                    }
                }
                else
                {
                    Iterator it =
                        FindEmplace(*look_key, false, !req.IsForWrite());
                    cce = it->second;
                    ccp = it.GetPage();
                    // The read request accesses a new key not in the cc map.
                    // But the cc map is full and cannot allocates a new entry.
                    if (cce == nullptr)
                    {
                        shard_->EnqueueWaitListIfMemoryFull(&req);
                        return false;
                    }
                    // if ccm contains all the ccentries, then unknown status
                    // means that we can skip accessing kv store and return
                    // deleted status directly.
                    if (ccm_has_full_entries_ &&
                        cce->PayloadStatus() == RecordStatus::Unknown)
                    {
                        cce->SetCommitTsPayloadStatus(1U,
                                                      RecordStatus::Deleted);
                        cce->SetCkptTs(1U);
                    }

                    if (metrics::enable_cache_hit_rate &&
                        !req.CacheHitMissCollected())
                    {
                        if (cce->PayloadStatus() == RecordStatus::Unknown)
                        {
                            shard_->CollectCacheMiss();
                        }
                        else
                        {
                            shard_->CollectCacheHit();
                        }
                        req.SetCacheHitMissCollected();
                    }

                    if (cce->PayloadStatus() == RecordStatus::Unknown)
                    {
                        // Create key lock and extra struct for the cce. Fetch
                        // record will pin the cce to prevent it from being
                        // recycled before fetch record returns.
                        cce->GetOrCreateKeyLock(shard_, this, ccp);
                        auto fetch_ret_status = shard_->FetchRecord(
                            this->table_name_,
                            this->table_schema_,
                            TxKey(look_key),
                            cce,
                            this->cc_ng_id_,
                            ng_term,
                            &req,
                            req.PartitionId(),
                            false,
                            0U,
                            is_read_snapshot ? req.ReadTimestamp() : 0,
                            false);
                        if (fetch_ret_status ==
                            store::DataStoreHandler::DataStoreOpStatus::Retry)
                        {
                            shard_->Enqueue(shard_->core_id_, &req);
                        }
                        else
                        {
                            req.SetBlockType(ReadCc::BlockByFetchBaseRecord);
                            req.SetCcePtr(cce);
                        }

                        return false;
                    }
                }

                req.SetCcePtr(cce);
                CODE_FAULT_INJECTOR("remote_read_msg_missed", {
                    LOG(INFO) << "FaultInject  remote_read_msg_missed"
                              << "txID: " << req.Txn();
                    if (!req.IsLocal())
                    {
                        remote::RemoteRead &remote_req =
                            static_cast<remote::RemoteRead &>(req);
                        remote_req.Acknowledge();
                    }

                    return false;
                });
                if (req.Isolation() == IsolationLevel::Snapshot)
                {
                    // MVCC update last_read_ts_ of lastest ccentry to tell
                    // later writer's commit_ts must be higher than MVCC
                    // reader's ts. Or it will break the REPEATABLE READ since
                    // the next MVCC read in the same transaction will read the
                    // new updated ccentry.
                    shard_->UpdateLastReadTs(req.ReadTimestamp());
                }
                std::tie(acquired_lock, err_code) =
                    AcquireCceKeyLock(cce,
                                      cce->CommitTs(),
                                      ccp,
                                      cce->PayloadStatus(),
                                      &req,
                                      ng_id,
                                      ng_term,
                                      req.TxTerm(),
                                      cc_op,
                                      iso_lvl,
                                      cc_proto,
                                      req.ReadTimestamp(),
                                      req.IsCoveringKeys());

                cce_addr.SetCceLock(reinterpret_cast<uint64_t>(
                                        cce->GetKeyGapLockAndExtraData()),
                                    ng_term,
                                    req.NodeGroupId(),
                                    shard_->LocalCoreId());
            }

            // After acquiring lock
            switch (err_code)
            {
            case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
            {
                req.SetBlockType(ReadCc::BlockByPostWrite);
                return false;
            }
            case CcErrorCode::NO_ERROR:
            {
                hd_res->Value().lock_type_ = acquired_lock;
                break;
            }
            case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
            {
                // If the read request comes from a remote node, sends
                // acknowledgement to the sender when the request is
                // blocked.
                if (!req.IsLocal())
                {
                    remote::RemoteRead &remote_req =
                        static_cast<remote::RemoteRead &>(req);
                    remote_req.Acknowledge();
                }
                req.SetBlockType(ReadCc::BlockByLock);
                // ReadLock fail should stop the execution of current
                // ReadCc request since it's already in blocking queue.
                return false;
            }
            default:
            {
                // lock confilct: back off and retry.
                req.Result()->SetError(err_code);
                return true;
            }
            }  //-- end: switch

        }  //-- end: read insde
        else
        {
            // For the read-outside request whose goal is to bring in a
            // record from the data store for caching, the cc entry's
            // address is known.
            assert(req.NodeGroupId() == cce_addr.NodeGroupId());
            assert(cce_addr.ExtractCce() != nullptr);
            cce = reinterpret_cast<
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                cce_addr.ExtractCce());
        }  //-- end: read outside

        // The request brings in the record to the cc entry for caching if
        // cce->payload_status_ is Unknown which means it doesn't override by
        // another transaction yet.
        if (req.Type() != ReadType::Inside)
        {
            RecordStatus tmp_payload_status = RecordStatus::Normal;
            std::unique_ptr<ValueT> tmp_payload = std::make_unique<ValueT>();
            if (req.Type() == ReadType::OutsideNormal)
            {
                tmp_payload_status = RecordStatus::Normal;
                if (req.Record() != nullptr)
                {
                    ValueT *typed_rec = static_cast<ValueT *>(req.Record());
                    tmp_payload = std::make_unique<ValueT>(*typed_rec);
                }
                else
                {
                    assert(req.RecordBlob() != nullptr);
                    size_t offset = 0;
                    tmp_payload = std::make_unique<ValueT>();
                    tmp_payload->Deserialize(req.RecordBlob()->data(), offset);
                }
            }
            else
            {
                // set tomb ccentry to prevent access data store again.
                tmp_payload_status = RecordStatus::Deleted;
            }

            if (cce->PayloadStatus() == RecordStatus::Unknown)
            {
                cce->payload_.PassInCurrentPayload(std::move(tmp_payload));
                cce->SetCommitTsPayloadStatus(req.ReadTimestamp(),
                                              tmp_payload_status);
            }
            else if (shard_->EnableMvcc() &&
                     cce->CommitTs() > req.ReadTimestamp())
            {
                // Trying to insert the record to backfill into archives is
                // needed, because the entry may be created when executing
                // "ReplayLogCc".
                cce->AddArchiveRecord(std::move(tmp_payload),
                                      tmp_payload_status,
                                      req.ReadTimestamp());
            }
            // Updates the ckpt timestamp such that it is no smaller than the
            // backfill version.
            cce->SetCkptTs(req.ReadTimestamp());

            // Refill mvcc archives
            if (shard_->EnableMvcc() &&
                (req.Type() == ReadType::OutsideNormal ||
                 req.Type() == ReadType::OutsideDeleted) &&
                req.ArchivesPtr() != nullptr && req.ArchivesPtr()->size() > 0)
            {
                cce->AddArchiveRecords(*req.ArchivesPtr());
            }
        }

        if (is_read_snapshot && VersionedRecord)
        {
            assert(req.Type() == ReadType::Inside);

            VersionResultRecord<ValueT> v_rec;
            cce->MvccGet(req.ReadTimestamp(), v_rec);
            if (v_rec.payload_status_ == RecordStatus::Normal)
            {
                if (req.Record() != nullptr)
                {
                    ValueT *typed_rec = static_cast<ValueT *>(req.Record());
                    *typed_rec = *(v_rec.payload_ptr_);
                }
                else
                {
                    assert(req.RecordBlob() != nullptr);
                    v_rec.payload_ptr_->Serialize(*req.RecordBlob());
                }
            }

            if (metrics::enable_cache_hit_rate && !req.CacheHitMissCollected())
            {
                if (v_rec.payload_status_ == RecordStatus::Unknown ||
                    v_rec.payload_status_ == RecordStatus::VersionUnknown ||
                    v_rec.payload_status_ == RecordStatus::BaseVersionMiss)
                {
                    shard_->CollectCacheMiss();
                }
                req.SetCacheHitMissCollected();
            }
            assert(v_rec.payload_status_ != RecordStatus::Unknown);

            if (v_rec.payload_status_ == RecordStatus::VersionUnknown ||
                v_rec.payload_status_ == RecordStatus::BaseVersionMiss ||
                v_rec.payload_status_ == RecordStatus::ArchiveVersionMiss)
            {
                cce->GetOrCreateKeyLock(shard_, this, ccp);
                auto fetch_ret_status = shard_->FetchRecord(
                    this->table_name_,
                    this->table_schema_,
                    TxKey(look_key),
                    cce,
                    this->cc_ng_id_,
                    ng_term,
                    &req,
                    req.PartitionId(),
                    false,
                    0U,
                    req.ReadTimestamp(),
                    v_rec.payload_status_ == RecordStatus::ArchiveVersionMiss);

                assert(fetch_ret_status ==
                       store::DataStoreHandler::DataStoreOpStatus::Success);
                (void) fetch_ret_status;
                req.SetBlockType(ReadCc::BlockByFetchArchiveRecord);
                req.SetCcePtr(cce);

                return false;
            }

            hd_res->Value().ts_ = v_rec.commit_ts_;
            hd_res->Value().rec_status_ = v_rec.payload_status_;
            hd_res->SetFinished();
            return true;
        }
        else if (cce->PayloadStatus() == RecordStatus::Normal &&
                 (req.Type() == ReadType::Inside || cce->CommitTs() > 1))
        {
            // Copies the newest committed payload to the read result, if (1)
            // this is a read request that starts concurrency control for
            // the input key (i.e., read inside), or (2) this is a read request
            // that brings in the record from the data store for caching, but
            // the key has been updated by another committed tx since the first
            // read request.
            // TODO: TxExecution and runtime also use this new value as read
            // result to avoid future PostRead abort.

            if (req.Isolation() == IsolationLevel::ReadCommitted &&
                cce->CommitTs() > 0 && cce->CommitTs() < req.ReadTimestamp())
            {
                // When backtracking the content of primary key record according
                // to the secondary index key, if the commit_ts of this
                // primary key record is smaller than the commit_ts of the
                // secondary index key("req.ReadTimestamp()"), it means that the
                // current primary key has not been updated and there must be a
                // PostWriteCc request waiting to be executed. So, this read
                // should wait for the PostWriteCc completed.
                req.SetBlockType(ReadCc::BlockByPostWrite);
                NonBlockingLock *key_lock = cce->GetKeyLock();
                assert(key_lock != nullptr && key_lock->HasWriteLock() &&
                       key_lock->WriteLockTx() != req.Txn());
                // Put the request to top of key lock's blocking queue with
                // acquring readlock. And then should release the readlock
                // before handling this requst when PostWriteCc finished.
                key_lock->InsertBlockingQueue(&req, LockType::ReadLock);
                shard_->CheckRecoverTx(key_lock->WriteLockTx(), ng_id, ng_term);

                // After inserting to blocking queue, the execution of current
                // ReadCc request should stop.
                return false;
            }
            else
            {
                // safe to read the record
                if (req.Record() != nullptr)
                {
                    ValueT *typed_rec = static_cast<ValueT *>(req.Record());
                    *typed_rec = *(cce->payload_.cur_payload_);
                }
                else
                {
                    assert(req.RecordBlob() != nullptr);
                    cce->payload_.cur_payload_->Serialize(*req.RecordBlob());
                }
            }
        }

        hd_res->Value().ts_ = cce->CommitTs();
        hd_res->Value().rec_status_ = cce->PayloadStatus();
        hd_res->SetFinished();

        return true;
    }  // namespace txservice

    bool Execute(remote::RemoteReadOutside &req) override
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

        if (table_name_.Type() == TableType::Secondary ||
            table_name_.Type() == TableType::UniqueSecondary)
        {
            assert(false);
            return true;
        }

        const CcEntryAddr &cce_addr = req.cce_addr_;
        if (!Sharder::Instance().CheckLeaderTerm(cce_addr.NodeGroupId(),
                                                 cce_addr.Term()))
        {
            LOG(INFO) << "RemoteReadOutside, node_group(#"
                      << cce_addr.NodeGroupId()
                      << ") term < 0, tx:" << req.Txn()
                      << " ,cce: " << cce_addr.ExtractCce();
            req.Finish();
            return true;
        }

        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce =
            reinterpret_cast<
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                cce_addr.ExtractCce());

        if (cce->PayloadStatus() == RecordStatus::Unknown)
        {
            assert(cce->CommitTs() == 1);
            if (req.RecordStatus() == RecordStatus::Normal)
            {
                size_t offset = 0;
                cce->payload_.DeserializeCurrentPayload(req.rec_str_->data(),
                                                        offset);
            }
            cce->SetCommitTsPayloadStatus(req.CommitTs(), req.RecordStatus());
        }
        else if (shard_->EnableMvcc() && cce->CommitTs() > req.CommitTs())
        {
            // Trying to insert the record to backfill into archives is needed,
            // because the entry may be created when executing "ReplayLogCc".
            std::unique_ptr<ValueT> tmp_payload = std::make_unique<ValueT>();
            if (req.RecordStatus() == RecordStatus::Normal)
            {
                size_t offset = 0;
                tmp_payload->Deserialize(req.rec_str_->data(), offset);
            }
            cce->AddArchiveRecord(
                std::move(tmp_payload), req.RecordStatus(), req.CommitTs());
        }
        // Updates the ckpt timestamp such that it is no smaller than the
        // backfill version.
        cce->SetCkptTs(req.CommitTs());

        // Refill mvcc archives.
        if (shard_->EnableMvcc())
        {
            const remote::ReadOutsideRequest &tmp_req =
                req.input_msg_->read_outside_req();
            if (tmp_req.archives_size() > 0)
            {
                // de-serialize records
                std::vector<VersionTxRecord> archives;
                for (auto &vrec_msg : tmp_req.archives())
                {
                    auto &v_rec = archives.emplace_back();
                    v_rec.commit_ts_ = vrec_msg.version_ts();
                    v_rec.record_status_ =
                        remote::ToLocalType::ConvertRecordStatusType(
                            vrec_msg.rec_status());
                    v_rec.record_ = std::make_unique<ValueT>();
                    size_t offset = 0;
                    v_rec.record_->Deserialize(vrec_msg.record().data(),
                                               offset);
                }
                cce->AddArchiveRecords(archives);
            }
        }

        req.Finish();
        return true;
    }

    bool Execute(ScanCloseCc &req) override
    {
        return true;
    }

    void AddScanTuple(
        const KeyT *key,
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
        TemplateScanCache<KeyT, ValueT> *typed_cache,
        ScanType scan_type,
        uint32_t ng_id,
        int64_t ng_term,
        TxNumber txn,
        uint64_t read_ts,
        int32_t object_type,
        bool is_read_snapshot,
        bool &need_fetch_snapshot,
        bool keep_deleted = true,
        bool is_ckpt_delta = false,
        bool is_require_keys = true,
        bool is_require_recs = true)
    {
        assert(scan_type != ScanType::ScanUnknow);

        switch (scan_type)
        {
        case ScanType::ScanGap:
        {
            need_fetch_snapshot = false;
            TemplateScanTuple<KeyT, ValueT> *scan_tuple =
                typed_cache->AddScanTuple();
            ScanGap(key, cce, scan_tuple, ng_id, ng_term, object_type);
            break;
        }
        case ScanType::ScanBoth:
            ScanKey(
                key,
                cce,
                typed_cache,
                true,
                ng_id,
                ng_term,
                txn,
                read_ts,
                object_type,
                is_read_snapshot,
                need_fetch_snapshot,
                keep_deleted,
                (table_name_.Type() != TableType::Secondary) && is_ckpt_delta,
                is_require_keys,
                is_require_recs);
            break;
        case ScanType::ScanKey:
            ScanKey(
                key,
                cce,
                typed_cache,
                false,
                ng_id,
                ng_term,
                txn,
                read_ts,
                object_type,
                is_read_snapshot,
                need_fetch_snapshot,
                keep_deleted,
                (table_name_.Type() != TableType::Secondary) && is_ckpt_delta,
                is_require_keys,
                is_require_recs);
            break;
        default:
            break;
        }
    }

    bool Execute(ScanOpenBatchCc &req) override
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

        assert(false && "Unimplemented");
        return true;
    }

    bool Execute(ScanNextBatchCc &req) override
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

        uint32_t ng_id = req.NodeGroupId();
        int64_t tx_term = req.TxTerm();
        bool is_standby_tx = IsStandbyTx(tx_term);
        int64_t ng_term;
        if (is_standby_tx)
        {
            ng_term = Sharder::Instance().StandbyNodeTerm();
        }
        else
        {
            ng_term = Sharder::Instance().LeaderTerm(ng_id);
        }

        if (ng_term < 0 || ng_term != req.NodeGroupTerm())
        {
            return req.SetError(shard_->core_id_, CcErrorCode::NG_TERM_CHANGED);
        }

        if (req.ShardIsDrained(shard_->core_id_))
        {
            return req.SetFinish(shard_->core_id_);
        }

        if (req.IsWaitForFetchBucket(shard_->core_id_))
        {
            return req.SetFinish(shard_->core_id_);
        }

        IsolationLevel iso_lvl = req.Isolation();
        CcProtocol cc_proto = req.Protocol();
        CcOperation cc_op;
        bool is_read_snapshot;
        bool need_fetch_snapshot = false;
        if (table_name_.Type() == TableType::Secondary ||
            table_name_.Type() == TableType::UniqueSecondary)
        {
            cc_op = CcOperation::ReadSkIndex;
            is_read_snapshot = (iso_lvl == IsolationLevel::Snapshot);
        }
        else
        {
            cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::Read;
            is_read_snapshot =
                (iso_lvl == IsolationLevel::Snapshot && !req.IsForWrite());
        }

        const TxKey &start_key = req.StartKey(shard_->core_id_);
        absl::flat_hash_map<uint16_t, bool> &bucket_ids =
            req.BucketIds(shard_->core_id_);
        assert(!bucket_ids.empty());

        auto filter_bucket_lambda = [this, &bucket_ids](size_t hash_code)
        {
            // return hash_code == SIZE_MAX;
            uint16_t bucket_id =
                Sharder::Instance().MapKeyHashToBucketId(hash_code);
            return bucket_ids.count(bucket_id) > 0;
        };

        uint32_t shard_code = (ng_id << 10) + shard_->core_id_;
        TemplateScanCache<KeyT, ValueT> *typed_cache =
            static_cast<TemplateScanCache<KeyT, ValueT> *>(
                req.GetLocalMemoryCache(shard_code));
        typed_cache->SetCacheCapacity(bucket_ids.size() * 16);

        ScanDirection direction = typed_cache->Scanner()->Direction();
        Iterator scan_ccm_it;
        Iterator end_it = End();
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *prior_cce;
        auto [cce_lock_addr, end_lock_addr] =
            req.BlockingCceLockAddr(shard_->core_id_);
        if (cce_lock_addr == 0 &&
            table_name_.Type() != TableType::RangePartition &&
            Sharder::Instance().GetDataStoreHandler() != nullptr)
        {
            // first enter, send async request to kv store
            shard_->FetchBucketData(
                &table_name_,
                table_schema_,
                ng_id,
                ng_term,
                shard_,
                true,
                bucket_ids,
                req.pushdown_cond_,
                std::string_view(start_key.Data(), start_key.Size()),
                start_key.Type(),
                req.StartKeyInlcuisve(shard_->core_id_),
                std::string_view(req.EndKey().Data(), req.EndKey().Size()),
                req.EndKey().Type(),
                req.EndKeyInclusive(),
                ScanNextBatchCc::KvBucketBatchSize,
                &req,
                &BackfillForScanNextBatch<KeyT, ValueT, VersionedRecord>);
        }

        if (cce_lock_addr != 0)
        {
            KeyGapLockAndExtraData *lock =
                reinterpret_cast<KeyGapLockAndExtraData *>(cce_lock_addr);
            assert(lock != nullptr && lock->GetCcEntry() != nullptr);
            prior_cce = static_cast<
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                lock->GetCcEntry());
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                static_cast<
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                    prior_cce->GetCcPage());
            assert(ccp != nullptr);

            if (end_lock_addr != 0)
            {
                KeyGapLockAndExtraData *end_lock =
                    reinterpret_cast<KeyGapLockAndExtraData *>(end_lock_addr);
                assert(end_lock != nullptr &&
                       end_lock->GetCcEntry() != nullptr);
                auto end_cce = static_cast<
                    CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                    end_lock->GetCcEntry());

                assert(end_cce->GetCcPage() != nullptr);

                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>
                    *end_ccp = static_cast<
                        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>
                            *>(end_cce->GetCcPage());

                // Release read intent lock
                end_it = Iterator(end_cce, end_ccp, &neg_inf_);
                ReleaseCceLock(end_cce->GetKeyLock(),
                               end_cce,
                               req.Txn(),
                               ng_id,
                               LockType::ReadIntent);
            }

            scan_ccm_it = Iterator(prior_cce, ccp, &neg_inf_);
            const KeyT *prior_cce_key = scan_ccm_it->first;

            auto [blocking_type, scan_type] =
                req.BlockingPair(shard_->core_id_);

            if (blocking_type == ScanBlockingType::NoBlocking)
            {
                // Release read intent lock
                ReleaseCceLock(prior_cce->GetKeyLock(),
                               prior_cce,
                               req.Txn(),
                               ng_id,
                               LockType::ReadIntent);
            }
            else
            {
                if (blocking_type == ScanBlockingType::BlockOnFuture)
                {
                    prior_cce->GetKeyLock()->ReleaseReadLock(req.Txn(), shard_);
                }

                // Lock has been acquired, UpsertLockHoldingTx
                auto lock_pair =
                    LockHandleForResumedRequest(prior_cce,
                                                prior_cce->CommitTs(),
                                                prior_cce->PayloadStatus(),
                                                &req,
                                                ng_id,
                                                ng_term,
                                                tx_term,
                                                cc_op,
                                                iso_lvl,
                                                cc_proto,
                                                req.ReadTimestamp(),
                                                req.IsCoveringKeys());
                if (lock_pair.second != CcErrorCode::NO_ERROR)
                {
                    assert(lock_pair.second ==
                           CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT);
                    return req.SetError(shard_->core_id_, lock_pair.second);
                }

                int32_t object_type = GetObjectType(prior_cce);

                AddScanTuple(prior_cce_key,
                             prior_cce,
                             typed_cache,
                             scan_type,
                             ng_id,
                             ng_term,
                             req.Txn(),
                             req.ReadTimestamp(),
                             object_type,
                             is_read_snapshot,
                             need_fetch_snapshot,
                             true,
                             req.is_ckpt_delta_,
                             req.is_require_keys_,
                             req.is_require_recs_);
            }
        }
        else
        {
            const KeyT *look_key = start_key.GetKey<KeyT>();
            const KeyT *end_key = req.end_key_.GetKey<KeyT>();

            std::pair<Iterator, ScanType> start_pair = ForwardScanStart(
                *look_key, req.StartKeyInlcuisve(shard_->core_id_));
            scan_ccm_it = start_pair.first;
            if (start_pair.second == ScanType::ScanGap)
            {
                ++scan_ccm_it;
            }

            if (end_key == nullptr || end_key->Type() == KeyType::PositiveInf)
            {
                end_it = End();
            }
            else
            {
                std::pair<Iterator, ScanType> end_pair =
                    ForwardScanStart(*end_key, req.end_key_inclusive_);
                end_it = end_pair.first;
                if (end_pair.second == ScanType::ScanGap)
                {
                    ++end_it;
                }
            }

            assert(typed_cache->Size() == 0);
            // typed_cache->Reset();
        }

        bool scan_finished = false;

        if (direction == ScanDirection::Forward)
        {
            // ++scan_ccm_it;
            size_t scan_cnt = 0;
            for (; scan_ccm_it != end_it && !typed_cache->Full() &&
                   scan_cnt < ScanNextBatchCc::ScanBatchSize;
                 ++scan_ccm_it, ++scan_cnt)
            {
                const KeyT *key = scan_ccm_it->first;
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce =
                    scan_ccm_it->second;
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                    scan_ccm_it.GetPage();

                if (req.is_ckpt_delta_ && cce->IsPersistent())
                {
                    // If this is a scan for modified records since last
                    // checkpoint, skips those that have been checkpointed.
                    continue;
                }

                if (!filter_bucket_lambda(key->Hash()) ||
                    !FilterRecord(key,
                                  cce,
                                  req.GetRedisObjectType(),
                                  req.GetRedisScanPattern()))
                {
                    continue;
                }

                int32_t object_type = GetObjectType(cce);
                RecordStatus status =
                    cce->PayloadStatus() == RecordStatus::Unknown
                        ? RecordStatus::Deleted
                        : cce->PayloadStatus();
                auto lock_pair = AcquireCceKeyLock(cce,
                                                   cce->CommitTs(),
                                                   ccp,
                                                   status,
                                                   &req,
                                                   ng_id,
                                                   ng_term,
                                                   tx_term,
                                                   cc_op,
                                                   iso_lvl,
                                                   cc_proto,
                                                   req.ReadTimestamp(),
                                                   req.IsCoveringKeys());

                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    uint64_t end_it_lock_addr = 0;
                    if (end_it != End())
                    {
                        AcquireReadIntent(end_it->second,
                                          end_it.GetPage(),
                                          req.Txn(),
                                          tx_term);
                        end_it_lock_addr = reinterpret_cast<uint64_t>(
                            end_it->second->GetLockAddr());
                    }

                    req.SetBlockingInfo(
                        shard_->core_id_,
                        reinterpret_cast<uint64_t>(cce->GetLockAddr()),
                        end_it_lock_addr,
                        ScanType::ScanBoth,
                        ScanBlockingType::BlockOnFuture);
                    return false;
                }
                case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
                {
                    uint64_t end_it_lock_addr = 0;
                    if (end_it != End())
                    {
                        AcquireReadIntent(end_it->second,
                                          end_it.GetPage(),
                                          req.Txn(),
                                          tx_term);
                        end_it_lock_addr = reinterpret_cast<uint64_t>(
                            end_it->second->GetLockAddr());
                    }

                    req.SetBlockingInfo(
                        shard_->core_id_,
                        reinterpret_cast<uint64_t>(cce->GetLockAddr()),
                        end_it_lock_addr,
                        ScanType::ScanBoth,
                        ScanBlockingType::BlockOnLock);
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    return false;
                }
                default:
                {
                    // lock confilct: back off and retry.
                    return req.SetError(shard_->core_id_, lock_pair.second);
                }
                }  //-- end: switch

                AddScanTuple(key,
                             cce,
                             typed_cache,
                             ScanType::ScanBoth,
                             ng_id,
                             ng_term,
                             req.Txn(),
                             req.ReadTimestamp(),
                             object_type,
                             is_read_snapshot,
                             need_fetch_snapshot,
                             true,
                             req.is_ckpt_delta_,
                             req.is_require_keys_,
                             req.is_require_recs_);
            }
        }
        else
        {
            assert(false);
        }

        scan_finished = (scan_ccm_it == end_it);
        if (scan_finished)
        {
            req.GetBucketScanProgress()
                ->at(shard_->core_id_)
                .memory_scan_is_finished_ = true;
            return req.SetFinish(shard_->core_id_);
        }
        else
        {
            if (!typed_cache->Full())
            {
                uint64_t end_it_lock_addr = 0;
                if (end_it != End())
                {
                    AcquireReadIntent(
                        end_it->second, end_it.GetPage(), req.Txn(), tx_term);
                    end_it_lock_addr = reinterpret_cast<uint64_t>(
                        end_it->second->GetLockAddr());
                }

                AcquireReadIntent(scan_ccm_it->second,
                                  scan_ccm_it.GetPage(),
                                  req.Txn(),
                                  tx_term);

                req.SetBlockingInfo(shard_->core_id_,
                                    reinterpret_cast<uint64_t>(
                                        scan_ccm_it->second->GetLockAddr()),
                                    end_it_lock_addr,
                                    ScanType::ScanBoth,
                                    ScanBlockingType::NoBlocking);

                shard_->Enqueue(&req);
                return false;
            }

            return req.SetFinish(shard_->core_id_);
        }
    }

    void AddScanTupleMsg(
        const KeyT *key,
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
        RemoteScanCache *remote_cache,
        ScanType scan_type,
        NodeGroupId ng_id,
        int64_t ng_term,
        TxNumber txn,
        uint64_t read_ts,
        int32_t object_type,
        bool is_read_snapshot,
        bool &need_fetch_snapshot,
        bool keep_deleted = true,
        bool is_ckpt_delta = false,
        bool is_require_keys = true,
        bool is_require_recs = true)
    {
        assert(scan_type != ScanType::ScanUnknow);

        switch (scan_type)
        {
        case ScanType::ScanGap:
            need_fetch_snapshot = false;
            if (!is_ckpt_delta)
            {
                remote::ScanTuple_msg *tuple =
                    remote_cache->GetMemoryCache()->add_scan_tuple();
                remote_cache->AddHashCode(key->Hash());
                ScanGap(key, cce, tuple, ng_term, object_type);
            }
            break;
        case ScanType::ScanBoth:
            ScanKey(
                key,
                cce,
                remote_cache,
                true,
                ng_id,
                ng_term,
                txn,
                read_ts,
                object_type,
                is_read_snapshot,
                need_fetch_snapshot,
                keep_deleted,
                (table_name_.Type() != TableType::Secondary) && is_ckpt_delta,
                is_require_keys,
                is_require_recs);
            break;
        case ScanType::ScanKey:
            ScanKey(
                key,
                cce,
                remote_cache,
                false,
                ng_id,
                ng_term,
                txn,
                read_ts,
                object_type,
                is_read_snapshot,
                need_fetch_snapshot,
                keep_deleted,
                (table_name_.Type() != TableType::Secondary) && is_ckpt_delta,
                is_require_keys,
                is_require_recs);
            break;
        default:
            break;
        }
    }

    void AddScanTupleMsg(
        const KeyT *key,
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
        RemoteScanSliceCache *remote_cache,
        ScanType scan_type,
        int64_t ng_term,
        uint64_t read_ts,
        bool is_read_snapshot,
        bool &need_fetch_snapshot,
        bool keep_deleted = true,
        bool is_ckpt_delta = false,
        bool is_require_keys = true,
        bool is_require_recs = true)
    {
        assert(scan_type != ScanType::ScanUnknow);

        switch (scan_type)
        {
        case ScanType::ScanGap:
            need_fetch_snapshot = false;
            if (!is_ckpt_delta)
            {
                ScanGap(key, cce, remote_cache, ng_term);
            }
            break;
        case ScanType::ScanBoth:
            ScanKey(
                key,
                cce,
                remote_cache,
                true,
                ng_term,
                read_ts,
                is_read_snapshot,
                need_fetch_snapshot,
                keep_deleted,
                (table_name_.Type() != TableType::Secondary) && is_ckpt_delta,
                is_require_keys,
                is_require_recs);
            break;
        case ScanType::ScanKey:
            ScanKey(
                key,
                cce,
                remote_cache,
                false,
                ng_term,
                read_ts,
                is_read_snapshot,
                need_fetch_snapshot,
                keep_deleted,
                (table_name_.Type() != TableType::Secondary) && is_ckpt_delta,
                is_require_keys,
                is_require_recs);
            break;
        default:
            break;
        }
    }

    bool Execute(remote::RemoteScanOpen &req) override
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

        assert(false && "Unimplemented");
        return true;
    }

    bool Execute(remote::RemoteScanNextBatch &req) override
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

        uint32_t ng_id = req.NodeGroupId();
        int64_t tx_term = req.TxTerm();
        bool is_standby_tx = IsStandbyTx(tx_term);
        int64_t ng_term;
        if (is_standby_tx)
        {
            ng_term = Sharder::Instance().StandbyNodeTerm();
        }
        else
        {
            ng_term = Sharder::Instance().LeaderTerm(ng_id);
        }

        if (ng_term < 0 || ng_term != req.NodeGroupTerm())
        {
            return req.SetError(shard_->core_id_, CcErrorCode::NG_TERM_CHANGED);
        }

        if (req.ShardIsDrained(shard_->core_id_))
        {
            return req.SetFinish(shard_->core_id_);
        }

        if (req.IsWaitForFetchBucket(shard_->core_id_))
        {
            return req.SetFinish(shard_->core_id_);
        }

        IsolationLevel iso_lvl = req.Isolation();
        CcProtocol cc_proto = req.Protocol();
        CcOperation cc_op;
        bool is_read_snapshot;
        bool need_fetch_snapshot = false;
        if (table_name_.Type() == TableType::Secondary ||
            table_name_.Type() == TableType::UniqueSecondary)
        {
            cc_op = CcOperation::ReadSkIndex;
            is_read_snapshot = (iso_lvl == IsolationLevel::Snapshot);
        }
        else
        {
            cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::Read;
            is_read_snapshot =
                (iso_lvl == IsolationLevel::Snapshot && !req.IsForWrite());
        }

        absl::flat_hash_map<uint16_t, bool> &bucket_ids =
            req.BucketIds(shard_->core_id_);
        assert(!bucket_ids.empty());

        auto filter_bucket_lambda = [this, &bucket_ids](size_t hash_code)
        {
            uint16_t bucket_id =
                Sharder::Instance().MapKeyHashToBucketId(hash_code);
            return bucket_ids.count(bucket_id) > 0;
        };

        RemoteScanCache *remote_cache =
            req.GetRemoteScanCache(shard_->core_id_);
        remote_cache->SetCapacity(bucket_ids.size() * 16);

        Iterator scan_ccm_it;
        Iterator end_it = End();
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *prior_cce;
        auto [cce_lock_addr, end_lock_addr] =
            req.BlockingCceLockAddr(shard_->core_id_);
        TxKey start_key_owner;
        const KeyT *req_start_key = nullptr;
        const KeyT *req_end_key = nullptr;
        if (cce_lock_addr == 0)
        {
            if (shard_->core_id_ == req.BucketIds().begin()->first)
            {
                KeyType end_key_type = req.EndKeyType();
                switch (end_key_type)
                {
                case KeyType::NegativeInf:
                {
                    req_end_key = KeyT::NegativeInfinity();
                    req.SetEndKey(TxKey(req_end_key));
                    break;
                }
                case KeyType::PositiveInf:
                {
                    req_end_key = KeyT::PositiveInfinity();
                    req.SetEndKey(TxKey(req_end_key));
                    break;
                }
                case KeyType::Normal:
                {
                    std::unique_ptr<KeyT> decoded_key =
                        std::make_unique<KeyT>();
                    size_t offset = 0;
                    decoded_key->Deserialize(
                        req.EndKeyStr()->data(), offset, KeySchema());
                    req_end_key = decoded_key.get();
                    req.SetEndKey(TxKey(std::move(decoded_key)));
                    break;
                }
                default:
                    assert(false);
                }

                // Dispatch to remaining cores
                req.SetUnfinishedCoreCnt(req.scan_buckets_.size());
                for (const auto &[core_id, progress] : req.scan_buckets_)
                {
                    if (core_id != shard_->core_id_)
                    {
                        shard_->local_shards_.EnqueueCcRequest(
                            shard_->core_id_, core_id, &req);
                    }
                }
            }
            else
            {
                req_end_key = req.EndKey()->GetKey<KeyT>();
            }

            KeyType start_key_type = req.StartKeyType(shard_->core_id_);
            switch (start_key_type)
            {
            case KeyType::NegativeInf:
            {
                req_start_key = KeyT::NegativeInfinity();
                break;
            }
            case KeyType::PositiveInf:
            {
                req_start_key = KeyT::PositiveInfinity();
                break;
            }
            case KeyType::Normal:
            {
                std::unique_ptr<KeyT> decoded_key = std::make_unique<KeyT>();
                size_t offset = 0;
                decoded_key->Deserialize(
                    req.StartKeyStr(shard_->core_id_)->data(),
                    offset,
                    KeySchema());
                req_start_key = decoded_key.get();
                start_key_owner = TxKey(std::move(decoded_key));
                break;
            }
            default:
                assert(false);
            }

            if (table_name_.Type() != TableType::RangePartition &&
                Sharder::Instance().GetDataStoreHandler() != nullptr)
            {
                std::string_view req_start_key_view;
                std::string_view req_end_key_view;
                if (req_start_key->Type() == KeyType::Normal)
                {
                    req_start_key_view = std::string_view(
                        req_start_key->Data(), req_start_key->Size());
                }

                if (req_end_key->Type() == KeyType::Normal)
                {
                    req_end_key_view = std::string_view(req_end_key->Data(),
                                                        req_end_key->Size());
                }

                // first enter, send async request to kv store
                shard_->FetchBucketData(
                    &table_name_,
                    table_schema_,
                    ng_id,
                    ng_term,
                    shard_,
                    false,
                    bucket_ids,
                    req.PushdownCond(),
                    std::move(req_start_key_view),
                    req_start_key->Type(),
                    req.StartKeyInclusive(shard_->core_id_),
                    std::move(req_end_key_view),
                    req_end_key->Type(),
                    req.EndKeyInclusive(),
                    ScanNextBatchCc::KvBucketBatchSize,
                    &req,
                    &BackfillForScanNextBatch<KeyT, ValueT, VersionedRecord>);
            }
        }

        if (cce_lock_addr != 0)
        {
            KeyGapLockAndExtraData *lock =
                reinterpret_cast<KeyGapLockAndExtraData *>(cce_lock_addr);
            assert(lock != nullptr && lock->GetCcEntry() != nullptr);
            prior_cce = static_cast<
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                lock->GetCcEntry());
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                static_cast<
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                    prior_cce->GetCcPage());
            assert(ccp != nullptr);

            if (end_lock_addr != 0)
            {
                KeyGapLockAndExtraData *end_lock =
                    reinterpret_cast<KeyGapLockAndExtraData *>(end_lock_addr);
                assert(end_lock != nullptr &&
                       end_lock->GetCcEntry() != nullptr);
                auto end_cce = static_cast<
                    CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                    end_lock->GetCcEntry());
                assert(end_cce->GetCcPage() != nullptr);

                // Release read intent lock
                end_it = Iterator(end_cce, ccp, &neg_inf_);
                ReleaseCceLock(end_cce->GetKeyLock(),
                               end_cce,
                               req.Txn(),
                               ng_id,
                               LockType::ReadIntent);
            }

            scan_ccm_it = Iterator(prior_cce, ccp, &neg_inf_);
            const KeyT *prior_cce_key = scan_ccm_it->first;

            auto [blocking_type, scan_type] =
                req.BlockingPair(shard_->core_id_);

            if (blocking_type == ScanBlockingType::NoBlocking)
            {
                // Release read intent lock
                ReleaseCceLock(prior_cce->GetKeyLock(),
                               prior_cce,
                               req.Txn(),
                               ng_id,
                               LockType::ReadIntent);
            }
            else
            {
                if (blocking_type == ScanBlockingType::BlockOnFuture)
                {
                    prior_cce->GetKeyLock()->ReleaseReadLock(req.Txn(), shard_);
                }

                // Lock has been acquired, UpsertLockHoldingTx
                auto lock_pair =
                    LockHandleForResumedRequest(prior_cce,
                                                prior_cce->CommitTs(),
                                                prior_cce->PayloadStatus(),
                                                &req,
                                                ng_id,
                                                ng_term,
                                                tx_term,
                                                cc_op,
                                                iso_lvl,
                                                cc_proto,
                                                req.ReadTimestamp(),
                                                req.IsCoveringKeys());
                if (lock_pair.second != CcErrorCode::NO_ERROR)
                {
                    assert(lock_pair.second ==
                           CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT);
                    return req.SetError(shard_->core_id_, lock_pair.second);
                }

                int32_t object_type = GetObjectType(prior_cce);

                AddScanTupleMsg(prior_cce_key,
                                prior_cce,
                                remote_cache,
                                scan_type,
                                ng_id,
                                ng_term,
                                req.Txn(),
                                req.ReadTimestamp(),
                                object_type,
                                is_read_snapshot,
                                need_fetch_snapshot,
                                true,
                                req.is_ckpt_delta_,
                                req.is_require_keys_,
                                req.is_require_recs_);
            }
        }
        else
        {
            std::pair<Iterator, ScanType> start_pair = ForwardScanStart(
                *req_start_key, req.StartKeyInclusive(shard_->core_id_));
            scan_ccm_it = start_pair.first;
            if (start_pair.second == ScanType::ScanGap)
            {
                ++scan_ccm_it;
            }

            if (req_end_key == nullptr ||
                req_end_key->Type() == KeyType::PositiveInf)
            {
                end_it = End();
            }
            else
            {
                std::pair<Iterator, ScanType> end_pair =
                    ForwardScanStart(*req_end_key, req.EndKeyInclusive());
                end_it = end_pair.first;
                if (end_pair.second == ScanType::ScanGap)
                {
                    ++end_it;
                }
            }
        }

        bool scan_finished = false;
        // ++scan_ccm_it;
        size_t scan_cnt = 0;
        for (; scan_ccm_it != end_it && !remote_cache->MemoryCacheIsFull() &&
               scan_cnt < ScanNextBatchCc::ScanBatchSize;
             ++scan_ccm_it, ++scan_cnt)
        {
            const KeyT *key = scan_ccm_it->first;
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce =
                scan_ccm_it->second;
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                scan_ccm_it.GetPage();

            if (req.is_ckpt_delta_ && cce->IsPersistent())
            {
                // If this is a scan for modified records since last
                // checkpoint, skips those that have been checkpointed.
                continue;
            }

            if (!filter_bucket_lambda(key->Hash()) ||
                !FilterRecord(key,
                              cce,
                              req.GetRedisObjectType(),
                              req.GetRedisScanPattern()))
            {
                continue;
            }

            int32_t object_type = GetObjectType(cce);

            RecordStatus status = cce->PayloadStatus() == RecordStatus::Unknown
                                      ? RecordStatus::Deleted
                                      : cce->PayloadStatus();
            auto lock_pair = AcquireCceKeyLock(cce,
                                               cce->CommitTs(),
                                               ccp,
                                               status,
                                               &req,
                                               ng_id,
                                               ng_term,
                                               tx_term,
                                               cc_op,
                                               iso_lvl,
                                               cc_proto,
                                               req.ReadTimestamp(),
                                               req.IsCoveringKeys());

            switch (lock_pair.second)
            {
            case CcErrorCode::NO_ERROR:
                break;
            case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
            {
                uint64_t end_it_lock_addr = 0;
                if (end_it != End())
                {
                    AcquireReadIntent(
                        end_it->second, end_it.GetPage(), req.Txn(), tx_term);
                    end_it_lock_addr = reinterpret_cast<uint64_t>(
                        end_it->second->GetLockAddr());
                }

                req.SetBlockingInfo(
                    shard_->core_id_,
                    reinterpret_cast<uint64_t>(cce->GetLockAddr()),
                    end_it_lock_addr,
                    ScanType::ScanBoth,
                    ScanBlockingType::BlockOnFuture);
                return false;
            }
            case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
            {
                uint64_t end_it_lock_addr = 0;
                if (end_it != End())
                {
                    AcquireReadIntent(
                        end_it->second, end_it.GetPage(), req.Txn(), tx_term);
                    end_it_lock_addr = reinterpret_cast<uint64_t>(
                        end_it->second->GetLockAddr());
                }

                req.SetBlockingInfo(
                    shard_->core_id_,
                    reinterpret_cast<uint64_t>(cce->GetLockAddr()),
                    end_it_lock_addr,
                    ScanType::ScanBoth,
                    ScanBlockingType::BlockOnLock);
                // Lock fail should stop the execution of current
                // CC request since it's already in blocking queue.
                return false;
            }
            default:
            {
                // lock confilct: back off and retry.
                return req.SetError(shard_->core_id_, lock_pair.second);
            }
            }  //-- end: switch

            AddScanTupleMsg(key,
                            cce,
                            remote_cache,
                            ScanType::ScanBoth,
                            ng_id,
                            ng_term,
                            req.Txn(),
                            req.ReadTimestamp(),
                            object_type,
                            is_read_snapshot,
                            need_fetch_snapshot,
                            true,
                            req.is_ckpt_delta_,
                            req.is_require_keys_,
                            req.is_require_recs_);
        }

        scan_finished = (scan_ccm_it == end_it);
        if (scan_finished)
        {
            req.memory_is_drained_[shard_->core_id_] = true;
            return req.SetFinish(shard_->core_id_);
        }
        else
        {
            if (!remote_cache->MemoryCacheIsFull())
            {
                uint64_t end_it_lock_addr = 0;
                if (end_it != End())
                {
                    AcquireReadIntent(
                        end_it->second, end_it.GetPage(), req.Txn(), tx_term);
                    end_it_lock_addr = reinterpret_cast<uint64_t>(
                        end_it->second->GetLockAddr());
                }

                AcquireReadIntent(scan_ccm_it->second,
                                  scan_ccm_it.GetPage(),
                                  req.Txn(),
                                  tx_term);

                req.SetBlockingInfo(shard_->core_id_,
                                    reinterpret_cast<uint64_t>(
                                        scan_ccm_it->second->GetLockAddr()),
                                    end_it_lock_addr,
                                    ScanType::ScanBoth,
                                    ScanBlockingType::NoBlocking);

                shard_->Enqueue(&req);
                return false;
            }

            return req.SetFinish(shard_->core_id_);
        }

        return true;
    }

    bool Execute(ScanSliceCc &req) override
    {
        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0 ||
            (req.RangeCcNgTerm() > 0 && req.RangeCcNgTerm() != ng_term))
        {
            return req.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }

        if (req.SchemaVersion() != 0 && req.SchemaVersion() != schema_ts_)
        {
            req.SetError(CcErrorCode::REQUESTED_TABLE_SCHEMA_MISMATCH);
            return true;
        }

        if (req.SendResponseIfFinished())
        {
            req.UnpinSlices();
            return true;
        }

        if (req.IsWaitForSnapshot(shard_->core_id_))
        {
            assert(req.WaitForSnapshotCnt(shard_->core_id_) == 0);
            if (req.SetFinish())
            {
                if (req.Result()->Value().is_local_)
                {
                    req.UnpinSlices();
                    return true;
                }
                else if (req.IsResponseSender(shard_->core_id_))
                {
                    req.SendResponseIfFinished();
                    req.UnpinSlices();
                    return true;
                }
                else
                {
                    shard_->local_shards_.EnqueueCcRequest(
                        shard_->core_id_, req.Txn(), &req);
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        CcOperation cc_op;
        bool is_read_snapshot;
        if (table_name_.Type() == TableType::Secondary ||
            table_name_.Type() == TableType::UniqueSecondary)
        {
            cc_op = CcOperation::ReadSkIndex;
            is_read_snapshot = (req.Isolation() == IsolationLevel::Snapshot);
        }
        else
        {
            cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::Read;
            is_read_snapshot = (req.Isolation() == IsolationLevel::Snapshot &&
                                !req.IsForWrite());
        }

        LockType lock_type = LockTypeUtil::DeduceLockType(
            cc_op, req.Isolation(), req.Protocol(), req.IsCoveringKeys());

        bool is_require_keys = req.IsRequireKeys();
        bool is_require_recs = req.IsRequireRecords();

        CcHandlerResult<RangeScanSliceResult> *hd_res = req.Result();
        const KeyT *req_start_key = nullptr;
        if (req.StartKey() != nullptr)
        {
            req_start_key = static_cast<const KeyT *>(req.StartKey());
        }
        else if (req.StartKeyStr() != nullptr && !req.StartKeyStr()->empty())
        {
            // For a remote scan request, the request is first enqueued into the
            // 1st core, where the start key is de-serialized and re-used for
            // scans in remaining cores.
            std::unique_ptr<KeyT> decoded_key = std::make_unique<KeyT>();
            size_t offset = 0;
            decoded_key->Deserialize(
                req.StartKeyStr()->data(), offset, KeySchema());
            req_start_key = decoded_key.get();

            req.SetStartKey(TxKey(std::move(decoded_key)));
        }
        else if (req.Direction() == ScanDirection::Forward)
        {
            req_start_key = KeyT::NegativeInfinity();
        }
        else
        {
            req_start_key = KeyT::PositiveInfinity();
        }

        const KeyT *req_end_key = nullptr;
        if (req.EndKey() != nullptr)
        {
            req_end_key = static_cast<const KeyT *>(req.EndKey());
        }
        else if (req.EndKeyStr() != nullptr && !req.EndKeyStr()->empty())
        {
            std::unique_ptr<KeyT> decoded_end_key = std::make_unique<KeyT>();
            size_t offset = 0;
            decoded_end_key->Deserialize(
                req.EndKeyStr()->data(), offset, KeySchema());
            req_end_key = decoded_end_key.get();

            req.SetEndKey(TxKey(std::move(decoded_end_key)));
        }

        uint16_t core_id = shard_->LocalCoreId();
        TemplateScanCache<KeyT, ValueT> *scan_cache = nullptr;
        RemoteScanSliceCache *remote_scan_cache = nullptr;
        if (req.IsLocal())
        {
            scan_cache = static_cast<TemplateScanCache<KeyT, ValueT> *>(
                req.GetLocalScanCache(core_id));
            assert(scan_cache != nullptr);
            scan_cache->SetCacheMaxBytes(
                StoreSlice::slice_upper_bound *
                (1 + req.PrefetchSize() / shard_->core_cnt_));
        }
        else
        {
            remote_scan_cache = req.GetRemoteScanCache(core_id);
            assert(remote_scan_cache != nullptr);
            remote_scan_cache->SetCacheMaxBytes(
                StoreSlice::slice_upper_bound *
                (1 + req.PrefetchSize() / shard_->core_cnt_) * 125 / 100);
        }

        auto is_cache_full = [&req, scan_cache, remote_scan_cache] {
            return req.IsLocal() ? scan_cache->IsFull()
                                 : remote_scan_cache->IsFull();
        };

        auto last_cce_of_cache = [&req, scan_cache, remote_scan_cache]()
            -> CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *
        {
            if (req.IsLocal())
            {
                if (scan_cache->Last())
                {
                    return reinterpret_cast<
                        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>
                            *>(scan_cache->Last()->cce_ptr_);
                }
                else
                {
                    return nullptr;
                }
            }
            else
            {
                if (remote_scan_cache->Size() > 0)
                {
                    return reinterpret_cast<
                        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>
                            *>(remote_scan_cache->LastCce());
                }
                else
                {
                    return nullptr;
                }
            }
        };

        if (req.SliceId().Slice() == nullptr)
        {
            // The scan slice request is first dispatched to one core, which
            // pins the slice in memory. After the slice is pinned, the request
            // is dispatched to other cores to scan in parallel. The slice is
            // unpinned by the last core finishing the scan batch.
            RangeSliceOpStatus pin_status = RangeSliceOpStatus::NotPinned;
            const StoreSlice *last_pinned_slice;
            uint32_t max_pin_cnt = req.PrefetchSize();

            RangeSliceId slice_id = shard_->local_shards_.PinRangeSlices(
                table_name_,
                req.NodeGroupId(),
                ng_term,
                KeySchema(),
                RecordSchema(),
                schema_ts_,
                table_schema_->GetKVCatalogInfo(),
                req.RangeId(),
                *req_start_key,
                req.StartInclusive(),
                req_end_key,
                req.EndInclusive(),
                &req,
                shard_,
                false,
                req.PrefetchSize(),
                max_pin_cnt,
                req.Direction() == ScanDirection::Forward,
                pin_status,
                last_pinned_slice);

            if (metrics::enable_cache_hit_rate && !req.CacheHitMissCollected())
            {
                if (pin_status == RangeSliceOpStatus::Successful ||
                    pin_status == RangeSliceOpStatus::KeyNotExists)
                {
                    shard_->CollectCacheHit();
                }
                else
                {
                    shard_->CollectCacheMiss();
                }
                req.SetCacheHitMissCollected();
            }

            if (pin_status == RangeSliceOpStatus::Retry)
            {
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
            }
            else if (pin_status == RangeSliceOpStatus::Delay)
            {
                if (slice_id.Range()->HasLock())
                {
                    return req.SetError(CcErrorCode::OUT_OF_MEMORY);
                }
                else
                {
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
                }
            }
            else if (pin_status == RangeSliceOpStatus::BlockedOnLoad)
            {
                return false;
            }
            else if (pin_status == RangeSliceOpStatus::Error ||
                     pin_status == RangeSliceOpStatus::NotOwner)
            {
                // If the pin operation returns an error, the data store
                // is inaccessible.
                return req.SetError(CcErrorCode::PIN_RANGE_SLICE_FAILED);
            }

            assert(pin_status == RangeSliceOpStatus::Successful);
            req.PinSlices(slice_id, last_pinned_slice);
            // Update unfinished cnt before dispatching to remaining cores.
            req.SetUnfinishedCoreCnt(req.GetShardCount());

            // Dispatches to remaining cores to scan pinned slice(s) in
            // parallel.
            for (uint16_t core_id = 0; core_id < shard_->local_shards_.Count();
                 ++core_id)
            {
                if (core_id == shard_->core_id_)
                {
                    continue;
                }

                shard_->local_shards_.EnqueueCcRequest(
                    shard_->core_id_, core_id, &req);
            }
        }

        Iterator scan_ccm_it;
        const KeyT *cce_key = nullptr;
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce = nullptr;
        uint32_t ng_id = req.NodeGroupId();
        int64_t tx_term = req.TxTerm();

        enum struct ScanReturnType
        {
            Success,
            Blocked,
            Yield,
            Error,
        };

        auto scan_tuple_func =
            [&, this](
                const KeyT *cce_key,
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp,
                ScanType scan_type) -> std::pair<ScanReturnType, CcErrorCode>
        {
            bool is_locked = false;

            if (lock_type != LockType::NoLock ||
                req.Isolation() == IsolationLevel::Snapshot)
            {
                // A CcEntry with unknown status was emplaced by a transaction
                // which had tried to insert a new key before aborted.
                // The abort PostWriteCc try to erase CcEntry only if there
                // is no read intent on it. ScanSliceCc shoudn't acquire read
                // intent on unknown CcEntry.
                RecordStatus status =
                    cce->PayloadStatus() == RecordStatus::Unknown
                        ? RecordStatus::Deleted
                        : cce->PayloadStatus();
                auto lock_pair = AcquireCceKeyLock(cce,
                                                   cce->CommitTs(),
                                                   ccp,
                                                   status,
                                                   &req,
                                                   ng_id,
                                                   ng_term,
                                                   tx_term,
                                                   lock_type,
                                                   cc_op,
                                                   req.Isolation(),
                                                   req.Protocol(),
                                                   req.ReadTimestamp());
                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    req.SetBlockingInfo(
                        shard_->core_id_,
                        reinterpret_cast<uint64_t>(cce->GetLockAddr()),
                        scan_type,
                        ScanBlockingType::BlockOnFuture);
                    return {ScanReturnType::Blocked, CcErrorCode::NO_ERROR};
                }
                case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
                {
                    req.SetBlockingInfo(
                        shard_->core_id_,
                        reinterpret_cast<uint64_t>(cce->GetLockAddr()),
                        scan_type,
                        ScanBlockingType::BlockOnLock);
                    req.SetRangeCcNgTerm(ng_term);
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    return {ScanReturnType::Blocked, CcErrorCode::NO_ERROR};
                }
                default:
                {
                    // lock confilct: back off and retry.
                    return {ScanReturnType::Error, lock_pair.second};
                }
                }  //-- end: switch

                is_locked = lock_pair.first != LockType::NoLock;
            }

            bool need_fetch_snapshot = false;
            if (req.IsLocal())
            {
                AddScanTuple(cce_key,
                             cce,
                             scan_cache,
                             scan_type,
                             ng_id,
                             ng_term,
                             req.Txn(),
                             req.ReadTimestamp(),
                             -1,
                             is_read_snapshot,
                             need_fetch_snapshot,
                             is_locked,
                             false,
                             is_require_keys,
                             is_require_recs);

                if (need_fetch_snapshot)
                {
                    assert(!is_locked);
                    size_t last_tuple_idx = scan_cache->Size() - 1;
                    auto *last_tuple = scan_cache->At(last_tuple_idx);
                    auto fetch_ret_status = shard_->FetchSnapshot(
                        this->table_name_,
                        this->table_schema_,
                        TxKey(cce_key),
                        this->cc_ng_id_,
                        ng_term,
                        req.ReadTimestamp(),
                        last_tuple->rec_status_ ==
                            RecordStatus::ArchiveVersionMiss,
                        &req,
                        last_tuple_idx,
                        &BackfillSnapshotForScanSlice<KeyT, ValueT>,
                        req.RangeId());

                    assert(fetch_ret_status ==
                           store::DataStoreHandler::DataStoreOpStatus::Success);
                    (void) fetch_ret_status;
                    req.IncreaseWaitForSnapshotCnt(shard_->core_id_);
                }
            }
            else
            {
                AddScanTupleMsg(cce_key,
                                cce,
                                remote_scan_cache,
                                scan_type,
                                ng_term,
                                req.ReadTimestamp(),
                                is_read_snapshot,
                                need_fetch_snapshot,
                                is_locked,
                                false,
                                is_require_keys,
                                is_require_recs);

                if (need_fetch_snapshot)
                {
                    assert(!is_locked);

                    auto last_rec_status =
                        remote_scan_cache->rec_status_.back();
                    assert(last_rec_status ==
                               remote::RecordStatusType::VERSIONUNDEFIND ||
                           last_rec_status ==
                               remote::RecordStatusType::ArchiveVersionMiss ||
                           last_rec_status ==
                               remote::RecordStatusType::BaseVersionMiss);

                    size_t last_tuple_idx =
                        remote_scan_cache->archive_positions_.size() - 1;
                    auto fetch_ret_status = shard_->FetchSnapshot(
                        this->table_name_,
                        this->table_schema_,
                        TxKey(cce_key),
                        this->cc_ng_id_,
                        ng_term,
                        req.ReadTimestamp(),
                        last_rec_status ==
                            remote::RecordStatusType::ArchiveVersionMiss,
                        &req,
                        last_tuple_idx,
                        &BackfillSnapshotForScanSlice<KeyT, ValueT>,
                        req.RangeId());

                    assert(fetch_ret_status ==
                           store::DataStoreHandler::DataStoreOpStatus::Success);
                    (void) fetch_ret_status;
                    req.IncreaseWaitForSnapshotCnt(shard_->core_id_);
                }
            }

            return {ScanReturnType::Success, CcErrorCode::NO_ERROR};
        };

        uint64_t cce_lock_addr = req.BlockingCceLockAddr(core_id);
        if (cce_lock_addr != 0)
        {
            KeyGapLockAndExtraData *lock =
                reinterpret_cast<KeyGapLockAndExtraData *>(cce_lock_addr);
            assert(lock != nullptr && lock->GetCcEntry() != nullptr);
            cce = reinterpret_cast<
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                lock->GetCcEntry());

            auto [blocking_type, scan_type] = req.BlockingPair(core_id);
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                static_cast<
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                    cce->GetCcPage());
            assert(ccp != nullptr);
            scan_ccm_it = Iterator(cce, ccp, &neg_inf_);
            cce_key = scan_ccm_it->first;

            if (blocking_type == ScanBlockingType::NoBlocking)
            {
                // This is a resumed scan slice cc. If the scan itself won't
                // lock the cce, we will put a read intent on the last cce so
                // that it can't be kicked from memory and this cce addr is
                // valid. This lock should be released when the scan slicecc
                // resumes.
                if (lock_type == LockType::NoLock)
                {
                    DecrReadIntent(cce->GetKeyLock(), cce, req.Txn(), ng_id);
                }
            }
            else
            {
                bool is_locked = false;

                if (blocking_type == ScanBlockingType::BlockOnFuture)
                {
                    // The scan was blocked because it intends to scan a key's
                    // version that has not been committed.
                    cce->GetKeyLock()->ReleaseReadLock(req.Txn(), shard_);
                }
                else
                {
                    // The scan was blocked because of read-write conflicts. The
                    // read lock/write intent has been acquired, updates the
                    // lock holding tx collection in this shard.
                    auto lock_pair =
                        LockHandleForResumedRequest(cce,
                                                    cce->CommitTs(),
                                                    cce->PayloadStatus(),
                                                    &req,
                                                    ng_id,
                                                    ng_term,
                                                    tx_term,
                                                    cc_op,
                                                    req.Isolation(),
                                                    req.Protocol(),
                                                    req.ReadTimestamp(),
                                                    req.IsCoveringKeys());

                    if (lock_pair.second != CcErrorCode::NO_ERROR)
                    {
                        assert(lock_pair.second ==
                               CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT);

                        if (req.IsLocal())
                        {
                            req.GetLocalScanner()->CommitAtCore(core_id);
                        }

                        if (is_read_snapshot &&
                            req.WaitForSnapshotCnt(shard_->core_id_) > 0)
                        {
                            req.SetIsWaitForSnapshot(shard_->core_id_);
                            req.DeferSetError(lock_pair.second);
                            return false;
                        }

                        if (req.SetError(lock_pair.second))
                        {
                            if (req.Result()->Value().is_local_)
                            {
                                req.UnpinSlices();
                                return true;
                            }
                            else if (req.IsResponseSender(shard_->core_id_))
                            {
                                req.SendResponseIfFinished();
                                req.UnpinSlices();
                                return true;
                            }
                            else
                            {
                                shard_->local_shards_.EnqueueCcRequest(
                                    shard_->core_id_, req.Txn(), &req);
                                return false;
                            }
                        }
                        else
                        {
                            return false;
                        }
                    }

                    is_locked = lock_pair.first != LockType::NoLock;
                }

                bool need_fetch_snapshot = false;
                if (req.IsLocal())
                {
                    AddScanTuple(cce_key,
                                 cce,
                                 scan_cache,
                                 scan_type,
                                 ng_id,
                                 ng_term,
                                 req.Txn(),
                                 req.ReadTimestamp(),
                                 -1,
                                 is_read_snapshot,
                                 need_fetch_snapshot,
                                 is_locked,
                                 false,
                                 is_require_keys,
                                 is_require_recs);
                    if (need_fetch_snapshot)
                    {
                        assert(!is_locked);
                        size_t last_tuple_idx = scan_cache->Size() - 1;
                        auto *last_tuple = scan_cache->At(last_tuple_idx);
                        auto fetch_ret_status = shard_->FetchSnapshot(
                            this->table_name_,
                            this->table_schema_,
                            TxKey(cce_key),
                            this->cc_ng_id_,
                            ng_term,
                            req.ReadTimestamp(),
                            last_tuple->rec_status_ ==
                                RecordStatus::ArchiveVersionMiss,
                            &req,
                            last_tuple_idx,
                            &BackfillSnapshotForScanSlice<KeyT, ValueT>,
                            req.RangeId());

                        assert(fetch_ret_status ==
                               store::DataStoreHandler::DataStoreOpStatus::
                                   Success);
                        (void) fetch_ret_status;
                        req.IncreaseWaitForSnapshotCnt(shard_->core_id_);
                    }
                }
                else
                {
                    AddScanTupleMsg(cce_key,
                                    cce,
                                    remote_scan_cache,
                                    scan_type,
                                    ng_term,
                                    req.ReadTimestamp(),
                                    is_read_snapshot,
                                    need_fetch_snapshot,
                                    is_locked,
                                    false,
                                    is_require_keys,
                                    is_require_recs);
                    if (need_fetch_snapshot)
                    {
                        assert(!is_locked);

                        auto last_rec_status =
                            remote_scan_cache->rec_status_.back();
                        assert(
                            last_rec_status ==
                                remote::RecordStatusType::VERSIONUNDEFIND ||
                            last_rec_status ==
                                remote::RecordStatusType::ArchiveVersionMiss ||
                            last_rec_status ==
                                remote::RecordStatusType::BaseVersionMiss);

                        size_t last_tuple_idx =
                            remote_scan_cache->archive_positions_.size() - 1;
                        auto fetch_ret_status = shard_->FetchSnapshot(
                            this->table_name_,
                            this->table_schema_,
                            TxKey(cce_key),
                            this->cc_ng_id_,
                            ng_term,
                            req.ReadTimestamp(),
                            last_rec_status ==
                                remote::RecordStatusType::ArchiveVersionMiss,
                            &req,
                            last_tuple_idx,
                            &BackfillSnapshotForScanSlice<KeyT, ValueT>,
                            req.RangeId());

                        assert(fetch_ret_status ==
                               store::DataStoreHandler::DataStoreOpStatus::
                                   Success);
                        (void) fetch_ret_status;
                        req.IncreaseWaitForSnapshotCnt(shard_->core_id_);
                    }
                }
            }
            if (req.Direction() == ScanDirection::Forward)
            {
                scan_ccm_it++;
            }
            else
            {
                scan_ccm_it--;
            }
        }
        else
        {
            if (req.Isolation() == IsolationLevel::Snapshot)
            {
                // MVCC update last_read_ts_ of lastest ccentry to tell
                // later writer's commit_ts must be higher than MVCC
                // reader's ts. Or it will break the REPEATABLE READ since
                // the next MVCC read in the same transaction will read the
                // new updated ccentry.
                shard_->UpdateLastReadTs(req.ReadTimestamp());
            }

            std::pair<Iterator, ScanType> start_pair =
                req.Direction() == ScanDirection::Forward
                    ? ForwardScanStart(*req_start_key, req.StartInclusive())
                    : BackwardScanStart(*req_start_key, req.StartInclusive());

            scan_ccm_it = start_pair.first;
            cce_key = scan_ccm_it->first;
            if (start_pair.second == ScanType::ScanGap)
            {
                if (req.Direction() == ScanDirection::Forward)
                {
                    scan_ccm_it++;
                }
                else
                {
                    scan_ccm_it--;
                }
            }
        }

        RangeScanSliceResult &slice_result = hd_res->Value();
        auto [final_end_tx_key, end_finalized] = slice_result.PeekLastKey();
        if (req.Direction() == ScanDirection::Forward)
        {
            const TemplateStoreSlice<KeyT> *last_slice =
                static_cast<const TemplateStoreSlice<KeyT> *>(
                    req.LastPinnedSlice());

            // The scan at core 0 sets the scan's end key. By default, the
            // scan's end is the exclusive end of the slice or the request's
            // specified end key, whichever is smaller. In case keys in the
            // slice are too many to fit into the scan cache, the key right
            // after the last scanned tuple at core 0 becomes the exclusive end
            // of scans at other cores. In such a case, it is mandatory that all
            // keys smaller than the end key at other cores are returned in this
            // batch. So, scans at other cores may slightly exceed the scan
            // cache's capacity.

            const KeyT *initial_end = nullptr;
            bool init_end_inclusive = false;

            // Given the scan batch's final end key, deduces the local scan's
            // end and inclusiveness.
            auto deduce_scan_end =
                [](const KeyT *batch_end_key,
                   const KeyT *req_end_key,
                   bool req_inclusive) -> std::pair<const KeyT *, bool>
            {
                const KeyT *end = nullptr;
                bool inclusive = false;

                assert(batch_end_key != nullptr);
                // If the request specifies the end key and it is the scan
                // batch's end key, the scan's inclusiveness is determined by
                // the request. Or, the scan batch's end must be the exclusive
                // end of a slice or positive infinity.
                if (batch_end_key == req_end_key)
                {
                    end = req_end_key;
                    inclusive = req_inclusive;
                }
                else
                {
                    end = batch_end_key;
                    inclusive = false;
                }

                return {end, inclusive};
            };

            if (!end_finalized)
            {
                // This scan batch's end key has not been set. Takes the smaller
                // of the slice's last key and the request's end key as the
                // local scan's initial end. The initial end may be modified, if
                // another core finishes earlier and finalizes the batch's end
                // before this core. The final end may be smaller or greater
                // than the initial end.
                const KeyT *slice_end = last_slice->EndKey();
                assert(slice_end != nullptr);

                // If the request specifies the end key and it falls into the
                // slice, initializes the local scan's end to the request's end
                // key. Or, the scan end is the slice's end.
                if (req_end_key != nullptr &&
                    (*req_end_key < *slice_end ||
                     (*req_end_key == *slice_end && !req.EndInclusive())))
                {
                    initial_end = req_end_key;
                    init_end_inclusive = req.EndInclusive();
                }
                else
                {
                    initial_end = slice_end;
                    init_end_inclusive = false;
                }
            }
            else
            {
                // This scan batch's end key has been finalized by one of the
                // cores. Deduces the local scan's end and inclusiveness.
                std::tie(initial_end, init_end_inclusive) =
                    deduce_scan_end(final_end_tx_key->GetKey<KeyT>(),
                                    req_end_key,
                                    req.EndInclusive());
            }

            auto scan_batch_func =
                [&scan_tuple_func](
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>
                        *ccp,
                    size_t start_idx,
                    size_t end_idx) -> std::pair<ScanReturnType, CcErrorCode>
            {
                ScanReturnType scan_ret = ScanReturnType::Success;
                CcErrorCode err_code = CcErrorCode::NO_ERROR;
                for (size_t idx = start_idx; idx < end_idx; ++idx)
                {
                    const KeyT &key = ccp->keys_[idx];
                    CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>
                        *cce = ccp->entries_[idx].get();
                    std::tie(scan_ret, err_code) =
                        scan_tuple_func(&key, cce, ccp, ScanType::ScanBoth);
                    if (scan_ret != ScanReturnType::Success)
                    {
                        break;
                    }
                }
                return {scan_ret, err_code};
            };

            auto scan_loop_func = [this, &scan_batch_func, &is_cache_full](
                                      Iterator &scan_ccm_it,
                                      const KeyT &end_key,
                                      bool inclusive,
                                      bool end_finalized)
                -> std::pair<ScanReturnType, CcErrorCode>
            {
                ScanReturnType scan_ret = ScanReturnType::Success;
                CcErrorCode err_code = CcErrorCode::NO_ERROR;

                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                    scan_ccm_it.GetPage();
                size_t idx_in_page = scan_ccm_it.GetIdxInPage();

                if (ccp == &pos_inf_page_)
                {
                    return {scan_ret, err_code};
                }

                assert(idx_in_page < ccp->keys_.size());

                while (ccp)
                {
                    size_t page_idx_end;
                    if (end_key < ccp->LastKey() ||
                        (!inclusive && end_key == ccp->LastKey()))
                    {
                        auto it =
                            std::lower_bound(ccp->keys_.begin() + idx_in_page,
                                             ccp->keys_.end(),
                                             end_key);
                        if (inclusive && *it == end_key)
                        {
                            ++it;
                        }
                        page_idx_end = std::distance(ccp->keys_.begin(), it);
                    }
                    else
                    {
                        page_idx_end = ccp->Size();
                    }

                    std::tie(scan_ret, err_code) =
                        scan_batch_func(ccp, idx_in_page, page_idx_end);
                    if (scan_ret != ScanReturnType::Success)
                    {
                        break;
                    }

                    if (page_idx_end == ccp->keys_.size())
                    {
                        // The page is fully scanned.
                        if (ccp->next_page_ == &pos_inf_page_)
                        {
                            scan_ccm_it = End();
                            ccp = nullptr;
                        }
                        else if (!end_finalized && is_cache_full())
                        {
                            scan_ccm_it =
                                Iterator(ccp->next_page_, 0, &neg_inf_);
                            ccp = nullptr;
                        }
                        else
                        {
                            ccp = ccp->next_page_;
                            idx_in_page = 0;
                        }
                    }
                    else
                    {
                        scan_ccm_it = Iterator(ccp, page_idx_end, &neg_inf_);
                        ccp = nullptr;
                    }
                }

                return {scan_ret, err_code};
            };

            auto [scan_ret, err] = scan_loop_func(
                scan_ccm_it, *initial_end, init_end_inclusive, end_finalized);
            switch (scan_ret)
            {
            case ScanReturnType::Blocked:
                return false;
            case ScanReturnType::Error:
                if (req.IsLocal())
                {
                    req.GetLocalScanner()->CommitAtCore(core_id);
                }

                if (is_read_snapshot &&
                    req.WaitForSnapshotCnt(shard_->core_id_) > 0)
                {
                    req.SetIsWaitForSnapshot(shard_->core_id_);
                    req.DeferSetError(err);
                    return false;
                }

                if (req.SetError(err))
                {
                    if (req.Result()->Value().is_local_)
                    {
                        req.UnpinSlices();
                        return true;
                    }
                    else if (req.IsResponseSender(shard_->core_id_))
                    {
                        req.SendResponseIfFinished();
                        req.UnpinSlices();
                        return true;
                    }
                    else
                    {
                        shard_->local_shards_.EnqueueCcRequest(
                            shard_->core_id_, req.Txn(), &req);
                        return false;
                    }
                }
                else
                {
                    return false;
                }
            case ScanReturnType::Yield:
                shard_->Enqueue(shard_->core_id_, &req);
                return false;
            default:
                break;
            }

            // If the end of this scan batch is not finalized when the local
            // scan at this core started, tries to set the batch's end using the
            // local end. If another core has finalized the batch's end, the
            // scan at this core may need to be adjusted: if the batch's final
            // end is less than the end at this core, keys after the final end
            // needs to be removed from the local scan cache; if the batch's
            // final end is greater than the end of this core, keys smaller than
            // the batch's final end but greater than the local end need to be
            // included in the local scan cache.
            if (!end_finalized)
            {
                const KeyT *local_end = nullptr;
                SlicePosition slice_position;

                // scan_ccm_it points to the entry after the last scanned tuple.
                // If the slice ends with positive infinity and has been fully
                // scanned, scan_ccm_it would point to positive infinity.
                auto pos_inf_it = End();
                if (scan_ccm_it != pos_inf_it &&
                    (*scan_ccm_it->first < *initial_end ||
                     (init_end_inclusive &&
                      *scan_ccm_it->first == *initial_end)))
                {
                    // The slice is too large. The scan has not fully scanned
                    // the slice, before reaching the cache's size limit.
                    // Pretends the slice's exclusive end to be the key after
                    // the last scanned tuple, from which the next scan batch
                    // resume.
                    local_end = scan_ccm_it->first;
                    slice_position = SlicePosition::Middle;
                }
                else
                {
                    // The slice has been fully scanned. If the request
                    // specifies the end key, which falls into the slice, given
                    // that the slice has been fully scanned, no future scan
                    // batches are needed. So, we pretend that the scan has
                    // reached the last slice ending with positive infinity.
                    // The calling tx will terminate the scan.
                    if (initial_end == KeyT::PositiveInfinity() ||
                        req_end_key == initial_end)
                    {
                        local_end = initial_end;
                        slice_position = SlicePosition::LastSlice;
                    }
                    else
                    {
                        // The local scan end must be the end of the slice.
                        local_end = initial_end;
                        const TemplateStoreRange<KeyT> *range =
                            static_cast<const TemplateStoreRange<KeyT> *>(
                                req.SliceId().Range());
                        const KeyT *range_end = range->RangeEndKey();
                        if (range_end != nullptr && *initial_end == *range_end)
                        {
                            slice_position = SlicePosition::LastSliceInRange;
                        }
                        else
                        {
                            slice_position = SlicePosition::Middle;
                        }
                    }
                }

                auto [batch_end, set_success] =
                    slice_result.UpdateLastKey(local_end, slice_position);

                if (set_success)
                {
                    req.SetRangeCcNgTerm(ng_term);
                }
                else
                {
                    // The local scan tries to set the scan batch's end, but the
                    // scan at another core have set the batch's end. The scan
                    // results need to be adjusted, if the results include the
                    // keys greater than the batch's end, or the results miss
                    // some keys smaller than the batch's end.
                    auto [end_key, end_inclusive] = deduce_scan_end(
                        batch_end, req_end_key, req.EndInclusive());
                    size_t trailing_cnt = 0;

                    // Excludes keys from the scan cache greater than the
                    // batch's end.
                    if (req.IsLocal())
                    {
                        while (scan_cache->Size() > 0)
                        {
                            // If req.is_require_keys_ is false, the KeyT object
                            // in scan cache is invalid, so, should use the cce,
                            // which is valid in any situation, to get the
                            // corresponding key.
                            auto last_cce =
                                reinterpret_cast<CcEntry<KeyT,
                                                         ValueT,
                                                         VersionedRecord,
                                                         RangePartitioned> *>(
                                    scan_cache->Last()->cce_ptr_);
                            while (scan_ccm_it->second != last_cce)
                            {
                                --scan_ccm_it;
                                assert(scan_ccm_it != Begin());
                            }
                            const KeyT *last_key =
                                static_cast<const KeyT *>(scan_ccm_it->first);
                            if (*end_key < *last_key ||
                                (*end_key == *last_key && !end_inclusive))
                            {
                                ++trailing_cnt;
                                // Remove cce from scan cache, but keep possible
                                // locks, because those locks might acquired by
                                // other ScanSliceCc/ReadCc from the
                                // transaction.
                                scan_cache->RemoveLast();
                            }
                            else
                            {
                                // Reset iterator to the key after the last
                                // scanned tuple since we might need to continue
                                // scanning if trailing_cnt == 0.
                                ++scan_ccm_it;
                                break;
                            }
                        }
                    }
                    else
                    {
                        while (remote_scan_cache->Size() > 0)
                        {
                            // Cc entry pointers here are always valid since
                            // the slices are still pinned so the cce cannot
                            // be kicked from memory regardless of the lock
                            // type.
                            auto last_remote_cce =
                                reinterpret_cast<CcEntry<KeyT,
                                                         ValueT,
                                                         VersionedRecord,
                                                         RangePartitioned> *>(
                                    remote_scan_cache->LastCce());
                            while (scan_ccm_it->second != last_remote_cce)
                            {
                                // As long as remote scan cache is not empty,
                                // iterator should not reach neg inf.
                                --scan_ccm_it;
                                assert(scan_ccm_it != Begin());
                            }
                            const KeyT *last_key =
                                static_cast<const KeyT *>(scan_ccm_it->first);
                            if (*end_key < *last_key ||
                                (*end_key == *last_key && !end_inclusive))
                            {
                                trailing_cnt++;
                                // Remove cce from scan cache, but keep possible
                                // locks, because those locks might acquired by
                                // other ScanSliceCc/ReadCc from the
                                // transaction.
                                remote_scan_cache->RemoveLast();
                            }
                            else
                            {
                                // Reset iterator to the key after the last
                                // scanned tuple since we might need to continue
                                // scanning if trailing_cnt == 0.
                                ++scan_ccm_it;
                                break;
                            }
                        }
                    }

                    // If no key is removed from the scan cache, it's possible
                    // that the local scan may miss keys smaller than the
                    // batch's end. Re-scans the cc map using the batch's end.
                    if (trailing_cnt == 0)
                    {
                        auto [scan_ret, err] = scan_loop_func(
                            scan_ccm_it, *end_key, end_inclusive, true);
                        switch (scan_ret)
                        {
                        case ScanReturnType::Blocked:
                            return false;
                        case ScanReturnType::Error:
                            if (req.IsLocal())
                            {
                                req.GetLocalScanner()->CommitAtCore(core_id);
                            }

                            if (is_read_snapshot &&
                                req.WaitForSnapshotCnt(shard_->core_id_) > 0)
                            {
                                req.SetIsWaitForSnapshot(shard_->core_id_);
                                req.DeferSetError(err);
                                return false;
                            }

                            if (req.SetError(err))
                            {
                                if (req.Result()->Value().is_local_)
                                {
                                    req.UnpinSlices();
                                    return true;
                                }
                                else if (req.IsResponseSender(shard_->core_id_))
                                {
                                    req.SendResponseIfFinished();
                                    req.UnpinSlices();
                                    return true;
                                }
                                else
                                {
                                    shard_->local_shards_.EnqueueCcRequest(
                                        shard_->core_id_, req.Txn(), &req);
                                    return false;
                                }
                            }
                            else
                            {
                                return false;
                            }
                        case ScanReturnType::Yield:
                            shard_->Enqueue(shard_->core_id_, &req);
                            return false;
                        default:
                            break;
                        }
                    }
                }
            }

            // Sets the iterator to the last cce, which may need to be pinned to
            // resume the next scan batch.
            if (CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>
                    *last_cce = last_cce_of_cache();
                last_cce)
            {
                while (scan_ccm_it->second != last_cce)
                {
                    --scan_ccm_it;
                }
            }
        }
        else
        {
            const TemplateStoreSlice<KeyT> *last_slice =
                static_cast<const TemplateStoreSlice<KeyT> *>(
                    req.LastPinnedSlice());

            const KeyT *initial_end = nullptr;
            bool init_end_inclusive = false;

            auto deduce_scan_end =
                [](const KeyT *batch_end_key,
                   const KeyT *req_end_key,
                   bool req_inclusive) -> std::pair<const KeyT *, bool>
            {
                const KeyT *end = nullptr;
                bool inclusive = false;

                if (batch_end_key == req_end_key)
                {
                    end = req_end_key;
                    inclusive = req_inclusive;
                }
                else
                {
                    end = batch_end_key;
                    inclusive = true;
                }

                return {end, inclusive};
            };

            if (!end_finalized)
            {
                const KeyT *slice_begin = last_slice->StartKey();
                assert(slice_begin != nullptr);

                if (req_end_key != nullptr && (*slice_begin < *req_end_key ||
                                               *slice_begin == *req_end_key))
                {
                    initial_end = req_end_key;
                    init_end_inclusive = req.EndInclusive();
                }
                else
                {
                    initial_end = slice_begin;
                    init_end_inclusive = true;
                }
            }
            else
            {
                // This scan batch's end key has been finalized by one of the
                // cores. Deduces the local scan's end and inclusiveness.
                std::tie(initial_end, init_end_inclusive) =
                    deduce_scan_end(final_end_tx_key->GetKey<KeyT>(),
                                    req_end_key,
                                    req.EndInclusive());
            }

            auto scan_batch_func =
                [&scan_tuple_func](
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>
                        *ccp,
                    ssize_t start_idx,
                    ssize_t end_idx) -> std::pair<ScanReturnType, CcErrorCode>
            {
                ScanReturnType scan_ret = ScanReturnType::Success;
                CcErrorCode err_code = CcErrorCode::NO_ERROR;
                for (ssize_t idx = start_idx; idx > end_idx; --idx)
                {
                    const KeyT &key = ccp->keys_[idx];
                    CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>
                        *cce = ccp->entries_[idx].get();
                    std::tie(scan_ret, err_code) =
                        scan_tuple_func(&key, cce, ccp, ScanType::ScanBoth);
                    if (scan_ret != ScanReturnType::Success)
                    {
                        break;
                    }
                }
                return {scan_ret, err_code};
            };

            auto scan_loop_func = [this, &scan_batch_func, &is_cache_full](
                                      Iterator &scan_ccm_it,
                                      const KeyT &end_key,
                                      bool inclusive,
                                      bool end_finalized)
                -> std::pair<ScanReturnType, CcErrorCode>
            {
                ScanReturnType scan_ret = ScanReturnType::Success;
                CcErrorCode err_code = CcErrorCode::NO_ERROR;

                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                    scan_ccm_it.GetPage();
                ssize_t idx_in_page = scan_ccm_it.GetIdxInPage();

                if (ccp == &neg_inf_page_)
                {
                    return {scan_ret, err_code};
                }

                assert(static_cast<size_t>(idx_in_page) < ccp->keys_.size());

                auto comp = [](const KeyT &iter_key, const KeyT &end_key)
                { return end_key < iter_key; };

                while (ccp)
                {
                    ssize_t page_idx_end;
                    if (ccp->FirstKey() < end_key ||
                        (!inclusive && ccp->FirstKey() == end_key))
                    {
                        auto rbegin = ccp->keys_.rbegin() +
                                      (ccp->Size() - idx_in_page - 1);
                        auto it = std::lower_bound(
                            rbegin, ccp->keys_.rend(), end_key, comp);
                        if (inclusive && *it == end_key)
                        {
                            ++it;
                        }
                        page_idx_end = idx_in_page - std::distance(rbegin, it);
                    }
                    else
                    {
                        page_idx_end = -1;
                    }

                    std::tie(scan_ret, err_code) =
                        scan_batch_func(ccp, idx_in_page, page_idx_end);
                    if (scan_ret != ScanReturnType::Success)
                    {
                        break;
                    }

                    if (page_idx_end == -1)
                    {
                        // The page is fully scanned.
                        if (ccp->prev_page_ == &neg_inf_page_)
                        {
                            scan_ccm_it = Begin();
                            ccp = nullptr;
                        }
                        else if (!end_finalized && is_cache_full())
                        {
                            scan_ccm_it = Iterator(ccp->prev_page_,
                                                   ccp->prev_page_->Size() - 1,
                                                   &pos_inf_);
                            ccp = nullptr;
                        }
                        else
                        {
                            ccp = ccp->prev_page_;
                            idx_in_page = ccp->Size() - 1;
                        }
                    }
                    else
                    {
                        scan_ccm_it = Iterator(ccp, page_idx_end, &pos_inf_);
                        ccp = nullptr;
                    }
                }

                return {scan_ret, err_code};
            };

            auto [scan_ret, err] = scan_loop_func(
                scan_ccm_it, *initial_end, init_end_inclusive, end_finalized);
            switch (scan_ret)
            {
            case ScanReturnType::Blocked:
                return false;
            case ScanReturnType::Error:
                if (req.IsLocal())
                {
                    req.GetLocalScanner()->CommitAtCore(core_id);
                }

                if (is_read_snapshot &&
                    req.WaitForSnapshotCnt(shard_->core_id_) > 0)
                {
                    req.SetIsWaitForSnapshot(shard_->core_id_);
                    req.DeferSetError(err);
                    return false;
                }

                if (req.SetError(err))
                {
                    if (req.Result()->Value().is_local_)
                    {
                        req.UnpinSlices();
                        return true;
                    }
                    else if (req.IsResponseSender(shard_->core_id_))
                    {
                        req.SendResponseIfFinished();
                        req.UnpinSlices();
                        return true;
                    }
                    else
                    {
                        shard_->local_shards_.EnqueueCcRequest(
                            shard_->core_id_, req.Txn(), &req);
                        return false;
                    }
                }
                else
                {
                    return false;
                }
            case ScanReturnType::Yield:
                shard_->Enqueue(shard_->core_id_, &req);
                return false;
            default:
                break;
            }

            // If the end of this scan batch is not finalized when the local
            // scan at this core started, tries to set the batch's end using the
            // local end. If another core has finalized the batch's end, the
            // scan at this core may need to be adjusted: if the batch's final
            // end is less than the end at this core, keys before the final end
            // needs to be removed from the local scan cache; if the batch's
            // final end is smaller than the end of this core, keys greater than
            // the batch's final end but less than the local end need to be
            // included in the local scan cache.

            if (!end_finalized)
            {
                const KeyT *local_end = nullptr;
                SlicePosition slice_position;

                // scan_ccm_it points to the entry before the last scanned
                // tuple.
                auto neg_inf_it = Begin();
                if (scan_ccm_it != neg_inf_it &&
                    (*initial_end < *scan_ccm_it->first ||
                     (init_end_inclusive &&
                      *scan_ccm_it->first == *initial_end)))
                {
                    // The slice is too large. The scan has not fully scanned
                    // the slice, before reaching the cache's size limit.
                    // Pretends the slice's inclusive start to be the last
                    // scanned key, from which the next scan batch resumes.
                    ++scan_ccm_it;
                    local_end = scan_ccm_it->first;
                    slice_position = SlicePosition::Middle;
                }
                else
                {
                    // The slice has been fully scanned. If the request
                    // specifies the end key, which falls into the slice, given
                    // that the slice has been fully scanned, no future scan
                    // batches are needed. So, we pretend that the scan has
                    // reached the first slice (starting with negative
                    // infinity). The calling tx will terminate the scan.
                    if (initial_end == KeyT::NegativeInfinity() ||
                        req_end_key == initial_end)
                    {
                        local_end = initial_end;
                        slice_position = SlicePosition::FirstSlice;
                    }
                    else
                    {
                        // The local scan end must be the start of the slice.
                        local_end = initial_end;

                        const TemplateStoreRange<KeyT> *range =
                            static_cast<const TemplateStoreRange<KeyT> *>(
                                req.SliceId().Range());
                        const KeyT *range_start = range->RangeStartKey();
                        if (range_start != nullptr &&
                            *initial_end == *range_start)
                        {
                            slice_position = SlicePosition::FirstSliceInRange;
                        }
                        else
                        {
                            slice_position = SlicePosition::Middle;
                        }
                    }
                }

                auto [batch_end, set_success] =
                    slice_result.UpdateLastKey(local_end, slice_position);

                if (set_success)
                {
                    req.SetRangeCcNgTerm(ng_term);
                }
                else
                {
                    // The local scan tries to set the scan batch's end, but the
                    // scan at another core have set the batch's end. The scan
                    // results need to be adjusted, if the results include the
                    // keys smaller than the batch's end, or the results miss
                    // some keys greater than the batch's end.
                    auto [end_key, end_inclusive] = deduce_scan_end(
                        batch_end, req_end_key, req.EndInclusive());
                    size_t trailing_cnt = 0;

                    // Excludes keys from the scan cache smaller than the
                    // batch's end.
                    if (req.IsLocal())
                    {
                        while (scan_cache->Size() > 0)
                        {
                            // If req.is_require_keys_ is false, the KeyT object
                            // in scan cache is invalid, so, should use the cce,
                            // which is valid in any situation, to get the
                            // corresponding key.
                            CcEntry<KeyT,
                                    ValueT,
                                    VersionedRecord,
                                    RangePartitioned> *last_cce =
                                reinterpret_cast<CcEntry<KeyT,
                                                         ValueT,
                                                         VersionedRecord,
                                                         RangePartitioned> *>(
                                    scan_cache->Last()->cce_ptr_);
                            while (scan_ccm_it->second != last_cce)
                            {
                                ++scan_ccm_it;
                                assert(scan_ccm_it != End());
                            }
                            const KeyT *last_key =
                                static_cast<const KeyT *>(scan_ccm_it->first);
                            if (*last_key < *end_key ||
                                (*last_key == *end_key && !end_inclusive))
                            {
                                ++trailing_cnt;
                                scan_cache->RemoveLast();
                            }
                            else
                            {
                                // Reset iterator to the key after the last
                                // scanned tuple since we might need to continue
                                // scanning if trailing_cnt == 0.
                                --scan_ccm_it;
                                break;
                            }
                        }
                    }
                    else
                    {
                        while (remote_scan_cache->Size() > 0)
                        {
                            // Cc entry pointers here are always valid since
                            // the slices are still pinned so the cce cannot
                            // be kicked from memory regardless of the lock
                            // type.
                            CcEntry<KeyT,
                                    ValueT,
                                    VersionedRecord,
                                    RangePartitioned> *last_remote_cce =
                                reinterpret_cast<CcEntry<KeyT,
                                                         ValueT,
                                                         VersionedRecord,
                                                         RangePartitioned> *>(
                                    remote_scan_cache->LastCce());
                            while (scan_ccm_it->second != last_remote_cce)
                            {
                                // As long as remote scan cache is not empty,
                                // iterator should not reach pos inf.
                                ++scan_ccm_it;
                                assert(scan_ccm_it != End());
                            }
                            const KeyT *last_key =
                                static_cast<const KeyT *>(scan_ccm_it->first);
                            if (*last_key < *end_key ||
                                (*last_key == *end_key && !end_inclusive))
                            {
                                trailing_cnt++;
                                remote_scan_cache->RemoveLast();
                            }
                            else
                            {
                                // Reset iterator to the key after the last
                                // scanned tuple since we might need to continue
                                // scanning if trailing_cnt == 0.
                                --scan_ccm_it;
                                break;
                            }
                        }
                    }

                    // If no key is removed from the scan cache, it's possible
                    // that the local scan may miss keys greater than the
                    // batch's end. Re-scans the cc map using the batch's end.
                    if (trailing_cnt == 0)
                    {
                        auto [scan_ret, err] = scan_loop_func(
                            scan_ccm_it, *end_key, end_inclusive, true);
                        switch (scan_ret)
                        {
                        case ScanReturnType::Blocked:
                            return false;
                        case ScanReturnType::Error:
                            if (req.IsLocal())
                            {
                                req.GetLocalScanner()->CommitAtCore(core_id);
                            }

                            if (is_read_snapshot &&
                                req.WaitForSnapshotCnt(shard_->core_id_) > 0)
                            {
                                req.SetIsWaitForSnapshot(shard_->core_id_);
                                req.DeferSetError(err);
                                return false;
                            }

                            if (req.SetError(err))
                            {
                                if (req.Result()->Value().is_local_)
                                {
                                    req.UnpinSlices();
                                    return true;
                                }
                                else if (req.IsResponseSender(shard_->core_id_))
                                {
                                    req.SendResponseIfFinished();
                                    req.UnpinSlices();
                                    return true;
                                }
                                else
                                {
                                    shard_->local_shards_.EnqueueCcRequest(
                                        shard_->core_id_, req.Txn(), &req);
                                    return false;
                                }
                            }
                            else
                            {
                                return false;
                            }
                        case ScanReturnType::Yield:
                            shard_->Enqueue(shard_->core_id_, &req);
                            return false;
                        default:
                            break;
                        }
                    }
                }
            }

            // Sets the iterator to the last cce, which may need to be pinned to
            // resume the next scan batch.
            if (CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>
                    *last_cce = last_cce_of_cache();
                last_cce)
            {
                while (scan_ccm_it->second != last_cce)
                {
                    ++scan_ccm_it;
                }
            }
        }

        if (slice_result.slice_position_ == SlicePosition::Middle)
        {
            // When the scan batch stops in the middle of the range,
            // acquires the read intent on the last scanned key to prevent
            // if from kicking out. The next scan batch will resume from the
            // last key without searching the cc map.
            if (CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>
                    *last_cce = last_cce_of_cache();
                last_cce)
            {
                assert(last_cce->PayloadStatus() != RecordStatus::Unknown);

                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>
                    *last_ccp = scan_ccm_it.GetPage();
                AcquireReadIntent(last_cce, last_ccp, req.Txn(), tx_term);

                // Lock might have not been acquired during the scan, set the
                // lock addr of last tuple here.
                if (req.IsLocal())
                {
                    ScanTuple *last_tuple =
                        const_cast<ScanTuple *>(scan_cache->LastTuple());
                    assert(last_tuple != nullptr);
                    assert(last_cce->GetLockAddr() != nullptr);
                    last_tuple->cce_addr_.SetCceLock(
                        reinterpret_cast<uint64_t>(last_cce->GetLockAddr()));
                }
                else
                {
                    assert(remote_scan_cache->Size() > 0);
                    assert(last_cce->GetLockAddr() != nullptr);
                    remote_scan_cache->SetLastCceLock(
                        reinterpret_cast<uint64_t>(last_cce->GetLockAddr()));
                }
            }
        }

        if (req.IsLocal())
        {
            req.GetLocalScanner()->CommitAtCore(core_id);
        }

        if (is_read_snapshot && req.WaitForSnapshotCnt(shard_->core_id_) > 0)
        {
            req.SetIsWaitForSnapshot(shard_->core_id_);
            return false;
        }

        if (req.SetFinish())
        {
            if (req.Result()->Value().is_local_)
            {
                req.UnpinSlices();
                return true;
            }
            else if (req.IsResponseSender(shard_->core_id_))
            {
                req.SendResponseIfFinished();
                req.UnpinSlices();
                return true;
            }
            else
            {
                // Renqueue the cc req to the sender req list.
                // We assign a dedicated core to be the response sender instead
                // of directly sending the response on the last finished core.
                // This is to avoid serialization of response message causing
                // one core to become significantly slower than others and would
                // end up being the sender of all scan slice response.
                shard_->local_shards_.EnqueueCcRequest(
                    shard_->core_id_, req.Txn(), &req);
                return false;
            }
        }
        else
        {
            return false;
        }
    }

    /**
     * @brief Wrapper function for export cce by adding the heap full checking.
     * @return size_t: the number of records exported.
     *         bool: is the heap full.
     *
     */
    inline std::pair<size_t, bool> ExportForCkpt(
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
        const KeyT &key,
        std::vector<FlushRecord> &ckpt_vec,
        std::vector<FlushRecord> &akv_vec,
        std::vector<size_t> &mv_base_vec,
        uint64_t to_ts,
        uint64_t oldest_active_tx_ts,
        bool mvcc_enabled,
        size_t &ckpt_vec_size,
        bool export_base_table_item_if_need,
        bool export_base_table_item_only,
        bool export_base_table_key_only,
        uint64_t &flush_size) const
    {
        // This override heap thread call is not necessary, since the thread is
        // alreay be overrided before cc_request execution
        // shard_->OverrideHeapThread();
        CcShardHeap *scan_heap = shard_->GetShardDataSyncScanHeap();
        mi_heap_t *prev_heap = scan_heap->SetAsDefaultHeap();
        assert(shard_->GetShardHeapThreadId() == mi_thread_id());

        // If the heap is full, we should stop exporting.
        std::pair<size_t, bool> export_size = {0, true};
        // Do not try to call mi_heap_collect, since it is expensive, flush data
        // will return the memory anyway when it done
        if (!scan_heap->Full())
        {
            export_size.first =
                cce->ExportForCkpt(key,
                                   ckpt_vec,
                                   akv_vec,
                                   mv_base_vec,
                                   to_ts,
                                   oldest_active_tx_ts,
                                   mvcc_enabled,
                                   ckpt_vec_size,
                                   export_base_table_item_if_need,
                                   export_base_table_item_only,
                                   export_base_table_key_only,
                                   flush_size);
            export_size.second = false;
        }

        mi_heap_set_default(prev_heap);
        return export_size;
    }

    bool Execute(RangePartitionDataSyncScanCc &req) override
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
        // data sync scan cc for range partitioned table
        assert(RangePartitioned);

        if (req.schema_version_ != 0 &&
            table_schema_->Version() != req.schema_version_)
        {
            assert(req.schema_version_ > table_schema_->Version());
            LOG(WARNING) << "Table schema version mismatched for data sync "
                            "scan, table name: "
                         << req.table_name_->String()
                         << " ,scan carried version: " << req.schema_version_
                         << " ,ccmap version: " << table_schema_->Version();
            req.SetError(CcErrorCode::REQUESTED_TABLE_SCHEMA_MISMATCH);
            return false;
        }

        if (req.IsDrained(shard_->core_id_))
        {
            // scan is already finished on this core
            req.SetFinish(shard_->core_id_);
            return false;
        }

        auto &pause_key_and_is_drained = req.PausePos(shard_->core_id_);

        std::function<int32_t(int32_t, bool)> next_slice_func;
        if (req.export_base_table_item_)
        {
            next_slice_func = [&](int32_t idx, bool forward) -> int32_t
            {
                assert(forward);
                return idx + 1;
            };
        }
        else
        {
            next_slice_func = [&](int32_t idx, bool forward) -> int32_t
            {
                const TemplateStoreRange<KeyT> *range =
                    static_cast<TemplateStoreRange<KeyT> *>(
                        req.StoreRangePtr());
                auto &slices = range->TypedSlices();
                assert(forward && static_cast<size_t>(idx) < slices.size());
                int32_t sid = idx;
                while (static_cast<size_t>(++sid) < slices.size())
                {
                    uint64_t slice_post_ckpt_size = slices[sid]->PostCkptSize();
                    if (slice_post_ckpt_size != UINT64_MAX &&
                        slice_post_ckpt_size > StoreSlice::slice_upper_bound)
                    {
                        break;
                    }
                }
                return sid;
            };
        }

        auto pin_range_slice =
            [this, &req, &pause_key_and_is_drained, &next_slice_func](
                const KeyT &search_key) -> std::pair<RangeSliceId, bool>
        {
            bool succ = false;
            RangeSliceOpStatus pin_status;
            RangeSliceId slice_id = shard_->local_shards_.PinRangeSlice(
                table_name_,
                req.NodeGroupId(),
                req.NodeGroupTerm(),
                KeySchema(),
                RecordSchema(),
                schema_ts_,
                table_schema_->GetKVCatalogInfo(),
                -1,
                search_key,
                true,
                &req,
                shard_,
                pin_status,
                true,
                32,
                false,
                false,
                !req.export_base_table_item_,
                next_slice_func);

            switch (pin_status)
            {
            case RangeSliceOpStatus::Successful:
            {
                assert(
                    [&]()
                    {
                        const TemplateStoreSlice<KeyT> *new_slice =
                            static_cast<const TemplateStoreSlice<KeyT> *>(
                                slice_id.Slice());
                        return search_key == *new_slice->StartKey();
                    }());
                succ = true;
                break;
            }
            case RangeSliceOpStatus::BlockedOnLoad:
            {
                pause_key_and_is_drained.first = search_key.CloneTxKey();
                succ = false;
                break;
            }
            case RangeSliceOpStatus::Retry:
            {
                pause_key_and_is_drained.first = search_key.CloneTxKey();
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                succ = false;
                break;
            }
            case RangeSliceOpStatus::NotOwner:
            {
                assert("Dead branch");
                // The recovered cc entry does not belong to this ng
                // anymore. This will happen if ng failover after a
                // range split just finished but before checkpointer
                // is able to truncate the log. In this case the log
                // records of the data that now falls on another ng
                // will still be replayed on the old ng on recover.
                // Skip the cc entry and remove it at the end.
                succ = true;
                break;
            }
            default:
            {
                assert(pin_status == RangeSliceOpStatus::Error);
                req.SetError(CcErrorCode::PIN_RANGE_SLICE_FAILED);
                succ = false;
                break;
            }
            }

            return {slice_id, succ};
        };

        auto deduce_iterator = [this](const KeyT &search_key) -> Iterator
        {
            Iterator res_it;
            std::pair<Iterator, ScanType> search_pair =
                ForwardScanStart(search_key, true);
            res_it = search_pair.first;
            if (search_pair.second == ScanType::ScanGap)
            {
                ++res_it;
            }
            return res_it;
        };

        auto check_split_slice =
            [this, &req](std::vector<TxKey>::const_iterator &slice_it) -> bool
        {
            const StoreSlice *slice = req.StoreRangePtr()->FindSlice(*slice_it);

            assert(slice->PostCkptSize() != UINT64_MAX);
            return slice->PostCkptSize() > StoreSlice::slice_upper_bound;
        };

        const KeyT *req_end_key = nullptr;
        if (req.export_base_table_item_)
        {
            req_end_key = req.end_key_ != nullptr ? req.end_key_->GetKey<KeyT>()
                                                  : KeyT::PositiveInfinity();
        }
        else
        {
            auto curr_slice_it = req.CurrentSliceIt(shard_->core_id_);
            while (std::next(curr_slice_it) != req.EndSliceIt())
            {
                ++curr_slice_it;
            }
            req_end_key = req.StoreRangePtr()
                              ->FindSlice(*curr_slice_it)
                              ->EndTxKey()
                              .GetKey<KeyT>();
        }

        auto find_non_empty_slice = [this,
                                     &req,
                                     req_end_key,
                                     &deduce_iterator,
                                     &check_split_slice,
                                     &pin_range_slice](const KeyT &search_key,
                                                       bool move_next = true)
            -> std::tuple<Iterator, Iterator, const KeyT *, bool, bool, bool>
        {
            // Check whether need to pinslice.
            bool need_pin_slice = false;
            const KeyT *start_key = nullptr;
            if (req.export_base_table_item_)
            {
                start_key = &search_key;
                need_pin_slice = !req.slice_ids_[shard_->core_id_].Slice();
            }
            else
            {
                auto &curr_slice_it = req.CurrentSliceIt(shard_->core_id_);
                if (move_next)
                {
                    ++curr_slice_it;
                }
                assert(curr_slice_it != req.EndSliceIt());

                need_pin_slice = !req.slice_ids_[shard_->core_id_].Slice() &&
                                 check_split_slice(curr_slice_it);
                const KeyT *curr_start_key = req.StoreRangePtr()
                                                 ->FindSlice(*curr_slice_it)
                                                 ->StartTxKey()
                                                 .GetKey<KeyT>();
                start_key = (*curr_start_key < search_key ? &search_key
                                                          : curr_start_key);
                assert(
                    [&]()
                    {
                        const KeyT *curr_end_key =
                            req.StoreRangePtr()
                                ->FindSlice(*curr_slice_it)
                                ->EndTxKey()
                                .GetKey<KeyT>();
                        return !(*curr_start_key < search_key) ||
                               (search_key < *curr_end_key);
                    }());
            }

            // Loop to find slice if need until find one non-empty slice or
            // reach to the last slice of this request.
            StoreSlice *store_slice = nullptr;
            bool slice_pinned =
                req.slice_ids_[shard_->core_id_].Slice() != nullptr;
            Iterator it;
            Iterator end_it;
            const KeyT *slice_end_key = nullptr;
            bool is_last_slice = false;
            do
            {
                if (need_pin_slice)
                {
                    // Execute the pinslice operation, and return the
                    // RangeSliceId If the value of export_base_table_item_ is
                    // false, it means that the pin slice operation is required
                    // due to slice splitting, then, only need pin the current
                    // slice that needs to be split.
                    auto [new_slice_id, succ] = pin_range_slice(*start_key);
                    if (!succ)
                    {
                        return {Iterator(),
                                Iterator(),
                                nullptr,
                                false,
                                false,
                                false};
                    }
                    assert(new_slice_id.Slice());

                    // Store the range slice id
                    req.slice_ids_[shard_->core_id_] = new_slice_id;
                    slice_pinned = true;
                }

                // Get the begin iterator and the end iterator of current slice.
                it = deduce_iterator(*start_key);

                if (req.slice_ids_[shard_->core_id_].Slice())
                {
                    assert(slice_pinned);
                    store_slice = req.slice_ids_[shard_->core_id_].Slice();
                }
                else
                {
                    assert(!req.export_base_table_item_);
                    auto &curr_slice_it = req.CurrentSliceIt(shard_->core_id_);
                    // Store the slice
                    store_slice =
                        req.StoreRangePtr()->FindSlice(*curr_slice_it);
                }

                const TemplateStoreSlice<KeyT> *typed_slice =
                    static_cast<const TemplateStoreSlice<KeyT> *>(store_slice);
                slice_end_key = typed_slice->EndKey();
                end_it = deduce_iterator(*slice_end_key);

                is_last_slice = !(*slice_end_key < *req_end_key);

                if (it == end_it && !is_last_slice)
                {
                    // The current slice is empty, and it is not the last slice
                    // of the request. Try to find next slice if need. Unpin and
                    // reset
                    if (slice_pinned)
                    {
                        req.slice_ids_[shard_->core_id_].Unpin();
                        req.slice_ids_[shard_->core_id_].Reset();
                        slice_pinned = false;
                    }

                    // Try find next slice if the current slice is not the last
                    // slice of this request.
                    if (req.export_base_table_item_)
                    {
                        need_pin_slice = true;
                        // Get the search start key
                        start_key = slice_end_key;
                    }
                    else
                    {
                        auto &curr_slice_it =
                            req.CurrentSliceIt(shard_->core_id_);
                        ++curr_slice_it;
                        assert(curr_slice_it != req.EndSliceIt());
                        need_pin_slice = check_split_slice(curr_slice_it);

                        // Get the search start key of next slice that contains
                        // non-persisted keys.
                        start_key = req.StoreRangePtr()
                                        ->FindSlice(*curr_slice_it)
                                        ->StartTxKey()
                                        .GetKey<KeyT>();
                    }
                }
                else
                {
                    // The slice is non-empty or the empty slice is the last
                    // slice of this request.
                    break;
                }
            } while (true);

            return {
                it, end_it, slice_end_key, slice_pinned, is_last_slice, true};
        };

        const KeyT *const req_start_key = req.start_key_ != nullptr
                                              ? req.start_key_->GetKey<KeyT>()
                                              : KeyT::NegativeInfinity();

        Iterator key_it;
        Iterator slice_end_it = key_it;

        // 1. Find the first slice to be scanned.
        const KeyT *search_start_key = nullptr;
        if (pause_key_and_is_drained.first.KeyPtr() == nullptr)
        {
            // If this is a new scan cc, start from the specified start
            // key or negative inf.
            search_start_key = req_start_key;
        }
        else
        {
            // Begin from the last paused position.
            search_start_key = pause_key_and_is_drained.first.GetKey<KeyT>();
        }

        bool slice_pinned = false;
        bool succ = false;
        const KeyT *slice_end_key = req_end_key;
        bool is_last_slice = false;
        bool is_scan_mem_full = false;
        std::tie(key_it,
                 slice_end_it,
                 slice_end_key,
                 slice_pinned,
                 is_last_slice,
                 succ) = find_non_empty_slice(*search_start_key, false);
        if (!succ)
        {
            // The request is blocked by pin slice.
            return false;
        }

        uint64_t recycle_ts = 1U;
        if (shard_->EnableMvcc() && !req.export_base_table_item_only_)
        {
            recycle_ts = shard_->GlobalMinSiTxStartTs();
        }

        // The following flag is used to mark the behavior of one slice.
        // Only need to export the key if the key is already persisted, this
        // will happen when the slice need to split, and should export all the
        // keys in this slice to get the subslice keys.
        bool export_persisted_key_only =
            !req.export_base_table_item_ && slice_pinned;
        assert(key_it != slice_end_it || is_last_slice);

        // 3. Loop to scan keys
        // DataSyncScanCc is running on TxProcessor thread. To avoid
        // blocking other transaction for a long time, we only process
        // CkptScanBatch number of pages in each round.
        for (size_t scan_cnt = 0;
             key_it != slice_end_it &&
             scan_cnt < RangePartitionDataSyncScanCc::DataSyncScanBatchSize &&
             req.accumulated_scan_cnt_.at(shard_->core_id_) <
                 req.scan_batch_size_;
             ++scan_cnt)
        {
            const KeyT *key = key_it->first;
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce =
                key_it->second;

            if (shard_->EnableMvcc() && !req.export_base_table_item_only_)
            {
                cce->KickOutArchiveRecords(recycle_ts);
            }

            // Check whether need export this ccentry and fix the data store
            // size.
            bool need_export = cce->CommitTs() <= req.data_sync_ts_ &&
                               (slice_pinned || cce->NeedCkpt());
            if (RangePartitioned &&
                cce->entry_info_.DataStoreSize() == INT32_MAX && need_export)
            {
                if (slice_pinned)
                {
                    // Update unknown data store size since slice is already
                    // pinned
                    cce->entry_info_.SetDataStoreSize(0);
                }
                else if ((table_name_.Type() == TableType::Secondary ||
                          table_name_.Type() == TableType::UniqueSecondary) &&
                         cce->CommitTs() <= SchemaTs() && cce->NeedCkpt())
                {
                    // `cce->CommitTs() <= SchemaTs() && cce->NeedCkpt()` means
                    // the secondary index is in dirty state.
                    // If the data store size of the cce is unknown at this
                    // time, it means the cce was inserted into the ccmap
                    // between the execution of the `ScanSliceDeltaSizeCc` and
                    // `DataSyncScanCc` requests. Just skip this cce in this
                    // round ckpt.
                    need_export = false;
                }
                else
                {
                    LOG(ERROR)
                        << "Dead branch with data store size is unknown.";
                    assert(false);
                }
            }

            // Export this item if need.
            if (need_export)
            {
                assert(cce->entry_info_.DataStoreSize() != INT32_MAX);
                uint64_t flush_size = 0;
                auto export_result =
                    ExportForCkpt(cce,
                                  *key,
                                  req.DataSyncVec(shard_->core_id_),
                                  req.ArchiveVec(shard_->core_id_),
                                  req.MoveBaseIdxVec(shard_->core_id_),
                                  req.data_sync_ts_,
                                  recycle_ts,
                                  shard_->EnableMvcc(),
                                  req.accumulated_scan_cnt_[shard_->core_id_],
                                  req.export_base_table_item_,
                                  req.export_base_table_item_only_,
                                  export_persisted_key_only,
                                  flush_size);

                req.accumulated_flush_data_size_[shard_->core_id_] +=
                    flush_size;

                if (export_result.second)
                {
                    is_scan_mem_full = true;
                    break;
                }
            }

            // Forward the iterator
            ++key_it;

            // Check whether reach to the end of the current slice
            if (key_it == slice_end_it)
            {
                if (slice_pinned)
                {
                    req.slice_ids_[shard_->core_id_].Unpin();
                    req.slice_ids_[shard_->core_id_].Reset();
                    slice_pinned = false;
                }

                if (!is_last_slice)
                {
                    // Reach to the end of current slice, and find the next
                    // slice.
                    search_start_key = slice_end_key;
                    std::tie(key_it,
                             slice_end_it,
                             slice_end_key,
                             slice_pinned,
                             is_last_slice,
                             succ) = find_non_empty_slice(*search_start_key);
                    if (!succ)
                    {
                        // The request is blocked by pin slice.
                        return false;
                    }

                    export_persisted_key_only =
                        !req.export_base_table_item_ && slice_pinned;
                }
            }
        } /* End of loop */

        // 4. Check whether the request is finished.
        TxKey next_pause_key;
        bool no_more_data = (key_it == slice_end_it);
        if (!no_more_data)
        {
            next_pause_key = key_it->first->CloneTxKey();
        }
        else if (slice_pinned)
        {
            assert(no_more_data);
            // Unpin slice
            req.slice_ids_[shard_->core_id_].Unpin();
            req.slice_ids_[shard_->core_id_].Reset();
        }

        // Set the pause_pos_ to mark resume position.
        pause_key_and_is_drained = {std::move(next_pause_key), no_more_data};

        if (is_scan_mem_full)
        {
            req.scan_heap_is_full_[shard_->core_id_] = 1;
            req.SetFinish(shard_->core_id_);
            return false;
        }

        if (no_more_data ||
            req.accumulated_scan_cnt_[shard_->core_id_] >= req.scan_batch_size_)
        {
            req.SetFinish(shard_->core_id_);
        }
        else
        {
            // Put DataSyncScanCc request into CcQueue again.
            shard_->Enqueue(&req);
        }

        // Access DataSyncScanCc member variable is unsafe after
        // SetFinished(...).
        return false;
    }

    bool Execute(HashPartitionDataSyncScanCc &req) override
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

        if (req.schema_version_ != 0 &&
            table_schema_->Version() != req.schema_version_)
        {
            assert(req.schema_version_ > table_schema_->Version());
            LOG(WARNING) << "Table schema version mismatched for data sync "
                            "scan, table name: "
                         << req.table_name_->String()
                         << " ,scan carried version: " << req.schema_version_
                         << " ,ccmap version: " << table_schema_->Version();
            req.SetError(CcErrorCode::REQUESTED_TABLE_SCHEMA_MISMATCH);
            return false;
        }

        if (req.IsDrained())
        {
            // scan is already finished on this core
            req.SetFinish();
            return false;
        }

        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        int64_t standby_node_term = Sharder::Instance().StandbyNodeTerm();
        int64_t current_term = std::max(ng_term, standby_node_term);

        if (current_term < 0 || current_term != req.node_group_term_)
        {
            req.SetError(CcErrorCode::TX_NODE_NOT_LEADER);
            return false;
        }
        Iterator it;
        Iterator end_it = End();

        auto &pause_pos_and_is_drained = req.PausePos();

        if (pause_pos_and_is_drained.first == nullptr)
        {
            // If this is a new scan cc, start from the negative inf.
            it = Begin();
        }
        else
        {
            KeyGapLockAndExtraData *pause_lock_struct =
                pause_pos_and_is_drained.first;
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>
                *pause_entry = static_cast<
                    CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                    pause_lock_struct->GetCcEntry());
            if (pause_entry == nullptr)
            {
                // the pause_entry may be kicked by other operation such as
                // KickoutBucketData.
                req.SetError(CcErrorCode::TASK_EXPIRED);
                return false;
            }

            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                static_cast<
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                    pause_lock_struct->GetCcPage());
            it = Iterator(pause_entry, ccp, &neg_inf_);
            DecrReadIntent(
                pause_entry->GetKeyLock(), pause_entry, req.Txn(), cc_ng_id_);
            if (req.IsTerminated())
            {
                // Just release this read intent.
                req.SetFinish();
                return false;
            }
        }

        // Since we might skip the page that end_it is on if it's not
        // updated since last ckpt, it might skip end_it. If the last
        // page is skipped it will be set as the first entry on the next
        // page. Also check if (it == end_it_next_page_it).
        Iterator end_it_next_page_it = end_it;
        if (end_it_next_page_it != End())
        {
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                end_it_next_page_it.GetPage();
            assert(ccp != nullptr);
            if (ccp->next_page_ == PagePosInf())
            {
                end_it_next_page_it = End();
            }
            else
            {
                end_it_next_page_it = Iterator(ccp->next_page_, 0, &neg_inf_);
            }
        }

        uint64_t recycle_ts = 1U;
        if (shard_->EnableMvcc())
        {
            recycle_ts = shard_->GlobalMinSiTxStartTs();
        }

        // DataSyncScanCc is running on TxProcessor thread. To avoid
        // blocking other transaction for a long time, we only process
        // CkptScanBatch number of pages in each round.
        //
        // indicate if export cce failed due to oom
        bool is_scan_mem_full = false;
        bool replay_cmds_notnull = false;

        size_t export_data_cnt = 0;
        auto l_start = std::chrono::high_resolution_clock::now();

        for (size_t scan_cnt = 0;
             scan_cnt < HashPartitionDataSyncScanCc::DataSyncScanBatchSize &&
             req.accumulated_scan_cnt_ < req.scan_batch_size_ && it != end_it &&
             it != end_it_next_page_it;
             scan_cnt++)
        {
            if (export_data_cnt > 0 && export_data_cnt % 4 == 0)
            {
                auto l_now = std::chrono::high_resolution_clock::now();
                if (std::chrono::duration_cast<std::chrono::microseconds>(
                        l_now - l_start)
                        .count() >= std::chrono::microseconds(50).count())
                {
                    break;
                }
            }

            const KeyT *key = it->first;
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce =
                it->second;
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                it.GetPage();
            assert(ccp);

            if (ccp->last_dirty_commit_ts_ <= req.previous_ckpt_ts_ &&
                !req.include_persisted_data_)
            {
                // Skip the pages that have no updates since last data
                // sync.
                if (ccp->next_page_ == PagePosInf())
                {
                    it = End();
                }
                else
                {
                    it = Iterator(ccp->next_page_, 0, &neg_inf_);
                }
                continue;
            }

            if (VersionedRecord)
            {
                if (shard_->EnableMvcc())
                {
                    cce->KickOutArchiveRecords(recycle_ts);
                }

                if ((!req.filter_lambda_ || req.filter_lambda_(key->Hash())) &&
                    (cce->NeedCkpt() || req.include_persisted_data_))
                {
                    uint64_t flush_data_size = 0;
                    auto export_result =
                        ExportForCkpt(cce,
                                      *key,
                                      req.DataSyncVec(),
                                      req.ArchiveVec(),
                                      req.MoveBaseIdxVec(),
                                      req.data_sync_ts_,
                                      recycle_ts,
                                      shard_->EnableMvcc(),
                                      req.accumulated_scan_cnt_,
                                      req.include_persisted_data_,
                                      false,
                                      false,
                                      flush_data_size);
                    req.accumulated_flush_data_size_ += flush_data_size;

                    if (export_result.second)
                    {
                        is_scan_mem_full = true;
                        break;
                    }
                }
            }
            else
            {
                uint64_t key_hash = key->Hash();
                if (!req.filter_lambda_ || req.filter_lambda_(key_hash))
                {
                    if (cce->HasBufferedCommandList())
                    {
                        DLOG(INFO)
                            << "cce has buffer command, skip "
                               "ckpt cce: "
                            << cce->KeyString()
                            << ", cce status: " << int(cce->PayloadStatus())
                            << ", BufferCommand: " << cce->BufferedCommandList()
                            << ", data_sync_ts_: " << req.data_sync_ts_;
                        replay_cmds_notnull = true;

                        // The fetch record may failed when the
                        // cce is touch at 1st place, so the record status can
                        // be Unknown. If the data is owned by this ng, fetch
                        // the record, otherwise only skip this record for now
                        // and don't truncate redo log.
                        if (cce->PayloadStatus() == RecordStatus::Unknown)
                        {
                            uint16_t bucket_id =
                                Sharder::Instance().MapKeyHashToBucketId(
                                    key_hash);
                            if (shard_->GetBucketOwner(bucket_id, cc_ng_id_) ==
                                    cc_ng_id_ &&
                                current_term > 0)
                            {
                                cce->GetOrCreateKeyLock(shard_, this, ccp);
                                TxKey tx_key(key);
                                int32_t part_id =
                                    Sharder::MapKeyHashToHashPartitionId(
                                        key->Hash());
                                shard_->FetchRecord(table_name_,
                                                    table_schema_,
                                                    TxKey(key),
                                                    cce,
                                                    cc_ng_id_,
                                                    ng_term,
                                                    nullptr,
                                                    part_id);
                            }
                        }
                        else if (ng_term > 0)
                        {
                            // After node escalate to leader, and we've loaded
                            // from kv, there should be no gap in the buffered
                            // cmd list.
                            LOG(ERROR)
                                << "Buffered cmds found on leader node"
                                << ", cce key: " << cce->KeyString()
                                << ", cce CommitTs: " << cce->CommitTs() << "\n"
                                << cce->BufferedCommandList();
                            assert(false);
                        }
                        else
                        {
                            // It is normal that we have some buffered cmds in
                            // standby mode since the commands might be handled
                            // out of order. The buffered cmds should soon be
                            // cleared when gap in the command list is closed.
                        }
                    }
                    else if (cce->NeedCkpt())
                    {
                        uint64_t flush_data_size = 0;
                        auto export_result =
                            ExportForCkpt(cce,
                                          *key,
                                          req.DataSyncVec(),
                                          req.ArchiveVec(),
                                          req.MoveBaseIdxVec(),
                                          req.data_sync_ts_,
                                          recycle_ts,
                                          shard_->EnableMvcc(),
                                          req.accumulated_scan_cnt_,
                                          false,
                                          false,
                                          false,
                                          flush_data_size);
                        req.accumulated_flush_data_size_ += flush_data_size;
                        if (export_result.second)
                        {
                            is_scan_mem_full = true;
                            break;
                        }

                        export_data_cnt++;
                    }
                }
            }

            // Forward iterator
            it++;
        }

        if (replay_cmds_notnull)
        {
            assert(!VersionedRecord);
            req.SetNotTruncateLog();
        }

        bool no_more_data = (it == end_it) || (it == end_it_next_page_it);

        if (no_more_data)
        {
            pause_pos_and_is_drained = {nullptr, true};
            // scan data drained
            req.SetFinish();
            // Access DataSyncScanCc member variable is unsafe after
            // SetFinished(...).
            return false;
        }
        else
        {
            // set the pause_key_ to mark resume position
            assert(pause_pos_and_is_drained.second == false);
            AcquireReadIntent(
                it->second, it.GetPage(), req.Txn(), req.NodeGroupTerm());

            // TODO: use lock_ptr_ when we allow the ccentry to move to another
            // memory
            pause_pos_and_is_drained.first =
                it->second->GetKeyGapLockAndExtraData();
            if (is_scan_mem_full)
            {
                //  scan memory is full and there are
                //  data for flush
                req.scan_heap_is_full_ = 1;
                req.SetFinish();
                return false;
            }
            else if (req.accumulated_scan_cnt_ < req.scan_batch_size_)
            {
                // Put DataSyncScanCc request into CcQueue again.
                shard_->Enqueue(&req);
            }
            else
            {
                // scan data is not drained
                req.SetFinish();
                return false;
            }
        }

        return false;
    }

    bool Execute(DefragShardHeapCc &req) override
    {
        mi_heap_t *heap = mi_heap_get_default();

        auto &pause_pos = req.PausePos();
        auto &defrag_cnt = req.defrag_cnt_;
        auto &lock_cnt = req.lock_cnt_;
        auto &kv_load_cnt = req.kv_load_cnt_;
        auto &ckpt_cnt = req.ckpt_cnt_;
        auto &no_frag_cnt = req.non_frag_cnt_;
        auto &total_cnt = req.total_cnt_;
        bool &ccmp_key_defraged = req.ccmp_key_defraged_;

        // Defrag the keys in ccmp at first
        if (!ccmp_key_defraged)
        {
            BtreeMapIterator it;
            TxKey &resume_tx_key = pause_pos.first;
            if (resume_tx_key.KeyPtr() == nullptr)
            {
                it = ccmp_.begin();
            }
            else
            {
                const KeyT *resume_key = resume_tx_key.GetKey<KeyT>();
                it = ccmp_.upper_bound(*resume_key);
                if (it != ccmp_.begin())
                {
                    it--;
                }
            }

            size_t current_defrag_cnt = 0;
            auto l_start = std::chrono::high_resolution_clock::now();

            // defrag ccmap key
            for (size_t scan_cnt = 0;
                 scan_cnt < req.scan_batch_size_ && it != ccmp_.end();
                 scan_cnt++)
            {
                if (current_defrag_cnt > 0 && current_defrag_cnt % 4 == 0)
                {
                    auto l_now = std::chrono::high_resolution_clock::now();
                    if (std::chrono::duration_cast<std::chrono::microseconds>(
                            l_now - l_start)
                            .count() >= std::chrono::microseconds(50).count())
                    {
                        break;
                    }
                }

                const KeyT &key = it->first;
                TxKey tx_key(&key);
                if (tx_key.NeedsDefrag(heap))
                {
                    KeyT key_clone = key;

                    // move out data from the old page,
                    std::unique_ptr<
                        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>>
                        old_cc_page_uptr = std::move(it->second);
                    // remove the old cc page entry
                    it = ccmp_.erase(it);

                    float cc_page_utilization =
                        mi_heap_page_utilization(heap, old_cc_page_uptr.get());

                    if (!(cc_page_utilization < 0.8))
                    {
                        it = ccmp_.try_emplace(
                            it, key_clone, std::move(old_cc_page_uptr));
                    }
                    else
                    {
                        // emplace new key/ccpage in the ccmp_
                        it = ccmp_.try_emplace(
                            it,
                            key_clone,
                            std::make_unique<CcPage<KeyT,
                                                    ValueT,
                                                    VersionedRecord,
                                                    RangePartitioned>>(
                                this,
                                std::move(old_cc_page_uptr->keys_),
                                std::move(old_cc_page_uptr->entries_),
                                old_cc_page_uptr->prev_page_,
                                old_cc_page_uptr->next_page_));

                        // prepare for ~CcPage()
                        old_cc_page_uptr->next_page_ = nullptr;
                        old_cc_page_uptr->prev_page_ = nullptr;

                        auto new_cc_page_ptr = it->second.get();
                        assert(new_cc_page_ptr->keys_.size() != 0);
                        new_cc_page_ptr->last_access_ts_ =
                            old_cc_page_uptr->last_access_ts_;

                        // replace old_cc_page_ptr with new_cc_page_ptr in LRU
                        // list
                        if (old_cc_page_uptr->lru_next_ != nullptr)
                        {
                            shard_->ReplaceLru(old_cc_page_uptr.get(),
                                               new_cc_page_ptr);
                        }
                        else
                        {
                            shard_->UpdateLruList(new_cc_page_ptr, false);
                        }

                        // Update the page pointer in every cce which has lock
                        for (auto &cce : new_cc_page_ptr->entries_)
                        {
                            cce->UpdateCcPage(new_cc_page_ptr);
                        }

                        if (shard_->CleanStart() == old_cc_page_uptr.get())
                        {
                            // Update the pointer of clean start ccp.
                            shard_->ResetCleanStart(new_cc_page_ptr);
                        }
                    }

                    current_defrag_cnt++;
                }
                it++;
            }

            if (it == ccmp_.end())
            {
                ccmp_key_defraged = true;
                pause_pos = {TxKey(), false};
            }
            else
            {
                const KeyT &key = it->first;
                TxKey pause_key(&key);
                pause_pos = {pause_key.Clone(), false};
            }

            shard_->Enqueue(&req);
        }
        else
        {
            // then defrag the cc page
            Iterator it;
            Iterator end_it = End();

            TxKey &resume_tx_key = pause_pos.first;
            if (resume_tx_key.KeyPtr() == nullptr)
            {
                it = Begin();
                it++;
            }
            else
            {
                const KeyT *resume_key = resume_tx_key.GetKey<KeyT>();
                std::pair<Iterator, ScanType> resume_pair =
                    ForwardScanStart(*resume_key, true);
                it = resume_pair.first;
                if (it.GetPage() == &neg_inf_page_)
                {
                    it++;
                }
            }

            size_t current_defrag_cnt = 0;
            auto l_start = std::chrono::high_resolution_clock::now();

            for (size_t scan_cnt = 0;
                 scan_cnt < req.scan_batch_size_ && it != end_it;
                 scan_cnt++)
            {
                if (current_defrag_cnt > 0 && current_defrag_cnt % 4 == 0)
                {
                    auto l_now = std::chrono::high_resolution_clock::now();
                    if (std::chrono::duration_cast<std::chrono::microseconds>(
                            l_now - l_start)
                            .count() >= std::chrono::microseconds(50).count())
                    {
                        break;
                    }
                }

                bool defraged = false;
                // defrag the keys_ and entries_ when enter a new page
                if (it != end_it && it.GetIdxInPage() == 0)
                {
                    auto current_page = it.GetPage();
                    float keys_utilization = mi_heap_page_utilization(
                        heap, current_page->keys_.data());
                    if (keys_utilization < 0.8)
                    {
                        std::vector<KeyT> new_keys;
                        new_keys.reserve(current_page->keys_.capacity());
                        for (auto &key : current_page->keys_)
                        {
                            new_keys.push_back(std::move(key));
                        }
                        current_page->keys_.swap(new_keys);
                        defraged = true;
                    }

                    float entries_utilization = mi_heap_page_utilization(
                        heap, current_page->entries_.data());
                    if (entries_utilization < 0.8)
                    {
                        std::vector<std::unique_ptr<CcEntry<KeyT,
                                                            ValueT,
                                                            VersionedRecord,
                                                            RangePartitioned>>>
                            new_entries;
                        new_entries.reserve(current_page->entries_.capacity());
                        for (auto &entry : current_page->entries_)
                        {
                            new_entries.push_back(std::move(entry));
                        }
                        current_page->entries_.swap(new_entries);
                        defraged = true;
                    }

                    if (defraged)
                    {
                        it.UpdateCurrent();
                    }
                }

                auto rs = it.DefragCurrentIfNecessary(heap);

                if (rs == DefragResult::FAILED_BY_LOCK)
                {
                    lock_cnt++;
                }
                else if (rs == DefragResult::FAILED_BY_CKPT)
                {
                    ckpt_cnt++;
                }
                else if (rs == DefragResult::FAILED_BY_KV_LOADING)
                {
                    kv_load_cnt++;
                }
                else if (rs == DefragResult::DEFRAGED)
                {
                    defrag_cnt++;
                    defraged = true;
                }
                else if (rs == DefragResult::NOFRAGED)
                {
                    no_frag_cnt++;
                }

                if (defraged)
                {
                    current_defrag_cnt++;
                }

                total_cnt++;

                // Forward iterator
                it++;
            }

            if (it == end_it)
            {
                pause_pos = {TxKey(), true};
            }
            else
            {
                // set the pause_key_ to mark resume position
                assert(pause_pos.second == false);
                TxKey pause_key(it->first);
                pause_pos.first = pause_key.Clone();
                pause_pos.second = false;
            }

            shard_->Enqueue(&req);
        }

        return false;
    }

    bool Execute(BroadcastStatisticsCc &req) override
    {
        assert(false && "CatalogCcMap::Execute(BroadcastStatisticsCc &) only");
        return true;
    }

    /**
     * 1. Sample slices by iterate all ranges of current node group.
     * 2. For each sampled slice, sample key by iterate it on every cc_shard.
     * 3. Estimate table records of current node group and apply records and
     * sample keys.
     */
    bool Execute(AnalyzeTableAllCc &req) override
    {
        CcHandlerResult<Void> *hd_res = req.Result();
        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        assert(table_name_ == *req.GetTableName());
        assert(req.NodeGroupId() == cc_ng_id_);

        // Both AnalyzeTableAllCc::key_sample_pool_ and
        // AnalyzeTableAllCc::slice_sample_pool_ are unique_ptr to a abstract
        // class. But TemplateCcMap::Execute(AnalyzeTableAllCc) expects concrete
        // class.
        using KeySamplePool = AnalyzeTableAllCc::
            SamplePool<KeyT, typename TemplateCcMapSamplePool<KeyT>::CopyKey>;
        using StoreSliceSamplePool = AnalyzeTableAllCc::SamplePool<
            std::pair<int32_t /*range_id*/, const TemplateStoreSlice<KeyT> *>,
            Copy<std::pair<int32_t, const TemplateStoreSlice<KeyT> *>>>;

        KeySamplePool *key_sample_pool = [&req]
        {
            if (req.key_sample_pool_ == nullptr)
            {
                req.key_sample_pool_ = std::make_unique<KeySamplePool>(
                    AnalyzeTableAllCc::sample_pool_capacity_);
            }
            return static_cast<KeySamplePool *>(req.key_sample_pool_.get());
        }();

        StoreSliceSamplePool *store_slice_sample_pool = [&req]
        {
            if (req.store_slice_sample_pool_ == nullptr)
            {
                req.store_slice_sample_pool_ =
                    std::make_unique<StoreSliceSamplePool>(
                        AnalyzeTableAllCc::sample_pool_capacity_);
            }
            return static_cast<StoreSliceSamplePool *>(
                req.store_slice_sample_pool_.get());
        }();

        if (!req.built_slice_sample_pool_)
        {
            // All ranges has been added read lock. It is safe to access
            // them. Also since all ranges are locked, the range map in
            // local cc shards will not change so we can trust the map
            // iterator after cc req resumes.
            if (req.PinStoreRanges(shard_))
            {
                for (const TableRangeEntry *range_entry :
                     req.PinnedStoreRanges())
                {
                    const TemplateTableRangeEntry<KeyT> *typed_range_entry =
                        static_cast<const TemplateTableRangeEntry<KeyT> *>(
                            range_entry);
                    const TemplateStoreRange<KeyT> *store_range =
                        typed_range_entry->TypedStoreRange();
                    assert(store_range != nullptr);

                    // Total slice count is unknown, thus it is unsuitable to
                    // use Kunth's algorithm S. Have to use an reservoir
                    // sampling algorithm here.
                    for (const auto &store_slice : store_range->TypedSlices())
                    {
                        // Since StoreRange has been pinned, it is safe to keep
                        // pointer to StoreSlice into StoreSliceSamplePool.
                        store_slice_sample_pool->Insert(std::make_pair(
                            store_range->PartitionId(), store_slice.get()));
                    }
                }

                req.built_slice_sample_pool_ = true;
                assert(req.pin_slice_idx_ == 0);
            }
            else
            {
                // Yield tx_processor and retry after all StoreRange on
                // cc_ng_id_ have been pinned.
                return false;
            }
        }

        // Pin-slice is necessary. On one hand, pin-slice would
        // guarantee enough samples, even if the amount of sampled slice
        // is very few. On the other hand, pin-slice would help
        // estimating average bytes of records.
        //
        // Capacity of slice sample pool cannot be too small. Otherwise
        // the final sampled keys could not reflect original key
        // distribution. Capacity of slice sample pool cannot be too
        // large. Otherwise too many pin-slice calls can lead to too
        // many accesses to storage.
        if (req.pin_slice_idx_ < store_slice_sample_pool->SampleKeys().size())
        {
            const auto [range_id, store_slice] =
                store_slice_sample_pool->SampleKeys().at(req.pin_slice_idx_);

            const KeyT *store_slice_start_key = store_slice->StartKey();
            assert(store_slice_start_key != nullptr);
            const KeyT *store_slice_end_key = store_slice->EndKey();
            assert(store_slice_end_key != nullptr);
            const KeyT *scan_start_key = req.continue_key_.KeyPtr()
                                             ? req.continue_key_.GetKey<KeyT>()
                                             : store_slice_start_key;

            RangeSliceOpStatus pin_status;
            const StoreSlice *last_pinned_slice;
            RangeSliceId slice_id = shard_->local_shards_.PinRangeSlices(
                table_name_,
                cc_ng_id_,
                ng_term,
                KeySchema(),
                table_schema_->RecordSchema(),
                schema_ts_,
                table_schema_->GetKVCatalogInfo(),
                range_id,
                *scan_start_key,
                true,
                store_slice_end_key,
                false,
                &req,
                shard_,
                false,
                0,
                1,
                true,
                pin_status,
                last_pinned_slice);

            if (pin_status == RangeSliceOpStatus::Retry)
            {
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
            }
            else if (pin_status == RangeSliceOpStatus::Delay)
            {
                if (slice_id.Range()->HasLock())
                {
                    hd_res->SetError(CcErrorCode::OUT_OF_MEMORY);
                    return true;
                }
                else
                {
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
                }
            }
            else if (pin_status == RangeSliceOpStatus::BlockedOnLoad)
            {
                return false;
            }
            else if (pin_status == RangeSliceOpStatus::Error ||
                     pin_status == RangeSliceOpStatus::NotOwner)
            {
                // If the pin operation returns an error, the data store
                // is inaccessible.
                hd_res->SetError(CcErrorCode::PIN_RANGE_SLICE_FAILED);
                return true;
            }
            else
            {
                assert(pin_status == RangeSliceOpStatus::Successful);
                if (req.pinned_slice_id_ != slice_id)
                {
                    req.pinned_slice_id_ = slice_id;

                    if (shard_->core_id_ == shard_->core_cnt_ - 1)
                    {
                        req.visit_slices_ += 1;
                    }
                }
            }

            uint32_t n = 0;
            constexpr uint32_t scan_batch_keys = 1024;

            auto [iter, scan_type] = ForwardScanStart(*scan_start_key, true);

            TemplateStoreSlice<KeyT> *typed_slice =
                static_cast<TemplateStoreSlice<KeyT> *>(
                    req.pinned_slice_id_.Slice());
            const KeyT *slice_end_key = typed_slice->EndKey();
            assert(slice_end_key != nullptr);

            while (iter != End() && *iter->first < *store_slice_end_key &&
                   *iter->first < *slice_end_key && n < scan_batch_keys)
            {
                const KeyT &key = *iter->first;
                const CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>
                    &cc_entry = *iter->second;

                if (cc_entry.PayloadStatus() == RecordStatus::Normal &&
                    key.Type() == KeyType::Normal)
                {
                    key_sample_pool->Insert(key);

                    req.visit_keys_ += 1;
                }
                ++iter;
                ++n;
            }

            req.pinned_slice_id_.Unpin();

            if (*iter->first < *store_slice_end_key)
            {
                // Yield tx_processor
                assert(req.continue_key_.KeyPtr() == nullptr ||
                       *req.continue_key_.GetKey<KeyT>() != *iter->first);
                req.continue_key_ = iter->first->CloneTxKey();
                shard_->Enqueue(shard_->LocalCoreId(), &req);
            }
            else
            {
                req.continue_key_ = TxKey();
                req.pinned_slice_id_.Reset();

                if (shard_->core_id_ < shard_->core_cnt_ - 1)
                {
                    // Iterate next cc_shards.
                    req.ResetCcm();
                    MoveRequest(&req, shard_->core_id_ + 1);
                }
                else
                {
                    // Iterate next StoreSlice.
                    ++req.pin_slice_idx_;
                    req.ResetCcm();
                    MoveRequest(&req, 0);
                }
            }

            return false;
        }
        else
        {
            uint16_t leader_core = Statistics::LeaderCore(table_name_);
            if (shard_->core_id_ != leader_core)
            {
                req.ResetCcm();
                MoveRequest(&req, leader_core);
                return false;
            }

            uint64_t node_group_records = 0;
            if (req.visit_slices_ > 0)
            {
                uint64_t ng_slices =
                    shard_->CountSlices(table_name_, cc_ng_id_, cc_ng_id_);
                node_group_records =
                    (ng_slices * req.visit_keys_ + req.visit_slices_ - 1) /
                    req.visit_slices_;  // Ceiling divide
            }

            sample_pool_->Reset(std::move(key_sample_pool->random_pairing_),
                                node_group_records,
                                KeySchema());

            req.UnPinStoreRanges();
            hd_res->SetFinished();
            return true;
        }
    }

    bool Execute(ReloadCacheCc &req) override
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

        return true;
    }

    bool Execute(FaultInjectCC &req) override
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

        return true;
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

        KeyT key;
        // A psuedo record that is used to deserialize and move forward
        // the record that is not sharded to the core.
        ValueT rec;
        size_t offset = 0;
        uint16_t next_core = UINT16_MAX;
        const std::string_view &log_blob = req.LogContentView();

        // If the log record's commit ts is smaller than that of the cc
        // map, this record is generated before the latest schema of the
        // table and hence should skip the replay process.
        if (req.CommitTs() < schema_ts_)
        {
            req.SetFinish();
            return true;
        }

        while (offset < log_blob.size())
        {
            key.Deserialize(log_blob.data(), offset, KeySchema());
            uint8_t op_val =
                *reinterpret_cast<const uint8_t *>(log_blob.data() + offset);
            OperationType op_type = static_cast<OperationType>(op_val);
            assert(op_type == OperationType::Insert ||
                   op_type == OperationType::Update ||
                   op_type == OperationType::Delete);

            offset += sizeof(uint8_t);

            uint16_t core_id = (key.Hash() & 0x3FF) % shard_->core_cnt_;
            if (core_id != shard_->core_id_)
            {
                // Skips the key in the log record that is not sharded
                // to this core.
                if (op_type == OperationType::Insert ||
                    op_type == OperationType::Update)
                {
                    rec.Deserialize(log_blob.data(), offset);
                }
                if (shard_->core_id_ == req.FirstCore() ||
                    (core_id != req.FirstCore() && core_id > shard_->core_id_))
                {
                    // Move to the smallest unvisited core id
                    next_core = std::min(core_id, next_core);
                }
                continue;
            }

            // Skip records that no longer belong to this ng.
            if (RangePartitioned)
            {
                const TableRangeEntry *range_entry = shard_->GetTableRangeEntry(
                    table_name_, cc_ng_id_, TxKey(&key));

                const BucketInfo *bucket_info = shard_->GetBucketInfo(
                    Sharder::MapRangeIdToBucketId(
                        range_entry->GetRangeInfo()->PartitionId()),
                    cc_ng_id_);
                // Check if range bucket belongs to this ng or is migrating
                // to this ng.
                if (bucket_info->BucketOwner() != cc_ng_id_ &&
                    bucket_info->DirtyBucketOwner() != cc_ng_id_)
                {
                    int32_t new_range_id =
                        range_entry->GetRangeInfo()->GetKeyNewRangeId(
                            TxKey(&key));
                    // If range is splitting, check if new range belongs to
                    // this ng.
                    if (new_range_id >= 0)
                    {
                        const BucketInfo *new_bucket_info =
                            shard_->GetBucketInfo(
                                Sharder::MapRangeIdToBucketId(
                                    range_entry->GetRangeInfo()->PartitionId()),
                                cc_ng_id_);
                        if (new_bucket_info->BucketOwner() != cc_ng_id_ &&
                            new_bucket_info->DirtyBucketOwner() != cc_ng_id_)
                        {
                            if (op_type != OperationType::Delete)
                            {
                                rec.Deserialize(log_blob.data(), offset);
                            }
                            continue;
                        }
                    }
                }
            }
            else
            {
                uint16_t bucket_id = Sharder::MapKeyHashToBucketId(key.Hash());
                const BucketInfo *bucket_info =
                    shard_->GetBucketInfo(bucket_id, cc_ng_id_);
                if (bucket_info->BucketOwner() != cc_ng_id_ &&
                    bucket_info->DirtyBucketOwner() != cc_ng_id_)
                {
                    continue;
                }
            }

            Iterator it = FindEmplace(key);
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce =
                it->second;
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                it.GetPage();

            // For orphan lock recovery, verify if the transaction still holds
            // the lock on this CC entry.
            if (req.IsLockRecovery())
            {
                if (const NonBlockingLock *key_lock =
                        cce != nullptr ? cce->GetKeyLock() : nullptr;
                    key_lock == nullptr ||
                    !key_lock->HasWriteLockOrWriteIntent(req.Txn()))
                {
                    if (op_type != OperationType::Delete)
                    {
                        rec.Deserialize(log_blob.data(), offset);
                    }
                    continue;
                }
            }

            if (cce == nullptr)
            {
                // Since we're not holding any range lock that would
                // block data sync during replay, just keep retrying
                // until we have free space in cc map.
                shard_->EnqueueWaitListIfMemoryFull(&req);
                return false;
            }

            if (cce->CommitTs() >= req.CommitTs())
            {
                // If the key exists in the cc map and its commit ts is
                // greater than that of the log record, and if (1) mvcc
                // is enabled, then install  the log record into
                // archives; (2) mvcc is not enabled, then skips
                // installing the log record in the cc map and moves to
                // the next key in the log record.
                if (shard_->EnableMvcc())
                {
                    assert(VersionedRecord);
                    auto rec_ptr = std::make_unique<ValueT>();
                    RecordStatus rec_status = RecordStatus::Normal;
                    if (op_type == OperationType::Insert ||
                        op_type == OperationType::Update)
                    {
                        rec_ptr->Deserialize(log_blob.data(), offset);
                    }
                    else
                    {
                        rec_status = RecordStatus::Deleted;
                    }
                    if (cce->CommitTs() > req.CommitTs())
                    {
                        cce->AddArchiveRecord(
                            std::move(rec_ptr), rec_status, req.CommitTs());
                    }
                }
                else if (op_type == OperationType::Insert ||
                         op_type == OperationType::Update)
                {
                    rec.Deserialize(log_blob.data(), offset);
                }
            }
            else
            {
                if (VersionedRecord && shard_->EnableMvcc())
                {
                    cce->ArchiveBeforeUpdate();
                }
                RecordStatus rec_status;
                if (op_type == OperationType::Insert ||
                    op_type == OperationType::Update)
                {
                    cce->payload_.DeserializeCurrentPayload(log_blob.data(),
                                                            offset);
                    rec_status = RecordStatus::Normal;
                }
                else
                {
                    if (Type() != TableType::Secondary)
                    {
                        cce->payload_.SetCurrentPayload(nullptr);
                    }
                    rec_status = RecordStatus::Deleted;
                }
                const uint64_t commit_ts = req.CommitTs();
                cce->SetCommitTsPayloadStatus(commit_ts, rec_status);

                if (commit_ts > last_dirty_commit_ts_)
                {
                    last_dirty_commit_ts_ = commit_ts;
                }
                if (commit_ts > ccp->last_dirty_commit_ts_)
                {
                    ccp->last_dirty_commit_ts_ = commit_ts;
                }
                if (sample_pool_)
                {
                    if (op_type == OperationType::Insert)
                    {
                        sample_pool_->OnInsert(*shard_, key, KeySchema());
                    }
                    else if (op_type == OperationType::Delete)
                    {
                        sample_pool_->OnDelete(*shard_, key, KeySchema());
                    }
                }

                NonBlockingLock *key_lock = cce->GetKeyLock();
                if (key_lock != nullptr && key_lock->HasWriteLock())
                {
                    // If the record in the log has a commit ts greater
                    // than that of the cc entry and the cc entry has a
                    // write lock, the lock's owner must be the tx that
                    // commits the log record.
                    // TODO: it is safer if we ship the tx ID with the
                    // recovering message and match it against the lock
                    // holder.
                    TxNumber txn = key_lock->WriteLockTx();
                    ReleaseCceLock(key_lock,
                                   cce,
                                   txn,
                                   req.NodeGroupId(),
                                   LockType::WriteLock);
                }
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

    bool Execute(CleanCcEntryForTestCc &req) override
    {
        const TxKey *tx_key = req.Key();
        bool only_archives = req.OnlyCleanArchives();
        assert(tx_key != nullptr);
        if (tx_key != nullptr)
        {
            // find cc entry
            const KeyT *typed_key_ptr = tx_key->GetKey<KeyT>();
            const KeyT &key = *typed_key_ptr;
            Iterator it = Find(key);
            const KeyT *cce_key = it->first;
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce =
                it->second;

            if (cce != nullptr)
            {
                if (req.WithFlush())
                {
                    std::vector<FlushRecord> tmp_ckpt_vec(1);
                    size_t flush_size = 0;
                    size_t tmp_ckpt_vec_size = 0;

                    std::vector<FlushRecord> tmp_akv_vec;
                    std::vector<size_t> tmp_mv_base_idx_vec;
                    std::vector<TxKey> tmp_mv_base_key_vec;

                    ExportForCkpt(cce,
                                  *cce_key,
                                  tmp_ckpt_vec,
                                  tmp_akv_vec,
                                  tmp_mv_base_idx_vec,
                                  cce->CommitTs(),
                                  1U,
                                  shard_->EnableMvcc(),
                                  tmp_ckpt_vec_size,
                                  false,
                                  false,
                                  false,
                                  flush_size);

                    assert(tmp_ckpt_vec_size <= 1);
                    size_t offset = 0;
                    for (size_t i = 0; i < tmp_akv_vec.size(); ++i)
                    {
                        auto &rec = tmp_akv_vec[i];
                        rec.SetKey(
                            tmp_ckpt_vec[rec.GetKeyIndex() + offset].Key());
                    }

                    for (size_t i = 0; i < tmp_mv_base_idx_vec.size(); ++i)
                    {
                        size_t key_idx = tmp_mv_base_idx_vec[i];
                        TxKey key_raw = tmp_ckpt_vec[key_idx].Key();
                        tmp_mv_base_key_vec.emplace_back(std::move(key_raw));
                    }

                    std::shared_ptr<TableSchema> table_schema =
                        shard_->local_shards_.GetSharedTableSchema(table_name_,
                                                                   cc_ng_id_);

                    std::shared_ptr<DataSyncTask> data_sync_task =
                        std::make_shared<DataSyncTask>(
                            table_name_,
                            1,
                            1,
                            cc_ng_id_,
                            Sharder::Instance().LeaderTerm(cc_ng_id_),
                            1,
                            nullptr,
                            false,
                            false,
                            nullptr,
                            nullptr,
                            false,
                            false);

                    std::unique_ptr<FlushTaskEntry> flush_task_entry =
                        std::make_unique<FlushTaskEntry>(
                            std::make_unique<std::vector<FlushRecord>>(
                                std::move(tmp_ckpt_vec)),
                            std::make_unique<std::vector<FlushRecord>>(
                                std::move(tmp_akv_vec)),
                            std::make_unique<
                                std::vector<std::pair<TxKey, int32_t>>>(),
                            nullptr,
                            data_sync_task,
                            table_schema,
                            flush_size);

                    std::unordered_map<
                        std::string_view,
                        std::vector<std::unique_ptr<FlushTaskEntry>>>
                        flush_task_entries;
                    std::string_view kv_table_name =
                        table_schema->GetKVCatalogInfo()->GetKvTableName(
                            table_name_);
                    flush_task_entries.try_emplace(
                        kv_table_name,
                        std::vector<std::unique_ptr<FlushTaskEntry>>());
                    flush_task_entries[kv_table_name].emplace_back(
                        std::move(flush_task_entry));

                    bool res = shard_->FlushEntryForTest(flush_task_entries,
                                                         only_archives);

                    assert(res == true);
                    // This silences the -Wunused-but-set-variable
                    // warning without any runtime overhead.
                    (void) res;
                }
                if (only_archives)
                {
                    cce->ClearArchives();
                }
                else
                {
                    ccm_has_full_entries_ = false;
                    // Clean(cce);
                }
            }
        }
        req.Result()->SetValue(true);
        req.Result()->SetFinished();
        return true;
    }

    bool Execute(FillStoreSliceCc &req) override
    {
        std::deque<SliceDataItem> &slice_vec = req.SliceData(shard_->core_id_);

        size_t index = req.NextIndex(shard_->core_id_);
        size_t last_index = std::min(index + FillStoreSliceCc::MaxScanBatchSize,
                                     slice_vec.size());

        bool success =
            BatchFillSlice(slice_vec, req.ForceLoad(), index, last_index);

        if (!success)
        {
            shard_->EnqueueWaitListIfMemoryFull(&req);
            return false;
        }

        index = last_index;
        if (index == slice_vec.size())
        {
            slice_vec.clear();
            return req.SetFinish(shard_);
        }
        else
        {
            req.SetNextIndex(shard_->core_id_, index);
            shard_->Enqueue(shard_->LocalCoreId(), &req);
            return false;
        }
    }

    bool Execute(InitKeyCacheCc &req) override
    {
        Iterator map_it, map_end_it;
        TxKey &resume_key = req.PauseKey(shard_->core_id_);
        const KeyT *start_key = nullptr;
        if (!resume_key.KeyPtr())
        {
            // First time being processed.
            if (req.Slice().IsValidInKeyCache(shard_->core_id_))
            {
                // No need to init key cache.
                return req.SetFinish(shard_->core_id_, true);
            }
            req.Slice().SetLoadingKeyCache(shard_->core_id_, true);
            start_key = req.Slice().StartTxKey().GetKey<KeyT>();
        }
        else
        {
            start_key = resume_key.GetKey<KeyT>();
        }

        if (start_key == nullptr || start_key->Type() == KeyType::NegativeInf)
        {
            map_it = Begin();
        }
        else
        {
            std::pair<Iterator, ScanType> start_pair =
                ForwardScanStart(*start_key, true);
            map_it = start_pair.first;
            if (start_pair.second == ScanType::ScanGap)
            {
                ++map_it;
            }
        }

        const KeyT *end_key = req.Slice().EndTxKey().GetKey<KeyT>();

        // nullptr end key means PositiveInfinity
        if (end_key == nullptr || end_key->Type() == KeyType::PositiveInf)
        {
            map_end_it = End();
        }
        else
        {
            std::pair<Iterator, ScanType> end_pair =
                ForwardScanStart(*end_key, true);
            map_end_it = end_pair.first;
            if (end_pair.second == ScanType::ScanGap)
            {
                ++map_end_it;
            }
        }
        assert(!end_key || !start_key || *start_key < *end_key);

        TemplateStoreRange<KeyT> *range =
            static_cast<TemplateStoreRange<KeyT> *>(&req.Range());

        for (size_t scan_cnt = 0; scan_cnt < InitKeyCacheCc::MaxScanBatchSize &&
                                  map_it != map_end_it;
             map_it++, scan_cnt++)
        {
            VersionedLruEntry<VersionedRecord, RangePartitioned> *entry =
                static_cast<
                    VersionedLruEntry<VersionedRecord, RangePartitioned> *>(
                    map_it->second);
            if (entry->PayloadStatus() == RecordStatus::Unknown)
            {
                // key cache only contains keys that are inserted to ccm. entry
                // with unknown status are still being loaded from kv.
                continue;
            }
            const KeyT *key = map_it->first;
            auto ret =
                range->AddKey(*key, shard_->core_id_, &req.Slice(), true);
            if (ret == RangeSliceOpStatus::Error)
            {
                // Stop immediately if one of the add key fails.
                return req.SetFinish(shard_->core_id_, false);
            }
        }

        if (map_it == map_end_it)
        {
            return req.SetFinish(shard_->core_id_, true);
        }
        else
        {
            // record pause position and resume in next round.
            TxKey pause_key(map_it->first);
            req.SetPauseKey(pause_key, shard_->core_id_);
            shard_->Enqueue(&req);
            return false;
        }
    }

    bool Execute(KickoutCcEntryCc &req) override
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

        if (req.StartKeyStr() && !req.StartKey())
        {
            // first time processing remote req.
            if (req.StartKeyStr()->empty())
            {
                req.SetStartKey(KeyT::NegativeInfinity());
            }
            else
            {
                const std::string *key_str = req.StartKeyStr();
                assert(key_str != nullptr);
                std::unique_ptr<KeyT> decoded_key = std::make_unique<KeyT>();
                size_t offset = 0;
                decoded_key->Deserialize(key_str->data(), offset, KeySchema());
                // target_key = decoded_key.get();
                req.SetDecodedStartKey(TxKey(std::move(decoded_key)));
            }
            if (req.EndKeyStr()->empty())
            {
                req.SetEndKey(KeyT::PositiveInfinity());
            }
            else
            {
                const std::string *key_str = req.EndKeyStr();
                assert(key_str != nullptr);
                std::unique_ptr<KeyT> decoded_key = std::make_unique<KeyT>();
                size_t offset = 0;
                decoded_key->Deserialize(key_str->data(), offset, KeySchema());
                // target_key = decoded_key.get();
                req.SetDecodedEndKey(TxKey(std::move(decoded_key)));
            }

            // Update unfinished core cnt
            req.SetUnfinishedCoreCnt(shard_->core_cnt_);

            // dispatch req to all cores after setting keys
            for (uint16_t i = 1; i < shard_->core_cnt_; i++)
            {
                shard_->local_shards_.EnqueueToCcShard(i, &req);
            }
        }
        // Iterate the cc map using the original page list.
        const KeyT *start_key = static_cast<const KeyT *>(req.StartKey());
        if (!start_key)
        {
            start_key = KeyT::NegativeInfinity();
        }
        const KeyT *end_key = static_cast<const KeyT *>(req.EndKey());
        if (!end_key)
        {
            end_key = KeyT::PositiveInfinity();
        }
        LruPage *lru_page;
        uint16_t pause_idx = shard_->core_id_;
        if (req.GetCleanType() == CleanType::CleanBucketData)
        {
            // For clean bucket data, cc req is only sent to 1 core.
            pause_idx = 0;
        }
        if (req.ResumeKey(pause_idx)->KeyPtr() != nullptr)
        {
            // resume key is the first key we need to start with, find
            // the floor key of it in case resume key has already been
            // kicked out.
            const KeyT *resume_key = req.ResumeKey(pause_idx)->GetKey<KeyT>();
            Iterator it = Floor(*resume_key);
            lru_page = it.GetPage();
        }
        else
        {
            Iterator it = Floor(*start_key);
            if (it->first == KeyT::NegativeInfinity())
            {
                lru_page = neg_inf_page_.next_page_;
            }
            else
            {
                lru_page = it.GetPage();
            }
        }

        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
            static_cast<
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                lru_page);

        auto l_start = std::chrono::high_resolution_clock::now();

        // To avoid occupy the TxProcessor thread for a long time, only
        // process KickoutPageBatchSize number of pages in each round.
        size_t scan_page_cnt = 0;
        size_t clean_page_cnt = 0;
        bool is_success = true;
        while (scan_page_cnt < KickoutCcEntryCc::KickoutPageBatchSize &&
               ccp->FirstKey() < *end_key && ccp != &pos_inf_page_)
        {
            if (clean_page_cnt > 0 && clean_page_cnt % 2 == 0)
            {
                auto l_now = std::chrono::high_resolution_clock::now();
                if (std::chrono::duration_cast<std::chrono::microseconds>(
                        l_now - l_start)
                        .count() >= std::chrono::microseconds(50).count())
                {
                    break;
                }
            }

            ++scan_page_cnt;
            if (req.GetCleanType() == CleanType::CleanDeletedData &&
                ccp->smallest_ttl_ > shard_->NowInMilliseconds())
            {
                // Skip pages that does not have expired keys
                ccp = ccp->next_page_;
                continue;
            }
            auto [freed_cnt, next_page] =
                CleanPageAndReBalance(ccp, &req, &is_success);
            // Move to next page
            ccp = static_cast<
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                next_page);
            if (!is_success &&
                req.GetCleanType() != CleanType::CleanDeletedData)
            {
                // Clean failed, retry in the next round.
                LOG(ERROR) << "Failed to clean all target ccentries on core: "
                           << shard_->core_id_;
                break;
            }

            if (freed_cnt > 0)
            {
                clean_page_cnt++;
            }
        }

        if (ccp == &pos_inf_page_ || !(ccp->FirstKey() < *end_key))
        {
            return req.SetFinish();
        }
        else
        {
            // Set the resume key for next round
            std::unique_ptr<KeyT> clone_key =
                std::make_unique<KeyT>(ccp->FirstKey());
            req.SetResumeKey(TxKey(std::move(clone_key)), pause_idx);
            shard_->Enqueue(&req);
            return false;
        }
    }

    bool Execute(UpdateKeyCacheCc &req) override
    {
        const KeyT *const req_end_key = req.end_key_ != nullptr
                                            ? req.end_key_->GetKey<KeyT>()
                                            : KeyT::PositiveInfinity();

        const KeyT *start_key =
            req.paused_pos_[shard_->core_id_].KeyPtr() != nullptr
                ? req.paused_pos_[shard_->core_id_].GetKey<KeyT>()
                : (req.end_key_ != nullptr ? req.start_key_->GetKey<KeyT>()
                                           : KeyT::NegativeInfinity());

        auto deduce_iterator = [this](const KeyT &search_key) -> Iterator
        {
            Iterator res_it;
            std::pair<Iterator, ScanType> search_pair =
                ForwardScanStart(search_key, true);
            res_it = search_pair.first;
            if (search_pair.second == ScanType::ScanGap)
            {
                ++res_it;
            }
            return res_it;
        };

        // The end iterator of this request.
        Iterator req_end_it = deduce_iterator(*req_end_key);

        TemplateStoreRange<KeyT> *range =
            static_cast<TemplateStoreRange<KeyT> *>(req.store_range_);
        assert(range != nullptr);

        TemplateStoreSlice<KeyT> *curr_slice = nullptr;
        auto find_valid_slice =
            [this, &req_end_it, range, &curr_slice, &deduce_iterator](
                const KeyT &search_key) -> std::pair<Iterator, Iterator>
        {
            Iterator it;
            Iterator end_it;
            const KeyT *key = &search_key;
            while (!curr_slice)
            {
                curr_slice = range->FindSlice(*key);
                it = deduce_iterator(*key);
                end_it = deduce_iterator(*(curr_slice->EndKey()));
                if ((!curr_slice->IsValidInKeyCache(shard_->core_id_) ||
                     it == end_it) &&
                    end_it != req_end_it)
                {
                    // The slice is empty or the slice is invalid in key cache,
                    // and the slice is not the last one of this request.
                    // Forward to the next slice.
                    key = curr_slice->EndKey();
                    curr_slice = nullptr;
                }
                else if (!curr_slice->IsValidInKeyCache(shard_->core_id_) &&
                         end_it == req_end_it)
                {
                    // Reach to the last slice, and the slice is invalid in key
                    // cache. Forward the key iteraror to the slice end
                    // iterator and stop loop.
                    it = end_it;
                }
            }

            return {it, end_it};
        };

        Iterator key_it;
        Iterator slice_end_it;
        std::tie(key_it, slice_end_it) = find_valid_slice(*start_key);

        for (size_t scan_cnt = 0;
             scan_cnt < UpdateKeyCacheCc::BatchSize && key_it != slice_end_it;
             ++scan_cnt)
        {
            const KeyT *cce_key = key_it->first;
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce =
                key_it->second;
            if (cce->PayloadStatus() != RecordStatus::Unknown)
            {
                assert(cce->PayloadStatus() == RecordStatus::Normal ||
                       cce->PayloadStatus() == RecordStatus::Deleted);
                range->DeleteKey(*cce_key, shard_->core_id_);
            }

            // Forward the iterator.
            ++key_it;

            if (key_it == slice_end_it && slice_end_it != req_end_it)
            {
                // Forward to the next slice.
                start_key = curr_slice->EndKey();
                std::tie(key_it, slice_end_it) = find_valid_slice(*start_key);
            }
        }

        if (key_it == slice_end_it)
        {
            req.paused_pos_[shard_->core_id_] = TxKey();
            return req.SetFinish();
        }
        else
        {
            req.paused_pos_[shard_->core_id_] = key_it->first->CloneTxKey();
            shard_->Enqueue(&req);
            return false;
        }
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

        auto entry_vec = req.EntryVector();
        auto entry_tuples = req.EntryTuple();
        size_t batch_size = req.BatchSize();
        size_t start_key_index = req.StartKeyIndex();

        const TxRecord *req_rec = nullptr;

        const KeyT *key = nullptr;
        KeyT decoded_key;
        const ValueT *commit_val = nullptr;
        ValueT decoded_rec;
        uint64_t commit_ts = 0;
        RecordStatus rec_status = RecordStatus::Normal;

        auto &resume_pos = req.GetPausedPosition(shard_->core_id_);
        size_t key_pos = std::get<0>(resume_pos);
        size_t key_offset = std::get<1>(resume_pos);
        size_t rec_offset = std::get<2>(resume_pos);
        size_t ts_offset = std::get<3>(resume_pos);
        size_t status_offset = std::get<4>(resume_pos);
        size_t hash = 0;

        Iterator it;
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce;
        const KeyT *write_key = nullptr;
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *cc_page =
            nullptr;
        size_t key_idx = 0;
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
            if (entry_vec != nullptr)
            {
                key_idx = start_key_index + key_pos;
                // get key
                key = entry_vec->at(key_idx)->key_.GetKey<KeyT>();
                // get record
                req_rec = entry_vec->at(key_idx)->rec_.get();
                if (req_rec)
                {
                    rec_status = RecordStatus::Normal;
                    commit_val = static_cast<const ValueT *>(req_rec);
                }
                else
                {
                    rec_status = RecordStatus::Deleted;
                    commit_val = nullptr;
                }
                // get commit ts
                commit_ts = entry_vec->at(key_idx)->commit_ts_;
            }
            else
            {
                auto [key_str, rec_str, ts_str, status_str] = *entry_tuples;
                // deserialize key
                decoded_key.Deserialize(
                    key_str.data(), next_key_offset, KeySchema());
                key = &decoded_key;
                // deserialize record status
                rec_status = *(
                    (RecordStatus *) (status_str.data() + next_status_offset));
                next_status_offset += sizeof(RecordStatus);
                if (rec_status == RecordStatus::Normal)
                {
                    // deserialize rec
                    decoded_rec.Deserialize(rec_str.data(), next_rec_offset);
                    commit_val = &decoded_rec;
                }
                else
                {
                    commit_val = nullptr;
                }

                // deserialize commit ts
                commit_ts = *((uint64_t *) (ts_str.data() + next_ts_offset));
                next_ts_offset += sizeof(uint64_t);
            }

            hash = key->Hash();
            size_t core_idx = (hash & 0x3FF) % shard_->core_cnt_;
            if (!(core_idx == shard_->core_id_) || commit_ts <= 1)
            {
                // Skip the key that does not belong to this core or
                // commit ts does not greater than 1. Move to next key.
                key_offset = next_key_offset;
                rec_offset = next_rec_offset;
                ts_offset = next_ts_offset;
                status_offset = next_status_offset;
                continue;
            }

            it = FindEmplace(*key);
            cce = it->second;
            cc_page = it.GetPage();
            if (cce == nullptr)
            {
                // This cc shard has reached max memory limit. We didn't write
                // data log for this upload batch req, but we have acquired
                // range read lock for this key. If we do not return error and
                // release the range read lock, it might block range split from
                // finishing. We should return error here so that coordinator
                // can release range read lock and retry later.
                // OOM will occurred when shard clean failed.
                shard_->EnqueueWaitListIfMemoryFull(&req);
                return false;
            }
            write_key = it->first;

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

            // Now, all versions of non-unique SecondaryIndex key shared
            // the unpack info in current version's payload, though the
            // unpack info will not be used for deleted key, we must not
            // change the payload of secondary key ccentry if it is not
            // null.
            if (Type() != TableType::Secondary ||
                cce->payload_.cur_payload_ == nullptr)
            {
                if (rec_status == RecordStatus::Normal)
                {
                    cce->payload_.SetCurrentPayload(commit_val);
                }
                else
                {
                    cce->payload_.SetCurrentPayload(nullptr);
                }
            }

            cce->SetCommitTsPayloadStatus(commit_ts, rec_status);
            if (req.Kind() == UploadBatchType::DirtyBucketData)
            {
                if (RangePartitioned)
                {
                    uint32_t rec_store_size =
                        rec_status == RecordStatus::Deleted
                            ? 0
                            : write_key->Size() + commit_val->Size();
                    cce->entry_info_.SetDataStoreSize(rec_store_size);
                }
                cce->SetCkptTs(commit_ts);
            }
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

            if (shard_->realtime_sampling_ && sample_pool_)
            {
                assert(write_key != nullptr);
                sample_pool_->OnInsert(*shard_, *write_key, KeySchema());
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

    bool Execute(UploadBatchSlicesCc &req) override
    {
        if (!shard_->local_shards_.template AcceptsDirtyRangeData<KeyT>(
                table_name_, cc_ng_id_, req.RangeId(), req.DirtyVersion()))
        {
            LOG(INFO) << "Refuse to receive remote records for cache, range "
                      << req.RangeId();
            return req.SetError(CcErrorCode::UPLOAD_BATCH_REJECTED);
        }

        if (!req.Parsed())
        {
            auto entry_tuples = req.EntryTuple();
            KeyT decoded_key;
            ValueT decoded_rec;
            uint64_t commit_ts = 0;
            RecordStatus rec_status = RecordStatus::Normal;

            auto [key_offset, rec_offset, ts_offset, status_offset] =
                req.ParsePosition();

            auto [key_str, rec_str, ts_str, status_str] = *entry_tuples;

            for (uint32_t cnt = 0;
                 cnt < UploadBatchSlicesCc::MaxParseBatchSize &&
                 key_offset < key_str.size();
                 cnt++)
            {
                // deserialize key
                decoded_key.Deserialize(
                    key_str.data(), key_offset, KeySchema());

                // deserialize commit ts
                commit_ts = *((uint64_t *) (ts_str.data() + ts_offset));
                ts_offset += sizeof(uint64_t);

                // deserialize record status
                rec_status =
                    *((RecordStatus *) (status_str.data() + status_offset));
                status_offset += sizeof(RecordStatus);

                if (rec_status == RecordStatus::Normal)
                {
                    // deserialize rec
                    decoded_rec.Deserialize(rec_str.data(), rec_offset);
                    req.AddDataItem(
                        TxKey(std::make_unique<KeyT>(std::move(decoded_key))),
                        std::make_unique<ValueT>(std::move(decoded_rec)),
                        commit_ts,
                        false);
                }
                else
                {
                    req.AddDataItem(
                        TxKey(std::make_unique<KeyT>(std::move(decoded_key))),
                        nullptr,
                        commit_ts,
                        true);
                }
            }

            if (key_offset < key_str.size())
            {
                // Parsed one batch
                req.SetParseOffset(
                    key_offset, rec_offset, ts_offset, status_offset);

                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
            }
            else
            {
                // Parsed all records
                req.SetParsed();

                // Emplace key on all cores
                for (size_t core = 0; core < shard_->core_cnt_; ++core)
                {
                    if (core != shard_->core_id_)
                    {
                        shard_->Enqueue(shard_->core_id_, core, &req);
                    }
                }
            }

        }  // end-parsed

        std::deque<SliceDataItem> &slice_vec = req.SliceData(shard_->core_id_);

        size_t index = req.NextIndex(shard_->core_id_);
        size_t last_index = std::min(
            index + UploadBatchSlicesCc::MaxEmplaceBatchSize, slice_vec.size());

        bool success = BatchFillSlice(slice_vec, false, index, last_index);

        if (!success)
        {
            // The is used for send range cache to the future owner during range
            // split. If the memory is not enough, we should abort the request
            // to avoid blocking the range split.
            // This is a performance optimization, and it does't matter if the
            // request is aborted.
            shard_->EnqueueWaitListIfMemoryFull(&req);
            return false;
        }

        if (last_index == slice_vec.size())
        {
            auto [finish_res, slices_info] = req.SetFinish();
            if (finish_res)
            {
                assert(slices_info != nullptr);
                shard_->local_shards_.template UpdateDirtyRangeSlice<KeyT>(
                    table_name_,
                    cc_ng_id_,
                    slices_info->range_,
                    slices_info->new_range_,
                    slices_info->slice_idxs_,
                    slices_info->version_ts_,
                    SliceStatus::FullyCached);
            }
        }
        else
        {
            index = last_index;
            req.SetNextIndex(shard_->core_id_, index);
            shard_->Enqueue(shard_->LocalCoreId(), &req);
        }
        return false;
    }

    bool Execute(EscalateStandbyCcmCc &req) override
    {
        auto it = Begin();
        auto end_it = End();
        uint64_t ckpt_ts = req.PrimaryCkptTs();
        uint64_t now_ts = shard_->Now();
        while (it != end_it)
        {
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce =
                it->second;
            if (txservice_skip_wal)
            {
                // If wal log is disabled, we need to flush all in memory cache
                // to overwrite potential newer version in kv.
                RecordStatus status = cce->PayloadStatus();
                cce->SetCommitTsPayloadStatus(now_ts, status);
                if (!VersionedRecord && cce->HasBufferedCommandList())
                {
                    BufferedTxnCmdList &buffered_cmds =
                        cce->BufferedCommandList();
                    int64_t buffered_cmd_cnt_old = buffered_cmds.Size();
                    buffered_cmds.Clear();
                    shard_->UpdateBufferedCommandCnt(0 - buffered_cmd_cnt_old);
                }
            }
            else
            {
                // if log is enabled, we only need to flush data after primary
                // last ckpt ts.
                if (cce->CommitTs() <= ckpt_ts)
                {
                    cce->SetCkptTs(cce->CommitTs());
                }
            }
            it++;
        }

        return true;
    }

    bool Execute(InvalidateTableCacheCc &req) override
    {
        return true;
    }

    bool Execute(ScanSliceDeltaSizeCcForRangePartition &req) override
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
        if (req.schema_version_ != 0 &&
            table_schema_->Version() != req.schema_version_)
        {
            assert(req.schema_version_ > table_schema_->Version());
            LOG(WARNING) << "Table schema version mismatched for data sync "
                            "scan, table name: "
                         << req.table_name_.String()
                         << " ,scan carried version: " << req.schema_version_
                         << " ,ccmap version: " << table_schema_->Version();
            req.SetError(CcErrorCode::REQUESTED_TABLE_SCHEMA_MISMATCH);
            return false;
        }

        const KeyT *const req_start_key = req.StartTxKey().GetKey<KeyT>();
        const KeyT *const req_end_key = req.EndTxKey().GetKey<KeyT>();

        auto &paused_position = req.PausedPos(shard_->core_id_);

        bool is_dirty = req.IsDirty();

        auto deduce_iterator = [this](const KeyT &search_key) -> Iterator
        {
            Iterator it;
            std::pair<Iterator, ScanType> search_pair =
                ForwardScanStart(search_key, true);
            it = search_pair.first;
            if (search_pair.second == ScanType::ScanGap)
            {
                ++it;
            }
            return it;
        };

        auto next_page_it = [this](Iterator &end_it) -> Iterator
        {
            Iterator it = end_it;
            if (it != End())
            {
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                    it.GetPage();
                assert(ccp != nullptr);
                if (ccp->next_page_ == PagePosInf())
                {
                    it = End();
                }
                else
                {
                    it = Iterator(ccp->next_page_, 0, &neg_inf_);
                }
            }
            return it;
        };

        Iterator key_it;
        Iterator req_end_it;

        // The key iterator.
        const KeyT *search_start_key =
            paused_position.first.KeyPtr() == nullptr
                ? req_start_key
                : paused_position.first.GetKey<KeyT>();
        key_it = deduce_iterator(*search_start_key);

        // The request end iterator
        req_end_it = deduce_iterator(*req_end_key);

        // Since we might skip the page that end_it is on if it's not updated
        // since last ckpt, it might skip end_it. If the last page is skipped it
        // will be set as the first entry on the next page. Also check if
        // (key_it == end_next_page_it).
        Iterator req_end_next_page_it = next_page_it(req_end_it);

        // The current slice end iterator
        Iterator slice_end_it = req_end_it;
        Iterator slice_end_next_page_it = req_end_next_page_it;

        int64_t *curr_slice_delta_size = nullptr;
        if (paused_position.second)
        {
            // The current slice has not completed in the last round.
            // Fix the slice end iterator.
            const KeyT *slice_end_key =
                paused_position.second->EndTxKey().GetKey<KeyT>();
            slice_end_it = deduce_iterator(*slice_end_key);

            slice_end_next_page_it = next_page_it(slice_end_it);

            curr_slice_delta_size =
                &(req.SliceDeltaSize(shard_->core_id_).back().second);
        }

        bool has_dml_since_ddl = false;
        bool check_key_version = (is_dirty && req.StoreRangePtr())
                                     ? req.StoreRangePtr()->HasDmlSinceDdl()
                                     : true;

        // ScanSliceDeltaSizeCcForRangePartition is running on TxProcessor
        // thread. To avoid blocking other transaction for a long time, we only
        // process ScanBatchSize number of keys in each round.
        for (size_t scan_cnt = 0;
             scan_cnt < ScanSliceDeltaSizeCcForRangePartition::ScanBatchSize &&
             key_it != req_end_it && key_it != req_end_next_page_it;
             ++scan_cnt)
        {
            const KeyT *key = key_it->first;
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce =
                key_it->second;
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                key_it.GetPage();
            assert(ccp);

            if (ccp->last_dirty_commit_ts_ <= req.LastDataSyncTs())
            {
                assert(!cce->NeedCkpt());
                // Skip the pages that have no updates since last data sync.
                if (ccp->next_page_ == PagePosInf())
                {
                    key_it = End();
                }
                else
                {
                    key_it = Iterator(ccp->next_page_, 0, &neg_inf_);
                }

                // Check the slice iterator.
                if (key_it == slice_end_it || key_it == slice_end_next_page_it)
                {
                    // Reach the end of current slice.
                    paused_position.second = nullptr;
                    curr_slice_delta_size = nullptr;

                    // Reset the slice end iterator
                    slice_end_it = req_end_it;
                    slice_end_next_page_it = next_page_it(slice_end_it);
                }
                continue;
            }

            const uint64_t commit_ts = cce->CommitTs();

            if (is_dirty && commit_ts > this->schema_ts_)
            {
                // The key is generated by a write transaction during index
                // creation.
                // The value of chec_key_version is false means that this table
                // must be in dirty status, such during add index.
                has_dml_since_ddl = true;
            }

            // The commit_ts <= 1 means the key is non-existed or a new inserted
            // key that the tx has not finished post-processing.
            if (cce->NeedCkpt() && commit_ts > 1 && commit_ts <= req.ScanTs())
            {
                if (req.StoreRangePtr() == nullptr)
                {
                    // Pin the range so that it can't be kicked out during data
                    // sync.
                    auto [range_entry, store_range] =
                        shard_->local_shards_.PinStoreRange(table_name_,
                                                            req.NodeGroupId(),
                                                            req.NodeGroupTerm(),
                                                            req.StartTxKey(),
                                                            &req,
                                                            shard_);
                    if (store_range &&
                        !req.SetStoreRange(store_range, shard_->core_id_))
                    {
                        // StoreRange already been pinned by other core, unpin
                        // on the current core.
                        range_entry->UnPinStoreRange();
                    }
                    else if (!store_range)
                    {
                        // Blocked by loading RangeSlices.
                        paused_position.first = key->CloneTxKey();
                        return false;
                    }

                    check_key_version =
                        is_dirty ? store_range->HasDmlSinceDdl() : true;
                }

                bool need_export = true;
                if (cce->entry_info_.DataStoreSize() == INT32_MAX &&
                    !check_key_version)
                {
                    assert(cce->CkptTs() == 0);
                    cce->entry_info_.SetDataStoreSize(0);
                    cce->SetCkptTs(1U);
                }
                else if (cce->entry_info_.DataStoreSize() == INT32_MAX)
                {
                    // Load data store size by pinning the slice. Data store
                    // size is required to decide slice & range update plan.
                    RangeSliceOpStatus pin_status;
                    RangeSliceId slice_id = shard_->local_shards_.PinRangeSlice(
                        table_name_,
                        req.NodeGroupId(),
                        req.NodeGroupTerm(),
                        KeySchema(),
                        RecordSchema(),
                        schema_ts_,
                        table_schema_->GetKVCatalogInfo(),
                        -1,
                        *key,
                        true,
                        &req,
                        shard_,
                        pin_status,
                        true,
                        UINT8_MAX);
                    switch (pin_status)
                    {
                    case RangeSliceOpStatus::Successful:
                    {
                        if (cce->entry_info_.DataStoreSize() == INT32_MAX)
                        {
                            // If data store size is still unavailable after the
                            // slice is loaded from data store, that means this
                            // entry does not exist in data store.
                            cce->entry_info_.SetDataStoreSize(0);
                        }
                        if (cce->CkptTs() == 0)
                        {
                            // Set the ckpt ts as 1 to indicate this is the
                            // first version of the key.
                            cce->SetCkptTs(1U);
                        }
                        slice_id.Unpin();
                        break;
                    }
                    case RangeSliceOpStatus::Retry:
                    {
                        paused_position.first = key->CloneTxKey();
                        shard_->Enqueue(shard_->LocalCoreId(), &req);
                        return false;
                    }
                    case RangeSliceOpStatus::BlockedOnLoad:
                    {
                        paused_position.first = key->CloneTxKey();
                        return false;
                    }
                    case RangeSliceOpStatus::NotOwner:
                    {
                        assert(false && "Dead branch");
                        // The recovered cc entry does not belong to this ng
                        // anymore. This will happen if ng failover after a
                        // range split just finished but before checkpointer
                        // is able to truncate the log. In this case the log
                        // records of the data that now falls on another ng
                        // will still be replayed on the old ng on recover.
                        // Skip the cc entry and remove it at the end.
                        need_export = false;
                        break;
                    }
                    default:
                    {
                        // Checkpointing needs to load a slice only if one or
                        // more changed records are to be flushed. Range catalog
                        // must have been loaded when initial changes were made.
                        // So, pinning slice in checkpointing never returns
                        // BlockedOnCatalog. Moreover, since the force_load flag
                        // is set, pinning slice in checkpointing never returns
                        // Delay.
                        req.SetError(CcErrorCode::PIN_RANGE_SLICE_FAILED);
                        return false;
                    }
                    }
                }

                if (need_export)
                {
                    assert(commit_ts > cce->CkptTs());
                    if (paused_position.second == nullptr)
                    {
                        // Step into a new slice.
                        TemplateStoreRange<KeyT> *store_range =
                            static_cast<TemplateStoreRange<KeyT> *>(
                                req.StoreRangePtr());
                        TemplateStoreSlice<KeyT> *slice =
                            store_range->FindSlice(*key);

                        paused_position.second = slice;

                        const KeyT *slice_end_key = slice->EndKey();
                        slice_end_it = deduce_iterator(*slice_end_key);

                        slice_end_next_page_it = next_page_it(slice_end_it);

                        auto &slice_delta_size =
                            req.SliceDeltaSize(shard_->core_id_);
                        slice_delta_size.emplace_back(slice->StartTxKey(), 0);
                        curr_slice_delta_size = &slice_delta_size.back().second;
                    }

                    // Export the delta size of this cce.
                    assert(cce->entry_info_.DataStoreSize() != INT32_MAX &&
                           curr_slice_delta_size);
                    *curr_slice_delta_size +=
                        (cce->PayloadStatus() != RecordStatus::Deleted
                             ? (key->Size() + cce->PayloadSize() -
                                cce->entry_info_.DataStoreSize())
                             : (-cce->entry_info_.DataStoreSize()));
                }
            }

            // Forward key iterator
            ++key_it;

            if (key_it == slice_end_it)
            {
                // Reach the current slice end.
                paused_position.second = nullptr;
                curr_slice_delta_size = nullptr;

                // Update the end it.
                slice_end_it = req_end_it;
                slice_end_next_page_it = next_page_it(slice_end_it);
            }
        }

        if (has_dml_since_ddl)
        {
            req.SetHasDmlSinceDdl();
        }

        if (key_it == req_end_it || key_it == req_end_next_page_it)
        {
            // Reach the end of this request.
            paused_position = {TxKey(), nullptr};
            req.SetFinish();
        }
        else
        {
            paused_position.first = key_it->first->CloneTxKey();
            shard_->Enqueue(&req);
        }

        // Access ScanSliceDeltaSizeCcForRangePartition member variable is
        // unsafe after SetFinished().
        return false;
    }

    bool Execute(ScanDeltaSizeCcForHashPartition &req) override
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
        if (req.schema_version_ != 0 &&
            table_schema_->Version() != req.schema_version_)
        {
            assert(req.schema_version_ > table_schema_->Version());
            LOG(WARNING) << "Table schema version mismatched for data sync "
                            "scan, table name: "
                         << req.table_name_.String()
                         << " ,scan carried version: " << req.schema_version_
                         << " ,ccmap version: " << table_schema_->Version();
            req.SetError(CcErrorCode::REQUESTED_TABLE_SCHEMA_MISMATCH);
            return false;
        }

        auto &paused_key = req.PausedKey();

        auto deduce_iterator = [this](const KeyT &search_key) -> Iterator
        {
            Iterator it;
            std::pair<Iterator, ScanType> search_pair =
                ForwardScanStart(search_key, true);
            it = search_pair.first;
            if (search_pair.second == ScanType::ScanGap)
            {
                ++it;
            }
            return it;
        };

        auto next_page_it = [this](Iterator &end_it) -> Iterator
        {
            Iterator it = end_it;
            if (it != End())
            {
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                    it.GetPage();
                assert(ccp != nullptr);
                if (ccp->next_page_ == PagePosInf())
                {
                    it = End();
                }
                else
                {
                    it = Iterator(ccp->next_page_, 0, &neg_inf_);
                }
            }
            return it;
        };

        Iterator key_it;
        Iterator req_end_it;

        // The key iterator.
        const KeyT *const search_start_key =
            paused_key.GetKey<KeyT>() == nullptr ? KeyT::NegativeInfinity()
                                                 : paused_key.GetKey<KeyT>();
        key_it = deduce_iterator(*search_start_key);

        // The request end iterator
        req_end_it = deduce_iterator(*KeyT::PositiveInfinity());

        // Since we might skip the page that end_it is on if it's not updated
        // since last ckpt, it might skip end_it. If the last page is skipped it
        // will be set as the first entry on the next page. Also check if
        // (key_it == end_next_page_it).
        Iterator req_end_next_page_it = next_page_it(req_end_it);

        // The current slice end iterator
        Iterator slice_end_it = req_end_it;
        Iterator slice_end_next_page_it = req_end_next_page_it;

        // ScanSliceDeltaSizeCcForRangePartition is running on TxProcessor
        // thread. To avoid blocking other transaction for a long time, we only
        // process ScanBatchSize number of keys in each round.
        for (size_t scan_cnt = 0;
             scan_cnt < ScanDeltaSizeCcForHashPartition::ScanBatchSize &&
             key_it != req_end_it && key_it != req_end_next_page_it;
             ++scan_cnt)
        {
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce =
                key_it->second;
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                key_it.GetPage();
            assert(ccp);

            if (ccp->last_dirty_commit_ts_ <= req.LastDataSyncTs())
            {
                // Skip the pages that have no updates since last data sync.
                if (ccp->next_page_ == PagePosInf())
                {
                    key_it = End();
                }
                else
                {
                    key_it = Iterator(ccp->next_page_, 0, &neg_inf_);
                }

                // Check the slice iterator.
                if (key_it == slice_end_it || key_it == slice_end_next_page_it)
                {
                    // Reset the slice end iterator
                    slice_end_it = req_end_it;
                    slice_end_next_page_it = next_page_it(slice_end_it);
                }
                continue;
            }

            const uint64_t commit_ts = cce->CommitTs();

            // The commit_ts <= 1 means the key is non-existed or a new inserted
            // key that the tx has not finished post-processing.
            req.UpdateKeyCount(cce->NeedCkpt() && commit_ts > 1 &&
                               commit_ts <= req.ScanTs());

            // Forward key iterator
            ++key_it;

            if (key_it == slice_end_it)
            {
                // Update the end it.
                slice_end_it = req_end_it;
                slice_end_next_page_it = next_page_it(slice_end_it);
            }
        }

        if (key_it == req_end_it || key_it == req_end_next_page_it)
        {
            int64_t allocated, committed;
            mi_thread_stats(&allocated, &committed);
            req.SetMemoryUsage(allocated);
            req.SetFinish();
        }
        else
        {
            paused_key = key_it->first->CloneTxKey();
            shard_->Enqueue(&req);
        }

        // Access ScanSliceDeltaSizeCcForRangePartition member variable is
        // unsafe after SetFinished().
        return false;
    }

    bool Execute(SampleSubRangeKeysCc &req) override
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

        const KeyT *const req_start_key = req.StartTxKey()->GetKey<KeyT>();
        const KeyT *const req_end_key = req.EndTxKey()->GetKey<KeyT>();

        TxKey &pause_pos = req.PausePos();
        const KeyT *slice_start_key = pause_pos.KeyPtr() != nullptr
                                          ? pause_pos.GetKey<KeyT>()
                                          : req_start_key;
        const KeyT *slice_end_key = req_end_key;

        auto deduce_iterator = [this](const KeyT &search_key) -> Iterator
        {
            Iterator it;
            std::pair<Iterator, ScanType> search_pair =
                ForwardScanStart(search_key, true);
            it = search_pair.first;
            if (search_pair.second == ScanType::ScanGap)
            {
                ++it;
            }
            return it;
        };

        Iterator it = deduce_iterator(*slice_start_key);
        Iterator end_it = deduce_iterator(*slice_end_key);

        using KeySamplePool = SampleSubRangeKeysCc::
            SamplePool<KeyT, typename TemplateCcMapSamplePool<KeyT>::CopyKey>;

        KeySamplePool *const key_sample_pool = [&req]
        {
            if (req.SamplePoolPtr() == nullptr)
            {
                req.SetSamplePool(std::move(
                    std::make_unique<KeySamplePool>(req.SamplePoolCapacity())));
            }
            return static_cast<KeySamplePool *>(req.SamplePoolPtr());
        }();

        for (size_t scan_cnt = 0;
             scan_cnt < SampleSubRangeKeysCc::ScanBatchSize && it != end_it;
             ++scan_cnt)
        {
            const KeyT *key = it->first;
            const CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>
                *cce = it->second;
            assert(key);

            // sample keys
            if (cce->CommitTs() <= req.DataSyncTs() &&
                cce->PayloadStatus() == RecordStatus::Normal)
            {
                key_sample_pool->Insert(*key);
            }

            ++it;
        }

        if (it == end_it)
        {
            // Get the target keys
            const std::vector<KeyT> &sample_keys =
                key_sample_pool->SampleKeys();
            assert(sample_keys.size() <= req.SamplePoolCapacity());

            const size_t target_cnt = req.TargetKeyCount();
            std::vector<TxKey> &target_txkeys = req.TargetTxKeys();
            target_txkeys.reserve(target_cnt);
            size_t step_size = sample_keys.size() / (target_cnt + 1);
            step_size = step_size > 0 ? step_size : 1;

            for (size_t tk_idx = 0, sk_idx = step_size;
                 tk_idx < target_cnt && sk_idx < sample_keys.size();
                 ++tk_idx, sk_idx = (step_size * (tk_idx + 1)))
            {
                target_txkeys.emplace_back(&sample_keys[sk_idx]);
            }

            // Finish
            req.SetFinish();
        }
        else
        {
            // Set the paused position and put the request into ccqueue again.
            pause_pos = it->first->CloneTxKey();
            shard_->Enqueue(&req);
        }

        return false;
    }

    bool Execute(ApplyCc &req) override
    {
        return true;
    }

    bool Execute(UploadTxCommandsCc &req) override
    {
        return true;
    }

    bool Execute(UploadRangeSlicesCc &req) override
    {
        assert(false);
        return true;
    }

    bool Execute(KeyObjectStandbyForwardCc &req) override
    {
        assert(false);
        return true;
    }

    bool Execute(RestoreCcMapCc &req) override
    {
        assert(false);
        return true;
    }

    size_t size() const override
    {
        return size_;
    }

    void CleanEntry(LruEntry *entry, LruPage *page) override
    {
        auto cce = static_cast<
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(entry);
        assert(cce->IsFree());
        auto ccpage = static_cast<
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(page);
        auto page_it = ccmp_.end();
        if (ccpage->Entry(0) == cce)
        {
            // The page's first key is cleaned, needs to locate the page
            // position to update the map.
            page_it = ccmp_.find(ccpage->FirstKey());
        }
        ccpage->Remove(cce);
        size_--;

        RebalancePage(ccpage, page_it, true);
    }

    /**
     * Clean erasable entries in lru_page, re-balance pages after clean.
     *
     * @param lru_page
     * @param clean_type
     * @param kickout_cc [optional]
     * @param is_success [optional]
     * @return result pair of which the first is free count and the
     * scond is the next page which will be cleaned. If clean type is
     * CleanForFree, the next page is the lru_next_ of the page.
     * Otherwise, the value of the next page is setted depending on
     * clean status: When clean successfully, return the current page's
     * next page in the below cases: page is empty; no borrow or merge;
     * borrow from previous; merge with previous. Return the current
     * page in the below cases: borrow from next; merge with next. When
     * clean failed, return the current page always.
     */
    std::pair<size_t, LruPage *> CleanPageAndReBalance(
        LruPage *lru_page,
        KickoutCcEntryCc *kickout_cc = nullptr,
        bool *is_success = nullptr) override
    {
        size_t free_cnt = 0;

        // clean page
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page =
            static_cast<
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                lru_page);
        auto page_it = ccmp_.end();
        bool success = CleanPage(page, page_it, free_cnt, kickout_cc);

        // Output the operation result if the caller care it.
        if (is_success != nullptr)
        {
            *is_success = success;
        }

        LruPage *next_page =
            RebalancePage(page, page_it, success, kickout_cc == nullptr);

        return {free_cnt, next_page};
    }

    void Clean() override
    {
        for (auto it = ccmp_.begin(); it != ccmp_.end(); it++)
        {
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> &page =
                *it->second;
            if (page.lru_next_ != nullptr)
            {
                shard_->DetachLru(&page);
            }

            for (auto &cce : page.entries_)
            {
                cce->ClearLocks(*shard_, cc_ng_id_);
            }
        }

        normal_obj_sz_ = 0;
        size_ = 0;
        ccmp_.clear();
    }

    bool CleanBatchPages(size_t clean_page_cnt) override
    {
        size_t cnt = 0;
        auto it = ccmp_.begin();
        while (it != ccmp_.end() && cnt < clean_page_cnt)
        {
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page =
                it->second.get();
            if (page->lru_next_ != nullptr)
            {
                shard_->DetachLru(page);
            }

            for (auto &cce : page->entries_)
            {
                if (!VersionedRecord &&
                    cce->PayloadStatus() == RecordStatus::Normal)
                {
                    normal_obj_sz_--;
                }
                cce->ClearLocks(*shard_, cc_ng_id_);
            }

            size_ -= page->Size();
            cnt += 1;

            it = ccmp_.erase(it);
        }

        if (it != ccmp_.end())
        {
            // More data
            return false;
        }

        assert(size_ == 0);
        assert(VersionedRecord || normal_obj_sz_ == 0);
        return true;
    }

    LruPage *RebalancePage(
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page,
        BtreeMapIterator &page_it,
        bool success,
        bool use_lru_list = true)
    {
        LruPage *next_page = nullptr;
        if (!use_lru_list)
        {
            // For target ccmap, go along with CcPage::next_page_
            next_page = page->next_page_;
        }
        else
        {
            // go along with the lru list.
            next_page = page->lru_next_;
        }
        if (page->Empty())  // remove page if empty
        {
            if (page->lru_next_ != nullptr)
            {
                shard_->DetachLru(page);
            }
            assert(page_it != ccmp_.end());
            ccmp_.erase(page_it);
        }
        else
        {
            if (page_it != ccmp_.end())
            {
                // page_it is set, which means the page's first key has been
                // removed when cleaning the page
                TryUpdatePageKey(page_it);
            }

            if (page->Size() >=
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>::
                    merge_threshold_)
            {
                // page is still half full, no redistribution or merge
                // needed

                if (!use_lru_list && !success)
                {
                    // If the caller care the clean status, reset the value
                    // of
                    // @@next_page depending on the clean result:
                    // 1) When the current page has been cleaned
                    // successfully, there is no need to reset the value of
                    // @@next_page. 2) When this current page has not been
                    // cleaned successfully, should set the current page as
                    // the next_page.
                    next_page = page;
                }
            }
            else
            {
                // redistribute or merge page with its siblings
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *prev =
                    page->prev_page_;
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *next =
                    page->next_page_;

                std::optional<bool> borrow_from_prev;
                std::optional<bool> borrow_from_next;
                std::optional<bool> merge_with_prev;
                std::optional<bool> merge_with_next;

                auto can_borrow_from_prev = [&]
                {
                    if (!borrow_from_prev.has_value())
                    {
                        borrow_from_prev =
                            prev != &neg_inf_page_ &&
                            page->Size() + prev->Size() >
                                CcPage<KeyT,
                                       ValueT,
                                       VersionedRecord,
                                       RangePartitioned>::split_threshold_;
                    }
                    return *borrow_from_prev;
                };

                auto can_borrow_from_next = [&]
                {
                    if (!borrow_from_next.has_value())
                    {
                        borrow_from_next =
                            next != &pos_inf_page_ &&
                            page->Size() + next->Size() >
                                CcPage<KeyT,
                                       ValueT,
                                       VersionedRecord,
                                       RangePartitioned>::split_threshold_;
                    }
                    return *borrow_from_next;
                };

                auto can_merge_with_prev = [&]
                {
                    if (!merge_with_prev.has_value())
                    {
                        merge_with_prev =
                            prev != &neg_inf_page_ &&
                            page->Size() + prev->Size() <=
                                CcPage<KeyT,
                                       ValueT,
                                       VersionedRecord,
                                       RangePartitioned>::split_threshold_;
                    }
                    return *merge_with_prev;
                };

                auto can_merge_with_next = [&]
                {
                    if (!merge_with_next.has_value())
                    {
                        merge_with_next =
                            next != &pos_inf_page_ &&
                            page->Size() + next->Size() <=
                                CcPage<KeyT,
                                       ValueT,
                                       VersionedRecord,
                                       RangePartitioned>::split_threshold_;
                    }
                    return *merge_with_next;
                };

                if (can_borrow_from_prev() || can_borrow_from_next())
                {
                    // map needs to be updated through iterator after
                    // redistribution
                    if (page_it == ccmp_.end())
                    {
                        page_it = ccmp_.find(page->FirstKey());
                    }
                    // the two pages whose entries need to be redistributed
                    // are identified by page1 and page2, page1 is the page
                    // with smaller key
                    auto page1_it = page_it;
                    auto page2_it = page_it;
                    if (can_borrow_from_prev())
                    {
                        // borrow entries from previous page
                        --page1_it;
                    }
                    else if (can_borrow_from_next())
                    {
                        // borrow entries from next page
                        ++page2_it;
                        assert(page2_it != ccmp_.end());
                    }

                    RedistributeBetweenPages(page1_it, page2_it);

                    bool page_needs_reclean =
                        !can_borrow_from_prev() && can_borrow_from_next();
                    if (!use_lru_list &&
                        ((success && page_needs_reclean) || !success))
                    {
                        // If the caller care the clean status, reset the
                        // value of
                        // @@next_page depending on the clean result:
                        // 1) When the current page has been cleaned
                        // successfully, if borrow from the next(that's mean
                        // page == page1), should set the current page as
                        // the @@next_page. 2) When this current page has
                        // not been cleaned successfully, should return the
                        // current page as the
                        // @@next_page.
                        next_page = page;
                    }
                }
                else if (can_merge_with_prev() || can_merge_with_next())
                {
                    // map needs to be updated through iterator after
                    // redistribution
                    if (page_it == ccmp_.end())
                    {
                        page_it = ccmp_.find(page->FirstKey());
                    }
                    // the two pages to be merged are identified by page1
                    // and page2, page1 is the page with smaller key
                    auto page1_it = page_it;
                    auto page2_it = page_it;

                    bool real_merge_with_prev = false;
                    if (can_merge_with_prev())
                    {
                        real_merge_with_prev = true;
                        // merge `page` with its previous page
                        --page1_it;
                    }
                    else if (can_merge_with_next())
                    {
                        // merge `page` with its next page
                        ++page2_it;
                        assert(page2_it != ccmp_.end());
                    }

                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>
                        *merged_page = page1_it->second.get();
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>
                        *discarded_page = page2_it->second.get();

                    if (use_lru_list && next_page == discarded_page)
                    {
                        // For this case, the next page to be cleaned comes
                        // from the lru list. The next_page is current
                        // page's lru_next_, and if the next_page ==
                        // discarded_page, the discarded_page must be the
                        // next page of the current page, that is to say,
                        // the current page will merge with the next. So the
                        // current page must equal to the merged page, and
                        // should set the
                        // @@next_page is merged page.
                        assert(page == merged_page);
                        next_page = merged_page;
                    }

                    // merge page1 and page2
                    MergePages(page1_it, page2_it);

                    if (!use_lru_list)
                    {
                        // For this case, should set the value of
                        // @@next_page depending on the clean result: 1)
                        // When the current page has been cleaned
                        // successfully, if merged with previous page, set
                        // the value is the merged_page's next_page_; if
                        // merged with next page, set the value is the
                        // merged_page itself. 2) When the current page has
                        // not been cleaned successfully. Should set the
                        // value is the merged_page itself no matter merged
                        // with previous page or merged with next page.
                        next_page = (real_merge_with_prev && success)
                                        ? merged_page->next_page_
                                        : merged_page;
                    }
                }
            }
        }

        return next_page;
    }

    TableType Type() const override
    {
        return table_name_.Type();
    }

    const txservice::KeySchema *KeySchema() const override
    {
        if (table_schema_ != nullptr)
        {
            if (table_name_.Type() == TableType::Secondary ||
                table_name_.Type() == TableType::UniqueSecondary)
            {
                return table_schema_->IndexKeySchema(table_name_);
            }
            else
            {
                return table_schema_->KeySchema();
            }
        }
        return nullptr;
    }

    const txservice::RecordSchema *RecordSchema() const override
    {
        if ((table_name_.Type() != TableType::Secondary) &&
            (table_schema_ != nullptr))
        {
            return table_schema_->RecordSchema();
        }
        return nullptr;
    }

    /**
     * Used for debug to verify the map_link is complete and keys are in
     * order.
     */
    size_t VerifyOrdering() override
    {
        // verify page order in map
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *prev_page =
            &neg_inf_page_;
        for (auto it = ccmp_.begin(); it != ccmp_.end(); it++)
        {
            const KeyT &page_key = it->first;
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page =
                it->second.get();
            if (page_key != page->FirstKey())
            {
                DLOG(FATAL)
                    << "error page: " << page
                    << " map key: " << page_key.ToString()
                    << ", page first key: " << page->FirstKey().ToString();
            }
            assert(page_key == page->FirstKey());
            // This silences the -Wunused-but-set-variable warning
            // without any runtime overhead.
            (void) page_key;
            assert(page->prev_page_ == prev_page &&
                   prev_page->next_page_ == page);
            prev_page = page;
        }
        assert(prev_page->next_page_ == &pos_inf_page_ &&
               pos_inf_page_.prev_page_ == prev_page);
        // This silences the -Wunused-but-set-variable warning without
        // any runtime overhead.
        (void) prev_page;
        // verify key order in all pages
        Iterator ccm_it = Begin();
        Iterator pos_inf_it = End();
        const KeyT *prev_key = ccm_it->first;
        ccm_it++;
        size_t cnt = 0;
        for (; ccm_it != pos_inf_it; ccm_it++)
        {
            const KeyT *key = ccm_it->first;
            assert(*prev_key < *key);
            // This silences the -Wunused-but-set-variable warning
            // without any runtime overhead.
            (void) prev_key;
            prev_key = key;
            ++cnt;
        }
        assert(cnt > 0);

        return cnt;
    }

    bool BulkEmplaceForTest(std::vector<KeyT *> &keys)
    {
        std::random_device rd;
        std::default_random_engine generator(rd());
        std::uniform_int_distribution<uint64_t> distribution(0, 0xFFFFFFFF);
        for (auto key : keys)
        {
            bool emplace = false;
            auto it = FindEmplace(*key, emplace, false, false);
            if (!emplace)
            {
                assert(false);
                return false;
            }
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce =
                it->second;
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                it.GetPage();
            // randomly set ckpt_ts and commit_ts
            cce->SetCommitTsPayloadStatus(distribution(generator),
                                          RecordStatus::Normal);
            cce->SetCkptTs(distribution(generator));
            ccp->last_dirty_commit_ts_ =
                std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
        }
        return true;
    }

protected:
    // The CcEntry defragment result
    enum DefragResult
    {
        FAILED_BY_LOCK = 0,
        FAILED_BY_CKPT,
        FAILED_BY_KV_LOADING,
        DEFRAGED,
        NOFRAGED
    };

    class Iterator
    {
        using iterator_category = std::bidirectional_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = std::pair<
            const KeyT *,
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>;
        using pointer = value_type *;    // or also value_type*
        using reference = value_type &;  // or also value_type&

    public:
        Iterator() = default;

        void DebugPrint()
        {
            bool is_neg_inf = (current_.first == KeyT::NegativeInfinity());
            bool is_pos_inf = (current_.first == KeyT::PositiveInfinity());
            LOG(INFO) << "key: " << current_.first
                      << ", cce: " << current_.second
                      << ", is neg inf: " << is_neg_inf
                      << ", is pos inf: " << is_pos_inf
                      << ", current_page_: " << current_page_
                      << ", idx_in_page_: " << idx_in_page_;
            LOG(INFO) << "neg_inf key: " << KeyT::NegativeInfinity()
                      << ", pos_inf key: " << KeyT::PositiveInfinity()
                      << ", neg_inf cce: " << neg_inf_cce_;
            if (!is_neg_inf && !is_pos_inf)
            {
                LOG(INFO) << ", cce parent page: " << current_page_;
                current_page_->DebugPrint();
            }
        }

        Iterator(CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page,
                 size_t idx,
                 CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>
                     *neg_inf_cce)
            : neg_inf_cce_(neg_inf_cce), current_page_(page), idx_in_page_(idx)
        {
            assert(current_page_ != nullptr);
            UpdateCurrent();
        }

        Iterator(
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *cc_page,
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>
                *neg_inf_cce)
            : neg_inf_cce_(neg_inf_cce)
        {
            if (cc_page->IsNegInf())
            {
                current_.first = KeyT::NegativeInfinity();
                current_.second = neg_inf_cce_;
                current_page_ = cc_page;
            }
            else if (cc_page->IsPosInf())
            {
                current_.first = KeyT::PositiveInfinity();
                current_.second = nullptr;
                current_page_ = cc_page;
            }
            else
            {
                current_page_ = cc_page;
                idx_in_page_ = current_page_->FindEntry(cce);
                UpdateCurrent();
            }
        }

        Iterator(Iterator &&rhs)
            : current_(rhs.current_),
              neg_inf_cce_(rhs.neg_inf_cce_),
              current_page_(rhs.current_page_),
              idx_in_page_(rhs.idx_in_page_)
        {
        }

        Iterator(const Iterator &rhs)
            : current_(rhs.current_),
              neg_inf_cce_(rhs.neg_inf_cce_),
              current_page_(rhs.current_page_),
              idx_in_page_(rhs.idx_in_page_)
        {
        }

        Iterator &operator=(const Iterator &rhs)
        {
            current_ = rhs.current_;
            neg_inf_cce_ = rhs.neg_inf_cce_;
            current_page_ = rhs.current_page_;
            idx_in_page_ = rhs.idx_in_page_;
            return *this;
        }

        reference operator*() const
        {
            return current_;
        }

        pointer operator->()
        {
            return &current_;
        }

        // Prefix increment
        Iterator &operator++()
        {
            if (current_.first == KeyT::NegativeInfinity())
            {
                // The iterator points to negative infinity. Increments
                // the iterator to the first page in the map, if the map
                // is not empty.
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>
                    *next_page = current_page_->next_page_;
                if (next_page->IsPosInf())
                {
                    // If the next page is the positive infinity page, the
                    // map is empty. The advanced iterator points to
                    // positive infinity.
                    current_.first = KeyT::PositiveInfinity();
                    current_.second = nullptr;
                    current_page_ = next_page;
                }
                else
                {
                    current_page_ = next_page;
                    idx_in_page_ = 0;
                    UpdateCurrent();
                }
            }
            else if (current_.first != KeyT::PositiveInfinity())
            {
                if (idx_in_page_ + 1 == current_page_->Size())
                {
                    current_page_ = current_page_->next_page_;
                    if (current_page_->IsPosInf())
                    {
                        // The next entry points to positive infinity.
                        current_.first = KeyT::PositiveInfinity();
                        current_.second = nullptr;
                    }
                    else
                    {
                        idx_in_page_ = 0;
                        UpdateCurrent();
                    }
                }
                else
                {
                    idx_in_page_++;
                    UpdateCurrent();
                }
            }
            // If the iterator points to positive infinity, keeps the
            // iterator unchanged.

            return *this;
        }

        // Prefix decrement
        Iterator &operator--()
        {
            if (current_.first == KeyT::PositiveInfinity())
            {
                // The iterator points to positive infinity. Decrements
                // the iterator to the last entry in the map, if the map
                // is not empty.
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>
                    *prev_page = current_page_->prev_page_;
                if (prev_page->IsNegInf())
                {
                    // If the previous page is the negative infinity page,
                    // the map is empty. The advanced iterator points to
                    // negative infinity.
                    current_.first = KeyT::NegativeInfinity();
                    current_.second = neg_inf_cce_;
                    current_page_ = prev_page;
                }
                else
                {
                    current_page_ = prev_page;
                    idx_in_page_ = current_page_->Size() - 1;
                    UpdateCurrent();
                }
            }
            else if (current_.first != KeyT::NegativeInfinity())
            {
                if (idx_in_page_ == 0)
                {
                    // update current_page to previous page, if any
                    current_page_ = current_page_->prev_page_;
                    if (current_page_->IsNegInf())
                    {
                        current_.first = KeyT::NegativeInfinity();
                        current_.second = neg_inf_cce_;
                    }
                    else
                    {
                        idx_in_page_ = current_page_->Size() - 1;
                        UpdateCurrent();
                    }
                }
                else
                {
                    idx_in_page_--;
                    UpdateCurrent();
                }
            }
            // If the current points to negative infinity, keeps the
            // iterator unchanged.

            return *this;
        }

        // Postfix incremen+t
        Iterator operator++(int)
        {
            Iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        // Postfix increment
        Iterator operator--(int)
        {
            Iterator tmp = *this;
            --(*this);
            return tmp;
        }

        friend bool operator==(const Iterator &lhs, const Iterator &rhs)
        {
            // The two iterators are equal, if they point to the same cc
            // entry. Note that when the iterator points to positive
            // infinity, the pointed cc entry is null.
            return lhs.current_.second == rhs.current_.second;
        };

        friend bool operator!=(const Iterator &lhs, const Iterator &rhs)
        {
            return lhs.current_.second != rhs.current_.second;
        };

        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *GetPage() const
        {
            return current_page_;
        }

        size_t GetIdxInPage() const
        {
            return idx_in_page_;
        }

        DefragResult DefragCurrentIfNecessary(mi_heap_t *heap)
        {
            assert(current_page_ != nullptr &&
                   idx_in_page_ < current_page_->Size());

            KeyT *key = const_cast<KeyT *>(current_.first);
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce =
                current_.second;
            NonBlockingLock *key_lock = cce->GetKeyLock();
            NonBlockingLock *gap_lock = cce->GetGapLock();
            RecordStatus record_status = cce->PayloadStatus();

            // it is possible to move the re-allocate the cce during
            // defragmentation, and cce pointer will be changed, since cce
            // pointer will be kept some where else and used later so we need to
            // avoid defrag in case of below conditions
            // 1. key or gap lock is not empty(cce pointer kept by cc req for
            // re-entry)
            // 2. cce is not persistent(flush worker save the record, cce
            // pointer used for update ckpt ts)
            // 3. record status is unknown(fetch from data store, cce pointer
            // used for backfill)
            if ((key_lock != nullptr && !key_lock->IsEmpty()) ||
                (gap_lock != nullptr && !gap_lock->IsEmpty()))
            {
                return FAILED_BY_LOCK;
            }

            if (cce->GetBeingCkpt())
            {
                return FAILED_BY_CKPT;
            }

            if (record_status == RecordStatus::Unknown)
            {
                return FAILED_BY_KV_LOADING;
            }

            // if defrag happen on key or cce or cce payload
            bool defraged = false;

            // defrag key
            TxKey tx_key = TxKey(key);
            if (tx_key.NeedsDefrag(heap))
            {
                auto key_clone = std::make_unique<KeyT>(*key);
                auto &keys = current_page_->keys_;
                auto it = keys.begin() + idx_in_page_;
                assert(it != keys.end());
                it = keys.erase(it);
                it = keys.emplace(it, std::move(*(key_clone.release())));
                defraged = true;
            }

            // defrag cce
            if (cce != nullptr)
            {
                float cce_utilization = mi_heap_page_utilization(heap, cce);
                float payload_utilization = 1.0;
                bool payload_needs_defrag = false;
                TxRecord *payload =
                    static_cast<TxRecord *>(cce->payload_.cur_payload_.get());
                if (payload != nullptr)
                {
                    payload_utilization =
                        mi_heap_page_utilization(heap, payload);
                    payload_needs_defrag = payload->NeedsDefrag(heap);
                }
                if (cce_utilization < 0.8 || payload_utilization < 0.8 ||
                    payload_needs_defrag)
                {
                    auto &entries = current_page_->entries_;
                    auto it = entries.begin() + idx_in_page_;
                    assert(it != entries.end());
                    std::unique_ptr<CcEntry<KeyT,
                                            ValueT,
                                            VersionedRecord,
                                            RangePartitioned>>
                        old_cce = std::move(*it);
                    it = entries.erase(it);
                    it = entries.emplace(
                        it,
                        std::make_unique<CcEntry<KeyT,
                                                 ValueT,
                                                 VersionedRecord,
                                                 RangePartitioned>>());
                    CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>
                        *cce_clone = it->get();
                    old_cce->CloneForDefragment(cce_clone);
                    defraged = true;
                }
            }

            // if key or cce is defraged, update current
            if (defraged)
            {
                UpdateCurrent();
                key = const_cast<KeyT *>(current_.first);
                cce = current_.second;
            }

            if (VersionedRecord && cce->payload_.GetArchives() != nullptr)
            {
                auto archives = cce->payload_.GetArchives();
                // To defrag a list, we go through and defrag each node in the
                // list
                for (auto it = archives->begin(); it != archives->end(); ++it)
                {
                    // try to defrag the list
                    void *archive_address = &(*it);
                    bool vs_rec_need_defraged = it->NeedsDefrag(heap);
                    if (mi_heap_page_utilization(heap, archive_address) < 0.8 ||
                        vs_rec_need_defraged)
                    {
                        auto old_vr = std::move(*it);
                        it = archives->erase(it);
                        it = archives->emplace(it);
                        old_vr.CloneForDefragment(&(*it));
                        defraged = true;
                    }
                }
            }

            if (defraged)
            {
                return DEFRAGED;
            }

            return NOFRAGED;
        }

        void UpdateCurrent()
        {
            assert(current_page_ != nullptr &&
                   idx_in_page_ < current_page_->Size());
            current_.first = &current_page_->keys_[idx_in_page_];
            current_.second = current_page_->entries_[idx_in_page_].get();
        }

    protected:
        std::pair<const KeyT *,
                  CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>
            current_{nullptr, nullptr};
        // neg_inf_cce_ is necessary for its gap
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *neg_inf_cce_{
            nullptr};

        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *current_page_{
            nullptr};
        size_t idx_in_page_{};
    };

    friend Iterator;

    /**
     * @brief Returns an iterator that points to negative infinity.
     *
     * @return Iterator
     */
    Iterator Begin()
    {
        return Iterator(&neg_inf_, &neg_inf_page_, &neg_inf_);
    }

    /**
     * @brief Returns an iterator that points to positive infinity.
     *
     * @return Iterator
     */
    Iterator End()
    {
        return Iterator(&pos_inf_, &pos_inf_page_, &neg_inf_);
    }

    Iterator Find(const KeyT &key)
    {
        if (&key == KeyT::NegativeInfinity())
        {
            return Begin();
        }

        Iterator lb_it = LowerBound(key);
        if (lb_it != End() && *lb_it->first == key)
        {
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
                lb_it.GetPage();
            shard_->UpdateLruList(ccp, false);
            return lb_it;
        }
        else
        {
            // The input key does not exist.
            return End();
        }
    }

    Iterator FindEmplace(const KeyT &key,
                         bool force_emplace = false,
                         bool read_only_req = false)
    {
        bool emplace;
        return FindEmplace(key, emplace, force_emplace, read_only_req);
    }

    std::pair<bool, size_t> ShuffleKeyAndPageSplit(
        BtreeMapIterator &target_iter,
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *&target_page,
        std::deque<SliceDataItem> &slice_items,
        std::vector<std::pair<size_t, bool>> &location_infos,
        size_t new_key_cnt,
        size_t first_key_index_in_page,
        int32_t &normal_rec_change)
    {
        size_t new_first_key_idx_in_page = 0;
        bool need_forward_iter = true;
        assert(target_iter->second.get() == target_page);

        size_t total_size = target_page->Size() + new_key_cnt;
        assert(total_size == location_infos.size());

        if (total_size <=
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>::
                split_threshold_)
        {
            target_page->keys_.resize(total_size);
            target_page->entries_.resize(total_size);
            target_page->PlaceKeys(target_page->keys_,
                                   target_page->entries_,
                                   slice_items,
                                   location_infos,
                                   first_key_index_in_page,  // start index
                                   total_size,               // end index
                                   first_key_index_in_page,  // offset
                                   shard_->EnableMvcc(),
                                   normal_rec_change,
                                   shard_,
                                   shard_->NowInMilliseconds());

            shard_->UpdateLruList(target_page, true);
            TryUpdatePageKey(target_iter);
            assert(target_iter->second.get() == target_page);
            assert(target_iter->first == target_page->FirstKey());
        }
        else
        {
            // 0.7
            double fill_factor = 1;
            size_t data_cnt_per_page = std::ceil(
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>::
                    split_threshold_ *
                fill_factor);
            assert(data_cnt_per_page <= 64);

            size_t page_cnt = total_size / data_cnt_per_page;
            size_t remain_size = total_size % data_cnt_per_page;
            assert((page_cnt * data_cnt_per_page) + remain_size == total_size);

            std::vector<KeyT> old_page_keys = std::move(target_page->keys_);
            std::vector<std::unique_ptr<
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>>
                old_page_entries = std::move(target_page->entries_);
            assert(target_page->keys_.empty());
            assert(target_page->entries_.empty());
            target_page->keys_.reserve(
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>::
                    split_threshold_);
            target_page->entries_.reserve(
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>::
                    split_threshold_);

            uint64_t old_page_last_dirty_commit_ts =
                target_page->last_dirty_commit_ts_;

            for (size_t page_idx = 0; page_idx < page_cnt; ++page_idx)
            {
                size_t offset = page_idx * data_cnt_per_page;
                std::unique_ptr<
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>>
                    new_page_owner = nullptr;

                if (page_idx != 0)
                {
                    // create new page
                    new_page_owner = std::make_unique<CcPage<KeyT,
                                                             ValueT,
                                                             VersionedRecord,
                                                             RangePartitioned>>(
                        this, target_page, target_page->next_page_);
                    target_page = new_page_owner.get();
                }

                target_page->keys_.resize(data_cnt_per_page);
                target_page->entries_.resize(data_cnt_per_page);
                target_page->PlaceKeys(old_page_keys,
                                       old_page_entries,
                                       slice_items,
                                       location_infos,
                                       0,
                                       data_cnt_per_page,
                                       offset,
                                       shard_->EnableMvcc(),
                                       normal_rec_change,
                                       shard_,
                                       shard_->NowInMilliseconds());

                if (page_idx == 0)
                {
                    // Update ccmap node key
                    TryUpdatePageKey(target_iter);
                    assert(target_iter->second.get() == target_page);
                }
                else
                {
                    assert(new_page_owner != nullptr);
                    assert(target_page == new_page_owner.get());
                    // Emplace new page to ccmap
                    target_iter = ccmp_.try_emplace(++target_iter,
                                                    target_page->FirstKey(),
                                                    std::move(new_page_owner));

                    assert(target_iter->second.get() == target_page);
                }

                // Update `last_dirty_commit_ts_`
                target_page->last_dirty_commit_ts_ =
                    old_page_last_dirty_commit_ts;
                // Move target_page to lru tail. Update last
                // access ts
                shard_->UpdateLruList(target_page, true);
            }

            if (remain_size > 0)
            {
                bool need_rebalance =
                    remain_size <
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>::
                        merge_threshold_;
                // Insert remaining data to next page.
                if (need_rebalance &&
                    target_page->next_page_ != &pos_inf_page_ &&
                    target_page->next_page_->Size() + remain_size <=
                        CcPage<KeyT,
                               ValueT,
                               VersionedRecord,
                               RangePartitioned>::split_threshold_)
                {
                    size_t next_page_old_size = target_page->next_page_->Size();
                    target_page->next_page_->keys_.resize(next_page_old_size +
                                                          remain_size);
                    target_page->next_page_->entries_.resize(
                        next_page_old_size + remain_size);

                    for (size_t idx = next_page_old_size; idx > 0; --idx)
                    {
                        target_page->next_page_->keys_[idx + remain_size - 1] =
                            std::move(target_page->next_page_->keys_[idx - 1]);
                        target_page->next_page_
                            ->entries_[idx + remain_size - 1] = std::move(
                            target_page->next_page_->entries_[idx - 1]);
                    }

                    target_page->next_page_->PlaceKeys(
                        old_page_keys,
                        old_page_entries,
                        slice_items,
                        location_infos,
                        0,
                        remain_size,
                        page_cnt * data_cnt_per_page,
                        shard_->EnableMvcc(),
                        normal_rec_change,
                        shard_,
                        shard_->NowInMilliseconds());

                    target_page->next_page_->last_dirty_commit_ts_ =
                        std::max(target_page->next_page_->last_dirty_commit_ts_,
                                 old_page_last_dirty_commit_ts);
                    shard_->UpdateLruList(target_page->next_page_, true);

                    // Update next page key
                    target_iter++;
                    TryUpdatePageKey(target_iter);
                    assert(target_iter->second.get() ==
                           target_page->next_page_);
                    target_iter--;

                    new_first_key_idx_in_page = remain_size;
                    return {need_forward_iter, new_first_key_idx_in_page};
                }

                // Create new page.
                auto new_page_owner = std::make_unique<
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>>(
                    this, target_page, target_page->next_page_);
                target_page = new_page_owner.get();

                target_page->keys_.resize(remain_size);
                target_page->entries_.resize(remain_size);

                target_page->PlaceKeys(old_page_keys,
                                       old_page_entries,
                                       slice_items,
                                       location_infos,
                                       0,
                                       remain_size,
                                       page_cnt * data_cnt_per_page,
                                       shard_->EnableMvcc(),
                                       normal_rec_change,
                                       shard_,
                                       shard_->NowInMilliseconds());

                target_page->last_dirty_commit_ts_ =
                    old_page_last_dirty_commit_ts;
                shard_->UpdateLruList(target_page, true);
                target_iter = ccmp_.try_emplace(++target_iter,
                                                target_page->FirstKey(),
                                                std::move(new_page_owner));
                assert(target_iter->second.get() == target_page);

                // Move data from `next_page` to `target_page`
                if (need_rebalance && target_page->next_page_ != &pos_inf_page_)
                {
                    assert(target_page->Size() <
                           target_page->next_page_->Size());
                    new_first_key_idx_in_page = target_page->Size();

                    auto next_page_iter = target_iter;
                    ++next_page_iter;
                    RedistributeBetweenPages(target_iter, next_page_iter);

                    assert(target_iter->second.get() == target_page);
                    need_forward_iter = false;
                }
            }
        }
        assert(target_iter->second.get() == target_page);
        return {need_forward_iter, new_first_key_idx_in_page};
    }

    bool BatchFillSlice(std::deque<SliceDataItem> &slice_items,
                        bool force_emplace,
                        size_t first_index,
                        size_t end_idx)
    {
        if (slice_items.empty() || first_index >= end_idx)
        {
            return true;
        }

        // catalog and range ccmap bypass shard memory limit. since
        // checkpointer may emplace ccentry into ccmap.
        CcShardHeap *shard_heap = shard_->GetShardHeap();
        int64_t heap_alloc, heap_commit;
        if (shard_->EnableDefragment() && shard_heap != nullptr &&
            shard_heap->NeedDefragment(&heap_alloc, &heap_commit) &&
            shard_heap->AsyncDefragment())
        {
            LOG(INFO) << "Found memory fragementation in ccs "
                      << shard_->core_id_
                      << ", total comitted memory: " << heap_commit
                      << ", actual used memory " << heap_alloc
                      << ", frag ratio " << std::setprecision(2)
                      << 100 * (static_cast<float>(heap_commit - heap_alloc) /
                                heap_commit);
        }

        if (shard_heap != nullptr &&
            shard_heap->Full(&heap_alloc, &heap_commit))
        {
            if (txservice_enable_cache_replacement &&
                shard_heap->NeedCleanShard(heap_alloc, heap_commit))
            {
                // The shard has reached the maximal capacity. Tries to clean cc
                // entries that have been checkpointed but are not being
                // accessed by active tx's.
                shard_->WakeUpShardCleanCc();
            }
            // else: cache replacement is disable or no need to clean shard.

            if (!force_emplace && !table_name_.IsMeta())
            {
                return false;
            }
        }

        int32_t normal_rec_change = 0;
        typename decltype(ccmp_)::iterator target_iter;
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *target_page =
            nullptr;

        if (ccmp_.begin() == ccmp_.end())
        {
            bool inserted;
            // ccmap is empty, insert a page
            const KeyT *search_key = static_cast<const KeyT *>(
                slice_items[first_index].key_.KeyPtr());
            std::tie(target_iter, inserted) = ccmp_.try_emplace(
                *search_key,
                std::make_unique<
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>>(
                    this, &neg_inf_page_, &pos_inf_page_));
            assert(inserted);
        }
        else
        {
            const KeyT *search_key = static_cast<const KeyT *>(
                slice_items[first_index].key_.KeyPtr());
            target_iter = ccmp_.upper_bound(*search_key);

            if (target_iter != ccmp_.begin())
            {
                target_iter--;
            }
        }

        target_page = target_iter->second.get();

        // find first key index
        const KeyT *first_key =
            static_cast<const KeyT *>(slice_items[first_index].key_.KeyPtr());
        size_t key_idx_in_page = target_page->LowerBound(*first_key);
        size_t new_key_cnt = 0;
        size_t first_key_index_in_page = key_idx_in_page;

        // `location_infos` records when all new keys are inserted into the
        // page, where does the key in each position on the page come from
        // The first value of pair: the index of vector or the index of
        // target_page.
        // The second value of pair: true means the key come from items vector,
        // false means the key come from target_page.
        // <key_idx_in_page, false>, <slice_items_idx, true>
        std::vector<std::pair<size_t, bool>> location_infos;
        location_infos.reserve(
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>::
                split_threshold_ +
            FillStoreSliceCc::MaxScanBatchSize);

        // keys before `key_idx_in_page` are come from target_page. So set all
        // values to false
        for (size_t idx = 0; idx < key_idx_in_page; ++idx)
        {
            location_infos.emplace_back(idx, false);
        }

        // fast path if all inserted keys fit in between 2 existing keys
        bool all_key_no_exists = false;
        if (key_idx_in_page == target_page->Size() ||
            *first_key < *target_page->Key(key_idx_in_page))
        {
            assert(end_idx > 0);
            const KeyT *end_key = static_cast<const KeyT *>(
                slice_items[end_idx - 1].key_.KeyPtr());

            if (key_idx_in_page == target_page->Size())
            {
                if (*end_key < target_page->next_page_->FirstKey())
                {
                    all_key_no_exists = true;
                }
            }
            else
            {
                assert(key_idx_in_page < target_page->Size());
                if (*end_key < *target_page->Key(key_idx_in_page))
                {
                    all_key_no_exists = true;
                }
            }
        }

        if (all_key_no_exists)
        {
            new_key_cnt += (end_idx - first_index);
            // keys are come from `slice_items`. So set all values to true
            for (size_t idx = first_index; idx < end_idx; ++idx)
            {
                location_infos.emplace_back(idx, true);
            }
            // keys after `key_idx_in_page` are come from target_page. So set
            // all values to false
            for (size_t idx = key_idx_in_page; idx < target_page->Size(); ++idx)
            {
                location_infos.emplace_back(idx, false);
            }

            ShuffleKeyAndPageSplit(target_iter,
                                   target_page,
                                   slice_items,
                                   location_infos,
                                   new_key_cnt,
                                   first_key_index_in_page,
                                   normal_rec_change);
            if (!VersionedRecord)
            {
                normal_obj_sz_ += normal_rec_change;
            }
            size_ += (end_idx - first_index);

            return true;
        }

        bool need_forward_iter = true;
        size_t item_idx = first_index;

        while (item_idx < end_idx)
        {
            const KeyT *target_key =
                static_cast<const KeyT *>(slice_items[item_idx].key_.KeyPtr());

            // Move to next page.
            if (key_idx_in_page == target_page->Size())
            {
                if (*target_key < target_page->next_page_->FirstKey())
                {
                    location_infos.emplace_back(item_idx, true);
                    new_key_cnt++;
                    item_idx++;
                    continue;
                }

                if (new_key_cnt > 0)
                {
                    std::tie(need_forward_iter, key_idx_in_page) =
                        ShuffleKeyAndPageSplit(target_iter,
                                               target_page,
                                               slice_items,
                                               location_infos,
                                               new_key_cnt,
                                               first_key_index_in_page,
                                               normal_rec_change);

                    assert(new_key_cnt > 0);
                    // Update CCMap size
                    size_ += new_key_cnt;
                }
                else
                {
                    // Forward to next page
                    key_idx_in_page = 0;
                    need_forward_iter = true;
                    // all keys exists. only move target page to lru tail
                    shard_->UpdateLruList(target_page, false);
                }

                new_key_cnt = 0;
                location_infos.clear();
                first_key_index_in_page = key_idx_in_page;
                // keys before `key_idx_in_page` are come from target_page. So
                // set all values to false
                for (size_t lidx = 0; lidx < key_idx_in_page; ++lidx)
                {
                    location_infos.emplace_back(lidx, false);
                }

                if (need_forward_iter)
                {
                    target_iter++;
                }

                if (target_iter == ccmp_.end())
                {
                    assert(item_idx == end_idx);
                    assert(new_key_cnt == 0);
                    break;
                }

                target_page = target_iter->second.get();

                continue;
            }

            if (*target_key < *target_page->Key(key_idx_in_page))
            {
                // `true` means the key comes from the items vector.
                location_infos.emplace_back(item_idx, true);

                new_key_cnt++;
                // Move forwrd item index
                item_idx++;
            }
            else if (*target_key == *target_page->Key(key_idx_in_page))
            {
                location_infos.emplace_back(key_idx_in_page, false);
                // Don't need to empalce key. But we need to update entry's
                // payload
                target_page->Entry(key_idx_in_page)
                    ->UpdateCcEntry(slice_items[item_idx],
                                    shard_->EnableMvcc(),
                                    normal_rec_change,
                                    target_page,
                                    shard_,
                                    shard_->NowInMilliseconds());
                item_idx++;
                key_idx_in_page++;
            }
            else
            {
                location_infos.emplace_back(key_idx_in_page, false);
                key_idx_in_page++;
            }
        }

        if (new_key_cnt > 0)
        {
            assert(target_iter->second.get() == target_page);
            // keys after `key_idx_in_page` are come from target_page. So set
            // all values to false
            for (size_t idx = key_idx_in_page; idx < target_page->Size(); ++idx)
            {
                location_infos.emplace_back(idx, false);
            }

            ShuffleKeyAndPageSplit(target_iter,
                                   target_page,
                                   slice_items,
                                   location_infos,
                                   new_key_cnt,
                                   first_key_index_in_page,
                                   normal_rec_change);

            assert(new_key_cnt > 0);
            // Update Ccmap size
            size_ += new_key_cnt;
        }

        if (!VersionedRecord)
        {
            normal_obj_sz_ += normal_rec_change;
        }

        return true;
    }

    /**
     * Find or Emplace the CcEntry with key @param key.
     *
     * @param key
     * @param read_only_req For read only request we will try to find
     * the cce regardless of shard memory. If we can't find it and shard
     * is full, we will return End.
     * @return The Iterator pointing to the target CcEntry
     */
    Iterator FindEmplace(const KeyT &key,
                         bool &emplace,
                         bool force_emplace = false,
                         bool read_only_req = false)
    {
        emplace = false;
        bool fail_if_not_found = false;
        if (&key == KeyT::NegativeInfinity())
        {
            return Begin();
        }
        if (&key == KeyT::PositiveInfinity())
        {
            return End();
        }

        // catalog and range ccmap bypass shard memory limit. since
        // checkpointer may emplace ccentry into ccmap.
        CcShardHeap *shard_heap = shard_->GetShardHeap();
        int64_t heap_alloc, heap_commit;
        if (shard_->EnableDefragment() && shard_heap != nullptr &&
            shard_heap->NeedDefragment(&heap_alloc, &heap_commit) &&
            shard_heap->AsyncDefragment())
        {
            LOG(INFO) << "Found memory fragementation in ccs "
                      << shard_->core_id_
                      << ", total comitted memory: " << heap_commit
                      << ", actual used memory " << heap_alloc
                      << ", frag ratio " << std::setprecision(2)
                      << 100 * (static_cast<float>(heap_commit - heap_alloc) /
                                heap_commit);
        }

        if (shard_heap != nullptr &&
            shard_heap->Full(&heap_alloc, &heap_commit))
        {
            if (txservice_enable_cache_replacement &&
                shard_heap->NeedCleanShard(heap_alloc, heap_commit))
            {
                // The shard has reached the maximal capacity. Tries to clean cc
                // entries that have been checkpointed but are not being
                // accessed by active tx's.
                shard_->WakeUpShardCleanCc();
            }
            // else: cache replacement is disable or no need to clean shard.

            if (!force_emplace && !table_name_.IsMeta() && !read_only_req)
            {
                return End();
            }

            fail_if_not_found = read_only_req;
        }

        if (ccmp_.begin() == ccmp_.end())
        {
            if (fail_if_not_found)
            {
                return End();
            }
            // ccmap is empty, insert a page
            auto [it, inserted] = ccmp_.try_emplace(
                key,
                std::make_unique<
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>>(
                    this, &neg_inf_page_, &pos_inf_page_));
            assert(inserted);
            // This silences the -Wunused-but-set-variable warning
            // without any runtime overhead.
            (void) inserted;
        }

        // First locate target page, then find or emplace `key` in the
        // page.
        auto ub_it = ccmp_.upper_bound(key);
        assert(ub_it == ccmp_.end() || key < ub_it->first);
        auto target_it = ub_it;
        if (target_it != ccmp_.begin())
        {
            --target_it;
            assert(target_it->first <= key);
        }
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *target_page =
            target_it->second.get();

        size_t idx_in_page = target_page->Find(key);
        if (idx_in_page < target_page->Size())
        {
            // found, return Iterator
            shard_->UpdateLruList(target_page, false);
            Iterator iterator(target_page, idx_in_page, &neg_inf_);
            return iterator;
        }
        if (fail_if_not_found)
        {
            return End();
        }

        // not found, emplace key into target page, split the page if
        // it's full
        if (target_page->Full())
        {
            // target page is full, choose the next or previous page if `key`
            // can be inserted into them
            if (target_page->LastKey() < key)
            {
                // choose the next page
                target_it++;
                if (target_it == ccmp_.end())
                {
                    // create the next page
                    target_it = ccmp_.try_emplace(
                        target_it,
                        key,
                        std::make_unique<CcPage<KeyT,
                                                ValueT,
                                                VersionedRecord,
                                                RangePartitioned>>(
                            this, target_page, target_page->next_page_));
                }
            }
            else if (key < target_page->FirstKey())
            {
                // choose the previous page
                if (target_it == ccmp_.begin())
                {
                    // create the previous page
                    target_it = ccmp_.try_emplace(
                        target_it,
                        key,
                        std::make_unique<CcPage<KeyT,
                                                ValueT,
                                                VersionedRecord,
                                                RangePartitioned>>(
                            this, target_page->prev_page_, target_page));
                }
                else
                {
                    --target_it;
                }
            }
            target_page = target_it->second.get();
        }

        if (target_page->Full())
        {
            // split this page
            std::vector<KeyT> new_page_keys;
            std::vector<std::unique_ptr<
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>>
                new_page_entries;
            uint64_t new_last_commit_ts = 0;
            uint64_t new_smallest_ttl = UINT64_MAX;
            target_page->Split(new_page_keys,
                               new_page_entries,
                               new_last_commit_ts,
                               new_smallest_ttl);

            const KeyT &key_of_new_page = *new_page_keys.begin();

            auto new_page_it = ccmp_.try_emplace(
                target_it,
                key_of_new_page,
                std::make_unique<
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>>(
                    this,
                    std::move(new_page_keys),
                    std::move(new_page_entries),
                    target_page,
                    target_page->next_page_));
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *new_page =
                new_page_it->second.get();
            assert(new_page_it->first == new_page->FirstKey());
            new_page->last_dirty_commit_ts_ = new_last_commit_ts;
            new_page->smallest_ttl_ = new_smallest_ttl;

            for (auto &cce : new_page->entries_)
            {
                cce->UpdateCcPage(new_page);
            }

            // insert new page into lru list right after old
            // page
            if (target_page->lru_next_ != nullptr)
            {
                LruPage *next = target_page->lru_next_;
                new_page->lru_next_ = next;
                next->lru_prev_ = new_page;
                target_page->lru_next_ = new_page;
                new_page->lru_prev_ = target_page;
                new_page->last_access_ts_ = target_page->last_access_ts_;
            }

            // Iterators are invalidated after split. Find the target page.
            if (new_page->FirstKey() <= key)
            {
                target_it = new_page_it;
                target_page = new_page;
            }
            else
            {
                target_it = --new_page_it;
                target_page = target_it->second.get();
                assert(target_it->first == target_page->FirstKey());
                assert(target_it->second->LastKey() < new_page->FirstKey());
            }
        }

        idx_in_page = target_page->Emplace(key);
        emplace = true;
        // modify page key in the map if it changed
        TryUpdatePageKey(target_it);

        // update lru list
        shard_->UpdateLruList(target_page, true);
        size_++;

        return Iterator(target_page, idx_in_page, &neg_inf_);
    }

    Iterator Emplace(const KeyT &key)
    {
        return FindEmplace(key);
    }

    bool BackFill(LruEntry *entry,
                  uint64_t commit_ts,
                  RecordStatus status,
                  const std::string &rec_str) override
    {
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce =
            static_cast<
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                entry);
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp =
            static_cast<
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                cce->GetCcPage());
        assert(ccp != nullptr);
        if (status == RecordStatus::Unknown)
        {
            // fetch record fails. Remove the pin on the cce.
            cce->GetKeyGapLockAndExtraData()->ReleasePin();
            cce->RecycleKeyLock(*shard_);
            if (cce->IsFree())
            {
                CleanEntry(cce, ccp);
            }
            return true;
        }
        const uint64_t cce_version = cce->CommitTs();

        cce->SetCkptTs(commit_ts);

        if (cce_version < commit_ts)
        {
            if (RangePartitioned && txservice_enable_key_cache &&
                table_name_.IsBase() && status == RecordStatus::Deleted &&
                cce->PayloadStatus() == RecordStatus::Unknown)
            {
                // Addking a new key to the ccm. If the key is in Normal rec
                // status, it should already be in the key cache. Only add it if
                // it's in DELETED.
                auto res = shard_->local_shards_.AddKeyToKeyCache(
                    table_name_,
                    cc_ng_id_,
                    shard_->core_id_,
                    *ccp->KeyOfEntry(cce));
                if (res == RangeSliceOpStatus::Retry)
                {
                    // Retry if the slice key cache is being loaded.
                    return false;
                }
            }
            cce->SetCommitTsPayloadStatus(commit_ts, status);

            if (status == RecordStatus::Deleted)
            {
                cce->payload_.SetCurrentPayload(nullptr);
            }
            else
            {
                size_t offset = 0;
                cce->payload_.DeserializeCurrentPayload(rec_str.c_str(),
                                                        offset);
            }
        }
        else if (cce_version > 1 && commit_ts < cce_version &&
                 shard_->EnableMvcc())
        {
            // The cc entry's commit ts is 1 when it is initialized.
            // Commit ts greater than 1 means that the key is
            // already cached in memory.
            auto payload = std::make_shared<ValueT>();
            size_t offset = 0;
            payload->Deserialize(rec_str.c_str(), offset);
            cce->AddArchiveRecord(payload, status, commit_ts);
        }
        if (RangePartitioned && cce->entry_info_.DataStoreSize() == INT32_MAX)
        {
            cce->entry_info_.SetDataStoreSize(
                status == RecordStatus::Deleted
                    ? 0
                    : cce->payload_.cur_payload_->Size() +
                          ccp->KeyOfEntry(cce)->Size());
        }

        cce->GetKeyGapLockAndExtraData()->ReleasePin();
        cce->RecycleKeyLock(*shard_);

        return true;
    }

    bool BackFillArchives(
        LruEntry *entry,
        const std::vector<std::tuple<uint64_t, RecordStatus, std::string>>
            &archive_records,
        bool need_unpin = false) override
    {
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce =
            static_cast<
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                entry);

        assert(shard_->EnableMvcc());
        for (auto &archive_record : archive_records)
        {
            RecordStatus rec_st = std::get<1>(archive_record);
            if (rec_st == RecordStatus::Normal)
            {
                auto payload = std::make_shared<ValueT>();
                size_t offset = 0;
                auto &rec_str = std::get<2>(archive_record);
                payload->Deserialize(rec_str.c_str(), offset);
                cce->AddArchiveRecord(payload,
                                      std::get<1>(archive_record),
                                      std::get<0>(archive_record));
            }
            else
            {
                cce->AddArchiveRecord(nullptr,
                                      std::get<1>(archive_record),
                                      std::get<0>(archive_record));
            }
        }

        if (need_unpin)
        {
            cce->GetKeyGapLockAndExtraData()->ReleasePin();
            cce->RecycleKeyLock(*shard_);
        }

        return true;
    }

    ScanType GetScanType(bool is_include_floor_cce)
    {
        if (is_include_floor_cce)
        {
            return ScanType::ScanBoth;
        }
        else
        {
            return ScanType::ScanGap;
        }
    }

    /**
     * Whether ScanGap or ScanBoth depends on whether this is
     * range_cc_map scan. For template_cc_map, start from it's gap; for
     * range_cc_map, start from it's key and gap.
     * @param it
     * @param is_include_floor_cce
     * @return
     */
    std::pair<Iterator, ScanType> MakeForwardScanPair(Iterator it,
                                                      bool is_include_floor_cce)
    {
        ScanType scan_type = GetScanType(is_include_floor_cce);

        return std::make_pair(it, scan_type);
    }

    /**
     * Find lower bound of @param key in map, i.e. the first entry whose
     * key is equal to or greater than @param key. Return an Iterator
     * pointing to this entry, if no such entry, return Begin() which
     * points to neg_inf_.
     * @param key
     * @return
     */
    Iterator LowerBound(const KeyT &key)
    {
        if (&key == KeyT::NegativeInfinity())
        {
            return Begin();
        }

        // ccmp_ key is each page's smallest key, so the lower bound of
        // `key` might fall into either of two adjacent pages
        auto lb_it = ccmp_.lower_bound(key);
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page1 =
            nullptr;
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page2 =
            nullptr;
        auto pg_it1 = lb_it;
        auto pg_it2 = lb_it;
        if (pg_it2 != ccmp_.end())
        {
            page2 = pg_it2->second.get();
        }
        if (pg_it1 != ccmp_.begin())
        {
            --pg_it1;
            page1 = pg_it1->second.get();
        }
        // now we have the order:
        // neg_inf < page1(if exist)->FirstKey() < key <= page2(if
        // exist)->FirstKey() < pos_inf_

        if (page2 == nullptr && (page1 == nullptr || page1->LastKey() < key))
        {
            // no key bigger than or equal to `key`
            return End();
        }
        else if (page1 != nullptr && key <= page1->LastKey())
        {
            // page1->FirstKey < key <= page1->LastKey(), the lower
            // bound of `key` must locate in page1
            size_t idx_in_page = page1->LowerBound(key);
            return Iterator(page1, idx_in_page, &neg_inf_);
        }
        else
        {
            // page1->LastKey() < key <= page2->FirstKey(), the lower
            // bound of `key` must be the first key of page2
            return Iterator(page2, 0, &neg_inf_);
        }
    }

    /**
     * Find upper bound of @param key in map, i.e. the first entry whose
     * key is greater than @param key. Return an Iterator pointing to
     * this entry, if no such entry, return End() which points to
     * pos_inf_.
     * @param key
     * @return
     */
    Iterator UpperBound(const KeyT &key)
    {
        // ccmp_ key is each page's smallest key, so the upper bound of
        // `key` might fall into either of two adjacent pages
        auto ub_it = ccmp_.upper_bound(key);
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page1 =
            nullptr;
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page2 =
            nullptr;
        auto pg_it1 = ub_it;
        auto pg_it2 = ub_it;
        if (pg_it2 != ccmp_.end())
        {
            page2 = pg_it2->second.get();
        }
        if (pg_it1 != ccmp_.begin())
        {
            --pg_it1;
            page1 = pg_it1->second.get();
        }
        // now we have the order:
        // neg_inf <= page1(if exist)->FirstKey() <= key < page2(if
        // exist)->FirstKey() < pos_inf

        if (page2 == nullptr && (page1 == nullptr || page1->LastKey() <= key))
        {
            // no key bigger than `key`
            return End();
        }
        else if (page1 != nullptr && key < page1->LastKey())
        {
            // page1->FirstKey <= key < page1->LastKey(), the upper
            // bound of `key` must locate in page1
            size_t idx_in_page = page1->UpperBound(key);
            return Iterator(page1, idx_in_page, &neg_inf_);
        }
        else
        {
            // page1->LastKey() <= key < page2->FirstKey(), the upper
            // bound of `key` must be the first key of page2
            return Iterator(page2, 0, &neg_inf_);
        }
    }

    /**
     * @brief Finds the greatest cc entry whose key is less than or
     * equal to the input key. If the map is empty, the floor key is
     * negative infinity.
     *
     * @param key The input key
     * @return The Iterator pointing to the cc entry whose key is the
     * greatest key less than or equal to the input key. If `key` is
     * positive infinity, the Iterator points to the last key in the
     * map.
     */
    Iterator Floor(const KeyT &key)
    {
        Iterator it = LowerBound(key);
        if (*it->first != key ||
            it == End())  // special case for positive infinity
        {
            // lower bound of a non-negative infinity key should never
            // be Begin()
            assert(it != Begin());
            it--;
        }
        return it;
    }

    /**
     * @brief Searches the start cc entry of a forward scan.
     *
     * @param key Search key
     * @param inclusive Whether or not the start key is included in the
     * scan
     * @param is_include_floor_cce This param is used only by
     * range_cc_map scan, and is always true. Range scan searches for
     * the floor of the search key and returns both its key and gap.
     * @return std::pair<typename std::map<KeyT, CcEntry<KeyT,
     * ValueT>>::const_iterator, ScanType> A pair of a forward map
     * iterator starting from the start cc entry and whether the scan
     * includes the start cc entry's key or gap or both.
     */
    std::pair<Iterator, ScanType> ForwardScanStart(
        const KeyT &key, bool inclusive, bool is_include_floor_cce = false)
    {
        if (key.Type() == KeyType::NegativeInf)
        {
            return MakeForwardScanPair(Begin(), is_include_floor_cce);
        }
        if (key.Type() == KeyType::PositiveInf)
        {
            return MakeForwardScanPair(End(), is_include_floor_cce);
        }

        if (inclusive ||
            is_include_floor_cce)  // >= `key` or range_cc_map scan,
                                   // search for lower_bound(key)
        {
            auto lb_it = LowerBound(key);
            if (lb_it == End() || !(*lb_it->first == key))
            {
                // for template_cc_map, start from previous entry's gap;
                // for range_cc_map, start from previous entry's key and
                // gap
                if (lb_it == Begin())
                {
                    return MakeForwardScanPair(lb_it, is_include_floor_cce);
                }
                else
                {
                    --lb_it;
                    return MakeForwardScanPair(lb_it, is_include_floor_cce);
                }
            }
            else
            {
                // lb_it's key is exactly equal to `key`, start from
                // lb_it and its gap, but not including previous key gap
                return std::make_pair(lb_it, ScanType::ScanBoth);
            }
        }
        else  // > `key`, search for the entry before upper_bound(key)
        {
            auto ub_it = UpperBound(key);
            // start from the gap of previous entry of ub_it
            if (ub_it == Begin())
            {
                return std::make_pair(ub_it, ScanType::ScanGap);
            }
            else
            {
                ub_it--;
                return std::make_pair(ub_it, ScanType::ScanGap);
            }
        }
    }

    /**
     * @brief Searches the start cc entry of a backward scan.
     *
     * @param key Search key
     * @param inclusive Whether or not the start key is included in the
     * scan
     * @return std::pair<typename std::map<KeyT, CcEntry<KeyT,
     * ValueT>>::const_iterator, ScanType> A pair of a backward map
     * iterator starting from the start cc entry and whether the scan
     * includes the start cc entry's key or gap or both.
     */
    std::pair<Iterator, ScanType> BackwardScanStart(const KeyT &key,
                                                    bool inclusive)
    {
        if (key.Type() == KeyType::PositiveInf)
        {
            auto start_it = End();
            --start_it;
            if (start_it->first == KeyT::NegativeInfinity())
            {
                return std::make_pair(start_it, ScanType::ScanGap);
            }
            else
            {
                return std::make_pair(start_it, ScanType::ScanBoth);
            }
        }

        if (inclusive)  // <= `key`, search for the entry before
                        // upper_bound(key)
        {
            Iterator ub_it = UpperBound(key);
            if (ub_it == Begin())
            {
                // map empty or every key in map is greater than `key`,
                // return neg_inf_'s gap
                return std::make_pair(ub_it, ScanType::ScanGap);
            }
            else
            {
                --ub_it;
                // now, ub_it is the greatest entry equal to or less
                // than `key`

                if (!(*ub_it->first == key))
                {
                    // key not equal, should include the gap
                    return std::make_pair(ub_it, ScanType::ScanBoth);
                }
                else
                {
                    // exactly full key match, not include the gap
                    return std::make_pair(ub_it, ScanType::ScanKey);
                }
            }
        }
        else  // < `key`, search for the entry before lower_bound(key)
        {
            Iterator lb_it = LowerBound(key);
            if (lb_it == Begin())
            {
                // map empty or every key in ccm_ is greater than or
                // equal to `key`, return neg_inf_ gap
                return std::make_pair(lb_it, ScanType::ScanGap);
            }
            else
            {
                // starting from the gap and key of entry before lb_it
                --lb_it;
                return std::make_pair(lb_it, ScanType::ScanBoth);
            }
        }
    }

    void ScanKey(const KeyT *key,
                 CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
                 TemplateScanCache<KeyT, ValueT> *typed_cache,
                 bool include_gap,
                 uint32_t ng_id,
                 int64_t ng_term,
                 TxNumber txn,
                 uint64_t read_ts,
                 int32_t object_type,
                 bool is_read_snapshot,
                 bool &need_fetch_snapshot,
                 bool keep_deleted,
                 bool is_ckpt_delta = false,
                 bool is_require_keys = true,
                 bool is_require_recs = true)
    {
        TemplateScanTuple<KeyT, ValueT> *tuple = nullptr;
        uint32_t tuple_size = ScanCache::MetaDataSize;
        need_fetch_snapshot = false;
        if (is_read_snapshot)
        {
            assert(VersionedRecord);
            VersionResultRecord<ValueT> v_rec;
            cce->MvccGet(read_ts, v_rec);

            // For snapshot reads, only if the visible version's record
            // status is deleted and no lock has been put on it, should
            // the record be skipped in the result set. Note that if the
            // visible version is mising in memory, the key still needs
            // to be returned. Runtime will use the key to retrieve the
            // visible version from the data store.
            //
            // AcquireCc might emplace an unknown record and put a wlock on it.
            // If the wlock_ts of the record is larger than the scan read_ts,
            // then the record is regard as nonexistent. Note that the slice is
            // fully cached.
            if (v_rec.payload_status_ == RecordStatus::Unknown ||
                (v_rec.payload_status_ == RecordStatus::Deleted &&
                 !keep_deleted))
            {
                return;
            }

            tuple = typed_cache->AddScanTuple();
            tuple->obj_type_ = object_type;

            if (is_require_keys ||
                v_rec.payload_status_ != RecordStatus::Normal)
            {
                tuple->KeyObj().Copy(*key);
                tuple_size += key->Size();
            }

            if (is_require_recs)
            {
                if (v_rec.payload_status_ == RecordStatus::Normal ||
                    (is_ckpt_delta &&
                     v_rec.payload_status_ == RecordStatus::Deleted))
                {
                    if (v_rec.payload_ptr_ != nullptr)
                    {
                        tuple->SetRecord(v_rec.payload_ptr_);
                        // We're only copying the shared_ptr here so we
                        // exclude the actual payload size.
                    }
                }
            }

            tuple->key_ts_ = v_rec.commit_ts_;
            tuple->rec_status_ = v_rec.payload_status_;
            need_fetch_snapshot =
                (v_rec.payload_status_ != RecordStatus::Normal &&
                 v_rec.payload_status_ != RecordStatus::Deleted);
        }
        else
        {
            NonBlockingLock *lk = cce->GetKeyLock();
            bool check_dirty_status =
                lk != nullptr && lk->HasWriteLock(txn) &&
                cce->DirtyPayloadStatus() != RecordStatus::NonExistent;

            if (check_dirty_status)
            {
                assert(!VersionedRecord);

                tuple = typed_cache->AddScanTuple();
                tuple->obj_type_ = object_type;
                if (is_require_keys)
                {
                    tuple->KeyObj().Copy(*key);
                    tuple_size += key->Size();
                }
                const RecordStatus rec_dirty_status = cce->DirtyPayloadStatus();
                if (rec_dirty_status == RecordStatus::Normal)
                {
                    tuple->rec_status_ = RecordStatus::Normal;
                }
                else if (rec_dirty_status == RecordStatus::Deleted)
                {
                    tuple->rec_status_ = RecordStatus::Deleted;
                }
                else if (rec_dirty_status == RecordStatus::Uncreated)
                {
                    CreateDirtyPayloadFromPendingCommand(cce);

                    assert(cce->DirtyPayloadStatus() !=
                               RecordStatus::NonExistent &&
                           cce->DirtyPayloadStatus() !=
                               RecordStatus::Uncreated);

                    tuple->rec_status_ = cce->DirtyPayloadStatus();
                }
                else
                {
                    assert(false && "Unknown dirty record status");
                }

                tuple->key_ts_ = cce->CommitTs() + 1;
            }
            else
            {
                const RecordStatus rec_status = cce->PayloadStatus();
                if (rec_status == RecordStatus::Normal ||
                    (rec_status == RecordStatus::Deleted && keep_deleted))
                {
                    tuple = typed_cache->AddScanTuple();
                    tuple->obj_type_ = object_type;
                }
                else
                {
                    return;
                }
                if (is_require_keys)
                {
                    tuple->KeyObj().Copy(*key);
                    tuple_size += key->Size();
                }

                if (is_require_recs)
                {
                    // Redis KEYS command doesn't need value. But
                    // ObjectCcMap doesn't override ScanKey() on
                    // local ccmap. Thus, TemplateCcMap::ScanKey() on
                    // local ccmp may be called, and it need not set
                    // record.
                    assert(VersionedRecord);
                    if (rec_status == RecordStatus::Normal ||
                        (is_ckpt_delta && rec_status == RecordStatus::Deleted))
                    {
                        if (cce->payload_.cur_payload_ != nullptr)
                        {
                            // We're only copying the shared_ptr here so we
                            // exclude the actual payload size.
                            tuple->SetRecord(
                                cce->payload_.VersionedCurrentPayload());
                        }
                    }
                }

                tuple->rec_status_ = rec_status;
                tuple->key_ts_ = cce->CommitTs();
            }
        }
        tuple->gap_ts_ = 0;
        tuple->cce_ptr_ = cce;
        tuple->cce_addr_.SetCceLock(
            reinterpret_cast<uint64_t>(cce->GetKeyGapLockAndExtraData()),
            ng_term,
            ng_id,
            shard_->LocalCoreId());

        typed_cache->AddScanTupleSize(tuple_size);
    }

    void ScanKey(const KeyT *key,
                 CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
                 RemoteScanSliceCache *remote_cache,
                 bool include_gap,
                 int64_t ng_term,
                 uint64_t read_ts,
                 bool is_read_snapshot,
                 bool &need_fetch_snapshot,
                 bool keep_deleted,
                 bool is_ckpt_delta = false,
                 bool is_require_keys = true,
                 bool is_require_recs = true)
    {
        // Skip key and/or payload whenever unnecessary by filling an
        // empty key/payload, so the receiver doesn't have to care about whether
        // skipping when deserializing.
        static KeyT empty_key;
        static ValueT empty_val;
        const ValueT *payload = &empty_val;
        need_fetch_snapshot = false;

        uint32_t tuple_size = RemoteScanSliceCache::MetaDataSize;
        if (is_read_snapshot)
        {
            assert(VersionedRecord);
            VersionResultRecord<ValueT> v_rec;
            cce->MvccGet(read_ts, v_rec);

            // For snapshot reads, only if the visible version's record
            // status is deleted and no lock has been put on it, should
            // the record be skipped in the result set. Note that if the
            // visible version is mising in memory, the key still needs
            // to be returned. Runtime will use the key to retrieve the
            // visible version from the data store.
            //
            // AcquireCc might emplace an unknown record and put a wlock on it.
            // If the wlock_ts of the record is larger than the scan read_ts,
            // then the record is regard as nonexistent. Note the slice is fully
            // cached.
            if (v_rec.payload_status_ == RecordStatus::Unknown ||
                (v_rec.payload_status_ == RecordStatus::Deleted &&
                 !keep_deleted))
            {
                return;
            }

            if (!(is_require_keys ||
                  v_rec.payload_status_ != RecordStatus::Normal))
            {
                key = &empty_key;
            }

            key->Serialize(remote_cache->keys_);
            tuple_size += key->SerializedLength();

            if (is_require_recs)
            {
                if (v_rec.payload_status_ == RecordStatus::Normal ||
                    (is_ckpt_delta &&
                     v_rec.payload_status_ == RecordStatus::Deleted))
                {
                    if (v_rec.payload_ptr_ != nullptr)
                    {
                        payload = v_rec.payload_ptr_.get();
                    }
                }
            }

            if (v_rec.payload_status_ != RecordStatus::Normal &&
                v_rec.payload_status_ != RecordStatus::Deleted)
            {
                need_fetch_snapshot = true;

                remote_cache->archive_positions_.emplace_back(
                    remote_cache->key_ts_.size(),
                    remote_cache->records_.size());
                auto &ref = remote_cache->archive_records_.emplace_back();
                payload->Serialize(ref);

                // Estimate the size of archive record based on current payload
                // size or average payload size.
                if (cce->payload_.cur_payload_ != nullptr)
                {
                    tuple_size +=
                        cce->payload_.cur_payload_->SerializedLength();
                }
                else if (remote_cache->records_.size() > 0 &&
                         remote_cache->key_ts_.size() > 0)
                {
                    tuple_size += (remote_cache->records_.size() /
                                   remote_cache->key_ts_.size());
                }
                else
                {
                    tuple_size += payload->SerializedLength();
                }
            }
            else
            {
                payload->Serialize(remote_cache->records_);
                tuple_size += payload->SerializedLength();
            }

            remote_cache->rec_status_.push_back(
                remote::ToRemoteType::ConvertRecordStatus(
                    v_rec.payload_status_));
            remote_cache->key_ts_.push_back(v_rec.commit_ts_);
        }
        else
        {
            const RecordStatus rec_status = cce->PayloadStatus();
            if (!(rec_status == RecordStatus::Normal ||
                  (rec_status == RecordStatus::Deleted && keep_deleted)))
            {
                return;
            }

            if (!(is_require_keys || rec_status != RecordStatus::Normal))
            {
                key = &empty_key;
            }

            key->Serialize(remote_cache->keys_);
            tuple_size += key->SerializedLength();

            if (is_require_recs)
            {
                // Redis KEYS command doesn't need value. But
                // ObjectCcMap doesn't override ScanKey() on local
                // ccmap. Thus, TemplateCcMap::ScanKey() on local ccmp
                // may be called, and it need not set record.
                assert(VersionedRecord);
                if (rec_status == RecordStatus::Normal ||
                    (is_ckpt_delta && rec_status == RecordStatus::Deleted))
                {
                    if (cce->payload_.cur_payload_ != nullptr)
                    {
                        payload = cce->payload_.cur_payload_.get();
                    }
                }
            }

            payload->Serialize(remote_cache->records_);
            tuple_size += payload->SerializedLength();

            remote_cache->rec_status_.push_back(
                remote::ToRemoteType::ConvertRecordStatus(rec_status));
            remote_cache->key_ts_.push_back(cce->CommitTs());
        }

        if (include_gap)
        {
            // Gap timestamps are not used at the moment.
            remote_cache->gap_ts_.push_back(0);
        }
        else
        {
            remote_cache->gap_ts_.push_back(0);
        }

        remote_cache->cce_ptr_.push_back(reinterpret_cast<uint64_t>(cce));
        remote_cache->cce_lock_ptr_.push_back(
            reinterpret_cast<uint64_t>(cce->GetLockAddr()));
        remote_cache->term_.push_back(ng_term);
        // For remote scans, the returned cc entries' node group ID is
        // set on the sender side when the sender receives the response.

        remote_cache->cache_mem_size_ += tuple_size;
    }

    void ScanKey(const KeyT *key,
                 CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
                 RemoteScanCache *remote_cache,
                 bool include_gap,
                 NodeGroupId ng_id,
                 int64_t ng_term,
                 TxNumber txn,
                 uint64_t read_ts,
                 int32_t object_type,
                 bool is_read_snapshot,
                 bool &need_fetch_snapshot,
                 bool keep_deleted,
                 bool is_ckpt_delta = false,
                 bool is_require_keys = true,
                 bool is_require_recs = true)
    {
        static ValueT empty_val;
        const ValueT *payload = &empty_val;
        need_fetch_snapshot = false;
        remote::ScanTuple_msg *tuple = nullptr;
        uint32_t tuple_size = 0;

        if (is_read_snapshot)
        {
            assert(VersionedRecord);
            VersionResultRecord<ValueT> v_rec;
            cce->MvccGet(read_ts, v_rec);

            // For snapshot reads, only if the visible version's record
            // status is deleted and no lock has been put on it, should
            // the record be skipped in the result set. Note that if the
            // visible version is mising in memory, the key still needs
            // to be returned. Runtime will use the key to retrieve the
            // visible version from the data store.
            //
            // AcquireCc might emplace an unknown record and put a wlock on it.
            // If the wlock_ts of the record is larger than the scan read_ts,
            // then the record is regard as nonexistent. Note the slice is fully
            // cached.
            if (v_rec.payload_status_ == RecordStatus::Unknown ||
                (v_rec.payload_status_ == RecordStatus::Deleted &&
                 !keep_deleted))
            {
                return;
            }

            tuple = remote_cache->GetMemoryCache()->add_scan_tuple();
            remote_cache->AddHashCode(key->Hash());
            tuple->set_obj_type(object_type);

            if (is_require_keys)
            {
                tuple->mutable_key()->assign(key->Data(), key->Size());
                tuple_size += key->Size();
            }

            if (is_require_recs)
            {
                if (v_rec.payload_status_ == RecordStatus::Normal ||
                    (is_ckpt_delta &&
                     v_rec.payload_status_ == RecordStatus::Deleted))
                {
                    if (v_rec.payload_ptr_ != nullptr)
                    {
                        payload = v_rec.payload_ptr_.get();
                    }
                }
            }
            tuple->clear_record();
            payload->Serialize(*tuple->mutable_record());
            tuple_size += payload->Size();

            tuple->set_rec_status(remote::ToRemoteType::ConvertRecordStatus(
                v_rec.payload_status_));
            tuple->set_key_ts(v_rec.commit_ts_);

            need_fetch_snapshot =
                (v_rec.payload_status_ != RecordStatus::Normal &&
                 v_rec.payload_status_ != RecordStatus::Deleted);
        }
        else
        {
            NonBlockingLock *lk = cce->GetKeyLock();
            bool check_dirty_status =
                lk != nullptr && lk->HasWriteLock(txn) &&
                cce->DirtyPayloadStatus() != RecordStatus::NonExistent;

            if (check_dirty_status)
            {
                assert(!VersionedRecord);

                tuple = remote_cache->GetMemoryCache()->add_scan_tuple();
                remote_cache->AddHashCode(key->Hash());
                tuple->set_obj_type(object_type);

                tuple->mutable_key()->assign(key->Data(), key->Size());
                tuple_size += key->Size();

                const RecordStatus rec_dirty_status = cce->DirtyPayloadStatus();
                if (rec_dirty_status == RecordStatus::Normal)
                {
                    tuple->set_rec_status(
                        remote::ToRemoteType::ConvertRecordStatus(
                            RecordStatus::Normal));
                }
                else if (rec_dirty_status == RecordStatus::Deleted)
                {
                    tuple->set_rec_status(
                        remote::ToRemoteType::ConvertRecordStatus(
                            RecordStatus::Deleted));
                }
                else if (rec_dirty_status == RecordStatus::Uncreated)
                {
                    CreateDirtyPayloadFromPendingCommand(cce);

                    assert(cce->DirtyPayloadStatus() !=
                               RecordStatus::NonExistent &&
                           cce->DirtyPayloadStatus() !=
                               RecordStatus::Uncreated);

                    tuple->set_rec_status(
                        remote::ToRemoteType::ConvertRecordStatus(
                            cce->DirtyPayloadStatus()));
                }
                else
                {
                    assert(false && "Unknown dirty record status");
                }

                tuple->set_key_ts(cce->CommitTs() + 1);
            }
            else
            {
                const RecordStatus rec_status = cce->PayloadStatus();
                if (rec_status == RecordStatus::Normal ||
                    (rec_status == RecordStatus::Deleted && keep_deleted))
                {
                    tuple = remote_cache->GetMemoryCache()->add_scan_tuple();
                    remote_cache->AddHashCode(key->Hash());
                    tuple->set_obj_type(object_type);
                }
                else
                {
                    return;
                }
                if (is_require_keys)
                {
                    tuple->mutable_key()->assign(key->Data(), key->Size());
                    tuple_size += key->Size();
                }

                if (is_require_recs)
                {
                    // Redis KEYS command doesn't need value. But
                    // ObjectCcMap doesn't override ScanKey() on local
                    // ccmap. Thus, TemplateCcMap::ScanKey() on local ccmp
                    // may be called, and it need not set record.
                    assert(VersionedRecord);
                    if (rec_status == RecordStatus::Normal ||
                        (is_ckpt_delta && rec_status == RecordStatus::Deleted))
                    {
                        if (cce->payload_.cur_payload_ != nullptr)
                        {
                            payload = cce->payload_.cur_payload_.get();
                        }
                    }
                }

                tuple->clear_record();
                payload->Serialize(*tuple->mutable_record());
                tuple_size += payload->Size();

                tuple->set_rec_status(
                    remote::ToRemoteType::ConvertRecordStatus(rec_status));
                tuple->set_key_ts(cce->CommitTs());
            }
        }

        if (include_gap)
        {
            // Gap timestamps are not used at the moment.
            tuple->set_gap_ts(0);
        }
        else
        {
            tuple->set_gap_ts(0);
        }

        remote::CceAddr_msg *cce_addr = tuple->mutable_cce_addr();
        cce_addr->set_node_group_id(ng_id);
        cce_addr->set_cce_lock_ptr(
            reinterpret_cast<uint64_t>(cce->GetLockAddr()));
        cce_addr->set_term(ng_term);
        cce_addr->set_core_id(shard_->LocalCoreId());
        // For remote scans, the returned cc entries' node group ID is
        // set on the sender side when the sender receives the response.
    }

    void ScanGap(const KeyT *key,
                 CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
                 TemplateScanTuple<KeyT, ValueT> *tuple,
                 uint32_t ng_id,
                 int64_t ng_term,
                 int32_t object_type) const
    {
        tuple->key_ts_ = 0;
        tuple->gap_ts_ = 0;
        tuple->cce_ptr_ = cce;
        tuple->obj_type_ = object_type;
        tuple->cce_addr_.SetCceLock(
            reinterpret_cast<uint64_t>(cce->GetKeyGapLockAndExtraData()),
            ng_term,
            ng_id,
            shard_->LocalCoreId());
    }

    void ScanGap(const KeyT *key,
                 CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
                 remote::ScanTuple_msg *tuple,
                 int64_t ng_term,
                 int32_t object_type) const
    {
        tuple->set_key_ts(0);
        tuple->set_gap_ts(0);
        tuple->set_obj_type(object_type);

        remote::CceAddr_msg *cce_addr = tuple->mutable_cce_addr();
        cce_addr->set_cce_lock_ptr(
            reinterpret_cast<uint64_t>(cce->GetLockAddr()));
        cce_addr->set_term(ng_term);
        cce_addr->set_core_id(shard_->LocalCoreId());

        // For remote scans, the returned cc entries' node group ID is
        // set on the sender side when the sender receives the response.
    }

    void ScanGap(const KeyT *key,
                 CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
                 RemoteScanSliceCache *cache,
                 int64_t ng_term) const
    {
        cache->key_ts_.push_back(0);
        cache->gap_ts_.push_back(0);

        cache->cce_ptr_.push_back(reinterpret_cast<uint64_t>(cce));
        cache->cce_lock_ptr_.push_back(
            reinterpret_cast<uint64_t>(cce->GetLockAddr()));
        cache->term_.push_back(ng_term);

        // For remote scans, the returned cc entries' node group ID is
        // set on the sender side when the sender receives the response.
    }

    /**
     * @brief If key is in range of [start key, end key), left inclusive
     * right open.
     *
     * @param key
     * @param start_key
     * @param end_key
     * @return true
     * @return false
     */
    bool KeyInRange(const KeyT *key, const KeyT *start_key, const KeyT *end_key)
    {
        if (start_key == nullptr || *start_key < *key || *start_key == *key)
        {
            if (end_key)
            {
                return *key < *end_key;
            }
            else
            {
                // end key is pos inf
                return true;
            }
        }

        return false;
    }

    /**
     * If the a record is according to the conditions, return true, or
     * return false to neglect this record.
     */
    virtual bool FilterRecord(
        const KeyT *key,
        const CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
        int32_t obj_type,
        const std::string_view &scan_pattern)
    {
        return true;
    }

    virtual int32_t GetObjectType(
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce)
    {
        return -1;
    }

    virtual void CreateDirtyPayloadFromPendingCommand(
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce)
    {
        assert(false);
    }

    void TryUpdatePageKey(BtreeMapIterator &page_it)
    {
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> &page =
            *page_it->second;
        if (page_it->first != page.FirstKey())
        {
            auto node_handle = ccmp_.extract(page_it);
            KeyT new_key(page.FirstKey());
            node_handle.key() = std::move(new_key);
            auto insert_res = ccmp_.insert(std::move(node_handle));
            assert(insert_res.inserted);
            assert(insert_res.position->first == page.FirstKey());
            page_it = insert_res.position;
        }
    }

    /*
     * Clean page and return the last_read_ts of page.
     *
     * @param page
     * @param page_it the position of page in ccmp_. If page's first key is
     * deleted, page_it needs to be set to update the map.
     * @param free_cnt
     * @return The bool value stand for the clean status, if return
     * false, it mean that the target ccentry can not be clean, the
     * caller should retry the kickout request. Currently, only when
     * clean_type is CleanForSplitRange and CleanForAlterTable care this
     * status.
     */
    bool CleanPage(
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page,
        BtreeMapIterator &page_it,
        size_t &free_cnt,
        KickoutCcEntryCc *kickout_cc = nullptr)
    {
        bool success;

        CcPageCleanGuard<KeyT, ValueT, VersionedRecord, RangePartitioned>
            *clean_guard;  // Allocate it on stack.
        constexpr size_t buffer_size = std::max(
            sizeof(CcPageCleanGuardWithKickoutCc<KeyT,
                                                 ValueT,
                                                 VersionedRecord,
                                                 RangePartitioned>),
            sizeof(CcPageCleanGuardWithoutKickoutCc<KeyT,
                                                    ValueT,
                                                    VersionedRecord,
                                                    RangePartitioned>));
        char buffer[buffer_size] = {};
        if (kickout_cc &&
            kickout_cc->GetCleanType() != CleanType::CleanDataForTest)
        {
            clean_guard = new (buffer) CcPageCleanGuardWithKickoutCc(
                shard_, cc_ng_id_, table_name_, page, kickout_cc);
        }
        else
        {
            clean_guard = new (buffer) CcPageCleanGuardWithoutKickoutCc(
                shard_, cc_ng_id_, table_name_, page);
        }

        shard_->local_shards_.KickoutPage(clean_guard);

        if (clean_guard->ToCleanPageHeadKey())
        {
            // The page's first key will be cleaned, needs to locate the page
            // position to update the map.
            const KeyT &key = page->keys_[0];
            page_it = ccmp_.find(key);
        }
        if (clean_guard->FreedCount() > 0)
        {
            clean_guard->Compact();
            free_cnt += clean_guard->FreedCount();
        }
        if (!VersionedRecord)
        {
            normal_obj_sz_ -= clean_guard->CleanObjectCount();
        }
        size_ -= clean_guard->FreedCount();
        if (clean_guard->EvictedValidKeys())
        {
            ccm_has_full_entries_ = false;
        }

        success = clean_guard->CleanSuccess();

        std::destroy_at(buffer);
        return success;
    }

    /**
     * Redistribute entries between page1 and page2. This happens when
     * one page is cleaned and its size is below merge threshold and it
     * needs to borrow entries from its siblings to keep the tree
     * balanced.
     *
     * @param page1_it
     * @param page2_it
     * @param page1_last_read_ts
     * @param page2_last_read_ts
     * @return
     */
    void RedistributeBetweenPages(BtreeMapIterator &page1_it,
                                  BtreeMapIterator &page2_it)
    {
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> &page1 =
            *page1_it->second;
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> &page2 =
            *page2_it->second;
        assert(page1.next_page_ == &page2 && &page1 == page2.prev_page_);
        assert(page1_it->first == page1.FirstKey());
        assert(page2_it->first == page2.FirstKey());

        if (page1.Size() > page2.Size())
        {
            // move keys and entries from page1's tail to page2's head
            size_t move_pos = (page1.Size() + page2.Size()) / 2;
            page2.keys_.insert(
                page2.keys_.begin(),
                std::make_move_iterator(page1.keys_.begin() + move_pos),
                std::make_move_iterator(page1.keys_.end()));
            page1.keys_.erase(page1.keys_.begin() + move_pos,
                              page1.keys_.end());

            // Updates the parent page of the locks of the to-be-moved
            // entries.
            for (auto entry_it = page1.entries_.begin() + move_pos;
                 entry_it != page1.entries_.end();
                 ++entry_it)
            {
                (*entry_it)->UpdateCcPage(&page2);
            }

            page2.entries_.insert(
                page2.entries_.begin(),
                std::make_move_iterator(page1.entries_.begin() + move_pos),
                std::make_move_iterator(page1.entries_.end()));
            page1.entries_.erase(page1.entries_.begin() + move_pos,
                                 page1.entries_.end());
            page2.last_dirty_commit_ts_ = std::max(page1.last_dirty_commit_ts_,
                                                   page2.last_dirty_commit_ts_);
        }
        else
        {
            // move keys and entries from page2's head to page1's tail
            size_t move_idx = page2.Size() - (page1.Size() + page2.Size()) / 2;
            page1.keys_.insert(
                page1.keys_.end(),
                std::make_move_iterator(page2.keys_.begin()),
                std::make_move_iterator(page2.keys_.begin() + move_idx));
            page2.keys_.erase(page2.keys_.begin(),
                              page2.keys_.begin() + move_idx);

            // Updates the parent page of the locks of the to-be-moved
            // entries.
            for (auto entry_it = page2.entries_.begin();
                 entry_it != page2.entries_.begin() + move_idx;
                 ++entry_it)
            {
                (*entry_it)->UpdateCcPage(&page1);
            }

            page1.entries_.insert(
                page1.entries_.end(),
                std::make_move_iterator(page2.entries_.begin()),
                std::make_move_iterator(page2.entries_.begin() + move_idx));
            page2.entries_.erase(page2.entries_.begin(),
                                 page2.entries_.begin() + move_idx);
            page1.last_dirty_commit_ts_ = std::max(page1.last_dirty_commit_ts_,
                                                   page2.last_dirty_commit_ts_);
        }

        // The locks of cc entries of a page should point to the same
        // page.
        assert(
            [&]()
            {
                for (const auto &cce : page1.entries_)
                {
                    if (cce->GetCcPage() != nullptr &&
                        cce->GetCcPage() != &page1)
                    {
                        return false;
                    }
                }

                return true;
            }());

        assert(
            [&]()
            {
                for (const auto &cce : page2.entries_)
                {
                    if (cce->GetCcPage() != nullptr &&
                        cce->GetCcPage() != &page2)
                    {
                        return false;
                    }
                }
                return true;
            }());

        // update page key in the map
        assert(page1_it->first == page1.FirstKey());
        assert(page2_it->first != page2.FirstKey());
        TryUpdatePageKey(page2_it);
        assert(page2_it->second.get() == &page2);
        page1_it = page2_it;
        --page1_it;
        assert(page1_it->second.get() == &page1);

        // update LRU list
        // after redistribution, the two pages should be seen as one in
        // the LRU list, insert the less recently used page after the
        // more recently used one
        LruPage *less_recently_used = nullptr, *more_recently_used = nullptr;
        if (page1.last_access_ts_ > page2.last_access_ts_)
        {
            more_recently_used = &page1;
            less_recently_used = &page2;
        }
        else
        {
            more_recently_used = &page2;
            less_recently_used = &page1;
        }

        if (less_recently_used->lru_next_ != nullptr)
        {
            shard_->DetachLru(less_recently_used);
        }
        LruPage *next = more_recently_used->lru_next_;
        less_recently_used->lru_next_ = next;
        next->lru_prev_ = less_recently_used;
        less_recently_used->lru_prev_ = more_recently_used;
        more_recently_used->lru_next_ = less_recently_used;
        less_recently_used->last_access_ts_ =
            more_recently_used->last_access_ts_;
    }

    /**
     * Merge page1 and page2. Update the map and lru list after the
     * merge.
     *
     * @param page1_it The iterator to the merged page
     * @param page2_it The iterator to the discarded page
     */
    void MergePages(BtreeMapIterator &page1_it, BtreeMapIterator &page2_it)
    {
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page1 =
            page1_it->second.get();
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page2 =
            page2_it->second.get();

        auto merged_page_it = page1_it;
        auto discarded_page_it = page2_it;
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *merged_page =
            merged_page_it->second.get();
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>
            *discarded_page = discarded_page_it->second.get();

        // merge the key vector and entry vector
        std::vector<KeyT> merged_keys = std::move(page1->keys_);
        merged_keys.insert(merged_keys.end(),
                           std::make_move_iterator(page2->keys_.begin()),
                           std::make_move_iterator(page2->keys_.end()));
        std::vector<std::unique_ptr<
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>>
            merged_entries = std::move(page1->entries_);

        for (auto it = page2->entries_.begin(); it != page2->entries_.end();
             ++it)
        {
            (*it)->UpdateCcPage(merged_page);
        }

        merged_entries.insert(merged_entries.end(),
                              std::make_move_iterator(page2->entries_.begin()),
                              std::make_move_iterator(page2->entries_.end()));
        merged_page->keys_ = std::move(merged_keys);
        merged_page->entries_ = std::move(merged_entries);

        assert(
            [&]()
            {
                for (const auto &cce : merged_page->entries_)
                {
                    if (cce->GetCcPage() != nullptr &&
                        cce->GetCcPage() != merged_page)
                    {
                        return false;
                    }
                }
                return true;
            }());

        // Update the page order list.
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *map_prev =
            page1->prev_page_;
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *map_next =
            page2->next_page_;
        merged_page->prev_page_ = map_prev;
        merged_page->next_page_ = map_next;
        map_prev->next_page_ = merged_page;
        map_next->prev_page_ = merged_page;

        // Update the LRU list.
        // the merged page should take the more recently used page's
        // position in the LRU list
        if (page1->lru_next_ == page2 || page1->lru_prev_ == page2)
        {
            // corner case: the two pages are adjacent in LRU list, just
            // detach the discarded page
            if (discarded_page->lru_next_ != nullptr)
            {
                shard_->DetachLru(discarded_page);
            }
        }
        else
        {
            LruPage *lru_prev = nullptr, *lru_next = nullptr;
            uint64_t merge_last_access_ts;
            if (page1->last_access_ts_ > page2->last_access_ts_)
            {
                lru_prev = page1->lru_prev_;
                lru_next = page1->lru_next_;
                merge_last_access_ts = page1->last_access_ts_;
            }
            else
            {
                lru_prev = page2->lru_prev_;
                lru_next = page2->lru_next_;
                merge_last_access_ts = page2->last_access_ts_;
            }

            if (merged_page->lru_next_ != nullptr)
            {
                shard_->DetachLru(merged_page);
            }
            if (discarded_page->lru_next_ != nullptr)
            {
                shard_->DetachLru(discarded_page);
            }
            // reinsert the merged page into lru list
            merged_page->lru_prev_ = lru_prev;
            merged_page->lru_next_ = lru_next;
            lru_prev->lru_next_ = merged_page;
            lru_next->lru_prev_ = merged_page;
            merged_page->last_access_ts_ = merge_last_access_ts;
        }

        // last_dirty_commit_ts_ of merged page will inherit the larger
        // one.
        merged_page->last_dirty_commit_ts_ =
            std::max(merged_page->last_dirty_commit_ts_,
                     discarded_page->last_dirty_commit_ts_);

        merged_page->smallest_ttl_ =
            std::min(merged_page->smallest_ttl_, discarded_page->smallest_ttl_);

        // remove discarded page from the map
        // note that all iterators are invalid after the erasion
        ccmp_.erase(discarded_page_it);
    }

    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *PageNegInf()
    {
        return &neg_inf_page_;
    }

    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *PagePosInf()
    {
        return &pos_inf_page_;
    }

    absl::btree_map<
        KeyT,
        std::unique_ptr<
            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>>>
        ccmp_;
    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> neg_inf_page_,
        pos_inf_page_;
    CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> neg_inf_, pos_inf_;

    TemplateCcMapSamplePool<KeyT> *sample_pool_;
    size_t
        size_{};  // The count of all records, including ones in deleted status.
    size_t normal_obj_sz_{
        0};  // The count of all normal status objects, only used for redis
};

template <typename KeyT, typename ValueT>
void BackfillSnapshotForScanSlice(FetchSnapshotCc *fetch_cc,
                                  CcRequestBase *requester)
{
    ScanSliceCc *req = static_cast<ScanSliceCc *>(requester);
    CcShard &shard = *fetch_cc->ccs_;
    auto core_id = shard.LocalCoreId();
    size_t tuple_idx = fetch_cc->tuple_idx_;
    // backfill fetched snapshot
    if (req->IsLocal())
    {
        TemplateScanCache<KeyT, ValueT> *scan_cache =
            static_cast<TemplateScanCache<KeyT, ValueT> *>(
                req->GetLocalScanCache(core_id));
        assert(scan_cache != nullptr);
        auto *scan_tuple = const_cast<TemplateScanTuple<KeyT, ValueT> *>(
            scan_cache->At(tuple_idx));
        assert(scan_tuple->rec_status_ != RecordStatus::Normal &&
               scan_tuple->rec_status_ != RecordStatus::Deleted);

        if (fetch_cc->rec_status_ == RecordStatus::Normal)
        {
            auto payload = std::make_shared<ValueT>();
            size_t offset = 0;
            payload->Deserialize(fetch_cc->rec_str_.data(), offset);
            scan_tuple->SetRecord(payload);
        }
        scan_tuple->key_ts_ = fetch_cc->rec_ts_;
        scan_tuple->rec_status_ = fetch_cc->rec_status_;
    }
    else
    {
        RemoteScanSliceCache *remote_scan_cache =
            req->GetRemoteScanCache(core_id);
        assert(remote_scan_cache != nullptr);
        assert(remote_scan_cache->archive_records_.size() >= tuple_idx);
        auto &tmp_pair = remote_scan_cache->archive_positions_[tuple_idx];
        remote_scan_cache->key_ts_[tmp_pair.first] = fetch_cc->rec_ts_;
        remote_scan_cache->rec_status_[tmp_pair.first] =
            remote::ToRemoteType::ConvertRecordStatus(fetch_cc->rec_status_);
        if (fetch_cc->rec_str_.size() > 0)
        {
            remote_scan_cache->archive_records_[tuple_idx] =
                std::move(fetch_cc->rec_str_);
        }
        assert(remote_scan_cache->archive_records_[tuple_idx].size() > 0);
    }

    // trigger request
    req->DecreaseWaitForSnapshotCnt(core_id);
    if (req->IsWaitForSnapshot(core_id) &&
        req->WaitForSnapshotCnt(core_id) == 0)
    {
        shard.Enqueue(core_id, req);
    }
}

template <typename KeyT, typename ValueT, bool VersionedRecord>
void BackfillForScanNextBatch(FetchBucketDataCc *fetch_cc,
                              CcRequestBase *requester)
{
    uint16_t bucket_id = fetch_cc->bucket_id_;
    CcShard &shard = *fetch_cc->ccs_;
    bool fetch_again = false;
    if (fetch_cc->is_local_)
    {
        ScanNextBatchCc *req = static_cast<ScanNextBatchCc *>(requester);
        int32_t obj_type = req->GetRedisObjectType();
        const auto &pattern = req->GetRedisScanPattern();
        uint32_t shard_code = (req->NodeGroupId() << 10) + shard.core_id_;
        TemplateScanCache<KeyT, ValueT> *scan_cache =
            static_cast<TemplateScanCache<KeyT, ValueT> *>(req->GetLocalKvCache(
                shard_code, bucket_id, fetch_cc->batch_size_));
        assert(scan_cache != nullptr);

        // Reset kv cache
        scan_cache->Reset();
        assert(scan_cache->Size() == 0);

        for (auto &item : fetch_cc->bucket_data_items_)
        {
            if constexpr (!VersionedRecord)
            {
                if (!KeyT::IsMatch(
                        item.key_str_.data(), item.key_str_.size(), pattern))
                {
                    continue;
                }

                if (obj_type >= 0)
                {
                    int8_t store_rec_obj_type =
                        static_cast<int8_t>(*item.rec_str_.data());
                    if (static_cast<int32_t>(store_rec_obj_type) != obj_type)
                    {
                        continue;
                    }
                }
            }

            TemplateScanTuple<KeyT, ValueT> *scan_tuple =
                scan_cache->AddScanTuple();
            scan_tuple->key_ts_ = item.version_ts_;
            scan_tuple->cce_addr_.SetCceLock(0, -1, 0, 0);
            scan_tuple->KeyObj().SetPackedKey(item.key_str_.data(),
                                              item.key_str_.size());
            scan_tuple->obj_type_ = static_cast<int8_t>(*item.rec_str_.data());

            if (!item.is_deleted_)
            {
                scan_tuple->rec_status_ = RecordStatus::Normal;
                if (req->IsRequireRecs())
                {
                    size_t rec_offset = 0;
                    if constexpr (VersionedRecord)
                    {
                        ValueT val;
                        val.Deserialize(item.rec_str_.data(), rec_offset);
                    }
                    else
                    {
                        ValueT tx_obj;
                        auto val = tx_obj.DeserializeObject(
                            item.rec_str_.data(), rec_offset);
                        scan_tuple->SetRecord(std::unique_ptr<ValueT>(
                            static_cast<ValueT *>(val.release())));
                    }
                }
            }
            else
            {
                scan_tuple->rec_status_ = RecordStatus::Deleted;
            }
        }

        if (fetch_cc->is_drained_)
        {
            req->GetBucketScanProgress()
                ->at(shard.core_id_)
                .scan_buckets_[bucket_id] = true;
        }

        fetch_again = (scan_cache->Size() == 0 && !fetch_cc->is_drained_);
        if (!fetch_again)
        {
            req->DecreaseWaitForFetchBucketCnt(shard.core_id_);
            if (req->IsWaitForFetchBucket(shard.core_id_) &&
                req->WaitForFetchBucketCnt(shard.core_id_) == 0)
            {
                shard.Enqueue(requester);
            }

            return;
        }
    }
    else
    {
        remote::RemoteScanNextBatch *req =
            static_cast<remote::RemoteScanNextBatch *>(requester);
        ::txservice::remote::ScanCache_msg *remote_kv_cache =
            req->GetRemoteScanCache(shard.core_id_)->GetKvCache(bucket_id);
        int32_t obj_type = req->GetRedisObjectType();
        const auto &pattern = req->GetRedisScanPattern();

        for (auto &item : fetch_cc->bucket_data_items_)
        {
            if constexpr (!VersionedRecord)
            {
                if (!KeyT::IsMatch(
                        item.key_str_.data(), item.key_str_.size(), pattern))
                {
                    continue;
                }

                if (obj_type >= 0)
                {
                    int8_t store_rec_obj_type =
                        static_cast<int8_t>(*item.rec_str_.data());
                    if (static_cast<int32_t>(store_rec_obj_type) != obj_type)
                    {
                        continue;
                    }
                }
            }

            ::txservice::remote::ScanTuple_msg *scan_tuple =
                remote_kv_cache->add_scan_tuple();
            *scan_tuple->mutable_key() = std::move(item.key_str_);
            scan_tuple->set_key_ts(item.version_ts_);
            ::txservice::remote::CceAddr_msg *cce_addr =
                scan_tuple->mutable_cce_addr();
            cce_addr->set_cce_lock_ptr(0);
            cce_addr->set_term(-1);
            cce_addr->set_core_id(0);
            cce_addr->set_node_group_id(0);

            int8_t object_type = static_cast<int8_t>(*item.rec_str_.data());

            scan_tuple->set_obj_type(object_type);

            if (!item.is_deleted_)
            {
                scan_tuple->set_rec_status(
                    remote::ToRemoteType::ConvertRecordStatus(
                        RecordStatus::Normal));

                if (req->IsRequireRecs())
                {
                    *scan_tuple->mutable_record() = std::move(item.rec_str_);
                }
            }
            else
            {
                scan_tuple->set_rec_status(
                    remote::ToRemoteType::ConvertRecordStatus(
                        RecordStatus::Deleted));
            }
        }

        if (fetch_cc->is_drained_)
        {
            req->SetKvBucketIsDrain(shard.core_id_, bucket_id);
        }
        fetch_again =
            (remote_kv_cache->scan_tuple_size() == 0 && !fetch_cc->is_drained_);
        if (!fetch_again)
        {
            req->DecreaseWaitForFetchBucketCnt(shard.core_id_);
            if (req->IsWaitForFetchBucket(shard.core_id_) &&
                req->WaitForFetchBucketCnt(shard.core_id_) == 0)
            {
                shard.Enqueue(requester);
            }

            return;
        }
    }

    if (fetch_again)
    {
        assert(!fetch_cc->is_drained_);
        assert(!fetch_cc->bucket_data_items_.empty());
        fetch_cc->kv_start_key_.clear();
        fetch_cc->kv_end_key_.clear();

        fetch_cc->start_key_ =
            std::move(fetch_cc->bucket_data_items_.back().key_str_);
        assert(std::holds_alternative<std::string>(fetch_cc->start_key_));
        fetch_cc->start_key_type_ = KeyType::Normal;
        fetch_cc->start_key_inclusive_ = false;
        // Fetch again
        fetch_cc->err_code_ = 0;
        fetch_cc->bucket_data_items_.clear();

        shard.FetchBucketData(fetch_cc);
        return;
    }
}

}  // namespace txservice
