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

#include <memory>
#include <string>
#include <utility>

#include "cc_entry.h"
#include "cc_handler_result.h"
#include "cc_protocol.h"
#include "cc_request.h"
#include "error_messages.h"  //CcErrorCode
#include "range_bucket_cc_map.h"
#include "range_bucket_key_record.h"
#include "range_record.h"
#include "range_slice.h"
#include "statistics.h"
#include "template_cc_map.h"
#include "tx_operation.h"
#include "tx_serialize.h"
#include "type.h"

namespace txservice
{
/**
 * @brief A range cc map is a special cc map that maps a table's sorted ranges
 * into their partition IDs. Each range is represented by the start key of the
 * range. When the table is newly created, the table has only one range, from
 * negative infinity to positive infinity, i.e., [neg_inf, pos_inf). The range
 * starting from negative infinity has a reserved partition ID: 0. Range
 * [pos_inf, pos_inf) is a special (non-existent) range, whose partition ID is
 * UINT32_MAX. So, the first range's partition ID is 0 and its next range's
 * partition ID is UINT32_MAX. When [neg_inf, pos_inf) is (evenly) split into
 * two sub-ranges [neg_inf, mid_key), [mid_key, pos_inf), the first sub-range
 * inherits the original range's partition ID, i.e., the partition ID of
 * [neg_inf, mid_key) is 0. The second sub-range's partition ID is designated as
 * the half of the original range's partition ID and that of the next range,
 * i.e., the partition ID of [mid_key, pos_inf) is (0 + UINT32_MAX) / 2.
 *
 * @tparam KeyT The type of the table's primary key.
 */
template <typename KeyT>
class RangeCcMap : public TemplateCcMap<KeyT, RangeRecord, true, false>
{
public:
    RangeCcMap(const RangeCcMap &rhs) = delete;

    using TemplateCcMap<KeyT, RangeRecord, true, false>::Execute;
    using TemplateCcMap<KeyT, RangeRecord, true, false>::FindEmplace;
    using TemplateCcMap<KeyT, RangeRecord, true, false>::Emplace;
    using CcMap::AcquireCceKeyLock;
    using CcMap::ReleaseCceLock;
    using TemplateCcMap<KeyT, RangeRecord, true, false>::
        LockHandleForResumedRequest;
    using TemplateCcMap<KeyT, RangeRecord, true, false>::MoveRequest;
    using TemplateCcMap<KeyT, RangeRecord, true, false>::shard_;
    using TemplateCcMap<KeyT, RangeRecord, true, false>::Floor;
    using TemplateCcMap<KeyT, RangeRecord, true, false>::neg_inf_;
    using TemplateCcMap<KeyT, RangeRecord, true, false>::neg_inf_page_;
    using TemplateCcMap<KeyT, RangeRecord, true, false>::pos_inf_;
    using TemplateCcMap<KeyT, RangeRecord, true, false>::table_schema_;
    using TemplateCcMap<KeyT, RangeRecord, true, false>::KeySchema;
    using TemplateCcMap<KeyT, RangeRecord, true, false>::Find;
    using TemplateCcMap<KeyT, RangeRecord, true, false>::Begin;
    using TemplateCcMap<KeyT, RangeRecord, true, false>::End;

    /**
     * @brief Construct a new range cc map object. The range cc map has no
     * schema, so the schema's timestamp is set to 1 (the beginning of history).
     *
     * @param range_table_name
     * @param shard
     */
    RangeCcMap(const TableName &range_table_name,
               const txservice::TableSchema *table_schema,
               uint64_t schema_ts,
               CcShard *shard,
               NodeGroupId ng_id)
        : TemplateCcMap<KeyT, RangeRecord, true, false>(
              shard, ng_id, range_table_name, schema_ts, table_schema, false)
    {
        auto ranges =
            CcMap::shard_->GetTableRangesForATable(range_table_name, ng_id);
        assert(ranges != nullptr);
        neg_inf_.payload_.cur_payload_ = std::make_shared<RangeRecord>();
        pos_inf_.payload_.cur_payload_ = std::make_shared<RangeRecord>();
        bucket_ccm_ = static_cast<RangeBucketCcMap *>(
            shard->GetCcm(range_bucket_ccm_name, ng_id));

        for (auto &[key, table_range] : *ranges)
        {
            const RangeInfo *range_info = table_range->GetRangeInfo();
            const KeyT *start_key = key.template GetKey<KeyT>();
            if (start_key->Type() == KeyType::NegativeInf)
            {
                neg_inf_.payload_.cur_payload_->range_info_ = range_info;
                neg_inf_.payload_.cur_payload_->range_owner_rec_ =
                    bucket_ccm_->GetBucketRecord(Sharder::MapRangeIdToBucketId(
                        range_info->PartitionId()));
                neg_inf_.SetCommitTsPayloadStatus(range_info->version_ts_,
                                                  RecordStatus::Normal);
            }
            else
            {
                auto it =
                    TemplateCcMap<KeyT, RangeRecord, true, false>::FindEmplace(
                        *start_key);
                CcEntry<KeyT, RangeRecord, true, false> *cce = it->second;
                cce->payload_.cur_payload_ = std::make_shared<RangeRecord>();
                cce->payload_.cur_payload_->range_info_ = range_info;
                cce->payload_.cur_payload_->range_owner_rec_ =
                    bucket_ccm_->GetBucketRecord(Sharder::MapRangeIdToBucketId(
                        range_info->PartitionId()));
                cce->SetCommitTsPayloadStatus(range_info->version_ts_,
                                              RecordStatus::Normal);
            }
        }
    }

    ~RangeCcMap()
    {
        // Clean up bucket record locks if range record has read lock on it.
        // This happens with drop table, in which case we remove all read entry
        // related with the dropped table from readset and directly drop the
        // range cc map. In this case we need to manually clear the bucket locks
        // since postread will not be called.
        for (auto it = Begin(); it != End(); it++)
        {
            auto range_cce = it->second;
            NonBlockingLock *lock = range_cce->GetKeyLock();
            if (lock != nullptr && !lock->ReadLocks().empty())
            {
                for (TxNumber txn : lock->ReadLocks())
                {
                    auto bucket_cce = static_cast<
                        CcEntry<RangeBucketKey, RangeBucketRecord, true, false>
                            *>(
                        range_cce->payload_.cur_payload_->range_owner_rec_);
                    ReleaseCceLock(bucket_cce->GetKeyLock(),
                                   bucket_cce,
                                   txn,
                                   this->cc_ng_id_,
                                   LockType::ReadLock);
                }
            }
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
        req.is_include_floor_cce_ = true;
        return TemplateCcMap<KeyT, RangeRecord, true, false>::Execute(req);
    }

    /**
     * @brief A read request toward the range cc map searches a range containing
     * the input key, adds a read lock on the range and returns a pointer to the
     * table range entry in the returned record. The table range entry gives the
     * range's partition ID and the dirty range's partition ID, if the range is
     * being split or merged. It will also put a read lock on the range owner
     * bucket record, which will be released on range record post read. The
     * range owner bucket record is used to check which node group is holding
     * the data of this range.
     *
     * @param req The read request containing the input key and returned record.
     * @return true, if the request has been executed and is to be freed; false,
     * if the request blocked and should not be freed.
     */
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

        assert(req.IsLocal());
        assert(this->table_name_ == *req.GetTableName());

        CcHandlerResult<ReadKeyResult> *hd_result = req.Result();
        int64_t ng_term = req.NodeGroupTerm();
        if (ng_term < 0)
        {
            if (req.AllowRunOnCandidate())
            {
                ng_term =
                    Sharder::Instance().CandidateLeaderTerm(req.NodeGroupId());
            }
            if (ng_term < 0)
            {
                ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
                int64_t standby_node_term =
                    Sharder::Instance().StandbyNodeTerm();
                ng_term = std::max(ng_term, standby_node_term);
            }
        }
        assert(ng_term > 0);

        // For range cc maps, we assume that all of a table's ranges are loaded
        // into memory for caching when the range cc map is initialized. There
        // is never a read-outside request that brings an individual range into
        // memory for caching.
        assert(req.Type() != ReadType::OutsideNormal);
        // We directly abort cc request if blocked by write lock. So we should
        // not have any resumed range read cc.
        assert(req.CcePtr() == nullptr);

        LockType acquired_lock;
        CcErrorCode err_code;
        // Rather than looking for an exact match, looks up the floor key
        // that represents the range containing the input key.
        const KeyT *look_key = nullptr;
        KeyT decoded_key;
        if (req.Key() != nullptr)
        {
            look_key = static_cast<const KeyT *>(req.Key());
        }
        else
        {
            assert(req.KeyBlob() != nullptr);
            size_t offset = 0;
            decoded_key.Deserialize(req.KeyBlob()->data(), offset, KeySchema());
            look_key = &decoded_key;
        }

        auto it = Floor(*look_key);
        CcEntry<KeyT, RangeRecord, true, false> *floor_cce = it->second;
        CcPage<KeyT, RangeRecord, true, false> *range_page = it.GetPage();

        // When we're acquiring bucket lock and range lock, always acquire
        // bucket lock before range lock to avoid internal dead lock.
        auto bucket_cce = static_cast<
            CcEntry<RangeBucketKey, RangeBucketRecord, true, false> *>(
            floor_cce->payload_.cur_payload_->range_owner_rec_);

        assert(bucket_cce != nullptr);

        // Acquire bucket read lock
        std::tie(acquired_lock, err_code) =
            AcquireCceKeyLock(bucket_cce,
                              bucket_cce->CommitTs(),
                              nullptr,
                              bucket_cce->PayloadStatus(),
                              &req,
                              req.NodeGroupId(),
                              ng_term,
                              req.TxTerm(),
                              CcOperation::Read,
                              IsolationLevel::RepeatableRead,
                              CcProtocol::Locking,
                              req.ReadTimestamp(),
                              false,
                              bucket_ccm_);
        if (err_code != CcErrorCode::NO_ERROR)
        {
            assert(err_code == CcErrorCode::ACQUIRE_LOCK_BLOCKED);
            bucket_cce->GetKeyLock()->AbortQueueRequest(
                req.Txn(),
                CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT);
            return true;
        }

        CcOperation cc_op =
            req.IsForWrite() ? CcOperation::ReadForWrite : CcOperation::Read;
        std::tie(acquired_lock, err_code) =
            AcquireCceKeyLock(floor_cce,
                              floor_cce->CommitTs(),
                              range_page,
                              floor_cce->PayloadStatus(),
                              &req,
                              req.NodeGroupId(),
                              ng_term,
                              req.TxTerm(),
                              cc_op,
                              req.Isolation(),
                              req.Protocol(),
                              req.ReadTimestamp(),
                              false);

        if (err_code == CcErrorCode::NO_ERROR)
        {
            CcEntryAddr &cce_addr = hd_result->Value().cce_addr_;
            cce_addr.SetCceLock(reinterpret_cast<uint64_t>(
                                    floor_cce->GetKeyGapLockAndExtraData()),
                                ng_term,
                                req.NodeGroupId(),
                                shard_->LocalCoreId());
            RangeRecord *range_rec = static_cast<RangeRecord *>(req.Record());
            range_rec->CopyForReadResult(*(floor_cce->payload_.cur_payload_));
            hd_result->Value().ts_ = floor_cce->CommitTs();
            hd_result->Value().rec_status_ = RecordStatus::Normal;
            hd_result->Value().lock_type_ = acquired_lock;
            hd_result->SetFinished();
            return true;
        }
        else
        {
            assert(err_code == CcErrorCode::ACQUIRE_LOCK_BLOCKED);
            // Release the acquired bucket read lock before aborting.
            ReleaseCceLock(bucket_cce->GetKeyLock(),
                           bucket_cce,
                           req.Txn(),
                           this->cc_ng_id_,
                           LockType::ReadLock);
            floor_cce->GetKeyLock()->AbortQueueRequest(
                req.Txn(),
                CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT);
            return true;
        }

        return true;
    }

    bool Execute(PostReadCc &req) override
    {
        const CcEntryAddr &cce_addr = *req.CceAddr();
        CcEntry<KeyT, RangeRecord, true, false> &cc_entry =
            *reinterpret_cast<CcEntry<KeyT, RangeRecord, true, false> *>(
                cce_addr.ExtractCce());

        // Release bucket record read lock. This lock was acquried in range
        // cc map read cc, and is not put into readset. So we need to be
        // releasing it here manually.
        auto bucket_cce = static_cast<
            CcEntry<RangeBucketKey, RangeBucketRecord, true, false> *>(
            cc_entry.payload_.cur_payload_->range_owner_rec_);
        shard_->UpdateLastReadTs(req.CommitTs());
        ReleaseCceLock(bucket_cce->GetKeyLock(),
                       bucket_cce,
                       req.Txn(),
                       req.NodeGroupId(),
                       LockType::ReadLock);

        shard_->UpdateLastReadTs(req.CommitTs());
        ReleaseCceLock(cc_entry.GetKeyLock(),
                       &cc_entry,
                       req.Txn(),
                       req.NodeGroupId(),
                       LockType::ReadLock);

        req.Result()->SetFinished();
        return true;
    }

    bool Execute(AcquireAllCc &req) override
    {
        if (shard_->core_id_ == 0 && req.Key() != nullptr)
        {
            // If we are the owner of this range, mark the StoreRange as
            // locked. Note that req.Key() is always not null if we're the owner
            // since the owner if always the split range coordinator.
            const KeyT *range_key = static_cast<const KeyT *>(req.Key());
            TxKey range_tx_key(range_key);
            TemplateTableRangeEntry<KeyT> *range_entry =
                static_cast<TemplateTableRangeEntry<KeyT> *>(
                    shard_->GetTableRangeEntry(
                        this->table_name_, req.NodeGroupId(), range_tx_key));

            TemplateStoreRange<KeyT> *range = range_entry->TypedStoreRange();
            if (range)
            {
                range->Lock();
            }
        }

        return TemplateCcMap<KeyT, RangeRecord, true, false>::Execute(req);
    }

    /**
     * @brief
     */
    bool Execute(PostWriteAllCc &req) override
    {
        // When the commit ts is 0 or the commit type is DowngradeLock, the
        // request commits nothing and only removes the write intents/locks
        // acquired earlier.
        if (req.CommitTs() == TransactionOperation::tx_op_failed_ts_ ||
            req.CommitType() == PostWriteType::DowngradeLock)
        {
            return TemplateCcMap<KeyT, RangeRecord, true, false>::Execute(req);
        }

        assert(req.CommitType() != PostWriteType::DowngradeLock);

        // Prepare RangeRecord
        RangeRecord *upload_range_rec = nullptr;
        const KeyT *target_key = nullptr;
        // Place holder for decoded range info if req is remote
        if (req.Key() != nullptr)
        {
            upload_range_rec = static_cast<RangeRecord *>(req.Payload());
            target_key = static_cast<const KeyT *>(req.Key());
        }
        else
        {
            // Request comes from a remote node and is processed for the first
            // time. Deserialize the keys and payloads.
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
            assert(req.PayloadStr() != nullptr);
            std::unique_ptr<RangeRecord> decoded_rec =
                std::make_unique<RangeRecord>();
            DeserializeRangeRecord(*req.PayloadStr(), decoded_rec.get());
            upload_range_rec = decoded_rec.get();
            req.SetDecodedPayload(std::move(decoded_rec));
        }

        CcEntry<KeyT, RangeRecord, true, false> *target_cce =
            Find(*target_key)->second;

        // Check whether cce key lock holder is the given tx of the
        // PostWriteAllCc before apply change.
        if (target_cce == nullptr || target_cce->GetKeyLock() == nullptr ||
            !target_cce->GetKeyLock()->HasWriteLock(req.Txn()))
        {
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

        if (req.CommitType() == PostWriteType::PrepareCommit)
        {
            // update the local shards' table range value on the first core.
            if (shard_->core_id_ == 0)
            {
                assert(
                    upload_range_rec->GetRangeInfo()->new_key_.size() ==
                    upload_range_rec->GetRangeInfo()->new_partition_id_.size());

                // upload the new range info to local cc shards,
                // point the req range rec to the local cc shard table_ranges_
                // entry. The req range rec will be used to update ccmap on each
                // core.
                upload_range_rec->SetRangeInfo(
                    shard_->local_shards_
                        .UploadNewRangeInfo(
                            this->table_name_,
                            this->cc_ng_id_,
                            *target_key,
                            upload_range_rec->GetRangeInfo()->new_key_,
                            upload_range_rec->GetRangeInfo()->new_partition_id_,
                            req.CommitTs())
                        ->GetRangeInfo());

                NodeGroupId range_owner =
                    shard_
                        ->GetRangeOwner(
                            upload_range_rec->GetRangeInfo()->PartitionId(),
                            this->cc_ng_id_)
                        ->BucketOwner();
                if (range_owner == this->cc_ng_id_)
                {
                    TxKey tx_key(target_key);
                    TemplateTableRangeEntry<KeyT> *range_entry =
                        static_cast<TemplateTableRangeEntry<KeyT> *>(
                            shard_->GetTableRangeEntry(
                                this->table_name_, req.NodeGroupId(), tx_key));
                    assert(range_entry->RangeSlices());
                    range_entry->TypedStoreRange()->Unlock();
                }
                else
                {
                    ACTION_FAULT_INJECTOR(
                        "range_split_participant_prepare_post_all");
                }
            }
            // Register the range owner bucket for the new ranges
            auto bucket_map = static_cast<RangeBucketCcMap *>(
                shard_->GetCcm(range_bucket_ccm_name, this->cc_ng_id_));
            auto new_range_owner_rec =
                std::make_unique<std::vector<LruEntry *>>();
            for (int32_t new_id :
                 upload_range_rec->GetRangeInfo()->new_partition_id_)
            {
                // Link bucket owner record
                new_range_owner_rec->push_back(bucket_map->GetBucketRecord(
                    Sharder::MapRangeIdToBucketId(new_id)));
            }
            upload_range_rec->SetNewRangeOwnerRec(
                std::move(new_range_owner_rec));
            // Reuse range_owner_rec_ from old cce. range_owner_rec_ needs to be
            // reset on each core since they point to bucket records on
            // different cores.
            upload_range_rec->range_owner_rec_ =
                target_cce->payload_.cur_payload_->range_owner_rec_;
        }
        else if (req.CommitType() == PostWriteType::PostCommit)
        {
            std::vector<const TemplateTableRangeEntry<KeyT> *>
                new_range_entries;
            TxKey tx_key(target_key);
            TemplateTableRangeEntry<KeyT> *old_entry =
                static_cast<TemplateTableRangeEntry<KeyT> *>(
                    shard_->local_shards_.GetTableRangeEntry(
                        this->table_name_, req.NodeGroupId(), tx_key));
            assert(old_entry != nullptr);
            TemplateRangeInfo<KeyT> *old_info = old_entry->TypedRangeInfo();
            NodeGroupId range_owner =
                shard_
                    ->GetRangeOwner(
                        upload_range_rec->GetRangeInfo()->PartitionId(),
                        this->cc_ng_id_)
                    ->BucketOwner();

            if (shard_->core_id_ == 0)
            {
                // Check if the range entry is still dirty.
                // If not, that means the post write has already been executed
                // on this ng. We cannot execute the code below repeatedly,
                // since the code below will install the slice specs received
                // from the post write message. However once the write lock has
                // been removed, the slice specs could already be updated by
                // others(i.e. checkpointer). In this case we shold not
                // overwrite the udpated slice specs with the ones received from
                // the post write message.
                if (!old_info->IsDirty())
                {
                    assert(old_info->VersionTs() >= req.CommitTs());
                    req.Result()->SetFinished();
                    req.SetDecodedPayload(nullptr);
                    return true;
                }

                // Update the table range map in local cc shards.
                size_t estimate_rec_size = UINT64_MAX;
                if (txservice_enable_key_cache && this->table_name_.IsBase())
                {
                    TableStatistics<KeyT> *statistics =
                        static_cast<TableStatistics<KeyT> *>(
                            table_schema_->StatisticsObject().get());
                    if (statistics)
                    {
                        estimate_rec_size = statistics->EstimateRecordSize();
                    }
                }
                new_range_entries =
                    shard_->local_shards_.SplitTableRange(this->table_name_,
                                                          this->cc_ng_id_,
                                                          old_entry,
                                                          estimate_rec_size);
                if (new_range_entries.empty())
                {
                    // Range split failed due to slice pinned or being
                    // loaded, yield and retry
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
                }

                if (txservice_enable_key_cache && this->table_name_.IsBase())
                {
                    int64_t ng_term = req.NodeGroupTerm();
                    assert(ng_term > 0);
                    // try to init the key cache for new range if it
                    // lands on this ng
                    for (auto new_range : new_range_entries)
                    {
                        auto new_store_range = new_range->TypedStoreRange();
                        if (new_store_range)
                        {
                            new_store_range->InitKeyCache(shard_,
                                                          &this->table_name_,
                                                          this->cc_ng_id_,
                                                          ng_term);
                        }
                    }

                    // The old range might have its key cache invalidated since
                    // too many keys are inserted to the range before range
                    // split, now that the keys are splitted to new ranges, try
                    // to re-initialize the key cache of old range if it's not
                    // valid.
                    auto old_store_range = old_entry->TypedStoreRange();
                    if (old_store_range)
                    {
                        old_store_range->InitKeyCache(shard_,
                                                      &this->table_name_,
                                                      this->cc_ng_id_,
                                                      ng_term,
                                                      true);
                    }
                }

                // Mark the range as commited, which makes the new keys
                // invisible to range read requests. The new keys are not
                // deleted yet because we need to pass them to other cores to
                // update range cc map.
                old_info->CommitDirty();
                old_entry->SetRangeEndKey(
                    new_range_entries.front()->RangeStartKey());
                upload_range_rec->SetRangeInfo(old_info);
                upload_range_rec->SetNewRangeOwnerRec(nullptr);

                if (range_owner == this->cc_ng_id_)
                {
                    ACTION_FAULT_INJECTOR("range_split_post_commit");
                    old_entry->TypedStoreRange()->Unlock();
                }
                else
                {
                    ACTION_FAULT_INJECTOR(
                        "range_split_post_commit_participant");
                }
            }
            else
            {
                for (const TxKey &new_key : old_info->new_key_)
                {
                    const TemplateTableRangeEntry<KeyT> *range_entry =
                        static_cast<const TemplateTableRangeEntry<KeyT> *>(
                            shard_->GetTableRangeEntry(
                                this->table_name_, this->cc_ng_id_, new_key));
                    new_range_entries.push_back(range_entry);
                }
            }

            // The flush data operation performed during the range splitting
            // process only flushes the data that falls into the new range to
            // the new range. Therefore, it can only be removed from the memory
            // after the range write lock is acquired. In other words, only
            // after committing the dirty range can the start page be cleaned
            // and the wait queue be dequeued.
            shard_->OnDirtyDataFlushed();

            assert(new_range_entries.size());

            // add new range entry to range cc map
            auto &new_range_owner_rec =
                *target_cce->payload_.cur_payload_->new_range_owner_rec_;
            for (uint idx = 0; idx < new_range_entries.size(); idx++)
            {
                const TemplateRangeInfo<KeyT> *new_range_info =
                    new_range_entries.at(idx)->TypedRangeInfo();
                const KeyT *start_key = new_range_info->StartKey();
                auto it =
                    TemplateCcMap<KeyT, RangeRecord, true, false>::FindEmplace(
                        *start_key);
                CcEntry<KeyT, RangeRecord, true, false> *cce = it->second;

                if (cce->CommitTs() >= new_range_info->version_ts_)
                {
                    // Skip if the new range entry is already committed.
                    continue;
                }
                cce->payload_.cur_payload_ = std::make_shared<RangeRecord>();
                cce->payload_.cur_payload_->range_info_ = new_range_info;
                cce->payload_.cur_payload_->range_owner_rec_ =
                    new_range_owner_rec.at(idx);

                // update previous cce's end key
                cce->SetCommitTsPayloadStatus(new_range_info->version_ts_,
                                              RecordStatus::Normal);
            }
            // range_owner_rec_ needs to be reset on each core since they point
            // to bucket records on different cores.
            upload_range_rec->range_owner_rec_ =
                target_cce->payload_.cur_payload_->range_owner_rec_;

            if (shard_->realtime_sampling_ &&
                shard_->core_id_ == Statistics::LeaderCore(this->table_name_))
            {
                SplitSamplePool(old_info);
            }

            // Now that every core has inserted new range entries into ccmap, we
            // don't need the new keys anymore. Remove new keys from the
            // original range.
            if (shard_->core_id_ == shard_->core_cnt_ - 1)
            {
                old_entry->TypedRangeInfo()->ClearDirty();
            }
        }

        return TemplateCcMap<KeyT, RangeRecord, true, false>::Execute(req);
    }

    bool Execute(ReplayLogCc &req) override
    {
        uint32_t group_id = req.NodeGroupId();
        int64_t ng_term = Sharder::Instance().CandidateLeaderTerm(group_id);
        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        // restore the SplitRangeOpMessage
        const std::string_view &content = req.LogContentView();
        ::txlog::SplitRangeOpMessage ds_split_range_op_msg;
        ds_split_range_op_msg.ParseFromArray(content.data(), content.length());

        // Restore old range key
        std::unique_ptr<KeyT> old_range_key = std::make_unique<KeyT>();
        const KeyT *old_range_key_ptr;
        size_t offset = 0;

        if (ds_split_range_op_msg.range_key_case() ==
            txlog::SplitRangeOpMessage::RangeKeyCase::kRangeKeyValue)
        {
            old_range_key->Deserialize(
                const_cast<char *>(
                    ds_split_range_op_msg.range_key_value().c_str()),
                offset,
                KeySchema());
            old_range_key_ptr = old_range_key.get();
        }
        else
        {
            old_range_key_ptr = KeyT::NegativeInfinity();
        }

        size_t new_range_cnt = ds_split_range_op_msg.new_partition_id_size();
        std::vector<TxKey> new_range_keys;
        std::vector<int32_t> new_range_ids;
        new_range_ids.reserve(new_range_cnt);
        new_range_keys.reserve(new_range_cnt);
        for (size_t i = 0; i < new_range_cnt; i++)
        {
            int32_t range_id = ds_split_range_op_msg.new_partition_id(i);
            new_range_ids.push_back(range_id);
            std::unique_ptr<KeyT> new_range_key = std::make_unique<KeyT>();
            offset = 0;
            new_range_key->Deserialize(
                const_cast<char *>(
                    ds_split_range_op_msg.new_range_key(i).c_str()),
                offset,
                this->KeySchema());
            new_range_keys.emplace_back(std::move(new_range_key));
        }

        // Restore partition partition id
        int32_t partition_id = ds_split_range_op_msg.partition_id();

        // Restore stage
        ::txlog::SplitRangeOpMessage_Stage stage =
            ds_split_range_op_msg.stage();

        uint32_t tx_node_id = (req.Txn() >> 32L) >> 10;
        int64_t tx_candidate_term =
            Sharder::Instance().CandidateLeaderTerm(tx_node_id);
        bool is_coordinator =
            tx_node_id == req.NodeGroupId() && tx_candidate_term >= 0;

        // Restore local_cc_shards state at core 0
        TemplateTableRangeEntry<KeyT> *old_table_range_entry = nullptr;
        const KeyT *old_end_key = nullptr;
        CcEntry<KeyT, RangeRecord, true, false> *old_range_cce = nullptr;
        CcPage<KeyT, RangeRecord, true, false> *old_range_page = nullptr;

        if (ds_split_range_op_msg.range_key_case() ==
            txlog::SplitRangeOpMessage::RangeKeyCase::kRangeKeyNegInf)
        {
            old_range_cce = &neg_inf_;
            old_range_page = &neg_inf_page_;
        }
        else
        {
            auto it = Find(*old_range_key_ptr);
            assert(it->first);
            old_range_cce = it->second;
            old_range_page = it.GetPage();
        }

        if (shard_->core_id_ == 0 && is_coordinator)
        {
            if (old_range_cce != nullptr &&
                old_range_cce->GetKeyLock() != nullptr &&
                old_range_cce->GetKeyLock()->SearchLock(req.Txn()) !=
                    LockType::NoLock)
            {
                req.SetFinish();
                return true;
            }
        }

        // Restore range end key
        if (stage == ::txlog::SplitRangeOpMessage_Stage_PrepareSplit)
        {
            // We can safely use the end key of the old range ccentry,
            // since we know for sure that the new range ccentries have
            // not been inserted into ccmap yet.
            const TemplateRangeInfo<KeyT> *old_range_info =
                static_cast<const TemplateRangeInfo<KeyT> *>(
                    old_range_cce->payload_.cur_payload_->range_info_);
            old_end_key = old_range_info->EndKey();
        }
        else if (stage == ::txlog::SplitRangeOpMessage_Stage_CommitSplit)
        {
            // Find the next cce of the last new range key, since we
            // don't know if the new range cce has been created or not.
            auto it = Floor(*new_range_keys.back().GetKey<KeyT>());
            CcEntry<KeyT, RangeRecord, true, false> *next_cce = it->second;
            const TemplateRangeInfo<KeyT> *next_range_info =
                static_cast<const TemplateRangeInfo<KeyT> *>(
                    next_cce->payload_.cur_payload_->range_info_);
            old_end_key = next_range_info->EndKey();
        }
        else
        {
            assert(false);
        }

        std::vector<const TemplateRangeInfo<KeyT> *> new_range_infos;
        if (shard_->core_id_ == 0)
        {
            if (stage == ::txlog::SplitRangeOpMessage_Stage_PrepareSplit)
            {
                if (!is_coordinator)
                {
                    // Participants needs to be restored to state right before
                    // commit log is written, so we need to install the dirty
                    // range.
                    old_table_range_entry =
                        shard_->local_shards_.UploadNewRangeInfo(
                            this->table_name_,
                            this->cc_ng_id_,
                            *old_range_key_ptr,
                            new_range_keys,
                            new_range_ids,
                            req.CommitTs());
                }
                else
                {
                    // For coordinator, the replay tx will install the dirty
                    // range for us.
                    old_table_range_entry =
                        static_cast<TemplateTableRangeEntry<KeyT> *>(
                            shard_->local_shards_.GetTableRangeEntry(
                                this->table_name_,
                                this->cc_ng_id_,
                                TxKey(old_range_key_ptr)));

                    if (old_table_range_entry->TypedStoreRange() == nullptr)
                    {
                        old_table_range_entry->FetchRangeSlices(
                            this->table_name_,
                            &req,
                            this->cc_ng_id_,
                            ng_term,
                            this->shard_);
                        return false;
                    }
                    old_table_range_entry->TypedStoreRange()->Lock();
                }
            }
            else if (stage == ::txlog::SplitRangeOpMessage_Stage_CommitSplit)
            {
                std::vector<SliceInitInfo> range_slices;
                range_slices.emplace_back(TxKey(),
                                          ds_split_range_op_msg.slice_sizes(0),
                                          SliceStatus::PartiallyCached);
                for (int idx = 0; idx < ds_split_range_op_msg.slice_keys_size();
                     idx++)
                {
                    std::unique_ptr<KeyT> slice_key = std::make_unique<KeyT>();
                    offset = 0;
                    slice_key->Deserialize(
                        const_cast<char *>(
                            ds_split_range_op_msg.slice_keys(idx).c_str()),
                        offset,
                        this->KeySchema());
                    range_slices.emplace_back(
                        TxKey(std::move(slice_key)),
                        ds_split_range_op_msg.slice_sizes(idx + 1),
                        SliceStatus::PartiallyCached);
                }

                if (!is_coordinator)
                {
                    // For non coordinator, we need to assume that the
                    // postwriteall has already been executed. So install the
                    // dirty range info as the current range info and no lock is
                    // needed.
                    old_table_range_entry =
                        static_cast<TemplateTableRangeEntry<KeyT> *>(
                            shard_->local_shards_.GetTableRangeEntry(
                                this->table_name_,
                                this->cc_ng_id_,
                                TxKey(old_range_key_ptr)));

                    TemplateRangeInfo<KeyT> *old_info =
                        old_table_range_entry->TypedRangeInfo();
                    // Initialize slice specs if any of the new ranges falls on
                    // this ng.
                    std::unique_ptr<TemplateStoreRange<KeyT>> store_range =
                        std::make_unique<TemplateStoreRange<KeyT>>(
                            old_info->StartKey(),
                            old_end_key,
                            old_info->partition_id_,
                            tx_node_id,
                            *Sharder::Instance().GetLocalCcShards(),
                            false);
                    store_range->InitSlices(std::move(range_slices));
                    std::vector<SliceInitInfo> new_slice_keys;
                    store_range->SplitRange(
                        new_range_keys.front().GetKey<KeyT>(), new_slice_keys);

                    if (new_slice_keys.empty())
                    {
                        // If all current slices should stay in old range,
                        // assign empty slice for new range
                        for (const TxKey &new_key : new_range_keys)
                        {
                            const KeyT *new_range_start =
                                new_key.GetKey<KeyT>();
                            new_slice_keys.emplace_back(
                                TxKey(std::make_unique<KeyT>(*new_range_start)),
                                0,
                                SliceStatus::FullyCached);
                        }
                    }

                    // Create new range entries in local cc shard
                    size_t cur_slice_idx = 0;
                    for (uint new_range_idx = 0; new_range_idx < new_range_cnt;
                         new_range_idx++)
                    {
                        std::vector<SliceInitInfo> cur_range_slices;
                        // First slice start key will reuse the new range start
                        // key, so we can just pass in nullptr.
                        std::unique_ptr<KeyT> range_start_key =
                            new_slice_keys[cur_slice_idx].key_.MoveKey<KeyT>();
                        cur_range_slices.emplace_back(
                            std::move(new_slice_keys.at(cur_slice_idx)));
                        cur_slice_idx++;
                        // Move the range slices that falls into the new range.
                        while (cur_slice_idx != new_slice_keys.size() &&
                               (new_range_idx + 1 == new_range_cnt ||
                                new_slice_keys.at(cur_slice_idx).key_ <
                                    new_range_keys.at(new_range_idx + 1)))
                        {
                            cur_range_slices.emplace_back(
                                std::move(new_slice_keys.at(cur_slice_idx++)));
                        }

                        if (new_range_idx < new_range_cnt - 1 &&
                            cur_slice_idx == new_slice_keys.size())
                        {
                            // If we run out of slice before we reach last new
                            // range, insert an empty slice for the next new
                            // range
                            for (size_t idx = new_range_idx + 1;
                                 idx < new_range_cnt;
                                 ++idx)
                            {
                                const KeyT *r_start =
                                    new_range_keys[idx].GetKey<KeyT>();
                                new_slice_keys.emplace_back(
                                    TxKey(std::make_unique<KeyT>(*r_start)),
                                    0,
                                    SliceStatus::FullyCached);
                            }
                        }
                        assert(*range_start_key ==
                               *new_range_keys[new_range_idx].GetKey<KeyT>());

                        const TemplateTableRangeEntry<KeyT> *new_range =
                            shard_->local_shards_.CreateTableRange(
                                this->table_name_,
                                this->cc_ng_id_,
                                new_range_ids.at(new_range_idx),
                                *range_start_key,
                                old_info->dirty_ts_,
                                &cur_range_slices);
                        new_range_infos.emplace_back(
                            new_range->TypedRangeInfo());
                    }
                    old_info->ClearDirty(req.CommitTs());
                }
                else
                {
                    // Restore the current and dirty range info. Acquire write
                    // lock.
                    old_table_range_entry =
                        shard_->local_shards_.UploadNewRangeInfo(
                            this->table_name_,
                            this->cc_ng_id_,
                            *old_range_key_ptr,
                            new_range_keys,
                            new_range_ids,
                            req.CommitTs());
                    // Restore range slice specs from log message. the range
                    // slice specs we read from data store is unreliable since
                    // it could've already been updated before the crash.

                    std::unique_lock<std::mutex> heap_lk(
                        shard_->local_shards_.table_ranges_heap_mux_);
                    bool is_override_thd = mi_is_override_thread();
                    mi_threadid_t prev_thd = mi_override_thread(
                        shard_->local_shards_.GetTableRangesHeapThreadId());
                    mi_heap_t *prev_heap = mi_heap_set_default(
                        shard_->local_shards_.GetTableRangesHeap());

#if defined(WITH_JEMALLOC)
                    uint32_t prev_arena;
                    JemallocArenaSwitcher::ReadCurrentArena(prev_arena);
                    // override arena id
                    auto table_range_arena_id =
                        shard_->local_shards_.GetTableRangesArenaId();
                    JemallocArenaSwitcher::SwitchToArena(table_range_arena_id);
#endif
                    old_table_range_entry->InitRangeSlices(
                        std::move(range_slices),
                        this->cc_ng_id_,
                        this->table_name_.IsBase());

                    mi_heap_set_default(prev_heap);
                    if (is_override_thd)
                    {
                        mi_override_thread(prev_thd);
                    }
                    else
                    {
                        mi_restore_default_thread_id();
                    }

#if defined(WITH_JEMALLOC)
                    JemallocArenaSwitcher::SwitchToArena(prev_arena);
#endif
                    heap_lk.unlock();
                    old_table_range_entry->RangeSlices()->Lock();
                }
            }
            else
            {
                // should not be here if log is in clean stage
                assert(false);
            }

            if (!shard_->LoadRangesAndStatisticsNx(
                    table_schema_, req.NodeGroupId(), ng_term, &req))
            {
                return false;
            }
        }
        else
        {
            old_table_range_entry =
                static_cast<TemplateTableRangeEntry<KeyT> *>(
                    shard_->local_shards_.GetTableRangeEntry(
                        this->table_name_,
                        this->cc_ng_id_,
                        TxKey(old_range_key_ptr)));
            if (stage == ::txlog::SplitRangeOpMessage_Stage_CommitSplit &&
                !is_coordinator)
            {
                for (auto &new_key : new_range_keys)
                {
                    TemplateTableRangeEntry<KeyT> *new_range_entry =
                        static_cast<TemplateTableRangeEntry<KeyT> *>(
                            shard_->local_shards_.GetTableRangeEntry(
                                this->table_name_, this->cc_ng_id_, new_key));
                    new_range_infos.emplace_back(
                        new_range_entry->TypedRangeInfo());
                }
            }
        }
        assert(old_table_range_entry != nullptr);

        // Create new range records in ccmap if commit log and as participant.
        // In this case we should recover to the stage where post write all
        // has already been executed on this ng.
        if (stage == ::txlog::SplitRangeOpMessage_Stage_CommitSplit &&
            !is_coordinator)
        {
            // add new range entry to range cc map
            auto bucket_map = static_cast<RangeBucketCcMap *>(
                shard_->GetCcm(range_bucket_ccm_name, this->cc_ng_id_));
            for (uint idx = 0; idx < new_range_infos.size(); idx++)
            {
                const TemplateRangeInfo<KeyT> *new_range_info =
                    new_range_infos.at(idx);
                const KeyT *start_key = new_range_info->StartKey();
                auto it =
                    TemplateCcMap<KeyT, RangeRecord, true, false>::FindEmplace(
                        *start_key);
                CcEntry<KeyT, RangeRecord, true, false> *cce = it->second;

                if (cce->CommitTs() >= new_range_info->version_ts_)
                {
                    continue;
                }
                cce->payload_.cur_payload_ = std::make_shared<RangeRecord>();
                cce->payload_.cur_payload_->range_info_ = new_range_info;
                // Link bucket owner record
                cce->payload_.cur_payload_->range_owner_rec_ =
                    bucket_map->GetBucketRecord(Sharder::MapRangeIdToBucketId(
                        new_range_info->PartitionId()));
                cce->SetCommitTsPayloadStatus(new_range_info->version_ts_,
                                              RecordStatus::Normal);
            }
        }

        if (ds_split_range_op_msg.stage() ==
                ::txlog::SplitRangeOpMessage_Stage_PrepareSplit ||
            (ds_split_range_op_msg.stage() ==
                 ::txlog::SplitRangeOpMessage_Stage_CommitSplit &&
             is_coordinator))
        {
            // For coordinator node, we need to restore to the state
            // right after the current log is written, for participant
            // node, we need to restore to the state right before the
            // next log is written. So if the log state is at prepare
            // stage, we need to acquire write lock on range no matter
            // what.
            auto lock_pair = AcquireCceKeyLock(old_range_cce,
                                               old_range_cce->CommitTs(),
                                               old_range_page,
                                               old_range_cce->PayloadStatus(),
                                               &req,
                                               req.NodeGroupId(),
                                               ng_term,
                                               0,
                                               CcOperation::Write,
                                               IsolationLevel::RepeatableRead,
                                               CcProtocol::Locking,
                                               0,
                                               false);
            // When a cc node recovers, no one should be holding read
            // locks. So, the acquire operation should always succeed.
            assert(lock_pair.first == LockType::WriteLock &&
                   lock_pair.second == CcErrorCode::NO_ERROR);
            (void) lock_pair;

            // Register the range owner bucket for the new ranges
            auto bucket_map = static_cast<RangeBucketCcMap *>(
                shard_->GetCcm(range_bucket_ccm_name, this->cc_ng_id_));
            auto new_range_owner_rec =
                std::make_unique<std::vector<LruEntry *>>();
            for (int32_t new_id : new_range_ids)
            {
                // Link bucket owner record
                new_range_owner_rec->push_back(bucket_map->GetBucketRecord(
                    Sharder::MapRangeIdToBucketId(new_id)));
            }
            old_range_cce->payload_.cur_payload_->SetNewRangeOwnerRec(
                std::move(new_range_owner_rec));
        }

        // Move to next core
        if (shard_->core_id_ < shard_->core_cnt_ - 1)
        {
            req.ResetCcm();
            MoveRequest(&req, shard_->core_id_ + 1);
        }
        else
        {
            // Restore transaction and catalog read lock at last core if this
            // recovering node group is the tx coordinator. This will spawn
            // a worker thread that will restore the transaction. It will call
            // SetFinish() after acqruing all needed locks.
            if (tx_node_id == req.NodeGroupId() && tx_candidate_term >= 0)
            {
                const TemplateRangeInfo<KeyT> *old_range_info =
                    old_table_range_entry->TypedRangeInfo();
                shard_->local_shards_.CreateSplitRangeRecoveryTx(
                    req,
                    ds_split_range_op_msg,
                    partition_id,
                    old_range_info,
                    std::move(new_range_keys),
                    std::move(new_range_ids),
                    tx_node_id,
                    tx_candidate_term);
            }
            else
            {
                req.SetFinish();
            }
        }

        return true;
    }

    bool Execute(UploadRangeSlicesCc &req) override
    {
        const auto *key_schema = KeySchema();

        std::vector<SliceInitInfo> &new_slices = req.Slices();
        const std::string &slices_keys = req.NewSlicesKeys();
        const char *buf = slices_keys.data();
        size_t keys_len = slices_keys.size();

        const std::string &slices_sizes = req.NewSlicesSizes();
        const char *sizes_buf = slices_sizes.data();

        const std::string &slices_status = req.NewSlicesStatus();
        const char *status_buf = slices_status.data();

        uint16_t parsed_count = 0;
        auto [keys_offset, sizes_offset, status_offset] = req.ParseOffsets();

        while (keys_offset < keys_len &&
               parsed_count < UploadRangeSlicesCc::MaxParseBatchSize)
        {
            // slice start key
            std::unique_ptr<KeyT> typed_key = std::make_unique<KeyT>();
            typed_key->Deserialize(buf, keys_offset, key_schema);
            // slice size
            uint32_t slice_size =
                *(reinterpret_cast<const uint32_t *>(sizes_buf + sizes_offset));
            sizes_offset += sizeof(uint32_t);
            SliceStatus status = static_cast<SliceStatus>(*(
                reinterpret_cast<const int8_t *>(status_buf + status_offset)));
            status_offset += sizeof(int8_t);

            new_slices.emplace_back(
                TxKey(std::move(typed_key)), slice_size, status);

            parsed_count++;
        }

        if (keys_offset < keys_len)
        {
            req.SetParseOffset(keys_offset, sizes_offset, status_offset);
            shard_->Enqueue(shard_->LocalCoreId(), &req);
            return false;
        }

        if (new_slices.size() != req.NewSlicesCount())
        {
            LOG(WARNING) << "UploadRangeSlicesCc: slices size not match.";
            assert(false);
        }

        bool res = shard_->local_shards_.template UploadDirtyRangeSlices<KeyT>(
            *req.GetTableName(),
            req.NodeGroupId(),
            req.RangeId(),
            req.VersionTs(),
            req.NewRangeId(),
            req.HasDmlSinceDdl(),
            std::move(new_slices));
        if (res)
        {
            req.SetFinish();
        }
        else
        {
            req.SetError(CcErrorCode::UPLOAD_BATCH_REJECTED);
        }

        return false;
    }

    TableType Type() const override
    {
        return TableType::RangePartition;
    }

private:
    void DeserializeRangeRecord(const std::string &payload,
                                RangeRecord *range_record)
    {
        const char *buf = payload.data();
        size_t offset = 0;
        KeyT start_key;
        bool is_normal;
        DesrializeFrom(buf, offset, &is_normal);
        if (is_normal)
        {
            start_key.Deserialize(buf, offset, nullptr);
        }

        uint64_t version_ts;
        uint32_t partition_id;
        DesrializeFrom(buf, offset, &partition_id);
        DesrializeFrom(buf, offset, &version_ts);
        std::unique_ptr<TemplateRangeInfo<KeyT>> range_info =
            std::make_unique<TemplateRangeInfo<KeyT>>(
                is_normal ? &start_key : KeyT::NegativeInfinity(),
                version_ts,
                partition_id,
                KeyT::PositiveInfinity());
        uint16_t new_part_size;
        DesrializeFrom(buf, offset, &new_part_size);
        if (new_part_size > 0)
        {
            range_info->is_dirty_ = true;
        }
        for (size_t idx = 0; idx < new_part_size; idx++)
        {
            std::unique_ptr<KeyT> new_key = std::make_unique<KeyT>();
            new_key->Deserialize(buf, offset, nullptr);
            range_info->new_key_.emplace_back(TxKey(std::move(new_key)));
        }
        for (size_t idx = 0; idx < new_part_size; idx++)
        {
            int32_t new_part_id;
            DesrializeFrom(buf, offset, &new_part_id);
            range_info->new_partition_id_.push_back(new_part_id);
        }
        DesrializeFrom(buf, offset, &range_info->dirty_ts_);
        range_record->SetRangeInfo(std::move(range_info));
    }

    void SplitSamplePool(const RangeInfo *old_info)
    {
        TableName table_or_index_name(this->table_name_.StringView(),
                                      this->table_name_.IsBase()
                                          ? TableType::Primary
                                          : TableType::Secondary,
                                      this->table_name_.Engine());
        TableStatistics<KeyT> *statistics =
            static_cast<TableStatistics<KeyT> *>(
                table_schema_->StatisticsObject().get());
        if (statistics)
        {
            statistics->OnSplitSamplePool(
                shard_, this->cc_ng_id_, table_or_index_name, old_info);
        }
    }

    RangeBucketCcMap *bucket_ccm_{nullptr};
};
}  // namespace txservice
