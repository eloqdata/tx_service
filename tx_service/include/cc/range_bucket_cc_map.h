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

#include "cc_request.h"
#include "range_bucket_key_record.h"
#include "sharder.h"
#include "template_cc_map.h"
#include "tx_service.h"
#include "type.h"

namespace txservice
{
class RangeBucketCcMap
    : public TemplateCcMap<RangeBucketKey, RangeBucketRecord, true, false>
{
public:
    RangeBucketCcMap(const RangeBucketCcMap &rhs) = delete;
    ~RangeBucketCcMap() = default;

    using TemplateCcMap<RangeBucketKey, RangeBucketRecord, true, false>::
        Execute;
    using TemplateCcMap<RangeBucketKey, RangeBucketRecord, true, false>::
        FindEmplace;
    using TemplateCcMap<RangeBucketKey, RangeBucketRecord, true, false>::Find;
    using TemplateCcMap<RangeBucketKey, RangeBucketRecord, true, false>::
        AcquireCceKeyLock;
    using TemplateCcMap<RangeBucketKey, RangeBucketRecord, true, false>::
        LockHandleForResumedRequest;

    RangeBucketCcMap(CcShard *shard,
                     NodeGroupId cc_ng_id,
                     const TableName &table_name)
        : TemplateCcMap<RangeBucketKey, RangeBucketRecord, true, false>(
              shard, cc_ng_id, table_name, 1, nullptr, true)
    {
        auto bucket_map = shard->GetAllBucketInfos(cc_ng_id);
        assert(bucket_map != nullptr);

        // Fill bucket keys as slice items to batch load bucket records
        std::deque<SliceDataItem> slice_items;
        std::vector<RangeBucketKey> bucket_keys;
        bucket_keys.reserve(bucket_map->size());
        for (size_t i = 0; i < bucket_map->size(); ++i)
        {
            auto &bucket = bucket_map->at(i);
            bucket_keys.emplace_back(i);
            slice_items.emplace_back(
                TxKey(&bucket_keys.back()),
                std::make_unique<RangeBucketRecord>(bucket.get()),
                bucket->Version(),
                false);
        }
        BatchFillSlice(slice_items, true, 0, slice_items.size());
    }

    bool Execute(ReadCc &req) override
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"range_bucket_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append(std::to_string(req.TxTerm()));
            });
        TX_TRACE_DUMP(&req);

        assert(req.IsLocal());
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

        const RangeBucketKey *bucket_key =
            static_cast<const RangeBucketKey *>(req.Key());

        Iterator it = Find(*bucket_key);
        CcEntry<RangeBucketKey, RangeBucketRecord, true, false> *cce =
            it->second;
        CcPage<RangeBucketKey, RangeBucketRecord, true, false> *ccp =
            it.GetPage();
        assert(cce != nullptr && ccp != nullptr);
        auto hd_result = req.Result();
        LockType acquired_lock;
        CcErrorCode err_code;
        if (req.CcePtr() != nullptr && req.CcePtr() == cce)
        {
            // The request was blocked before. This is execution resumption
            // after the request is unblocked. The read lock/intention must have
            // been acquired.
            CcOperation cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                                 : CcOperation::Read;
            std::tie(acquired_lock, err_code) =
                LockHandleForResumedRequest(cce,
                                            cce->CommitTs(),
                                            cce->PayloadStatus(),
                                            &req,
                                            req.NodeGroupId(),
                                            ng_term,
                                            req.TxTerm(),
                                            cc_op,
                                            req.Isolation(),
                                            req.Protocol(),
                                            req.ReadTimestamp(),
                                            false);
        }
        else
        {
            // try to acquire lock
            int64_t tx_term = req.TxTerm();
            uint32_t ng_id = req.NodeGroupId();
            IsolationLevel iso_lvl = req.Isolation();
            CcProtocol cc_proto = req.Protocol();
            CcOperation cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                                 : CcOperation::Read;
            std::tie(acquired_lock, err_code) =
                AcquireCceKeyLock(cce,
                                  cce->CommitTs(),
                                  ccp,
                                  cce->PayloadStatus(),
                                  &req,
                                  ng_id,
                                  ng_term,
                                  tx_term,
                                  cc_op,
                                  iso_lvl,
                                  cc_proto,
                                  req.ReadTimestamp(),
                                  false);
        }

        CcEntryAddr &cce_addr = hd_result->Value().cce_addr_;
        assert(cce != nullptr);
        cce_addr.SetCceLock(
            reinterpret_cast<uint64_t>(cce->GetKeyGapLockAndExtraData()),
            ng_term,
            req.NodeGroupId(),
            shard_->LocalCoreId());

        // after acquiring lock
        switch (err_code)
        {
        case CcErrorCode::NO_ERROR:
        {
            CcEntryAddr &cce_addr = hd_result->Value().cce_addr_;
            cce_addr.SetCceLock(
                reinterpret_cast<uint64_t>(cce->GetKeyGapLockAndExtraData()),
                ng_term,
                req.NodeGroupId(),
                shard_->LocalCoreId());

            RangeBucketRecord *bucket_rec =
                static_cast<RangeBucketRecord *>(req.Record());
            *bucket_rec = *(cce->payload_.cur_payload_);
            hd_result->Value().ts_ = cce->CommitTs();
            hd_result->Value().rec_status_ = RecordStatus::Normal;
            hd_result->Value().lock_type_ = acquired_lock;
            hd_result->SetFinished();
            return true;
        }
        case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
        {
            // You don't need a remote acknowledge here, since bucket owner read
            // is a local read anyway. Set CcePtr to indicate this is a resumed
            // req.
            req.SetCcePtr(cce);
            return false;
        }
        default:
        {
            return true;
        }
        }  //-- end: switch

        return true;
    }

    bool Execute(PostWriteAllCc &req) override
    {
        RangeBucketRecord *upload_bucket_rec = nullptr;
        const RangeBucketKey *target_key = nullptr;

        assert(req.CommitTs() != 0);

        if (req.Key() != nullptr)
        {
            // Local request
            upload_bucket_rec = static_cast<RangeBucketRecord *>(req.Payload());
            target_key = static_cast<const RangeBucketKey *>(req.Key());
        }
        else
        {
            // Request comes from a remote node and is processed for the first
            // time. Deserialize the keys and payloads.
            assert(*req.KeyStrType() == KeyType::Normal);

            const std::string *key_str = req.KeyStr();
            assert(key_str != nullptr);
            std::unique_ptr<RangeBucketKey> decoded_key =
                std::make_unique<RangeBucketKey>();
            size_t offset = 0;
            decoded_key->Deserialize(key_str->data(), offset, KeySchema());
            target_key = decoded_key.get();
            req.SetDecodedKey(TxKey(std::move(decoded_key)));
            assert(req.PayloadStr() != nullptr);
            std::unique_ptr<RangeBucketRecord> decoded_rec =
                std::make_unique<RangeBucketRecord>();
            offset = 0;
            decoded_rec->Deserialize(req.PayloadStr()->data(), offset);
            upload_bucket_rec = decoded_rec.get();
            req.SetDecodedPayload(std::move(decoded_rec));
        }

        Iterator it = Find(*target_key);
        CcEntry<RangeBucketKey, RangeBucketRecord, true, false> *cce =
            it->second;
        assert(cce != nullptr);

        // Check whether cce key lock holder is the given tx of the
        // PostWriteAllCc before apply change.
        NonBlockingLock *lock = cce->GetKeyLock();
        if (lock == nullptr || !lock->HasWriteLock(req.Txn()))
        {
            // Check if the tx still has lock on this cce. If this
            // is a duplicate post write all req or this ng has already
            // failed over and has released the lock during recovery, skip
            // processing this request.
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
            if (shard_->core_id_ == 0)
            {
                // Update bucket info in local cc shards. Upload dirty bucket
                // owner.
                upload_bucket_rec->SetBucketInfo(
                    shard_->local_shards_.UploadNewBucketInfo(
                        this->cc_ng_id_,
                        target_key->bucket_id_,
                        upload_bucket_rec->GetBucketInfo()->DirtyBucketOwner(),
                        upload_bucket_rec->GetBucketInfo()->DirtyVersion()));
            }
        }
        else if (req.CommitType() == PostWriteType::PostCommit ||
                 req.CommitType() == PostWriteType::Commit)
        {
            if (shard_->core_id_ == 0)
            {
                // Commit dirty bucket info in local cc shards.
                const BucketInfo *bucket_info = shard_->GetBucketInfo(
                    target_key->bucket_id_, this->cc_ng_id_);
                assert(bucket_info != nullptr);
                if ((req.CommitType() == PostWriteType::PostCommit &&
                     bucket_info->DirtyVersion() > 0) ||
                    (req.CommitType() == PostWriteType::Commit &&
                     bucket_info->Version() <
                         upload_bucket_rec->GetBucketInfo()->Version()))
                {
                    // First time processing post write all. Commit the dirty
                    // version and drop store ranges if is old bucket owner.
                    NodeGroupId orig_owner = bucket_info->BucketOwner();
                    if (req.CommitType() == PostWriteType::PostCommit)
                    {
                        // Commit the dirty bucket info first so that store
                        // range can be loaded into memory on the dirty bucket
                        // owner ng.
                        bucket_info =
                            shard_->local_shards_.CommitDirtyBucketInfo(
                                this->cc_ng_id_, target_key->bucket_id_);
                    }
                    else
                    {
                        // This is a one phase commit, just overwrite with the
                        // passed in bucket info.
                        assert(req.CommitType() == PostWriteType::Commit);
                        bucket_info = shard_->local_shards_.UploadBucketInfo(
                            this->cc_ng_id_,
                            target_key->bucket_id_,
                            upload_bucket_rec->GetBucketInfo()->BucketOwner(),
                            upload_bucket_rec->GetBucketInfo()->Version());
                    }
                    if (this->cc_ng_id_ == orig_owner)
                    {
                        // If this ng is the original owner of the bucket,
                        // drop store ranges in this bucket since they are
                        // now migrated to other ng. Do this after dirty
                        // bucket info is committed to make sure all ranges
                        // are removed.
                        if (!shard_->local_shards_.DropStoreRangesInBucket(
                                this->cc_ng_id_, target_key->bucket_id_))
                        {
                            // If drop store range is blocked, retry later.
                            shard_->Enqueue(shard_->LocalCoreId(), &req);
                            return false;
                        }
                    }
                }
                else if (bucket_info->Version() ==
                             upload_bucket_rec->GetBucketInfo()->Version() &&
                         bucket_info->BucketOwner() != this->cc_ng_id_)
                {
                    // This is a resumed request that was blocked by drop store
                    // range. Retry.
                    if (!shard_->local_shards_.DropStoreRangesInBucket(
                            this->cc_ng_id_, target_key->bucket_id_))
                    {
                        shard_->Enqueue(shard_->LocalCoreId(), &req);
                        return false;
                    }
                }
                upload_bucket_rec->SetBucketInfo(bucket_info);
            }
        }
        else
        {
            assert(req.CommitType() == PostWriteType::DowngradeLock);
            // the request commits nothing and only downgrade the write
            // locks to write intent.
        }

        return TemplateCcMap::Execute(req);
    }

    bool Execute(ReplayLogCc &req) override
    {
        int64_t tx_candidate_term =
            Sharder::Instance().CandidateLeaderTerm(req.NodeGroupId());
        if (tx_candidate_term < 0)
        {
            // No longer candidate leader, stop immediately
            req.AbortCcRequest(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return false;
        }
        const std::string_view &content = req.LogContentView();
        ::txlog::ClusterScaleOpMessage scale_op_msg;
        scale_op_msg.ParseFromArray(content.data(), content.length());
        TxNumber cluster_scale_txn = req.Txn();

        // Restore bucket info
        auto &migrate_process =
            scale_op_msg.node_group_bucket_migrate_process();
        auto bucket_map = shard_->GetAllBucketInfos(req.NodeGroupId());

        // Only used on first core to restore migrate tx.
        // map from tx number to cur bucket each migrate tx is working on.
        std::unordered_map<TxNumber,
                           std::vector<const ::txlog::BucketMigrateMessage *>>
            migrate_tx_state;
        // list of buckets that haven't started migrating.
        std::vector<const ::txlog::BucketMigrateMessage *> pending_buckets;

        for (auto &[ng_id, ng_process] : migrate_process)
        {
            for (auto &[bucket_id, bucket_process] :
                 ng_process.bucket_migrate_process())
            {
                if (shard_->core_id_ == 0)
                {
                    BucketInfo *info = bucket_map->at(bucket_id).get();
                    // Update bucket owner on the first core
                    switch (bucket_process.stage())
                    {
                    case ::txlog::BucketMigrateStage::NotStarted:
                    {
                        info->bucket_owner_ = bucket_process.old_owner();
                        if (ng_id == req.NodeGroupId())
                        {
                            pending_buckets.push_back(&bucket_process);
                        }
                        break;
                    }
                    case ::txlog::BucketMigrateStage::BeforeLocking:
                    {
                        info->bucket_owner_ = bucket_process.old_owner();
                        if (ng_id == req.NodeGroupId())
                        {
                            // A worker tx has started on migrating this bucket,
                            // and might have acquired lock on other ngs, so we
                            // need to assign this bucket to the specified
                            // migration worker tx, since the acquired write
                            // lock on only be released by the same tx number.
                            auto tx_it = migrate_tx_state.try_emplace(
                                bucket_process.migration_txn());
                            tx_it.first->second.push_back(&bucket_process);
                        }
                        break;
                    }
                    case ::txlog::BucketMigrateStage::PrepareMigrate:
                    {
                        info->bucket_owner_ = bucket_process.old_owner();
                        assert(bucket_process.migrate_ts() > info->Version());
                        info->SetDirty(bucket_process.new_owner(),
                                       bucket_process.migrate_ts());
                        if (ng_id == req.NodeGroupId())
                        {
                            auto tx_it = migrate_tx_state.try_emplace(
                                bucket_process.migration_txn());
                            tx_it.first->second.push_back(&bucket_process);
                        }
                        break;
                    }
                    case ::txlog::BucketMigrateStage::CommitMigrate:
                    {
                        info->Set(bucket_process.new_owner(),
                                  bucket_process.migrate_ts());
                        if (ng_id == req.NodeGroupId())
                        {
                            auto tx_it = migrate_tx_state.try_emplace(
                                bucket_process.migration_txn());
                            tx_it.first->second.push_back(&bucket_process);
                        }
                        break;
                    }
                    case ::txlog::BucketMigrateStage::CleanMigrate:
                    {
                        info->Set(bucket_process.new_owner(),
                                  bucket_process.migrate_ts());
                        break;
                    }
                    default:
                    {
                        assert(false);
                    }
                    }
                }

                // Restore lock on bucket record
                LockType lock_type = LockType::NoLock;
                if (ng_id == req.NodeGroupId())
                {
                    // Coordinator of migrate tx. We need to restore the lock
                    // state just after the log is written.
                    if (bucket_process.stage() ==
                            ::txlog::BucketMigrateStage::PrepareMigrate ||
                        bucket_process.stage() ==
                            ::txlog::BucketMigrateStage::CommitMigrate)
                    {
                        lock_type = LockType::WriteLock;
                    }
                }
                else
                {
                    // Participant of migrate tx. We need to restore the lock
                    // state just before writing the next log.
                    if ((bucket_process.stage() ==
                         ::txlog::BucketMigrateStage::BeforeLocking) ||
                        bucket_process.stage() ==
                            ::txlog::BucketMigrateStage::PrepareMigrate)
                    {
                        lock_type = LockType::WriteLock;
                    }
                }

                if (lock_type == LockType::WriteLock)
                {
                    RangeBucketKey key(bucket_process.bucket_id());
                    Iterator it = Find(key);
                    auto bucket_cce = it->second;
                    CcPage<RangeBucketKey, RangeBucketRecord, true, false>
                        *bucket_ccp = it.GetPage();
                    assert(bucket_cce != nullptr && bucket_ccp != nullptr);
                    // We need to set the req txn to the data migrate txn so
                    // that the acquired lock records the correct lock owner tx.
                    req.ResetTxn(bucket_process.migration_txn());
                    assert(bucket_process.migration_txn() != 0);
                    auto lock_pair =
                        AcquireCceKeyLock(bucket_cce,
                                          bucket_cce->CommitTs(),
                                          bucket_ccp,
                                          RecordStatus::Normal,
                                          &req,
                                          req.NodeGroupId(),
                                          tx_candidate_term,
                                          0,
                                          CcOperation::Write,
                                          IsolationLevel::RepeatableRead,
                                          CcProtocol::Locking,
                                          0,
                                          false);
                    req.ResetTxn(cluster_scale_txn);
                    // When a cc node recovers, no one should be holding
                    // read
                    // locks. So, the acquire operation should always
                    // succeed.
                    assert(lock_pair.first == LockType::WriteLock &&
                           lock_pair.second == CcErrorCode::NO_ERROR);
                    // This silences the -Wunused-but-set-variable warning
                    // without any runtime overhead.
                    (void) lock_pair;
                }
            }
        }

        // Start migration worker tx if this node group is doing data migration
        if (shard_->core_id_ == 0)
        {
            auto ng_migrate_iter = migrate_process.find(req.NodeGroupId());
            if (ng_migrate_iter != migrate_process.end() &&
                ng_migrate_iter->second.stage() ==
                    ::txlog::NodeGroupMigrateMessage_Stage::
                        NodeGroupMigrateMessage_Stage_Prepared)
            {
                std::unordered_map<TxNumber, size_t>
                    migrate_tx_current_bucket_idx;
                std::vector<std::vector<uint16_t>> bucket_ids_per_task;
                std::vector<std::vector<NodeGroupId>> new_owner_ngs_per_task;
                std::vector<TxNumber> migrate_txns;
                // Put in progress buckets into todo bucket list first and
                // record the idx of which bucket each migrate tx is working on.
                for (int idx = 0;
                     idx < ng_migrate_iter->second.migration_txns_size();
                     idx++)
                {
                    TxNumber migrate_txn =
                        ng_migrate_iter->second.migration_txns(idx);
                    migrate_txns.push_back(migrate_txn);
                    if (migrate_tx_state.find(migrate_txn) !=
                        migrate_tx_state.end())
                    {
                        migrate_tx_current_bucket_idx.try_emplace(
                            migrate_txn, bucket_ids_per_task.size());
                        bucket_ids_per_task.emplace_back();
                        new_owner_ngs_per_task.emplace_back();
                        for (auto bucket_process :
                             migrate_tx_state[migrate_txn])
                        {
                            bucket_ids_per_task.back().push_back(
                                bucket_process->bucket_id());
                            new_owner_ngs_per_task.back().push_back(
                                bucket_process->new_owner());
                        }
                    }
                }
                // Idle migrate tx should fetch bucket from next_bucket_idx.
                size_t next_bucket_idx = bucket_ids_per_task.size();
                // Put all buckets on the same core to the same task.
                std::unordered_map<
                    uint16_t,
                    std::vector<const ::txlog::BucketMigrateMessage *>>
                    bucket_ids_per_core;
                for (size_t i = 0; i < pending_buckets.size(); i++)
                {
                    uint16_t core_id =
                        Sharder::Instance().ShardBucketIdToCoreIdx(
                            pending_buckets[i]->bucket_id());
                    auto core_tasks_it =
                        bucket_ids_per_core.try_emplace(core_id);
                    core_tasks_it.first->second.push_back(pending_buckets[i]);
                }

                // Put these pending tasks to the task list
                for (auto &[core_id, buckets] : bucket_ids_per_core)
                {
                    bucket_ids_per_task.emplace_back();
                    new_owner_ngs_per_task.emplace_back();
                    for (auto bucket_msg : buckets)
                    {
                        bucket_ids_per_task.back().push_back(
                            bucket_msg->bucket_id());
                        new_owner_ngs_per_task.back().push_back(
                            bucket_msg->new_owner());
                    }
                }

                std::shared_ptr<DataMigrationStatus> status =
                    std::make_shared<DataMigrationStatus>(
                        cluster_scale_txn,
                        std::move(bucket_ids_per_task),
                        std::move(new_owner_ngs_per_task),
                        std::move(migrate_txns));
                status->next_bucket_idx_ = next_bucket_idx;

                // Now we have all the infos needed, restore the migrate txs.
                TxService *tx_service = shard_->local_shards_.GetTxService();
                for (TxNumber migrate_txn : status->migration_txns_)
                {
                    const ::txlog::BucketMigrateMessage *migrate_msg;
                    uint64_t commit_ts;
                    size_t cur_bucket_idx;
                    auto msg_iter = migrate_tx_state.find(migrate_txn);
                    if (msg_iter == migrate_tx_state.end())
                    {
                        // this tx is not working on any bucket now. Use current
                        // clock as its ts and pass in empty stage. It will
                        // fetch the next pending bucket from status on start.
                        migrate_msg = nullptr;
                        commit_ts = shard_->local_shards_.ClockTs();
                        // cur idx is ignored if migrate_msg is null
                        cur_bucket_idx = 0;
                    }
                    else
                    {
                        // pass down the log message of the current bucket this
                        // migrate tx is working on.
                        migrate_msg = migrate_tx_state[migrate_txn][0];
                        // all buckets in this migrate tx should have the same
                        // commit ts.
                        commit_ts = migrate_msg->migrate_ts();
                        cur_bucket_idx =
                            migrate_tx_current_bucket_idx[migrate_txn];
                    }
                    LOG(INFO)
                        << "Recovering data migration tx, txn: " << migrate_txn
                        << ", term: " << tx_candidate_term;
                    TransactionExecution *txm = tx_service->NewTx();
                    txm->SetRecoverTxState(
                        migrate_txn, tx_candidate_term, commit_ts);
                    txm->RecoverDataMigration(
                        migrate_msg, cur_bucket_idx, status);
                }
            }
        }

        if (shard_->core_id_ != shard_->core_cnt_ - 1)
        {
            req.ResetCcm();
            MoveRequest(&req, shard_->core_id_ + 1);
        }
        else
        {
            req.SetFinish();
        }

        return false;
    }

    // Get bucket record of bucket id in cc map. This is used
    // when linking range record and range owner bucket record. It
    // should not be used for reading bucket record value.
    LruEntry *GetBucketRecord(uint16_t bucket_id)
    {
        RangeBucketKey bucket_key(bucket_id);
        return Find(bucket_key)->second;
    }
};
}  // namespace txservice
