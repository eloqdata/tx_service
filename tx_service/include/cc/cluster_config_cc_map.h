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

#include "cc_map.h"
#include "cluster_config_record.h"
#include "log.pb.h"
#include "template_cc_map.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_service.h"

namespace txservice
{
class ClusterConfigCcMap
    : public TemplateCcMap<VoidKey, ClusterConfigRecord, true, false>
{
public:
    ClusterConfigCcMap(const ClusterConfigCcMap &rhs) = delete;

    using TemplateCcMap<VoidKey, ClusterConfigRecord, true, false>::Execute;
    using TemplateCcMap<VoidKey, ClusterConfigRecord, true, false>::
        AcquireCceKeyLock;
    using TemplateCcMap<VoidKey, ClusterConfigRecord, true, false>::
        LockHandleForResumedRequest;
    using TemplateCcMap<VoidKey, ClusterConfigRecord, true, false>::neg_inf_;

    ClusterConfigCcMap(CcShard *shard,
                       NodeGroupId cc_ng_id,
                       uint64_t config_version)
        : TemplateCcMap<VoidKey, ClusterConfigRecord, true, false>(
              shard, cc_ng_id, cluster_config_ccm_name, 1, nullptr, true)
    {
        // We only store one record in ClusterConfigCcMap as neg_inf_ key. It is
        // is only used for concurrency control purpose.
        neg_inf_.payload_.PassInCurrentPayload(
            std::make_unique<ClusterConfigRecord>());
        assert(config_version > 0);
        neg_inf_.SetCommitTsPayloadStatus(config_version, RecordStatus::Normal);
    }

    bool Execute(AcquireAllCc &req) override
    {
        CcHandlerResult<AcquireAllResult> *hd_res = req.Result();
        // cluster config map is only stored on the first core so there's no
        // need to distribute the request to other cores. Reset the ref cnt
        // to 1.
        hd_res->ClearRefCnt();
        AcquireAllResult &acquire_all_result = hd_res->Value();
        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = req.NodeGroupTerm();
        assert(ng_term > 0);

        LockType acquired_lock = LockType::NoLock;
        CcErrorCode err_code = CcErrorCode::NO_ERROR;
        CcEntry<VoidKey, ClusterConfigRecord, true, false> *cce_ptr = nullptr;
        bool resume = false;
        if (req.CcePtr(shard_->core_id_) != nullptr)
        {
            // The request was blocked before and is now unblocked.
            resume = true;
            cce_ptr = static_cast<
                CcEntry<VoidKey, ClusterConfigRecord, true, false> *>(
                req.CcePtr(shard_->core_id_));
            std::tie(acquired_lock, err_code) =
                LockHandleForResumedRequest(cce_ptr,
                                            cce_ptr->CommitTs(),
                                            neg_inf_.PayloadStatus(),
                                            &req,
                                            ng_id,
                                            ng_term,
                                            req.TxTerm(),
                                            req.CcOp(),
                                            req.Isolation(),
                                            req.Protocol(),
                                            0,
                                            false);
        }
        else
        {
            // First time the request is processed in this shard. Or the request
            // is blocked previously because the cc map is full.
            // There's only one record in ClusterConfigCcMap, so we don't need
            // to worry about the key.
            req.SetCcePtr(&neg_inf_, shard_->core_id_);
        }

        // On execution resumption, the write lock has been acquired when
        // being unblocked.
        CcEntry<VoidKey, ClusterConfigRecord, true, false> &cc_entry = neg_inf_;
        if (!resume)
        {
            int64_t tx_term = req.TxTerm();
            IsolationLevel iso_lvl = req.Isolation();
            CcProtocol cc_proto = req.Protocol();
            CcOperation cc_op = req.CcOp();
            std::tie(acquired_lock, err_code) =
                AcquireCceKeyLock(&cc_entry,
                                  cc_entry.CommitTs(),
                                  &neg_inf_page_,
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
            // Updates last_vali_ts such that it is no smaller than (1) all
            // read transactions that have read the item in all shards, and
            // (2) the local time.
            acquire_all_result.last_vali_ts_ = shard_->LastReadTs();
            uint64_t lock_addr =
                cce_ptr == nullptr ? 0
                                   : reinterpret_cast<uint64_t>(
                                         cce_ptr->GetKeyGapLockAndExtraData());
            acquire_all_result.local_cce_addr_.SetCceLock(
                lock_addr, ng_term, req.NodeGroupId(), shard_->LocalCoreId());
            acquire_all_result.commit_ts_ = cc_entry.CommitTs();
            acquire_all_result.node_term_ = ng_term;

            // Cluster config map is only stored on the first core, so we don't
            // need to pass this request to other cores.
            hd_res->SetFinished();

            return true;
        }
        case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
        {
            // If the request comes from a remote node, sends
            // acknowledgement to the sender when the request is
            // blocked.
            if (!resume && !req.IsLocal())
            {
                remote::RemoteAcquireAll &remote_req =
                    static_cast<remote::RemoteAcquireAll &>(req);

                remote_req.SetCoreCnt(1);
                remote_req.TryAcknowledge(
                    ng_term,
                    ng_id,
                    shard_->core_id_,
                    reinterpret_cast<uint64_t>(cc_entry.GetLockAddr()));
            }

            return false;
        }
        default:
        {
            // lock confilct: back off and retry.
            req.Result()->SetError(err_code);
            return true;
        }
        }
    }

    bool Execute(PostWriteAllCc &req) override
    {
        if (!shard_->IsNative(req.NodeGroupId()))
        {
            // We only process cluster config update on preferred leader node
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        // The cluster config cc map has a single cc entry, which coordinates
        // cluster reconfiguration and all other operations.
        NonBlockingLock *lock = neg_inf_.GetKeyLock();

        if (req.CommitTs() == TransactionOperation::tx_op_failed_ts_)
        {
            // transaction failed before prepare log. Release lock and return.
            ReleaseCceLock(lock, &neg_inf_, req.Txn(), req.NodeGroupId());
            req.Result()->SetFinished();
            return true;
        }
        else if (req.CommitType() == PostWriteType::DowngradeLock)
        {
            // The request commits nothing. We downgrade write lock to
            // resolve deadlock issue
            DowngradeCceKeyWriteLock(&neg_inf_, req.Txn());
            req.Result()->SetFinished();
            return true;
        }

        assert(req.CommitType() == PostWriteType::Commit);

        ClusterConfigRecord *config_rec = nullptr;
        if (req.Key() != nullptr)
        {
            // request comes from same node.
            assert(req.Key() == VoidKey::NegativeInfinity());
            config_rec = static_cast<ClusterConfigRecord *>(req.Payload());
            ACTION_FAULT_INJECTOR("cluster_config_PostWriteAll_local");
        }
        else
        {
            assert(*req.KeyStrType() == KeyType::NegativeInf);
            req.SetTxKey(VoidKey::NegativeInfinity());
            assert(req.PayloadStr() != nullptr);
            std::unique_ptr<ClusterConfigRecord> decoded_rec =
                std::make_unique<ClusterConfigRecord>();
            size_t offset = 0;
            decoded_rec->Deserialize(req.PayloadStr()->c_str(), offset);
            config_rec = decoded_rec.get();
            req.SetDecodedPayload(std::move(decoded_rec));
            ACTION_FAULT_INJECTOR("cluster_config_PostWriteAll_remote");
        }
        // First we need to update cluster configs in Sharder.
        if (Sharder::Instance().ClusterConfigVersion() < req.CommitTs())
        {
            // async call to host manager to update ng is made. cc req will be
            // put back in queue once it's done.
            Sharder::Instance().UpdateClusterConfig(
                config_rec->GetNodeGroupConfigs(),
                req.CommitTs(),
                &req,
                this->shard_);
            return false;
        }

        // Lastly release/downgrade lock. We do not need to update the record in
        // cluster config cc map since it is always empty.
        LockType lk_type = LockType::NoLock;
        TxNumber txn = req.Txn();
        if (lock != nullptr)
        {
            // AcquireAllCc only acquire WriteIntent or WriteLock
            auto [write_lk_txn, write_lk_type] = lock->WriteTx();

            if (write_lk_type != NonBlockingLock::WriteLockType::NoWritelock &&
                write_lk_txn == txn)
            {
                lk_type =
                    write_lk_type == NonBlockingLock::WriteLockType::WriteLock
                        ? LockType::WriteLock
                        : LockType::WriteIntent;
            }
        }

        if (lk_type != LockType::NoLock)
        {
            neg_inf_.SetCommitTsPayloadStatus(req.CommitTs(),
                                              RecordStatus::Normal);
            ReleaseCceLock(lock, &neg_inf_, txn, req.NodeGroupId());
        }

        // No need to move the request to next core since this map is only
        // stored on core 0.
        req.Result()->SetFinished();
        req.SetDecodedPayload(nullptr);
        return true;
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
        shard_->SetBucketMigrating(true);
        const std::string_view &content = req.LogContentView();
        ::txlog::ClusterScaleOpMessage scale_op_msg;
        scale_op_msg.ParseFromArray(content.data(), content.length());
        TxNumber cluster_scale_txn = req.Txn();
        bool is_coordinator =
            ((cluster_scale_txn >> 32L) >> 10) == req.NodeGroupId();
        bool dm_started = false, dm_finished = true;

        auto &migrate_process =
            scale_op_msg.node_group_bucket_migrate_process();
        if (migrate_process.empty())
        {
            // If there's no data migrate process, we should treat it as
            // data migration is finished.
            dm_started = true;
            dm_finished = true;
        }
        else
        {
            // Check the data migrate process of each node group.
            for (auto &[ng_id, ng_process] : migrate_process)
            {
                // If any node group has started migration, we will replay from
                // the data migrate stage.
                dm_started =
                    ng_process.stage() !=
                        ::txlog::NodeGroupMigrateMessage_Stage_NotStarted ||
                    dm_started;

                if (ng_process.stage() !=
                    ::txlog::NodeGroupMigrateMessage_Stage_Cleaned)
                {
                    dm_finished = false;
                }
            }
        }

        bool locked = false, update_local_config = false;
        bool is_bucket_migrating = false;
        if (is_coordinator)
        {
            if (scale_op_msg.node_group_bucket_migrate_process_size() == 0)
            {
                // No bucket migration is needed.
                if (scale_op_msg.stage() ==
                    ::txlog::ClusterScaleStage::ConfigUpdate)
                {
                    locked = true;
                }
            }
            else if (scale_op_msg.event_type() ==
                     ::txlog::ClusterScaleOpMessage_ScaleOpType_AddNodeGroup)
            {
                // If we see cluster config log and data migration has not
                // started on any node group, we should replay from cluster
                // config update log.
                if (scale_op_msg.stage() ==
                        ::txlog::ClusterScaleStage::ConfigUpdate &&
                    !dm_started)
                {
                    locked = true;
                }
            }
            else if (scale_op_msg.event_type() ==
                     ::txlog::ClusterScaleOpMessage_ScaleOpType_RemoveNode)
            {
                // During remove node cluster config is done after data
                // migration. If we see config update log, that means we've
                // already acquired write lock on cluster config cc map.
                if (scale_op_msg.stage() ==
                    ::txlog::ClusterScaleStage::ConfigUpdate)
                {
                    locked = true;
                }
            }
            else if (scale_op_msg.event_type() ==
                     ::txlog::
                         ClusterScaleOpMessage_ScaleOpType_AddNodeGroupPeers)
            {
                // Add node group peers does not need to do bucket migration.
                assert(false);
            }
            else
            {
                assert(false);
            }

            if (scale_op_msg.stage() !=
                    ::txlog::ClusterScaleStage::CleanScale &&
                scale_op_msg.node_group_bucket_migrate_process_size() != 0)
            {
                is_bucket_migrating = true;
            }
        }
        else
        {
            // As coordinator we should recover the state of right before
            // writing the next log.
            if (scale_op_msg.event_type() ==
                ::txlog::ClusterScaleOpMessage_ScaleOpType_AddNodeGroup)
            {
                if (scale_op_msg.stage() ==
                    ::txlog::ClusterScaleStage::PrepareScale)
                {
                    // For add node if we only see prepare log, then recover to
                    // the state of before writing cluster config update log.
                    locked = true;
                }
                else if (scale_op_msg.stage() ==
                             ::txlog::ClusterScaleStage::ConfigUpdate &&
                         !dm_started)
                {
                    update_local_config = true;
                }

                if (!dm_finished)
                {
                    is_bucket_migrating = true;
                }
            }
            else if (scale_op_msg.event_type() ==
                     ::txlog::ClusterScaleOpMessage_ScaleOpType_RemoveNode)
            {
                if (scale_op_msg.stage() ==
                        ::txlog::ClusterScaleStage::PrepareScale &&
                    dm_finished)
                {
                    // For remove node, cluster config update is done after data
                    // migrate. We only need to recover the lock if data
                    // migration is finished.
                    locked = true;
                }
                else if (scale_op_msg.stage() ==
                         ::txlog::ClusterScaleStage::ConfigUpdate)
                {
                    update_local_config = true;
                }

                if (!update_local_config)
                {
                    is_bucket_migrating = true;
                }
            }
        }

        shard_->SetBucketMigrating(is_bucket_migrating);
        (void)
            is_bucket_migrating;  // Silence compiler warning in range partition
        if (locked)
        {
            auto lock_pair = AcquireCceKeyLock(&neg_inf_,
                                               neg_inf_.CommitTs(),
                                               &neg_inf_page_,
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
            // When a cc node recovers, no one should be holding read
            // locks. So, the acquire operation should always
            // succeed.
            assert(lock_pair.first == LockType::WriteLock &&
                   lock_pair.second == CcErrorCode::NO_ERROR);
            // This silences the -Wunused-but-set-variable warning without any
            // runtime overhead.
            (void) lock_pair;
        }

        if (update_local_config &&
            Sharder::Instance().ClusterConfigVersion() < req.CommitTs())
        {
            // Read new cluster config from log
            int ng_cnt = scale_op_msg.new_ng_configs_size();
            auto new_ng_configs_uptr = std::make_unique<
                std::unordered_map<NodeGroupId, std::vector<NodeConfig>>>();
            std::unordered_map<uint32_t, std::vector<NodeConfig>>
                &new_ng_configs = *new_ng_configs_uptr;
            std::unordered_map<uint32_t, NodeConfig> node_configs;
            for (int idx = 0; idx < scale_op_msg.node_configs_size(); idx++)
            {
                auto &node_config = scale_op_msg.node_configs(idx);
                node_configs.try_emplace(node_config.node_id(),
                                         node_config.node_id(),
                                         node_config.host_name(),
                                         node_config.port());
            }
            for (int ng_idx = 0; ng_idx < ng_cnt; ng_idx++)
            {
                int node_cnt =
                    scale_op_msg.new_ng_configs(ng_idx).member_nodes_size();
                int ng_id = scale_op_msg.new_ng_configs(ng_idx).ng_id();
                std::vector<NodeConfig> ng_nodes;
                for (int nidx = 0; nidx < node_cnt; nidx++)
                {
                    int member_nid =
                        scale_op_msg.new_ng_configs(ng_idx).member_nodes(nidx);
                    auto &member_node_msg = node_configs[member_nid];
                    ng_nodes.emplace_back(
                        member_node_msg.node_id_,
                        member_node_msg.host_name_,
                        member_node_msg.port_,
                        scale_op_msg.new_ng_configs(ng_idx).is_candidate(nidx));
                }
                new_ng_configs.try_emplace(ng_id, std::move(ng_nodes));
            }
            // This will update cluster config in sharder asynchronouslly
            // Temporarily cache the new_ng_configs in neg_inf_.payload_
            ClusterConfigRecord *config_rec =
                neg_inf_.payload_.cur_payload_.get();
            config_rec->SetNodeGroupConfigs(std::move(new_ng_configs_uptr));
            Sharder::Instance().UpdateClusterConfig(
                new_ng_configs, req.CommitTs(), &req, shard_);
            return false;
        }

        // Restore cluster scale tx if ng is coordinator
        if (is_coordinator)
        {
            LOG(INFO) << "Recovering cluster scale tx, txn: "
                      << cluster_scale_txn << ", term: " << tx_candidate_term;
            TxService *tx_service = shard_->local_shards_.GetTxService();
            TransactionExecution *txm = tx_service->NewTx();
            txm->SetRecoverTxState(
                cluster_scale_txn, tx_candidate_term, req.CommitTs());
            txm->RecoverClusterScale(scale_op_msg, dm_started, dm_finished);
        }
        neg_inf_.SetCommitTsPayloadStatus(
            Sharder::Instance().ClusterConfigVersion(), RecordStatus::Normal);

        req.SetFinish();
        return true;
    }
};
}  // namespace txservice
