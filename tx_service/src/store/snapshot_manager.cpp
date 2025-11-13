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
#include "store/snapshot_manager.h"

#include <vector>

#include "cc/local_cc_shards.h"

namespace txservice
{

namespace store
{

void SnapshotManager::Start()
{
    standby_sync_worker_ = std::thread([this] { StandbySyncWorker(); });
}

void SnapshotManager::Shutdown()
{
    if (standby_sync_worker_.joinable())
    {
        std::unique_lock<std::mutex> lk(standby_sync_mux_);
        terminated_ = true;
        standby_sync_cv_.notify_all();
        lk.unlock();
        standby_sync_worker_.join();
    }
    backup_worker_.Shutdown();
}

void SnapshotManager::StandbySyncWorker()
{
    while (true)
    {
        std::unique_lock<std::mutex> lk(standby_sync_mux_);
        standby_sync_cv_.wait(
            lk, [this] { return !pending_req_.empty() || terminated_; });

        if (terminated_)
        {
            return;
        }
        assert(!pending_req_.empty());
        lk.unlock();
        SyncWithStandby();
        lk.lock();
    }
}

void SnapshotManager::OnSnapshotSyncRequested(
    const txservice::remote::StorageSnapshotSyncRequest *req)
{
    DLOG(INFO) << "Received snapshot sync request from standby node #"
               << req->standby_node_id()
               << " for standby term: " << req->standby_node_term();
    assert(store_hd_ != nullptr);
    if (store_hd_ == nullptr)
    {
        LOG(ERROR) << "Store handler is nullptr but standby feature enabled.";
        return;
    }

    std::unique_lock<std::mutex> lk(standby_sync_mux_);

    if (!terminated_)
    {
        auto ins_pair = pending_req_.try_emplace(req->standby_node_id());
        if (!ins_pair.second)
        {
            // check if the queued task is newer than the new received req. If
            // so, discard the new req, otherwise, update the task.
            auto &cur_task = ins_pair.first->second;
            int64_t cur_task_standby_node_term = cur_task.standby_node_term();
            int64_t req_standby_node_term = req->standby_node_term();

            if (cur_task_standby_node_term >= req_standby_node_term)
            {
                // discard the task.
                return;
            }
        }

        ins_pair.first->second.CopyFrom(*req);
        standby_sync_cv_.notify_all();
    }
}

// If kvstore is enabled, we must flush data in-memory to kvstore firstly.
// For non-shared kvstore, also we create and send the snapshot to standby
// nodes.
// Then, notify standby nodes that data committed before subscribe timepoint
// has been flushed to kvstore. (standby nodes begin fetch record from
// kvstore on cache miss).
void SnapshotManager::SyncWithStandby()
{
    assert(store_hd_ != nullptr);
    if (store_hd_ == nullptr)
    {
        LOG(ERROR) << "Store handler is nullptr but standby feature enabled.";
        return;
    }

    using namespace txservice;
    NodeGroupId node_group = Sharder::Instance().NativeNodeGroup();
    int64_t leader_term = Sharder::Instance().LeaderTerm(node_group);
    if (leader_term < 0)
    {
        std::unique_lock<std::mutex> lk(standby_sync_mux_);
        // clear all requests
        pending_req_.clear();
        return;
    }

    uint32_t current_subscribe_id = Sharder::Instance().GetCurrentSubscribeId();

    bool ckpt_res = this->RunOneRoundCheckpoint(node_group, leader_term);

    if (!ckpt_res)
    {
        // data flush failed. Retry on next run.
        LOG(ERROR) << "Failed to do checkpoint on SyncWithStandby";
        return;
    }

    // Now take a snapshot for non-shared storage, and then send to standby
    // node.
    std::vector<std::string> snapshot_files;
    if (!store_hd_->IsSharedStorage())
    {
        bool res = store_hd_->CreateSnapshotForStandby(snapshot_files);
        if (!res)
        {
            LOG(ERROR) << "Failed to create snpashot for sync with standby";
            return;
        }
    }

    std::vector<remote::StorageSnapshotSyncRequest> tasks;

    // Dequeue all pending tasks that can be covered by this snapshot.
    {
        std::unique_lock<std::mutex> lk(standby_sync_mux_);
        auto it = pending_req_.begin();
        while (it != pending_req_.end())
        {
            auto &pending_task = it->second;
            int64_t pending_task_standby_node_term =
                pending_task.standby_node_term();
            int64_t pending_task_primary_term =
                PrimaryTermFromStandbyTerm(pending_task_standby_node_term);

            if (pending_task_primary_term < leader_term)
            {
                // discard the task.
                it = pending_req_.erase(it);
                continue;
            }
            else if (pending_task_primary_term > leader_term)
            {
                // A new term is requested. Our current snapshot has expired.
                assert(!Sharder::Instance().CheckLeaderTerm(node_group,
                                                            leader_term));
                return;
            }

            bool covered = true;
            uint32_t pending_task_subscribe_id =
                SubscribeIdFromStandbyTerm(pending_task_standby_node_term);

            if (pending_task_subscribe_id > current_subscribe_id)
            {
                covered = false;
            }

            if (!covered)
            {
                // requested version is newer than cur snapshot. Wait till next
                // round.
                it++;
                continue;
            }
            tasks.emplace_back(std::move(pending_task));

            // Don't erase pending_req_
            // it = pending_req_.erase(it);
        }
    }

    for (auto &req : tasks)
    {
        uint32_t node_id = req.standby_node_id();
        std::string ip;
        uint16_t port;
        Sharder::Instance().GetNodeAddress(node_id, ip, port);
        std::string remote_dest = req.user() + "@" + ip + ":" + req.dest_path();
        int64_t req_standby_node_term = req.standby_node_term();
        int64_t req_primary_term =
            PrimaryTermFromStandbyTerm(req_standby_node_term);

        if (!Sharder::Instance().CheckLeaderTerm(req.ng_id(), req_primary_term))
        {
            // Abort immediately if term no longer match.
            return;
        }

        bool succ = true;
        if (!snapshot_files.empty())
        {
            succ = store_hd_->SendSnapshotToRemote(
                req.ng_id(), req_primary_term, snapshot_files, remote_dest);
        }

        if (succ)
        {
            auto channel = Sharder::Instance().GetCcNodeServiceChannel(
                req.standby_node_id());
            DLOG(INFO) << "Notifying standby node #" << req.standby_node_id()
                       << " for snapshot synced at term "
                       << req.standby_node_term()
                       << ", channel: " << (channel ? "valid" : "null");

            if (channel)
            {
                // needs retry if failed
                // since the standby node may be still spinning up.
                remote::CcRpcService_Stub stub(channel.get());
                int retry_times = 5;
                while (retry_times-- > 0)
                {
                    brpc::Controller cntl;
                    cntl.set_timeout_ms(1);
                    remote::OnSnapshotSyncedRequest on_synced_req;
                    remote::OnSnapshotSyncedResponse on_sync_resp;
                    on_synced_req.set_snapshot_path(req.dest_path());
                    on_synced_req.set_standby_node_term(
                        req.standby_node_term());
                    stub.OnSnapshotSynced(
                        &cntl, &on_synced_req, &on_sync_resp, nullptr);
                    if (cntl.Failed())
                    {
                        LOG(WARNING) << "OnSnapshotSynced to standby node #"
                                     << req.standby_node_id() << " failed, "
                                     << " error: " << cntl.ErrorText()
                                     << " error code: " << cntl.ErrorCode();
                        // sleep 1 second and retry
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                        continue;
                    }
                    else
                    {
                        break;
                    }
                }
            }

            // We don't care if the notification is successful or not.
            // We just need to erase the same request. Even if the notification
            // fails, after a while, the standby node will resend the
            // request.
            std::unique_lock<std::mutex> lk(standby_sync_mux_);
            auto pending_req_iter = pending_req_.find(req.standby_node_id());
            if (pending_req_iter != pending_req_.end())
            {
                // Check again to see if the request has been updated.
                auto &next_pending_task = pending_req_iter->second;
                int64_t next_pending_task_standby_term =
                    next_pending_task.standby_node_term();
                int64_t next_pending_task_primary_term =
                    PrimaryTermFromStandbyTerm(next_pending_task_standby_term);

                assert(PrimaryTermFromStandbyTerm(req.standby_node_term()) ==
                       leader_term);

                if (next_pending_task_primary_term < leader_term)
                {
                    pending_req_.erase(pending_req_iter);
                }
                else if (next_pending_task_primary_term == leader_term)
                {
                    uint32_t next_pending_task_subscribe_id =
                        SubscribeIdFromStandbyTerm(
                            next_pending_task_standby_term);
                    uint32_t cur_task_subscribe_id =
                        SubscribeIdFromStandbyTerm(req.standby_node_term());
                    if (next_pending_task_subscribe_id <= cur_task_subscribe_id)
                    {
                        pending_req_.erase(pending_req_iter);
                    }
                }
            }
        }
    }
}

bool SnapshotManager::RunOneRoundCheckpoint(uint32_t node_group,
                                            int64_t ng_leader_term)
{
    using namespace txservice;
    auto &local_shards = *Sharder::Instance().GetLocalCcShards();

    // Get table names in this node group, checkpointer should be TableName
    // string owner.
    // Use max number for ckpt ts to flush all in memory data to kv.
    std::unordered_map<TableName, bool> tables =
        local_shards.GetCatalogTableNameSnapshot(node_group, UINT64_MAX);

    std::shared_ptr<DataSyncStatus> data_sync_status =
        std::make_shared<DataSyncStatus>(node_group, ng_leader_term, true);
    data_sync_status->SetNoTruncateLog();

    bool can_be_skipped = false;
    uint64_t last_ckpt_ts = Sharder::Instance().GetNodeGroupCkptTs(node_group);
    size_t core_cnt = local_shards.Count();
    CkptTsCc ckpt_req(core_cnt, node_group);

    // Find minimum ckpt_ts from all the ccshards in parallel. ckpt_ts is
    // the minimum timestamp minus 1 among all the active transactions, thus
    // it's safe to flush all the entries smaller than or equal to ckpt_ts.
    for (size_t i = 0; i < local_shards.Count(); i++)
    {
        local_shards.EnqueueCcRequest(i, &ckpt_req);
    }
    ckpt_req.Wait();

    // Iterate all the tables and execute CkptScanCc requests on this node
    // group's ccmaps on each ccshard. The result of CkptScanCc is stored in
    // ckpt_vec.
    for (auto it = tables.begin(); it != tables.end(); ++it)
    {
        const TableName &table_name = it->first;
        bool is_dirty = it->second;
        // This should correspond to CcShard::ActiveTxMinTs.
        if (!table_name.IsMeta())
        {
            // Skip the table if it's not updated since last sync ts.
            GetTableLastCommitTsCc get_commit_ts_cc(
                table_name, node_group, local_shards.Count());
            for (size_t core = 0; core < local_shards.Count(); core++)
            {
                local_shards.EnqueueCcRequest(core, &get_commit_ts_cc);
            }
            get_commit_ts_cc.Wait();

            if (get_commit_ts_cc.LastCommitTs() < last_ckpt_ts)
            {
                continue;
            }

            uint64_t table_last_synced_ts = UINT64_MAX;
            local_shards.EnqueueDataSyncTaskForTable(table_name,
                                                     node_group,
                                                     ng_leader_term,
                                                     UINT64_MAX,
                                                     table_last_synced_ts,
                                                     false,
                                                     is_dirty,
                                                     can_be_skipped,
                                                     data_sync_status);
        }
    }

    std::unique_lock<std::mutex> task_sender_lk(data_sync_status->mux_);
    data_sync_status->all_task_started_ = true;

    // Waiting for the flush task to be completed.
    data_sync_status->cv_.wait(
        task_sender_lk,
        [&data_sync_status]
        { return data_sync_status->unfinished_tasks_ == 0; });

    return (data_sync_status->err_code_ == CcErrorCode::NO_ERROR);
}

void SnapshotManager::UpdateBackupTaskStatus(
    txservice::remote::CreateBackupRequest *task_ptr,
    txservice::remote::BackupTaskStatus st,
    const std::vector<std::string> &snapshot_files,
    const uint64_t checkpoint_ts)
{
    std::unique_lock<std::mutex> lk(backup_tasks_mux_);
    task_ptr->set_status(st);
    for (const auto &f : snapshot_files)
    {
        task_ptr->add_backup_files(f);
    }
    task_ptr->set_backup_ts(checkpoint_ts);
}

void SnapshotManager::HandleBackupTask(
    txservice::remote::CreateBackupRequest *task_ptr)
{
    using namespace txservice;
    uint32_t node_group = task_ptr->ng_id();
    int64_t leader_term = task_ptr->ng_term();
    const std::string backup_name = task_ptr->backup_name();
    assert(leader_term > 0);
    assert(store_hd_ != nullptr);

    if (!txservice::Sharder::Instance().CheckLeaderTerm(node_group,
                                                        leader_term))
    {
        std::unique_lock<std::mutex> lk(backup_tasks_mux_);
        task_ptr->set_status(txservice::remote::BackupTaskStatus::Failed);
        LOG(INFO) << "NodeGroup term changed, terminate to create "
                     "backup, name: "
                  << backup_name;
        return;
    }
    LOG(INFO) << "Begin to create backup, name: " << backup_name;

    {
        std::unique_lock<std::mutex> lk(backup_tasks_mux_);
        if (task_ptr->status() !=
            txservice::remote::BackupTaskStatus::Terminating)
        {
            task_ptr->set_status(txservice::remote::BackupTaskStatus::Running);
        }
        else
        {
            task_ptr->set_status(txservice::remote::BackupTaskStatus::Failed);
            LOG(INFO) << "Terminated backup, name: " << backup_name;
            return;
        }
    }

    bool ckpt_res = this->RunOneRoundCheckpoint(node_group, leader_term);
    if (!ckpt_res)
    {
        task_ptr->set_status(txservice::remote::BackupTaskStatus::Failed);
        LOG(INFO) << "Failed to do backup for checkpoint failed, name: "
                  << backup_name;

        return;
    }

    // Fetch last checkpoint ts of the recent checkpoint.
    uint64_t last_ckpt_ts = Sharder::Instance().GetNodeGroupCkptTs(node_group);

    {
        std::unique_lock<std::mutex> lk(backup_tasks_mux_);
        if (task_ptr->status() ==
            txservice::remote::BackupTaskStatus::Terminating)
        {
            task_ptr->set_status(txservice::remote::BackupTaskStatus::Failed);
            LOG(INFO) << "Terminated backup, name: " << backup_name;
            return;
        }
    }

    // Now take a snapshot for non-shared storage, and then send to dest path.
    // For shared storage, need user to call storage to create snapshot.
    std::vector<std::string> snapshot_files;

    if (store_hd_->IsSharedStorage())
    {
#if (defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) || \
     defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS))
        // For shared storage with cloud filesystem enabled, create snapshot
        bool res = store_hd_->CreateSnapshotForBackup(
            backup_name, snapshot_files, last_ckpt_ts);
        if (!res)
        {
            LOG(ERROR) << "Failed to create snapshot for backup in shared "
                          "storage mode";
            this->UpdateBackupTaskStatus(
                task_ptr, txservice::remote::BackupTaskStatus::Failed);
            store_hd_->RemoveBackupSnapshot(backup_name);
            return;
        }
        LOG(INFO) << "Backup finished with snapshot creation, name:"
                  << backup_name;
#else
        LOG(INFO) << "Backup finished, name:" << backup_name;
#endif

        this->UpdateBackupTaskStatus(
            task_ptr,
            txservice::remote::BackupTaskStatus::Finished,
            snapshot_files,
            last_ckpt_ts);
        return;
    }
    else
    {
        bool res = store_hd_->CreateSnapshotForBackup(
            backup_name, snapshot_files, last_ckpt_ts);
        if (!res)
        {
            LOG(ERROR) << "Failed to create snpashot for backup";
            this->UpdateBackupTaskStatus(
                task_ptr, txservice::remote::BackupTaskStatus::Failed);
            store_hd_->RemoveBackupSnapshot(backup_name);
            return;
        }
    }

    if (task_ptr->dest_path().empty())
    {
        this->UpdateBackupTaskStatus(
            task_ptr,
            txservice::remote::BackupTaskStatus::Finished,
            snapshot_files,
            last_ckpt_ts);
        LOG(INFO) << "Backup task is finished, backup name: " << backup_name
                  << ", dest_path is not set.";
        return;
    }

    assert(!snapshot_files.empty());
    if (snapshot_files.empty())
    {
        DLOG(INFO) << "snapshot_files is empty";
        this->UpdateBackupTaskStatus(
            task_ptr,
            txservice::remote::BackupTaskStatus::Finished,
            snapshot_files,
            last_ckpt_ts);
        return;
    }
    else
    {
        std::atomic<bool> succ{true};
        if (!Sharder::Instance().CheckLeaderTerm(node_group, leader_term))
        {
            // Abort immediately if term no longer match.
            succ.store(false, std::memory_order_relaxed);
            LOG(WARNING) << "Terminate backup task for leader term changed.";

            this->UpdateBackupTaskStatus(
                task_ptr, txservice::remote::BackupTaskStatus::Failed);
            store_hd_->RemoveBackupSnapshot(backup_name);

            return;
        }

        // Send the first snapshot file to remote node in order to create
        // non-existent directory. Otherwise, multiple rsync processes
        // creating a non-existent directory at the same time will result in
        // an error.

        std::string remote_dest;
        if (!task_ptr->dest_user().empty() && !task_ptr->dest_host().empty())
        {
            remote_dest = task_ptr->dest_user() + "@" + task_ptr->dest_host() +
                          ":" + task_ptr->dest_path();
        }
        else
        {
            remote_dest = task_ptr->dest_path();
        }
        if (remote_dest.back() != '/')
        {
            remote_dest.append("/");
        }
        remote_dest.append(std::to_string(node_group));
        remote_dest.append("/");

        bool send_result = store_hd_->SendSnapshotToRemote(
            node_group, leader_term, snapshot_files, remote_dest);
        if (send_result)
        {
            LOG(INFO) << "Backup task is finished, backup name: " << backup_name
                      << ", dest_path:" << remote_dest;
            this->UpdateBackupTaskStatus(
                task_ptr,
                txservice::remote::BackupTaskStatus::Finished,
                snapshot_files,
                last_ckpt_ts);
        }
        else
        {
            LOG(ERROR)
                << "Failed to send backup files to dest path, backup name: "
                << backup_name << ", dest_path:" << remote_dest;
            this->UpdateBackupTaskStatus(
                task_ptr, txservice::remote::BackupTaskStatus::Failed);
        }

        // remove local temporary store directory of backup files.
        bool remove_bucket_result =
            store_hd_->RemoveBackupSnapshot(backup_name);
        if (remove_bucket_result)
        {
            LOG(INFO) << "Removed local temporary store directory of "
                         "backup files , backup name:"
                      << backup_name;
        }
        else
        {
            LOG(ERROR) << "Failed to remove local temporary store directory of "
                          "backup files , backup name:"
                       << backup_name;
        }
    }
}

txservice::remote::BackupTaskStatus SnapshotManager::CreateBackup(
    const txservice::remote::CreateBackupRequest *req)
{
    if (store_hd_ == nullptr)
    {
        return txservice::remote::BackupTaskStatus::Failed;
    }

    txservice::remote::CreateBackupRequest *task_ptr;
    {
        std::unique_lock<std::mutex> lk(backup_tasks_mux_);

        if (backup_task_queue_.size() >= MaxBackupTaskCount)
        {
            auto tmp_status = backup_task_queue_.front()->status();
            if (tmp_status != txservice::remote::BackupTaskStatus::Failed &&
                tmp_status != txservice::remote::BackupTaskStatus::Finished)
            {
                return txservice::remote::BackupTaskStatus::Failed;
            }
            txservice::NodeGroupId ng_id = backup_task_queue_.front()->ng_id();
            const std::string &backup_name =
                backup_task_queue_.front()->backup_name();
            if (!store_hd_->IsSharedStorage())
            {
                store_hd_->RemoveBackupSnapshot(backup_name);
            }
            ng_backup_tasks_.at(ng_id).erase(backup_name);
            backup_task_queue_.pop_front();
        }

        txservice::NodeGroupId ng_id = req->ng_id();
        const std::string &backup_name = req->backup_name();
        auto ng_it = ng_backup_tasks_.find(ng_id);
        if (ng_it != ng_backup_tasks_.end())
        {
            auto backup_it = ng_it->second.find(backup_name);
            if (backup_it != ng_it->second.end())
            {
                assert(false);
                return txservice::remote::BackupTaskStatus::Failed;
            }
        }
        else
        {
            ng_backup_tasks_.try_emplace(ng_id);
        }

        auto ins_pair = ng_backup_tasks_.at(ng_id).try_emplace(backup_name);
        assert(ins_pair.second);
        auto &task = ins_pair.first->second;
        backup_task_queue_.push_back(&task);
        task.CopyFrom(*req);
        task.set_status(txservice::remote::BackupTaskStatus::Inited);
        task_ptr = &task;

        int64_t ng_term =
            txservice::Sharder::Instance().LeaderTerm(task.ng_id());
        task.set_ng_term(ng_term);
    }

    backup_worker_.SubmitWork([this, task_ptr](size_t)
                              { HandleBackupTask(task_ptr); });
    return txservice::remote::BackupTaskStatus::Inited;
}

txservice::remote::BackupTaskStatus SnapshotManager::GetBackupStatus(
    txservice::NodeGroupId ng_id,
    const std::string &backup_name,
    ::txservice::remote::FetchBackupResponse *response)
{
    if (store_hd_ == nullptr)
    {
        return txservice::remote::BackupTaskStatus::Failed;
    }

    std::unique_lock<std::mutex> lk(backup_tasks_mux_);
    auto ng_it = ng_backup_tasks_.find(ng_id);
    if (ng_it != ng_backup_tasks_.end())
    {
        auto backup_it = ng_it->second.find(backup_name);
        if (backup_it != ng_it->second.end())
        {
            auto backup_status = backup_it->second.status();
            if (backup_status == txservice::remote::BackupTaskStatus::Finished)
            {
                for (const auto &f : backup_it->second.backup_files())
                {
                    response->add_backup_files(f);
                }
                response->set_backup_ts(backup_it->second.backup_ts());
            }

            return backup_status;
        }
    }

    return txservice::remote::BackupTaskStatus::Unknown;
}

void SnapshotManager::TerminateBackup(txservice::NodeGroupId ng_id,
                                      const std::string &backup_name)
{
    if (store_hd_ == nullptr)
    {
        return;
    }
    using namespace txservice::remote;
    std::unique_lock<std::mutex> lk(backup_tasks_mux_);
    auto ng_it = ng_backup_tasks_.find(ng_id);
    if (ng_it != ng_backup_tasks_.end())
    {
        auto backup_it = ng_it->second.find(backup_name);
        if (backup_it != ng_it->second.end() &&
            backup_it->second.status() != BackupTaskStatus::Failed &&
            backup_it->second.status() != BackupTaskStatus::Finished)
        {
            backup_it->second.set_status(BackupTaskStatus::Terminating);
        }
    }
}

}  // namespace store
}  // namespace txservice
