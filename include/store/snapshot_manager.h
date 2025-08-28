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

#include <tx_worker_pool.h>

#include <atomic>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "store/data_store_handler.h"

namespace txservice
{

namespace store
{

// Now, standby and backup features only be enabled in EloqKV.

// used on primary node
class SnapshotManager
{
public:
    static SnapshotManager &Instance()
    {
        static SnapshotManager instance_;
        return instance_;
    }

    void Init(store::DataStoreHandler *store_hd)
    {
        store_hd_ = store_hd;
    }
    void Start();
    void Shutdown();

    void OnSnapshotSyncRequested(
        const txservice::remote::StorageSnapshotSyncRequest *req);

    txservice::remote::BackupTaskStatus CreateBackup(
        const txservice::remote::CreateBackupRequest *req);

    txservice::remote::BackupTaskStatus GetBackupStatus(
        txservice::NodeGroupId ng_id, const std::string &backup_name);

    void TerminateBackup(txservice::NodeGroupId ng_id,
                         const std::string &backup_name);

    // Run one round checkpoint to flush data in memory to kvstore.
    bool RunOneRoundCheckpoint(uint32_t node_group, int64_t ng_leader_term);

private:
    SnapshotManager() = default;
    ~SnapshotManager() = default;

    void StandbySyncWorker();
    void SyncWithStandby();

    void HandleBackupTask(txservice::remote::CreateBackupRequest *task);
    void UpdateBackupTaskStatus(
        txservice::remote::CreateBackupRequest *task,
        txservice::remote::BackupTaskStatus st,
        const std::vector<std::string> &snapshot_files = {},
        const uint64_t checkpoint_ts = 0);

    store::DataStoreHandler *store_hd_{nullptr};

    std::thread standby_sync_worker_;
    std::mutex standby_sync_mux_;
    std::condition_variable standby_sync_cv_;
    std::unordered_map<uint32_t, txservice::remote::StorageSnapshotSyncRequest>
        pending_req_;
    bool terminated_{false};

    const std::string backup_path_;
    std::mutex backup_tasks_mux_;
    // Queue of backup tasks whose status can be queried
    std::deque<txservice::remote::CreateBackupRequest *> backup_task_queue_;
    // Limit the max number of backup tasks in queue
    static const size_t MaxBackupTaskCount = 100;
    std::unordered_map<
        txservice::NodeGroupId,
        std::unordered_map<std::string, txservice::remote::CreateBackupRequest>>
        ng_backup_tasks_;

    txservice::TxWorkerPool backup_worker_{1};
};

}  // namespace store
}  // namespace txservice
