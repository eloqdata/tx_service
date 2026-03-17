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
#include <unordered_set>
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

    // Handle snapshot sync request from standby node. Returns true if the
    // request is accepted (queued or safely deduped), false otherwise.
    bool OnSnapshotSyncRequested(
        const txservice::remote::StorageSnapshotSyncRequest *req);

    /**
     * @brief Register the active-tx barrier captured at standby subscription
     * time.
     * @param standby_node_id Standby's node id.
     * @param standby_node_term Standby's term ((primary_term << 32) |
     * subscribe_id).
     * @param active_tx_max_ts Maximum active write-tx timestamp observed at
     * subscription time.
     */
    void RegisterSubscriptionBarrier(uint32_t standby_node_id,
                                     int64_t standby_node_term,
                                     uint64_t active_tx_max_ts);

    /**
     * @brief Lookup subscription barrier by standby node id and standby term.
     * @param standby_node_id Standby's node id.
     * @param standby_node_term Standby's term ((primary_term << 32) |
     * subscribe_id).
     * @param active_tx_max_ts Output barrier timestamp if found.
     * @return true if barrier exists, false otherwise.
     */
    bool GetSubscriptionBarrier(uint32_t standby_node_id,
                                int64_t standby_node_term,
                                uint64_t *active_tx_max_ts);

    /**
     * @brief Remove an existing subscription barrier.
     * @param standby_node_id Standby's node id.
     * @param standby_node_term Standby's term ((primary_term << 32) |
     * subscribe_id).
     */
    void EraseSubscriptionBarrier(uint32_t standby_node_id,
                                  int64_t standby_node_term);

    /**
     * @brief Remove all subscription barriers and queued sync task for one
     * standby node.
     * @param standby_node_id Standby's node id.
     */
    void EraseSubscriptionBarriersByNode(uint32_t standby_node_id);

    txservice::remote::BackupTaskStatus CreateBackup(
        const txservice::remote::CreateBackupRequest *req);

    txservice::remote::BackupTaskStatus GetBackupStatus(
        txservice::NodeGroupId ng_id,
        const std::string &backup_name,
        ::txservice::remote::FetchBackupResponse *response);

    void TerminateBackup(txservice::NodeGroupId ng_id,
                         const std::string &backup_name);

    // Run one round checkpoint to flush data in memory to kvstore.
    bool RunOneRoundCheckpoint(uint32_t node_group, int64_t ng_leader_term);

    // Collect current checkpoint ts from cc shards without triggering data
    // flush.
    uint64_t GetCurrentCheckpointTs(uint32_t node_group);

private:
    /**
     * @brief Remove one subscription barrier entry while caller already holds
     * standby_sync_mux_.
     */
    void EraseSubscriptionBarrierLocked(uint32_t standby_node_id,
                                        int64_t standby_node_term);
    bool IsSnapshotSyncCompletedLocked(uint32_t standby_node_id,
                                       int64_t standby_node_term) const;
    void MarkSnapshotSyncCompletedLocked(uint32_t standby_node_id,
                                         int64_t standby_node_term);
    void EraseSnapshotSyncCompletedByNodeLocked(uint32_t standby_node_id);

    struct PendingSnapshotSyncTask
    {
        txservice::remote::StorageSnapshotSyncRequest req;
        uint64_t subscription_active_tx_max_ts{0};
    };

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
    std::unordered_map<uint32_t, PendingSnapshotSyncTask> pending_req_;
    // standby node id -> (standby node term -> subscription-time active tx
    // max ts)
    std::unordered_map<uint32_t, std::unordered_map<int64_t, uint64_t>>
        subscription_barrier_;
    // standby node id -> completed standby terms
    std::unordered_map<uint32_t, std::unordered_set<int64_t>>
        completed_snapshot_terms_;
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

    txservice::TxWorkerPool backup_worker_{"tx_snapbk", 1};
};

}  // namespace store
}  // namespace txservice
