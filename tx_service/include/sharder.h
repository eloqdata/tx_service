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

#include <brpc/channel.h>
#include <bthread/moodycamelqueue.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "brpc/server.h"
#include "butil/third_party/murmurhash3/murmurhash3.h"
#include "proto/cc_request.pb.h"
#include "tx_serialize.h"
#include "txlog.h"
#include "type.h"

namespace txservice
{
#define GET_CCNODE_RPC_PORT(port) port + 1
#define GET_LOG_REPLAY_RPC_PORT(port) port + 3

class LocalCcShards;
class TxLog;
class TxWorkerPool;
struct TableName;
struct CcRequestBase;
class CcShard;
class Checkpointer;

namespace store
{
class DataStoreHandler;
}

namespace fault
{
class CcNode;
class RecoveryService;
}  // namespace fault

namespace remote
{
class CcNodeService;
class CcStreamSender;
class CcStreamReceiver;
}  // namespace remote
struct NodeConfig
{
public:
    NodeConfig() = default;
    NodeConfig(uint32_t node_id,
               const std::string &host_name,
               uint16_t port,
               bool is_candidate = false)
        : node_id_(node_id),
          host_name_(host_name),
          port_(port),
          is_candidate_(is_candidate)
    {
    }

    NodeConfig(const NodeConfig &rhs)
        : node_id_(rhs.node_id_),
          host_name_(rhs.host_name_),
          port_(rhs.port_),
          is_candidate_(rhs.is_candidate_)
    {
    }

    void Serialize(std::string &buf) const
    {
        SerializeToStr(&node_id_, buf);
        Serializer<std::string>::Serialize(host_name_, buf);
        SerializeToStr(&port_, buf);
        SerializeToStr(&is_candidate_, buf);
    }

    size_t SerializedLength() const
    {
        return sizeof(uint32_t) + sizeof(uint16_t) + host_name_.length() +
               sizeof(uint16_t) + sizeof(bool);
    }

    void Deserialize(const char *buf, size_t &offset)
    {
        DesrializeFrom(buf, offset, &node_id_);
        host_name_ = Serializer<std::string>::Deserialize(buf, offset);
        DesrializeFrom(buf, offset, &port_);
        DesrializeFrom(buf, offset, &is_candidate_);
    }

    uint32_t node_id_{UINT32_MAX};
    std::string host_name_{""};
    uint16_t port_{0};
    // Identify this is a candidate of node_group.
    bool is_candidate_{false};
};
struct ClusterConfig
{
    ClusterConfig() = default;
    ClusterConfig(const ClusterConfig &rhs) : version_(rhs.version_)
    {
        for (auto &pair : rhs.ng_configs_)
        {
            ng_configs_.emplace(pair.first, pair.second);
        }
        for (auto &pair : rhs.cc_nodes_)
        {
            cc_nodes_.emplace(pair.first, pair.second);
        }
    }

    ClusterConfig &operator&=(ClusterConfig &&rhs)
    {
        version_ = rhs.version_;
        ng_configs_ = std::move(rhs.ng_configs_);
        cc_nodes_ = std::move(rhs.cc_nodes_);

        return *this;
    }

    std::unordered_map<NodeGroupId, std::vector<NodeConfig>> ng_configs_;
    std::unordered_map<NodeGroupId, std::shared_ptr<fault::CcNode>> cc_nodes_;
    uint64_t version_{0};
    // These two variables is used frequently, so we copy shared pointer
    // instead of copying objects.
    std::shared_ptr<std::set<NodeGroupId>> ng_ids_;
    std::shared_ptr<std::unordered_map<NodeId, NodeConfig>> nodes_configs_;
};

/**
 * Sharder is the collection of services supplied by TxService, which includes:
 * 1. cc_stream rpc service, which transfers CcRequests between different
 * TxService nodes.
 * 2. cc_node rpc service, which is used by raft protocol to communicate CcNodes
 * among the raft group.
 * 3. log_replay rpc service, which receives and replay redo log from log
 * service by streaming.
 *
 * Sharder also specifies the hash function which shards the key to the
 * corresponding TxProcessor.
 */
class Sharder
{
public:
    static Sharder &Instance(
        uint32_t node_id = 0,
        const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
            *ng_configs = nullptr,
        uint64_t config_version = 0,
        const std::vector<std::string> *txlog_ips = nullptr,
        const std::vector<uint16_t> *txlog_ports = nullptr,
        LocalCcShards *local_shards = nullptr,
        std::unique_ptr<TxLog> log_agent = nullptr,
        const std::string *local_path = nullptr)
    {
        static Sharder instance_;
        return instance_;
    }

    void Shutdown();
    void CloseStreamSender();

    /**
     * @brief Returns the ID of the leader node of the input cc node group.
     *
     * @param cc_ng_id The cc node group ID.
     * @return uint32_t The ID of the leader node.
     */
    uint32_t LeaderNodeId(uint32_t cc_ng_id)
    {
        return ng_leader_cache_[cc_ng_id].load(std::memory_order_relaxed);
    }

    uint16_t ShardBucketIdToCoreIdx(uint16_t bucket_id);

    uint32_t ShardToCcNodeGroup(uint32_t sharding_code)
    {
        return sharding_code >> 10;
    }

    static inline uint16_t MapRangeIdToBucketId(int32_t range_id)
    {
        uint32_t hash_val;
        butil::MurmurHash3_x86_32(&range_id, sizeof(range_id), 9001, &hash_val);
        return hash_val % total_range_buckets;
    }

    static inline uint16_t MapKeyHashToBucketId(uint64_t hash_code)
    {
        uint16_t slot_id = hash_code & 0x3FFF;
        uint16_t bucket_id = slot_id % total_range_buckets;
        return bucket_id;
    }

    static inline int32_t MapKeyHashToHashPartitionId(uint64_t hash_code)
    {
        return static_cast<int32_t>(hash_code % total_hash_partitions);
    }

    static inline uint16_t TotalRangeBuckets()
    {
        return total_range_buckets;
    }

    static inline int32_t MapBucketIdToHashPartitionId(uint16_t bucket_id)
    {
        assert(total_hash_partitions <= total_range_buckets);
        return bucket_id % total_hash_partitions;
    }

    static inline uint32_t MapHashPartitionIdToBucketId(int32_t partition_id)
    {
        return static_cast<uint32_t>(partition_id % total_range_buckets);
    }

    uint32_t NativeNodeGroup() const
    {
        return native_ng_;
    }

    uint32_t NodeGroupCount()
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        return cluster_config_.ng_configs_.size();
    }

    std::shared_ptr<std::set<uint32_t>> AllNodeGroups()
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        return cluster_config_.ng_ids_;
    }

    std::unordered_map<uint32_t, std::vector<NodeConfig>> GetNodeGroupConfigs()
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        return cluster_config_.ng_configs_;
    }

    uint32_t NodeId() const
    {
        return node_id_;
    }

    uint32_t GetNodeCount()
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        return cluster_config_.nodes_configs_->size();
    }

    std::shared_ptr<std::unordered_map<uint32_t, NodeConfig>>
    GetAllNodesConfigs()
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        return cluster_config_.nodes_configs_;
    }

    /**
     * @brief Gets the ip and port of the input node. Result is saved in ip and
     * port. If the node id is not found in cluster, set ip as empty string.
     *
     */
    void GetNodeAddress(uint32_t node_id, std::string &ip, uint16_t &port);

    /**
     * @brief Initializes cc nodes in this node and starts network services.
     * Given a node's port number local_port, the cc stream service is on
     * local_port. The raft and the cc rpc services are on local_port+1. The log
     * group is on local_port+2. The replay service is on local_port+3.
     *
     * @return int Error code.
     */
    int Init(uint32_t node_id,
             uint32_t ng_id,
             const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
                 *ng_configs,
             uint64_t config_version,
             const std::vector<std::string> *txlog_ips,
             const std::vector<uint16_t> *txlog_ports,
             const std::string *hm_ip,
             const uint16_t *hm_port,
             const std::string *hm_bin_path,
             LocalCcShards *local_shards,
             std::unique_ptr<TxLog> log_agent,
             const std::string &local_path,
             const std::string &cluster_config_path,
             const uint16_t rep_group_cnt,
             bool fork_host_manager);

    /**
     * @brief Checks if the current leader of the input cc node group is on the
     * same term as the expected one.
     *
     * @param ng_id The cc node group ID.
     * @param term The expected leader term.
     * @param check_candidate Whether to check the candidate leader term.
     * @return true, if the current leader is the expected one.
     * @return false, otherwise.
     */
    bool CheckLeaderTerm(uint32_t ng_id,
                         int64_t term,
                         bool check_candidate = false) const;

    void SetLeaderTerm(NodeGroupId ng_id, int64_t term)
    {
        leader_term_cache_[ng_id].store(term, std::memory_order_release);
    }

    void SetCandidateTerm(NodeGroupId ng_id, int64_t term)
    {
        candidate_leader_term_cache_[ng_id].store(term,
                                                  std::memory_order_release);
    }

    /**
     * @brief The term of the leader of the input cc node group.
     *
     * @param ng_id The cc node group ID.
     * @return int64_t The leader's term.
     */
    int64_t LeaderTerm(uint32_t ng_id) const;

    int64_t CandidateLeaderTerm(uint32_t ng_id) const;

    /**
     * @brief Updates the leader cache of all cc node groups.
     *
     */
    void UpdateLeaders();

    /**
     * @brief When a remote cc node group fails over, this node is unaware of
     * the new leader. Its cc requests are directed to a non-leader node and
     * return with errors. The method updates the leader of the input cc node
     * group in the local cache.
     *
     * @param ng_id The cc node group ID.
     */
    void UpdateLeader(uint32_t ng_id);

    /**
     * @brief NotifyNewLeaderStart rpc will send the node_id of the new leader
     * to each node. Update the leader cache without referring to hm service.
     *
     * @param ng_id The cc node group ID.
     * @param node_id The node_id of leader.
     * @param term The node group leader term.
     */
    void UpdateLeader(uint32_t ng_id, uint32_t node_id, int64_t term = -1);

    bool OnLeaderStart(uint32_t ng_id,
                       int64_t term,
                       uint64_t &replay_start_ts,
                       bool &retry,
                       uint32_t *next_leader_node = nullptr);

    bool OnLeaderStop(uint32_t ng_id, int64_t term);

    bool Failover(const std::string &target_host,
                  const uint16_t target_port,
                  std::string &error_message);

    void OnStartFollowing(uint32_t ng_id,
                          int64_t term,
                          uint32_t leader_node,
                          bool resubscribe = false);

    bool OnSnapshotReceived(const remote::OnSnapshotSyncedRequest *req);

    /**
     * @brief Update the log group's leader node id when the log group leader
     * changed.
     *
     * @param lg_id The log group ID.
     * @param node_id The node_id of leader in log group.
     */
    void UpdateLogGroupLeader(uint32_t lg_id, uint32_t node_id)
    {
        log_agent_->UpdateLeaderCache(lg_id, node_id);
    }

    /**
     * Whether this node is preferred node group's leader. If not, should
     * request leader transfer.
     * @return
     */
    bool IsPreferredGroupLeader()
    {
        return CandidateLeaderTerm(node_id_) > 0 || LeaderTerm(node_id_) > 0;
    }

    TxLog *GetLogAgent() const
    {
        return log_agent_.get();
    }

    /**
     * @brief Updates the specified cc node's status, when it is recovering as
     * the leader and has received all log records from the specifid log group.
     * The cc node starts serving as the leader, after receiving committed log
     * records from all log groups.
     *
     * @param cc_ng_id The cc node group ID.
     * @param cc_ng_term The cc node's term.
     * @param log_group_id The ID of the log group from which committed log
     * records have been shipped.
     * @param latest_txn_no The latest txn number committed from cc node group
     * cc_ng_id, valid only when cc node group cc_ng_id is bound to log group
     * log_group_id, otherwise it should be 0.
     * @param latest_commit_ts The latest commit ts the log group has ever seen,
     * used to update each ccshard's ts_base_ (last_read_ts).
     */
    void FinishLogReplay(uint32_t cc_ng_id,
                         int64_t cc_ng_term,
                         uint32_t log_group_id,
                         uint32_t latest_txn_no,
                         uint64_t latest_commit_ts,
                         uint64_t last_ckpt_ts);

    bool CheckLogGroupReplayFinished(uint32_t cc_ng_id,
                                     uint32_t log_group_id,
                                     int64_t cc_ng_term);

    /**
     * @brief Wait for all the tx_service nodes to finish the log recovery
     * process and setup the cc_stream_sender.
     *
     */
    void WaitClusterReady();

    /**
     * @brief Wait for the node become native node group leader.
     */
    void WaitNodeBecomeNativeGroupLeader();

    /**
     * @brief Recovers the input orphan lock held for an extended period of
     * time.
     *
     * @param lock_tx_number The number of the tx who holds the lock.
     * @param lock_tx_coord_term The term of the tx coordinator node when
     * the lock was acquired.
     * @param lock_cc_ng_id The cc node group in which the lock resides.
     * @param lock_cc_ng_term The term of the cc node group of the lock.
     */
    void RecoverTx(uint64_t lock_tx_number,
                   int64_t lock_tx_coord_term,
                   uint64_t write_lock_ts,
                   uint32_t lock_cc_ng_id,
                   int64_t lock_cc_ng_term);
    /**
     * @brief Transfer the leader for the log group. This function is developed
     * for test
     *
     * @param log_group_id log group id that need to transfer leader
     * @param leader_idx the node index from 0 in log group for new leader
     */
    void LogTransferLeader(uint32_t log_group_id, uint32_t leader_idx);

    remote::CcStreamSender *GetCcStreamSender()
    {
        return cc_stream_sender_ != nullptr ? cc_stream_sender_.get() : nullptr;
    }

    void NodeGroupFinishRecovery(uint32_t ng_id)
    {
        std::lock_guard<std::mutex> lk(recovery_state_mux_);

        recovered_leader_set_.emplace(ng_id);
        recovery_state_cv_.notify_one();
    }

    LocalCcShards *GetLocalCcShards()
    {
        return local_shards_;
    }

    store::DataStoreHandler *GetDataStoreHandler();

    CcShard *GetCcShard(size_t core_idx);

    void CleanCcTable(const TableName &tabname);

    void NotifyCheckPointer();

    Checkpointer *GetCheckpointer();

    std::vector<uint32_t> LocalNodeGroups();

    /**
     * Try to pin data of cc_ng_id if it is group leader
     * @param cc_ng_id
     * @return leader term of cc_ng_id, -1 if not found
     */
    int64_t TryPinNodeGroupData(uint32_t cc_ng_id);

    int64_t TryPinStandbyNodeGroupData();

    /**
     * Unpin data of cc_ng_id, clear ccmaps and catalogs if this node is no
     * longer group leader and pinning threads number decreases to 0.
     * Must be called in pair with TryPinNodeGroupData if TryPinNodeGroupData
     * returns success, otherwise OnLeaderStop will be blocked
     * forever.
     * @param cc_ng_id
     */
    void UnpinNodeGroupData(uint32_t cc_ng_id);

    uint64_t GetNodeGroupCkptTs(uint32_t cc_ng_id);

    uint64_t NativeNodeGroupCkptTs();

    bool UpdateNodeGroupCkptTs(uint32_t cc_ng_id, uint64_t ckpt_ts);

    TxWorkerPool *GetTxWorkerPool()
    {
        return tx_worker_pool_.get();
    }

    size_t GetLocalCcShardsCount();

    /**
     * @brief Calculate new node group config after adding new nodes to current
     * cluster.
     * @return New cluster node group configs.
     */
    std::unordered_map<uint32_t, std::vector<NodeConfig>> AddNodeGroupToCluster(
        const std::vector<std::pair<std::string, uint16_t>> &new_nodes);

    /**
     * @brief Calculate new node group config after removing nodes from current
     * cluster.
     * @return New cluster node group configs.
     */
    std::unordered_map<uint32_t, std::vector<NodeConfig>> RemoveNodeFromCluster(
        const std::vector<std::pair<std::string, uint16_t>> &removed_nodes);

    /**
     * @brief Calculate new node group config after adding new peers to
     * specified node group.
     * @return New cluster node group configs.
     */
    std::unordered_map<uint32_t, std::vector<NodeConfig>>
    AddNodeGroupPeersToCluster(
        uint32_t ng_id,
        const std::vector<std::pair<std::string, uint16_t>> &new_nodes,
        const std::vector<bool> &is_candidate);

    /**
     * @brief Update current cluster config to the new_ng_configs. The
     * config will only be updated if current config version is older than
     * given version. This is an async call and cc_req will be reenqueued
     * when the update finishes.
     */
    void UpdateClusterConfig(
        const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
            &new_ng_configs,
        uint64_t version,
        CcRequestBase *cc_req,
        CcShard *cc_shard);

    /**
     * @brief Update the in-memory cluster config in sharder.
     */
    void UpdateInMemoryClusterConfig(
        const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
            &new_ng_configs,
        std::shared_ptr<std::unordered_map<uint32_t, NodeConfig>>
            new_nodes_sptr,
        uint64_t version);

    uint64_t ClusterConfigVersion()
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        return cluster_config_.version_;
    }

    /**
     * @brief Should accept the cc requests from remote node after the
     * TxProcessor thread start, so, should start cc stream server after the
     * txservice::Start().
     */
    void StartCcStreamReceiver();

    /**
     * @brief Connect cc stream sender to remote nodes
     */
    void ConnectCcStreamSender();

    std::shared_ptr<brpc::Channel> GetCcNodeServiceChannel(uint32_t node_id);

    /**
     * Update cc node service channel if the current cached channel equals
     * old_channel. If current cached channel != old_channel, that means it's
     * already updated by someone else.
     */
    std::shared_ptr<brpc::Channel> UpdateCcNodeServiceChannel(
        uint32_t node_id, std::shared_ptr<brpc::Channel> old_channel);
    uint32_t GetPrimaryNodeId()
    {
        return Sharder::Instance().LeaderNodeId(
            Sharder::Instance().NativeNodeGroup());
    }

    void SetCandidateStandbyNodeTerm(int64_t standby_term)
    {
        if (!cc_nodes_init_.load(std::memory_order_acquire))
        {
            return;
        }

        candidate_standby_node_term_cache_.store(standby_term,
                                                 std::memory_order_release);
    }

    void SetStandbyNodeTerm(int64_t standby_term)
    {
        if (!cc_nodes_init_.load(std::memory_order_acquire))
        {
            return;
        }

        standby_node_term_cache_.store(standby_term, std::memory_order_release);
    }

    int64_t StandbyNodeTerm()
    {
        if (!cc_nodes_init_.load(std::memory_order_acquire))
        {
            return -1;
        }

        return standby_node_term_cache_.load(std::memory_order_acquire);
    }

    int64_t PrimaryNodeTerm()
    {
        int64_t term = standby_node_term_cache_.load(std::memory_order_acquire);
        if (term < 0)
        {
            term = candidate_standby_node_term_cache_.load(
                std::memory_order_acquire);
            if (term < 0)
            {
                return -1;
            }
        }

        return term >> 32;
    }

    int64_t CandidateStandbyNodeTerm()
    {
        if (!cc_nodes_init_.load(std::memory_order_acquire))
        {
            return -1;
        }
        return candidate_standby_node_term_cache_.load(
            std::memory_order_acquire);
    }

    void SetStandbyBecomingLeaderNodeTerm(int64_t leader_term)
    {
        if (!cc_nodes_init_.load(std::memory_order_acquire))
        {
            return;
        }

        standby_becoming_leader_term_cache_.store(leader_term,
                                                  std::memory_order_release);
    }

    int64_t StandbyBecomingLeaderNodeTerm() const
    {
        if (!cc_nodes_init_.load(std::memory_order_acquire))
        {
            return -1;
        }

        return standby_becoming_leader_term_cache_.load(
            std::memory_order_acquire);
    }

    bool IncrInflightStandbyReqCount(uint64_t cnt)
    {
        uint64_t value =
            inflight_standby_req_cnt_.load(std::memory_order_relaxed);

        if (value > std::numeric_limits<uint64_t>::max() - cnt)
        {
            return false;
        }

        inflight_standby_req_cnt_.fetch_add(cnt);
        return true;
    }

    void DecrInflightStandbyReqCount(uint64_t cnt)
    {
        uint64_t value =
            inflight_standby_req_cnt_.load(std::memory_order_relaxed);
        if (value < cnt)
        {
            LOG(ERROR) << "Failed to decrease, inflight standby req count = "
                       << value << ",decrease cnt = " << cnt;
            assert(false && "bug");
            return;
        }

        inflight_standby_req_cnt_.fetch_sub(cnt);
    }

    uint64_t InflightStandbyReqCount() const
    {
        return inflight_standby_req_cnt_.load();
    }

    uint32_t GetNextSubscribeId()
    {
        uint32_t subscribe_id =
            subscribe_counter_.fetch_add(1, std::memory_order_acq_rel);
        return subscribe_id;
    }

    uint32_t GetCurrentSubscribeId()
    {
        return subscribe_counter_.load(std::memory_order_acquire);
    }

    bool NotifyShutdown()
    {
        bool expect = false;
        return cluster_is_shutting_down_.compare_exchange_strong(
            expect, true, std::memory_order_acq_rel);
    }

    bool CheckShutdownStatus()
    {
        return Sharder::Instance().cluster_is_shutting_down_.load(
            std::memory_order_acquire);
    }

    brpc::Channel *GetHostManagerChannel()
    {
        if (!hm_channel_init_.load(std::memory_order_acquire))
        {
            return nullptr;
        }
        return &hm_channel_;
    }

private:
    Sharder();

    ~Sharder();

    void SetCommandLineOptions();

    inline void RebalanceNgMembers(
        std::unordered_map<uint32_t, std::vector<NodeConfig>> &ng_configs);

private:
    uint32_t node_id_;
    uint32_t native_ng_;
    std::string host_name_;
    uint16_t port_;
    // Whether is candidate of native node group.
    // bool is_candidate_;

    std::shared_mutex cluster_cnf_mux_;
    // Stores the current cluster config. It contains the mapping relation
    // between node group id and node group members, current node group
    // leader etc.
    ClusterConfig cluster_config_;
    // The replicate number of node group.
    uint16_t rep_group_cnt_;

    // Ng leader cache. We preallocate it to the max cluster size so that we
    // don't need to modify the size of it.
    std::atomic<uint32_t> ng_leader_cache_[1000];
    // Ng leader term cache. Note this is not reliable.
    std::atomic<int64_t> ng_leader_term_cache_[1000];

    std::atomic<int64_t> leader_term_cache_[1000];
    std::atomic<int64_t> candidate_leader_term_cache_[1000];

    // The term that standby is subscribed to. Only used on standby node.
    std::atomic<int64_t> standby_node_term_cache_;
    std::atomic<int64_t> candidate_standby_node_term_cache_;
    // The term when standby is about to become leader.
    std::atomic<int64_t> standby_becoming_leader_term_cache_;

    std::atomic<uint32_t> subscribe_counter_{0};
    // The number of standby requests that have been received but not yet
    // execute finished.
    std::atomic<uint64_t> inflight_standby_req_cnt_{0};

    std::vector<std::string> txlog_ips_;
    std::vector<uint16_t> txlog_ports_;

    // Used to protect recovered_leader_set
    std::mutex recovery_state_mux_;
    std::condition_variable recovery_state_cv_;

    // Used at node start stage to check whether all the involed tx_nodes
    // finish the log recovery. If some nodes stepdown during cluster
    // startup, the normal retry logic for each operation will handle it.
    std::unordered_set<uint32_t> recovered_leader_set_;

    /**
     * @brief Acts as a memory barrier such that initialized cc nodes are
     * synced with following reads of cc nodes at all cores.
     *
     */
    std::atomic<bool> cc_nodes_init_{false};

    moodycamel::ConcurrentQueue<std::unique_ptr<remote::CcMessage>> msg_pool_;

    // The cc stream sender establishes connections to remote nodes and
    // sends cc requests and responses to remote nodes via streams. It is
    // initialized before the cc stream receiver, given that the cc stream
    // receiver accepts and dispatches cc requests to tx processors, which
    // process the requests and send the responses back via the cc stream
    // sender.
    std::unique_ptr<remote::CcStreamSender> cc_stream_sender_;

    // The RPC server that listens on the port of local_port and serves the
    // cc stream service.
    brpc::Server cc_stream_server_;
    // The stream service that accepts a stream of cc requests from remote
    // nodes.
    std::unique_ptr<remote::CcStreamReceiver> cc_stream_receiver_;

    // The RPC server that listens on the port of local_port+1. It provides
    // sync RPCs toward this node and serves Raft communications within cc
    // node groups.
    brpc::Server cc_node_server_;
    // The service that provides sync RPCs to remote nodes, i.e., leader
    // transfer and checking tx status.
    std::unique_ptr<remote::CcNodeService> cc_node_service_;

    // The RPC server that listens on the port of local_port+3 and serves
    // the log replay service.
    brpc::Server log_replay_server_;
    // The replay service that accepts a stream of log replay messages from
    // all log groups.
    std::unique_ptr<fault::RecoveryService> recovery_service_;

    // Worker pool for doing various aync works
    std::unique_ptr<TxWorkerPool> tx_worker_pool_;

    // Worker thread that communicates with host manager process.
    std::unique_ptr<TxWorkerPool> sharder_worker_;

    LocalCcShards *local_shards_;
    std::unique_ptr<TxLog> log_agent_;
    std::atomic<bool> log_agent_interrupt_{false};

    // Channel to cc node service of other nodes.
    std::unordered_map<uint32_t, std::shared_ptr<brpc::Channel>>
        cc_node_service_channels_;

    std::shared_mutex node_channel_mux_;
    // Host manager ip and port
    std::string hm_ip_{""};
    uint16_t hm_port_{0};
    brpc::Channel hm_channel_;
    std::atomic<bool> hm_channel_init_{false};

    // To stop the cluster, it should perform a checkpoint before any node is
    // terminated (as the final checkpoint will fail if the majority of nodes
    // are down). Additionally, the service should block external requests once
    // the cluster initiates the shutdown process.
    std::atomic<bool> cluster_is_shutting_down_{false};
};
}  // namespace txservice
