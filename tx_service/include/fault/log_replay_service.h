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
#include <brpc/server.h>
#include <brpc/stream.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "txlog.h"
#include "type.h"

namespace txservice
{
class LocalCcShards;

struct ReplayLogCc;

namespace fault
{
class CcNode;

struct ReplayLogTask
{
    ReplayLogTask() = default;

    ReplayLogTask(uint32_t cc_ng_id,
                  int64_t cc_ng_term,
                  int log_group,
                  uint64_t replay_start_ts)
        : cc_ng_id_(cc_ng_id),
          cc_ng_term_(cc_ng_term),
          log_group_(log_group),
          replay_start_ts_(replay_start_ts)
    {
    }

    uint32_t cc_ng_id_;
    int64_t cc_ng_term_;
    int log_group_;
    uint64_t replay_start_ts_;
};

struct RecoverTxTask
{
    RecoverTxTask() = default;

    RecoverTxTask(uint64_t tx_number,
                  int64_t tx_term,
                  uint64_t write_lock_ts,
                  uint32_t cc_ng_id,
                  int64_t cc_ng_term)
        : tx_number_(tx_number),
          tx_term_(tx_term),
          write_lock_ts_(write_lock_ts),
          cc_ng_id_(cc_ng_id),
          cc_ng_term_(cc_ng_term)
    {
    }

    // The number of tx who holds the intention/lock.
    uint64_t tx_number_;
    // The term of the cc node group in which the tx resides when the tx
    // acquires the intention/lock.
    int64_t tx_term_;
    // The write lock timestamp
    uint64_t write_lock_ts_;
    // The ID of the cc node group in which the lock/intention resides.
    uint32_t cc_ng_id_;
    // The term of the cc node group in which the lock/intention resides.
    int64_t cc_ng_term_;
};

class RecoveryService : public brpc::StreamInputHandler,
                        public ::txlog::LogReplayService
{
public:
    enum struct WaitingStatus : int8_t
    {
        Active = 0,
        WaitForAll = 1,
        WaitForMany = 2,
    };
    RecoveryService() = delete;
    RecoveryService(LocalCcShards &local_shards,
                    TxLog *log_agent,
                    std::string ip,
                    uint16_t port);
    ~RecoveryService() = default;

    void Shutdown();

    void Connect(::google::protobuf::RpcController *controller,
                 const ::txlog::LogReplayConnectRequest *request,
                 ::txlog::LogReplayConnectResponse *response,
                 ::google::protobuf::Closure *done) override;

    void UpdateLogGroupLeader(::google::protobuf::RpcController *controller,
                              const ::txlog::LogLeaderUpdateRequest *request,
                              ::txlog::LogLeaderUpdateResponse *response,
                              ::google::protobuf::Closure *done) override;

    void NotifyCheckpointer(::google::protobuf::RpcController *controller,
                            const ::txlog::NotifyCheckpointerRequest *request,
                            ::txlog::NotifyCheckpointerResponse *response,
                            ::google::protobuf::Closure *done) override;

    /**
     * @brief Queues a ReplayLogRequest to the log groups, sent by the notify
     * thread (ProcessReplayLogTask). If log_group is negative, to every log
     * group that has not finished the term's replay, else to that one.
     */
    void ReplayLog(uint32_t cc_ng_id,
                   int64_t cc_ng_term,
                   int log_group = -1,
                   uint64_t replay_start_ts = 0);

    void RecoverTx(uint64_t tx_number,
                   int64_t tx_term,
                   uint64_t write_lock_ts,
                   uint32_t cc_ng_id,
                   int64_t cc_ng_term);

    int on_received_messages(brpc::StreamId stream_id,
                             butil::IOBuf *const messages[],
                             size_t size) override;

    void on_idle_timeout(brpc::StreamId id) override;

    void on_closed(brpc::StreamId id) override;

private:
    /**
     * @brief The replay requests for a node group this node is the candidate
     * leader of; used by the notify thread alone.
     */
    struct CandidateReplayWatch
    {
        int64_t term{-1};  ///< The candidate term the fields below are for.
        /** The start timestamp of term's first request (the leader start's),
         * which every later request reuses. */
        uint64_t replay_start_ts{0};
        /** Per log group: when a request was last sent to it, or it was last
         * seen streaming the replay. */
        std::unordered_map<uint32_t, uint64_t> last_send_ts;
    };

    /**
     * @brief Sends a queued ReplayLogRequest: one bounded attempt per log
     * group (TxLog::ReplayLog), skipping log groups that have finished the
     * term's replay, and none once the node is no longer the node group's
     * candidate leader at that term. Records each send for the watchdog.
     *
     * @param task The request.
     */
    void ProcessReplayLogTask(ReplayLogTask &task);

    void ProcessRecoverTxTask(RecoverTxTask &task);

    struct ConnectionInfo
    {
        ConnectionInfo() = default;
        ConnectionInfo(uint32_t lg_id,
                       uint32_t cc_ng_id,
                       int64_t cc_ng_term,
                       uint64_t replay_start_ts,
                       bool recovering)
            : log_group_id_(lg_id),
              cc_ng_id_(cc_ng_id),
              cc_ng_term_(cc_ng_term),
              mux_(),
              replay_start_ts_(replay_start_ts),
              recovering_(recovering)
        {
        }
        ConnectionInfo(const ConnectionInfo &rhs) = delete;

        uint32_t log_group_id_;
        uint32_t cc_ng_id_;
        int64_t cc_ng_term_;
        // protects finished_cnt_ and recovery_error_.
        bthread::Mutex mux_;
        std::atomic<WaitingStatus> status_{WaitingStatus::Active};
        std::atomic<size_t> on_fly_cnt_{0};
        uint64_t replay_start_ts_{0};
        // Only true if this stream is for log replay.
        bool recovering_;
        bool recovery_error_{false};
    };

    enum class ReplayPhase : uint8_t
    {
        CollectSplitTable = 0,
        ReplayDdlLog = 1,
        ReplayDataLog = 2,
    };

    struct NodeGroupReplayBarrier
    {
        NodeGroupReplayBarrier() = default;

        bool WaitSplitTableCollected(
            const ConnectionInfo *info,
            brpc::StreamId stream_id,
            std::unordered_set<TableName> &&range_split_tables);

        bool WaitDdlReplayed(
            const ConnectionInfo *info,
            brpc::StreamId stream_id,
            std::unordered_map<TableName, std::unordered_map<int32_t, uint64_t>>
                &&split_range_info);

        void UpdateBarrier(brpc::StreamId stream_id,
                           uint32_t lg_id,
                           uint32_t lg_count,
                           int64_t ng_term);

        bool Finished(brpc::StreamId stream_id, uint32_t lg_id);

        int64_t current_term_{-1};
        uint32_t expected_log_group_cnt_{0};
        ReplayPhase phase_{ReplayPhase::CollectSplitTable};

        // Log group id set that has completed related phase.
        std::unordered_set<uint32_t> done_log_groups_;

        // Current active stream for each log group.
        std::unordered_map<uint32_t, brpc::StreamId> current_stream_;

        /**
         * Cross log group aggregation, for schema replay: the base tables with
         * a split in flight that the recovering node group coordinates, whose
         * catalog read lock that split holds on it.
         */
        std::unordered_set<TableName> range_split_tables_;

        // Cross log group aggregation: split range commit ts for data replay.
        std::unordered_map<TableName, std::unordered_map<int32_t, uint64_t>>
            split_range_info_;

        bthread::Mutex mux_;
        bthread::ConditionVariable cv_;
    };

    void CleanupReplayBarrier(uint32_t cc_ng_id);
    NodeGroupReplayBarrier *GetReplayBarrier(uint32_t cc_ng_id,
                                             bool emplace = false);

    LocalCcShards &local_shards_;
    // Each ConnectionInfo is uniquely identified by <cc_ng_id, log_group_id>
    // pair. unique_ptr ensures pointer stability across map rehashing.
    std::unordered_map<brpc::StreamId, std::unique_ptr<ConnectionInfo>>
        inbound_connections_;
    int active_stream_cnt_ = 0;
    /** @brief Test only: the replay stream the replay_drop_stream_once fault
     * dropped, whose messages are ignored. */
    std::atomic<brpc::StreamId> fault_dropped_stream_{brpc::INVALID_STREAM_ID};
    bthread::Mutex inbound_mux_;
    bthread::ConditionVariable inbound_cv_;

    void WaitAndClearRequests(
        brpc::StreamId stream_id,
        bthread::Mutex &mux,
        std::atomic<size_t> &on_fly_cnt,
        std::atomic<WaitingStatus> &status,
        bool &recovery_error,
        WaitingStatus waiting_status = WaitingStatus::WaitForAll);
    static const int timeout_ms_ = 2000;
    // to resend ReplayLogRequest on stream timeout
    TxLog *log_agent_;

    std::thread notify_thread_;
    std::deque<ReplayLogTask> replay_log_queue_;
    std::deque<RecoverTxTask> recover_tx_queue_;
    /**
     * @brief Node group id -> its replay watch, for the node groups this node
     * is recovering only: the watchdog visits these alone.
     */
    std::unordered_map<uint32_t, CandidateReplayWatch> candidate_replay_watch_;
    std::mutex queue_mux_;
    std::condition_variable queue_cv_;
    std::atomic<bool> finish_;

    /**
     * @brief How long a log group may go without a recovering replay stream
     * after a request was sent to it (or its stream was last seen) before the
     * request is sent to it again, and again at this interval until its
     * stream appears. Counted from the send: a lost request, a log group with
     * no leader and a lost reply look the same to the sender.
     */
    static constexpr uint64_t kReplayCheckIntervalUs = 5ULL * 1000 * 1000;
    /**
     * @brief How often the watchdog looks for log groups to ask again (when
     * the notify thread has had no task for this long), so that a resend comes
     * within this much after kReplayCheckIntervalUs has passed. It acts only
     * for node groups replaying the log (with a candidate term).
     */
    static constexpr int64_t kReplayWatchPeriodMs = 1000;

    // ip and port of log replay server of this node
    std::string ip_;
    uint16_t port_;

    void ClearTx(uint64_t tx_number);
    // key: cc_ng_id
    std::unordered_map<uint32_t, NodeGroupReplayBarrier> replay_barriers_;
};
}  // namespace fault
}  // namespace txservice
