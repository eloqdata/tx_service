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
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "txlog.h"

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
                  uint64_t replay_start_ts,
                  uint64_t queued_clock)
        : cc_ng_id_(cc_ng_id),
          cc_ng_term_(cc_ng_term),
          log_group_(log_group),
          replay_start_ts_(replay_start_ts),
          queued_clock_(queued_clock)
    {
    }

    uint32_t cc_ng_id_;
    int64_t cc_ng_term_;
    int log_group_;
    uint64_t replay_start_ts_;
    // the clock when the request is put into the replay queue.
    // set queued_clock_ to 0, if it's not a delayed request.
    uint64_t queued_clock_;
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
     * @brief Send ReplayLogRequest to log groups. If log_group is negative,
     * send to all log groups, else just the specified log group.
     */
    void ReplayLog(uint32_t cc_ng_id,
                   int64_t cc_ng_term,
                   int log_group = -1,
                   uint64_t replay_start_ts = 0,
                   bool delayed_request = false);

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
     * @brief ReplayNow() check whether the replay request can be processed
     * immediately. When replay error happens, we will put the replay request
     * into replay queue again, but with a delay (default 10 seconds).
     */
    bool ReplayNow(ReplayLogTask &task);

    void ProcessReplayLogTask(ReplayLogTask &task);

    int ProcessDelayedReplayLogTask();

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

    LocalCcShards &local_shards_;
    // Each ConnectionInfo is uniquely identified by <cc_ng_id, log_group_id>
    // pair.
    std::unordered_map<brpc::StreamId, ConnectionInfo> inbound_connections_;
    int active_stream_cnt_ = 0;
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
    std::deque<ReplayLogTask> delayed_replay_queue_;
    std::deque<RecoverTxTask> recover_tx_queue_;
    std::mutex queue_mux_;
    std::condition_variable queue_cv_;
    std::atomic<bool> finish_;

    // ip and port of log replay server of this node
    std::string ip_;
    uint16_t port_;

    void ClearTx(uint64_t tx_number);
};
}  // namespace fault
}  // namespace txservice
