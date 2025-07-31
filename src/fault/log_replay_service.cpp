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
#include "log_replay_service.h"

#include <brpc/stream.h>

#include <memory>
#include <mutex>
#include <vector>

#include "cc/cc_request.h"
#include "cc/local_cc_shards.h"
#include "log.pb.h"
#include "log_type.h"
#include "proto/cc_request.pb.h"
#include "sharder.h"
#include "type.h"

/**
 * RecoveryService serves three purposes:
 * replay log after a ccnode becomes leader; recover orphan lock's belonging
 * txn; check whether this node is preferred node group's leader periodically
 * and request leader transfer if not.
 *
 * Here is a brief replay protocol description:
 * 1. ccnode becomes a raft leader and begins to send ReplayLogRequest to all
 * the log groups. If log group's leader is not elected, the ccnode will call
 * braft API to wait for the log leader to be elected.
 * 2. Log group will connect to log_replay_service and send replay logs to
 * ccnode.
 * 3. ccnode receives and replays the logs from the stream.
 * 4. If ccnode fails to receive new logs after a timeout(2000ms), it will close
 * the stream and send a new ReplayLogRequest to log group leader. And replay
 * from the beginning.
 * 5. ccnode finishes the log replay and become the actual leader from candidate
 * leader. Then it will close the log replay stream.
 * 6. Later when an orphan lock is detected, will send RecoveryTx request to log
 * service.
 * 7. log service will send the orphan lock replay log to log_replay_service if
 * the orphan lock tx is committed. Since the stream is closed, will reconnect
 * the stream and send the message. Note that the new connection will not set
 * idle_timeout since orphan lock will not happens in normal case.
 * 8. ccnode replay the orphan lock record and release the orphan lock.
 */

namespace txservice
{
namespace fault
{
thread_local CcRequestPool<ReplayLogCc> replay_cc_pool_;
thread_local CcRequestPool<ParseDataLogCc> parse_datalog_cc_pool_;
RecoveryService::RecoveryService(LocalCcShards &local_shards,
                                 TxLog *log_agent,
                                 std::string ip,
                                 uint16_t port)
    : local_shards_(local_shards),
      log_agent_(log_agent),
      finish_(false),
      ip_(std::move(ip)),
      port_(port)
{
    notify_thread_ = std::thread(
        [this]
        {
            // LOG(INFO) << "replay service notify thread started";
            while (!finish_.load(std::memory_order_acquire))
            {
                std::unique_lock<std::mutex> lk(queue_mux_);
                queue_cv_.wait_for(
                    lk,
                    std::chrono::seconds(10),
                    [this]
                    {
                        return !replay_log_queue_.empty() ||
                               !recover_tx_queue_.empty() ||
                               finish_.load(std::memory_order_acquire);
                    });
                if (finish_.load(std::memory_order_acquire))
                {
                    LOG(INFO) << "replay service notify thread quits";
                    break;
                }
                if (!replay_log_queue_.empty())
                {
                    ReplayLogTask task = replay_log_queue_.front();
                    replay_log_queue_.pop_front();
                    lk.unlock();
                    LOG(INFO) << "replay service processes a ReplayLog task";
                    ProcessReplayLogTask(task);
                    continue;
                }
                if (!delayed_replay_queue_.empty())
                {
                    int ready_cnt = ProcessDelayedReplayLogTask();
                    if (ready_cnt > 0)
                    {
                        // ReplayLog task is of the highest priority, continue
                        // to next round and process the ready ReplayTask
                        continue;
                    }
                }
                if (!recover_tx_queue_.empty())
                {
                    RecoverTxTask task = recover_tx_queue_.front();
                    recover_tx_queue_.pop_front();
                    lk.unlock();
                    LOG(INFO)
                        << "replay service processes a RecoverTx task, txn: "
                        << task.tx_number_;
                    ProcessRecoverTxTask(task);
                    continue;
                }
            }
        });
}

bool RecoveryService::ReplayNow(ReplayLogTask &task)
{
    // queued_clock_ == 0 means it's not a delayed request.
    if (task.queued_clock_ == 0)
    {
        return true;
    }

    // check whether the delayed request can be executed now.
    uint64_t now_ts = LocalCcShards::ClockTs();
    using namespace std::chrono_literals;
    uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::seconds(30))
                            .count();
    if (now_ts - task.queued_clock_ > duration)
    {
        return true;
    }
    else
    {
        return false;
    }
}

void RecoveryService::Shutdown()
{
    // reap background thread
    {
        std::unique_lock lk(queue_mux_);
        finish_.store(true, std::memory_order_release);
        queue_cv_.notify_one();
    }
    notify_thread_.join();

    // close all streams
    std::unique_lock<std::mutex> lk(inbound_mux_);
    for (auto it = inbound_connections_.begin();
         it != inbound_connections_.end();
         it++)
    {
        brpc::StreamClose(it->first);
    }

    inbound_cv_.wait(lk, [this]() { return active_stream_cnt_ == 0; });
}

void RecoveryService::Connect(::google::protobuf::RpcController *controller,
                              const ::txlog::LogReplayConnectRequest *request,
                              ::txlog::LogReplayConnectResponse *response,
                              ::google::protobuf::Closure *done)
{
    brpc::StreamId stream_socket;
    brpc::ClosureGuard done_guard(done);

    uint32_t cc_ng_id = request->cc_node_group_id();
    uint32_t log_group_id = request->log_group_id();
    int64_t cc_ng_term = request->cc_ng_term();
    uint64_t replay_start_ts = request->replay_start_ts();
    std::unique_lock lk(inbound_mux_);

    // indicates whether this connection (<cc_ng_id, lg_id> pair) is still
    // recovering.
    // It will affect the stream option. If recovering is true, then
    // the stream will set idle_timeout and try to resend ReplayLogRequest if
    // timeout happens.
    bool recovering = false;
    // First check if cc ng id has already finished replay.
    if (!Sharder::Instance().CheckLeaderTerm(cc_ng_id, cc_ng_term))
    {
        if (Sharder::Instance().CandidateLeaderTerm(cc_ng_id) != cc_ng_term)
        {
            // Invalid connection request.
            return;
        }
        // node group is trying to recover cc_ng_term. Check if this log group
        // has finished replay.
        if (Sharder::Instance().CheckLogGroupReplayFinished(
                cc_ng_id, log_group_id, cc_ng_term))
        {
            // This log group has already finished log replay, but cc node is
            // not node group leader yet. So this connection cannot be orphan
            // lock check. It must be an out dated log replay connection.
            return;
        }

        recovering = true;
        // Clean up old log group stream connection.
        for (auto &[stream_id, info] : inbound_connections_)
        {
            if (info.cc_ng_id_ == cc_ng_id &&
                info.log_group_id_ == log_group_id)
            {
                if (cc_ng_term > info.cc_ng_term_)
                {
                    // cc_ng_term > info.cc_ng_term_, the cc_ng has failed over
                }
                else if (cc_ng_term == info.cc_ng_term_)
                {
                    // the cc_ng is still recovering, and this request is a
                    // response for RecoveryService's stream timeout and resend
                    // ReplayLogRequest;
                }
                else
                {
                    // an outdated connect request, ignore
                    return;
                }
                // close old stream and remove entry
                brpc::StreamClose(stream_id);
                break;
            }
        }
    }

    // either no old connection found for <cc_ng_id, lg_id> pair, or old
    // connection has been removed. accept connect request and insert
    // ConnectionInfo.

    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

    brpc::StreamOptions stream_options;
    stream_options.handler = this;
    // set idle_timeout_ms when this is a new term connection or a reconnect of
    // recovering connection
    stream_options.idle_timeout_ms = recovering ? timeout_ms_ : -1;
    if (brpc::StreamAccept(&stream_socket, *cntl, &stream_options) != 0)
    {
        cntl->SetFailed("Failed to accept stream");
        return;
    }

    inbound_connections_.try_emplace(stream_socket,
                                     log_group_id,
                                     cc_ng_id,
                                     cc_ng_term,
                                     replay_start_ts,
                                     recovering);
    response->set_success(true);
    LOG(INFO) << "replay service accepting new stream: " << stream_socket
              << " from log group: " << log_group_id
              << " to cc_ng: " << cc_ng_id << " at term: " << cc_ng_term;

    active_stream_cnt_++;
}

void RecoveryService::UpdateLogGroupLeader(
    ::google::protobuf::RpcController *controller,
    const ::txlog::LogLeaderUpdateRequest *request,
    ::txlog::LogLeaderUpdateResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    uint32_t lg_id = request->lg_id();
    uint32_t node_id = request->node_id();
    Sharder::Instance().UpdateLogGroupLeader(lg_id, node_id);
    response->set_error(false);
    LOG(INFO) << "Update log group:" << lg_id
              << " leader to node_id:" << node_id;
}

void RecoveryService::NotifyCheckpointer(
    ::google::protobuf::RpcController *controller,
    const ::txlog::NotifyCheckpointerRequest *request,
    ::txlog::NotifyCheckpointerResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    Sharder::Instance().NotifyCheckPointer();
    LOG(INFO) << "Notified checkpointer.";
}

void RecoveryService::ReplayLog(uint32_t cc_ng_id,
                                int64_t cc_ng_term,
                                int log_group,
                                uint64_t replay_start_ts,
                                bool delayed_request)
{
    std::unique_lock lk(queue_mux_);
    if (delayed_request)
    {
        uint64_t queued_clock = LocalCcShards::ClockTs();
        delayed_replay_queue_.emplace_back(
            cc_ng_id, cc_ng_term, log_group, replay_start_ts, queued_clock);
    }
    else
    {
        replay_log_queue_.emplace_back(
            cc_ng_id, cc_ng_term, log_group, replay_start_ts, 0);
    }
    queue_cv_.notify_one();
}

void RecoveryService::RecoverTx(uint64_t tx_number,
                                int64_t tx_term,
                                uint64_t write_lock_ts,
                                uint32_t cc_ng_id,
                                int64_t cc_ng_term)
{
    std::unique_lock lk(queue_mux_);
    recover_tx_queue_.emplace_back(
        tx_number, tx_term, write_lock_ts, cc_ng_id, cc_ng_term);
    queue_cv_.notify_one();
}

int RecoveryService::on_received_messages(brpc::StreamId stream_id,
                                          butil::IOBuf *const messages[],
                                          size_t size)
{
    std::shared_ptr<std::vector<::txlog::ReplayMessage>> msg_vec =
        std::make_shared<std::vector<::txlog::ReplayMessage>>(size);
    std::unordered_map<TableName, std::shared_ptr<std::atomic_uint32_t>>
        table_range_split_cnt;
    std::unordered_set<TableName> range_split_tables;

    ConnectionInfo *info;
    {
        std::lock_guard<std::mutex> lk(inbound_mux_);
        info = &inbound_connections_.find(stream_id)->second;
    }
    bthread::Mutex &mux = info->mux_;
    bool &recovery_error = info->recovery_error_;
    uint16_t next_core = 0;
    std::atomic<WaitingStatus> &status = info->status_;
    std::atomic<size_t> &on_fly_cnt = info->on_fly_cnt_;

    for (size_t idx = 0; idx < size; ++idx)
    {
        ::txlog::ReplayMessage &msg = msg_vec->at(idx);
        butil::IOBufAsZeroCopyInputStream wrapper(*messages[idx]);
        msg.ParseFromZeroCopyStream(&wrapper);
        const bool is_lock_recovery = msg.is_lock_recovery();
        if (idx == 0)
        {
            // All of the shema and range split logs should be in the first msg.
            // Collect which tables are range splitting. For these table we only
            // need to recover write intent on catalog entry instead of write
            // lock when processing schema op msg.
            for (const ::txlog::ReplaySplitRangeMsg &split_range_msg :
                 msg.split_range_op_msgs())
            {
                const std::string &split_range_op_blob =
                    split_range_msg.split_range_op_blob();
                size_t blob_offset = 0;
                uint8_t table_name_len = *reinterpret_cast<const uint8_t *>(
                    split_range_op_blob.data() + blob_offset);
                blob_offset += sizeof(uint8_t);

                // Table name string
                std::string_view table_name_view(
                    split_range_op_blob.data() + blob_offset, table_name_len);
                blob_offset += table_name_len;

                TableEngine table_engine =
                    ::txlog::ToLocalType::ConvertTableEngine(
                        ::txlog::TableEngine(*reinterpret_cast<const uint8_t *>(
                            split_range_op_blob.data() + blob_offset)));
                blob_offset += sizeof(uint8_t);

                // Add read lock on catalog
                TableName table_name{table_name_view,
                                     TableName::Type(table_name_view),
                                     table_engine};
                TableName base_table_name{table_name.GetBaseTableNameSV(),
                                          TableType::Primary,
                                          table_engine};
                range_split_tables.insert(base_table_name);
            }
        }

        uint32_t cc_ng_id = msg.cc_node_group_id();
        int64_t cc_ng_term = msg.cc_node_group_term();

        // process cluster scale ops first
        if (msg.has_cluster_scale_op_msg())
        {
            const std::string &scale_op_blob =
                msg.cluster_scale_op_msg().cluster_scale_op_blob();

            // We need to recover both cluster topology and bucket owner
            // based on the cluster scale log.

            // Replay cluster topology first
            ReplayLogCc *cc_req = replay_cc_pool_.NextRequest();
            cc_req->Reset(
                cc_ng_id,
                cc_ng_term,
                cluster_config_ccm_name_sv,
                TableType::ClusterConfig,
                TableEngine::None,
                std::string_view(scale_op_blob.data(), scale_op_blob.length()),
                msg.cluster_scale_op_msg().commit_ts(),
                msg.cluster_scale_op_msg().txn(),
                mux,
                status,
                on_fly_cnt,
                recovery_error,
                is_lock_recovery);

            on_fly_cnt.fetch_add(1, std::memory_order_release);
            local_shards_.EnqueueCcRequest(0, cc_req);
            WaitAndClearRequests(
                stream_id, mux, on_fly_cnt, status, recovery_error);
            if (recovery_error)
            {
                return 0;
            }

            // Recover bucket owner to correct state
            cc_req = replay_cc_pool_.NextRequest();
            cc_req->Reset(
                cc_ng_id,
                cc_ng_term,
                range_bucket_ccm_name_sv,
                TableType::RangeBucket,
                TableEngine::None,
                std::string_view(scale_op_blob.data(), scale_op_blob.length()),
                msg.cluster_scale_op_msg().commit_ts(),
                msg.cluster_scale_op_msg().txn(),
                mux,
                status,
                on_fly_cnt,
                recovery_error,
                is_lock_recovery);

            on_fly_cnt.fetch_add(1, std::memory_order_release);
            local_shards_.EnqueueCcRequest(0, cc_req);
            WaitAndClearRequests(
                stream_id, mux, on_fly_cnt, status, recovery_error);
            if (recovery_error)
            {
                return 0;
            }
        }

        // process schema ops before processing data ops
        for (const ::txlog::ReplaySchemaMsg &schema_op_msg :
             msg.schema_op_msgs())
        {
            const std::string &schema_op_blob = schema_op_msg.schema_op_blob();

            ReplayLogCc *cc_req = replay_cc_pool_.NextRequest();
            cc_req->Reset(cc_ng_id,
                          cc_ng_term,
                          catalog_ccm_name_sv,
                          TableType::Catalog,
                          TableEngine::None,
                          std::string_view(schema_op_blob.data(),
                                           schema_op_blob.length()),
                          schema_op_msg.commit_ts(),
                          schema_op_msg.txn(),
                          mux,
                          status,
                          on_fly_cnt,
                          recovery_error,
                          is_lock_recovery,
                          nullptr,
                          &range_split_tables);

            on_fly_cnt.fetch_add(1, std::memory_order_release);
            local_shards_.EnqueueCcRequest(0, cc_req);

            // wait for this schema operation to be recovered at all shards
            // before processing next
            WaitAndClearRequests(
                stream_id, mux, on_fly_cnt, status, recovery_error);
            if (recovery_error)
            {
                return 0;
            }
        }

        // process range split ops
        for (const ::txlog::ReplaySplitRangeMsg &split_range_msg :
             msg.split_range_op_msgs())
        {
            const std::string &split_range_op_blob =
                split_range_msg.split_range_op_blob();
            size_t blob_offset = 0;
            uint8_t table_name_len = *reinterpret_cast<const uint8_t *>(
                split_range_op_blob.data() + blob_offset);
            blob_offset += sizeof(uint8_t);

            const uint64_t txn = split_range_msg.txn();
            const uint64_t ts = split_range_msg.commit_ts();

            // Table name string
            std::string_view table_name_view(
                split_range_op_blob.data() + blob_offset, table_name_len);
            blob_offset += table_name_len;

            TableEngine table_engine = ::txlog::ToLocalType::ConvertTableEngine(
                ::txlog::TableEngine(*reinterpret_cast<const uint8_t *>(
                    split_range_op_blob.data() + blob_offset)));
            blob_offset += sizeof(uint8_t);

            TableName table_name{table_name_view,
                                 TableName::Type(table_name_view),
                                 table_engine};
            TableName base_table_name{table_name.GetBaseTableNameSV(),
                                      TableType::Primary,
                                      table_engine};

            auto res_pair = table_range_split_cnt.try_emplace(
                base_table_name, std::make_shared<std::atomic_uint32_t>(0));

            // Replay Split
            ReplayLogCc *cc_req = replay_cc_pool_.NextRequest();
            cc_req->Reset(
                cc_ng_id,
                cc_ng_term,
                table_name_view,
                TableType::RangePartition,
                table_engine,
                std::string_view(split_range_op_blob.data() + blob_offset,
                                 split_range_op_blob.length() - blob_offset),
                ts,
                txn,
                mux,
                status,
                on_fly_cnt,
                recovery_error,
                is_lock_recovery,
                res_pair.first->second);

            on_fly_cnt.fetch_add(1, std::memory_order_release);
            local_shards_.EnqueueCcRequest(0, cc_req);
            // wait for this range split operation to be recovered at all shards
            // before processing next
            WaitAndClearRequests(
                stream_id, mux, on_fly_cnt, status, recovery_error);
            if (recovery_error)
            {
                return 0;
            }
        }

        // parse and process log records
        const std::string &log_records = msg.binary_log_records();
        ParseDataLogCc *cc_req = parse_datalog_cc_pool_.NextRequest();
        cc_req->Reset(log_records,
                      cc_ng_id,
                      cc_ng_term,
                      mux,
                      status,
                      on_fly_cnt,
                      recovery_error,
                      is_lock_recovery);
        on_fly_cnt.fetch_add(1, std::memory_order_release);
        local_shards_.EnqueueCcRequest(next_core, cc_req);
        next_core = (next_core + 1) % local_shards_.Count();

        if (msg.has_finish())
        {
            // finish log replay of this log group
            // wait for all preceding ReplayLogCc requests finish
            WaitAndClearRequests(
                stream_id, mux, on_fly_cnt, status, recovery_error);
            // update recovering status and then close this stream,
            // log_shipping_agent has to create a new stream to send recoverTx
            // log records. when accepting that new stream, set no
            // idle_timeout_ms as that is a long-running connection.
            {
                BAIDU_SCOPED_LOCK(info->mux_);
                // ignore the old stream which is not in inbound_connections_.
                // if error happens during replay, the current ng's leader term
                // should not be updated.
                if (info->recovery_error_)
                {
                    LOG(ERROR)
                        << "Failed to recovery on ccnode group:" << cc_ng_id
                        << " with term:" << cc_ng_term;
                }
                else
                {
                    // finish log replay of this log group
                    const ::txlog::ReplayFinishMsg &finish_msg = msg.finish();
                    uint32_t lg_id = finish_msg.log_group_id();
                    uint32_t latest_txn_no = finish_msg.latest_txn_no();
                    uint64_t last_ckpt_ts = finish_msg.last_ckpt_ts();
                    Sharder::Instance().FinishLogReplay(cc_ng_id,
                                                        cc_ng_term,
                                                        lg_id,
                                                        latest_txn_no,
                                                        last_ckpt_ts);
                }

                LOG(INFO) << "replay connection: cc node group: "
                          << info->cc_ng_id_ << ", term: " << info->cc_ng_term_
                          << ", log group: " << info->log_group_id_
                          << ", set recovering status to finished";
            }
            brpc::StreamClose(stream_id);
            // assumption: finish message must be the last message so return
            return 0;
        }
    }

    bool wait_for_on_the_fly = false;
    bool recovery_failed = false;
    {
        BAIDU_SCOPED_LOCK(mux);
        // If there are too many on the fly reqs, block until
        // all of them are done. This will push log service back
        // from sending too many log msgs that we cannot handle.
        if (on_fly_cnt.load(std::memory_order_acquire) > 10000)
        {
            wait_for_on_the_fly = true;
        }
        // If the recover has already errored out, terminate all
        // replay connections of this node group.
        recovery_failed = recovery_error;
    }

    if (wait_for_on_the_fly || recovery_failed)
    {
        WaitAndClearRequests(stream_id,
                             mux,
                             on_fly_cnt,
                             status,
                             recovery_error,
                             WaitingStatus::WaitForMany);
    }
    return 0;
}

void RecoveryService::on_idle_timeout(brpc::StreamId id)
{
    // if the cc_node is still recovering, resend replay request to
    // corresponding log group. on_idle_timeout will be triggered every
    // timeout_ms_ until ReplayLogRequest succeeds and a new stream on this
    // <cc_ng_id, log_group_id> pair is established, this stream will be closed
    // then.
    ConnectionInfo *info;
    {
        std::unique_lock lk(inbound_mux_);
        auto it = inbound_connections_.find(id);
        if (it == inbound_connections_.end())
        {
            // this stream has been replaced by a newer one
            return;
        }
        info = &it->second;
    }
    BAIDU_SCOPED_LOCK(info->mux_);
    if (info->recovering_)
    {
        // still recovering, resend replay request to log group
        uint32_t lg_id = info->log_group_id_;
        uint32_t cc_ng_id = info->cc_ng_id_;
        int64_t cc_ng_term = info->cc_ng_term_;
        LOG(INFO) << "replay service stream: " << id
                  << " timeouts, cc_node group: " << cc_ng_id
                  << ", log group: " << lg_id
                  << ", still recovering, resend ReplayLogRequest";
        ReplayLog(cc_ng_id, cc_ng_term, lg_id, info->replay_start_ts_);
        brpc::StreamClose(id);
    }
}

void RecoveryService::on_closed(brpc::StreamId id)
{
    // If remote log group crashes, the stream will be closed, should check cc
    // node's recovering status and resend ReplayLogRequest here?
    // Seems unnecessary as log group's new leader will try to reship records.
    // Besides, the stream might be closed by LogShippingAgent intentionally.
    // There is no way to tell the difference. So, just do nothing.

    // Wait for all reqs from this stream is done before erasing it from inbound
    // conns since those cc reqs have pointers to ConnectionInfo. If everything
    // goes right, all reqs should have been finished at this point since we
    // waited once on the last msg. But if the stream is closed on error, the
    // reqs might not be all finished. We need to make sure there's no more
    // reference to ConnectionInfo here.
    ConnectionInfo *info;
    {
        std::lock_guard<std::mutex> lk(inbound_mux_);
        info = &inbound_connections_.find(id)->second;
    }
    WaitAndClearRequests(id,
                         info->mux_,
                         info->on_fly_cnt_,
                         info->status_,
                         info->recovery_error_);

    std::unique_lock<std::mutex> lk(inbound_mux_);
    active_stream_cnt_--;
    inbound_connections_.erase(id);
    LOG(INFO) << "replay service stream: " << id
              << ", is closed, active stream cnt: " << active_stream_cnt_;
    if (active_stream_cnt_ == 0)
    {
        inbound_cv_.notify_one();
    }
}

void RecoveryService::WaitAndClearRequests(brpc::StreamId stream_id,
                                           bthread::Mutex &mux,
                                           std::atomic<size_t> &on_fly_cnt_,
                                           std::atomic<WaitingStatus> &status,
                                           bool &recovery_error,
                                           WaitingStatus waiting_status)
{
    size_t on_fly_cnt = on_fly_cnt_.load(std::memory_order_relaxed);
    if ((on_fly_cnt > 0 && waiting_status == WaitingStatus::WaitForAll) ||
        (on_fly_cnt > 10000 && waiting_status == WaitingStatus::WaitForMany))
    {
        status.store(waiting_status, std::memory_order_relaxed);
        on_fly_cnt = on_fly_cnt_.load(std::memory_order_relaxed);
        while (
            (on_fly_cnt > 0 && waiting_status == WaitingStatus::WaitForAll) ||
            (on_fly_cnt > 10000 &&
             waiting_status == WaitingStatus::WaitForMany))
        {
            bthread_usleep(100);
            on_fly_cnt = on_fly_cnt_.load(std::memory_order_relaxed);
        }
        status.store(WaitingStatus::Active, std::memory_order_relaxed);
    }
    std::unique_lock<bthread::Mutex> lk(mux);
    if (recovery_error)
    {
        lk.unlock();
        std::unique_lock lk(inbound_mux_);
        uint32_t error_node_group_id = 0;
        int64_t error_term = -1;

        // find the replay error node group and its term.
        auto it = inbound_connections_.find(stream_id);
        if (it == inbound_connections_.end())
        {
            return;
        }

        error_node_group_id = it->second.cc_ng_id_;
        error_term = it->second.cc_ng_term_;
        uint64_t replay_start_ts = 0;

        // close all the streams belonging to the current node group and term.
        for (const auto &[stream_id, info] : inbound_connections_)
        {
            assert(replay_start_ts == 0 ||
                   replay_start_ts == info.replay_start_ts_);
            replay_start_ts = info.replay_start_ts_;
            if (info.cc_ng_id_ == error_node_group_id &&
                info.cc_ng_term_ == error_term)
            {
                brpc::StreamClose(stream_id);
            }
        }
        // put the replay log request back to the replay queue, but the replay
        // request will be scheduled with 10 senconds delay. log_id = -1 means
        // replay from all the log groups.
        ReplayLog(error_node_group_id, error_term, -1, replay_start_ts, true);
    }
}

void RecoveryService::ClearTx(uint64_t tx_number)
{
    ClearTxCc req(local_shards_.Count());
    req.Set(tx_number);

    for (uint32_t core_id = 0; core_id < local_shards_.Count(); ++core_id)
    {
        local_shards_.EnqueueCcRequest(core_id, &req);
    }

    req.Wait();
}

void RecoveryService::ProcessReplayLogTask(ReplayLogTask &task)
{
    if (Sharder::Instance().CandidateLeaderTerm(task.cc_ng_id_) < 0)
    {
        LOG(INFO) << "node group is not recovering, skip replay.";
        // node group is not recovering, skip replay.
        return;
    }
    // call log agent replay log api
    log_agent_->ReplayLog(task.cc_ng_id_,
                          task.cc_ng_term_,
                          ip_,
                          port_,
                          task.log_group_,
                          task.replay_start_ts_,
                          finish_);
}

int RecoveryService::ProcessDelayedReplayLogTask()
{
    // move the delayed replay requests whose timer is fired to
    // replay_log_queue_
    int ready_cnt = 0;
    for (auto it = delayed_replay_queue_.begin();
         it != delayed_replay_queue_.end();)
    {
        if (ReplayNow(*it))
        {
            replay_log_queue_.emplace_back(*it);
            it = delayed_replay_queue_.erase(it);
            ready_cnt++;
        }
        else
        {
            it++;
        }
    }
    return ready_cnt;
}

void RecoveryService::ProcessRecoverTxTask(RecoverTxTask &task)
{
    // process RecoverTx request

    // Recovering a tx's lock consists of two parts: (1)
    // inquires the tx status in the cc node in which the tx
    // resides, and (2) if the tx's status is committed or the
    // tx is not found, checks the tx status in the log group.

    // The tx node ID is represented by the higher 4 bytes, in
    // which the lower 10 bits represents the local core ID.
    uint32_t tx_ng = (task.tx_number_ >> 32L) >> 10;
    uint32_t tx_leader = Sharder::Instance().LeaderNodeId(tx_ng);
    remote::CheckTxStatusResponse_TxStatus tx_status;
    auto all_node_groups = Sharder::Instance().AllNodeGroups();
    if (all_node_groups->find(tx_ng) == all_node_groups->end())
    {
        // Node group is already removed from cluster. Need to ask log for
        // tx status.
        tx_status = remote::CheckTxStatusResponse_TxStatus_RESULT_UNKNOWN;
    }
    else if (tx_leader == Sharder::Instance().NodeId())
    {
        CheckTxStatusCc check_tx_cc(task.tx_number_);
        local_shards_.EnqueueCcRequest(task.tx_number_ >> 32L, &check_tx_cc);
        check_tx_cc.Wait();

        if (check_tx_cc.Exists())
        {
            switch (check_tx_cc.TxStatus())
            {
            case TxnStatus::Committed:
                tx_status = remote::CheckTxStatusResponse_TxStatus_COMMITTED;
                break;
            case TxnStatus::Unknown:
                tx_status =
                    remote::CheckTxStatusResponse_TxStatus_RESULT_UNKNOWN;
                break;
            case TxnStatus::Aborted:
                tx_status = remote::CheckTxStatusResponse_TxStatus_ABORTED;
                break;
            default:
                tx_status = remote::CheckTxStatusResponse_TxStatus_ONGOING;
                break;
            }
        }
        else
        {
            tx_status = remote::CheckTxStatusResponse_TxStatus_NOT_FOUND;
        }
    }
    else
    {
        auto channel = Sharder::Instance().GetCcNodeServiceChannel(tx_leader);
        if (channel == nullptr)
        {
            // Fails to establish the channel to the tx node.
            // Silently returns. The tx will be recovered again
            // by next conflicting tx.
            return;
        }

        remote::CcRpcService_Stub stub(channel.get());

        remote::CheckTxStatusRequest req;
        req.set_tx_number(task.tx_number_);
        req.set_tx_term(task.tx_term_);
        remote::CheckTxStatusResponse res;

        brpc::Controller cntl;
        stub.CheckTxStatus(&cntl, &req, &res, nullptr);

        if (cntl.Failed())
        {
            LOG(ERROR) << "Failed to check the tx status in ng#" << tx_ng
                       << ". Error code: " << cntl.ErrorCode()
                       << ". Msg: " << cntl.ErrorText();
            Sharder::Instance().UpdateCcNodeServiceChannel(tx_leader, channel);
            return;
        }

        tx_status = res.tx_status();
    }

    if (tx_status == remote::CheckTxStatusResponse_TxStatus_ONGOING)
    {
        LOG(INFO) << "The tx " << task.tx_number_
                  << " is ongoing. Does nothing for recovery.";
        return;
    }
    else if (tx_status == remote::CheckTxStatusResponse_TxStatus_ABORTED)
    {
        LOG(INFO) << "The tx" << task.tx_number_
                  << " has aborted. Clears the tx's lock.";
        ClearTx(task.tx_number_);
    }
    else
    {
        // The tx is either committed or result unknown or not
        // found in the tx's cc node, either because the tx node
        // fails or because the tx didn't finish post-processing
        // but decided to move on. In either case, asks the log
        // group: if the tx has committed, the log group ships
        // the tx's log record to the cc node to recover the
        // committed record. Or, the tx must have aborted.

        assert(log_agent_ != nullptr);
        RecoverTxStatus status = log_agent_->RecoverTx(task.tx_number_,
                                                       task.tx_term_,
                                                       task.write_lock_ts_,
                                                       task.cc_ng_id_,
                                                       task.cc_ng_term_,
                                                       ip_,
                                                       port_);

        if (status == RecoverTxStatus::NotCommitted ||
            status == RecoverTxStatus::Alive)
        {
            LOG(INFO) << "The tx " << task.tx_number_
                      << " is to be cleared, after asking "
                         "the log group. status: "
                      << int(status);

            // If the tx is not committed, sends a cc request to
            // local cc shards to clear write intentions left by
            // the tx. If the tx node is still alive according
            // to the log group, and yet no log record is found,
            // given that the prior inquiry of the tx status is
            // inconclusive, the tx must have aborted
            // proactively. Clears the tx's locks.
            ClearTx(task.tx_number_);
        }
        else if (status == RecoverTxStatus::RecoverError)
        {
            LOG(INFO) << "There is a tx recovery error when asking "
                         "the log group. Tx number "
                      << task.tx_number_;
        }
        else
        {
            LOG(INFO) << "The tx " << task.tx_number_
                      << " to be recovered has committed.";
            // For DML transactions, if the tx has committed,
            // the log group will ship the tx's committed
            // records to the cc node. If there is an error,
            // does nothing. The next conflicting tx will try a
            // new recovery.
            // For multi-stage transactions, the tx has written
            // log and is guaranteed to succeed and release the
            // lock, do nothing and the lock will be released by
            // the coordinator.
        }
    }
}

}  // namespace fault
}  // namespace txservice
