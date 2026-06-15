#pragma once

#include <braft/util.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/stream.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "fault_inject.h"
#include "log.pb.h"
#include "log_state.h"

namespace txlog
{
/*
 * Agent to ship redo log records to cc node leader, who is waiting for
 * uncheckpointed log record to replay. Shipping agent is implemented as a
 * separate thread
 *
 */
class LogShippingAgent
{
public:
    LogShippingAgent(uint32_t log_group_id,
                     uint32_t cc_ng_id,
                     int64_t cc_ng_term,
                     const std::string &ip,
                     uint16_t port,
                     std::unique_ptr<ItemIterator> &&iterator,
                     uint32_t latest_txn_no,
                     uint64_t latest_commit_ts,
                     uint64_t last_ckpt_ts,
                     bool start_with_replay)
        : log_group_id_(log_group_id),
          cc_node_group_id_(cc_ng_id),
          cc_node_group_term_(cc_ng_term),
          full_ip_(ip + ":" + std::to_string(port)),
          iterator_(std::move(iterator)),
          latest_txn_no_(latest_txn_no),
          latest_commit_ts_(latest_commit_ts),
          last_ckpt_ts_(last_ckpt_ts),
          start_with_replay_(start_with_replay)
    {
        stream_write_options_.write_in_background = true;
        thd_ = std::thread(
            [this]
            {
                brpc::ChannelOptions options;
                options.protocol = brpc::PROTOCOL_BAIDU_STD;
                options.timeout_ms = 100;
                options.max_retry = 3;
                butil::ip_t ip_t;
                int err;
                size_t comma_pos = full_ip_.find(':');
                assert(comma_pos != std::string::npos);
                std::string node_ip_str = full_ip_.substr(0, comma_pos);
                uint16_t node_port = std::stoi(full_ip_.substr(comma_pos + 1));
                if (0 != butil::str2ip(node_ip_str.c_str(), &ip_t))
                {
                    // for case `node_ip_str` is hostname format.
                    std::string naming_service_url;
                    braft::HostNameAddr hostname_addr(node_ip_str, node_port);
                    braft::HostNameAddr2NSUrl(hostname_addr,
                                              naming_service_url);
                    err = channel_.Init(naming_service_url.c_str(),
                                        braft::LOAD_BALANCER_NAME,
                                        &options);
                }
                else
                {
                    err = channel_.Init(full_ip_.c_str(), &options);
                }
                while (err != 0)
                {
                    if (shipping_agent_status_.load(
                            std::memory_order_acquire) != Status::Active)
                    {
                        shipping_agent_status_.store(Status::Terminated,
                                                     std::memory_order_release);
                        return;
                    }
                    LOG(ERROR)
                        << "Log shipping agent of the log group #"
                        << log_group_id_
                        << " fails to connect to the cc node at " << full_ip_;
                    using namespace std::chrono_literals;
                    std::this_thread::sleep_for(1s);
                }

                while (ConnectStream() != 0)
                {
                    if (shipping_agent_status_.load(
                            std::memory_order_acquire) != Status::Active)
                    {
                        shipping_agent_status_.store(Status::Terminated,
                                                     std::memory_order_release);
                        return;
                    }
                    using namespace std::chrono_literals;
                    std::this_thread::sleep_for(1s);
                }

                if (start_with_replay_)
                {
                    LOG(INFO) << "log group " << log_group_id_
                              << " shipping agent starts shipping replay log "
                                 "records to cc node group "
                              << cc_node_group_id_;
                    ACTION_FAULT_INJECTOR("log_during_log_shipping");
                    if (Send() == 0)
                    {
                        // Sends a concluding message to notify the recovery
                        // leader that all uncheckpointed records have been
                        // sent.
                        ReplayMessage replay_msg;
                        replay_msg.set_cc_node_group_id(cc_node_group_id_);
                        replay_msg.set_cc_node_group_term(cc_node_group_term_);
                        ReplayFinishMsg *finish_msg =
                            replay_msg.mutable_finish();
                        finish_msg->set_log_group_id(log_group_id_);
                        finish_msg->set_latest_txn_no(latest_txn_no_);
                        finish_msg->set_latest_commit_ts(latest_commit_ts_);
                        LOG(INFO)
                            << "set latest commit ts to " << latest_commit_ts_;
                        finish_msg->set_last_ckpt_ts(last_ckpt_ts_);

                        int eagain = 0;
                        if (SendMessage(replay_msg, iobuf_, true, eagain) != 0)
                        {
                            // interrupted or stream invalid
                            LOG(ERROR) << "log group " << log_group_id_
                                       << " faild to send finish message to cc"
                                       << " node group " << cc_node_group_id_;
                            brpc::StreamClose(stream_id_);
                            shipping_agent_status_.store(
                                Status::Terminated, std::memory_order_release);
                            return;
                        }
                    }
                }

                // Waits for the incoming request to send tx logs to recover
                // orphan locks in the cc node group leader.
                while (shipping_agent_status_.load(std::memory_order_acquire) ==
                       Status::Active)
                {
                    {
                        std::unique_lock<std::mutex> lk(to_send_mux_);
                        to_send_cv_.wait(
                            lk,
                            [this]()
                            {
                                return !to_send_list_.empty() ||
                                       shipping_agent_status_.load(
                                           std::memory_order_acquire) !=
                                           Status::Active;
                            });

                        assert(recovered_txn_log_list_.size() == 0);
                        recovered_txn_log_list_.swap(to_send_list_);
                    }

                    if (!recovered_txn_log_list_.empty())
                    {
                        LOG(INFO)
                            << "send recovered txn records to cc node group: "
                            << cc_node_group_id_;
                        // write to stream directly in this thread, reconnect
                        // the stream if it is broken as recovered txns are rare
                        // and they are sent to cc node asynchronously, so it's
                        // LogShippingAgent's responsibility to reconnect the
                        // stream.
                        WriteToStreamInBatch(recovered_txn_log_list_);
                        recovered_txn_log_list_.clear();
                    }
                }
                brpc::StreamClose(stream_id_);
                shipping_agent_status_.store(Status::Terminated,
                                             std::memory_order_release);
            });
    }

    ~LogShippingAgent()
    {
        Status expected = Status::Active;
        if (shipping_agent_status_.compare_exchange_strong(
                expected, Status::Terminating, std::memory_order_acq_rel))
        {
            std::unique_lock<std::mutex> lk(to_send_mux_);
            to_send_cv_.notify_all();
        }

        if (thd_.joinable())
        {
            thd_.join();
        }
    }

    void Terminate()
    {
        // The log shipping worker may be already terminated when the stream is
        // invalidated, so we need to check the status before terminating.
        Status expected = Status::Active;
        if (shipping_agent_status_.compare_exchange_strong(
                expected, Status::Terminating, std::memory_order_acq_rel))
        {
            std::unique_lock<std::mutex> lk(to_send_mux_);
            to_send_cv_.notify_all();
        }
    }

    bool IsTerminated()
    {
        return shipping_agent_status_.load(std::memory_order_acquire) ==
               Status::Terminated;
    }

    void AddLogRecord(Item::Pointer log_rec)
    {
        std::unique_lock<std::mutex> lk(to_send_mux_);
        to_send_list_.emplace_back(std::move(log_rec));
        to_send_cv_.notify_one();
    }

    void AddLogRecord(std::vector<Item::Pointer> &log_vec)
    {
        std::unique_lock<std::mutex> lk(to_send_mux_);
        to_send_list_.insert(
            to_send_list_.end(), log_vec.begin(), log_vec.end());
        to_send_cv_.notify_one();
    }

    [[nodiscard]] int64_t NodeGroupTerm() const
    {
        return cc_node_group_term_;
    }

private:
    int ConnectStream()
    {
        LOG(INFO) << "Log shipping agent from log group " << log_group_id_
                  << " to cc node group " << cc_node_group_id_ << " term "
                  << cc_node_group_term_
                  << " trying to create a new stream to cc node at "
                  << full_ip_;
        brpc::Controller cntl;
        LogReplayService_Stub stub(&channel_);
        brpc::StreamOptions stream_option;
        stream_option.max_buf_size = remote_buf_size_;
        int err = brpc::StreamCreate(&stream_id_, cntl, &stream_option);
        if (err != 0)
        {
            LOG(ERROR) << "Log shipping agent of the log group #"
                       << log_group_id_
                       << " fails to create the stream to the cc node at "
                       << full_ip_;
            return err;
        }

        LogReplayConnectRequest req;
        LogReplayConnectResponse resp;
        req.set_log_group_id(log_group_id_);
        req.set_cc_node_group_id(cc_node_group_id_);
        req.set_cc_ng_term(cc_node_group_term_);
        stub.Connect(&cntl, &req, &resp, nullptr);
        err = cntl.Failed() ? cntl.ErrorCode() : (resp.success() ? 0 : -1);
        if (err != 0)
        {
            LOG(ERROR) << "Log shipping agent of the log group #"
                       << log_group_id_
                       << " fails to establish the stream to the cc node at "
                       << full_ip_;
            return err;
        }

        return 0;
    }

    int Send()
    {
        auto start = std::chrono::high_resolution_clock::now();
        std::atomic<int> total{0};
        std::atomic<int> eagain{0};

        int err = WriteToStreamInBatch(iterator_.get(), total, eagain);

        auto stop = std::chrono::high_resolution_clock::now();
        long us =
            std::chrono::duration_cast<std::chrono::microseconds>(stop - start)
                .count();
        LOG(INFO)
            << "LogShippingAgent shipping replay log records to cc node group: "
            << cc_node_group_id_ << ", term: " << cc_node_group_term_
            << " finished, sent " << total << " messages, took " << us
            << " microseconds, " << eagain.load(std::memory_order_relaxed)
            << " eagains";
        // release rocksdb iterator
        iterator_.reset();
        return err;
    }

    /**
     * Write recovered txn's log to stream. Reconnect the stream if write fail.
     * @param send_list
     */
    void WriteToStreamInBatch(std::vector<Item::Pointer> &send_list)
    {
        if (send_list.empty())
        {
            return;
        }

        ReplayMessage replay_msg;
        replay_msg.set_is_lock_recovery(true);
        replay_msg.set_cc_node_group_id(cc_node_group_id_);
        replay_msg.set_cc_node_group_term(cc_node_group_term_);

        std::string *log_records_blob = replay_msg.mutable_binary_log_records();
        log_records_blob->reserve(log_records_batch_size);

        butil::IOBuf buf;
        int eagain = 0;
        for (const auto &item : send_list)
        {
            if (shipping_agent_status_.load(std::memory_order_acquire) !=
                Status::Active)
            {
                return;
            }

            if (item->item_type_ == LogItemType::ClusterScaleLog)
            {
                ReplayClusterScaleMsg *scale_msg =
                    replay_msg.mutable_cluster_scale_op_msg();
                scale_msg->set_commit_ts(item->timestamp_);
                scale_msg->clear_cluster_scale_op_blob();
                scale_msg->set_cluster_scale_op_blob(item->log_message_);
                scale_msg->set_txn(item->tx_number_);
            }
            else if (item->item_type_ == LogItemType::SchemaLog)
            {
                ReplaySchemaMsg *schema_msg = replay_msg.add_schema_op_msgs();
                schema_msg->set_commit_ts(item->timestamp_);
                schema_msg->clear_schema_op_blob();
                schema_msg->set_schema_op_blob(item->log_message_);
                schema_msg->set_txn(item->tx_number_);
            }
            else if (item->item_type_ == LogItemType::SplitRangeLog)
            {
                ReplaySplitRangeMsg *split_range_msg =
                    replay_msg.add_split_range_op_msgs();
                split_range_msg->set_commit_ts(item->timestamp_);
                split_range_msg->clear_split_range_op_blob();
                split_range_msg->set_split_range_op_blob(item->log_message_);
                split_range_msg->set_txn(item->tx_number_);
            }
            else if (item->item_type_ == LogItemType::DataLog)
            {
                if (log_records_blob->size() + sizeof(uint64_t) * 3 +
                        item->log_message_.size() >
                    log_records_batch_size)
                {
                    // log_records_blob exceeds log_records_batch_size, send
                    // message to stream.
                    if (SendMessage(replay_msg, buf, true, eagain) != 0)
                    {
                        // interrupted or stream invalid
                        return;
                    }
                    replay_msg.clear_binary_log_records();
                    replay_msg.clear_schema_op_msgs();
                    replay_msg.clear_split_range_op_msgs();
                    replay_msg.clear_cluster_scale_op_msg();
                }
                uint32_t message_length = item->log_message_.size();
                log_records_blob->append(
                    reinterpret_cast<const char *>(&item->timestamp_),
                    sizeof(uint64_t));
                log_records_blob->append(
                    reinterpret_cast<const char *>(&item->tx_number_),
                    sizeof(uint64_t));
                log_records_blob->append(
                    reinterpret_cast<char *>(&message_length),
                    sizeof(uint32_t));
                log_records_blob->append(item->log_message_.data(),
                                         message_length);
            }
        }

        SendMessage(replay_msg, buf, true, eagain);
    }

    /**
     * Read log items from iterator and write to stream in batch
     * @param iterator
     * @param total
     * @param eagain_cnt
     *
     * @return error_code
     */
    int WriteToStreamInBatch(ItemIterator *iterator,
                             std::atomic<int> &total,
                             std::atomic<int> &eagain_cnt)
    {
        LOG(INFO) << "thread: " << std::this_thread::get_id()
                  << " send log item list to replay service of cc node group: "
                  << cc_node_group_id_ << ", term: " << cc_node_group_term_;
        ReplayMessage replay_msg;
        replay_msg.set_cc_node_group_id(cc_node_group_id_);
        replay_msg.set_cc_node_group_term(cc_node_group_term_);

        std::string *log_records_blob = replay_msg.mutable_binary_log_records();
        log_records_blob->reserve(log_records_batch_size);

        butil::IOBuf buf;
        int idx = 0;
        int cnt = 0;
        int eagain = 0;
        for (iterator->SeekToDDLFirst(); iterator->ValidDDL();
             iterator->NextDDL(), idx++)
        {
            cnt++;
            const Item &item = iterator->GetDDLItem();
            if (item.item_type_ == LogItemType::ClusterScaleLog)
            {
                ReplayClusterScaleMsg *scale_msg =
                    replay_msg.mutable_cluster_scale_op_msg();
                scale_msg->set_commit_ts(item.timestamp_);
                scale_msg->clear_cluster_scale_op_blob();
                scale_msg->set_cluster_scale_op_blob(item.log_message_);
                scale_msg->set_txn(item.tx_number_);
            }
            else if (item.item_type_ == LogItemType::SchemaLog)
            {
                ReplaySchemaMsg *schema_msg = replay_msg.add_schema_op_msgs();
                schema_msg->set_commit_ts(item.timestamp_);
                schema_msg->clear_schema_op_blob();
                schema_msg->set_schema_op_blob(item.log_message_);
                schema_msg->set_txn(item.tx_number_);
            }
            else if (item.item_type_ == LogItemType::SplitRangeLog)
            {
                ReplaySplitRangeMsg *split_range_msg =
                    replay_msg.add_split_range_op_msgs();
                split_range_msg->set_commit_ts(item.timestamp_);
                split_range_msg->clear_split_range_op_blob();
                split_range_msg->set_split_range_op_blob(item.log_message_);
                split_range_msg->set_txn(item.tx_number_);
            }
        }
        if (const int err = SendMessage(replay_msg, buf, false, eagain);
            err != 0)
        {
            return err;
        }
        std::atomic<int> data_log_send_err{0};
        eagain_cnt.fetch_add(eagain, std::memory_order_relaxed);
        total.fetch_add(cnt, std::memory_order_relaxed);
        std::vector<std::thread> thds;
        thds.reserve(iterator->IteratorNum());
        for (size_t i = 0; i < iterator->IteratorNum(); i++)
        {
            thds.emplace_back(std::thread(
                [&, i]
                {
                    int eagain = 0;
                    int cnt = 0;
                    int msg_cnt = 0;
                    ReplayMessage replay_msg;
                    replay_msg.set_cc_node_group_id(cc_node_group_id_);
                    replay_msg.set_cc_node_group_term(cc_node_group_term_);

                    std::string *log_records_blob =
                        replay_msg.mutable_binary_log_records();
                    log_records_blob->reserve(log_records_batch_size);

                    butil::IOBuf buf;
                    for (iterator->SeekToFirst(i); iterator->Valid(i);
                         iterator->Next(i))
                    {
                        cnt++;
                        const Item &item = iterator->GetItem(i);
                        if (item.item_type_ == LogItemType::DataLog)
                        {
                            if (log_records_blob->size() +
                                    sizeof(uint64_t) * 2 +
                                    item.log_message_.size() >
                                log_records_batch_size)
                            {
                                // log_records_blob exceeds
                                // log_records_batch_size, send message to
                                // stream.
                                int err =
                                    SendMessage(replay_msg, buf, false, eagain);
                                if (err != 0)
                                {
                                    data_log_send_err.store(
                                        err, std::memory_order_relaxed);
                                    return;
                                }
                                msg_cnt++;
                                replay_msg.clear_binary_log_records();
                            }

                            // filter out log items not belong the
                            // cc_node_group_id_
                            if (item.cc_ng_ == cc_node_group_id_)
                            {
                                uint32_t message_length =
                                    item.log_message_.size();
                                log_records_blob->append(
                                    reinterpret_cast<const char *>(
                                        &item.timestamp_),
                                    sizeof(uint64_t));
                                log_records_blob->append(
                                    reinterpret_cast<char *>(&message_length),
                                    sizeof(uint32_t));
                                log_records_blob->append(
                                    item.log_message_.data(), message_length);
                            }
                        }
                    }
                    if (const int err =
                            SendMessage(replay_msg, buf, false, eagain);
                        err != 0)
                    {
                        data_log_send_err.store(err, std::memory_order_relaxed);
                        return;
                    }
                    msg_cnt++;
                    eagain_cnt.fetch_add(eagain, std::memory_order_relaxed);
                    total.fetch_add(cnt, std::memory_order_relaxed);
                }));
        }
        for (auto &thd : thds)
        {
            thd.join();
        }
        return data_log_send_err.load(std::memory_order_relaxed);
    }

    /**
     * Send msg to stream, if failed, whether reconnecting or not depends on
     * parameter reconnect_if_fail.
     * During cc node leader failover recover phase, LogShippingAgent does not
     * try to reconnect the stream if it is broken as the cc node side will
     * timeout and resend ReplyLogRequest. After finish message sent, when
     * sending recovered txn records later, it's LogShippingAgent's
     * responsibility to reconnect the stream as recovered txn is rare and it is
     * sent to cc node asynchronously.
     * @param msg
     * @param buf
     * @param reconnect_if_fail
     * @return
     */
    int SendMessage(const ReplayMessage &msg,
                    butil::IOBuf &buf,
                    bool reconnect_if_fail,
                    int &eagain)
    {
        buf.clear();
        butil::IOBufAsZeroCopyOutputStream wrapper(&buf);
        msg.SerializeToZeroCopyStream(&wrapper);

        int error_code =
            brpc::StreamWrite(stream_id_, buf, &stream_write_options_);
        while (error_code != 0 &&
               shipping_agent_status_.load(std::memory_order_acquire) ==
                   Status::Active)
        {
            if (error_code == EAGAIN)
            {
                eagain++;
                error_code =
                    brpc::StreamWrite(stream_id_, buf, &stream_write_options_);
            }
            else  // EINVAL
            {
                LOG(INFO) << "send message to stream failed, reconnect: "
                          << (reconnect_if_fail ? "yes" : "no")
                          << " cc_ng: " << cc_node_group_id_;
                if (reconnect_if_fail)
                {
                    brpc::StreamClose(stream_id_);
                    // try to reconnect the stream and repeat the StreamWrite
                    // after reconnect
                    if (ConnectStream() == 0)
                    {
                        error_code = brpc::StreamWrite(
                            stream_id_, buf, &stream_write_options_);
                    }
                    else
                    {
                        using namespace std::chrono_literals;
                        std::this_thread::sleep_for(500ms);
                    }
                }
                else
                {
                    break;
                }
            }
        }
        // error_code == 0 or interrupted or stream invalid
        return error_code;
    }

    enum struct Status
    {
        Active,
        Terminating,
        Terminated
    };

    const uint32_t log_group_id_;
    const uint32_t cc_node_group_id_;
    const int64_t cc_node_group_term_;
    const std::string full_ip_;
    brpc::Channel channel_;
    brpc::StreamWriteOptions stream_write_options_;
    brpc::StreamId stream_id_{};

    // iterator to get log items to be shipped
    std::unique_ptr<ItemIterator> iterator_;

    // a list of log records of recovered txn to send
    std::vector<Item::Pointer> recovered_txn_log_list_;
    // store recovered txn records temporarily, waiting for thd_ to fetch
    std::vector<Item::Pointer> to_send_list_;
    std::mutex to_send_mux_;
    std::condition_variable to_send_cv_;
    uint32_t latest_txn_no_{};
    uint64_t latest_commit_ts_{};
    uint64_t last_ckpt_ts_{};
    bool start_with_replay_;

    butil::IOBuf iobuf_;

    // remote side unconsumed buffer size, default 32MB
    static const int remote_buf_size_ = 32 * 1024 * 1024;
    // binary log_blobs batch size, default 32KB
    static const size_t log_records_batch_size = 32 * 1024;

    // The thread that ships the log record to the recovering leader of the cc
    // node group.
    std::thread thd_;

    // A log group maintains at most one log shipping agent for a cc node group
    // at any time. When a cc node group fails over to a new leader before the
    // old leader finishes log replay, the new leader's log replay request will
    // create a second log shipping agent and interruptes/de-allocates the first
    // one.
    std::atomic<Status> shipping_agent_status_{Status::Active};
};
}  // namespace txlog
