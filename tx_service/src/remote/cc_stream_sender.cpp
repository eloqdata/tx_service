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
#include "remote/cc_stream_sender.h"

#include <arpa/inet.h>
#include <bvar/latency_recorder.h>
#include <ifaddrs.h>
#include <netdb.h>  // getaddrinfo

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>

#include "sharder.h"
#include "tx_execution.h"
#include "tx_trace.h"

namespace txservice
{
namespace remote
{
CcStreamSender::~CcStreamSender()
{
    terminate_.store(true, std::memory_order_release);

    to_connect_cv_.notify_one();
    resend_cv_.notify_one();

    connect_thd_.join();
    resend_thd_.join();
}

CcStreamSender::CcStreamSender(
    moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> &msg_pool)
    : msg_pool_(msg_pool), terminate_(false), to_connect_flag_(false)
{
    stream_write_options_.write_in_background = true;
    connect_thd_ = std::thread([this] { ConnectStreams(); });
    resend_thd_ = std::thread([this] { ResendMessageToNode(); });
    pthread_setname_np(connect_thd_.native_handle(), "ccstream_conn");
    pthread_setname_np(resend_thd_.native_handle(), "ccstream_resend");
}

void CcStreamSender::RecycleCcMsg(std::unique_ptr<CcMessage> msg)
{
    msg_pool_.enqueue(std::move(msg));
}

void CcStreamSender::ReConnectStream(uint32_t dest_node_id)
{
    std::shared_lock<std::shared_mutex> outbound_lk(outbound_mux_);
    auto stream_it = outbound_streams_.find(dest_node_id);
    if (stream_it == outbound_streams_.end())
    {
        return;
    }

    std::atomic<int64_t> &stream_version = std::get<1>(stream_it->second);
    int64_t stream_ver = stream_version.load(std::memory_order_acquire);

    std::lock_guard<std::mutex> lk(to_connect_mux_);
    // If the stream version is -1, a separate thread has notified
    // the connecting thread to reconnect the stream. If the stream
    // version is greater than the previously-read one (stream_ver),
    // a new stream has been connected. In either case, the current
    // thread does not initiated a reconnection.
    if (stream_version.compare_exchange_strong(
            stream_ver, -1, std::memory_order_release))
    {
        to_connect_regular_streams_.try_emplace(dest_node_id, stream_ver + 1);
    }
}

void CcStreamSender::ReConnectLongMsgStream(uint32_t dest_node_id)
{
    std::shared_lock<std::shared_mutex> outbound_lk(outbound_mux_);
    auto stream_it = long_msg_outbound_streams_.find(dest_node_id);
    if (stream_it == long_msg_outbound_streams_.end())
    {
        return;
    }

    std::atomic<int64_t> &stream_version = std::get<1>(stream_it->second);
    int64_t stream_ver = stream_version.load(std::memory_order_acquire);

    std::lock_guard<std::mutex> lk(to_connect_mux_);
    // If the stream version is -1, a separate thread has notified
    // the connecting thread to reconnect the stream. If the stream
    // version is greater than the previously-read one (stream_ver),
    // a new stream has been connected. In either case, the current
    // thread does not initiated a reconnection.
    if (stream_version.compare_exchange_strong(
            stream_ver, -1, std::memory_order_release))
    {
        to_connect_long_msg_streams_.try_emplace(dest_node_id, stream_ver + 1);
    }
}

bool CcStreamSender::UpdateStreamIP(uint32_t node_id,
                                    const std::string &new_connection_ip)
{
    std::shared_lock<std::shared_mutex> outbound_lk(outbound_mux_);
    auto stream_it = outbound_streams_.find(node_id);
    if (stream_it == outbound_streams_.end())
    {
        return false;
    }

    std::string &stream_ip = std::get<2>(stream_it->second);
    if (stream_ip.empty())
    {
        // set stream ip with the ip of the first connection.
        std::get<2>(stream_it->second) = new_connection_ip;
        return false;
    }
    else if (stream_ip != new_connection_ip)
    {
        // return true to indicate ip changed
        std::get<2>(stream_it->second) = new_connection_ip;
        return true;
    }

    return false;
}

/**
 * @brief Send message to a specific node. Failed message will
 * be put into a retry list once the stream to the node is
 * reconstructed.
 *
 * @param dest_node_id
 * @param msg
 * @param res
 * @param resend
 */
SendMessageResult CcStreamSender::SendMessageToNode(uint32_t dest_node_id,
                                                    const CcMessage &msg,
                                                    CcHandlerResultBase *res,
                                                    bool resend)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &msg,
        (
            [&dest_node_id]() -> std::string
            {
                return std::string("{\"dest_node_id\":")
                    .append(std::to_string(dest_node_id))
                    .append("}");
            }));
    TX_TRACE_DUMP(&msg);
    std::shared_lock<std::shared_mutex> outbound_lk(outbound_mux_);
    auto stream_it = outbound_streams_.find(dest_node_id);
    if (stream_it == outbound_streams_.end())
    {
        // SendMessage error return -1 to indicate the request needs retry.
        if (res != nullptr)
        {
            res->SetLocalOrRemoteError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }

        LOG(ERROR) << "Trying to connect to an unknown remote node. Node Id: "
                   << dest_node_id;
        return SendMessageResult::Failed();
    }

    std::atomic<int64_t> &stream_version = std::get<1>(stream_it->second);
    int64_t stream_ver = stream_version.load(std::memory_order_acquire);
    if (stream_ver == -1)
    {
        // resend the message if stream is connecting
        std::lock_guard<std::mutex> lk(to_connect_mux_);
        auto resend_message_list = resend_message_list_.try_emplace(
            dest_node_id, moodycamel::ConcurrentQueue<ResendMessage::Uptr>());
        resend_message_list.first->second.enqueue(
            std::make_unique<ResendMessage>(msg, res));

        // always wake up connector thread to either reconnect streams or
        // resend messages.
        to_connect_flag_ = true;
        to_connect_cv_.notify_one();
        return SendMessageResult::Queued(true);
    }

    brpc::StreamId stream_id = std::get<0>(stream_it->second);

    outbound_lk.unlock();

    butil::IOBuf iobuf;
    butil::IOBufAsZeroCopyOutputStream wrapper(&iobuf);
    msg.SerializeToZeroCopyStream(&wrapper);

    int error_code =
        brpc::StreamWrite(stream_id, iobuf, &stream_write_options_);

    if (error_code != 0)
    {
        if (error_code == EAGAIN)
        {
            std::lock_guard<std::mutex> resend_lk(resend_mux_);

            auto bg_resend_msg_list =
                eagain_resend_message_list_.find(dest_node_id);
            if (bg_resend_msg_list != eagain_resend_message_list_.end())
            {
                eagain_resend_message_cnt_ += 1;
                bg_resend_msg_list->second.enqueue(
                    std::make_unique<ResendMessage>(msg, res));

                if (resend_thread_status_ == ResendThreadStatus::Sleeping)
                {
                    resend_thread_status_ = ResendThreadStatus::Running;
                    resend_cv_.notify_one();
                }
            }
            else
            {
                auto bg_resend_msg_list =
                    eagain_resend_message_list_.try_emplace(
                        dest_node_id,
                        moodycamel::ConcurrentQueue<ResendMessage::Uptr>());
                eagain_resend_message_cnt_ += 1;
                bg_resend_msg_list.first->second.enqueue(
                    std::make_unique<ResendMessage>(msg, res));

                if (resend_thread_status_ == ResendThreadStatus::Sleeping)
                {
                    resend_thread_status_ = ResendThreadStatus::Running;
                    resend_cv_.notify_one();
                }
            }

            return SendMessageResult::Queued(false);
        }
        else
        {
            // for resend message, we have already reconnect the stream, if it
            // still failed to send the message, it possibly means that the
            // remote node is dead. We should skip resend the message again.
            // TODO: discard the message? needs thorough thinking.
            if (resend)
            {
                // SendMessage error return -1 to indicate the request needs
                // retry.
                if (res != nullptr)
                {
                    TransactionExecution *txm =
                        reinterpret_cast<TransactionExecution *>(
                            msg.txm_addr());
                    if (txm != nullptr)
                    {
                        // Resend is called in a separate resend thread, so it
                        // won't cause deadlock with txm.Forward() on txm
                        // forward_latch_.
                        txm->AcquireSharedForwardLatch();
                    }
                    res->SetLocalOrRemoteError(
                        CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                    if (txm != nullptr)
                    {
                        txm->ReleaseSharedForwardLatch();
                    }
                }

                return SendMessageResult::Failed();
            }
            else
            {
                std::lock_guard<std::mutex> lk(to_connect_mux_);
                // If the stream version is -1, a separate thread has notified
                // the connecting thread to reconnect the stream. If the stream
                // version is greater than the previously-read one (stream_ver),
                // a new stream has been connected. In either case, the current
                // thread does not initiated a reconnection.
                if (stream_version.compare_exchange_strong(
                        stream_ver, -1, std::memory_order_release))
                {
                    to_connect_regular_streams_.try_emplace(dest_node_id,
                                                            stream_ver + 1);
                }
                // put the failed message into the resend_message_list.
                auto resend_message_list = resend_message_list_.try_emplace(
                    dest_node_id,
                    moodycamel::ConcurrentQueue<ResendMessage::Uptr>());
                resend_message_list.first->second.enqueue(
                    std::make_unique<ResendMessage>(msg, res));

                // always wake up connector thread to either reconnect streams
                // or resend messages.
                to_connect_flag_ = true;
                to_connect_cv_.notify_one();

                return SendMessageResult::Queued(true);
            }
        }
    }

    return SendMessageResult::Sent();
}

SendMessageResult CcStreamSender::SendScanRespToNode(
    uint32_t dest_node_id, const ScanSliceResponse &msg, bool resend)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &msg,
        (
            [&dest_node_id]() -> std::string
            {
                return std::string("{\"dest_node_id\":")
                    .append(std::to_string(dest_node_id))
                    .append("}");
            }));
    TX_TRACE_DUMP(&msg);

    std::shared_lock<std::shared_mutex> outbound_lk(outbound_mux_);
    auto stream_it = long_msg_outbound_streams_.find(dest_node_id);
    if (stream_it == long_msg_outbound_streams_.end())
    {
        LOG(ERROR) << "Trying to connect to an unknown remote node. Node Id: "
                   << dest_node_id;
        return SendMessageResult::Failed();
    }

    std::atomic<int64_t> &stream_version = std::get<1>(stream_it->second);
    int64_t stream_ver = stream_version.load(std::memory_order_acquire);
    if (stream_ver == -1)
    {
        // resend the message if stream is connecting
        std::lock_guard<std::mutex> lk(to_connect_mux_);
        DLOG(INFO) << "CC stream is connecting, buffer the message for resend";

        auto resend_message_list = long_msg_resend_message_list_.try_emplace(
            dest_node_id,
            moodycamel::ConcurrentQueue<ResendScanSliceResp::Uptr>());
        resend_message_list.first->second.enqueue(
            std::make_unique<ResendScanSliceResp>(msg));

        // always wake up connector thread to either reconnect streams or
        // resend messages.
        to_connect_flag_ = true;
        to_connect_cv_.notify_one();
        return SendMessageResult::Queued(true);
    }

    brpc::StreamId &stream_id = std::get<0>(stream_it->second);

    butil::IOBuf iobuf;
    butil::IOBufAsZeroCopyOutputStream wrapper(&iobuf);
    msg.SerializeToZeroCopyStream(&wrapper);

    int error_code =
        brpc::StreamWrite(stream_id, iobuf, &stream_write_options_);

    if (error_code != 0)
    {
        if (error_code == EAGAIN)
        {
            std::lock_guard<std::mutex> resend_lk(resend_mux_);

            auto bg_resend_long_msg_list =
                eagain_resend_long_message_list_.find(dest_node_id);
            if (bg_resend_long_msg_list !=
                eagain_resend_long_message_list_.end())
            {
                eagain_resend_long_message_cnt_ += 1;
                bg_resend_long_msg_list->second.enqueue(
                    std::make_unique<ResendScanSliceResp>(msg));
                if (resend_thread_status_ == ResendThreadStatus::Sleeping)
                {
                    resend_thread_status_ = ResendThreadStatus::Running;
                    resend_cv_.notify_one();
                }
            }
            else
            {
                auto bg_resend_long_msg_list =
                    eagain_resend_long_message_list_.try_emplace(
                        dest_node_id,
                        moodycamel::ConcurrentQueue<
                            ResendScanSliceResp::Uptr>());

                eagain_resend_long_message_cnt_ += 1;
                bg_resend_long_msg_list.first->second.enqueue(
                    std::make_unique<ResendScanSliceResp>(msg));
                if (resend_thread_status_ == ResendThreadStatus::Sleeping)
                {
                    resend_thread_status_ = ResendThreadStatus::Running;
                    resend_cv_.notify_one();
                }
            }

            return SendMessageResult::Queued(false);
        }
        else
        {
            // for resend message, we have already reconnect the stream, if it
            // still failed to send the message, it possibly means that the
            // remote node is dead. We should skip resend the message again.
            // TODO: discard the message? needs thorough thinking.
            if (resend)
            {
                return SendMessageResult::Failed();
            }
            else
            {
                std::lock_guard<std::mutex> lk(to_connect_mux_);
                // If the stream version is -1, a separate thread has notified
                // the connecting thread to reconnect the stream. If the stream
                // version is greater than the previously-read one (stream_ver),
                // a new stream has been connected. In either case, the current
                // thread does not initiated a reconnection.
                if (stream_version.compare_exchange_strong(
                        stream_ver, -1, std::memory_order_release))
                {
                    to_connect_long_msg_streams_.try_emplace(dest_node_id,
                                                             stream_ver + 1);
                }
                // put the failed message into the long_msg_resend_message_list.
                auto resend_message_list =
                    long_msg_resend_message_list_.try_emplace(
                        dest_node_id,
                        moodycamel::ConcurrentQueue<
                            ResendScanSliceResp::Uptr>());
                resend_message_list.first->second.enqueue(
                    std::make_unique<ResendScanSliceResp>(msg));

                // wake up connector thread to reconnect streams
                to_connect_flag_ = true;
                to_connect_cv_.notify_one();

                return SendMessageResult::Queued(true);
            }
        }
    }
    return SendMessageResult::Sent();
}

bool CcStreamSender::SendStandbyMessageToNode(uint32_t dest_node_id,
                                              const CcMessage &msg)
{
    std::shared_lock<std::shared_mutex> outbound_lk(outbound_mux_);
    auto stream_it = outbound_streams_.find(dest_node_id);
    if (stream_it == outbound_streams_.end())
    {
        LOG_EVERY_SECOND(ERROR)
            << "SendStandbyMessageToNode: unknown node " << dest_node_id;
        return false;
    }

    std::atomic<int64_t> &stream_version = std::get<1>(stream_it->second);
    if (stream_version.load(std::memory_order_acquire) == -1)
    {
        std::lock_guard<std::mutex> lk(to_connect_mux_);
        // wake up connector thread to reconnect streams
        to_connect_flag_ = true;
        to_connect_cv_.notify_one();
        return false;
    }

    brpc::StreamId stream_id = std::get<0>(stream_it->second);
    outbound_lk.unlock();

    butil::IOBuf iobuf;
    butil::IOBufAsZeroCopyOutputStream wrapper(&iobuf);
    msg.SerializeToZeroCopyStream(&wrapper);

    int error_code =
        brpc::StreamWrite(stream_id, iobuf, &stream_write_options_);
    if (error_code != 0)
    {
        DLOG(INFO) << "SendStandbyMessageToNode failed, node: " << dest_node_id
                   << ", err: " << error_code;
        return false;
    }

    return true;
}

/**
 * @brief Send a message to a node group leader. Failed message will
 * be put into a retry list once the stream to the node group leader is
 * reconstructed.
 *
 * @param node_group_id
 * @param msg
 * @param res
 * @param resend
 */
SendMessageResult CcStreamSender::SendMessageToNg(uint32_t node_group_id,
                                                  const CcMessage &msg,
                                                  CcHandlerResultBase *res,
                                                  bool resend)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(node_group_id);
    return SendMessageToNode(dest_node_id, msg, res, resend);
}

void CcStreamSender::UpdateRemoteNodes(
    const std::unordered_map<NodeId, NodeConfig> &nodes_configs)
{
    std::unique_lock<std::shared_mutex> lk(outbound_mux_);
    bool pending_connect_streams = false;

    for (const auto &[node_id, config] : nodes_configs)
    {
        // Connect to new nodes in new cluster configs.
        if (outbound_channels_.find(node_id) == outbound_channels_.end())
        {
            auto channel_it = outbound_channels_.try_emplace(node_id);
            const std::string &ip = config.host_name_;
            uint16_t port = config.port_;
            channel_it.first->second = ip + ":" + std::to_string(port);
            auto stream_it = outbound_streams_.try_emplace(node_id);
            std::get<0>(stream_it.first->second) = brpc::INVALID_STREAM_ID;
            std::get<1>(stream_it.first->second)
                .store(-1, std::memory_order_release);

            stream_it = long_msg_outbound_streams_.try_emplace(node_id);
            std::get<0>(stream_it.first->second) = brpc::INVALID_STREAM_ID;
            std::get<1>(stream_it.first->second)
                .store(-1, std::memory_order_release);

            {
                // Add it to the reconnect lists, the connect_thd_ will
                // connect to these nodes later.
                pending_connect_streams = true;
                std::unique_lock<std::mutex> to_connect_lk(to_connect_mux_);
                to_connect_regular_streams_.try_emplace(node_id, 0);
                to_connect_long_msg_streams_.try_emplace(node_id, 0);
            }
        }
    }

    std::unordered_set<uint32_t> removed_nodes;
    for (auto &[node_id, channel] : outbound_channels_)
    {
        // node is no longer in the new cluster config.
        if (nodes_configs.find(node_id) == nodes_configs.end())
        {
            {
                std::unique_lock<std::mutex> to_connect_lk(to_connect_mux_);
                to_connect_regular_streams_.erase(node_id);
                to_connect_long_msg_streams_.erase(node_id);
                resend_message_list_.erase(node_id);
                long_msg_resend_message_list_.erase(node_id);
            }

            {
                std::lock_guard<std::mutex> resend_lk(resend_mux_);

                auto message_list_it =
                    eagain_resend_message_list_.find(node_id);
                if (message_list_it != eagain_resend_message_list_.end())
                {
                    eagain_resend_message_cnt_ -=
                        message_list_it->second.size_approx();
                    eagain_resend_message_list_.erase(node_id);
                }

                auto long_message_list_it =
                    eagain_resend_long_message_list_.find(node_id);
                if (long_message_list_it !=
                    eagain_resend_long_message_list_.end())
                {
                    eagain_resend_long_message_cnt_ -=
                        long_message_list_it->second.size_approx();
                    eagain_resend_long_message_list_.erase(node_id);
                }
            }

            LOG(INFO) << "Closed cc stream to node " << node_id;
            brpc::StreamClose(std::get<0>(outbound_streams_.at(node_id)));
            outbound_streams_.erase(node_id);
            brpc::StreamClose(
                std::get<0>(long_msg_outbound_streams_.at(node_id)));
            long_msg_outbound_streams_.erase(node_id);
            removed_nodes.insert(node_id);
        }
    }
    for (auto nid : removed_nodes)
    {
        outbound_channels_.erase(nid);
    }

    if (pending_connect_streams)
    {
        std::unique_lock<std::mutex> lk(to_connect_mux_);
        spam_stream_connect_ = true;
    }
}

void CcStreamSender::NotifyConnectStream()
{
    std::lock_guard<std::mutex> lk(to_connect_mux_);
    to_connect_flag_ = true;
    to_connect_cv_.notify_one();
}

void CcStreamSender::ResendMessageToNode()
{
    using namespace std::chrono_literals;

    size_t no_message_round_cnt = 0;
    std::unique_lock<std::mutex> lk(resend_mux_);
    resend_thread_status_ = ResendThreadStatus::Running;

    while (!terminate_.load(std::memory_order_acquire))
    {
        if (no_message_round_cnt == 50)
        {
            no_message_round_cnt = 0;
            resend_thread_status_ = ResendThreadStatus::Sleeping;
            resend_cv_.wait(
                lk,
                [this]
                {
                    return terminate_.load(std::memory_order_acquire) ||
                           eagain_resend_message_cnt_ != 0 ||
                           eagain_resend_long_message_cnt_ != 0;
                });

            assert(terminate_ ||
                   resend_thread_status_ == ResendThreadStatus::Running);
        }
        else
        {
            resend_cv_.wait_for(
                lk,
                20ms,
                [this] { return terminate_.load(std::memory_order_acquire); });
        }

        if (terminate_.load(std::memory_order_acquire))
        {
            break;
        }

        if (eagain_resend_message_cnt_ == 0 &&
            eagain_resend_long_message_cnt_ == 0)
        {
            assert(resend_thread_status_ == ResendThreadStatus::Running);
            no_message_round_cnt += 1;
            continue;
        }

        no_message_round_cnt = 0;
        auto all_nodes_sptr = Sharder::Instance().GetAllNodesConfigs();

        for (auto &[nid, _] : *all_nodes_sptr)
        {
            if (eagain_resend_message_cnt_ == 0)
            {
                // No more resend message
                break;
            }

            auto message_list_it = eagain_resend_message_list_.find(nid);
            while (message_list_it != eagain_resend_message_list_.end() &&
                   !message_list_it->second.is_empty())
            {
                size_t send_cnt = 0;
                ResendMessage::Uptr messages[100];
                size_t msg_cnt =
                    message_list_it->second.try_dequeue_bulk(messages, 100);

                // mutex has been locked.
                assert(eagain_resend_message_cnt_ >= msg_cnt);
                eagain_resend_message_cnt_ -= msg_cnt;

                lk.unlock();

                for (size_t idx = 0; idx < msg_cnt; ++idx)
                {
                    auto send_result = SendMessageToNode(
                        nid, messages[idx]->msg_, messages[idx]->res_, false);
                    if (send_result.sent || send_result.queued_for_retry)
                    {
                        send_cnt += 1;
                    }
                }

                lk.lock();

                if (send_cnt == 0)
                {
                    // Failed to resend message on this brpc stream. We
                    // continue to process next brpc stream
                    break;
                }

                message_list_it = eagain_resend_message_list_.find(nid);
            }
        }

        for (auto &[nid, _] : *all_nodes_sptr)
        {
            if (eagain_resend_long_message_cnt_ == 0)
            {
                // No more resend message
                break;
            }

            auto long_message_list_it =
                eagain_resend_long_message_list_.find(nid);
            while (long_message_list_it !=
                       eagain_resend_long_message_list_.end() &&
                   !long_message_list_it->second.is_empty())
            {
                size_t send_cnt = 0;
                ResendScanSliceResp::Uptr messages[100];
                size_t msg_cnt = long_message_list_it->second.try_dequeue_bulk(
                    messages, 100);

                // mutex has been locked.
                assert(eagain_resend_long_message_cnt_ >= msg_cnt);
                eagain_resend_long_message_cnt_ -= msg_cnt;

                lk.unlock();

                for (size_t idx = 0; idx < msg_cnt; ++idx)
                {
                    auto send_result =
                        SendScanRespToNode(nid, messages[idx]->msg_, false);
                    if (send_result.sent || send_result.queued_for_retry)
                    {
                        send_cnt += 1;
                    }
                }

                lk.lock();

                if (send_cnt == 0)
                {
                    // Failed to resend message on this brpc stream. We
                    // continue to process next brpc stream
                    break;
                }

                // Update the message list since the node might be
                // removed when sending the mssages.
                long_message_list_it =
                    eagain_resend_long_message_list_.find(nid);
            }
        }
    }
}

void CcStreamSender::ConnectStreams()
{
    using namespace std::chrono_literals;
    std::unique_lock<std::mutex> lk(to_connect_mux_);
    while (!terminate_.load(std::memory_order_acquire))
    {
        to_connect_cv_.wait_for(
            lk,
            1s,
            [this]
            {
                return terminate_.load(std::memory_order_acquire) ||
                       to_connect_flag_ || spam_stream_connect_;
            });

        if (terminate_.load(std::memory_order_acquire))
        {
            break;
        }

        to_connect_flag_ = false;

        if (to_connect_regular_streams_.size() == 0 &&
            to_connect_long_msg_streams_.size() == 0)
        {
            spam_stream_connect_ = false;
            continue;
        }

        std::vector<std::pair<uint32_t, int64_t>> regular_streams(
            to_connect_regular_streams_.begin(),
            to_connect_regular_streams_.end());

        for (const auto &[nid, version] : regular_streams)
        {
            lk.unlock();
            int err = ConnectStream(nid, version);
            lk.lock();

            if (err == 0)
            {
                LOG(INFO) << "Establish the cc stream to node " << nid;

                // Resend failed messages to the reconnected node.
                bool need_reconnect = false;
                auto message_list_it = resend_message_list_.find(nid);
                while (!need_reconnect &&
                       message_list_it != resend_message_list_.end() &&
                       !message_list_it->second.is_empty())
                {
                    ResendMessage::Uptr messages[100];
                    size_t msg_cnt =
                        message_list_it->second.try_dequeue_bulk(messages, 100);

                    // release lock before resend queued messages.
                    lk.unlock();
                    for (size_t i = 0; i < msg_cnt; ++i)
                    {
                        auto send_result = SendMessageToNode(
                            nid, messages[i]->msg_, messages[i]->res_, true);
                        if (send_result.need_reconnect)
                        {
                            // re-enqueue messages from i+1 to msg_cnt-1
                            // (message at i was already enqueued in
                            // SendMessageToNode)
                            assert(send_result.queued_for_retry);
                            for (size_t left_idx = i + 1; left_idx < msg_cnt;
                                 ++left_idx)
                            {
                                if (messages[left_idx])
                                {
                                    message_list_it->second.enqueue(
                                        std::move(messages[left_idx]));
                                }
                            }
                            need_reconnect = true;
                            break;
                        }
                    }
                    lk.lock();
                    // Update the message list since the node might be
                    // removed when sending the mssages.
                    message_list_it = resend_message_list_.find(nid);
                }
            }
            else
            {
                LOG_EVERY_SECOND(ERROR)
                    << "Failed to connect the cc stream to node " << nid;
            }
        }

        std::vector<std::pair<uint32_t, int64_t>> long_msg_streams(
            to_connect_long_msg_streams_.begin(),
            to_connect_long_msg_streams_.end());
        for (const auto &[nid, version] : long_msg_streams)
        {
            lk.unlock();
            int err = ConnectLongMsgStream(nid, version);
            lk.lock();

            if (err == 0)
            {
                LOG(INFO) << "Establish the long msg cc stream to node " << nid;

                bool need_reconnect = false;
                auto message_list_it = long_msg_resend_message_list_.find(nid);
                while (!need_reconnect &&
                       message_list_it != long_msg_resend_message_list_.end() &&
                       !message_list_it->second.is_empty())
                {
                    ResendScanSliceResp::Uptr messages[100];
                    size_t msg_cnt =
                        message_list_it->second.try_dequeue_bulk(messages, 100);

                    // release lock before resend queued messages.
                    lk.unlock();
                    for (size_t i = 0; i < msg_cnt; ++i)
                    {
                        auto send_result =
                            SendScanRespToNode(nid, messages[i]->msg_, true);
                        if (send_result.need_reconnect)
                        {
                            // re-enqueue messages from i+1 to msg_cnt-1
                            // (message at i was already enqueued in
                            // SendMessageToNode)
                            assert(send_result.queued_for_retry);
                            for (size_t left_idx = i + 1; left_idx < msg_cnt;
                                 ++left_idx)
                            {
                                if (messages[left_idx])
                                {
                                    message_list_it->second.enqueue(
                                        std::move(messages[left_idx]));
                                }
                            }
                            need_reconnect = true;
                            break;
                        }
                    }
                    lk.lock();
                    // Update the message list since the node might be
                    // removed when sending the mssages.
                    message_list_it = long_msg_resend_message_list_.find(nid);
                }
            }
            else
            {
                LOG_EVERY_SECOND(ERROR)
                    << "Failed to connect the long msg cc stream to node "
                    << nid;
            }
        }
    }
}

int CcStreamSender::ConnectStream(uint32_t node_id, int64_t version)
{
    std::shared_lock<std::shared_mutex> outbound_lk(outbound_mux_);
    auto channel_it = outbound_channels_.find(node_id);
    if (channel_it == outbound_channels_.end())
    {
        // Connecting to an unkown node.
        std::lock_guard<std::mutex> lk(to_connect_mux_);
        to_connect_regular_streams_.erase(node_id);
        return -1;
    }
    brpc::Channel channel;
    std::string ip_addr = channel_it->second;

    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_BAIDU_STD;
    options.timeout_ms = 100;
    options.max_retry = 3;

    size_t comma_pos = ip_addr.find(':');
    assert(comma_pos != std::string::npos);
    (void) comma_pos;
    int err;
    err = channel.Init(ip_addr.c_str(), &options);
    if (err != 0)
    {
        LOG(ERROR) << "Failed to init cc stream channel to node " << node_id
                   << ", ip: " << ip_addr << ", channel init error: " << err;
        return err;
    }

    auto stream_it = outbound_streams_.find(node_id);
    brpc::StreamId &stream_id = std::get<0>(stream_it->second);
    if (stream_id != brpc::INVALID_STREAM_ID)
    {
        brpc::StreamClose(stream_id);
    }
    std::atomic<int64_t> &stream_version = std::get<1>(stream_it->second);
    assert(stream_version.load() == -1);

    txservice::remote::CcStreamService_Stub stub(&channel);
    brpc::Controller cntl;
    err = brpc::StreamCreate(&stream_id, cntl, nullptr);
    if (err != 0)
    {
        LOG(ERROR) << "Failed to create cc stream to node " << node_id
                   << ", ip: " << ip_addr
                   << ", connect error: " << cntl.ErrorCode() << ", "
                   << cntl.ErrorText();
        return err;
    }

    txservice::remote::ConnectRequest request;
    txservice::remote::ConnectResponse response;
    request.set_message("Connect");
    request.set_type(remote::StreamType::RegularCcStream);

    // Get local ip address
    struct ifaddrs *ifaddr, *ifa;
    char ip_str[NI_MAXHOST];
    if ((err = getifaddrs(&ifaddr)) == -1)
    {
        LOG(ERROR) << "ERROR!!! Failed to getifaddrs.";
        return err;
    }
    for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next)
    {
        if (ifa->ifa_addr == nullptr)
        {
            continue;
        }

        int family = ifa->ifa_addr->sa_family;
        if (family == AF_INET)
        {
            // Check for IPv4 addresses
            err = getnameinfo(ifa->ifa_addr,
                              sizeof(struct sockaddr_in),
                              ip_str,
                              NI_MAXHOST,
                              nullptr,
                              0,
                              NI_NUMERICHOST);
            if (err != 0)
            {
                LOG(ERROR) << "ERROR!!! failed to getnameinfo: "
                           << gai_strerror(err);
                return err;
            }
            // Skip loopback addresses
            if (strcmp(ifa->ifa_name, "lo") != 0)
            {
                break;
            }
        }
    }
    freeifaddrs(ifaddr);
    std::string node_ip(ip_str);

    request.set_node_id(Sharder::Instance().NodeId());
    request.set_node_ip(node_ip);
    stub.Connect(&cntl, &request, &response, nullptr);
    if (cntl.Failed())
    {
        LOG_EVERY_SECOND(ERROR)
            << "Failed to connect to node " << node_id << ", ip: " << ip_addr
            << ", connect error: " << cntl.ErrorCode() << ", "
            << cntl.ErrorText();
        return cntl.ErrorCode();
    }
    stream_version.store(version, std::memory_order_release);

    std::lock_guard<std::mutex> lk(to_connect_mux_);
    to_connect_regular_streams_.erase(node_id);
    return 0;
}

int CcStreamSender::ConnectLongMsgStream(uint32_t node_id, int64_t version)
{
    std::shared_lock<std::shared_mutex> outbound_lk(outbound_mux_);
    auto channel_it = outbound_channels_.find(node_id);
    if (channel_it == outbound_channels_.end())
    {
        // Connecting to an unkown node.
        long_msg_resend_message_list_.erase(node_id);
        return -1;
    }

    brpc::Channel channel;
    std::string ip_addr = channel_it->second;
    auto stream_it = long_msg_outbound_streams_.find(node_id);
    brpc::StreamId &long_msg_stream_id = std::get<0>(stream_it->second);
    if (long_msg_stream_id != brpc::INVALID_STREAM_ID)
    {
        brpc::StreamClose(long_msg_stream_id);
    }
    std::atomic<int64_t> &long_msg_stream_version =
        std::get<1>(stream_it->second);
    assert(long_msg_stream_version.load() == -1);

    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_BAIDU_STD;
    options.timeout_ms = 100;
    options.max_retry = 3;
    int err = channel.Init(ip_addr.c_str(), &options);
    if (err != 0)
    {
        LOG(ERROR) << "Failed to init long msg cc stream channel to node "
                   << node_id << ", ip: " << ip_addr
                   << ", channel init error: " << err;
        return err;
    }

    txservice::remote::CcStreamService_Stub stub(&channel);
    brpc::Controller long_msg_cntl;

    err = brpc::StreamCreate(&long_msg_stream_id, long_msg_cntl, nullptr);
    if (err != 0)
    {
        LOG(ERROR) << "Failed to create long msg cc stream to node " << node_id
                   << ", ip: " << ip_addr
                   << ", connect error: " << long_msg_cntl.ErrorCode() << ", "
                   << long_msg_cntl.ErrorText();
        return err;
    }

    txservice::remote::ConnectRequest long_msg_request;
    txservice::remote::ConnectResponse long_msg_response;
    long_msg_request.set_message("Connect");
    long_msg_request.set_type(remote::StreamType::LongMsgCcStream);
    stub.Connect(
        &long_msg_cntl, &long_msg_request, &long_msg_response, nullptr);
    if (long_msg_cntl.Failed())
    {
        LOG_EVERY_SECOND(ERROR)
            << "Failed the connect rpc to node " << node_id
            << ", ip: " << ip_addr
            << ", connect error: " << long_msg_cntl.ErrorCode() << ", "
            << long_msg_cntl.ErrorText();
        return long_msg_cntl.ErrorCode();
    }
    long_msg_stream_version.store(version, std::memory_order_release);

    std::lock_guard<std::mutex> lk(to_connect_mux_);
    to_connect_long_msg_streams_.erase(node_id);
    return 0;
}
}  // namespace remote
}  // namespace txservice
