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
#include <brpc/stream.h>
#include <bthread/moodycamelqueue.h>

#include <condition_variable>
#include <deque>
#include <memory>  // std::unique_ptr
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "cc/cc_handler_result.h"
#include "proto/cc_request.pb.h"
#include "sharder.h"

namespace txservice
{
namespace remote
{
/**
 * @brief Messages need to be resend.
 * cc_stream_sender may fail to send a message, possibly due to the remote
 * node is restarted, and cc_stream_sender needs to reconnect the stream.
 */
struct ResendMessage
{
public:
    using Uptr = std::unique_ptr<ResendMessage>;
    ResendMessage();
    ResendMessage(const CcMessage &msg, CcHandlerResultBase *res)
        : msg_(msg), res_(res)
    {
    }

    CcMessage msg_;
    CcHandlerResultBase *res_;
};

struct ResendScanSliceResp
{
public:
    using Uptr = std::unique_ptr<ResendScanSliceResp>;
    ResendScanSliceResp();
    ResendScanSliceResp(const ScanSliceResponse &msg) : msg_(msg)
    {
    }

    ScanSliceResponse msg_;
};

class CcStreamSender
{
public:
    CcStreamSender(
        moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> &msg_pool);
    ~CcStreamSender();

    void RecycleCcMsg(std::unique_ptr<CcMessage> msg);
    bool SendMessageToNg(uint32_t node_group_id,
                         const CcMessage &msg,
                         CcHandlerResultBase *res = nullptr,
                         bool resend = false);
    bool SendMessageToNode(uint32_t dest_node_id,
                           const CcMessage &msg,
                           CcHandlerResultBase *res = nullptr,
                           bool resend = false,
                           bool resend_on_eagain = true,
                           bool log_verbose = false);
    bool SendScanRespToNode(uint32_t dest_node_id,
                            const ScanSliceResponse &msg,
                            bool resend = false);
    void UpdateRemoteNodes(
        const std::unordered_map<NodeId, NodeConfig> &nodes_configs);

    /**
     * @brief Used by cc_stream_receiver. Nofity to setup stream to peer when
     * receiving peer's connect request.
     */
    void NotifyConnectStream();

    /**
     * @brief Reconnect stream give node_id. Add node_id to
     * to_connect_regular_streams_.
     *
     * @param node_id
     */
    void ReConnectStream(uint32_t node_id);

    /**
     * @brief Reconnect long msg stream give node_id. Add node_id to
     * to_connect_long_msg_streams_.
     *
     * @param node_id
     */
    void ReConnectLongMsgStream(uint32_t node_id);

    /**
     * @brief Update stream ip if the ip of the new connection is different from
     * target stream ip. return true if ip changed.
     *
     */
    bool UpdateStreamIP(uint32_t node_id, const std::string &new_connection_ip);

private:
    void ConnectStreams();

    void ResendMessageToNode();

    // Need to wrap these functions calls with lk on outbound_mux_ to prevent
    // other thread trying to delete node id from cluster while connect_thd_ is
    // still trying to connect to node id.
    int ConnectStream(uint32_t node_id, int64_t version);
    int ConnectLongMsgStream(uint32_t node_id, int64_t version);

    moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> &msg_pool_;

    brpc::StreamWriteOptions stream_write_options_;

    // Protects outbound_channels_ and outbound streams
    std::shared_mutex outbound_mux_;

    std::unordered_map<uint32_t, std::string> outbound_channels_;

    // A map mapping the destination node ID to the stream connecting to it.
    // Each stream is associated with a version number, to prevent two users
    // from re-connecting the stream simultaneously. Each stream record the
    // target IP address, which will trigger reconnect if IP changed.
    std::unordered_map<
        uint32_t,
        std::tuple<brpc::StreamId, std::atomic<int64_t>, std::string>>
        outbound_streams_;
    // Dedicated streams for msgs that would take a long time to deserialize.
    // This is to avoid these request from blocking other cc requests. Each
    // stream record the target IP address, which will trigger reconnect if IP
    // changed.
    std::unordered_map<
        uint32_t,
        std::tuple<brpc::StreamId, std::atomic<int64_t>, std::string>>
        long_msg_outbound_streams_;

    // Protects to connect streams and resend message lists.
    std::mutex to_connect_mux_;
    std::condition_variable to_connect_cv_;
    std::unordered_map<uint32_t, int64_t> to_connect_regular_streams_;
    std::unordered_map<uint32_t, int64_t> to_connect_long_msg_streams_;

    // <node_id, resend_queue_to_node_id>
    std::unordered_map<uint32_t,
                       moodycamel::ConcurrentQueue<ResendMessage::Uptr>>
        resend_message_list_;
    std::unordered_map<uint32_t,
                       moodycamel::ConcurrentQueue<ResendScanSliceResp::Uptr>>
        long_msg_resend_message_list_;

    // The background thread that establishes cc streams to remote nodes.
    std::thread connect_thd_;

    std::mutex resend_mux_;
    std::condition_variable resend_cv_;

    enum class ResendThreadStatus
    {
        Sleeping = 0,
        Running
    };

    ResendThreadStatus resend_thread_status_{ResendThreadStatus::Running};

    std::unordered_map<uint32_t,
                       moodycamel::ConcurrentQueue<ResendMessage::Uptr>>
        eagain_resend_message_list_;
    std::unordered_map<uint32_t,
                       moodycamel::ConcurrentQueue<ResendScanSliceResp::Uptr>>
        eagain_resend_long_message_list_;

    size_t eagain_resend_message_cnt_{0};
    size_t eagain_resend_long_message_cnt_{0};

    std::thread resend_thd_;

    std::atomic<bool> terminate_;

    // to_connect_flag_ is used to avoid to wait one second timeout in
    // to_connect_cv_ wait_for.
    bool to_connect_flag_;
    // Spam reconnect after cluster topology changed so that cluster can be
    // ready asap . This is set to true in UpdateRemoteNodes and set back to
    // false once all pending streams have been connected.
    bool spam_stream_connect_{false};
};
}  // namespace remote
}  // namespace txservice
