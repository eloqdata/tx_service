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

#include <brpc/stream.h>
#include <bthread/moodycamelqueue.h>

#include <condition_variable>
#include <memory>  // std::unique_ptr
#include <shared_mutex>
#include <unordered_set>

#include "cc_req_pool.h"
#include "proto/cc_request.pb.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
class LocalCcShards;

namespace remote
{
class CcStreamReceiver : public brpc::StreamInputHandler, public CcStreamService
{
public:
    explicit CcStreamReceiver(
        LocalCcShards &local_shards,
        moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> &msg_pool);
    ~CcStreamReceiver() = default;

    void Shutdown();

    void Connect(::google::protobuf::RpcController *controller,
                 const ConnectRequest *request,
                 ConnectResponse *response,
                 ::google::protobuf::Closure *done) override;

    int on_received_messages(brpc::StreamId stream_id,
                             butil::IOBuf *const messages[],
                             size_t size) override;

    void on_idle_timeout(brpc::StreamId stream) override;

    void on_closed(brpc::StreamId stream) override;

    std::unique_ptr<CcMessage> GetCcMsg();

    std::unique_ptr<ScanSliceResponse> GetScanSliceResp();
    void RecycleScanSliceResp(std::unique_ptr<ScanSliceResponse> scan_resp);

    void OnReceiveCcMsg(std::unique_ptr<CcMessage> msg);
    void PreProcessScanResp(std::unique_ptr<ScanSliceResponse> msg);

private:
    std::shared_mutex inbound_mux_;
    std::condition_variable_any inbound_cv_;
    std::unordered_map<brpc::StreamId, NodeId> inbound_streams_;
    std::unordered_set<brpc::StreamId> long_msg_inbound_streams_;
    LocalCcShards &local_shards_;

    // A pool of protobuf messages for remote cc requests. The stream service
    // receives a message, de-serializes it and dispatches it to local shards
    // for processing. The message is put back into the pool after the cc
    // request is processed.
    moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> &msg_pool_;
    moodycamel::ConcurrentQueue<std::unique_ptr<ScanSliceResponse>>
        scan_resp_pool_;
};
}  // namespace remote
}  // namespace txservice
