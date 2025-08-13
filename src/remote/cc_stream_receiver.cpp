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
#include "remote/cc_stream_receiver.h"

#include <brpc/controller.h>
#include <bvar/latency_recorder.h>

#include <atomic>
#include <chrono>
#include <utility>

#include "cc/local_cc_shards.h"
#include "cc_req_pool.h"
#include "cc_request.h"
#include "cc_request.pb.h"
#include "error_messages.h"  //CcErrorCode
#include "remote/remote_type.h"
#include "sharder.h"
#include "tx_execution.h"
#include "tx_operation_result.h"
#include "tx_trace.h"

namespace txservice
{
namespace remote
{
// Cc requests received via the stream are first de-serialized as remote cc
// requests and then enqueued into the local cc shards for processing.
thread_local CcRequestPool<RemoteAcquire> acquire_pool_;
thread_local CcRequestPool<RemotePostWrite> postwrite_pool_;
thread_local CcRequestPool<RemoteAcquireAll> acquire_all_pool_;
thread_local CcRequestPool<RemotePostWriteAll> post_write_all_pool_;
thread_local CcRequestPool<RemotePostRead> postread_pool_;
thread_local CcRequestPool<RemoteRead> read_pool_;
thread_local CcRequestPool<RemoteReadOutside> read_outside_pool_;
thread_local CcRequestPool<RemoteScanOpen> scan_open_pool_;
thread_local CcRequestPool<RemoteScanSlice> scan_slice_pool;
thread_local CcRequestPool<RemoteScanNextBatch> scan_next_pool_;
thread_local CcRequestPool<RemoteReloadCacheCc> reload_cache_pool_;
thread_local CcRequestPool<RemoteInvalidateTableCacheCc>
    invalidate_table_cache_pool_;
thread_local CcRequestPool<RemoteFaultInjectCC> fault_inject_pool_;
thread_local CcRequestPool<RemoteBroadcastStatisticsCc> broadcast_stat_pool_;
thread_local CcRequestPool<RemoteAnalyzeTableAllCc> analyze_table_all_pool_;
thread_local CcRequestPool<RemoteCleanCcEntryForTestCc> clean_cc_entry_pool_;
thread_local CcRequestPool<RemoteCheckDeadLockCc> dead_lock_pool_;
thread_local CcRequestPool<RemoteAbortTransactionCc> abort_tran_pool_;
thread_local CcRequestPool<RemoteBlockReqCheckCc> blocked_req_check_pool_;
thread_local CcRequestPool<RemoteKickoutCcEntry> kickout_cc_entry_pool_;
thread_local CcRequestPool<ProcessRemoteScanRespCc>
    process_remote_scan_resp_pool_;
thread_local CcRequestPool<RemoteApplyCc> apply_pool_;
thread_local CcRequestPool<RemoteUploadTxCommandsCc> upload_cmds_pool_;
thread_local CcRequestPool<RemoteDbSizeCc> dbsize_pool_;
thread_local CcRequestPool<KeyObjectStandbyForwardCc>
    key_obj_standby_forward_pool_;
thread_local CcRequestPool<ParseCcMsgCc> parse_standby_forward_pool_;

CcStreamReceiver::CcStreamReceiver(
    LocalCcShards &local_shards,
    moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> &msg_pool)
    : local_shards_(local_shards), msg_pool_(msg_pool)
{
}

thread_local uint16_t next_core_ = 0;
void CcStreamReceiver::Shutdown()
{
    std::unique_lock<std::shared_mutex> lk(inbound_mux_);
    for (auto &[stream_id, node_id] : inbound_streams_)
    {
        brpc::StreamClose(stream_id);
    }
    for (auto &stream_id : long_msg_inbound_streams_)
    {
        brpc::StreamClose(stream_id);
    }

    inbound_cv_.wait(lk,
                     [this]()
                     {
                         return inbound_streams_.size() == 0 &&
                                long_msg_inbound_streams_.size() == 0;
                     });
}

void CcStreamReceiver::Connect(::google::protobuf::RpcController *controller,
                               const ConnectRequest *request,
                               ConnectResponse *response,
                               ::google::protobuf::Closure *done)
{
    brpc::StreamId stream_socket;

    // This object helps you to call done->Run() in RAII style. If you need
    // to process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

    brpc::StreamOptions stream_options;
    stream_options.idle_timeout_ms = 100000;

    if (request->type() == remote::StreamType::RegularCcStream)
    {
        // 5s
        stream_options.idle_timeout_ms = 5000;
    }

    stream_options.handler = this;
    if (brpc::StreamAccept(&stream_socket, *cntl, &stream_options) != 0)
    {
        cntl->SetFailed("Failed to accept stream");
        return;
    }

    // Reconnect stream if ip of source node has been changed. The
    // connection to the old source node with the same hostname may not be
    // closed due to node being killed forcely. In this case, StreamWrite will
    // return succeed before tcp keep alive timeout. Hence we need to reconnect
    // to the new source node when ip change.
    if (request->type() == remote::StreamType::RegularCcStream &&
        Sharder::Instance().GetCcStreamSender()->UpdateStreamIP(
            request->node_id(), request->node_ip()))
    {
        Sharder::Instance().GetCcStreamSender()->ReConnectStream(
            request->node_id());
        Sharder::Instance().GetCcStreamSender()->ReConnectLongMsgStream(
            request->node_id());
        Sharder::Instance().GetCcStreamSender()->NotifyConnectStream();
        LOG(INFO) << "Reconnecting stream to " << request->node_ip()
                  << " as it has failed over to a new ip.";
    }

    response->set_message("Accepted");

    std::lock_guard<std::shared_mutex> guard(inbound_mux_);
    if (request->type() == remote::StreamType::RegularCcStream)
    {
        inbound_streams_.emplace(stream_socket, request->node_id());
    }
    else
    {
        long_msg_inbound_streams_.emplace(stream_socket);
    }
}

int CcStreamReceiver::on_received_messages(brpc::StreamId stream_id,
                                           butil::IOBuf *const messages[],
                                           size_t size)
{
    bool long_msg = false;
    {
        std::shared_lock<std::shared_mutex> shared_lk(inbound_mux_);
        if (long_msg_inbound_streams_.find(stream_id) !=
            long_msg_inbound_streams_.end())
        {
            long_msg = true;
        }
    }

    if (long_msg)
    {
        // Long message will be send to tx processor to speed up the
        // deserialization.
        for (size_t i = 0; i < size; ++i)
        {
            std::unique_ptr<remote::ScanSliceResponse> resp_msg =
                GetScanSliceResp();
            butil::IOBufAsZeroCopyInputStream wrapper(*messages[i]);
            resp_msg->ParseFromZeroCopyStream(&wrapper);
            PreProcessScanResp(std::move(resp_msg));
        }
    }
    else
    {
        assert(!long_msg);
        if (Sharder::Instance().PrimaryNodeTerm() > 0)
        {
            bool success =
                Sharder::Instance().IncrInflightStandbyReqCount(size);
            while (!success)
            {
                bthread_usleep(100);
                success = Sharder::Instance().IncrInflightStandbyReqCount(size);
            }

            // For standby node, all msgs are redirected from one stream
            // by primary node. To maximize throughput, offload parsing
            // to tx processor.
            ParseCcMsgCc *cc = parse_standby_forward_pool_.NextRequest();
            cc->Reset(true, messages, size, this);
            uint16_t first_core = next_core_;
            // Do not send this task to overloaded core.
            while (local_shards_.GetCcShard(next_core_)->QueueSize() > 1000)
            {
                next_core_++;
                if (next_core_ == local_shards_.Count())
                {
                    next_core_ = 0;
                }
                if (first_core == next_core_)
                {
                    break;
                }
            }
            local_shards_.EnqueueCcRequest(next_core_++, cc);
            if (next_core_ == local_shards_.Count())
            {
                next_core_ = 0;
            }
        }
        else
        {
            // For primary node, on received msg is usually not the bottleneck.
            // Parse the msg immediately to minimize latency.
            for (size_t i = 0; i < size; ++i)
            {
                std::unique_ptr<CcMessage> cc_msg = GetCcMsg();
                butil::IOBufAsZeroCopyInputStream wrapper(*messages[i]);
                cc_msg->ParseFromZeroCopyStream(&wrapper);

                if (cc_msg->type() ==
                    remote::CcMessage::MessageType::
                        CcMessage_MessageType_KeyObjectStandbyForwardRequest)
                {
                    msg_pool_.enqueue(std::move(cc_msg));
                    continue;
                }
                OnReceiveCcMsg(std::move(cc_msg));
            }
        }
    }

    return 0;
}

void CcStreamReceiver::on_idle_timeout(brpc::StreamId stream)
{
    std::shared_lock<std::shared_mutex> shared_lk(inbound_mux_);
    auto inbound_stream = inbound_streams_.find(stream);
    if (inbound_stream != inbound_streams_.end())
    {
        uint32_t stream_node_id = inbound_stream->second;
        shared_lk.unlock();

        int64_t cur_prim_term = Sharder::Instance().PrimaryNodeTerm();
        if (cur_prim_term > 0)
        {
            NodeGroupId native_ng = Sharder::Instance().NativeNodeGroup();
            uint32_t leader_node_id =
                Sharder::Instance().LeaderNodeId(native_ng);
            if (stream_node_id == leader_node_id)
            {
                Sharder::Instance().OnStartFollowing(
                    native_ng, cur_prim_term, leader_node_id, true);
            }
        }
    }
}

void CcStreamReceiver::on_closed(brpc::StreamId stream)
{
    std::unique_lock<std::shared_mutex> lk(inbound_mux_);
    inbound_streams_.erase(stream);
    long_msg_inbound_streams_.erase(stream);
    if (inbound_streams_.size() == 0 && long_msg_inbound_streams_.size() == 0)
    {
        inbound_cv_.notify_one();
    }
}

std::unique_ptr<CcMessage> CcStreamReceiver::GetCcMsg()
{
    std::unique_ptr<CcMessage> msg;
    if (msg_pool_.try_dequeue(msg))
    {
        return msg;
    }
    else
    {
        return std::make_unique<CcMessage>();
    }
}

std::unique_ptr<ScanSliceResponse> CcStreamReceiver::GetScanSliceResp()
{
    std::unique_ptr<ScanSliceResponse> msg;
    if (scan_resp_pool_.try_dequeue(msg))
    {
        return msg;
    }
    else
    {
        return std::make_unique<ScanSliceResponse>();
    }
}

void CcStreamReceiver::RecycleScanSliceResp(
    std::unique_ptr<ScanSliceResponse> scan_resp)
{
    scan_resp_pool_.enqueue(std::move(scan_resp));
}

void CcStreamReceiver::PreProcessScanResp(
    std::unique_ptr<ScanSliceResponse> msg)
{
    CODE_FAULT_INJECTOR("before_mark_remote_received", {
        std::this_thread::sleep_for(std::chrono::seconds(15));
        std::this_thread::yield();
    });
    CcHandlerResult<RangeScanSliceResult> *hd_res = nullptr;
    uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
    int64_t tx_term = msg->tx_term();
    TransactionExecution *txm = nullptr;

    if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
    {
        // The tx node has failed. Pointer stability does not hold anymore.
        scan_resp_pool_.enqueue(std::move(msg));
        return;
    }
    else
    {
        hd_res = reinterpret_cast<CcHandlerResult<RangeScanSliceResult> *>(
            msg->handler_addr());

        txm = reinterpret_cast<TransactionExecution *>(msg->txm_addr());

        txm->AcquireSharedForwardLatch();

        if (txm->TxNumber() != msg->tx_number() ||
            txm->CommandId() != msg->command_id() || hd_res->IsFinished())
        {
            // The original tx has terminated and the tx machine has been
            // recycled. The response message is directed to an obsolete tx.
            // Skips setting the cc handler result.
            scan_resp_pool_.enqueue(std::move(msg));
            txm->ReleaseSharedForwardLatch();
            return;
        }
    }

    assert(hd_res->Txm()->TxNumber() == msg->tx_number());
    assert(hd_res->Txm()->CommandId() == msg->command_id());

    RangeScanSliceResult &scan_slice_result = hd_res->Value();
    CcScanner &range_scanner = *scan_slice_result.ccm_scanner_;
    if (!msg->last_key().empty())
    {
        scan_slice_result.SetLastKey(range_scanner.DecodeKey(msg->last_key()));
    }
    else
    {
        scan_slice_result.SetLastKey(TxKey());
    }

    scan_slice_result.slice_position_ =
        ToLocalType::ConvertSlicePosition(msg->slice_position());

    const char *tuple_cnt_info = msg->tuple_cnt().data();
    uint16_t remote_core_cnt = *((const uint16_t *) tuple_cnt_info);
    tuple_cnt_info += sizeof(uint16_t);
    range_scanner.ResetShards(remote_core_cnt);

    const uint64_t *term_ptr = (const uint64_t *) msg->term().data();

    // The offset_table stores the start postition of meta data like `key_ts`
    // for all remote cores
    std::vector<size_t> offset_table;
    size_t meta_offset = 0;

    range_scanner.SetPartitionNgTerm(-1);

    bool all_remote_core_no_more_data = true;

    for (uint16_t core_id = 0; core_id < remote_core_cnt; ++core_id)
    {
        size_t tuple_cnt = *((const size_t *) tuple_cnt_info);
        tuple_cnt_info += sizeof(size_t);

        all_remote_core_no_more_data =
            all_remote_core_no_more_data && (tuple_cnt == 0);

        // All term value are same. We only set `partition_ng_term` once.
        if (range_scanner.PartitionNgTerm() == -1 && tuple_cnt != 0)
        {
            range_scanner.SetPartitionNgTerm(term_ptr[0]);
        }

        offset_table.push_back(meta_offset);
        meta_offset += tuple_cnt;
        term_ptr += tuple_cnt;
    }

    assert(offset_table.size() == remote_core_cnt);

    // No more data.
    if (all_remote_core_no_more_data)
    {
        if (msg->error_code() != 0)
        {
            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(msg->error_code()));
        }
        else
        {
            hd_res->SetFinished();
        }

        txm->ReleaseSharedForwardLatch();
        RecycleScanSliceResp(std::move(msg));
        return;
    }

    // Worker count means how many tx processer to parallel deserialize msg.
    // remote core count is not always equal to local core count
    size_t worker_cnt = std::min((size_t) remote_core_cnt,
                                 Sharder::Instance().GetLocalCcShardsCount());

    ProcessRemoteScanRespCc *request =
        process_remote_scan_resp_pool_.NextRequest();
    request->Reset(
        this, std::move(msg), std::move(offset_table), hd_res, worker_cnt);

    for (size_t idx = 0; idx < worker_cnt; ++idx)
    {
        local_shards_.EnqueueCcRequest(idx, request);
    }
}

void CcStreamReceiver::OnReceiveCcMsg(std::unique_ptr<CcMessage> msg)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        msg.get(),
        [&msg]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(msg->tx_number()))
                .append(",\"tx_term\":")
                .append(std::to_string(msg->tx_term()));
        });
    TX_TRACE_DUMP(msg.get());

    switch (msg->type())
    {
    case CcMessage::MessageType::CcMessage_MessageType_AcquireRequest:
    {
        RemoteAcquire *acquire_req = acquire_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), acquire_req);
        acquire_req->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(acquire_req->KeyShardCode(),
                                       acquire_req);

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_AcquireResponse:
    {
        // message AcquireResponse
        //{
        //    bool error = 1;
        //    uint64 vali_ts = 2;
        //    CceAddr_msg cce_addr = 3;
        //}

        assert(msg->has_acquire_resp());

        CcHandlerResult<std::vector<AcquireKeyResult>> *hd_res = nullptr;
        TransactionExecution *txm = nullptr;

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            txm = reinterpret_cast<TransactionExecution *>(msg->txm_addr());

            txm->AcquireSharedForwardLatch();

            hd_res = reinterpret_cast<
                CcHandlerResult<std::vector<AcquireKeyResult>> *>(
                msg->handler_addr());

            assert(txm == hd_res->Txm());
            if (txm->TxNumber() != msg->tx_number() ||
                txm->CommandId() != msg->command_id() || hd_res->IsFinished())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }
        }

        const AcquireResponse &cc_res = msg->acquire_resp();
        const CceAddr_msg &cce_addr_res = cc_res.cce_addr();
        AcquireKeyResult &acq_res = hd_res->Value()[cc_res.vec_idx()];

        if (acq_res.IsRemoteHdResultSet(std::memory_order_acquire))
        {
            msg_pool_.enqueue(std::move(msg));
            txm->ReleaseSharedForwardLatch();
            break;
        }

        if (cc_res.error_code() != 0)
        {
            if (acq_res.cce_addr_.Term() < 0)
            {
                acq_res.remote_ack_cnt_->fetch_sub(1);
            }

            acq_res.SetRemoteHdResult(true, std::memory_order_release);
            hd_res->SetRemoteError(
                ToLocalType::ConvertCcErrorCode(cc_res.error_code()));
            txm->ReleaseSharedForwardLatch();
        }
        else
        {
            if (acq_res.cce_addr_.Term() < 0)
            {
                acq_res.cce_addr_.SetCceLock(cce_addr_res.cce_lock_ptr(),
                                             cce_addr_res.term(),
                                             cce_addr_res.core_id());

                // Even though the role of remote_ack_cnt_ is to bookkeep how
                // many remote acknowledgements have been received, the cc entry
                // address is updated and will be read by the tx processor in a
                // separate thread. To ensure the updated address is visible to
                // the tx processor, the memory order must be
                // memory_order_release.
                acq_res.remote_ack_cnt_->fetch_sub(1,
                                                   std::memory_order_release);
            }

            if (!cc_res.is_ack())
            {
                // For locking-based protocols, when the acquire request is
                // blocked in a remote node, the remote node will send an
                // acknowledgement message to notify the sending tx the cce
                // address and the node's term. When the acquire request is
                // unblocked, the response will send back the last validation ts
                // of the key.
                acq_res.last_vali_ts_ = cc_res.vali_ts();
                acq_res.commit_ts_ = cc_res.commit_ts();
                acq_res.SetRemoteHdResult(true, std::memory_order_release);

                hd_res->SetRemoteFinished();
            }
            else
            {
                acq_res.last_vali_ts_ = 0;
                acq_res.commit_ts_ = 0;
            }

            txm->ReleaseSharedForwardLatch();
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_AcquireAllRequest:
    {
        RemoteAcquireAll *acquire_all_req = acquire_all_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), acquire_all_req);
        acquire_all_req->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(0, acquire_all_req);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_AcquireAllResponse:
    {
        assert(msg->has_acquire_all_resp());

        CcHandlerResult<AcquireAllResult> *hd_res = nullptr;
        TransactionExecution *txm = nullptr;

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            txm = reinterpret_cast<TransactionExecution *>(msg->txm_addr());

            txm->AcquireSharedForwardLatch();

            // Check txm before accessing hd_res which might be invalid if the
            // txm has already been reused.
            if (txm->TxNumber() != msg->tx_number() ||
                txm->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }

            hd_res = reinterpret_cast<CcHandlerResult<AcquireAllResult> *>(
                msg->handler_addr());

            // Double-check the txm status since it could have timed out and
            // been reused after the first check.
            assert(txm == hd_res->Txm());
            if (txm->TxNumber() != msg->tx_number() ||
                txm->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }
        }

        const AcquireAllResponse &cc_res = msg->acquire_all_resp();
        AcquireAllResult &acq_all_res = hd_res->Value();

        if (cc_res.error_code() != 0)
        {
            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(cc_res.error_code()));
            txm->ReleaseSharedForwardLatch();
        }
        else
        {
            if (acq_all_res.node_term_ < 0)
            {
                acq_all_res.node_term_ = cc_res.node_term();
                // Uses memory_order_release to ensure the updated node term is
                // visible to the tx processor, which is in a separate thread.
                acq_all_res.remote_ack_cnt_->fetch_sub(
                    1, std::memory_order_release);
            }
            if (!cc_res.is_ack())
            {
                // For locking-based protocols, when the acquire request is
                // blocked in a remote node, the remote node will send an
                // acknowledgement message to notify the sending tx the cce
                // address and the node's term. When the acquire request is
                // unblocked, the response will send back the last validation ts
                // of the key.
                acq_all_res.last_vali_ts_ = cc_res.vali_ts();
                acq_all_res.commit_ts_ = cc_res.commit_ts();
            }
            else
            {
                acq_all_res.blocked_remote_cce_addr_.clear();
                for (const auto &ack_cce_addr : cc_res.ack_cce_addr())
                {
                    auto &cce_addr =
                        acq_all_res.blocked_remote_cce_addr_.emplace_back();
                    cce_addr.SetNodeGroupId(ack_cce_addr.node_group_id());
                    cce_addr.SetCceLock(ack_cce_addr.cce_lock_ptr(),
                                        ack_cce_addr.term(),
                                        ack_cce_addr.core_id());
                }
            }

            if (!cc_res.is_ack())
            {
                hd_res->SetFinished();
            }

            txm->ReleaseSharedForwardLatch();
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ValidateRequest:
    {
        assert(msg->has_validate_req());

        const ValidateRequest &req = msg->validate_req();
        const CceAddr_msg &cce_addr = req.cce_addr();

        int64_t node_term = Sharder::Instance().LeaderTerm(req.node_group_id());
        CcErrorCode cc_err_code = CcErrorCode::NO_ERROR;
        if (node_term < 0)
        {
            cc_err_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
        }
        else if (node_term != cce_addr.term())
        {
            cc_err_code = CcErrorCode::VALIDATION_FAILED_FOR_VERSION_MISMATCH;
        }

        if (cc_err_code != CcErrorCode::NO_ERROR)
        {
            CcMessage return_msg;
            return_msg.set_type(
                CcMessage::MessageType::CcMessage_MessageType_ValidateResponse);

            return_msg.set_tx_number(msg->tx_number());
            return_msg.set_handler_addr(msg->handler_addr());
            return_msg.set_txm_addr(msg->txm_addr());
            return_msg.set_tx_term(msg->tx_term());
            return_msg.set_command_id(msg->command_id());

            ValidateResponse *resp = return_msg.mutable_validate_resp();
            resp->set_error_code(ToRemoteType::ConvertCcErrorCode(cc_err_code));

            CcStreamSender *cc_stream_sender =
                Sharder::Instance().GetCcStreamSender();

            cc_stream_sender->SendMessageToNode(req.src_node_id(), return_msg);
            msg_pool_.enqueue(std::move(msg));
        }
        else
        {
            RemotePostRead *vali_req = postread_pool_.NextRequest();
            TX_TRACE_ASSOCIATE(msg.get(), vali_req);
            vali_req->Reset(std::move(msg));
            local_shards_.EnqueueCcRequest(cce_addr.core_id(), vali_req);
        }

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ValidateResponse:
    {
        assert(msg->has_validate_resp());

        CcHandlerResult<PostProcessResult> *hd_res = nullptr;
        TransactionExecution *txm = nullptr;

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            txm = reinterpret_cast<TransactionExecution *>(msg->txm_addr());

            txm->AcquireSharedForwardLatch();

            hd_res = reinterpret_cast<CcHandlerResult<PostProcessResult> *>(
                msg->handler_addr());

            assert(txm == hd_res->Txm());
            if (txm->TxNumber() != msg->tx_number() ||
                txm->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }
        }

        const ValidateResponse &cc_res = msg->validate_resp();

        PostProcessResult &conflicting_txs = hd_res->Value();
        conflicting_txs.IncrConflictingTx(cc_res.conflicting_tx_cnt());

        if (cc_res.error_code() != 0)
        {
            hd_res->SetRemoteError(
                ToLocalType::ConvertCcErrorCode(cc_res.error_code()));
        }
        else
        {
            hd_res->SetRemoteFinished();
        }

        txm->ReleaseSharedForwardLatch();

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_PostprocessResponse:
    {
        assert(msg->has_post_resp());

        CcHandlerResult<PostProcessResult> *hd_res = nullptr;
        TransactionExecution *txm = nullptr;

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            txm = reinterpret_cast<TransactionExecution *>(msg->txm_addr());

            txm->AcquireSharedForwardLatch();

            hd_res = reinterpret_cast<CcHandlerResult<PostProcessResult> *>(
                msg->handler_addr());

            assert(txm == hd_res->Txm());
            if (txm->TxNumber() != msg->tx_number() ||
                txm->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }
        }

        const PostprocessResponse &cc_res = msg->post_resp();

        if (cc_res.error_code() != 0)
        {
            hd_res->SetRemoteError(
                ToLocalType::ConvertCcErrorCode(cc_res.error_code()));
        }
        else
        {
            hd_res->SetRemoteFinished();
        }

        txm->ReleaseSharedForwardLatch();

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ReadRequest:
    {
        RemoteRead *read = read_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), read);
        read->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(read->KeyShardCode(), read);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ReadOutsideRequest:
    {
        RemoteReadOutside *read_outside = read_outside_pool_.NextRequest();

        TX_TRACE_ASSOCIATE(msg.get(), read_outside);
        read_outside->Reset(std::move(msg));

        const CcEntryAddr &cce_addr = read_outside->CceAddr();
        local_shards_.EnqueueCcRequest(cce_addr.CoreId(), read_outside);

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ReadResponse:
    {
        assert(msg->has_read_resp());

        CcHandlerResult<ReadKeyResult> *hd_res = nullptr;
        TransactionExecution *txm = nullptr;

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            txm = reinterpret_cast<TransactionExecution *>(msg->txm_addr());

            txm->AcquireSharedForwardLatch();

            hd_res = reinterpret_cast<CcHandlerResult<ReadKeyResult> *>(
                msg->handler_addr());

            // Check txm before accessing hd_res which might be invalid if the
            // txm has already been reused.
            if (txm->TxNumber() != msg->tx_number() ||
                txm->CommandId() != msg->command_id() || hd_res->IsFinished())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }
        }

        const ReadResponse &read_res = msg->read_resp();

        if (read_res.error_code() != 0)
        {
            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(read_res.error_code()));
            txm->ReleaseSharedForwardLatch();
        }
        else
        {
            ReadKeyResult &read_result = hd_res->Value();

            if (read_result.cce_addr_.Term() < 0)
            {
                const CceAddr_msg &cce_addr_msg = read_res.cce_addr();
                read_result.cce_addr_.SetCceLock(cce_addr_msg.cce_lock_ptr(),
                                                 cce_addr_msg.term(),
                                                 cce_addr_msg.core_id());
                // CC entry's shard Id has been set when the read request was
                // sent.
            }

            if (!read_res.is_ack())
            {
                read_result.rec_status_ =
                    ToLocalType::ConvertRecordStatusType(read_res.rec_status());
                if (read_res.rec_status() == RecordStatusType::NORMAL)
                {
                    size_t offset = 0;
                    read_result.rec_->Deserialize(read_res.record().data(),
                                                  offset);
                }
                read_result.ts_ = read_res.ts();
                read_result.lock_type_ =
                    ToLocalType::ConvertLockType(read_res.lock_type());
            }

            if (!read_res.is_ack())
            {
                hd_res->SetFinished();
            }

            txm->ReleaseSharedForwardLatch();
        }
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_PostCommitRequest:
    {
        assert(msg->has_postcommit_req());

        const PostCommitRequest &post_commit = msg->postcommit_req();
        const CceAddr_msg &cce_addr_msg = post_commit.cce_addr();

        int64_t node_term =
            Sharder::Instance().LeaderTerm(post_commit.node_group_id());
        CcErrorCode cc_err_code = CcErrorCode::NO_ERROR;
        if (node_term < 0)
        {
            cc_err_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
        }
        else if (node_term != cce_addr_msg.term())
        {
            cc_err_code = CcErrorCode::VALIDATION_FAILED_FOR_VERSION_MISMATCH;
        }

        if (cc_err_code != CcErrorCode::NO_ERROR)
        {
            CcMessage return_msg;
            return_msg.set_type(CcMessage::MessageType::
                                    CcMessage_MessageType_PostprocessResponse);
            return_msg.set_tx_number(msg->tx_number());
            return_msg.set_handler_addr(msg->handler_addr());
            return_msg.set_txm_addr(msg->txm_addr());
            return_msg.set_tx_term(msg->tx_term());
            return_msg.set_command_id(msg->command_id());

            PostprocessResponse *resp = return_msg.mutable_post_resp();
            resp->set_error_code(ToRemoteType::ConvertCcErrorCode(cc_err_code));

            CcStreamSender *cc_stream_sender =
                Sharder::Instance().GetCcStreamSender();
            cc_stream_sender->SendMessageToNode(post_commit.src_node_id(),
                                                return_msg);
            msg_pool_.enqueue(std::move(msg));
        }
        else
        {
            RemotePostWrite *post_commit = postwrite_pool_.NextRequest();
            TX_TRACE_ASSOCIATE(msg.get(), post_commit);
            post_commit->Reset(std::move(msg));
            local_shards_.EnqueueCcRequest(cce_addr_msg.core_id(), post_commit);
        }
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ForwardPostCommitRequest:
    {
        assert(msg->has_forward_post_commit_req());

        const ForwardPostCommitRequest &post_commit =
            msg->forward_post_commit_req();
        if (Sharder::Instance().LeaderTerm(post_commit.node_group_id()) < 0)
        {
            CcMessage return_msg;
            return_msg.set_type(CcMessage::MessageType::
                                    CcMessage_MessageType_PostprocessResponse);
            return_msg.set_tx_number(msg->tx_number());
            return_msg.set_handler_addr(msg->handler_addr());
            return_msg.set_txm_addr(msg->txm_addr());
            return_msg.set_tx_term(msg->tx_term());
            return_msg.set_command_id(msg->command_id());

            PostprocessResponse *resp = return_msg.mutable_post_resp();
            resp->set_error_code(ToRemoteType::ConvertCcErrorCode(
                CcErrorCode::REQUESTED_NODE_NOT_LEADER));

            CcStreamSender *cc_stream_sender =
                Sharder::Instance().GetCcStreamSender();
            cc_stream_sender->SendMessageToNode(post_commit.src_node_id(),
                                                return_msg);
            msg_pool_.enqueue(std::move(msg));
        }
        else
        {
            RemotePostWrite *post_commit_cc = postwrite_pool_.NextRequest();
            TX_TRACE_ASSOCIATE(msg.get(), post_commit_cc);
            post_commit_cc->Reset(std::move(msg));
            local_shards_.EnqueueCcRequest(post_commit_cc->KeyShardCode(),
                                           post_commit_cc);
        }
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_PostWriteAllRequest:
    {
        RemotePostWriteAll *post_write_all = post_write_all_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), post_write_all);
        post_write_all->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(0, post_write_all);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ScanOpenRequest:
    {
        RemoteScanOpen *scan_open_req = scan_open_pool_.NextRequest();
        uint32_t local_core_cnt = (uint32_t) local_shards_.Count();
        TX_TRACE_ASSOCIATE(msg.get(), scan_open_req);
        scan_open_req->Reset(std::move(msg), local_core_cnt);

        for (uint32_t core_id = 0; core_id < local_core_cnt; ++core_id)
        {
            // The scan open request is directed to all local shards. The
            // request pre-allocates scan caches, one for each shard. Each shard
            // fills its own designated cache, so there is no synchronization
            // across cores.
            local_shards_.EnqueueCcRequest(core_id, scan_open_req);
        }

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ScanOpenResponse:
    {
        /*message ScanOpenResponse
        {
            bool error = 1;
            uint32 shard_id = 2;
            repeated ScanCache_msg scan_cache = 3;
        }*/

        assert(msg->has_scan_open_resp());

        CcHandlerResult<ScanOpenResult> *hd_res = nullptr;
        TransactionExecution *txm = nullptr;
        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            txm = reinterpret_cast<TransactionExecution *>(msg->txm_addr());

            txm->AcquireSharedForwardLatch();

            hd_res = reinterpret_cast<CcHandlerResult<ScanOpenResult> *>(
                msg->handler_addr());

            assert(txm == hd_res->Txm());
            if (txm->TxNumber() != msg->tx_number() ||
                txm->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }
        }

        const ScanOpenResponse &scan_open_res = msg->scan_open_resp();
        uint32_t ng_id = scan_open_res.node_group_id();
        hd_res->Value().cc_node_returned_[ng_id] = 1;

        // Even if ScanOpen operation fail, we should also move scan
        // result into read set to release acquired lock.
        {
            CcScanner &scanner = *hd_res->Value().scanner_;
            int64_t term = -1;

            for (int core_id = 0; core_id < scan_open_res.scan_cache_size();
                 ++core_id)
            {
                uint32_t shard_code = (ng_id << 10) + core_id;
                const ScanCache_msg &cache_msg =
                    scan_open_res.scan_cache(core_id);
                ScanCache *shard_cache = scanner.AddShard(shard_code);

                for (int idx = 0; idx < cache_msg.scan_tuple_size(); ++idx)
                {
                    const ScanTuple_msg &tuple_msg = cache_msg.scan_tuple(idx);
                    assert(tuple_msg.cce_addr().core_id() ==
                           (uint32_t) core_id);

                    term = tuple_msg.cce_addr().term();

                    RecordStatus rec_status =
                        ToLocalType::ConvertRecordStatusType(
                            tuple_msg.rec_status());

                    size_t key_offset = 0;
                    size_t rec_offset = 0;
                    shard_cache->AddScanTuple(
                        tuple_msg.key(),
                        key_offset,
                        tuple_msg.key_ts(),
                        tuple_msg.record(),
                        rec_offset,
                        rec_status,
                        tuple_msg.gap_ts(),
                        tuple_msg.cce_addr().cce_lock_ptr(),
                        tuple_msg.cce_addr().term(),
                        tuple_msg.cce_addr().core_id(),
                        ng_id);
                }
            }

            hd_res->Value().cc_node_terms_[ng_id] = term;
        }

        if (scan_open_res.error_code() != 0)
        {
            hd_res->SetRemoteError(
                ToLocalType::ConvertCcErrorCode(scan_open_res.error_code()));
        }
        else
        {
            hd_res->SetRemoteFinished();
        }

        txm->ReleaseSharedForwardLatch();

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ScanNextRequest:
    {
        RemoteScanNextBatch *scan_next_req = scan_next_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), scan_next_req);
        scan_next_req->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(scan_next_req->PriorCceAddr().CoreId(),
                                       scan_next_req);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ScanNextResponse:
    {
        /*message ScanNextResponse
        {
            bool error = 1;
            repeated ScanTuple_msg scan_tuple = 2;
            uint64 ccm_ptr = 3;
            uint64 scan_cache_ptr = 4;
        }*/

        assert(msg->has_scan_next_resp());

        CcHandlerResult<ScanNextResult> *hd_res = nullptr;
        TransactionExecution *txm = nullptr;
        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            txm = reinterpret_cast<TransactionExecution *>(msg->txm_addr());

            txm->AcquireSharedForwardLatch();

            hd_res = reinterpret_cast<CcHandlerResult<ScanNextResult> *>(
                msg->handler_addr());

            assert(txm == hd_res->Txm());
            if (txm->TxNumber() != msg->tx_number() ||
                txm->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }
        }

        const ScanNextResponse &scan_next_res = msg->scan_next_resp();

        // Even if ScanNext operation fail, we should also move scan
        // result into read set to release acquired lock.
        {
            ScanCache *shard_cache =
                reinterpret_cast<ScanCache *>(scan_next_res.scan_cache_ptr());

            uint32_t ng_id = shard_cache->LastTuple()->cce_addr_.NodeGroupId();
            shard_cache->Reset();
            const ScanCache_msg &scan_cache = scan_next_res.scan_cache();

            for (int idx = 0; idx < scan_cache.scan_tuple_size(); ++idx)
            {
                const ScanTuple_msg &tuple_msg = scan_cache.scan_tuple(idx);
                hd_res->Value().term_ = tuple_msg.cce_addr().term();

                RecordStatus rec_status = ToLocalType::ConvertRecordStatusType(
                    tuple_msg.rec_status());

                size_t key_offset = 0;
                size_t rec_offset = 0;
                shard_cache->AddScanTuple(tuple_msg.key(),
                                          key_offset,
                                          tuple_msg.key_ts(),
                                          tuple_msg.record(),
                                          rec_offset,
                                          rec_status,
                                          tuple_msg.gap_ts(),
                                          tuple_msg.cce_addr().cce_lock_ptr(),
                                          tuple_msg.cce_addr().term(),
                                          tuple_msg.cce_addr().core_id(),
                                          ng_id);
            }
        }

        if (scan_next_res.error_code() != 0)
        {
            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(scan_next_res.error_code()));
        }
        else
        {
            hd_res->SetFinished();
        }

        txm->ReleaseSharedForwardLatch();

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ScanSliceRequest:
    {
        RemoteScanSlice *scan_slice_req = scan_slice_pool.NextRequest();
        uint32_t local_core_cnt = (uint32_t) local_shards_.Count();
        TX_TRACE_ASSOCIATE(msg.get(), scan_slice_req);
        scan_slice_req->Reset(std::move(msg), local_core_cnt);
        // The scan slice request is enqueued into the first core, where it pins
        // the slice and sets the scan's end key. The request is then dispatched
        // to remaining cores to scan the slice in parallel.
        local_shards_.EnqueueCcRequest(0, scan_slice_req);

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ReloadCacheRequest:
    {
        RemoteReloadCacheCc *reload_req = reload_cache_pool_.NextRequest();
        reload_req->Reset(std::move(msg));
        TX_TRACE_ASSOCIATE(msg.get(), reload_req);
        local_shards_.EnqueueCcRequest(0, reload_req);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ReloadCacheResponse:
    {
        assert(msg->has_reload_cache_resp());

        CcHandlerResult<Void> *hd_res = nullptr;
        TransactionExecution *txm = nullptr;
        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            txm = reinterpret_cast<TransactionExecution *>(msg->txm_addr());

            txm->AcquireSharedForwardLatch();

            hd_res =
                reinterpret_cast<CcHandlerResult<Void> *>(msg->handler_addr());

            assert(txm == hd_res->Txm());
            if (txm->TxNumber() != msg->tx_number() ||
                txm->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }
        }

        const ReloadCacheResponse &reload_resp = msg->reload_cache_resp();
        if (reload_resp.error_code() != 0)
        {
            hd_res->SetRemoteError(
                ToLocalType::ConvertCcErrorCode(reload_resp.error_code()));
        }
        else
        {
            hd_res->SetRemoteFinished();
        }

        txm->ReleaseSharedForwardLatch();
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_FaultInjectRequest:
    {
        RemoteFaultInjectCC *fault_inject_req =
            fault_inject_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), fault_inject_req);
        fault_inject_req->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(0, fault_inject_req);

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_FaultInjectResponse:
    {
        assert(msg->has_fault_inject_resp());

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;

        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        CcHandlerResult<bool> *hd_res =
            reinterpret_cast<CcHandlerResult<bool> *>(msg->handler_addr());

        TransactionExecution *txm =
            reinterpret_cast<TransactionExecution *>(msg->txm_addr());

        txm->AcquireSharedForwardLatch();

        assert(txm == hd_res->Txm());
        if (txm->TxNumber() != msg->tx_number() ||
            txm->CommandId() != msg->command_id())
        {
            // The original tx has terminated and the tx machine has been
            // recycled. The response message is directed to an obsolete tx.
            // Skips setting the cc handler result.
            msg_pool_.enqueue(std::move(msg));
            txm->ReleaseSharedForwardLatch();
            break;
        }

        const FaultInjectResponse &fi_res = msg->fault_inject_resp();

        if (fi_res.error_code() != 0)
        {
            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(fi_res.error_code()));
        }
        else
        {
            hd_res->SetFinished();
        }
        txm->ReleaseSharedForwardLatch();

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_AnalyzeTableAllRequest:
    {
        RemoteAnalyzeTableAllCc *analyze_req =
            analyze_table_all_pool_.NextRequest();
        analyze_req->Reset(std::move(msg));
        TX_TRACE_ASSOCIATE(msg.get(), analyze_req);
        local_shards_.EnqueueCcRequest(0, analyze_req);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_AnalyzeTableAllResponse:
    {
        assert(msg->has_analyze_table_all_resp());

        CcHandlerResult<Void> *hd_res = nullptr;
        TransactionExecution *txm = nullptr;

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            txm = reinterpret_cast<TransactionExecution *>(msg->txm_addr());

            txm->AcquireSharedForwardLatch();

            hd_res =
                reinterpret_cast<CcHandlerResult<Void> *>(msg->handler_addr());

            assert(txm == hd_res->Txm());
            if (txm->TxNumber() != msg->tx_number() ||
                txm->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }
        }

        const AnalyzeTableAllResponse &analyze_resp =
            msg->analyze_table_all_resp();
        if (analyze_resp.error_code() != 0)
        {
            hd_res->SetRemoteError(
                ToLocalType::ConvertCcErrorCode(analyze_resp.error_code()));
        }
        else
        {
            hd_res->SetRemoteFinished();
        }

        txm->ReleaseSharedForwardLatch();
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_CleanCcEntryForTestRequest:
    {
        RemoteCleanCcEntryForTestCc *clean_req =
            clean_cc_entry_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), clean_req);
        clean_req->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(0, clean_req);

        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_CleanCcEntryForTestResponse:
    {
        assert(msg->has_clean_cc_entry_resp());

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;

        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        CcHandlerResult<bool> *hd_res =
            reinterpret_cast<CcHandlerResult<bool> *>(msg->handler_addr());

        const CleanCcEntryForTestResponse &clean_res =
            msg->clean_cc_entry_resp();

        if (clean_res.error_code() != 0)
        {
            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(clean_res.error_code()));
        }
        else
        {
            hd_res->SetFinished();
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_RecoverStateCheckRequest:
    {
        // this request is only used during cluster initialization.
        const RecoverStateCheckRequest &req = msg->recover_state_check_req();

        CcMessage send_msg;

        send_msg.set_type(CcMessage::MessageType::
                              CcMessage_MessageType_RecoverStateCheckResponse);

        RecoverStateCheckResponse *recover_resp =
            send_msg.mutable_recover_state_check_resp();

        // error_code is set to -1 if the log replay is not finished.
        if (Sharder::Instance().LeaderTerm(req.node_group_id()) > 0)
        {
            recover_resp->set_error_code(0);
        }
        else
        {
            DLOG(INFO) << "RecoverStateCheckRequest, node "
                       << Sharder::Instance().NodeId()
                       << " is not leader of node group "
                       << req.node_group_id();
            recover_resp->set_error_code(-1);
        }

        recover_resp->set_node_group_id(req.node_group_id());

        Sharder::Instance().GetCcStreamSender()->SendMessageToNode(
            req.src_node_id(), send_msg);
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_RecoverStateCheckResponse:
    {
        // this response is only used during cluster initialization.
        const RecoverStateCheckResponse &resp = msg->recover_state_check_resp();

        if (resp.error_code() == 0)
        {
            Sharder::Instance().NodeGroupFinishRecovery(resp.node_group_id());
        }
        else
        {
            DLOG(INFO) << "RecoverStateCheckResponse with error, node_group: "
                       << resp.node_group_id()
                       << ", error_code:" << resp.error_code();
            Sharder::Instance().UpdateLeader(resp.node_group_id());
        }
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_DeadLockRequest:
    {
        RemoteCheckDeadLockCc *dead_lock_req = dead_lock_pool_.NextRequest();
        dead_lock_req->Reset(std::move(msg));

        for (size_t i = 0; i < local_shards_.Count(); i++)
        {
            local_shards_.EnqueueCcRequest(i, dead_lock_req);
        }

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_DeadLockResponse:
    {
        const DeadLockResponse &rsp = msg->dead_lock_response();
        DeadLockCheck::MergeRemoteWaitingLockInfo(&rsp);
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_BroadcastStatisticsRequest:
    {
        RemoteBroadcastStatisticsCc *broadcast_stat_req =
            broadcast_stat_pool_.NextRequest();
        broadcast_stat_req->Reset(std::move(msg));
        TX_TRACE_ASSOCIATE(msg.get(), broadcast_stat_req);

        const TableName &table_name = *broadcast_stat_req->SamplingTableName();
        uint16_t core_idx = txservice::Statistics::LeaderCore(table_name);

        local_shards_.EnqueueToCcShard(core_idx, broadcast_stat_req);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_AbortTransactionRequest:
    {
        uint32_t core_id = msg->abort_tran_req().core_id();
        RemoteAbortTransactionCc *req = abort_tran_pool_.NextRequest();
        req->Reset(std::move(msg));

        local_shards_.EnqueueCcRequest(core_id, req);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_AbortTransactionResponse:
    {
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_BlockedCcReqCheckRequest:
    {
        RemoteBlockReqCheckCc *req = blocked_req_check_pool_.NextRequest();
        CcMessage *msg_raw_ptr = msg.get();

        req->Reset(std::move(msg),
                   msg_raw_ptr->blocked_check_req().cce_addr_size());

        for (const auto &cce_addr : msg_raw_ptr->blocked_check_req().cce_addr())
        {
            uint32_t core_id = cce_addr.core_id();
            local_shards_.EnqueueCcRequest(core_id, req);
        }

        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_BlockedCcReqCheckResponse:
    {
        assert(msg->has_blocked_check_resp());
        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }

        const BlockedCcReqCheckResponse &resp = msg->blocked_check_resp();
        ResultTemplateType type = (ResultTemplateType) resp.result_temp_type();
        TransactionExecution *txm =
            reinterpret_cast<TransactionExecution *>(msg->txm_addr());

        txm->AcquireSharedForwardLatch();

        if (type == ResultTemplateType::AcquireKeyResult)
        {
            CcHandlerResult<std::vector<AcquireKeyResult>> *hd_res =
                reinterpret_cast<
                    CcHandlerResult<std::vector<AcquireKeyResult>> *>(
                    msg->handler_addr());

            assert(txm == hd_res->Txm());
            if (txm->TxNumber() != msg->tx_number() ||
                txm->CommandId() != msg->command_id() || hd_res->IsFinished())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }

            auto &acq_key_result_vec = hd_res->Value();
            if (acq_key_result_vec.at(resp.acq_key_result_vec_idx())
                    .IsRemoteHdResultSet(std::memory_order_acquire))
            {
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }

            acq_key_result_vec.at(resp.acq_key_result_vec_idx())
                .SetRemoteHdResult(true, std::memory_order_release);

            AckStatus status = (AckStatus) resp.req_status();
            if (status == AckStatus::ErrorTerm)
            {
                hd_res->SetError(CcErrorCode::NG_TERM_CHANGED);
            }
            else if (status == AckStatus::Finished)
            {
                // The cc request has been executed or aborted on the remote
                // node, the result is unknown. Treat it as the response is
                // lost otherwise txm might block forever if the response is
                // really lost.
                hd_res->SetError(CcErrorCode::REQUEST_LOST);
            }
            else if (status == AckStatus::BlockQueue)
            {
                // The cc request is still in blocking queue. The txm
                // should keep waiting.
            }
            hd_res->UnsetBlockReqCheckTs();

            txm->ReleaseSharedForwardLatch();
        }
        else if (type == ResultTemplateType::ReadKeyResult ||
                 type == ResultTemplateType::AcquireAllResult)
        {
            CcHandlerResultBase *hd_res =
                reinterpret_cast<CcHandlerResultBase *>(msg->handler_addr());

            if (txm->TxNumber() != msg->tx_number() ||
                txm->CommandId() != msg->command_id() || hd_res->IsFinished())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }

            AckStatus status = (AckStatus) resp.req_status();

            if (status == AckStatus::ErrorTerm)
            {
                hd_res->SetError(CcErrorCode::NG_TERM_CHANGED);
            }
            else if (status == AckStatus::Finished)
            {
                // The cc request has been executed or aborted on the remote
                // node, the result is unknown. Treat it as the response is
                // lost otherwise txm might block forever if the response is
                // really lost.
                hd_res->SetError(CcErrorCode::REQUEST_LOST);
            }
            else if (status == AckStatus::BlockQueue)
            {
                // The cc request is still in blocking queue. The txm should
                // keep waiting.
            }
            hd_res->UnsetBlockReqCheckTs();

            txm->ReleaseSharedForwardLatch();
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_KickoutDataRequest:
    {
        RemoteKickoutCcEntry *kickout_cc_entry_req =
            kickout_cc_entry_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), kickout_cc_entry_req);
        // Construct the ccrequest by deserializing the ccmessage
        kickout_cc_entry_req->Reset(std::move(msg));
        if (kickout_cc_entry_req->GetCleanType() ==
            txservice::CleanType::CleanCcm)
        {
            // Truncate table does not need to desrialize key, dispatch to all
            // cores directly
            for (uint16_t i = 0; i < local_shards_.Count(); i++)
            {
                local_shards_.EnqueueCcRequest(i, kickout_cc_entry_req);
            }
        }
        else
        {
            // req is enqueued to first core to parse key, then dispatched to
            // other cores to run in parallel.
            local_shards_.EnqueueCcRequest(0, kickout_cc_entry_req);
        }
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_KickoutDataResponse:
    {
        assert(msg->has_kickout_data_resp());

        CcHandlerResult<Void> *hd_res = nullptr;
        TransactionExecution *txm = nullptr;
        // Firstly, check whether the tx coordinator node is healthy, and
        // the original tx is healthy, if not, throw away this ccmessage.
        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx coordinator node has failed. Pointer stability
            // does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            LOG(ERROR) << "Receive remote kickoutccentry response, but tx"
                          " coordinator has failed.";
            break;
        }
        else
        {
            txm = reinterpret_cast<TransactionExecution *>(msg->txm_addr());

            txm->AcquireSharedForwardLatch();

            hd_res =
                reinterpret_cast<CcHandlerResult<Void> *>(msg->handler_addr());

            assert(txm == hd_res->Txm());
            if (txm->TxNumber() != msg->tx_number() ||
                txm->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx state machine has
                // been recycled. The response message is directed to an
                // obsolete tx.
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }
        }

        // Handle the result.
        const KickoutDataResponse &cc_resp = msg->kickout_data_resp();

        if (!cc_resp.error_code())
        {
            hd_res->SetRemoteError(
                ToLocalType::ConvertCcErrorCode(cc_resp.error_code()));
        }
        else
        {
            hd_res->SetRemoteFinished();
        }

        txm->ReleaseSharedForwardLatch();

        // Recycle the cc message
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ApplyRequest:
    {
        RemoteApplyCc *apply = apply_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), apply);
        apply->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(apply->key_shard_code_, apply);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ApplyResponse:
    {
        assert(msg->has_apply_cc_resp());

        CcHandlerResult<ObjectCommandResult> *hd_res = nullptr;
        TransactionExecution *txm = nullptr;

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            // Check txm before accessing hd_res which might be invalid if the
            // txm has already been reused.
            txm = reinterpret_cast<TransactionExecution *>(msg->txm_addr());
            txm->AcquireSharedForwardLatch();

            hd_res = reinterpret_cast<CcHandlerResult<ObjectCommandResult> *>(
                msg->handler_addr());
            if (txm->TxNumber() != msg->tx_number() ||
                txm->CommandId() != msg->command_id() || hd_res->IsFinished())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }
        }

        const ApplyResponse &apply_res = msg->apply_cc_resp();

        if (apply_res.error_code() != 0)
        {
            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(apply_res.error_code()));
        }
        else
        {
            ObjectCommandResult &obj_cmd_result = hd_res->Value();

            if (obj_cmd_result.cce_addr_.Term() < 0)
            {
                const CceAddr_msg &cce_addr_msg = apply_res.cce_addr();
                obj_cmd_result.cce_addr_.SetCceLock(cce_addr_msg.cce_lock_ptr(),
                                                    cce_addr_msg.term(),
                                                    cce_addr_msg.core_id());
                // CC entry's shard Id has been set when the request was
                // sent.
            }

            if (!apply_res.is_ack())
            {
                obj_cmd_result.rec_status_ =
                    ToLocalType::ConvertRecordStatusType(
                        apply_res.rec_status());

                if (obj_cmd_result.cmd_result_ != nullptr &&
                    apply_res.cmd_result().size() > 0)
                {
                    size_t offset = 0;
                    obj_cmd_result.cmd_result_->Deserialize(
                        apply_res.cmd_result().data(), offset);
                    assert(offset == apply_res.cmd_result().size());
                }

                obj_cmd_result.commit_ts_ = apply_res.commit_ts();
                obj_cmd_result.last_vali_ts_ = apply_res.last_vali_ts();
                obj_cmd_result.lock_acquired_ =
                    ToLocalType::ConvertLockType(apply_res.lock_type());
                obj_cmd_result.object_modified_ = apply_res.object_modified();

                hd_res->SetFinished();
            }
        }
        txm->ReleaseSharedForwardLatch();
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_PublishRequest:
    {
        // Get args from msg
        remote::PublishRequest *publish_req = msg->mutable_publish_req();
        const std::string &chan = publish_req->chan();
        const std::string &message = publish_req->message();

        local_shards_.PublishMessage(chan, message);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_UploadTxCommandsRequest:
    {
        RemoteUploadTxCommandsCc *cmds_req = upload_cmds_pool_.NextRequest();
        cmds_req->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(cmds_req->CceAddr()->CoreId(), cmds_req);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_DBSizeRequest:
    {
        RemoteDbSizeCc *dbsize = dbsize_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), dbsize);
        dbsize->Reset(std::move(msg),
                      Sharder::Instance().GetLocalCcShardsCount());
        for (size_t core_idx = 0;
             core_idx < Sharder::Instance().GetLocalCcShardsCount();
             ++core_idx)
        {
            local_shards_.EnqueueCcRequest(core_idx, dbsize);
        }

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_DBSizeResponse:
    {
        // TODO: fix the concurrency issue when the DBSizeCc::Wait() times out.
        const DBSizeResponse &resp = msg->db_size_resp();
        DbSizeCc *dbcc = reinterpret_cast<DbSizeCc *>(msg->handler_addr());

        std::vector<int64_t> total_obj_sizes;
        for (int idx = 0; idx < resp.node_obj_size_size(); ++idx)
        {
            total_obj_sizes.push_back(resp.node_obj_size(idx));
        }

        dbcc->AddRemoteObjSize(resp.dbsize_term(), total_obj_sizes);
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_KeyObjectStandbyForwardRequest:
    {
        KeyObjectStandbyForwardCc *cc =
            key_obj_standby_forward_pool_.NextRequest();
        cc->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(cc->ForwardMessageGroup(), cc);

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_StandbyHeartbeatRequest:
    {
        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_InvalidateTableCacheRequest:
    {
        RemoteInvalidateTableCacheCc *invalidate_req =
            invalidate_table_cache_pool_.NextRequest();
        invalidate_req->Reset(std::move(msg));
        TX_TRACE_ASSOCIATE(msg.get(), invalidate_req);
        local_shards_.EnqueueCcRequest(0, invalidate_req);
        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_InvalidateTableCacheResponse:
    {
        assert(msg->has_invalidate_table_cache_resp());

        CcHandlerResult<Void> *hd_res = nullptr;
        TransactionExecution *txm = nullptr;

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            txm = reinterpret_cast<TransactionExecution *>(msg->txm_addr());

            txm->AcquireSharedForwardLatch();

            hd_res =
                reinterpret_cast<CcHandlerResult<Void> *>(msg->handler_addr());

            assert(txm == hd_res->Txm());
            if (txm->TxNumber() != msg->tx_number() ||
                txm->CommandId() != msg->command_id())
            {
                msg_pool_.enqueue(std::move(msg));
                txm->ReleaseSharedForwardLatch();
                break;
            }
        }

        const InvalidateTableCacheResponse &invalidate_resp =
            msg->invalidate_table_cache_resp();
        if (invalidate_resp.error_code() != 0)
        {
            hd_res->SetRemoteError(
                ToLocalType::ConvertCcErrorCode(invalidate_resp.error_code()));
        }
        else
        {
            hd_res->SetRemoteFinished();
        }

        txm->ReleaseSharedForwardLatch();
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    default:
        break;
    }
}

}  // namespace remote
}  // namespace txservice
