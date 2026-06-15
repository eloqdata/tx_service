#include "log_instance.h"

#include <braft/raft.h>
#include <braft/route_table.h>
#include <butil/endpoint.h>

#include <atomic>
#include <mutex>
#include <string>
#include <thread>

#include "closure.h"
#include "log_service.h"

namespace txlog
{
// Should be consistent with txservice::SKIP_CHECK_TERM.
static constexpr int64_t SKIP_CHECK_TERM = -2;

void LogInstance::WriteLog(const LogRequest *request,
                           LogResponse *response,
                           google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    const int64_t term =
        term_if_is_lg_leader_.load(butil::memory_order_acquire);
    if (term < 0)
    {
        LOG(INFO) << "Follower receives WriteLog request, lg: " << group_;
        SetWriteLogErrorResponse(
            request, response, LogResponse_ResponseStatus_NotLeader);
        return;
    }

    // serialize data into log entry.
    butil::IOBuf log;
    butil::IOBufAsZeroCopyOutputStream log_wrapper(&log);
    if (!request->SerializeToZeroCopyStream(&log_wrapper))
    {
        LOG(ERROR) << "Failed to serialize the logging request at the log group"
                   << log_group_id_;
        SetWriteLogErrorResponse(
            request, response, LogResponse_ResponseStatus_Fail);
        return;
    }

    braft::Task task;
    task.data = &log;
    task.expected_term = term;

    ACTION_FAULT_INJECTOR("before_raft_log_apply");
    ProposeTaskAndWait(task, request, response);
}

void LogInstance::SetWriteLogErrorResponse(const LogRequest *request,
                                           LogResponse *response,
                                           LogResponse::ResponseStatus status)
{
    const WriteLogRequest &req = request->write_log_request();
    if (req.retry())
    {
        // should return unknown status when retry write log request is
        // forwarded to a follower node. Client should resend the request to
        // the new leader.
        response->set_response_status(
            LogResponse::ResponseStatus::LogResponse_ResponseStatus_Unknown);
    }
    else
    {
        DLOG(INFO)
            << "Log service: Write log request encounter an error, tx_number:"
            << req.txn_number() << ", error status: " << int(status);
        response->set_response_status(status);
    }
}

void LogInstance::ReplayLog(const LogRequest *request,
                            LogResponse *response,
                            google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    const int64_t term =
        term_if_is_lg_leader_.load(butil::memory_order_acquire);
    if (term < 0)
    {
        response->set_response_status(
            LogResponse::ResponseStatus::LogResponse_ResponseStatus_Fail);
        LOG(INFO) << "Set response status to LogResponse_ResponseStatus_Fail "
                     "due to term = "
                  << term;
        return;
    }

    // serialize data into log entry.
    butil::IOBuf log;
    butil::IOBufAsZeroCopyOutputStream log_wrapper(&log);
    if (!request->SerializeToZeroCopyStream(&log_wrapper))
    {
        LOG(ERROR)
            << "Failed to serialize the replay log request at the log group #"
            << log_group_id_;
        response->set_response_status(
            LogResponse::ResponseStatus::LogResponse_ResponseStatus_Fail);

        return;
    }

    braft::Task task;
    task.data = &log;
    task.expected_term = term;

    ProposeTaskAndWait(task, request, response);
}

void LogInstance::NotifyCheckpointTs(const LogRequest *request,
                                     LogResponse *response,
                                     google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    const int64_t term =
        term_if_is_lg_leader_.load(butil::memory_order_acquire);
    if (term < 0)
    {
        response->set_response_status(
            LogResponse::ResponseStatus::LogResponse_ResponseStatus_Fail);
        return;
    }

    // serialize data into log entry.
    butil::IOBuf log;
    butil::IOBufAsZeroCopyOutputStream log_wrapper(&log);
    if (!request->SerializeToZeroCopyStream(&log_wrapper))
    {
        LOG(ERROR) << "Failed to serialize request";
        response->set_response_status(
            LogResponse::ResponseStatus::LogResponse_ResponseStatus_Fail);

        return;
    }

    braft::Task task;
    task.data = &log;
    task.expected_term = term;

    ProposeTaskAndWait(task, request, response);
}

void LogInstance::RemoveCcNodeGroup(const LogRequest *request,
                                    LogResponse *response,
                                    google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    const int64_t term =
        term_if_is_lg_leader_.load(butil::memory_order_acquire);
    if (term < 0)
    {
        response->set_response_status(
            LogResponse::ResponseStatus::LogResponse_ResponseStatus_Fail);
        return;
    }

    // serialize data into log entry.
    butil::IOBuf log;
    butil::IOBufAsZeroCopyOutputStream log_wrapper(&log);
    if (!request->SerializeToZeroCopyStream(&log_wrapper))
    {
        LOG(ERROR) << "Failed to serialize request";
        response->set_response_status(
            LogResponse::ResponseStatus::LogResponse_ResponseStatus_Fail);

        return;
    }

    braft::Task task;
    task.data = &log;
    task.expected_term = term;

    ProposeTaskAndWait(task, request, response);
}

/**
 * @brief RecoverTx is responsible for recovering orphan locks.
 *
 * Transaction coordinator could crash after acquiring locks but before
 * releasing locks on remote node, which results in orphan locks. Orphan locks
 * should be cleared if the original tx which acquired the locks was aborted.
 * While orphan locks should be uploaded (just like what PostWrite does) with
 * the latest value and then be released if the original tx was committed. Here
 * the upload value can be obtained from log service.
 *
 * Whether the tx (identified by tx_number) is committed or not can be
 * determined by checking the redo logs.
 *
 */
void LogInstance::RecoverTx(const RecoverTxRequest *request,
                            RecoverTxResponse *response,
                            google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    // RecoverTx can only be served by the latest leader.
    if (!IsLatestLeader())
    {
        response->set_tx_status(::txlog::RecoverTxResponse_TxStatus::
                                    RecoverTxResponse_TxStatus_RecoverError);
        return;
    }

    // participant_cc_ng_id is the node group id where the lock resides.
    uint32_t participant_cc_ng_id = request->cc_node_group_id();

    // The cc node group in which the tx resides has failed. Searches the
    // log state machine to find if there is a log record committed by the
    // specified tx.

    int64_t dest_node_term = log_state_->GetNgLeaderTerm(participant_cc_ng_id);

    if (dest_node_term < 0)
    {
        LOG(ERROR) << "The log state machine at the log group #"
                   << log_group_id_
                   << " does not contain information of the leader of the "
                      "cc node group #"
                   << participant_cc_ng_id;

        response->set_tx_status(
            ::txlog::RecoverTxResponse_TxStatus_RecoverError);
        return;
    }

    if (dest_node_term != request->cc_ng_term())
    {
        // The cc node group of the participant(where lock resides) has failed
        // over to a new leader. There is no need to recover the orphan lock in
        // the old leader because the cc map will be cleared anyway. So, pretend
        // the tx has failed.
        response->set_tx_status(
            ::txlog::RecoverTxResponse_TxStatus_NotCommitted);
        return;
    }

    // Assumption: we always store one log record in log state even if we have
    // several physical log records for heavy load case.
    auto [success, record] =
        log_state_->SearchTxDataLog(request->lock_tx_number(),
                                    participant_cc_ng_id,
                                    request->write_lock_ts());

    if (!success)
    {
        response->set_tx_status(
            ::txlog::RecoverTxResponse_TxStatus_RecoverError);
        return;
    }

    bool has_multi_stage_log =
        log_state_->SearchTxSchemaLog(request->lock_tx_number()).first;

    if (!has_multi_stage_log)
    {
        has_multi_stage_log =
            log_state_->SearchTxSplitRangeOp(request->lock_tx_number()).first;
    }

    if (!has_multi_stage_log)
    {
        has_multi_stage_log =
            log_state_->SearchTxClusterScaleOp(request->lock_tx_number()).first;
    }

    if (!has_multi_stage_log)
    {
        has_multi_stage_log =
            log_state_->SearchTxDataMigrationOp(request->lock_tx_number());
    }

    if (record == nullptr && !has_multi_stage_log)
    {
        // No log record found for the specified tx. The tx is deemed aborted.
        response->set_tx_status(
            ::txlog::RecoverTxResponse_TxStatus_NotCommitted);
        return;
    }

    // Set txn status to committed. The ccnode will not clear the txn's lock.
    // For DML txn, the txn log record will be shipped to ccnode asynchronously
    // and the lock will be cleared when the log record is replayed.
    // For multi-stage txn like DDL and range split, since it has witten log,
    // the txn is guaranteed to succeed and release all the locks.
    response->set_tx_status(::txlog::RecoverTxResponse_TxStatus_Committed);
    if (record != nullptr)
    {
        LogShippingAgent *ship_agent = nullptr;

        std::lock_guard guard(log_replay_workers_mutex_);
        auto replay_it = log_replay_workers_.find(participant_cc_ng_id);
        if (replay_it == log_replay_workers_.end())
        {
            auto new_replay_it = log_replay_workers_.try_emplace(
                participant_cc_ng_id,
                std::make_unique<LogShippingAgent>(
                    log_group_id_,
                    participant_cc_ng_id,
                    dest_node_term,
                    request->source_ip(),
                    request->source_port(),
                    std::unique_ptr<ItemIterator>{},
                    0,
                    0,
                    0,
                    false));

            ship_agent = new_replay_it.first->second.get();
        }
        else
        {
            ship_agent = replay_it->second.get();
        }

        // The tx committed a record. The record will be shipped by the
        // shipping agent.
        // Before add record to ship_agent, recheck that the dest node term
        // matches ship_agent, otherwise stale record would be shipped to a
        // newer cc node. This could happen if cc node group failover happens
        // after above term check.
        if (ship_agent->NodeGroupTerm() != dest_node_term)
        {
            response->set_tx_status(
                ::txlog::RecoverTxResponse_TxStatus_NotCommitted);
            return;
        }
        ship_agent->AddLogRecord(record);
    }
}

void LogInstance::CheckMigrationIsFinished(
    const CheckMigrationIsFinishedRequest *request,
    CheckMigrationIsFinishedResponse *response,
    google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    bool all_finished =
        log_state_->CheckAllMigrationIsFinished(request->cluster_scale_txn());
    response->set_finished(all_finished);
}

void LogInstance::CheckClusterScaleStatus(const LogRequest *request,
                                          LogResponse *response,
                                          google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    const int64_t term =
        term_if_is_lg_leader_.load(butil::memory_order_acquire);
    if (term < 0)
    {
        response->set_response_status(
            LogResponse_ResponseStatus::LogResponse_ResponseStatus_Fail);
        return;
    }

    // serialize data into log entry.
    butil::IOBuf log;
    butil::IOBufAsZeroCopyOutputStream log_wrapper(&log);
    if (!request->SerializeToZeroCopyStream(&log_wrapper))
    {
        LOG(ERROR) << "Failed to serialize the LogRequest "
                      "request at the log group #"
                   << log_group_id_;

        response->set_response_status(
            LogResponse_ResponseStatus::LogResponse_ResponseStatus_Fail);
        return;
    }

    braft::Task task;
    task.data = &log;
    task.expected_term = term;

    ProposeTaskAndWait(task, request, response);
}

bool LogInstance::BatchAddLogItems(
    std::array<LogStateAddLogItemTask, kLogStateAddLogItemTaskBatchSize>
        &write_log_tasks,
    size_t num) const
{
    for (size_t i = 0; i < num; ++i)
    {
        LogStateAddLogItemTask task = write_log_tasks[i];
        const WriteLogRequest *req = task.write_log_request;
        uint64_t txn_number = req->txn_number();
        uint64_t timestamp = req->commit_timestamp();
        const auto &node_txn_logs =
            req->log_content().data_log().node_txn_logs();
        for (auto it = node_txn_logs.begin(); it != node_txn_logs.end(); ++it)
        {
            log_state_->AddLogItemBatch(
                it->first, txn_number, timestamp, it->second);
        }
    }
    log_state_->FlushLogItemBatch();
    for (size_t i = 0; i < num; ++i)
    {
        LogStateAddLogItemTask task = write_log_tasks[i];
        if (task.done != nullptr)
        {
            task.done->Run();
        }
    }
    return true;
}

void LogInstance::on_apply(braft::Iterator &iter_log)
{
    // 4KB on the stack should be fine since bthread stack size is 32KB
    std::array<LogStateAddLogItemTask, kLogStateAddLogItemTaskBatchSize>
        write_log_batch;
    size_t batch_idx = 0;
    // Hold the LogRequest parsed from iter_log.data().
    LogRequest request;
    std::vector<LogRequest> requests;

    for (; iter_log.valid(); iter_log.next())
    {
        // Synchronous closure guard is sufficient when closure `run`
        // functor is not heavy. braft::AsyncClosureGuard could be used
        // in future in case for complicated closure.
        brpc::ClosureGuard done_guard(iter_log.done());
        LogResponse *response = nullptr;
        const LogRequest *req_ptr = nullptr;
        // If iter_log.done() is EITHER null OR equal to task.done
        // - If iter_log.done() is not null, this task is applied by this
        //   node(this node is leaders). We can skip the deserialization and
        //   get the closure from iter_log.done();
        // - If iter_log.done() is null, this task is synced from the leader.
        //   We have to parse the log.
        if (iter_log.done())
        {
            RaftLogClosure *closure =
                dynamic_cast<RaftLogClosure *>(iter_log.done());
            response = closure->response();
            req_ptr = closure->request();
        }
        else
        {
            butil::IOBufAsZeroCopyInputStream wrapper(iter_log.data());
            CHECK(request.ParseFromZeroCopyStream(&wrapper));
            req_ptr = &request;
        }

        if (BAIDU_UNLIKELY(req_ptr->request_case() ==
                               LogRequest::RequestCase::kReplayLogRequest ||
                           batch_idx == write_log_batch.size()))
        {
            BatchAddLogItems(write_log_batch, batch_idx);
            batch_idx = 0;
            requests.clear();
        }

        ExecuteResult res = execute(req_ptr, response);
        if (res == ExecuteResult::NeedBatch)
        {
            if (req_ptr == &request)
            {
                // Store the request object, since the WriteLogRequest is
                // batched and will be accessed later.
                if (requests.capacity() == 0)
                {
                    requests.reserve(write_log_batch.size());
                }
                requests.emplace_back(std::move(request));
                req_ptr = &requests.back();
            }
            write_log_batch[batch_idx++] = {&req_ptr->write_log_request(),
                                            iter_log.done()};
            // done->Run() will be called in BatchAddLogItems.
            done_guard.release();
        }
    }
    BatchAddLogItems(write_log_batch, batch_idx);
}

void LogInstance::TransferLeader(const TransferRequest *request,
                                 TransferResponse *response,
                                 google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    uint32_t leader_idx = request->leader_idx();

    Peer peer;
    {
        std::lock_guard<bthread::Mutex> lock(config_mutex_);
        if (leader_idx >= peer_vct_.size())
        {
            return;
        }

        // peer_vct_ store the addresses for each log group. leader_idx
        // specifies the target leader's index in the log group.
        peer = peer_vct_.at(leader_idx);
    }
    braft::PeerId first_peer;
    if (0 != butil::str2ip(peer.ip.c_str(), &first_peer.addr.ip))
    {
        // for case `peer.ip` is hostname format
        first_peer.type_ = braft::PeerId::Type::HostName;
        first_peer.hostname_addr.hostname = peer.ip;
        first_peer.hostname_addr.port = peer.port;
    }
    else
    {
        butil::str2endpoint(
            peer.ip.c_str(), static_cast<int>(peer.port), &first_peer.addr);
    }

    int err = node_->transfer_leadership_to(first_peer);
    response->set_error(err != 0);
}

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
void LogInstance::OnInMemStateFull(
    size_t log_count,
    size_t log_size,
    std::function<void(bool, uint64_t)> done) const
{
    SaveSnapshotDone *snapshot_done =
        new SaveSnapshotDone(log_count, std::move(done));
    // The snapshot is done asynchronously in node_, so we need to call it in
    // another thread
    node_->snapshot(snapshot_done);
}
#endif

LogInstance::ExecuteResult LogInstance::execute(const LogRequest *request,
                                                LogResponse *response)
{
    bool need_batch = false;
    switch (request->request_case())
    {
    case LogRequest::RequestCase::kWriteLogRequest:
    {
        if (IsLatestLeader())
        {
            ACTION_FAULT_INJECTOR("before_raft_execute_write_log");
        }

        const WriteLogRequest &req = request->write_log_request();

        const ::google::protobuf::Map<::google::protobuf::uint32,
                                      ::google::protobuf::int64> &node_terms =
            req.node_terms();

        // Match tx_term with the latest term in log service, reject the
        // WriteLogRequest from stale ccng leader as the locks it holds
        // might have been recovered.
        uint32_t high_half = req.txn_number() >> 32L;
        uint32_t ng_id = high_half >> 10;
        int64_t state_term = log_state_->GetNgLeaderTerm(ng_id);

        if (state_term != req.tx_term())
        {
            if (response != nullptr)
            {
                LOG(ERROR) << "Tx term not match, tx's ng id: " << ng_id
                           << ", tx_term: " << req.tx_term()
                           << ", ng latest term: " << state_term
                           << ", tx number: " << req.txn_number();
                response->set_response_status(
                    LogResponse::ResponseStatus::
                        LogResponse_ResponseStatus_Fail);
            }
            return ExecuteResult::Finished;
        }

        // Matches the terms of the locks obtained by the tx against the cc node
        // group terms in the log group. If there is a mismatch, it means that
        // the corresponding cc node group has failed over and the lock obtained
        // by the tx has been invalidated.
        // This is crucial for one-phase commit protocol. Considering the
        // following case: A transaction try to update a record A on node group
        // I. It will firstly acquire the write lock of A and record the term of
        // node group I. If node group I's leader changed (e.g. original leader
        // failover), then the write lock on A is lost in the new leader and the
        // record A can be updated by other transactions. Go back to the old
        // transaction, suppose it try to commit now, since the record A is
        // already updated by other transactions, we must reject this commit
        // command. Check whether node group term changed is the safe guard for
        // the above case.
        bool terms_match = true;
        for (auto iter = node_terms.begin(); iter != node_terms.end(); ++iter)
        {
            const ::google::protobuf::uint32 &cc_ng_id = iter->first;
            const ::google::protobuf::int64 &cc_ng_term = iter->second;
            state_term = log_state_->GetNgLeaderTerm(cc_ng_id);
            // In some case, before write log, there is no need to acquire lock
            // bacause no concurrent transactions to access the same key, so
            // there is no need to check the term(Identified by
            // SKIP_CHECK_TERM). For example, initialize the sequence record
            // during create table.
            if (state_term != cc_ng_term && cc_ng_term != SKIP_CHECK_TERM)
            {
                terms_match = false;
                LOG(ERROR) << "Txn lock term does not match, lock node group: "
                           << cc_ng_id << ", lock term=" << cc_ng_term
                           << ", state term=" << state_term;
                break;
            }
        }

        if (!terms_match)
        {
            // terms not match, write log failed
            if (response != nullptr)
            {
                // For retried write log request, we need to check whether the
                // previous write log succeeds or not, since we only do write
                // log retry when return status is unknown. Note that write log
                // is not idempotent. A successful write log request may become
                // failed in the next same request which is caused by ccnode
                // term changed. To handle this case, if a retried write log
                // request failed, log state needs to be searched to decide the
                // actual result of previous "result unknown"(from the txn
                // coordinator perspective) write log of this txn, which might
                // be success.
                bool is_retry_request = req.retry();
                if (is_retry_request)
                {
                    const ::google::protobuf::uint64 &tx_number =
                        req.txn_number();

                    bool committed = false;
                    const LogContentMessage &log_content = req.log_content();
                    switch (log_content.content_case())
                    {
                    case LogContentMessage::ContentCase::kDataLog:
                    {
                        // write log request at least contains one kv pair.
                        assert(node_terms.size() > 0);
                        const ::google::protobuf::uint32 &cc_ng_id =
                            node_terms.begin()->first;

                        // SearchTxDataLog return record not empty indicates
                        // last write log succeeds.
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
                        // With rocksdb cloud, only check leader if previous
                        // write is succeed.
                        const int64_t term = term_if_is_lg_leader_.load(
                            butil::memory_order_acquire);
                        if (term >= 0)
                        {
                            auto [success, record] =
                                log_state_->SearchTxDataLog(tx_number,
                                                            cc_ng_id);
                            if (success)
                            {
                                committed = record != nullptr;
                            }
                        }
#else
                        auto [success, record] =
                            log_state_->SearchTxDataLog(tx_number, cc_ng_id);
                        if (success)
                        {
                            committed = record != nullptr;
                        }
#endif
                        break;
                    }
                    case LogContentMessage::ContentCase::kSchemaLog:
                    {
                        // only prepare log will check term and reach here
                        std::pair<bool, SchemaOpMessage_Stage> result =
                            log_state_->SearchTxSchemaLog(tx_number);
                        bool found = result.first;
                        SchemaOpMessage_Stage flushed_stage = result.second;
                        SchemaOpMessage_Stage stage =
                            log_content.schema_log().stage();
                        committed = (found && flushed_stage >= stage);
                        break;
                    }
                    case LogContentMessage::ContentCase::kSplitRangeLog:
                    {
                        // only prepare log will check term and reach here
                        std::pair<bool, SplitRangeOpMessage_Stage> result =
                            log_state_->SearchTxSplitRangeOp(tx_number);
                        bool found = result.first;
                        SplitRangeOpMessage_Stage flushed_stage = result.second;
                        SplitRangeOpMessage_Stage stage =
                            log_content.split_range_log().stage();
                        committed = (found && flushed_stage >= stage);
                        break;
                    }
                    default:
                        break;
                    }

                    if (committed)
                    {
                        // although this retried write log failed, the txn's
                        // previous write log have succeeded
                        response->set_response_status(
                            LogResponse::ResponseStatus::
                                LogResponse_ResponseStatus_Success);
                        return ExecuteResult::Finished;
                    }
                }

                response->set_response_status(
                    LogResponse::ResponseStatus::
                        LogResponse_ResponseStatus_Fail);
            }

            return ExecuteResult::Finished;
        }

        const ::google::protobuf::uint64 &timestamp = req.commit_timestamp();
        bool duplicate_migration_tx = false;
        bool duplicate_cluster_scale_tx = false;
        uint64_t newest_cluster_scale_txn = (uint64_t) UINT32_MAX << 32L;

        const LogContentMessage &log_content = req.log_content();
        {
            switch (log_content.content_case())
            {
            case LogContentMessage::ContentCase::kDataLog:
            {
                need_batch = true;

                const DataLogMessage &data_log = log_content.data_log();
                if (!data_log.schema_logs().empty())
                {
                    log_state_->UpsertSchemaOpWithinDML(
                        req.txn_number(), timestamp, data_log.schema_logs());
                }
                break;
            }
            case LogContentMessage::ContentCase::kSchemaLog:
            {
                const SchemaOpMessage &schema_log = log_content.schema_log();
                log_state_->UpsertSchemaOp(
                    req.txn_number(), timestamp, schema_log);
                break;
            }
            case LogContentMessage::ContentCase::kSplitRangeLog:
            {
                const SplitRangeOpMessage &split_range_op_message =
                    log_content.split_range_log();
                log_state_->UpdateSplitRangeOp(
                    req.txn_number(), timestamp, split_range_op_message);
                break;
            }
            case LogContentMessage::ContentCase::kClusterScaleLog:
            {
                const ClusterScaleOpMessage &cluster_scale_op_message =
                    log_content.cluster_scale_log();
                bool success = false;
                std::tie(success, newest_cluster_scale_txn) =
                    log_state_->UpdateClusterScaleOp(
                        req.txn_number(), timestamp, cluster_scale_op_message);
                duplicate_cluster_scale_tx = (success == false);
                break;
            }
            case LogContentMessage::ContentCase::kMigrationLog:
            {
                const DataMigrateTxLogMessage &migration_tx_log_message =
                    log_content.migration_log();
                duplicate_migration_tx =
                    !log_state_->UpdateNodeGroupBucketMigrateLog(
                        req.txn_number(), timestamp, migration_tx_log_message);
                break;
            }
            default:
                break;
            }
        }

        // update latest txn number committed from this cc node group.
        // the lower 32 bits of req.txn_number() is the txn's local
        // identifier in the core it was generated from
        log_state_->UpdateLatestCommittedTxnNumber(
            ng_id, req.txn_number() & 0xFFFFFFFF);
        log_state_->UpdateLatestCommitTs(ng_id, timestamp);

        if (response != nullptr)
        {
            if (newest_cluster_scale_txn != ((uint64_t) UINT32_MAX << 32L))
            {
                response->clear_write_log_response();
                response->mutable_write_log_response()
                    ->set_newest_cluster_scale_txn(newest_cluster_scale_txn);
            }

            if (duplicate_migration_tx)
            {
                response->set_response_status(
                    LogResponse::ResponseStatus::
                        LogResponse_ResponseStatus_DuplicateMigrationTx);
            }
            else if (duplicate_cluster_scale_tx)
            {
                response->set_response_status(
                    LogResponse::ResponseStatus::
                        LogResponse_ResponseStatus_DuplicateClusterScaleTx);
            }
            else
            {
                if (log_content.content_case() ==
                        LogContentMessage::ContentCase::kSchemaLog ||
                    log_content.content_case() ==
                        LogContentMessage::ContentCase::kSplitRangeLog ||
                    log_content.content_case() ==
                        LogContentMessage::ContentCase::kClusterScaleLog ||
                    log_content.content_case() ==
                        LogContentMessage::ContentCase::kMigrationLog)
                {
                    // Pass all node group latest leader info to the multi-stage
                    // txn coordinator. So that the coordinator can send
                    // requests to the latest leader if participant node group
                    // failover happens.
                    auto *write_log_resp =
                        response->mutable_write_log_response();
                    auto *leader_infos =
                        write_log_resp->mutable_node_group_leader_info();
                    const std::unordered_map<uint32_t, LogState::CcNgInfo>
                        &all_ng_leader_infos = log_state_->GetCcNgInfo();
                    for (const auto &[ng_id, leader_info] : all_ng_leader_infos)
                    {
                        // Fill LeaderInfo: set term, ip and port.
                        auto &out = (*leader_infos)[ng_id];
                        out.set_term(leader_info.term_);
                        out.set_ip(leader_info.leader_ip_);
                        out.set_port(leader_info.leader_port_);
                    }
                }

                response->set_response_status(
                    LogResponse::ResponseStatus::
                        LogResponse_ResponseStatus_Success);
            }
        }
        break;
    }
    case LogRequest::RequestCase::kReplayLogRequest:
    {
        LOG(INFO) << "Received ReplayLogRequest.";
        const ReplayLogRequest &req = request->replay_log_request();
        uint32_t cc_ng_id = req.cc_node_group_id();
        int64_t cc_ng_term = req.term();
        const std::string &cc_node_ip = req.source_ip();
        uint16_t cc_node_port = req.source_port();

        if (term_if_is_lg_leader_.load(butil::memory_order_acquire) >= 0)
        {
            // Only the leader of a log group shipps tx logs to the
            // recovering leader of the cc node.

            bool term_match = log_state_->UpdateNgTerm(
                cc_ng_id, cc_ng_term, cc_node_ip, cc_node_port);

            LOG(INFO) << "ReplayLogRequest cc_ng_id: " << cc_ng_id
                      << " term: " << cc_ng_term << " ip: " << cc_node_ip
                      << " port: " << cc_node_port << " term match? "
                      << (term_match ? "yes" : "no");
            // if req.no_replay set, skip replay, only for test
            if (term_match && !req.no_replay())
            {
                LOG(INFO) << "shipping log records to: " << cc_node_ip << " "
                          << cc_node_port;
                uint64_t last_ckpt_ts = log_state_->LastCkptTimestamp(cc_ng_id);
                uint64_t latest_commit_ts =
                    log_state_->LatestCommitTsOfAllNodeGroups();
                // get log list since last_ckpt_ts+1
                // 0 indicates no checkpoint happened
                uint64_t start_ts = last_ckpt_ts == 0 ? 0 : last_ckpt_ts + 1;
                if (req.start_ts() != 0)
                {
                    start_ts = std::min(start_ts, req.start_ts());
                }
                LOG(INFO) << "start replaying from ts " << start_ts
                          << ". on log group id: " << log_group_id_;

                auto res = log_state_->GetLogReplayList(cc_ng_id, start_ts);
                auto &[ok, iterator] = res;

                if (!ok)
                {
                    if (response)
                    {
                        response->set_response_status(
                            LogResponse::ResponseStatus::
                                LogResponse_ResponseStatus_Fail);
                    }
                    return ExecuteResult::Finished;
                }

                std::lock_guard guard(log_replay_workers_mutex_);

                uint32_t latest_txn_no =
                    log_state_->LatestCommittedTxnNumber(cc_ng_id);

                auto it = log_replay_workers_.find(cc_ng_id);
                if (it != log_replay_workers_.end())
                {
                    std::unique_ptr<LogShippingAgent> &curr_ship_agent =
                        it->second;
                    curr_ship_agent->Terminate();
                    while (!curr_ship_agent->IsTerminated())
                    {
                        bthread_usleep(1000);
                    }
                }

                // The new log shipping agent spawns a background
                // thread that ships log records to the recovering
                // leader of the cc node group. When there is
                // already a shipping agent for the specified node
                // group, either active or inactive, replaces the
                // old shipping agent with a new one. The old agent
                // may still be active, when the old leader has not
                // finished failover before transferring the
                // leadership to a new cc node. The replacement will
                // interrupt and de-allocate the old shipping agent.
                log_replay_workers_.insert_or_assign(
                    cc_ng_id,
                    std::make_unique<LogShippingAgent>(log_group_id_,
                                                       cc_ng_id,
                                                       cc_ng_term,
                                                       cc_node_ip,
                                                       cc_node_port,
                                                       std::move(iterator),
                                                       latest_txn_no,
                                                       latest_commit_ts,
                                                       last_ckpt_ts,
                                                       true));
            }
        }
        else
        {
            // In a non-leader node of the log group, updating the
            // cc node group's term is thread-safe, because
            // concurrent reads of the log state machine for
            // recovering tx's locks are never directed to the log
            // group's followers.
            LOG(INFO) << "The node is not leader, term_if_is_lg_leader_ < 0";
            log_state_->UpdateNgTerm(
                cc_ng_id, cc_ng_term, cc_node_ip, cc_node_port);
        }

        if (response)
        {
            response->set_response_status(
                LogResponse::ResponseStatus::
                    LogResponse_ResponseStatus_Success);
            LOG(INFO)
                << "Set response status to LogResponse_ResponseStatus_Success";
        }

        break;
    }
    case LogRequest::RequestCase::kUpdateCkptTsRequest:
    {
        const auto &req = request->update_ckpt_ts_request();
        const ::google::protobuf::uint32 &node_group = req.cc_node_group_id();
        const ::google::protobuf::int64 &term = req.cc_ng_term();
        const ::google::protobuf::uint64 &timestamp = req.ckpt_timestamp();
        // only update checkpoint ts if request term matches latest term
        if (term == log_state_->GetNgLeaderTerm(node_group))
        {
            // TODO: keep txn logs for a period of time (weeks or months) for
            //  future point in time recovery, truncate log routinely instead
            //  of deleting them every time cc node do checkpoints

            // log_state_->DeleteLogItems(node_id, timestamp);

            log_state_->UpdateCkptTs(node_group, timestamp);
        }
        if (response)
        {
            response->set_response_status(
                LogResponse::ResponseStatus::
                    LogResponse_ResponseStatus_Success);
        }
        break;
    }
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    case LogRequest::RequestCase::kPostSnapshotRequest:
    {
        const PostSnapshotRequest &req = request->post_snapshot_request();
        const ::google::protobuf::uint64 &last_applied_tx_number =
            req.last_applied_tx_num();
        log_state_->PurgeLogItemsFromMemState(last_applied_tx_number);

        if (response)
        {
            response->set_response_status(
                LogResponse::ResponseStatus::
                    LogResponse_ResponseStatus_Success);
        }
        break;
    }
#endif
    case LogRequest::RequestCase::kCheckScaleStatusRequest:
    {
        const CheckClusterScaleStatusRequest &check_cluster_scale_status_req =
            request->check_scale_status_request();
        CheckClusterScaleStatusResponse::Status status =
            log_state_->CheckClusterScaleStatus(
                check_cluster_scale_status_req.id());

        if (response)
        {
            response->set_response_status(
                LogResponse::ResponseStatus::
                    LogResponse_ResponseStatus_Success);
            CheckClusterScaleStatusResponse *check_scale_status_response =
                response->mutable_check_scale_status_response();
            check_scale_status_response->set_status(status);
        }

        break;
    }
    case LogRequest::RequestCase::kRemoveCcNodeGroupRequest:
    {
        const auto &req = request->remove_cc_node_group_request();
        const ::google::protobuf::uint32 &node_group = req.cc_node_group_id();
        const ::google::protobuf::int64 &term = req.cc_ng_term();
        // only update checkpoint ts if request term matches latest term
        if (term == log_state_->GetNgLeaderTerm(node_group))
        {
            // remove cc node group from cc_ng_info_;
            log_state_->RemoveCcNodeGroup(node_group, term);
        }
        if (response)
        {
            response->set_response_status(
                LogResponse::ResponseStatus::
                    LogResponse_ResponseStatus_Success);
        }
        break;
    }
    default:
    {
        LOG(ERROR) << "ERR: request type unknown: " << request->request_case()
                   << " request: " << request->DebugString();
        throw std::runtime_error("Unknown request");
        break;
    }
    }
    return need_batch ? ExecuteResult::NeedBatch : ExecuteResult::Finished;
}

int LogInstance::SetDefaultOptions(braft::NodeOptions *node_options)
{
    if (node_options->initial_conf.parse_from(raft_conf_) != 0)
    {
        LOG(ERROR) << "Failed to parse configuration `" << raft_conf_;
        return -1;
    }

    node_options->fsm = this;
    node_options->log_uri = storage_path_ + "/log";
    node_options->raft_meta_uri = storage_path_ + "/raft_meta";
    node_options->snapshot_uri = storage_path_ + "/snapshot";

    return 0;
}

void LogInstance::LogGroupLeaderUpdate()
{
    const std::unordered_map<uint32_t, LogState::CcNgInfo> &cc_ngs =
        log_state_->GetCcNgInfo();
    for (const auto &[ng_id, leader_info] : cc_ngs)
    {
        brpc::Channel channel;
        butil::ip_t ip_t;
        if (0 != butil::str2ip(leader_info.leader_ip_.c_str(), &ip_t))
        {
            // for case `leader_info.leader_ip_` is hostname format
            std::string naming_service_url;
            braft::HostNameAddr hostname_addr(leader_info.leader_ip_,
                                              leader_info.leader_port_);
            braft::HostNameAddr2NSUrl(hostname_addr, naming_service_url);
            if (channel.Init(naming_service_url.c_str(),
                             braft::LOAD_BALANCER_NAME,
                             nullptr) != 0)
            {
                // Fails to establish the channel to the follower.
                LOG(ERROR)
                    << "Failed to init the channel to a node of log group:"
                    << log_group_id_ << " node id:" << node_id_
                    << " for leader update.";
                continue;
            }
        }
        else
        {
            if (channel.Init(leader_info.leader_ip_.c_str(),
                             static_cast<int>(leader_info.leader_port_),
                             nullptr) != 0)
            {
                // Fails to establish the channel to the follower.
                LOG(ERROR)
                    << "Failed to init the channel to a node of log group:"
                    << log_group_id_ << " node id:" << node_id_
                    << " for leader update.";
                continue;
            }
        }

        LogReplayService_Stub stub(&channel);
        LogLeaderUpdateRequest req;
        req.set_lg_id(log_group_id_);
        req.set_node_id(node_id_);
        LogLeaderUpdateResponse res;
        res.set_error(false);

        brpc::Controller cntl;
        cntl.set_timeout_ms(-1);
        stub.UpdateLogGroupLeader(&cntl, &req, &res, nullptr);

        // Retry is not needed at here, the remote nodes will also refresh their
        // leader index passively.
        if (cntl.Failed())
        {
            LOG(ERROR) << "Failed the UpdateLogGroupLeader RPC of log group:"
                       << log_group_id_ << "node id:" << node_id_
                       << ". Error code: " << cntl.ErrorCode()
                       << ". Msg: " << cntl.ErrorText();
        }
        else if (res.error())
        {
            LOG(ERROR) << "Failed to notify the new leader of log group: "
                       << log_group_id_ << " node id:" << node_id_;
        }
    }
}

void LogInstance::on_snapshot_save(braft::SnapshotWriter *writer,
                                   braft::Closure *done)
{
    // Save current StateMachine in memory and starts a new bthread to avoid
    // blocking StateMachine since it's a bit slow to write data to disk
    // file.
    auto start = std::chrono::high_resolution_clock::now();
    SnapshotClosure *sc = new SnapshotClosure;
    sc->writer = writer;
    sc->done = done;
    sc->log_state_ref = log_state_.get();
    sc->log_instance_ref = this;
    log_state_->BeginSnapshot();
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    if (term_if_is_lg_leader_.load(std::memory_order_acquire) >= 0)
    {
        LogStateRocksDBCloudImpl *log_state_rocksdb_cloud_ref =
            static_cast<LogStateRocksDBCloudImpl *>(sc->log_state_ref);
        sc->dbc_flush = log_state_rocksdb_cloud_ref->GetDBCloudContainer();
        sc->leader_term = term_if_is_lg_leader_.load(std::memory_order_acquire);
        sc->last_tx_num =
            log_state_rocksdb_cloud_ref->GetSnapshotLastAppliedTx();
    }
    else
    {
        sc->dbc_flush = nullptr;
        sc->leader_term = -1;
        sc->last_tx_num = 0;
    }
    LOG(INFO) << "save_snapshot: term_if_is_lg_leader_: "
              << term_if_is_lg_leader_.load(std::memory_order_acquire)
              << ", last_tx_num: " << sc->last_tx_num;
#endif
    auto stop = std::chrono::high_resolution_clock::now();
    uint64_t ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(stop - start)
            .count();
    LOG(INFO) << "log server generate snapshot cost " << ms << " millseconds";
    bthread_t tid;
    bthread_start_urgent(&tid, NULL, save_snapshot, sc);
}

void *LogInstance::save_snapshot(void *arg)
{
    LOG(INFO) << "Save snapshot start";

    SnapshotClosure *sc = static_cast<SnapshotClosure *>(arg);
    std::unique_ptr<SnapshotClosure> sc_guard(sc);
    brpc::ClosureGuard done_guard(sc->done);
    const std::string &snapshot_path = sc->writer->get_path();

    auto snapshot_files = sc->log_state_ref->WriteSnapshot(snapshot_path);
    for (const auto &file_path : snapshot_files)
    {
        LOG(INFO) << "adding snapshot file: " << file_path;
        CHECK_EQ(0, sc->writer->add_file(file_path));
    }

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    LogStateRocksDBCloudImpl *log_state_rocksdb_cloud_ref =
        static_cast<LogStateRocksDBCloudImpl *>(sc->log_state_ref);
    int64_t snapshot_leader_term = sc->leader_term;
    uint64_t snapshot_last_tx_num = sc->last_tx_num;

    if (snapshot_leader_term >= 0 &&
        snapshot_leader_term ==
            sc->log_instance_ref->term_if_is_lg_leader_.load(
                std::memory_order_acquire) &&
        snapshot_last_tx_num > 0)
    {
        LOG(INFO) << "Write PostSnapshotRequest to log from lg"
                  << sc->log_instance_ref->log_group_id_ << " leader";
        auto start = std::chrono::high_resolution_clock::now();
        if (!log_state_rocksdb_cloud_ref->CheckOrWaitForMemDBInSync(
                "FlushRocksDBCloud"))
        {
            // rocksdb cloud rolling up timeout
            sc->done->status().set_error(EPERM, "Flush RocksDB Cloud failed");
            return nullptr;
        }

        // the storage of rocksdb cloud is not local, so we don't need to do
        // local checkpoint instead, we just flush the memtable to sst files and
        // update the manifest file in the cloud storage. in case of fail over,
        // the new instance will attach to the cloud storage
        rocksdb::FlushOptions flush_opt;
        // don't block writes during flush, so log writes can continue without
        // waiting for the flush to finish in this save snapshot thread
        flush_opt.allow_write_stall = false;
        // wait for flush to finish in this save snapshot thread
        flush_opt.wait = true;
        rocksdb::Status status = sc->dbc_flush->GetDBPtr()->Flush(flush_opt);
        if (!status.ok())
        {
            LOG(ERROR) << "Flush RocksDB Cloud Failed! Error: "
                       << status.ToString();
            sc->done->status().set_error(EPERM, "Flush RocksDB Cloud failed");
            return nullptr;
        }
        uint64_t max_sst_file_num =
            sc->dbc_flush->GetDBPtr()->GetNextFileNumber();

        auto stop = std::chrono::high_resolution_clock::now();
        uint64_t ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(stop - start)
                .count();
        LOG(INFO) << "RocksDB Cloud flush when raft snapshot cost " << ms
                  << " millseconds";

        LogRequest req;
        PostSnapshotRequest *post_snapshot_req =
            req.mutable_post_snapshot_request();
        post_snapshot_req->set_last_applied_tx_num(snapshot_last_tx_num);
        LogResponse resp;
        // serialize data into log entry.
        butil::IOBuf post_snapshot_log;
        butil::IOBufAsZeroCopyOutputStream log_wrapper(&post_snapshot_log);
        if (!req.SerializeToZeroCopyStream(&log_wrapper))
        {
            LOG(ERROR) << "Failed to serialize the PostSnapshotRequest request";
            sc->done->status().set_error(
                EPERM, "Failed to serialize the PostSnapshotRequest request");
            return nullptr;
        }
        else
        {
            braft::Task task;
            task.data = &post_snapshot_log;
            // if post snapshot succeed with the snapshot leader term, it means
            // this lg node is still the same leader as the time of snapshot
            // happen
            task.expected_term = snapshot_leader_term;
            start = std::chrono::high_resolution_clock::now();
            sc->log_instance_ref->ProposeTaskAndWait(task, &req, &resp);
            stop = std::chrono::high_resolution_clock::now();
            ms = std::chrono::duration_cast<std::chrono::milliseconds>(stop -
                                                                       start)
                     .count();

            if (resp.response_status() ==
                LogResponse::ResponseStatus::LogResponse_ResponseStatus_Success)
            {
                LOG(INFO) << "Write PostSnapshotRequest to log"
                          << sc->log_instance_ref->log_group_id_
                          << " done, cost " << ms << " millseconds";
            }
            else
            {
                LOG(ERROR) << "Failed to apply PostSnapshotRequest request";
                sc->done->status().set_error(
                    EPERM, "Failed to apply PostSnapshotRequest request");
                return nullptr;
            }

            // When post snapshot succeed, so this lg node is still the same,
            // we can update the max sst file number, because data has been
            // flush to main branch of the backend rocksdb cloud
            log_state_rocksdb_cloud_ref->SetMaxFileNumberAfterLatestFlush(
                max_sst_file_num);
            LOG(INFO) << "Update max_sst_file_num to " << max_sst_file_num
                      << " after latest snapshot flush";
        }
    }
#endif

    sc->log_state_ref->CleanSnapshotState();
    LOG(INFO) << "Save snapshot done";
    return nullptr;
}

void LogInstance::on_leader_stop(const butil::Status &status)
{
    LOG(INFO) << "Log node " << ip_ << ":" << port_
              << " stops being the leader of the log group #" << log_group_id_
              << ", status: " << status;
    {
        std::unique_lock lk(stop_replay_log_size_checker_mutex_);
        stop_replay_log_size_checker_thd_.store(true,
                                                std::memory_order_release);
        stop_replay_log_size_checker_cond_.notify_one();
    }
    if (replay_log_size_checker_thd_.joinable())
    {
        replay_log_size_checker_thd_.join();
    }
    std::unique_lock lk(log_replay_workers_mutex_);

    term_if_is_lg_leader_.store(-1, butil::memory_order_release);
    // clear log_replay_workers_, as new leader will reship on_leader_start
    log_replay_workers_.clear();

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    // clone the db cloud for refill in_mem_state after leader step down
    dbc_clone_after_leader_step_down_ = log_state_->GetDBCloudContainer();
    // stop the rocksdb cloud
    log_state_->StopRocksDB();
#endif
}

void LogInstance::on_start_following(const ::braft::LeaderChangeContext &ctx)
{
    LOG(INFO) << "Log node " << ip_ << ":" << port_
              << " becomes the follower of the group #" << log_group_id_
              << ", term: " << ctx.term();

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    // if this node is a leader before, refill in_mem_state from rocksdb
    // cloud
    LogStateRocksDBCloudImpl *log_state_rocksdb_cloud =
        static_cast<LogStateRocksDBCloudImpl *>(log_state_.get());

    if (dbc_clone_after_leader_step_down_ != nullptr)
    {
        LOG(INFO) << "Log node " << ip_ << ":" << port_
                  << " refill in_mem_state from rocksdb cloud";
        // wait for async start cloud db finish, so we make sure mem state
        // and db is in sync
        LOG(INFO) << "Wait for async start cloud db finish";
        log_state_rocksdb_cloud->WaitForAsyncStartCloudDBFinishIfAny();
        // have a local copy of the db cloud
        std::shared_ptr<DBCloudContainer> dbc =
            dbc_clone_after_leader_step_down_;
        dbc_clone_after_leader_step_down_ = nullptr;
        if (dbc->IsOpened())
        {
            // Flush the memtable to sst files and refill in_mem_state by
            // comparing the file number after this flush with the max file
            // number after latest snapshot, find all the sst files between
            // these two file numbers, read all key/values from the sst files
            // and refill in_mem_state
            LOG(INFO) << "Refill in_mem_state from rocksdb cloud";
            // flush memtable to sst files
            rocksdb::FlushOptions flush_opt;
            // block writes during flush
            flush_opt.allow_write_stall = false;
            flush_opt.wait = true;
            dbc->GetDBPtr()->Flush(flush_opt);
            // get the max sst file number
            uint64_t max_file_num = dbc->GetDBPtr()->GetNextFileNumber() - 1;
            uint64_t prev_max_file_num =
                log_state_rocksdb_cloud->GetMaxFileNumberAfterLatestFlush();
            LOG(INFO) << "max_sst_file_num: " << max_file_num
                      << ", prev_max_sst_file_num: " << prev_max_file_num;
            bool res = log_state_rocksdb_cloud->RefillInMemStateFromCloudDB(
                dbc, prev_max_file_num, max_file_num);
            if (!res)
            {
                LOG(ERROR) << "Failed to refill in_mem_state from rocksdb "
                              "cloud, panic "
                              "and try to recover from snapshot and raft log";
                std::abort();
            }
        }
        else
        {
            // if the db cloud is not opened after waiting for aync start db, it
            // means some thing wrong, but it also means the in_mem_state is
            // still in the log state
            LOG(WARNING) << "Previous RocksDB Cloud is not open, fail to "
                            "refill in_mem_state from rocksdb cloud";
        }
    }
#endif

    braft::PeerId leader_id = ctx.leader_id();
    braft::PeerId::Type leader_peer_type = leader_id.type_;
    std::string leader_endpoint;
    std::string local_endpoint(ip_ + ":" + std::to_string(port_));

    CODE_FAULT_INJECTOR("disable_prefer_leader_transfer", {
        LOG(INFO) << "FaultInject triggered, "
                     "disable_prefer_leader_transfer";
        return;
    });

#ifdef WITH_CLOUD_AZ_INFO
    if (IsPreferLeaderWithZoneInfo())
#else
    if (IsPreferLeader())
#endif
    {
#ifdef WITH_CLOUD_AZ_INFO
        if (leader_id.prefer_zone.length() > 0 &&
            leader_id.current_zone.length() > 0 &&
            leader_id.prefer_zone == leader_id.current_zone)
        {
            return;
        }
#endif
        if (!leader_transfer_thd_.joinable())
        {
            leader_transfer_thd_ = std::thread(
#ifdef WITH_CLOUD_AZ_INFO
                [leader_id,
                 leader_peer_type,
                 local_endpoint,
                 log_group_id = log_group_id_,
                 node_id = node_id_,
                 log_group_name = group_,
                 node = node_]
#else
                [leader_id,
                 leader_peer_type,
                 local_endpoint,
                 log_group_id = log_group_id_,
                 node = node_]
#endif
                {
                    // wait seconds before request transfer, immediate
                    // request will block new leader from changing
                    // configuration
                    std::this_thread::sleep_for(std::chrono::seconds(5));

                    LOG(INFO)
                        << "Request transfer leader from "
                        << leader_id.to_string() << " to the prefer leader "
                        << local_endpoint << " of log group " << log_group_id;

                    brpc::Channel leader_channel;
                    brpc::ChannelOptions options;
                    options.timeout_ms = 10000;
                    options.max_retry = 3;

                    if (leader_peer_type == braft::PeerId::Type::EndPoint)
                    {
                        leader_channel.Init(
                            butil::endpoint2str(leader_id.addr).c_str(),
                            &options);
                    }
                    else
                    {
                        // for case `leader_ip` is hostname format.
                        std::string naming_service_url;
                        braft::HostNameAddr2NSUrl(leader_id.hostname_addr,
                                                  naming_service_url);
                        leader_channel.Init(naming_service_url.c_str(),
                                            braft::LOAD_BALANCER_NAME,
                                            &options);
                    }

                    LogService_Stub leader_stub(&leader_channel);
                    TransferRequest req;
                    TransferResponse res;
                    brpc::Controller cntl;
                    braft::NodeStatus status;
                    cntl.set_timeout_ms(-1);
                    using namespace std::chrono_literals;
                    req.set_lg_id(log_group_id);
#ifdef WITH_CLOUD_AZ_INFO
                    bool skip_check_zone = false;
                    if (leader_id.current_zone.length() == 0 ||
                        leader_id.prefer_zone.length() == 0)
                    {
                        skip_check_zone = true;
                    }
                    req.set_leader_idx(node_id);
                    braft::PeerId current_leader;
                    do
                    {
                        leader_stub.TransferLeader(&cntl, &req, &res, nullptr);
                        if (cntl.Failed())
                        {
                            LOG(ERROR)
                                << "Failed the TransferLeader RPC of log group "
                                << log_group_id
                                << ". Error code: " << cntl.ErrorCode()
                                << ". Msg: " << cntl.ErrorText();
                        }
                        else if (res.error())
                        {
                            LOG(ERROR)
                                << "Failed to transfer the leader of log group "
                                << log_group_id;
                            if (!skip_check_zone)
                            {
                                // refresh leader
                                while (true)
                                {
                                    butil::Status st =
                                        braft::rtb::refresh_leader(
                                            log_group_name, 500);
                                    if (!st.ok())
                                    {
                                        std::this_thread::sleep_for(2s);
                                        continue;
                                    }
                                    if (braft::rtb::select_leader(
                                            log_group_name, &current_leader) !=
                                        0)
                                    {
                                        std::this_thread::sleep_for(1s);
                                        continue;
                                    }
                                    else
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                        else
                        {
                            break;
                        }
                        std::this_thread::sleep_for(5s);
                        cntl.Reset();
                        node->get_status(&status);
                    } while (status.state < braft::STATE_SHUTTING &&
                             !node->is_leader() &&
                             (current_leader.current_zone !=
                                  current_leader.prefer_zone ||
                              skip_check_zone));
#else
                    req.set_leader_idx(0);
                    braft::PeerId current_leader = leader_id;

                    do
                    {
                        leader_stub.TransferLeader(&cntl, &req, &res, nullptr);
                        if (cntl.Failed())
                        {
                            LOG(ERROR)
                                << "Failed the TransferLeader RPC of log "
                                   "group "
                                << log_group_id
                                << ". Error code: " << cntl.ErrorCode()
                                << ". Msg: " << cntl.ErrorText();
                        }
                        else if (res.error())
                        {
                            LOG(ERROR) << "Failed to transfer the leader of "
                                          "log group "
                                       << log_group_id;
                        }
                        else
                        {
                            break;
                        }
                        std::this_thread::sleep_for(5s);
                        cntl.Reset();
                        node->get_status(&status);
                    } while (status.state < braft::STATE_SHUTTING &&
                             !node->is_leader());
#endif
                });
        }
    }
    else
    {
        LOG(INFO) << "This node is not the prefer leader of log group "
                  << log_group_id_;
#ifndef NDEBUG
        /*
         *Adjusting the election_timeout_ms on the non-preferred leader node
         *to match the preferred leader node ensures that the non-preferred
         *leader can quickly catch up with the leader election process. This
         *prevents prolonged absence of the new leader from the log node
         *group, improving system reliability.
         */
        node_->reset_election_timeout_ms(1000, 1000);
#endif
    }
}
void LogInstance::ProposeTaskAndWait(braft::Task &task,
                                     const LogRequest *request,
                                     LogResponse *response)
{
    bool finish = false;
    bthread::Mutex mu;
    bthread::ConditionVariable cv;
    RaftLogClosure closure(request, response, &finish, &mu, &cv);

    task.done = &closure;
    node_->apply(task);

    std::unique_lock lk(mu);
    while (!finish)
    {
        cv.wait(lk);
    }
}

void LogInstance::CheckReplayLogSizeAndNotifyCkptIfNeeded()
{
    while (!stop_replay_log_size_checker_thd_.load(std::memory_order_acquire))
    {
        if (log_state_->GetApproximateReplayLogSize() >=
            notify_checkpointer_threshold_size_)
        {
            int err;
            brpc::ChannelOptions options;
            options.protocol = brpc::PROTOCOL_BAIDU_STD;
            options.max_retry = 3;
            for (const auto &pair : log_state_->CopyCcNgInfo())
            {
                auto leader_ip = pair.second.leader_ip_;
                auto target_port = std::to_string(pair.second.leader_port_);
                auto target_ip_and_port = leader_ip + ":" + target_port;
                err = channel_.Init(target_ip_and_port.c_str(), &options);
                if (err != 0)
                {
                    LOG(ERROR)
                        << "Failed to initialize channel to "
                        << target_ip_and_port << " with error code " << err;
                    continue;  // Skip to next iteration
                }

                LogReplayService_Stub stub(&channel_);
                brpc::Controller cntl;
                NotifyCheckpointerRequest req;
                NotifyCheckpointerResponse resp;
                stub.NotifyCheckpointer(&cntl, &req, &resp, nullptr);
                if (cntl.Failed())
                {
                    LOG(ERROR) << "Failed to notify checkpointer: "
                               << cntl.ErrorText();
                }
                else
                {
                    LOG(INFO) << "Sent a checkpoint request.";
                }
            }
        }
        std::unique_lock lk(stop_replay_log_size_checker_mutex_);
        stop_replay_log_size_checker_cond_.wait_for(
            lk,
            std::chrono::seconds(check_replay_log_size_interval_sec_),
            [this] {
                return stop_replay_log_size_checker_thd_.load(
                    std::memory_order_relaxed);
            });
    }
}

// Define a closure class
class ChangePeersClosure : public braft::Closure
{
public:
    ChangePeersClosure(const braft::Configuration &new_peers,
                       ChangePeersResponse *response,
                       google::protobuf::Closure *done)
        : new_peers_(new_peers), response_(response), done_(done)
    {
    }

    void Run() override
    {
        if (status().ok())
        {
            LOG(INFO) << "Change peers succeeded.";
        }
        else
        {
            LOG(ERROR) << "Change peers failed: " << status().error_str();
        }
        response_->set_success(status().ok());
        done_->Run();
        delete this;
    }

private:
    braft::Configuration new_peers_;
    ChangePeersResponse *response_;
    google::protobuf::Closure *done_;
};

void LogInstance::ChangePeersToAdd(const std::vector<Peer> &new_peers,
                                   uint32_t log_group_id,
                                   ChangePeersResponse *response,
                                   google::protobuf::Closure *done)
{
    braft::NodeStatus node_status;
    node_->get_status(&node_status);
    if (node_status.state != braft::State::STATE_LEADER)
    {
        brpc::ClosureGuard done_guard(done);
        if (node_status.state > braft::State::STATE_FOLLOWER)
        {
            // Node is in invalid state, return error
            LOG(ERROR) << "Node is in invalid state: "
                       << braft::state2str(node_status.state);
            response->set_success(false);
            return;
        }
        // node is not leader, redirect to leader
        braft::PeerId leader_peer = node_->leader_id();
        std::string leader_ip_port;
        if (leader_peer.type_ == braft::PeerId::Type::HostName)
        {
            leader_ip_port.append(leader_peer.hostname_addr.hostname);
            leader_ip_port.append(":");
            leader_ip_port.append(
                std::to_string(leader_peer.hostname_addr.port));
        }
        else
        {
            leader_ip_port.append(
                butil::endpoint2str(leader_peer.addr).c_str());
        }
        brpc::Channel leader_channel;
        brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_BAIDU_STD;
        options.max_retry = 3;
        leader_channel.Init(leader_ip_port.c_str(), &options);
        LogService_Stub leader_stub(&leader_channel);
        AddPeerRequest req;
        req.set_log_group_id(log_group_id);
        for (const auto &peer : new_peers)
        {
            req.add_ip(peer.ip);
            req.add_port(peer.port);
        }
        brpc::Controller cntl;
        leader_stub.AddPeer(&cntl, &req, response, nullptr);
        if (cntl.Failed())
        {
            LOG(ERROR) << "Failed to add peer: " << cntl.ErrorText();
        }
        else
        {
            LOG(INFO) << "Added peer successfully.";
        }
        return;
    }
    std::vector<braft::PeerId> peers;
    butil::Status status = node_->list_peers(&peers);
    if (!status.ok())
    {
        LOG(ERROR) << "Failed to list peers: " << status.error_cstr();
        response->set_success(false);
        done->Run();
        return;
    }

    for (const auto &peer : new_peers)
    {
        std::string peer_str = peer.ip + ":" + std::to_string(peer.port) + ":0";
        braft::PeerId peer_id(peer_str);
#ifdef WITH_CLOUD_AZ_INFO
        peer_id.prefer_zone = prefer_zone_;
        peer_id.current_zone = current_zone_;
#endif
        if (std::find(peers.begin(), peers.end(), peer_id) == peers.end())
        {
            peers.push_back(peer_id);
        }
    }

    braft::Configuration conf;
    for (const auto &peer : peers)
    {
        conf.add_peer(peer);
    }
    auto *closure = new ChangePeersClosure(conf, response, done);
    node_->change_peers(conf, closure);
}

void LogInstance::ChangePeersToRemove(const std::vector<Peer> &remove_peers,
                                      uint32_t log_group_id,
                                      ChangePeersResponse *response,
                                      google::protobuf::Closure *done)
{
    braft::NodeStatus node_status;
    node_->get_status(&node_status);
    if (node_status.state != braft::State::STATE_LEADER)
    {
        brpc::ClosureGuard done_guard(done);
        if (node_status.state > braft::State::STATE_FOLLOWER)
        {
            // Node is in invalid state, return error
            LOG(ERROR) << "Node is in invalid state: "
                       << braft::state2str(node_status.state);
            response->set_success(false);
            return;
        }
        // node is not leader, redirect to leader
        braft::PeerId leader_peer = node_->leader_id();
        std::string leader_ip_port;
        if (leader_peer.type_ == braft::PeerId::Type::HostName)
        {
            leader_ip_port.append(leader_peer.hostname_addr.hostname);
            leader_ip_port.append(":");
            leader_ip_port.append(
                std::to_string(leader_peer.hostname_addr.port));
        }
        else
        {
            leader_ip_port.append(
                butil::endpoint2str(leader_peer.addr).c_str());
        }
        brpc::Channel leader_channel;
        brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_BAIDU_STD;
        options.max_retry = 3;
        leader_channel.Init(leader_ip_port.c_str(), &options);
        LogService_Stub leader_stub(&leader_channel);
        RemovePeerRequest req;
        req.set_log_group_id(log_group_id);
        for (const auto &peer : remove_peers)
        {
            req.add_ip(peer.ip);
            req.add_port(peer.port);
        }
        brpc::Controller cntl;
        leader_stub.RemovePeer(&cntl, &req, response, nullptr);
        if (cntl.Failed())
        {
            LOG(ERROR) << "Failed to remove peer: " << cntl.ErrorText();
        }
        else
        {
            LOG(INFO) << "Removed peer successfully.";
        }

        return;
    }
    std::vector<braft::PeerId> peers;
    butil::Status status = node_->list_peers(&peers);
    if (!status.ok())
    {
        LOG(ERROR) << "Failed to list peers: " << status.error_cstr();
        response->set_success(false);
        done->Run();
        return;
    }

    for (const auto &peer : remove_peers)
    {
        std::string peer_str = peer.ip + ":" + std::to_string(peer.port) + ":0";
        braft::PeerId peer_id(peer_str);
#ifdef WITH_CLOUD_AZ_INFO
        peer_id.prefer_zone = prefer_zone_;
        peer_id.current_zone = current_zone_;
#endif
        if (std::find(peers.begin(), peers.end(), peer_id) != peers.end())
        {
            peers.erase(std::remove(peers.begin(), peers.end(), peer_id),
                        peers.end());
        }
    }
    braft::Configuration conf;
    for (const auto &peer : peers)
    {
        conf.add_peer(peer);
    }

    // TODO(liunyl): invoke snapshot before change peer?

    auto *closure = new ChangePeersClosure(conf, response, done);
    node_->change_peers(conf, closure);
}

void LogInstance::UpdateInMemoryConfig(const braft::Configuration &new_peers)
{
    std::lock_guard<bthread::Mutex> lock(config_mutex_);  // Lock the mutex

    // Update peer_vct_ with new_peers
    peer_vct_.clear();
    for (const auto &peer : new_peers)
    {
        Peer new_peer;
        if (peer.type_ == braft::PeerId::Type::EndPoint)
        {
            new_peer.ip = butil::ip2str(peer.addr.ip).c_str();
            new_peer.port = peer.addr.port;
        }
        else
        {
            new_peer.ip = peer.hostname_addr.hostname;
            new_peer.port = peer.hostname_addr.port;
        }
        peer_vct_.push_back(new_peer);
    }

    // Update raft_conf_ with new_peers
    std::string new_raft_conf;
    for (const auto &peer : new_peers)
    {
        new_raft_conf += peer.to_string() + ",";
    }
    if (!new_raft_conf.empty())
    {
        new_raft_conf.pop_back();  // Remove the trailing comma
    }
    raft_conf_ = new_raft_conf;
}

}  // namespace txlog
