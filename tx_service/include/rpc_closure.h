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

#include <brpc/controller.h>
#include <brpc/errno.pb.h>
#include <bthread/condition_variable.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <utility>
#include <vector>

#include "cc/cc_handler_result.h"
#include "cc_req_misc.h"
#include "error_messages.h"
#include "local_cc_shards.h"
#include "proto/cc_request.pb.h"
#include "remote/remote_type.h"
#include "sharder.h"
#include "tx_operation_result.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
class FlushDataAllClosure : public ::google::protobuf::Closure
{
public:
    explicit FlushDataAllClosure(CcHandlerResult<Void> *hd_res)
        : hd_result_(hd_res)
    {
    }
    ~FlushDataAllClosure() = default;

    FlushDataAllClosure(const FlushDataAllClosure &rhs) = delete;
    FlushDataAllClosure(FlushDataAllClosure &&rhs) = delete;

    // Run() will be called when rpc request is processed by cc node service.
    void Run() override
    {
        // Free closure on exit
        std::unique_ptr<FlushDataAllClosure> self_guard(this);

        if (cntl_.Failed())
        {
            // RPC failed.
            LOG(ERROR)
                << "Failed to process the FlushDataAll RPC request of ng#"
                << request_.node_group_id()
                << " for table: " << request_.table_name_str()
                << " with Error code: " << cntl_.ErrorCode()
                << ". Error Msg: " << cntl_.ErrorText();
            Sharder::Instance().UpdateCcNodeServiceChannel(node_id_, channel_);
            channel_ = nullptr;
            hd_result_->SetError(CcErrorCode::REQUEST_LOST);
            return;
        }
        channel_ = nullptr;

        if (response_.error_code())
        {
            CcErrorCode error_code =
                static_cast<CcErrorCode>(response_.error_code());
            LOG(ERROR) << "Handle flush data all response of ng#"
                       << request_.node_group_id()
                       << " for table: " << request_.table_name_str()
                       << ". Failed with error: " << CcErrorMessage(error_code);
            hd_result_->SetError(error_code);
        }
        else
        {
            DLOG(INFO) << "Handle flush data all response successfully of ng#"
                       << request_.node_group_id()
                       << " for table: " << request_.table_name_str();
            hd_result_->SetFinished();
        }
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    remote::FlushDataAllRequest *FlushDataAllRequest()
    {
        return &request_;
    }

    remote::FlushDataAllResponse *FlushDataAllResponse()
    {
        return &response_;
    }

    void SetChannel(uint32_t node_id, std::shared_ptr<brpc::Channel> channel)
    {
        node_id_ = node_id;
        channel_ = channel;
    }

private:
    brpc::Controller cntl_;
    remote::FlushDataAllRequest request_;
    remote::FlushDataAllResponse response_;
    CcHandlerResult<Void> *hd_result_{nullptr};
    std::shared_ptr<brpc::Channel> channel_;
    uint32_t node_id_;
};

class UploadBatchClosure : public ::google::protobuf::Closure
{
public:
    UploadBatchClosure(std::function<void(CcErrorCode, int32_t)> post_lambda,
                       uint16_t upload_timeout,
                       bool retry_on_timeout)
        : post_lambda_(post_lambda),
          upload_timeout_(upload_timeout),
          retry_on_timeout_(retry_on_timeout),
          eagain_wait_ms_(100)
    {
    }
    ~UploadBatchClosure() = default;

    UploadBatchClosure(const UploadBatchClosure &rhs) = delete;
    UploadBatchClosure(UploadBatchClosure &&rhs) = delete;

    // Run() will be called when rpc request is processed by cc node service.
    void Run() override
    {
        // Free closure on exit
        std::unique_ptr<UploadBatchClosure> self_guard(this);
        if (cntl_.Failed())
        {
            // RPC failed.
            LOG(ERROR) << "Failed for UploadBatch RPC request of ng#"
                       << request_.node_group_id()
                       << ", with Error code: " << cntl_.ErrorCode()
                       << ". Error Msg: " << cntl_.ErrorText();
            if (cntl_.ErrorCode() == brpc::EOVERCROWDED ||
                cntl_.ErrorCode() == EAGAIN)
            {
                if (eagain_wait_ms_ > 2000)
                {
                    LOG(WARNING)
                        << "upload_batch_closure: retried too many times "
                           "after eagain.";
                    return;
                }
                bthread_usleep(1000 * eagain_wait_ms_);
                eagain_wait_ms_ *= 2;

                self_guard.release();
                // Retry if timeout.
                LOG(INFO)
                    << "Retry after EOVERCROWDED UploadBatch service of ng#"
                    << request_.node_group_id();
                cntl_.Reset();
                response_.Clear();
                remote::CcRpcService_Stub stub(channel_.get());
                cntl_.set_timeout_ms(upload_timeout_);
                stub.UploadBatch(&cntl_, &request_, &response_, this);
                return;
            }
            if (cntl_.ErrorCode() == brpc::ERPCTIMEDOUT && retry_on_timeout_)
            {
                self_guard.release();
                // Retry if timeout.
                DLOG(INFO) << "Retry UploadBatch service of ng#"
                           << request_.node_group_id();
                cntl_.Reset();
                response_.Clear();
                remote::CcRpcService_Stub stub(channel_.get());
                cntl_.set_timeout_ms(upload_timeout_);
                stub.UploadBatch(&cntl_, &request_, &response_, this);
                return;
            }
            Sharder::Instance().UpdateCcNodeServiceChannel(node_id_, channel_);
            if (post_lambda_)
            {
                post_lambda_(CcErrorCode::REQUEST_LOST, 0);
            }
        }
        else
        {
            CcErrorCode err_code =
                remote::ToLocalType::ConvertCcErrorCode(response_.error_code());
            if (post_lambda_)
            {
                post_lambda_(err_code, response_.ng_term());
            }
        }
        channel_ = nullptr;
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    remote::UploadBatchResponse *UploadBatchResponse()
    {
        return &response_;
    }

    remote::UploadBatchRequest *UploadBatchRequest()
    {
        return &request_;
    }

    void SetChannel(uint32_t node_id, std::shared_ptr<brpc::Channel> channel)
    {
        node_id_ = node_id;
        channel_ = channel;
    }

    brpc::Channel *Channel()
    {
        return channel_.get();
    }

    void SetPostLambda(std::function<void(CcErrorCode, int32_t)> post_lambda)
    {
        post_lambda_ = post_lambda;
    }

    uint16_t TimeoutValue() const
    {
        return upload_timeout_;
    }

    uint32_t NodeId() const
    {
        return node_id_;
    }

private:
    brpc::Controller cntl_;
    remote::UploadBatchRequest request_;
    remote::UploadBatchResponse response_;
    std::function<void(CcErrorCode, int32_t)> post_lambda_;
    uint16_t upload_timeout_{0};
    std::shared_ptr<brpc::Channel> channel_;
    uint32_t node_id_;
    bool retry_on_timeout_{false};

    uint16_t eagain_wait_ms_{100};
};

class GenerateSkFromPkClosure : public ::google::protobuf::Closure
{
public:
    GenerateSkFromPkClosure(
        std::mutex &mux,
        std::condition_variable &cv,
        std::vector<int64_t> &leader_terms,
        const std::vector<TableName> &new_indexes_name,
        std::vector<const KeySchema *> new_indexes_schema,
        uint32_t &unfinished_task_cnt,
        bool &all_task_started,
        uint32_t &total_pk_items_count,
        uint32_t &dispatched_task_count,
        CcErrorCode &task_res,
        GenerateSkParallelResult &task_value,
        std::function<void(TxKey batch_range_start_key,
                           TxKey batch_range_end_key,
                           const std::string *batch_range_start_key_str,
                           const std::string *batch_range_end_key_str,
                           TxKey &last_scanned_end_key,
                           bool &is_last_scanned_key_str,
                           size_t batch_range_cnt,
                           uint32_t &actual_task_cnt)> &dispatch_func)
        : mux_(mux),
          cv_(cv),
          leader_terms_(leader_terms),
          new_indexes_name_(new_indexes_name),
          new_indexes_schema_(std::move(new_indexes_schema)),
          unfinished_task_cnt_(unfinished_task_cnt),
          all_task_started_(all_task_started),
          total_pk_items_count_(total_pk_items_count),
          dispatched_task_count_(dispatched_task_count),
          task_res_(task_res),
          task_value_(task_value),
          dispatch_func_(dispatch_func)
    {
    }
    ~GenerateSkFromPkClosure() = default;

    GenerateSkFromPkClosure(const GenerateSkFromPkClosure &rhs) = delete;
    GenerateSkFromPkClosure(GenerateSkFromPkClosure &&rhs) = delete;

    // Run() will be called when rpc request is processed by cc node service.
    void Run() override
    {
        // Free closure on exit
        std::unique_ptr<GenerateSkFromPkClosure> self_guard(this);

        if (cntl_.Failed())
        {
            // RPC failed.
            LOG(ERROR) << "Failed for GenerateSkFromPk RPC request of ng#"
                       << request_.node_group_id()
                       << " for partition id: " << request_.partition_id()
                       << ". Error code: " << cntl_.ErrorCode()
                       << ". Error Msg: " << cntl_.ErrorText();
            Sharder::Instance().UpdateCcNodeServiceChannel(node_id_, channel_);
            channel_ = nullptr;
            std::unique_lock<std::mutex> lk(mux_);
            --unfinished_task_cnt_;
            task_res_ = task_res_ == CcErrorCode::NO_ERROR
                            ? CcErrorCode::REQUEST_LOST
                            : task_res_;
            cv_.notify_one();
            return;
        }
        channel_ = nullptr;

        CcErrorCode res_code =
            remote::ToLocalType::ConvertCcErrorCode(response_.error_code());
        if (res_code == CcErrorCode::GET_RANGE_ID_ERR)
        {
            LOG(WARNING) << "Terminate this generate sk task of ng#"
                         << request_.node_group_id()
                         << " for partition id: " << request_.partition_id()
                         << " for table: " << request_.table_name_str()
                         << " caused by the boundary of partition mismatch.";

            bthread::Mutex bthd_mux;
            bthread::ConditionVariable bthd_cv;
            bool is_finished = false;
            std::thread worker_thd = std::thread(
                [this, &bthd_mux, &bthd_cv, &is_finished]()
                {
                    // Update the task status
                    {
                        std::lock_guard<std::mutex> task_lk(mux_);
                        all_task_started_ = false;
                        --unfinished_task_cnt_;
                    }

                    TxKey range_start_key{};
                    TxKey range_end_key{};
                    const std::string *range_start_key_str =
                        &(request_.start_key());
                    const std::string *range_end_key_str =
                        &(request_.end_key());
                    bool is_last_scanned_key_str = true;
                    TableEngine table_engine =
                        remote::ToLocalType::ConvertTableEngine(
                            request_.table_engine());

                    // Re-dispatch this range task.
                    if (request_.start_key().size() == 0)
                    {
                        range_start_key = Sharder::Instance()
                                              .GetLocalCcShards()
                                              ->GetCatalogFactory(table_engine)
                                              ->NegativeInfKey();
                        is_last_scanned_key_str = false;
                        range_start_key_str = nullptr;
                    }
                    if (request_.end_key().size() == 0)
                    {
                        range_end_key = Sharder::Instance()
                                            .GetLocalCcShards()
                                            ->GetCatalogFactory(table_engine)
                                            ->PositiveInfKey();
                        range_end_key_str = nullptr;
                    }
                    TxKey last_scanned_end_key =
                        range_start_key.GetShallowCopy();
                    bool dispatch_next_range = true;
                    uint32_t actual_task_cnt = 0;
                    do
                    {
                        dispatch_func_(range_start_key.GetShallowCopy(),
                                       range_end_key.GetShallowCopy(),
                                       range_start_key_str,
                                       range_end_key_str,
                                       last_scanned_end_key,
                                       is_last_scanned_key_str,
                                       0,
                                       actual_task_cnt);
                        {
                            std::lock_guard<std::mutex> task_lk(mux_);
                            if (task_res_ == CcErrorCode::TX_NODE_NOT_LEADER)
                            {
                                all_task_started_ = true;
                                cv_.notify_one();
                                return;
                            }
                        }
                        // dispatch next range.
                        if (request_.end_key().size() == 0)
                        {
                            dispatch_next_range =
                                last_scanned_end_key < range_end_key;
                        }
                        else
                        {
                            assert(last_scanned_end_key.Type() ==
                                   KeyType::Normal);
                            std::string serialized_key;
                            last_scanned_end_key.Serialize(serialized_key);
                            dispatch_next_range =
                                serialized_key.length() !=
                                    request_.end_key().length() ||
                                serialized_key.compare(request_.end_key());
                        }

                    } while (dispatch_next_range);

                    // Update the task status
                    {
                        std::lock_guard<std::mutex> task_lk(mux_);
                        dispatched_task_count_ += (actual_task_cnt - 1);
                        all_task_started_ = true;
                        cv_.notify_one();
                    }

                    std::unique_lock<bthread::Mutex> lk(bthd_mux);
                    is_finished = true;
                    bthd_cv.notify_all();
                });

            std::unique_lock<bthread::Mutex> lk(bthd_mux);
            while (!is_finished)
            {
                bthd_cv.wait(lk);
            }

            worker_thd.join();
            return;
        }

        if (res_code != CcErrorCode::NO_ERROR)
        {
            LOG(ERROR) << "Response for GenerateSkFromPk failed of ng#"
                       << request_.node_group_id()
                       << " for partition id: " << request_.partition_id()
                       << " with error: " << CcErrorMessage(res_code);
            std::unique_lock<std::mutex> lk(mux_);
            --unfinished_task_cnt_;
            if (task_res_ == CcErrorCode::NO_ERROR)
            {
                task_res_ = res_code;
                if (res_code == CcErrorCode::PACK_SK_ERR)
                {
                    task_value_.pack_sk_error_.code_ =
                        response_.pack_err_code();
                    task_value_.pack_sk_error_.message_ =
                        response_.pack_err_msg();
                }
            }
            cv_.notify_one();
            return;
        }

        DLOG(INFO) << "Response for GenerateSkFromPk succeed of ng#"
                   << request_.node_group_id()
                   << " for partition id: " << request_.partition_id();
        // Check the node group terms.
        int64_t term = -1;
        size_t curr_ng_terms_cnt = response_.ng_terms_size();

        std::unique_lock<std::mutex> lk(mux_);
        size_t ng_terms_cnt = leader_terms_.size();
        if (curr_ng_terms_cnt > ng_terms_cnt)
        {
            leader_terms_.resize(curr_ng_terms_cnt, INIT_TERM);
        }
        for (int idx = 0; idx < response_.ng_terms_size(); ++idx)
        {
            term = response_.ng_terms(idx);
            if (term < 0)
            {
                continue;
            }

            auto &leader_term = leader_terms_.at(idx);
            if (leader_term < 0)
            {
                leader_term = term;
            }
            else if (leader_term != term)
            {
                LOG(ERROR) << "Response for GenerateSkFromPk succeed of ng#"
                           << request_.node_group_id()
                           << ", but leader transferred of ng#" << idx;
                --unfinished_task_cnt_;
                task_res_ = task_res_ == CcErrorCode::NO_ERROR
                                ? CcErrorCode::REQUESTED_NODE_NOT_LEADER
                                : task_res_;
                cv_.notify_one();
                return;
            }
            else
            {
                assert(term == leader_term);
            }
        }

        // Finished
        --unfinished_task_cnt_;
        total_pk_items_count_ += response_.pk_items_count();

        const std::vector<bool> &new_indexes_multikey =
            StoreIndexesMultiKey(response_.indexes_multikey_attr());
        std::vector<MultiKeyPaths::Uptr> &new_indexes_multikey_paths =
            StoreIndexesMultiKeyPaths(response_.indexes_multikey_attr());
        task_value_.IndexesMergeMultiKeyAttr(new_indexes_name_,
                                             new_indexes_multikey,
                                             new_indexes_multikey_paths);

        cv_.notify_one();
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    remote::GenerateSkFromPkRequest *GenerateSkFromPkRequest()
    {
        return &request_;
    }

    remote::GenerateSkFromPkResponse *GenerateSkFromPkResponse()
    {
        return &response_;
    }

    void SetChannel(uint32_t node_id, std::shared_ptr<brpc::Channel> channel)
    {
        node_id_ = node_id;
        channel_ = channel;
    }

    const std::vector<bool> &StoreIndexesMultiKey(
        const google::protobuf::RepeatedPtrField<remote::MultiKeyAttr>
            &indexes_multikey_attr)
    {
        assert(indexes_multikey_attr.size() ==
               static_cast<int>(new_indexes_name_.size()));
        new_indexes_multikey_.clear();
        new_indexes_multikey_.reserve(new_indexes_name_.size());
        for (uint16_t idx = 0; idx < indexes_multikey_attr.size(); ++idx)
        {
            const remote::MultiKeyAttr &attr = indexes_multikey_attr.Get(idx);
            new_indexes_multikey_.push_back(attr.multikey());
        }
        return new_indexes_multikey_;
    }

    std::vector<MultiKeyPaths::Uptr> &StoreIndexesMultiKeyPaths(
        const google::protobuf::RepeatedPtrField<remote::MultiKeyAttr>
            &indexes_multikey_attr)
    {
        assert(indexes_multikey_attr.size() ==
               static_cast<int>(new_indexes_name_.size()));
        new_indexes_multikey_paths_.clear();
        new_indexes_multikey_paths_.reserve(new_indexes_name_.size());
        for (uint16_t idx = 0; idx < new_indexes_name_.size(); ++idx)
        {
            const remote::MultiKeyAttr &attr = indexes_multikey_attr.Get(idx);
            const KeySchema *key_schema = new_indexes_schema_[idx];
            if (key_schema->MultiKeyPaths())
            {
                MultiKeyPaths::Uptr multikey_paths =
                    key_schema->MultiKeyPaths()->Clone();
                multikey_paths->Deserialize(key_schema, attr.multikey_paths());
                new_indexes_multikey_paths_.push_back(
                    std::move(multikey_paths));
            }
            else
            {
                // The index type doesn't support multikey index.
                new_indexes_multikey_paths_.push_back(nullptr);
            }
        }
        return new_indexes_multikey_paths_;
    }

private:
    brpc::Controller cntl_;
    remote::GenerateSkFromPkRequest request_;
    remote::GenerateSkFromPkResponse response_;
    std::shared_ptr<brpc::Channel> channel_;
    uint32_t node_id_;

    std::mutex &mux_;
    std::condition_variable &cv_;
    std::vector<int64_t> &leader_terms_;
    const std::vector<TableName> &new_indexes_name_;
    std::vector<const KeySchema *> new_indexes_schema_;
    std::vector<bool> new_indexes_multikey_;
    std::vector<MultiKeyPaths::Uptr> new_indexes_multikey_paths_;
    uint32_t &unfinished_task_cnt_;
    bool &all_task_started_;
    uint32_t &total_pk_items_count_;
    uint32_t &dispatched_task_count_;
    CcErrorCode &task_res_;
    GenerateSkParallelResult &task_value_;
    std::function<void(TxKey batch_range_start_key,
                       TxKey batch_range_end_key,
                       const std::string *batch_range_start_key_str,
                       const std::string *batch_range_end_key_str,
                       TxKey &last_scanned_end_key,
                       bool &is_last_scanned_key_str,
                       size_t batch_range_cnt,
                       uint32_t &actual_task_cnt)> &dispatch_func_;
};

class UploadBatchSlicesClosure : public ::google::protobuf::Closure
{
public:
    UploadBatchSlicesClosure(
        std::function<void(CcErrorCode, int32_t)> post_lambda,
        uint16_t upload_timeout,
        bool retry_on_timeout)
        : post_lambda_(post_lambda),
          upload_timeout_(upload_timeout),
          retry_on_timeout_(retry_on_timeout),
          eagain_wait_ms_(100)
    {
    }
    ~UploadBatchSlicesClosure() = default;

    UploadBatchSlicesClosure(const UploadBatchSlicesClosure &rhs) = delete;
    UploadBatchSlicesClosure(UploadBatchSlicesClosure &&rhs) = delete;

    // Run() will be called when rpc request is processed by cc node service.
    void Run() override
    {
        // Free closure on exit
        std::unique_ptr<UploadBatchSlicesClosure> self_guard(this);
        if (cntl_.Failed())
        {
            // RPC failed.
            LOG(ERROR) << "Failed for UploadBatchSlices RPC request of ng#"
                       << request_.node_group_id()
                       << ", with Error code: " << cntl_.ErrorCode()
                       << ". Error Msg: " << cntl_.ErrorText();
            if (cntl_.ErrorCode() == brpc::EOVERCROWDED ||
                cntl_.ErrorCode() == EAGAIN)
            {
                if (eagain_wait_ms_ > 2000)
                {
                    LOG(WARNING)
                        << "upload_batch_closure: retried too many times "
                           "after eagain.";
                    return;
                }
                bthread_usleep(1000 * eagain_wait_ms_);
                eagain_wait_ms_ *= 2;

                self_guard.release();
                // Retry if timeout.
                LOG(INFO) << "Retry after EOVERCROWDED UploadBatchSlices "
                             "service of ng#"
                          << request_.node_group_id();
                cntl_.Reset();
                response_.Clear();
                remote::CcRpcService_Stub stub(channel_.get());
                cntl_.set_timeout_ms(upload_timeout_);
                stub.UploadBatchSlices(&cntl_, &request_, &response_, this);
                return;
            }
            if (cntl_.ErrorCode() == brpc::ERPCTIMEDOUT && retry_on_timeout_)
            {
                self_guard.release();
                // Retry if timeout.
                DLOG(INFO) << "Retry UploadBatchSlices service of ng#"
                           << request_.node_group_id();
                cntl_.Reset();
                response_.Clear();
                remote::CcRpcService_Stub stub(channel_.get());
                cntl_.set_timeout_ms(upload_timeout_);
                stub.UploadBatchSlices(&cntl_, &request_, &response_, this);
                return;
            }
            Sharder::Instance().UpdateCcNodeServiceChannel(node_id_, channel_);
            if (post_lambda_)
            {
                post_lambda_(CcErrorCode::REQUEST_LOST, 0);
            }
        }
        else
        {
            CcErrorCode err_code =
                remote::ToLocalType::ConvertCcErrorCode(response_.error_code());
            if (post_lambda_)
            {
                post_lambda_(err_code, response_.ng_term());
            }
        }
        channel_ = nullptr;
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    remote::UploadBatchResponse *UploadBatchResponse()
    {
        return &response_;
    }

    remote::UploadBatchSlicesRequest *UploadBatchRequest()
    {
        return &request_;
    }

    void SetChannel(uint32_t node_id, std::shared_ptr<brpc::Channel> channel)
    {
        node_id_ = node_id;
        channel_ = channel;
    }

    brpc::Channel *Channel()
    {
        return channel_.get();
    }

    void SetPostLambda(std::function<void(CcErrorCode, int32_t)> post_lambda)
    {
        post_lambda_ = post_lambda;
    }

    uint16_t TimeoutValue() const
    {
        return upload_timeout_;
    }

    uint32_t NodeId() const
    {
        return node_id_;
    }

private:
    brpc::Controller cntl_;
    remote::UploadBatchSlicesRequest request_;
    remote::UploadBatchResponse response_;
    std::function<void(CcErrorCode, int32_t)> post_lambda_;
    uint16_t upload_timeout_{0};
    std::shared_ptr<brpc::Channel> channel_;
    uint32_t node_id_;
    bool retry_on_timeout_{false};

    uint16_t eagain_wait_ms_{100};
};
class FetchRecordClosure : public ::google::protobuf::Closure
{
public:
    FetchRecordClosure(FetchRecordCc *fetch_cc) : fetch_cc_(fetch_cc)
    {
    }

    FetchRecordClosure(const FetchRecordClosure &rhs) = delete;
    FetchRecordClosure(FetchRecordClosure &&rhs) = delete;

    // Run() will be called when rpc request is processed by cc node service.
    void Run() override
    {
        // Free closure on exit
        std::unique_ptr<FetchRecordClosure> self_guard(this);
        if (!fetch_cc_->ValidTermCheck())
        {
            channel_ = nullptr;
            return;
        }
        CcErrorCode err_code = CcErrorCode::NO_ERROR;
        if (cntl_.Failed())
        {
            // RPC failed.
            LOG(ERROR) << "Failed for Fetch Payload RPC request of ng#"
                       << request_.node_group_id()
                       << ", with Error code: " << cntl_.ErrorCode()
                       << ". Error Msg: " << cntl_.ErrorText();
            if (cntl_.ErrorCode() == brpc::EOVERCROWDED ||
                cntl_.ErrorCode() == EAGAIN)
            {
                bthread_usleep(10000);

                self_guard.release();
                // Retry if timeout.
                DLOG(INFO) << "Retry after EOVERCROWDED fetch record "
                              "service of ng#"
                           << request_.node_group_id();
                cntl_.Reset();
                response_.Clear();
                remote::CcRpcService_Stub stub(channel_.get());
                cntl_.set_timeout_ms(5000);
                cntl_.set_write_to_socket_in_background(true);
                stub.FetchPayload(&cntl_, &request_, &response_, this);
                return;
            }
            if (cntl_.ErrorCode() == brpc::ERPCTIMEDOUT)
            {
                self_guard.release();
                // Retry if timeout.
                DLOG(INFO) << "Fetch payload request timed out. Retry fetch "
                              "payload service of ng#"
                           << request_.node_group_id();
                cntl_.Reset();
                response_.Clear();
                remote::CcRpcService_Stub stub(channel_.get());
                cntl_.set_timeout_ms(5000);
                cntl_.set_write_to_socket_in_background(true);
                stub.FetchPayload(&cntl_, &request_, &response_, this);
                return;
            }
            Sharder::Instance().UpdateCcNodeServiceChannel(node_id_, channel_);
            err_code = CcErrorCode::RPC_CALL_ERR;
        }
        else
        {
            err_code =
                remote::ToLocalType::ConvertCcErrorCode(response_.error_code());
            if (err_code == CcErrorCode::NO_ERROR)
            {
                fetch_cc_->rec_status_ = response_.is_deleted()
                                             ? RecordStatus::Deleted
                                             : RecordStatus::Normal;
                fetch_cc_->rec_ts_ = response_.version();
                if (fetch_cc_->rec_status_ == RecordStatus::Normal)
                {
                    fetch_cc_->rec_str_ = response_.payload();
                }
            }
            else if (err_code != CcErrorCode::NG_TERM_CHANGED &&
                     err_code != CcErrorCode::REQUESTED_NODE_NOT_LEADER)
            {
                self_guard.release();
                // Retry until primary node term has changed.
                DLOG(INFO) << "Fetch payload failed with "
                           << CcErrorMessage(err_code)
                           << ". Retry fetch "
                              "payload service of ng#"
                           << request_.node_group_id();
                cntl_.Reset();
                response_.Clear();
                remote::CcRpcService_Stub stub(channel_.get());
                cntl_.set_timeout_ms(5000);
                cntl_.set_write_to_socket_in_background(true);
                stub.FetchPayload(&cntl_, &request_, &response_, this);
                return;
            }
        }
        fetch_cc_->SetFinish((int) err_code);
        channel_ = nullptr;
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    remote::FetchPayloadResponse *FetchPayloadResponse()
    {
        return &response_;
    }

    remote::FetchPayloadRequest *FetchPayloadRequest()
    {
        return &request_;
    }

    void SetChannel(uint32_t node_id, std::shared_ptr<brpc::Channel> channel)
    {
        node_id_ = node_id;
        channel_ = channel;
    }

    brpc::Channel *Channel()
    {
        return channel_.get();
    }

    uint32_t NodeId() const
    {
        return node_id_;
    }

private:
    brpc::Controller cntl_;
    remote::FetchPayloadRequest request_;
    remote::FetchPayloadResponse response_;
    std::shared_ptr<brpc::Channel> channel_;
    uint32_t node_id_;
    FetchRecordCc *fetch_cc_;
};

class FetchCatalogClosure : public ::google::protobuf::Closure
{
public:
    FetchCatalogClosure(FetchCatalogCc *fetch_cc) : fetch_cc_(fetch_cc)
    {
    }

    FetchCatalogClosure(const FetchCatalogClosure &rhs) = delete;
    FetchCatalogClosure(FetchCatalogClosure &&rhs) = delete;

    // Run() will be called when rpc request is processed by cc node service.
    void Run() override
    {
        // Free closure on exit
        std::unique_ptr<FetchCatalogClosure> self_guard(this);
        if (!fetch_cc_->ValidTermCheck())
        {
            channel_ = nullptr;
            fetch_cc_->SetFinish(RecordStatus::Deleted,
                                 (int) CcErrorCode::NG_TERM_CHANGED);
            return;
        }
        if (cntl_.Failed())
        {
            // RPC failed.
            LOG(ERROR) << "Failed for FetchCatalog RPC request of ng#"
                       << request_.node_group_id()
                       << ", with Error code: " << cntl_.ErrorCode()
                       << ". Error Msg: " << cntl_.ErrorText();
            if (cntl_.ErrorCode() == brpc::EOVERCROWDED ||
                cntl_.ErrorCode() == EAGAIN)
            {
                bthread_usleep(10000);

                self_guard.release();
                // Retry if timeout.
                DLOG(INFO) << "Retry after EOVERCROWDED FetchCatalog "
                              "service of ng#"
                           << request_.node_group_id();
                cntl_.Reset();
                response_.Clear();
                remote::CcRpcService_Stub stub(channel_.get());
                cntl_.set_timeout_ms(5000);
                cntl_.set_write_to_socket_in_background(true);
                stub.FetchCatalog(&cntl_, &request_, &response_, this);
                return;
            }
            if (cntl_.ErrorCode() == brpc::ERPCTIMEDOUT)
            {
                self_guard.release();
                // Retry if timeout.
                DLOG(INFO) << "Fetch catalog request timed out. Retry "
                              "FetchCatalog service of ng#"
                           << request_.node_group_id();
                cntl_.Reset();
                response_.Clear();
                remote::CcRpcService_Stub stub(channel_.get());
                cntl_.set_timeout_ms(5000);
                cntl_.set_write_to_socket_in_background(true);
                stub.FetchCatalog(&cntl_, &request_, &response_, this);
                return;
            }
            Sharder::Instance().UpdateCcNodeServiceChannel(node_id_, channel_);
        }
        else
        {
            CcErrorCode err_code =
                remote::ToLocalType::ConvertCcErrorCode(response_.error_code());

            if (err_code == CcErrorCode::NO_ERROR)
            {
                RecordStatus rec_status = response_.is_deleted()
                                              ? RecordStatus::Deleted
                                              : RecordStatus::Normal;
                if (rec_status == RecordStatus::Normal)
                {
                    assert(response_.payload().size() > 0);
                    std::string &catalog_image = fetch_cc_->CatalogImage();
                    catalog_image.clear();
                    catalog_image.append(response_.payload());
                }
                fetch_cc_->SetCommitTs(response_.version());
                fetch_cc_->SetFinish(rec_status, 0);
            }
            else if (err_code != CcErrorCode::NG_TERM_CHANGED &&
                     err_code != CcErrorCode::REQUESTED_NODE_NOT_LEADER)
            {
                self_guard.release();
                // Retry until primary node term has changed.
                DLOG(INFO) << "Fetch catalog failed with "
                           << CcErrorMessage(err_code)
                           << ". Retry FetchCatalog service of ng#"
                           << request_.node_group_id();
                cntl_.Reset();
                response_.Clear();
                remote::CcRpcService_Stub stub(channel_.get());
                cntl_.set_timeout_ms(5000);
                cntl_.set_write_to_socket_in_background(true);
                stub.FetchCatalog(&cntl_, &request_, &response_, this);
                return;
            }
            else
            {
                fetch_cc_->SetFinish(RecordStatus::Unknown, (int) err_code);
            }
        }
        channel_ = nullptr;
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    remote::FetchPayloadResponse *FetchPayloadResponse()
    {
        return &response_;
    }

    remote::FetchPayloadRequest *FetchPayloadRequest()
    {
        return &request_;
    }

    void SetChannel(uint32_t node_id, std::shared_ptr<brpc::Channel> channel)
    {
        node_id_ = node_id;
        channel_ = channel;
    }

    brpc::Channel *Channel()
    {
        return channel_.get();
    }

    uint32_t NodeId() const
    {
        return node_id_;
    }

private:
    brpc::Controller cntl_;
    remote::FetchPayloadRequest request_;
    remote::FetchPayloadResponse response_;
    std::shared_ptr<brpc::Channel> channel_;
    uint32_t node_id_;
    FetchCatalogCc *fetch_cc_;
};
}  // namespace txservice
