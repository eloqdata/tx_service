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

#ifdef __GNUC__

#include <brpc/controller.h>

#include "cc/cc_handler_result.h"
#include "cc_request.pb.h"
#include "error_messages.h"  //CcErrorCode
#include "fault_inject.h"
#include "log.pb.h"
#include "type.h"

namespace txservice
{
enum struct LogType
{
    RECORD,
    CREATE_TABLE,
    DROP_TABLE
};

/*
 * LogClosure is the closure for txlog service
 *
 * LogClosure will be passed to txlog service along with log request. When log
 * request is finished, Function Run() will be called to notify the txservice.
 */
class LogClosure : public google::protobuf::Closure
{
public:
    explicit LogClosure(CcHandlerResult<Void> *hd_result)
        : cntl_(), hd_result_(hd_result)
    {
    }

    ~LogClosure() = default;

    // Run() will be called when log request is processed by txlog service.
    void Run() override
    {
        CODE_FAULT_INJECTOR("log_closure_result_unknown", {
            hd_result_->SetError(CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR);
            return;
        });
        // rpc fails including timeout indicates the status of log request is
        // unknown.
        if (cntl_.Failed() || response_.response_status() ==
                                  ::txlog::LogResponse_ResponseStatus_Unknown)
        {
            hd_result_->SetError(CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR);
        }
        else if (response_.response_status() ==
                 ::txlog::LogResponse_ResponseStatus_DuplicateMigrationTx)
        {
            hd_result_->SetError(CcErrorCode::DUPLICATE_MIGRATION_TX_ERR);
        }
        else if (response_.response_status() ==
                 ::txlog::LogResponse_ResponseStatus_DuplicateClusterScaleTx)
        {
            hd_result_->SetError(CcErrorCode::DUPLICATE_CLUSTER_SCALE_TX_ERR);
        }
        else if (response_.response_status() ==
                 ::txlog::LogResponse_ResponseStatus_Success)
        {
            hd_result_->SetFinished();
        }
        else if (response_.response_status() ==
                 ::txlog::LogResponse_ResponseStatus_Fail)
        {
            hd_result_->SetError(CcErrorCode::WRITE_LOG_FAILED);
        }
        else if (response_.response_status() ==
                 ::txlog::LogResponse_ResponseStatus_NotLeader)
        {
            hd_result_->SetError(CcErrorCode::LOG_NODE_NOT_LEADER);
        }
        else
        {
            hd_result_->SetError(CcErrorCode::UNDEFINED_ERR);
        }
    }

    ::txlog::LogRequest &LogRequest()
    {
        return request_;
    }

    const ::txlog::LogRequest &LogRequest() const
    {
        return request_;
    }

    ::txlog::LogResponse &LogResponse()
    {
        return response_;
    }

    const ::txlog::LogResponse &LogResponse() const
    {
        return response_;
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    void Reset()
    {
        cntl_.Reset();
        // request should not reset since the log request could be resend.
        response_.Clear();
    }

private:
    brpc::Controller cntl_;
    ::txlog::LogRequest request_;
    ::txlog::LogResponse response_;
    CcHandlerResult<Void> *hd_result_;
};

class CheckMigrationIsFinishedClosure : public google::protobuf::Closure
{
public:
    explicit CheckMigrationIsFinishedClosure(bool *migration_is_finished,
                                             std::atomic<bool> *is_finished)
        : cntl_(),
          migration_is_finished_(migration_is_finished),
          is_finished_(is_finished)
    {
    }

    ~CheckMigrationIsFinishedClosure() = default;

    void Run() override
    {
        if (!cntl_.Failed())
        {
            *migration_is_finished_ = response_.finished();
        }
        else
        {
            *migration_is_finished_ = false;
        }

        is_finished_->store(true, std::memory_order_release);
    }

    txlog::CheckMigrationIsFinishedRequest &Request()
    {
        return request_;
    }

    txlog::CheckMigrationIsFinishedResponse &Response()
    {
        return response_;
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    void Reset()
    {
        cntl_.Reset();
        response_.Clear();
    }

private:
    brpc::Controller cntl_;
    txlog::CheckMigrationIsFinishedRequest request_;
    txlog::CheckMigrationIsFinishedResponse response_;
    bool *migration_is_finished_;
    std::atomic<bool> *is_finished_;
};

}  // namespace txservice

#endif
