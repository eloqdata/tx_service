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
#include "remote/remote_cc_request.h"

#include <atomic>
#include <memory>
#include <string_view>
#include <utility>

#include "cc/cc_handler_result.h"
#include "cc/ccm_scanner.h"
#include "cc_request.pb.h"
#include "error_messages.h"  //CcErrorCode
#include "remote/remote_cc_handler.h"
#include "remote/remote_type.h"  //ToRemoteType
#include "sharder.h"

txservice::remote::RemoteAcquire::RemoteAcquire()
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_AcquireResponse);

    cc_res_.post_lambda_ =
        [this](CcHandlerResult<std::vector<AcquireKeyResult>> *res)
    {
        CODE_FAULT_INJECTOR("remote_acquire_msg_missed", {
            LOG(INFO) << "FaultInject  remote_acquire_msg_missed";
            FaultInject::Instance().InjectFault("remote_acquire_msg_missed",
                                                "remove");
            return;
        });

        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        const AcquireRequest &acquire_req = input_msg_->acquire_req();
        AcquireResponse *resp = output_msg_.mutable_acquire_resp();
        resp->set_is_ack(false);
        resp->set_error_code(
            ToRemoteType::ConvertCcErrorCode(res->ErrorCode()));
        resp->set_vec_idx(acquire_req.vec_idx());

        if (!cc_res_.IsError())
        {
            const AcquireKeyResult &acquire_key_res = cc_res_.Value()[0];

            resp->set_vali_ts(acquire_key_res.last_vali_ts_);
            resp->set_commit_ts(acquire_key_res.commit_ts_);

            const CcEntryAddr &addr = acquire_key_res.cce_addr_;
            CceAddr_msg *resp_addr = resp->mutable_cce_addr();
            assert(addr.CceLockPtr() != 0);
            resp_addr->set_cce_lock_ptr(addr.CceLockPtr());
            resp_addr->set_term(addr.Term());
            resp_addr->set_core_id(addr.CoreId());
        }

        ACTION_FAULT_INJECTOR("remote_acquire_before_sendmessage");
        const AcquireRequest &req = input_msg_->acquire_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);

        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteAcquire::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_acquire_req());

    cc_res_.Reset();
    cc_res_.Value().resize(1);
    cc_res_.Value()[0].Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_acquire_resp();

    const AcquireRequest &req = input_msg->acquire_req();

    std::string_view table_name_sv{req.table_name_str()};
    // Need to parse the string if not include table type in protobuf
    remote_table_name_ = TableName(
        table_name_sv, ToLocalType::ConvertCcTableType(req.table_type()));

    AcquireCc::Reset(&remote_table_name_,
                     req.schema_version(),
                     &req.key(),
                     req.key_shard_code(),
                     input_msg->tx_number(),
                     input_msg->tx_term(),
                     req.ts(),
                     req.insert(),
                     &cc_res_,
                     req.vec_idx(),
                     ToLocalType::ConvertProtocol(req.protocol()),
                     ToLocalType::ConvertIsolation(req.iso_level()));

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

void txservice::remote::RemoteAcquire::Acknowledge()
{
    output_msg_.set_tx_number(input_msg_->tx_number());
    output_msg_.set_handler_addr(input_msg_->handler_addr());
    output_msg_.set_tx_term(input_msg_->tx_term());
    output_msg_.set_command_id(input_msg_->command_id());

    const AcquireRequest &acquire_req = input_msg_->acquire_req();
    AcquireResponse *acquire_resp = output_msg_.mutable_acquire_resp();
    acquire_resp->set_is_ack(true);
    acquire_resp->set_error_code(
        ToRemoteType::ConvertCcErrorCode(CcErrorCode::NO_ERROR));
    acquire_resp->set_vec_idx(acquire_req.vec_idx());

    CceAddr_msg *resp_addr = acquire_resp->mutable_cce_addr();
    const CcEntryAddr &addr = cc_res_.Value()[0].cce_addr_;
    assert(addr.CceLockPtr() != 0);
    resp_addr->set_cce_lock_ptr(addr.CceLockPtr());
    resp_addr->set_term(addr.Term());
    resp_addr->set_core_id(addr.CoreId());

    const AcquireRequest &req = input_msg_->acquire_req();
    hd_->SendMessageToNode(req.src_node_id(), output_msg_);
}

txservice::remote::RemoteAcquireAll::RemoteAcquireAll()
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_AcquireAllResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<AcquireAllResult> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        AcquireAllResponse *resp = output_msg_.mutable_acquire_all_resp();
        resp->set_error_code(
            ToRemoteType::ConvertCcErrorCode(res->ErrorCode()));
        resp->set_is_ack(false);

        if (!cc_res_.IsError())
        {
            const AcquireAllResult &acquire_all_res = cc_res_.Value();

            resp->set_vali_ts(acquire_all_res.last_vali_ts_);
            resp->set_commit_ts(acquire_all_res.commit_ts_);
            resp->set_node_term(acquire_all_res.node_term_);
        }

        const AcquireAllRequest &req = input_msg_->acquire_all_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteAcquireAll::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_acquire_all_req());

    core_cnt_ = Sharder::Instance().GetLocalCcShardsCount();
    cc_res_.Reset();
    cc_res_.Value().Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_acquire_all_resp();

    const AcquireAllRequest &req = input_msg->acquire_all_req();
    std::string_view table_name_sv{req.table_name_str()};
    remote_table_name_ = TableName(
        table_name_sv, ToLocalType::ConvertCcTableType(req.table_type()));

    if (req.acq_all_key_case() == AcquireAllRequest::AcqAllKeyCase::kNegInf)
    {
        key_type_ = KeyType::NegativeInf;
    }
    else if (req.acq_all_key_case() ==
             AcquireAllRequest::AcqAllKeyCase::kPosInf)
    {
        key_type_ = KeyType::PositiveInf;
    }
    else
    {
        key_type_ = KeyType::Normal;
    }

    AcquireAllCc::Reset(&remote_table_name_,
                        &req.key(),
                        &key_type_,
                        req.node_group_id(),
                        input_msg->tx_number(),
                        input_msg->tx_term(),
                        req.insert(),
                        &cc_res_,
                        Sharder::Instance().GetLocalCcShardsCount(),
                        ToLocalType::ConvertProtocol(req.protocol()),
                        ToLocalType::ConvertCcOperation(req.cc_op()));

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

void txservice::remote::RemoteAcquireAll::Acknowledge()
{
    assert(!cce_addrs_.empty());

    output_msg_.set_tx_number(input_msg_->tx_number());
    output_msg_.set_handler_addr(input_msg_->handler_addr());
    output_msg_.set_tx_term(input_msg_->tx_term());
    output_msg_.set_command_id(input_msg_->command_id());

    AcquireAllResponse *acquire_all_resp =
        output_msg_.mutable_acquire_all_resp();
    acquire_all_resp->set_is_ack(true);
    acquire_all_resp->set_error_code(
        ToRemoteType::ConvertCcErrorCode(CcErrorCode::NO_ERROR));
    acquire_all_resp->set_node_term(cce_addrs_.at(0).Term());

    acquire_all_resp->clear_ack_cce_addr();
    auto *mutable_cce_addrs = acquire_all_resp->mutable_ack_cce_addr();
    for (size_t idx = 0; idx < cce_addrs_.size(); ++idx)
    {
        CceAddr_msg *addr_msg = mutable_cce_addrs->Add();
        addr_msg->set_cce_lock_ptr(cce_addrs_[idx].CceLockPtr());
        addr_msg->set_term(cce_addrs_[idx].Term());
        addr_msg->set_core_id(cce_addrs_[idx].CoreId());
        addr_msg->set_node_group_id(cce_addrs_[idx].NodeGroupId());
    }

    const AcquireAllRequest &req = input_msg_->acquire_all_req();
    hd_->SendMessageToNode(req.src_node_id(), output_msg_);
}

txservice::remote::RemotePostRead::RemotePostRead()
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ValidateResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<PostProcessResult> *res)
    {
        if (need_resp_)
        {
            output_msg_.set_tx_number(input_msg_->tx_number());
            output_msg_.set_handler_addr(input_msg_->handler_addr());
            output_msg_.set_tx_term(input_msg_->tx_term());
            output_msg_.set_command_id(input_msg_->command_id());

            ValidateResponse *resp = output_msg_.mutable_validate_resp();
            resp->set_error_code(
                ToRemoteType::ConvertCcErrorCode(res->ErrorCode()));

            if (res->IsError())
            {
                // RemotePostRead at the remote node accesses one key, which
                // locates in a single shard. Hence, there are no concurrent
                // modifications of PostReadResult. It is safe to access the
                // result's array without the mutex protection.
                for (TxNumber &txn : res->Value().conflicting_txs_)
                {
                    resp->add_txs(txn);
                }
            }

            const ValidateRequest &req = input_msg_->validate_req();
            hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        }
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemotePostRead::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_validate_req());

    cc_res_.Reset();
    cc_res_.Value().Clear();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_validate_resp();

    const ValidateRequest &req = input_msg->validate_req();
    const CceAddr_msg &cce_addr = req.cce_addr();

    need_resp_ = req.need_resp();
    cce_addr_.SetCceLock(cce_addr.cce_lock_ptr(),
                         cce_addr.term(),
                         req.node_group_id(),
                         cce_addr.core_id());

    PostReadCc::Reset(&cce_addr_,
                      input_msg->tx_number(),
                      input_msg->tx_term(),
                      req.commit_ts(),
                      req.key_ts(),
                      req.gap_ts(),
                      &cc_res_);

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

txservice::remote::RemoteRead::RemoteRead()
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ReadResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<ReadKeyResult> *res)
    {
        CODE_FAULT_INJECTOR("remote_read_msg_missed", {
            LOG(INFO) << "FaultInject  remote_read_msg_missed";
            FaultInject::Instance().InjectFault("remote_read_msg_missed",
                                                "remove");
            return;
        });

        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        const ReadKeyResult &read_result = res->Value();
        ReadResponse *resp = output_msg_.mutable_read_resp();
        resp->set_is_ack(false);
        resp->set_error_code(
            ToRemoteType::ConvertCcErrorCode(res->ErrorCode()));

        if (!res->IsError())
        {
            resp->set_rec_status(
                ToRemoteType::ConvertRecordStatus(read_result.rec_status_));
            resp->set_lock_type(
                ToRemoteType::ConvertLockType(read_result.lock_type_));
            resp->set_ts(read_result.ts_);

            CceAddr_msg *cce_addr_msg = resp->mutable_cce_addr();
            cce_addr_msg->set_cce_lock_ptr(read_result.cce_addr_.CceLockPtr());
            cce_addr_msg->set_term(read_result.cce_addr_.Term());
            cce_addr_msg->set_core_id(read_result.cce_addr_.CoreId());
        }

        const ReadRequest &req = input_msg_->read_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteRead::Reset(std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_read_req());

    cc_res_.Reset();
    cc_res_.Value().Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_read_resp();

    const ReadRequest &req = input_msg->read_req();
    std::string_view table_name_sv{req.table_name_str()};
    remote_table_name_ = TableName(
        table_name_sv, ToLocalType::ConvertCcTableType(req.table_type()));

    ReadType read_type = ReadType::Inside;
    switch (req.read_type())
    {
    case ReadRequest_ReadType::ReadRequest_ReadType_INSIDE:
        read_type = ReadType::Inside;
        break;
    case ReadRequest_ReadType::ReadRequest_ReadType_OUTSIDE_NORMAL:
        read_type = ReadType::OutsideNormal;
        break;
    case ReadRequest_ReadType::ReadRequest_ReadType_OUTSIDE_DELETED:
        read_type = ReadType::OutsideDeleted;
        break;
    default:
        break;
    }

    cc_res_.Value().cce_addr_.SetCceLock(0, -1, req.key_shard_code() >> 10, 0);

    ReadResponse *resp = output_msg_.mutable_read_resp();
    resp->clear_record();
    if (read_type == ReadType::Inside)
    {
        ReadCc::Reset(&remote_table_name_,
                      req.schema_version(),
                      &req.key(),
                      req.key_shard_code(),
                      resp->mutable_record(),
                      read_type,
                      input_msg->tx_number(),
                      input_msg->tx_term(),
                      req.ts(),
                      &cc_res_,
                      ToLocalType::ConvertIsolation(req.iso_level()),
                      ToLocalType::ConvertProtocol(req.protocol()),
                      req.is_for_write(),
                      req.is_covering_keys(),
                      nullptr,
                      req.point_read_on_miss());
    }
    else
    {
        // The read brings in an external record (from the data store) for
        // concurrency control

        std::string *out_record = resp->mutable_record();
        *out_record = req.record();

        ReadCc::Reset(&remote_table_name_,
                      req.schema_version(),
                      &req.key(),
                      req.key_shard_code(),
                      out_record,
                      read_type,
                      input_msg->tx_number(),
                      input_msg->tx_term(),
                      req.ts(),
                      &cc_res_,
                      ToLocalType::ConvertIsolation(req.iso_level()),
                      ToLocalType::ConvertProtocol(req.protocol()),
                      req.is_for_write(),
                      req.is_covering_keys());
    }

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

void txservice::remote::RemoteRead::Acknowledge()
{
    output_msg_.set_tx_number(input_msg_->tx_number());
    output_msg_.set_handler_addr(input_msg_->handler_addr());
    output_msg_.set_tx_term(input_msg_->tx_term());
    output_msg_.set_command_id(input_msg_->command_id());

    ReadResponse *read_resp = output_msg_.mutable_read_resp();
    read_resp->set_is_ack(true);
    read_resp->set_error_code(
        ToRemoteType::ConvertCcErrorCode(CcErrorCode::NO_ERROR));

    CceAddr_msg *resp_addr = read_resp->mutable_cce_addr();
    const CcEntryAddr &addr = cc_res_.Value().cce_addr_;
    assert(addr.CceLockPtr() != 0);
    resp_addr->set_cce_lock_ptr(addr.CceLockPtr());
    resp_addr->set_term(addr.Term());
    resp_addr->set_core_id(addr.CoreId());

    const ReadRequest &req = input_msg_->read_req();
    hd_->SendMessageToNode(req.src_node_id(), output_msg_);
}

txservice::remote::RemotePostWrite::RemotePostWrite()
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_PostprocessResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<PostProcessResult> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        PostprocessResponse *resp = output_msg_.mutable_post_resp();
        resp->set_error_code(
            ToRemoteType::ConvertCcErrorCode(res->ErrorCode()));

        if (input_msg_->has_postcommit_req())
        {
            const PostCommitRequest &req = input_msg_->postcommit_req();
            hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        }
        else
        {
            assert(input_msg_->has_forward_post_commit_req());
            const ForwardPostCommitRequest &req =
                input_msg_->forward_post_commit_req();
            hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        }
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemotePostWrite::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_postcommit_req() ||
           input_msg->has_forward_post_commit_req());

    cc_res_.Reset();
    cc_res_.Value().Clear();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_post_resp();

    if (input_msg->has_postcommit_req())
    {
        const PostCommitRequest &post_commit = input_msg->postcommit_req();
        uint64_t commit_ts = post_commit.commit_ts();
        const std::string *rec_str =
            commit_ts > 0 ? &post_commit.record() : nullptr;
        const CceAddr_msg &cce_addr_msg = post_commit.cce_addr();

        cce_addr_.SetCceLock(cce_addr_msg.cce_lock_ptr(),
                             cce_addr_msg.term(),
                             post_commit.node_group_id(),
                             cce_addr_msg.core_id());
        PostWriteCc::Reset(
            &cce_addr_,
            input_msg->tx_number(),
            input_msg->tx_term(),
            commit_ts,
            rec_str,
            static_cast<OperationType>(post_commit.operation_type()),
            post_commit.key_shard_code(),
            &cc_res_);
    }
    else
    {
        const ForwardPostCommitRequest &post_commit =
            input_msg->forward_post_commit_req();
        uint64_t commit_ts = post_commit.commit_ts();
        const std::string *rec_str =
            commit_ts > 0 ? &post_commit.record() : nullptr;
        std::string_view table_name_sv{post_commit.table_name_str()};
        remote_table_name_ = TableName(
            table_name_sv,
            ToLocalType::ConvertCcTableType(post_commit.table_type()));
        OperationType op_type =
            static_cast<OperationType>(post_commit.operation_type());
        PostWriteCc::Reset(&remote_table_name_,
                           &post_commit.key(),
                           post_commit.node_group_id(),
                           input_msg->tx_number(),
                           input_msg->tx_term(),
                           commit_ts,
                           rec_str,
                           op_type,
                           post_commit.key_shard_code(),
                           &cc_res_,
                           op_type == OperationType::Insert,
                           post_commit.node_group_term());
    }
    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

txservice::remote::RemotePostWriteAll::RemotePostWriteAll()
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_PostprocessResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<PostProcessResult> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        PostprocessResponse *resp = output_msg_.mutable_post_resp();
        resp->set_error_code(
            ToRemoteType::ConvertCcErrorCode(res->ErrorCode()));

        const PostWriteAllRequest &req = input_msg_->post_write_all_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemotePostWriteAll::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_post_write_all_req());

    cc_res_.Reset();
    cc_res_.Value().Clear();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_post_resp();

    const PostWriteAllRequest &post_write_all = input_msg->post_write_all_req();

    std::string_view table_name_sv{post_write_all.table_name_str()};
    remote_table_name_ =
        TableName(table_name_sv,
                  ToLocalType::ConvertCcTableType(post_write_all.table_type()));

    uint64_t commit_ts = post_write_all.commit_ts();
    const std::string *rec_str =
        commit_ts > 0 ? &post_write_all.record() : nullptr;
    OperationType op_type =
        static_cast<OperationType>(post_write_all.operation_type());
    PostWriteType write_type =
        ToLocalType::ConvertCommitType(post_write_all.commit_type());

    int64_t tx_term = input_msg->tx_term();

    if (post_write_all.post_write_key_case() ==
        PostWriteAllRequest::PostWriteKeyCase::kNegInf)
    {
        key_type_ = KeyType::NegativeInf;
    }
    else if (post_write_all.post_write_key_case() ==
             PostWriteAllRequest::PostWriteKeyCase::kPosInf)
    {
        key_type_ = KeyType::PositiveInf;
    }
    else
    {
        key_type_ = KeyType::Normal;
    }

    PostWriteAllCc::Reset(&remote_table_name_,
                          &post_write_all.key(),
                          &key_type_,
                          post_write_all.node_group_id(),
                          input_msg->tx_number(),
                          commit_ts,
                          rec_str,
                          op_type,
                          &cc_res_,
                          write_type,
                          tx_term);

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

txservice::remote::RemoteScanOpen::RemoteScanOpen()
{
    parallel_req_ = true;
    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ScanOpenResponse);
    res_ = &cc_res_;

    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        ScanOpenResponse *scan_open = output_msg_.mutable_scan_open_resp();

        scan_open->set_error_code(
            ToRemoteType::ConvertCcErrorCode(cc_res_.ErrorCode()));

        if (cc_res_.IsError())
        {
            CcOperation cc_op;
            if (remote_table_name_.Type() == TableType::Secondary)
            {
                cc_op = CcOperation::ReadSkIndex;
            }
            else if (remote_table_name_.Type() == TableType::UniqueSecondary)
            {
                cc_op = IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::ReadSkIndex;
            }
            else
            {
                cc_op = IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::Read;
            }
            LockType lock_type = LockTypeUtil::DeduceLockType(
                cc_op, Isolation(), Protocol(), IsCoveringKeys());

            // When there is a scan error and the scan does not put locks on the
            // scanned entries, clears the scan cache and does not return them
            // back to the sender. If the scan puts locks on scanned entries,
            // returns them to the sending tx, who will release locks on
            // post-processing.
            if (lock_type == LockType::NoLock)
            {
                // Not acquire lock, just clear scan cache.
                for (int core_id = 0; core_id < scan_open->scan_cache_size();
                     ++core_id)
                {
                    ScanCache_msg *cache =
                        scan_open->mutable_scan_cache(core_id);
                    cache->Clear();
                }
            }
        }

        scan_open->set_node_group_id(node_group_id_);
        const ScanOpenRequest &req = input_msg_->scan_open_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteScanOpen::Reset(
    std::unique_ptr<CcMessage> input_msg, uint32_t core_cnt)
{
    assert(input_msg->has_scan_open_req());

    cc_res_.Reset();
    cc_res_.SetRefCnt(core_cnt);
    cce_ptr_.clear();
    cce_ptr_.resize(core_cnt);
    cce_ptr_scan_type_.clear();
    cce_ptr_scan_type_.resize(core_cnt);
    is_wait_for_post_write_.resize(core_cnt);

    const ScanOpenRequest &scan_open = input_msg->scan_open_req();

    std::string_view table_name_sv{scan_open.table_name_str()};
    remote_table_name_ = TableName(
        table_name_sv, ToLocalType::ConvertCcTableType(scan_open.table_type()));

    node_group_id_ = scan_open.shard_id();
    table_name_ = &remote_table_name_;
    tx_term_ = input_msg->tx_term();
    is_for_write_ = scan_open.is_for_write();
    is_covering_keys_ = scan_open.is_covering_keys();
    isolation_level_ = ToLocalType::ConvertIsolation(scan_open.iso_level());
    proto_ = ToLocalType::ConvertProtocol(scan_open.protocol());
    tx_number_ = input_msg->tx_number();
    snapshot_ts_ = scan_open.ts();

#ifdef ON_KEY_OBJECT
    obj_type_ = scan_open.obj_type();
    scan_pattern_ = scan_open.scan_pattern();
    schema_version_ = scan_open.schema_version();
#endif

    ccm_ = nullptr;

    if (scan_open.start_key_case() == ScanOpenRequest::StartKeyCase::kNegInf)
    {
        key_type_ = KeyType::NegativeInf;
        start_key_str_ = nullptr;
    }
    else if (scan_open.start_key_case() ==
             ScanOpenRequest::StartKeyCase::kPosInf)
    {
        key_type_ = KeyType::PositiveInf;
        start_key_str_ = nullptr;
    }
    else
    {
        key_type_ = KeyType::Normal;
        start_key_str_ = &scan_open.key();
    }

    inclusive_ = scan_open.inclusive();
    direct_ = scan_open.direction() ? ScanDirection::Forward
                                    : ScanDirection::Backward;

    is_ckpt_delta_ = scan_open.ckpt();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_scan_open_resp();

    ScanOpenResponse *resp = output_msg_.mutable_scan_open_resp();
    resp->clear_scan_cache();

    scan_caches_.clear();
    for (size_t cid = 0; cid < core_cnt; ++cid)
    {
        ScanCache_msg *cache_msg = resp->add_scan_cache();
        assert(cache_msg->scan_tuple_size() == 0);
        scan_caches_.emplace_back(cache_msg, 0);
    }

    unfinish_cnt_.store(core_cnt);

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }

    ng_term_ = -1;
}

void txservice::remote::RemoteScanOpen::Free()
{
    uint32_t prior_val = unfinish_cnt_.fetch_sub(1);
    if (prior_val == 1)
    {
        CcRequestBase::Free();
    }
}

txservice::remote::RemoteScanNextBatch::RemoteScanNextBatch()
{
    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ScanNextResponse);
    is_ckpt_delta_ = false;
    res_ = &cc_res_;
    cce_ptr_ = nullptr;

    /*message ScanNextResponse
    {
        bool error = 1;
        repeated ScanTuple_msg scan_tuple = 2;
        uint64 ccm_ptr = 3;
        uint64 scan_cache_ptr = 4;
    }*/

    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        ScanNextResponse *scan_next_resp = output_msg_.mutable_scan_next_resp();
        const ScanNextRequest &req = input_msg_->scan_next_req();
        scan_next_resp->set_error_code(
            ToRemoteType::ConvertCcErrorCode(res->ErrorCode()));

        if (res->IsError())
        {
            CcOperation cc_op;

            if (tbl_type_ == TableType::Secondary)
            {
                cc_op = CcOperation::ReadSkIndex;
            }
            else if (tbl_type_ == TableType::UniqueSecondary)
            {
                cc_op = IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::ReadSkIndex;
            }
            else
            {
                cc_op = IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::Read;
            }
            LockType lock_type = LockTypeUtil::DeduceLockType(
                cc_op, Isolation(), Protocol(), IsCoveringKeys());

            // When there is a scan error and the scan does not put locks on the
            // scanned entries, clears the scan cache and does not return them
            // back to the sender. If the scan puts locks on scanned entries,
            // returns them to the sending tx, who will release locks on
            // post-processing.
            if (lock_type == LockType::NoLock)
            {
                scan_next_resp->mutable_scan_cache()->Clear();
            }
        }
        scan_next_resp->set_scan_cache_ptr(req.scan_cache_ptr());

        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

bool txservice::remote::RemoteScanNextBatch::ValidTermCheck()
{
    int64_t cc_ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
    if (prior_cce_addr_.Term() != cc_ng_term)
    {
        return false;
    }

    const LruEntry *lru_entry = prior_cce_addr_.ExtractCce();
    ccm_ = lru_entry->GetCcMap();
    assert(ccm_ != nullptr);
    tbl_type_ = ccm_->Type();
    return true;
}

void txservice::remote::RemoteScanNextBatch::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_scan_next_req());

    cc_res_.Reset();

    const ScanNextRequest &scan_next = input_msg->scan_next_req();

    node_group_id_ = scan_next.node_group_id();
    const CceAddr_msg &cce_addr = scan_next.prior_cce_ptr();
    ng_term_ = -1;

    direct_ = scan_next.direction() ? ScanDirection::Forward
                                    : ScanDirection::Backward;
    tx_term_ = input_msg->tx_term();
    is_for_write_ = scan_next.is_for_write();
    is_covering_keys_ = scan_next.is_covering_keys();
    isolation_level_ = ToLocalType::ConvertIsolation(scan_next.iso_level());
    proto_ = ToLocalType::ConvertProtocol(scan_next.protocol());
    tx_number_ = input_msg->tx_number();
    cce_ptr_ = nullptr;
    snapshot_ts_ = scan_next.ts();

    prior_cce_addr_.SetCceLock(cce_addr.cce_lock_ptr(),
                               cce_addr.term(),
                               node_group_id_,
                               cce_addr.core_id());

#ifdef ON_KEY_OBJECT
    obj_type_ = scan_next.obj_type();
    scan_pattern_ = scan_next.scan_pattern();
#endif

    ccm_ = nullptr;

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_scan_next_resp();

    ScanNextResponse *resp = output_msg_.mutable_scan_next_resp();
    resp->clear_scan_cache();
    scan_cache_.cache_msg_ = resp->mutable_scan_cache();
    scan_cache_.cache_mem_size_ = 0;

    is_ckpt_delta_ = scan_next.ckpt();

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

txservice::remote::RemoteScanSlice::RemoteScanSlice()
{
    parallel_req_ = true;
    res_ = &cc_res_;

    cc_res_.Value().is_local_ = false;

    cc_res_.post_lambda_ = [this](CcHandlerResult<RangeScanSliceResult> *res)
    {
        output_msg_.set_error_code(
            ToRemoteType::ConvertCcErrorCode(cc_res_.ErrorCode()));
        bool send_cache = true;

        if (cc_res_.IsError())
        {
            CcOperation cc_op;

            if (remote_tbl_name_.Type() == TableType::Secondary)
            {
                cc_op = CcOperation::ReadSkIndex;
            }
            else if (remote_tbl_name_.Type() == TableType::UniqueSecondary)
            {
                cc_op = IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::ReadSkIndex;
            }
            else
            {
                cc_op = IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::Read;
            }

            LockType lock_type = LockTypeUtil::DeduceLockType(
                cc_op, Isolation(), Protocol(), IsCoveringKeys());

            // When there is a scan error and the scan does not put locks on the
            // scanned entries, clears the scan cache and does not return them
            // back to the sender. If the scan puts locks on scanned entries,
            // returns them to the sending tx, who will release locks on
            // post-processing.
            if (lock_type == LockType::NoLock)
            {
                send_cache = false;
            }
        }

        const RangeScanSliceResult &slice_result = cc_res_.Value();
        output_msg_.clear_last_key();
        auto [last_key, key_set] = slice_result.PeekLastKey();
        assert(key_set || cc_res_.IsError());
        // Only sends back the last key if this scan batch is not the last. The
        // next scan batch will use this last key as the beginning of the next
        // batch.
        if (!cc_res_.IsError() &&
            slice_result.slice_position_ !=
                txservice::SlicePosition::LastSlice &&
            slice_result.slice_position_ !=
                txservice::SlicePosition::FirstSlice)
        {
            assert(last_key->Type() == KeyType::Normal);
            last_key->Serialize(*output_msg_.mutable_last_key());
        }
        output_msg_.set_slice_position(
            ToRemoteType::ConvertSlicePosition(slice_result.slice_position_));

        uint16_t core_cnt = GetShardCount();
        // Add core cnt first
        output_msg_.mutable_tuple_cnt()->append((const char *) &core_cnt,
                                                sizeof(uint16_t));
        // Add tuple count for each core
        for (size_t idx = 0; idx < core_cnt; ++idx)
        {
            size_t tuple_cnt;
            if (send_cache)
            {
                tuple_cnt = scan_cache_vec_[idx].rec_status_.size();
            }
            else
            {
                tuple_cnt = 0;
            }
            output_msg_.mutable_tuple_cnt()->append((const char *) &tuple_cnt,
                                                    sizeof(size_t));
        }

        if (send_cache)
        {
            // Merge scan cache info into a single byte array to reduce
            // deserialization time on the receiver side.
            for (size_t idx = 0; idx < core_cnt; ++idx)
            {
                RemoteScanSliceCache &cache = scan_cache_vec_[idx];

                size_t keys_start_offset = output_msg_.keys().size();
                output_msg_.mutable_key_start_offsets()->append(
                    (const char *) &keys_start_offset, sizeof(size_t));
                size_t record_start_offset = output_msg_.records().size();
                output_msg_.mutable_record_start_offsets()->append(
                    (const char *) &record_start_offset, sizeof(size_t));

                output_msg_.mutable_keys()->append(cache.keys_);

                output_msg_.mutable_records()->append(cache.records_);
                output_msg_.mutable_key_ts()->append(
                    (const char *) cache.key_ts_.data(),
                    cache.key_ts_.size() * sizeof(uint64_t));
                output_msg_.mutable_gap_ts()->append(
                    (const char *) cache.gap_ts_.data(),
                    cache.gap_ts_.size() * sizeof(uint64_t));
                output_msg_.mutable_term()->append(
                    (const char *) cache.term_.data(),
                    cache.term_.size() * sizeof(uint64_t));
                output_msg_.mutable_cce_lock_ptr()->append(
                    (const char *) cache.cce_lock_ptr_.data(),
                    cache.cce_lock_ptr_.size() * sizeof(uint64_t));
                output_msg_.mutable_rec_status()->append(
                    (const char *) cache.rec_status_.data(),
                    cache.rec_status_.size() * sizeof(RecordStatusType));
            }
        }
        const ScanSliceRequest &req = input_msg_->scan_slice_req();
        hd_->SendScanRespToNode(req.src_node_id(), output_msg_, nullptr, false);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteScanSlice::Reset(
    std::unique_ptr<CcMessage> input_msg, uint16_t core_cnt)
{
    assert(input_msg->has_scan_slice_req());

    cc_res_.Value().Reset();
    cc_res_.Reset();
    output_msg_.Clear();

    const ScanSliceRequest &scan_slice_req = input_msg->scan_slice_req();
    std::string_view tbl_name_view(scan_slice_req.table_name_str());
    remote_tbl_name_ =
        TableName(tbl_name_view,
                  ToLocalType::ConvertCcTableType(scan_slice_req.table_type()));

    ScanSliceCc::Set(remote_tbl_name_,
                     scan_slice_req.schema_version(),
                     scan_slice_req.range_id(),
                     scan_slice_req.node_group_id(),
                     scan_slice_req.cc_ng_term(),
                     &scan_slice_req.start_key(),
                     scan_slice_req.start_inclusive(),
                     &scan_slice_req.end_key(),
                     scan_slice_req.end_inclusive(),
                     scan_slice_req.is_forward() ? ScanDirection::Forward
                                                 : ScanDirection::Backward,
                     scan_slice_req.ts(),
                     input_msg->tx_number(),
                     input_msg->tx_term(),
                     cc_res_,
                     ToLocalType::ConvertIsolation(scan_slice_req.iso_level()),
                     ToLocalType::ConvertProtocol(scan_slice_req.protocol()),
                     scan_slice_req.is_for_write(),
                     scan_slice_req.is_covering_keys(),
                     scan_slice_req.is_require_keys(),
                     scan_slice_req.is_require_recs(),
                     scan_slice_req.is_require_sort(),
                     scan_slice_req.prefetch_size());

    output_msg_.set_tx_number(input_msg->tx_number());
    output_msg_.set_handler_addr(input_msg->handler_addr());
    output_msg_.set_tx_term(input_msg->tx_term());
    output_msg_.set_command_id(input_msg->command_id());

    SetShardCount(core_cnt);

    size_t vec_size = scan_slice_req.prior_cce_lock_vec_size();
    for (size_t core_id = 0; core_id < core_cnt; ++core_id)
    {
        uint64_t cce_lock_addr =
            core_id < vec_size ? scan_slice_req.prior_cce_lock_vec(core_id) : 0;
        SetPriorCceLockAddr(cce_lock_addr, core_id);
    }

    RangeScanSliceResult &slice_result = cc_res_.Value();

    for (uint16_t core_id = 0; core_id < core_cnt; ++core_id)
    {
        if (core_id == scan_cache_vec_.size())
        {
            scan_cache_vec_.emplace_back(core_cnt);
        }
        else
        {
            scan_cache_vec_[core_id].Reset(core_cnt);
        }
    }
    slice_result.remote_scan_caches_ = &scan_cache_vec_;

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

void txservice::remote::RemoteReadOutside::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_read_outside_req());

    const ReadOutsideRequest &req = input_msg->read_outside_req();

    assert(req.cce_addr().cce_lock_ptr() != 0);
    cce_addr_.SetCceLock(req.cce_addr().cce_lock_ptr(),
                         req.cce_addr().term(),
                         req.node_group_id(),
                         req.cce_addr().core_id());
    rec_status_ = ToLocalType::ConvertRecordStatusType(req.rec_status());
    commit_ts_ = req.commit_ts();
    rec_str_ = &req.record();

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

void txservice::remote::RemoteReadOutside::Finish()
{
    hd_->RecycleCcMsg(std::move(input_msg_));
}

txservice::remote::RemoteReloadCacheCc::RemoteReloadCacheCc() : cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ReloadCacheResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());
        output_msg_.set_handler_addr(input_msg_->handler_addr());

        ReloadCacheResponse *resp = output_msg_.mutable_reload_cache_resp();
        resp->set_error_code(
            ToRemoteType::ConvertCcErrorCode(res->ErrorCode()));

        const ReloadCacheRequest &req = input_msg_->reload_cache_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteReloadCacheCc::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_reload_cache_req());

    cc_res_.Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_reload_cache_resp();

    ReloadCacheCc::Reset(&cc_res_);

    input_msg_ = std::move(input_msg);
    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

txservice::remote::RemoteFaultInjectCC::RemoteFaultInjectCC() : cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_FaultInjectResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<bool> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());
        output_msg_.set_handler_addr(input_msg_->handler_addr());

        FaultInjectResponse *resp = output_msg_.mutable_fault_inject_resp();
        resp->set_error_code(
            ToRemoteType::ConvertCcErrorCode(res->ErrorCode()));

        const FaultInjectRequest &req = input_msg_->fault_inject_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteFaultInjectCC::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_fault_inject_req());

    cc_res_.Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_acquire_resp();

    const FaultInjectRequest &req = input_msg->fault_inject_req();

    FaultInjectCC::Reset(&req.fault_name(), &req.fault_paras(), &cc_res_);

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

txservice::remote::RemoteBroadcastStatisticsCc::RemoteBroadcastStatisticsCc()
    : cc_res_(nullptr)
{
    res_ = &cc_res_;

    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    { hd_->RecycleCcMsg(std::move(input_msg_)); };
}

void txservice::remote::RemoteBroadcastStatisticsCc::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_broadcast_statistics_req());

    cc_res_.Reset();

    const BroadcastStatisticsRequest &req =
        input_msg->broadcast_statistics_req();
    remote_table_name_ =
        TableName(req.table_name_str(),
                  ToLocalType::ConvertCcTableType(req.table_type()));

    BroadcastStatisticsCc::Reset(req.node_group_id(),
                                 &remote_table_name_,
                                 req.schema_version(),
                                 req.node_group_sample_pool(),
                                 input_msg->tx_number(),
                                 input_msg->tx_term(),
                                 &cc_res_);
    input_msg_ = std::move(input_msg);
    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

txservice::remote::RemoteAnalyzeTableAllCc::RemoteAnalyzeTableAllCc()
    : cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_AnalyzeTableAllResponse);
    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());
        output_msg_.set_handler_addr(input_msg_->handler_addr());

        AnalyzeTableAllResponse *resp =
            output_msg_.mutable_analyze_table_all_resp();
        resp->set_error_code(
            ToRemoteType::ConvertCcErrorCode(res->ErrorCode()));

        const AnalyzeTableAllRequest &req = input_msg_->analyze_table_all_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteAnalyzeTableAllCc::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_analyze_table_all_req());

    cc_res_.Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_analyze_table_all_resp();

    const AnalyzeTableAllRequest &req = input_msg->analyze_table_all_req();
    std::string_view table_name_sv(req.table_name_str());
    remote_table_name_ = TableName(
        table_name_sv, ToLocalType::ConvertCcTableType(req.table_type()));

    AnalyzeTableAllCc::Reset(&remote_table_name_,
                             req.node_group_id(),
                             input_msg->tx_number(),
                             input_msg->tx_term(),
                             &cc_res_);

    input_msg_ = std::move(input_msg);
    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

txservice::remote::RemoteCleanCcEntryForTestCc::RemoteCleanCcEntryForTestCc()
    : cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(CcMessage::MessageType::
                             CcMessage_MessageType_CleanCcEntryForTestResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<bool> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());
        output_msg_.set_handler_addr(input_msg_->handler_addr());

        CleanCcEntryForTestResponse *resp =
            output_msg_.mutable_clean_cc_entry_resp();
        resp->set_error_code(
            ToRemoteType::ConvertCcErrorCode(res->ErrorCode()));

        const CleanCcEntryForTestRequest &req =
            input_msg_->clean_cc_entry_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteCleanCcEntryForTestCc::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_clean_cc_entry_req());

    cc_res_.Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_acquire_resp();

    const CleanCcEntryForTestRequest &req = input_msg->clean_cc_entry_req();
    std::string_view table_name_sv{req.table_name_str()};
    remote_table_name_ = TableName(
        table_name_sv, ToLocalType::ConvertCcTableType(req.table_type()));

    CleanCcEntryForTestCc::Reset(&remote_table_name_,
                                 &req.key(),
                                 req.only_archives(),
                                 req.flush(),
                                 req.key_shard_code(),
                                 input_msg->tx_number(),
                                 input_msg->tx_term(),
                                 &cc_res_);

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

void txservice::remote::RemoteCheckDeadLockCc::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_dead_lock_request());

    CheckDeadLockCc::Reset();
    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_acquire_resp();
    const DeadLockRequest &req = input_msg->dead_lock_request();
    DeadLockCheck::UpdateCheckNodeId(req.src_node_id());
    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

bool txservice::remote::RemoteCheckDeadLockCc::Execute(CcShard &ccs)
{
    ccs.CollectLockWaitingInfo(dead_lock_result_);

    int16_t unfinished = dead_lock_result_.unfinish_count_.fetch_sub(
        1, std::memory_order_acq_rel);
    if (unfinished == 1)
    {
        output_msg_.set_type(
            tr::CcMessage::MessageType::CcMessage_MessageType_DeadLockResponse);
        output_msg_.set_tx_number(0);
        output_msg_.set_handler_addr(0);
        output_msg_.set_tx_term(0);
        output_msg_.set_command_id(0);

        DeadLockResponse *resp = output_msg_.mutable_dead_lock_response();
        resp->set_error_code(0);
        resp->set_node_id(Sharder::Instance().NodeId());

        for (size_t i = 0; i < dead_lock_result_.entry_lock_info_vec_.size();
             i++)
        {
            auto &mapeti = dead_lock_result_.entry_lock_info_vec_[i];
            for (auto iter : mapeti)
            {
                BlockEntry *be = resp->add_block_entry();
                be->set_entry(iter.first);
                be->set_core_id(i);
                for (uint64_t txid : iter.second.lock_txids)
                {
                    be->add_locked_tids(txid);
                }

                for (uint64_t txid : iter.second.wait_txids)
                {
                    be->add_waited_tids(txid);
                }
            }
        }

        std::unordered_map<uint64_t, uint32_t> mte;
        for (auto &vcttx : dead_lock_result_.txid_ety_lock_count_)
        {
            for (auto &iter : vcttx)
            {
                auto it = mte.try_emplace(iter.first, 0);
                it.first->second += iter.second;
            }
        }

        for (auto &iter : mte)
        {
            TxEntrys *te = resp->add_tx_etys();
            te->set_txid(iter.first);
            te->set_ety_count(iter.second);
        }

        const DeadLockRequest &req = input_msg_->dead_lock_request();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    }
    return unfinished == 1;
}

void txservice::remote::RemoteAbortTransactionCc::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_abort_tran_req());

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_acquire_resp();
    const AbortTransactionRequest &req = input_msg->abort_tran_req();

    AbortTransactionCc::Reset(
        req.entry(), req.lock_txid(), req.wait_txid(), req.node_id());
    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

bool txservice::remote::RemoteAbortTransactionCc::Execute(CcShard &ccs)
{
    LruEntry *lru_entry = reinterpret_cast<LruEntry *>(entry_addr_);
    std::unordered_map<NodeGroupId, std::unordered_map<TxNumber, TxLockInfo>>
        &ltxs = ccs.GetLockHoldingTxs();
    int32_t err = 1;

    auto it_ng = ltxs.find(node_id_);
    if (it_ng != ltxs.end())
    {
        auto it_info = it_ng->second.find(tx_id_lock_);
        if (it_info != it_ng->second.end() &&
            it_info->second.cce_list_.find(lru_entry) !=
                it_info->second.cce_list_.end())
        {
            NonBlockingLock *key_lock = lru_entry->GetKeyLock();
            if (key_lock != nullptr)
            {
                key_lock->AbortQueueRequest(tx_id_wait_);
            }
            err = 0;
        }
    }

    output_msg_.set_type(tr::CcMessage::MessageType::
                             CcMessage_MessageType_AbortTransactionResponse);
    output_msg_.set_tx_number(0);
    output_msg_.set_handler_addr(0);
    output_msg_.set_tx_term(0);
    output_msg_.set_command_id(0);

    AbortTransactionResponse *resp = output_msg_.mutable_abort_tran_resp();
    resp->set_error_code(err);

    const AbortTransactionRequest &req = input_msg_->abort_tran_req();
    hd_->SendMessageToNode(req.src_node_id(), output_msg_);
    hd_->RecycleCcMsg(std::move(input_msg_));
    return true;
}

void txservice::remote::RemoteBlockReqCheckCc::Reset(
    std::unique_ptr<CcMessage> input_msg, size_t unfinish_core_cnt)
{
    assert(input_msg->has_blocked_check_req());

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_acquire_resp();

    input_msg_ = std::move(input_msg);

    unfinish_core_cnt_ = unfinish_core_cnt;
    term_changed_ = false;
    all_finished_ = true;

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

bool txservice::remote::RemoteBlockReqCheckCc::Execute(CcShard &ccs)
{
    const BlockedCcReqCheckRequest &req = input_msg_->blocked_check_req();
    AckStatus status = AckStatus::Finished;
    for (const auto &caddr : req.cce_addr())
    {
        if (caddr.core_id() == ccs.core_id_)
        {
            if (!Sharder::Instance().CheckLeaderTerm(req.node_group_id(),
                                                     caddr.term()))
            {
                status = AckStatus::ErrorTerm;
            }
            else
            {
                NonBlockingLock *lock =
                    reinterpret_cast<KeyGapLockAndExtraData *>(
                        caddr.cce_lock_ptr())
                        ->KeyLock();
                if (lock != nullptr)
                {
                    bool b = lock->FindQueueRequest(input_msg_->tx_number());
                    status = (b ? AckStatus::BlockQueue : AckStatus::Finished);
                }
            }
        }
    }

    CODE_FAULT_INJECTOR("block_req_term_changed", {
        LOG(INFO) << "FaultInject  block_req_term_changed";
        status = AckStatus::ErrorTerm;
        FaultInject::Instance().InjectFault("block_req_term_changed", "remove");
    });

    std::lock_guard<std::mutex> lk(mux_);
    assert(unfinish_core_cnt_ > 0);
    unfinish_core_cnt_--;

    if (status == AckStatus::ErrorTerm)
    {
        term_changed_ = true;
    }
    else if (status == AckStatus::BlockQueue)
    {
        all_finished_ = false;
    }

    // last core finished
    if (unfinish_core_cnt_ == 0)
    {
        output_msg_.set_type(
            tr::CcMessage::MessageType::
                CcMessage_MessageType_BlockedCcReqCheckResponse);

        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        AckStatus req_status = AckStatus::BlockQueue;
        if (term_changed_)
        {
            req_status = AckStatus::ErrorTerm;
        }
        else if (all_finished_)
        {
            req_status = AckStatus::Finished;
        }

        BlockedCcReqCheckResponse *resp =
            output_msg_.mutable_blocked_check_resp();
        resp->set_req_status((int32_t) req_status);
        resp->set_result_temp_type(
            input_msg_->blocked_check_req().result_temp_type());
        ResultTemplateType type =
            (ResultTemplateType) input_msg_->blocked_check_req()
                .result_temp_type();
        if (type == ResultTemplateType::AcquireKeyResult)
        {
            resp->set_acq_key_result_vec_idx(
                input_msg_->blocked_check_req().acq_key_result_vec_idx());
        }

        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    }
    return unfinish_core_cnt_ == 0;
}

txservice::remote::RemoteKickoutCcEntry::RemoteKickoutCcEntry()
{
    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_KickoutDataResponse);
    // Set callback function
    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *hres)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());
        output_msg_.set_handler_addr(input_msg_->handler_addr());

        // Construct response body
        KickoutDataResponse *resp = output_msg_.mutable_kickout_data_resp();
        resp->set_error_code(
            ToRemoteType::ConvertCcErrorCode(hres->ErrorCode()));

        // Send message
        const KickoutDataRequest &req = input_msg_->kickout_data_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);

        // Recycle the message
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteKickoutCcEntry::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_kickout_data_req());
    cc_res_.Reset();

    // Reset output msg
    output_msg_.clear_tx_number();
    output_msg_.clear_tx_term();
    output_msg_.clear_command_id();
    output_msg_.clear_handler_addr();
    output_msg_.clear_kickout_data_resp();

    // Construct local ccrequest using the info. in request body.
    const KickoutDataRequest &req = input_msg->kickout_data_req();

    std::string_view table_name_sv{req.table_name_str()};
    table_name_ = TableName(table_name_sv,
                            ToLocalType::ConvertCcTableType(req.table_type()));

    size_t core_cnt = 0;
    if (req.clean_type() == remote::CleanType::CleanCcm)
    {
        core_cnt = Sharder::Instance().GetLocalCcShardsCount();
    }
    else
    {
        core_cnt = 1;
    }

    KickoutCcEntryCc::Reset(table_name_,
                            req.node_group_id(),
                            core_cnt,
                            &cc_res_,
                            ToLocalType::ConvertCleanType(req.clean_type()),
                            &req.start_key(),
                            &req.end_key(),
                            nullptr,
                            req.clean_ts());

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

txservice::remote::RemoteApplyCc::RemoteApplyCc() : ApplyCc(false)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ApplyResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<ObjectCommandResult> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        const ObjectCommandResult &apply_result = res->Value();
        ApplyResponse *resp = output_msg_.mutable_apply_cc_resp();
        resp->set_is_ack(false);
        resp->set_error_code(
            ToRemoteType::ConvertCcErrorCode(res->ErrorCode()));

        if (!res->IsError())
        {
            resp->set_commit_ts(apply_result.commit_ts_);
            resp->set_last_vali_ts(apply_result.last_vali_ts_);

            CceAddr_msg *cce_addr_msg = resp->mutable_cce_addr();
            cce_addr_msg->set_cce_lock_ptr(apply_result.cce_addr_.CceLockPtr());
            cce_addr_msg->set_term(apply_result.cce_addr_.Term());
            cce_addr_msg->set_core_id(apply_result.cce_addr_.CoreId());

            resp->set_rec_status(
                ToRemoteType::ConvertRecordStatus(apply_result.rec_status_));
            resp->set_lock_type(
                ToRemoteType::ConvertLockType(apply_result.lock_acquired_));
            resp->set_object_modified(apply_result.object_modified_);

            assert(!is_local_);
            std::string *cmd_res_str = resp->mutable_cmd_result();
            assert(remote_input_.cmd_ != nullptr);
            if (remote_input_.cmd_->GetResult() != nullptr)
            {
                remote_input_.cmd_->GetResult()->Serialize(*cmd_res_str);
            }
        }

        const ApplyRequest &req = input_msg_->apply_cc_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteApplyCc::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_apply_cc_req());

    cc_res_.Reset();
    cc_res_.Value().Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_apply_cc_resp();

    const ApplyRequest &req = input_msg->apply_cc_req();
    std::string_view table_name_sv{req.table_name_str()};
    remote_table_name_ = TableName(
        table_name_sv, ToLocalType::ConvertCcTableType(req.table_type()));

    cc_res_.Value().cce_addr_.SetCceLock(0, -1, req.key_shard_code() >> 10, 0);

    ApplyResponse *resp = output_msg_.mutable_apply_cc_resp();
    resp->clear_cmd_result();
    ApplyCc::Reset(&remote_table_name_,
                   req.schema_version(),
                   &req.key(),
                   req.key_shard_code(),
                   &req.cmd(),
                   input_msg->tx_number(),
                   input_msg->tx_term(),
                   req.tx_ts(),
                   &cc_res_,
                   ToLocalType::ConvertProtocol(req.protocol()),
                   ToLocalType::ConvertIsolation(req.iso_level()),
                   req.apply_and_commit());

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

void txservice::remote::RemoteApplyCc::Acknowledge()
{
    output_msg_.set_tx_number(input_msg_->tx_number());
    output_msg_.set_handler_addr(input_msg_->handler_addr());
    output_msg_.set_tx_term(input_msg_->tx_term());
    output_msg_.set_command_id(input_msg_->command_id());

    ApplyResponse *apply_resp = output_msg_.mutable_apply_cc_resp();
    apply_resp->set_is_ack(true);
    apply_resp->set_error_code(
        ToRemoteType::ConvertCcErrorCode(CcErrorCode::NO_ERROR));

    CceAddr_msg *resp_addr = apply_resp->mutable_cce_addr();
    const CcEntryAddr &addr = cc_res_.Value().cce_addr_;
    assert(addr.CceLockPtr() != 0);
    resp_addr->set_cce_lock_ptr(addr.CceLockPtr());
    resp_addr->set_term(addr.Term());
    resp_addr->set_core_id(addr.CoreId());

    const ApplyRequest &req = input_msg_->apply_cc_req();
    hd_->SendMessageToNode(req.src_node_id(), output_msg_);
}

txservice::remote::RemoteUploadTxCommandsCc::RemoteUploadTxCommandsCc()
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_PostprocessResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<PostProcessResult> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        PostprocessResponse *resp = output_msg_.mutable_post_resp();
        resp->set_error_code(
            ToRemoteType::ConvertCcErrorCode(res->ErrorCode()));

        const UploadTxCommandsRequest &req = input_msg_->upload_cmds_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);

        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteUploadTxCommandsCc::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_upload_cmds_req());

    cc_res_.Reset();
    cc_res_.Value().Clear();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_post_resp();

    {
        const UploadTxCommandsRequest &cmds_req = input_msg->upload_cmds_req();
        uint64_t object_version = cmds_req.object_version();
        uint64_t commit_ts = cmds_req.commit_ts();
        bool has_overwrite = cmds_req.has_overwrite();

        assert(commit_ts > 0);
        cmds_vec_.reserve(cmds_req.cmd_list_size());
        for (int idx = 0; idx < cmds_req.cmd_list_size(); ++idx)
        {
            cmds_vec_.emplace_back(cmds_req.cmd_list(idx));
        }

        const CceAddr_msg &cce_addr_msg = cmds_req.cce_addr();
        cce_addr_.SetCceLock(cce_addr_msg.cce_lock_ptr(),
                             cce_addr_msg.term(),
                             cmds_req.node_group_id(),
                             cce_addr_msg.core_id());

        UploadTxCommandsCc::Reset(&cce_addr_,
                                  input_msg->tx_number(),
                                  input_msg->tx_term(),
                                  object_version,
                                  commit_ts,
                                  &cmds_vec_,
                                  has_overwrite,
                                  &cc_res_);
    }

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

txservice::remote::RemoteDbSizeCc::RemoteDbSizeCc()
{
    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_DBSizeResponse);

    post_lambda_ = [this]()
    {
        assert(total_ref_cnt_ == 0);
        output_msg_.set_handler_addr(input_msg_->handler_addr());

        const DBSizeRequest &req = input_msg_->dbsize_req();
        DBSizeResponse *resp = output_msg_.mutable_db_size_resp();
        for (size_t idx = 0; idx < total_obj_sizes_.size(); ++idx)
        {
            resp->add_node_obj_size(
                total_obj_sizes_[idx]->load(std::memory_order_relaxed));
        }

        resp->set_dbsize_term(req.dbsize_term());

        hd_->SendMessageToNode(req.src_node_id(), output_msg_);

        hd_->RecycleCcMsg(std::move(input_msg_));

        Clear();
    };
}

void txservice::remote::RemoteDbSizeCc::Reset(
    std::unique_ptr<CcMessage> input_msg, size_t core_cnt)
{
    Clear();
    assert(input_msg->has_dbsize_req());

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_db_size_resp();

    assert(table_names_ == nullptr);
    const DBSizeRequest &cmds_req = input_msg->dbsize_req();
    for (int idx = 0; idx < cmds_req.table_name_str_size(); ++idx)
    {
        std::string_view table_name_sv{cmds_req.table_name_str(idx)};
        redis_table_names_.emplace_back(
            table_name_sv,
            ToLocalType::ConvertCcTableType(cmds_req.table_type(idx)));
    }

    DbSizeCc::Reset(&redis_table_names_, core_cnt, 0);
    assert(table_names_ == &redis_table_names_);
    assert(total_ref_cnt_ == core_cnt);
    assert(remote_ref_cnt_ == 0);

    AddLocalNodeGroupId(cmds_req.node_group_id());

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

bool txservice::remote::RemoteDbSizeCc::Execute(CcShard &ccs)
{
    assert(vct_ng_id_.size() == 1);
    for (size_t idx = 0; idx < table_names_->size(); ++idx)
    {
        CcMap *map = ccs.GetCcm(table_names_->at(idx), vct_ng_id_[0]);
        if (map != nullptr)
        {
            total_obj_sizes_[idx]->fetch_add(map->NormalObjectSize(),
                                             std::memory_order_relaxed);
        }
    }

    std::unique_lock lk(mux_);
    assert(remote_ref_cnt_ == 0);
    --total_ref_cnt_;
    if (total_ref_cnt_ == 0)
    {
        table_names_ = nullptr;
        redis_table_names_.clear();
        redis_table_names_.shrink_to_fit();
        post_lambda_();
        return true;
    }

    return false;
}

txservice::remote::RemoteInvalidateTableCacheCc::RemoteInvalidateTableCacheCc()
    : cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::
            CcMessage_MessageType_InvalidateTableCacheResponse);
    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());
        output_msg_.set_handler_addr(input_msg_->handler_addr());

        InvalidateTableCacheResponse *resp =
            output_msg_.mutable_invalidate_table_cache_resp();
        resp->set_error_code(
            ToRemoteType::ConvertCcErrorCode(res->ErrorCode()));

        const InvalidateTableCacheRequest &req =
            input_msg_->invalidate_table_cache_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteInvalidateTableCacheCc::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_invalidate_table_cache_req());

    cc_res_.Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_invalidate_table_cache_resp();

    const InvalidateTableCacheRequest &req =
        input_msg->invalidate_table_cache_req();
    std::string_view table_name_sv(req.table_name_str());
    remote_table_name_ = TableName(
        table_name_sv, ToLocalType::ConvertCcTableType(req.table_type()));

    InvalidateTableCacheCc::Reset(&remote_table_name_,
                                  req.node_group_id(),
                                  input_msg->tx_number(),
                                  input_msg->tx_term(),
                                  &cc_res_);

    input_msg_ = std::move(input_msg);
    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}
