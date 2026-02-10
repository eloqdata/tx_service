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

#include <butil/logging.h>

#include "cc_protocol.h"
#include "error_messages.h"
#include "tx_execution.h"
#include "tx_request.h"
#include "tx_service.h"

namespace txservice
{
static inline void AbortTx(txservice::TransactionExecution *txm,
                           const std::function<void()> *yield_func = nullptr,
                           const std::function<void()> *resume_func = nullptr)
{
    if (txm == nullptr)
    {
        return;
    }

    // Abort tx request.
    CommitTxRequest req(false, yield_func, resume_func, txm);
    txm->CommitTx(req);
}

static inline std::pair<bool, TxErrorCode> CommitTx(
    txservice::TransactionExecution *txm,
    const std::function<void()> *yield_func = nullptr,
    const std::function<void()> *resume_func = nullptr)
{
    if (txm == nullptr)
    {
        return {true, TxErrorCode::NO_ERROR};
    }

    CommitTxRequest commit_req(true, yield_func, resume_func, txm);
    bool success = txm->CommitTx(commit_req);
    return {success, commit_req.ErrorCode()};
}

static inline TransactionExecution *NewTxInit(
    txservice::TxService *tx_service,
    txservice::IsolationLevel level = txservice::IsolationLevel::ReadCommitted,
    txservice::CcProtocol proto = txservice::CcProtocol::Locking,
    NodeGroupId tx_owner = UINT32_MAX,
    int16_t group_id = -1,
    bool start_now = false,
    const std::function<void()> *yield_fptr = nullptr,
    const std::function<void()> *resume_fptr = nullptr)
{
    assert(tx_service != nullptr);
    txservice::TransactionExecution *txm = nullptr;
#ifdef EXT_TX_PROC_ENABLED
    txm = group_id >= 0 ? tx_service->NewTx(group_id) : tx_service->NewTx();
#else
    txm = tx_service->NewTx();
#endif
    txm->InitTx(level, proto, tx_owner, start_now, yield_fptr, resume_fptr);

    return txm;
}

static inline TxErrorCode TxReadCatalog(TransactionExecution *txm,
                                        ReadTxRequest &read_tx_req,
                                        bool &exists)
{
    assert(txm != nullptr);
    LOG(INFO) << "yf: execute read catalog request";
    txm->Execute(&read_tx_req);
    read_tx_req.Wait();
    LOG(INFO) << "yf: read catalog request finished";
    if (read_tx_req.IsError())
    {
        return read_tx_req.ErrorCode();
    }

    const RecordStatus &rec_status = read_tx_req.Result().first;
    if (rec_status == RecordStatus::Deleted)
    {
        exists = false;
        return TxErrorCode::NO_ERROR;
    }
    else
    {
        assert(rec_status == RecordStatus::Normal);

        CatalogRecord *catalog_rec =
            static_cast<CatalogRecord *>(read_tx_req.rec_);
        if (catalog_rec->Schema())
        {
            exists = true;
            return TxErrorCode::NO_ERROR;
        }
        else
        {
            return TxErrorCode::UNDEFINED_ERR;
        }
    }
}

static inline int GetDbIndex(const TableName *table_name)
{
    std::string_view table_name_sv = table_name->StringView();
    unsigned int db_idx = table_name_sv.back() - '0';
    if (table_name_sv[table_name_sv.size() - 2] != '_')
    {
        db_idx = (table_name_sv[table_name_sv.size() - 2] - '0') * 10 + db_idx;
    }
    assert(db_idx < RedisDBCnt);
    return db_idx;
}

class BackupUtil
{
public:
    enum struct BackupResult : int8_t
    {
        Running = 0,
        Failed,
        Finished
    };

    static BackupResult CreateBackup(
        const std::string &backup_name,
        const std::string &dest_path,
        const std::string &dest_host,
        const std::string &dest_user,
        std::unordered_map<NodeGroupId, txservice::remote::FetchBackupResponse>
            &backup_responses)
    {
        auto all_node_groups = Sharder::Instance().AllNodeGroups();

        std::vector<std::unique_ptr<txservice::remote::CreateBackupRequest>>
            req_vec;
        std::vector<std::unique_ptr<txservice::remote::FetchBackupResponse>>
            resp_vec;
        std::vector<std::unique_ptr<brpc::Controller>> cntl_vec;
        std::unordered_map<uint32_t, uint32_t> sent_nodes;
        bool failed = false;
        for (uint32_t ng_id : *all_node_groups)
        {
            auto leader_node = Sharder::Instance().LeaderNodeId(ng_id);
            std::shared_ptr<brpc::Channel> channel =
                Sharder::Instance().GetCcNodeServiceChannel(leader_node);
            if (channel == nullptr)
            {
                LOG(ERROR) << "Fail to init the channel to the node("
                           << leader_node << ") .";

                failed = true;
                backup_responses.try_emplace(
                    ng_id, txservice::remote::FetchBackupResponse());
                backup_responses.at(ng_id).set_ng_id(ng_id);
                backup_responses.at(ng_id).set_status(
                    txservice::remote::BackupTaskStatus::Failed);
                break;
            }

            sent_nodes.try_emplace(ng_id, leader_node);

            backup_responses.try_emplace(
                ng_id, txservice::remote::FetchBackupResponse());
            backup_responses.at(ng_id).set_ng_id(ng_id);
            backup_responses.at(ng_id).set_status(
                txservice::remote::BackupTaskStatus::Unknown);
            req_vec.emplace_back(
                std::make_unique<txservice::remote::CreateBackupRequest>());
            resp_vec.emplace_back(
                std::make_unique<txservice::remote::FetchBackupResponse>());
            cntl_vec.emplace_back(std::make_unique<brpc::Controller>());

            auto *req = req_vec.back().get();
            auto *resp = resp_vec.back().get();
            auto *cntl = cntl_vec.back().get();

            req->set_ng_id(ng_id);
            // req->set_node_id(node.node_id_);
            req->set_backup_name(backup_name);
            req->set_dest_path(dest_path);
            req->set_dest_host(dest_host);
            req->set_dest_user(dest_user);
            cntl->set_timeout_ms(1000);
            cntl->set_max_retry(2);
            txservice::remote::CcRpcService_Stub stub(channel.get());
            stub.CreateBackup(cntl, req, resp, brpc::DoNothing());
        }

        for (auto &ref : cntl_vec)
        {
            // wait all rpc call
            brpc::Join(ref->call_id());
        }

        for (size_t i = 0; i < resp_vec.size(); i++)
        {
            // handler results
            auto *resp = resp_vec.at(i).get();
            auto *cntl = cntl_vec.at(i).get();

            uint32_t ng_id = req_vec.at(i)->ng_id();

            if (cntl->Failed())
            {
                DLOG(INFO) << "CreateBackup rpc call failed, "
                           << cntl->ErrorText();
                backup_responses.at(ng_id).set_status(
                    txservice::remote::BackupTaskStatus::Failed);
                failed = true;
                sent_nodes.erase(ng_id);
            }
            else
            {
                DLOG(INFO) << "CreateBackup ng# " << ng_id
                           << ", result:" << resp->status();
                backup_responses.at(ng_id).set_ng_id(ng_id);
                backup_responses.at(ng_id).set_status(resp->status());
                for (const auto &f : resp->backup_files())
                {
                    backup_responses.at(ng_id).add_backup_files(f);
                }
                backup_responses.at(ng_id).set_backup_ts(resp->backup_ts());
            }
        }

        if (failed && !sent_nodes.empty())
        {
            TerminateBackup(backup_name, sent_nodes);
        }

        return failed ? BackupResult::Failed : BackupResult::Running;
    }

    static BackupResult GetBackupStatus(
        const std::string &backup_name,
        std::unordered_map<NodeGroupId, txservice::remote::FetchBackupResponse>
            &backup_responses)
    {
        auto all_node_groups = Sharder::Instance().AllNodeGroups();

        std::vector<std::unique_ptr<txservice::remote::FetchBackupRequest>>
            req_vec;
        std::vector<std::unique_ptr<txservice::remote::FetchBackupResponse>>
            resp_vec;
        std::vector<std::unique_ptr<brpc::Controller>> cntl_vec;

        bool failed = false;
        for (uint32_t ng_id : *all_node_groups)
        {
            backup_responses.try_emplace(
                ng_id, txservice::remote::FetchBackupResponse());
            backup_responses.at(ng_id).set_ng_id(ng_id);
            backup_responses.at(ng_id).set_status(
                txservice::remote::BackupTaskStatus::Unknown);
            auto leader_node = Sharder::Instance().LeaderNodeId(ng_id);
            std::shared_ptr<brpc::Channel> channel =
                Sharder::Instance().GetCcNodeServiceChannel(leader_node);
            if (channel == nullptr)
            {
                LOG(ERROR) << "Fail to init the channel to the node("
                           << leader_node << ") .";
                failed = true;
                backup_responses.at(ng_id).set_status(
                    txservice::remote::BackupTaskStatus::Failed);
                continue;
            }

            req_vec.emplace_back(
                std::make_unique<txservice::remote::FetchBackupRequest>());
            resp_vec.emplace_back(
                std::make_unique<txservice::remote::FetchBackupResponse>());
            cntl_vec.emplace_back(std::make_unique<brpc::Controller>());

            auto *req = req_vec.back().get();
            auto *resp = resp_vec.back().get();
            auto *cntl = cntl_vec.back().get();

            req->set_ng_id(ng_id);
            // req->set_node_id(node.node_id_);
            req->set_backup_name(backup_name);
            cntl->set_timeout_ms(1000);
            cntl->set_max_retry(2);
            txservice::remote::CcRpcService_Stub stub(channel.get());
            stub.FetchBackup(cntl, req, resp, brpc::DoNothing());
        }

        for (auto &ref : cntl_vec)
        {
            // wait all rpc call
            brpc::Join(ref->call_id());
        }
        bool finished = true;
        for (size_t i = 0; i < resp_vec.size(); i++)
        {
            // handler results
            auto *resp = resp_vec.at(i).get();
            auto *cntl = cntl_vec.at(i).get();

            uint32_t ng_id = req_vec.at(i)->ng_id();

            if (cntl->Failed())
            {
                DLOG(INFO) << "FetchBackup rpc call failed, "
                           << cntl->ErrorText();
                backup_responses.at(ng_id).set_status(
                    ::txservice::remote::BackupTaskStatus::Unknown);
                failed = true;
            }
            else
            {
                auto st = resp->status();
                backup_responses.at(ng_id).set_ng_id(ng_id);
                backup_responses.at(ng_id).set_status(st);
                if (st == remote::BackupTaskStatus::Failed)
                {
                    failed = true;
                }
                else if (st != remote::BackupTaskStatus::Finished)
                {
                    finished = false;
                }
                else
                {
                    for (const auto &f : resp->backup_files())
                    {
                        backup_responses.at(ng_id).add_backup_files(f);
                    }
                    backup_responses.at(ng_id).set_backup_ts(resp->backup_ts());
                }
            }
        }

        if (failed)
        {
            return BackupResult::Failed;
        }
        else if (finished)
        {
            return BackupResult::Finished;
        }
        return BackupResult::Running;
    }

    static void TerminateBackup(const std::string &backup_name,
                                std::unordered_map<NodeGroupId, NodeId> &nodes)
    {
        std::vector<std::unique_ptr<txservice::remote::TerminateBackupRequest>>
            req_vec;
        std::vector<std::unique_ptr<txservice::remote::TerminateBackupResponse>>
            resp_vec;
        std::vector<std::unique_ptr<brpc::Controller>> cntl_vec;

        for (auto &[ng_id, node_id] : nodes)
        {
            std::shared_ptr<brpc::Channel> channel =
                Sharder::Instance().GetCcNodeServiceChannel(node_id);
            if (channel == nullptr)
            {
                LOG(ERROR) << "Fail to init the channel to the node(" << node_id
                           << ") .";
                continue;
            }

            req_vec.emplace_back(
                std::make_unique<txservice::remote::TerminateBackupRequest>());
            resp_vec.emplace_back(
                std::make_unique<txservice::remote::TerminateBackupResponse>());
            cntl_vec.emplace_back(std::make_unique<brpc::Controller>());

            auto *req = req_vec.back().get();
            auto *resp = resp_vec.back().get();
            auto *cntl = cntl_vec.back().get();

            req->set_ng_id(ng_id);
            req->set_backup_name(backup_name);
            cntl->set_timeout_ms(1000);
            cntl->set_max_retry(2);
            txservice::remote::CcRpcService_Stub stub(channel.get());
            stub.TerminateBackup(cntl, req, resp, brpc::DoNothing());
        }

        for (auto &ref : cntl_vec)
        {
            // wait all rpc call
            brpc::Join(ref->call_id());
        }

        // Does not care about the returned result
        for (size_t i = 0; i < resp_vec.size(); i++)
        {
            // handler results
            auto *cntl = cntl_vec.at(i).get();

            uint32_t ng_id = req_vec.at(i)->ng_id();

            if (cntl->Failed())
            {
                DLOG(INFO) << "Terminate Backup ng#" << ng_id
                           << " rpc call failed, " << cntl->ErrorText();
            }
            else
            {
                DLOG(INFO) << "Terminate Backup ng# " << ng_id;
            }
        }
    }
};

}  // namespace txservice
