#pragma once

#include <string>
#include <vector>

#include "log_agent.h"
#include "txlog.h"

namespace txservice
{

class MockLogAgent : public txservice::TxLog
{
public:
    MockLogAgent() = delete;
    MockLogAgent(const MockLogAgent &rhs) = delete;

    MockLogAgent(uint32_t lg_cnt, uint32_t lg_rep_num) : log_agent_()
    {
    }

    ~MockLogAgent() = default;

    void WriteLog(uint32_t log_group_id,
                  brpc::Controller *cntl,
                  const ::txlog::LogRequest &log_record,
                  ::txlog::LogResponse &log_response,
                  google::protobuf::Closure &done) override
    {
        log_agent_.WriteLog(
            log_group_id, cntl, &log_record, &log_response, &done);
    }

    void UpdateCheckpointTs(uint32_t cc_node_group_id,
                            int64_t term,
                            uint64_t checkpoint_timestamp) override
    {
        log_agent_.UpdateCheckpointTs(
            cc_node_group_id, term, checkpoint_timestamp);
    }

    void ReplayLog(uint32_t cc_node_group_id,
                   int64_t term,
                   const std::string &source_ip,
                   uint16_t source_port,
                   int log_group,
                   uint64_t start_ts,
                   std::atomic<bool> &interrupt) override
    {
        log_agent_.ReplayLog(cc_node_group_id,
                             term,
                             source_ip,
                             source_port,
                             log_group,
                             start_ts,
                             interrupt);
    }

    txservice::RecoverTxStatus RecoverTx(uint64_t tx_number,
                                         int64_t tx_term,
                                         uint64_t write_lock_ts,
                                         uint32_t cc_ng_id,
                                         int64_t cc_ng_term,
                                         const std::string &source_ip,
                                         uint16_t source_port) override
    {
        ::txlog::RecoverTxResponse_TxStatus status =
            log_agent_.RecoverTx(tx_number,
                                 tx_term,
                                 write_lock_ts,
                                 cc_ng_id,
                                 cc_ng_term,
                                 source_ip,
                                 source_port,
                                 0);

        switch (status)
        {
        case ::txlog::RecoverTxResponse_TxStatus_Alive:
            return txservice::RecoverTxStatus::Alive;
        case ::txlog::RecoverTxResponse_TxStatus_Committed:
            return txservice::RecoverTxStatus::Committed;
        case ::txlog::RecoverTxResponse_TxStatus_NotCommitted:
            return txservice::RecoverTxStatus::NotCommitted;
        default:
            return txservice::RecoverTxStatus::RecoverError;
        }
    }

    void TransferLeader(uint32_t log_group_id, uint32_t leader_idx) override
    {
        log_agent_.TransferLeader(log_group_id, leader_idx);
    }

    uint32_t LogGroupCount() override
    {
        return log_agent_.LogGroupCount();
    }

    uint32_t LogGroupReplicaNum() override
    {
        return log_agent_.LogGroupReplicaNum();
    }

    uint32_t GetLogGroupId(uint64_t tx_number) override
    {
        return tx_number % log_agent_.LogGroupCount();
    }

    void RefreshLeader(uint32_t log_group_id) override
    {
        log_agent_.RefreshLeader(log_group_id);
    }

    /**
     * @brief Initializae the log agent by setup the log request stub using
     * ip_list and port_list.
     */
    void Init(std::vector<std::string> &ip_list,
              std::vector<uint16_t> &port_list,
              const uint32_t start_log_group_id) override
    {
        log_agent_.Init(ip_list, port_list, start_log_group_id);
    }

    void UpdateLeaderCache(uint32_t lg_id, uint32_t node_id) override
    {
        log_agent_.UpdateLeaderCache(lg_id, node_id);
    }

    void CheckMigrationIsFinished(
        uint32_t log_group_id,
        brpc::Controller *cntl,
        const ::txlog::CheckMigrationIsFinishedRequest &request,
        ::txlog::CheckMigrationIsFinishedResponse &response,
        google::protobuf::Closure &done) override
    {
        log_agent_.CheckMigrationIsFinished(
            log_group_id, cntl, &request, &response, &done);
    }

    ::txlog::CheckClusterScaleStatusResponse::Status CheckClusterScaleStatus(
        uint32_t log_group_id, const std::string &id) override
    {
        return log_agent_.CheckClusterScaleStatus(log_group_id, id);
    }

private:
    ::txlog::LogAgent log_agent_;
};

}  // namespace txservice
