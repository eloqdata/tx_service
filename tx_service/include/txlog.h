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

#include <optional>
#include <string>
#include <vector>

#include "log.pb.h"

namespace txservice
{
enum struct RecoverTxStatus
{
    // The cc node in which the tx resides has not failed over to a new term.
    // The tx is considered to be still alive. No recovery is needed. Wait for
    // the tx to make futher actions.
    Alive = 0,
    // The cc node in which the tx resides has failed. There is a committed log
    // record from the tx in the log state machine.
    Committed,
    // The cc node in which the tx resides has failed. No log record from the tx
    // is found in the log state machine. The tx is deemed not committed and it
    // cannot commit in future (because there will be term mismatches for the
    // intentions/locks the tx holds).
    NotCommitted,
    // An error occurred when checking the tx status during recovery.
    RecoverError
};

class TxLog
{
public:
    virtual ~TxLog() = default;

    // Persists and replicates a log record in the specified log group.
    virtual void WriteLog(uint32_t log_group_id,
                          brpc::Controller *cntl,
                          const ::txlog::LogRequest &log_record,
                          ::txlog::LogResponse &log_response,
                          google::protobuf::Closure &done) = 0;

    virtual void CheckMigrationIsFinished(
        uint32_t log_group_id,
        brpc::Controller *cntl,
        const ::txlog::CheckMigrationIsFinishedRequest &request,
        ::txlog::CheckMigrationIsFinishedResponse &response,
        google::protobuf::Closure &done) = 0;

    virtual ::txlog::CheckClusterScaleStatusResponse::Status
    CheckClusterScaleStatus(uint32_t log_group_id, const std::string &id) = 0;

    /**
     * @brief Sends a change-data scan (CdcScan) to the log group's cached
     * leader, asynchronously.
     *
     * The default fails the call at once, for a log without change data
     * capture.
     *
     * @param log_group_id The log group to scan.
     * @param cntl The call's controller; its timeout bounds the call.
     * @param request The scan request.
     * @param[out] response The scan response.
     * @param[out] node_id Set to the node ID the call is sent to, before it
     * is sent; left empty when no leader is known and the call fails at once.
     * A NotLeader redirect is compared with it by node ID, looked up with
     * LogNodeByAddress, rather than by address format.
     * @param done Run exactly once when the call completes, possibly before
     * this returns.
     */
    virtual void CdcScan(uint32_t log_group_id,
                         brpc::Controller *cntl,
                         const ::txlog::CdcScanRequest &request,
                         ::txlog::CdcScanResponse &response,
                         std::optional<uint32_t> *node_id,
                         google::protobuf::Closure &done)
    {
        (void) log_group_id;
        (void) request;
        (void) response;
        (void) node_id;
        cntl->SetFailed("CdcScan is not supported by this log");
        done.Run();
    }

    /**
     * @brief The node ID configured with the given "host:port" in the log
     * group, as a CdcScan NotLeader response's redirect names it.
     *
     * Following a redirect is then UpdateLeaderCache with that node ID. The
     * default finds none, for a log without a node configuration.
     *
     * @param log_group_id The log group.
     * @param address A "host:port".
     * @return The node ID, or empty if no node ID of the group is configured
     * with that address.
     */
    virtual std::optional<uint32_t> LogNodeByAddress(uint32_t log_group_id,
                                                     const std::string &address)
    {
        (void) log_group_id;
        (void) address;
        return std::nullopt;
    }

    // Invoked by a cc node group's leader to update its checkpoint
    // timestamp in all log groups (so that the transaction logs before
    // checkpoint timestamp can be ignored and truncated).
    virtual void UpdateCheckpointTs(uint32_t cc_node_group_id,
                                    int64_t term,
                                    uint64_t checkpoint_timestamp) = 0;

    // Invoked by a cc node group's leader to remove cc node group in all log
    // groups on cluster scaling down.
    virtual void RemoveCcNodeGroup(uint32_t cc_node_group_id, int64_t term) = 0;

    /**
     * @brief Asks one log group, once, to register a failing-over node group's
     * new term and replay its log to the node (LogAgent::ReplayLogOnce).
     *
     * A bounded attempt: the caller learns whether the log group got the
     * request from the replay stream it opens, and asks again if none comes.
     *
     * @param cc_node_group_id The recovering node group.
     * @param term The node group's new term.
     * @param source_ip The address of the node's log replay service.
     * @param source_port The port of the node's log replay service.
     * @param log_group_id The log group.
     * @param start_ts The commit timestamp to replay from at the latest.
     * @return true if the log group's leader accepted the request.
     */
    virtual bool ReplayLog(uint32_t cc_node_group_id,
                           int64_t term,
                           const std::string &source_ip,
                           uint16_t source_port,
                           uint32_t log_group_id,
                           uint64_t start_ts) = 0;

    virtual RecoverTxStatus RecoverTx(uint64_t tx_number,
                                      int64_t tx_term,
                                      uint64_t write_lock_ts,
                                      uint32_t cc_ng_id,
                                      int64_t cc_ng_term,
                                      const std::string &source_ip,
                                      uint16_t source_port) = 0;

    virtual void TransferLeader(uint32_t log_group_id, uint32_t leader_idx) = 0;

    virtual uint32_t LogGroupCount() = 0;

    virtual uint32_t LogGroupReplicaNum() = 0;

    virtual uint32_t GetLogGroupId(uint64_t tx_number) = 0;

    virtual void RefreshLeader(uint32_t log_group_id) = 0;

    virtual void RequestRefreshLeader(uint32_t log_group_id) = 0;

    virtual void Init(std::vector<std::string> &ips,
                      std::vector<uint16_t> &ports,
                      const uint32_t start_log_group_id) = 0;
    virtual void UpdateLeaderCache(uint32_t lg_id, uint32_t node_id) = 0;
};
}  // namespace txservice
