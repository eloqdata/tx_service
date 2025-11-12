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

#include <queue>

#include "glog/logging.h"
#include "proto/cc_request.pb.h"
#include "sharder.h"
#include "tx_command.h"
#include "tx_id.h"
#include "type.h"

namespace txservice
{
struct ApplyCc;
struct StandbyForwardEntry
{
    StandbyForwardEntry()
    {
        msg.set_type(remote::CcMessage::MessageType::
                         CcMessage_MessageType_KeyObjectStandbyForwardRequest);
        msg.mutable_key_obj_standby_forward_req()->set_out_of_sync(false);
        sequence_id_ = UINT64_MAX;
    }

    void AddTxCommand(ApplyCc &cc_req);

    void AddTxCommand(TxCommand *cmd);

    remote::KeyObjectStandbyForwardRequest &Request()
    {
        return *msg.mutable_key_obj_standby_forward_req();
    }

    uint64_t SequenceId() const
    {
        return sequence_id_;
    }

    void SetSequenceId(uint64_t seq_id)
    {
        sequence_id_ = seq_id;
    }

    const remote::CcMessage &Message() const
    {
        return msg;
    }

private:
    uint64_t sequence_id_{UINT64_MAX};
    remote::CcMessage msg;
};

struct StandbySequenceGroup
{
    void Subscribe(uint64_t initial_seq_id)
    {
        next_expecting_standby_sequence_id_ = initial_seq_id;
        initial_sequnce_id_ = initial_seq_id;
        last_consistent_standby_sequence_id_ = initial_seq_id - 1;
        missing_standby_seqeunce_ids_.clear();
        while (!pending_standby_consistent_ts_.empty())
        {
            pending_standby_consistent_ts_.pop();
        }

        last_standby_consistent_ts_ = 0;
        if (finished_stanbdy_req_count_ > 0)
        {
            Sharder::Instance().DecrInflightStandbyReqCount(
                finished_stanbdy_req_count_);
            finished_stanbdy_req_count_ = 0;
        }

        subscribed_ = true;
    }

    void Unsubscribe()
    {
        if (finished_stanbdy_req_count_ > 0)
        {
            Sharder::Instance().DecrInflightStandbyReqCount(
                finished_stanbdy_req_count_);
            finished_stanbdy_req_count_ = 0;
        }

        subscribed_ = false;
    }

    uint64_t initial_sequnce_id_{0};
    // the largest sequencec received + 1
    uint64_t next_expecting_standby_sequence_id_{0};
    // the largest sequence number that we've received all msgs before this
    // sequence number.
    uint64_t last_consistent_standby_sequence_id_{0};
    // Set of missing sequence msgs
    std::set<uint64_t> missing_standby_seqeunce_ids_;
    // The largest ts that is consistent in this seq group.
    uint64_t last_standby_consistent_ts_{0};
    // The pending ts that are not consistent yet.
    std::queue<std::pair<uint64_t, uint64_t>> pending_standby_consistent_ts_;

    uint64_t finished_stanbdy_req_count_{0};
    bool subscribed_{false};
};

void BrocastPrimaryCkptTs(NodeGroupId node_group_id,
                          int64_t node_group_term,
                          uint64_t primary_ckpt_ts);

};  // namespace txservice