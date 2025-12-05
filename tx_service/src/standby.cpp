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
#include "standby.h"

#include "cc_request.h"
#include "local_cc_shards.h"

namespace txservice
{

void StandbyForwardEntry::AddTxCommand(ApplyCc &cc_req)
{
    auto &req = Request();
    if (cc_req.IsLocal())
    {
        TxCommand *cmd = cc_req.CommandPtr();

        std::string cmd_str;
        cmd->Serialize(cmd_str);
        req.add_cmd_list(std::move(cmd_str));
    }
    else
    {
        req.add_cmd_list(*cc_req.CommandImage());
    }

    assert(cc_req.GetCommand());
    if (cc_req.GetCommand()->IsOverwrite())
    {
        req.set_has_overwrite(true);
    }
}

void StandbyForwardEntry::AddOverWriteCommand(TxCommand *cmd)
{
    assert(cmd->IsOverwrite());
    auto &req = Request();
    req.set_has_overwrite(true);
    req.clear_cmd_list();
    std::string cmd_str;
    cmd->Serialize(cmd_str);
    req.add_cmd_list(std::move(cmd_str));
}

void BrocastPrimaryCkptTs(NodeGroupId node_group_id,
                          int64_t node_group_term,
                          uint64_t primary_ckpt_ts)
{
    std::vector<uint32_t> subscribe_node_ids;
    WaitableCc get_subscribe_node_ids_cc;
    get_subscribe_node_ids_cc.Reset(
        [&subscribe_node_ids](CcShard &ccs)
        {
            subscribe_node_ids = ccs.GetSubscribedStandbys();
            return true;
        });

    Sharder::Instance().GetLocalCcShards()->EnqueueCcRequest(
        0, &get_subscribe_node_ids_cc);
    get_subscribe_node_ids_cc.Wait();

    if (!subscribe_node_ids.empty())
    {
        brpc::Controller cntl;
        remote::UpdateStandbyCkptTsRequest update_standby_ckpt_ts_req;
        remote::UpdateStandbyCkptTsResponse update_standby_ckpt_ts_resp;

        update_standby_ckpt_ts_req.set_ng_term(node_group_term);
        update_standby_ckpt_ts_req.set_node_group_id(node_group_id);
        update_standby_ckpt_ts_req.set_primary_succ_ckpt_ts(primary_ckpt_ts);

        for (uint32_t node_id : subscribe_node_ids)
        {
            auto channel = Sharder::Instance().GetCcNodeServiceChannel(node_id);
            if (channel)
            {
                remote::CcRpcService_Stub stub(channel.get());
                cntl.Reset();
                cntl.set_timeout_ms(300);
                update_standby_ckpt_ts_resp.Clear();

                // We don't care about response
                stub.UpdateStandbyCkptTs(&cntl,
                                         &update_standby_ckpt_ts_req,
                                         &update_standby_ckpt_ts_resp,
                                         nullptr);
            }
        }
    }
}

};  // namespace txservice
