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
#include "tx_start_ts_collector.h"

#include <brpc/channel.h>
#include <butil/logging.h>

#include <chrono>

#include "cc/local_cc_shards.h"
#include "sharder.h"

namespace txservice
{
void TxStartTsCollector::Init(LocalCcShards *shards, uint32_t delay_seconds)
{
    active_ = false;
    min_start_ts_ = 1UL;
    local_shards_ = shards;
    delay_seconds_ = std::max(1U, delay_seconds);
    auto all_node_groups = Sharder::Instance().AllNodeGroups();
    for (uint32_t ng_id : *all_node_groups)
    {
        min_start_ts_map_.emplace(ng_id, 1U);
    }

    DLOG(INFO) << "TxStartTsCollector init, interval seconds: "
               << delay_seconds;
}

void TxStartTsCollector::Start()
{
    DLOG(INFO) << "TxStartTsCollector start, interval seconds: "
               << delay_seconds_;
    active_ = true;
    thd_ = std::thread([this] { Run(); });
    pthread_setname_np(thd_.native_handle(), "tx_start_ts_col");
}

void TxStartTsCollector::Shutdown()
{
    {
        std::unique_lock<std::mutex> lk(active_mux_);
        active_ = false;
        active_cv_.notify_one();
    }
    thd_.join();
}

void TxStartTsCollector::Run()
{
    std::unique_lock<std::mutex> lk(active_mux_);

    while (active_)
    {
        active_cv_.wait_for(lk,
                            std::chrono::seconds(delay_seconds_),
                            [this]() { return active_ == false; });
        lk.unlock();
        min_start_ts_ = CollectMinTxStartTs();

        lk.lock();
    }
}

/**
 * @brief Collect the minimal start_ts of all active transactions in all
 * cc node groups. If leader node failed, use the previous minimal start_ts.
 *
 * @return The minimal start_ts
 */
uint64_t TxStartTsCollector::CollectMinTxStartTs()
{
    // collect recyle ts from all ccshards in all cc_node_group
    uint64_t min_start_ts = UINT64_MAX;

    auto all_node_groups = Sharder::Instance().AllNodeGroups();
    for (uint32_t ng_id : *all_node_groups)
    {
        uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

        if (dest_node_id == local_shards_->NodeId())
        {
            min_start_ts_map_[ng_id] = local_shards_->StatsLocalActiveSiTxs();
            continue;
        }
        std::shared_ptr<brpc::Channel> channel =
            Sharder::Instance().GetCcNodeServiceChannel(dest_node_id);
        if (channel == nullptr)
        {
            LOG(ERROR) << "Fail to init the channel to the node("
                       << dest_node_id << ") .";

            continue;
        }

        remote::CcRpcService_Stub stub(channel.get());
        remote::GetMinTxStartTsRequest req;
        req.set_ng_id(ng_id);
        remote::GetMinTxStartTsResponse res;

        brpc::Controller cntl;
        cntl.set_timeout_ms(100);
        cntl.set_max_retry(3);
        stub.GetMinTxStartTs(&cntl, &req, &res, nullptr);

        if (cntl.Failed())
        {
            LOG(ERROR) << "Fail to call the GetMinTxStartTs RPC of node("
                       << dest_node_id << "). Error code: " << cntl.ErrorCode()
                       << ". Msg: " << cntl.ErrorText();
            Sharder::Instance().UpdateCcNodeServiceChannel(dest_node_id,
                                                           channel);
        }
        else
        {
            if (!res.error() && res.term() > 0)
            {
                auto ins_pair = min_start_ts_map_.try_emplace(ng_id);
                ins_pair.first->second = res.ts();
            }
        }
    }

    for (auto it : min_start_ts_map_)
    {
        min_start_ts = std::min(min_start_ts, it.second);
    }
    // LOG(INFO) << "collect min start ts of all active tx :" << min_start_ts;
    return min_start_ts;
}

}  // namespace txservice
