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

#include <assert.h>

#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "proto/cc_request.pb.h"
#include "schema.h"
#include "sharder.h"
#include "store/data_store_handler.h"
#include "tx_worker_pool.h"
#include "type.h"

namespace txservice
{
class CcShard;

class CcMapSamplePool
{
public:
    virtual ~CcMapSamplePool() = default;
};

class Distribution
{
public:
    virtual ~Distribution() = default;
    virtual uint64_t Records() const = 0;
    virtual uint64_t Records(const KeySchema *key_schema,
                             const TxKey &min_key,
                             const TxKey &max_key) const = 0;
    virtual std::vector<double> RecordsPerKey() const = 0;
};

class Statistics
{
public:
    static uint16_t LeaderCore(const TableName &table_or_index_name)
    {
        auto hash_code = std::hash<std::string_view>{}(
            table_or_index_name.GetBaseTableNameSV());

        uint32_t residual = hash_code & 0x3FF;
        uint16_t core_id =
            (residual) % Sharder::Instance().GetLocalCcShardsCount();
        return core_id;
    }

    static NodeGroupId LeaderNodeGroup(const TableName &table_or_index_name)
    {
        auto hash_code = std::hash<std::string_view>{}(
            table_or_index_name.GetBaseTableNameSV());

        // Map table statistics to some one node group.
        auto all_ng = Sharder::Instance().AllNodeGroups();
        uint32_t ng_idx = (hash_code >> 10) % all_ng->size();
        uint32_t target_ng = UINT32_MAX;
        for (auto ng_id : *all_ng)
        {
            if (ng_idx == 0)
            {
                target_ng = ng_id;
                break;
            }
            ng_idx--;
        }
        return target_ng;
    }

public:
    virtual ~Statistics() = default;

    virtual const TableName &BaseTableName() const = 0;

    virtual const Distribution *GetDistribution(
        const TableName &table_or_index_name) const = 0;

    virtual void CreateIndex(const TableName &index_name,
                             const KeySchema *key_schema,
                             NodeGroupId cc_ng_id) = 0;

    virtual void DropIndex(const TableName &index_name) = 0;

    virtual void OnRemoteStatisticsMessage(
        const TableName &table_or_index_name,
        const TableSchema *table_schema,
        const remote::NodeGroupSamplePool &remote_sample_pool) = 0;

    virtual std::unique_ptr<remote::NodeGroupSamplePool>
    MakeBroadcastSamplePool(NodeGroupId ng_id,
                            const TableName &table_or_index_name,
                            bool *updated_since_sync) const = 0;

    virtual std::unordered_map<TableName,
                               std::pair<uint64_t, std::vector<TxKey>>>
    MakeStoreStatistics(bool *updated_since_sync) const = 0;

    virtual void SetUpdatedSinceSync() = 0;

    virtual void SetEstimateRecordSize(size_t size) = 0;

    virtual size_t EstimateRecordSize() const = 0;
};

struct StatisticsEntry
{
    // The CcNode::on_leader_stop() method free memory after pinning_threads
    // down to zero.
    //
    // To use cross-threads pointer safely, one can:
    // (1) Protect that pointer with Sharder::TryPinNodeGroupData(). Once
    // pinned, row pointer can be used directly.
    // (2) Use a shared_pointer version of that pointer.
    //
    // Currently, sql threads choose method (1), and other threads choose
    // methods (2).
    std::shared_ptr<Statistics> statistics_{nullptr};
};

}  // namespace txservice
