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

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog_factory.h"
#include "distribution_steps.h"
#include "local_cc_shards.h"
#include "proto/cc_request.pb.h"
#include "random_pairing.h"
#include "schema.h"
#include "sharder.h"
#include "statistics.h"
#include "tx_key.h"
#include "type.h"

namespace txservice
{
template <typename KeyT>
struct SamplePoolParam
{
    std::vector<KeyT> sample_keys_{};
    uint64_t records_{0};
};

template <typename KeyT>
class TableStatistics;

template <typename KeyT>
class TemplateCcMapSamplePool : public CcMapSamplePool
{
public:
    struct CopyKey
    {
        constexpr void operator()(KeyT &lhs, const KeyT &rhs) const
        {
            lhs.Copy(rhs);
        }
    };

    static constexpr uint32_t sample_pool_capacity = 1024;
    using SamplePool = RandomPairing<KeyT, CopyKey>;

private:
    struct alignas(64) PerCcShardVar
    {
        std::atomic<int64_t> records_{0};
        std::atomic<int64_t> upserts_{0};
        std::atomic<bool> updated_since_sync_{false};
    };

public:
    TemplateCcMapSamplePool(const TableName *table_or_index_name,
                            NodeGroupId ng_id,
                            TableStatistics<KeyT> *statistics)
        : table_or_index_name_(table_or_index_name),
          ng_id_(ng_id),
          cc_shards_var_(Sharder::Instance().GetLocalCcShardsCount()),
          sample_pool_(sample_pool_capacity),
          statistics_(statistics)
    {
    }

    TemplateCcMapSamplePool(const TableName *table_or_index_name,
                            NodeGroupId ng_id,
                            const SamplePoolParam<KeyT> &param,
                            TableStatistics<KeyT> *statistics)
        : table_or_index_name_(table_or_index_name),
          ng_id_(ng_id),
          cc_shards_var_(Sharder::Instance().GetLocalCcShardsCount()),
          sample_pool_(param.sample_keys_, sample_pool_capacity),
          statistics_(statistics)
    {
        SetRecords(param.records_);
        assert(Records() >= static_cast<int64_t>(sample_pool_.Size()));
    }

    /**
     * sample_pool_mux_ is non-copyable and non-movable. To put
     * TemplateCcMapSamplePool into unordered_map, move-constructor or
     * move-assignment is required.
     */
    TemplateCcMapSamplePool(TemplateCcMapSamplePool &&rhs)
        : table_or_index_name_(rhs.table_or_index_name_),
          ng_id_(rhs.ng_id_),
          records_(rhs.records_.load(std::memory_order_acquire)),
          cc_shards_var_(std::move(rhs.cc_shards_var_)),
          sample_pool_(std::move(rhs.sample_pool_))
    {
    }

    TemplateCcMapSamplePool &operator=(TemplateCcMapSamplePool &&rhs)
    {
        if (this != &rhs)
        {
            table_or_index_name_ = rhs.table_or_index_name_;
            ng_id_ = rhs.ng_id_;
            cc_shards_var_ = std::move(rhs.cc_shards_var_);
            records_ = rhs.records_.load(std::memory_order_acquire);
            sample_pool_ = std::move(rhs.sample_pool_);
        }
        return *this;
    }

    void Reset(SamplePool &&sample_pool,
               uint64_t records,
               const KeySchema *key_schema)
    {
        std::unique_lock lk(sample_pool_mux_);

        SetRecords(records);
        assert(sample_pool.Capacity() == sample_pool_.Capacity());
        sample_pool_ = std::move(sample_pool);

        std::vector<std::unique_lock<std::mutex>> lk_others =
            statistics_->TryLockSamplePoolsBesides(*table_or_index_name_,
                                                   ng_id_);
        if (AllLocked(lk_others))
        {
            statistics_->RebuildDistribution(*table_or_index_name_, key_schema);
            ClearUpserts();
        }
    }

    void OnInsert(const CcShard &ccs,
                  const KeyT &key,
                  const KeySchema *key_schema)
    {
        std::unique_lock<std::mutex> lk(sample_pool_mux_, std::defer_lock);

        bool on_all =
            RunOnAllCcShards(records_.load(std::memory_order_acquire));
        bool on_leader =
            Statistics::LeaderCore(*table_or_index_name_) == ccs.core_id_;
        if (on_all || on_leader)
        {
            // As long as there is no contention, there are no system calls
            // made. Just a few atomic instructs.
            lk.lock();
        }

        PerCcShardVar &v = cc_shards_var_[ccs.core_id_];
        v.records_++;
        v.upserts_++;
        v.updated_since_sync_ = true;

        if (on_all)
        {
            // For small table, alter atomic counter directly.
            assert(lk.owns_lock());
            records_.fetch_add(1, std::memory_order_relaxed);
            statistics_->AccumulateRecords(*table_or_index_name_);
        }
        else
        {
            // For large table, accumulate per core counter.
            if (NeedAccumulateRecords(v.records_))
            {
                int64_t records = AccumulateRecords(cc_shards_var_);
                records_.store(records, std::memory_order_release);
                statistics_->AccumulateRecords(*table_or_index_name_);
            }
        }

        if (NeedAccumulateUpserts(v.records_, v.upserts_))
        {
            int64_t upserts = AccumulateUpserts(cc_shards_var_);
            upserts_.store(upserts, std::memory_order_release);
        }

        if (on_all || on_leader)
        {
            sample_pool_.Insert(key, records_.load(std::memory_order_acquire));

            if (NeedRebuildDistribution(
                    records_.load(std::memory_order_acquire),
                    upserts_.load(std::memory_order_acquire)))
            {
                std::vector<std::unique_lock<std::mutex>> lk_others =
                    statistics_->TryLockSamplePoolsBesides(
                        *table_or_index_name_, ng_id_);
                if (AllLocked(lk_others))
                {
                    statistics_->RebuildDistribution(*table_or_index_name_,
                                                     key_schema);
                    ClearUpserts();
                }
            }
        }
    }

    void OnDelete(const CcShard &ccs,
                  const KeyT &key,
                  const KeySchema *key_schema)
    {
        std::unique_lock<std::mutex> lk(sample_pool_mux_, std::defer_lock);

        bool on_all =
            RunOnAllCcShards(records_.load(std::memory_order_acquire));
        bool on_leader =
            Statistics::LeaderCore(*table_or_index_name_) == ccs.core_id_;
        if (on_all || on_leader)
        {
            // As long as there is no contention, there are no system calls
            // made. Just a few atomic instructs.
            lk.lock();
        }

        PerCcShardVar &v = cc_shards_var_[ccs.core_id_];

        if (v.records_ > 0)
        {
            v.records_--;
            v.upserts_++;
            v.updated_since_sync_ = true;

            if (on_all)
            {
                // For small table, alter atomic counter directly.
                assert(lk.owns_lock());
                if (records_ > 0)
                {
                    records_.fetch_sub(1, std::memory_order_relaxed);
                    statistics_->AccumulateRecords(*table_or_index_name_);
                }
            }
            else
            {
                // For large table, accumulate per core counter.
                if (NeedAccumulateRecords(v.records_))
                {
                    int64_t records = AccumulateRecords(cc_shards_var_);
                    records_.store(records, std::memory_order_release);
                    statistics_->AccumulateRecords(*table_or_index_name_);
                }
            }

            if (NeedAccumulateUpserts(v.records_, v.upserts_))
            {
                int64_t upserts = AccumulateUpserts(cc_shards_var_);
                upserts_.store(upserts, std::memory_order_release);
            }

            if (on_all || on_leader)
            {
                sample_pool_.Delete(key,
                                    records_.load(std::memory_order_acquire));

                if (NeedRebuildDistribution(
                        records_.load(std::memory_order_acquire),
                        upserts_.load(std::memory_order_acquire)))
                {
                    std::vector<std::unique_lock<std::mutex>> lk_others =
                        statistics_->TryLockSamplePoolsBesides(
                            *table_or_index_name_, ng_id_);
                    if (AllLocked(lk_others))
                    {
                        statistics_->RebuildDistribution(*table_or_index_name_,
                                                         key_schema);
                        ClearUpserts();
                    }
                }
            }
        }
    }

    const TableName &GetTableOrIndexName() const
    {
        return *table_or_index_name_;
    }

    NodeGroupId GetNodeGroupId() const
    {
        return ng_id_;
    }

    int64_t Records() const
    {
        return records_.load(std::memory_order_acquire);
    }

    std::pair<std::unique_lock<std::mutex>, const std::vector<KeyT> *>
    LockedSampleKeys() const
    {
        std::unique_lock lk(sample_pool_mux_);
        return std::make_pair(std::move(lk), &sample_pool_.SampleKeys());
    }

    std::unique_lock<std::mutex> Lock()
    {
        return std::unique_lock(sample_pool_mux_);
    }

    std::unique_lock<std::mutex> TryLock()
    {
        return std::unique_lock(sample_pool_mux_, std::try_to_lock);
    }

    const std::vector<KeyT> &SampleKeys() const
    {
        return sample_pool_.SampleKeys();
    }

    size_t SampleCount() const
    {
        std::unique_lock lk(sample_pool_mux_);
        return sample_pool_.SampleKeys().size();
    }

    void Merge(const SamplePoolParam<KeyT> &param)
    {
        std::unique_lock lk(sample_pool_mux_);
        int64_t records = records_.load(std::memory_order_acquire);
        for (const KeyT &sample_key : param.sample_keys_)
        {
            sample_pool_.Insert(
                sample_key,
                std::min(static_cast<int64_t>(sample_pool_.Size() * 2),
                         records));
        }
        records += param.records_;
        sample_pool_.ClearCounter();

        records = std::max(records, static_cast<int64_t>(sample_pool_.Size()));
        records_.store(records, std::memory_order_release);
        SetUpdatedSinceSync();
    }

    void Prune(const SamplePoolParam<KeyT> &param)
    {
        std::unique_lock lk(sample_pool_mux_);
        int64_t records = records_.load(std::memory_order_acquire);
        for (const KeyT &sample_key : param.sample_keys_)
        {
            sample_pool_.Delete(sample_key, records);
        }
        records -= std::min(records, static_cast<int64_t>(param.records_));
        sample_pool_.ClearCounter();

        records = std::max(records, static_cast<int64_t>(sample_pool_.Size()));
        records_.store(records, std::memory_order_release);
        SetUpdatedSinceSync();
    }

    void To(remote::NodeGroupSamplePool *remote_ccmap_sample_pool) const
    {
        std::unique_lock lk(sample_pool_mux_);
        remote_ccmap_sample_pool->set_ng_id(ng_id_);
        for (const KeyT &key : sample_pool_.SampleKeys())
        {
            std::string sample;
            key.Serialize(sample);
            remote_ccmap_sample_pool->add_samples(std::move(sample));
        }
        remote_ccmap_sample_pool->set_records(std::max(
            Records(),
            static_cast<int64_t>(remote_ccmap_sample_pool->samples_size())));
    }

    // Get value of updated_since_sync_, and set it to false atomically.
    bool ResetGetUpdatedSinceSync()
    {
        bool updated = false;
        for (PerCcShardVar &v : cc_shards_var_)
        {
            // If updated_since_sync_ is equal to expected, set
            // updated_since_sync_ to false; else set expected to
            // updated_since_sync_.
            bool expected = true;
            v.updated_since_sync_.compare_exchange_strong(expected, false);
            updated = updated || expected;
        }
        return updated;
    }

private:
    void SetRecords(int64_t records)
    {
        size_t shard_cnt = Sharder::Instance().GetLocalCcShardsCount();
        int n = records / shard_cnt;
        int m = records % shard_cnt;
        for (size_t i = 0; i < shard_cnt; i++)
        {
            PerCcShardVar &v = cc_shards_var_[i];
            v.records_ = n;
            if (i < static_cast<size_t>(m))
            {
                v.records_++;
            }
        }
        records_.store(records, std::memory_order_release);
    }

    void ClearUpserts()
    {
        for (PerCcShardVar &v : cc_shards_var_)
        {
            v.upserts_ = 0;
        }

        upserts_.store(0, std::memory_order_release);
    }

    void SetUpdatedSinceSync()
    {
        for (PerCcShardVar &v : cc_shards_var_)
        {
            v.updated_since_sync_.store(true, std::memory_order_release);
        }
    }

private:
    bool NeedAccumulateRecords(int64_t records)
    {
        return records <= sample_pool_.Capacity() ||
               static_cast<uint64_t>(records) %
                       Sharder::Instance().GetLocalCcShardsCount() ==
                   0;
    }

    bool NeedAccumulateUpserts(int64_t records, int64_t upserts)
    {
        return records <= sample_pool_.Capacity() ||
               static_cast<uint64_t>(upserts) %
                       Sharder::Instance().GetLocalCcShardsCount() ==
                   0;
    }

    static int64_t AccumulateRecords(
        const std::vector<PerCcShardVar> &cc_shards_var)
    {
        int64_t records = 0;
        for (const PerCcShardVar &v : cc_shards_var)
        {
            records += v.records_;
        }
        return records;
    }

    static int64_t AccumulateUpserts(
        const std::vector<PerCcShardVar> &cc_shards_var)
    {
        int64_t upserts = 0;
        for (const PerCcShardVar &v : cc_shards_var)
        {
            upserts += v.upserts_;
        }
        return upserts;
    }

    static bool NeedRebuildDistribution(int64_t records, int64_t upserts)
    {
        return upserts > records / 16;
    }

    static bool AllLocked(
        const std::vector<std::unique_lock<std::mutex>> &lk_vec)
    {
        return std::all_of(lk_vec.begin(),
                           lk_vec.end(),
                           [](const std::unique_lock<std::mutex> &lk)
                           { return lk.owns_lock(); });
    }

    // For small tables, do sampling/record on all cc_shards.
    // For large tables, do sampling on a assigned cc_shard.
    // For large tables, do record on per cc_shard.
    bool RunOnAllCcShards(int64_t records)
    {
        return records <= sample_pool_.Capacity();
    }

    // Points to key of index_sample_pool_map_.
    const TableName *table_or_index_name_;

    NodeGroupId ng_id_{0};

    // Table records sharding to the node group.
    std::atomic<int64_t> records_{0};

    // upserts = inserts + deletes.
    // It is used to determine whether to rebuild distribution.
    std::atomic<int64_t> upserts_{0};

    // Above two counters are central. If every cc_shard modifies them directly,
    // competition may happened. Consider both counters are allowed to be
    // rounded for large tables, let every cc_shard holds local counters, and
    // accumulate them frequently.
    std::vector<PerCcShardVar> cc_shards_var_;

    // Always sample_pool_.Size() <= Records().
    SamplePool sample_pool_;

    // To make index distribution and table records as stable as possible, do
    // sampling on all cc_shards for small tables, and do sampling on binding
    // cc_shard for large tables.
    mutable std::mutex sample_pool_mux_;

    TableStatistics<KeyT> *statistics_{nullptr};
};

template <typename KeyT>
class IndexDistribution : public Distribution
{
public:
    explicit IndexDistribution(const KeySchema *key_schema)
        : records_(0),
          distribution_steps_(),
          rec_per_key_(key_schema->ExtendKeyParts(), 0.0)
    {
    }

    IndexDistribution(const KeySchema *key_schema,
                      uint64_t records,
                      const std::set<const KeyT *, PtrLessThan<KeyT>> &keys)
        : records_(std::max(records, keys.size())),
          distribution_steps_(keys),
          rec_per_key_(key_schema->ExtendKeyParts(), 0.0)
    {
        CalRecordsPerKey(key_schema, keys);
    }

    void SetRecords(uint64_t records)
    {
        records_.store(records, std::memory_order_release);
    }

    uint64_t Records() const override
    {
        return records_.load(std::memory_order_acquire);
    }

    uint64_t Records(const KeySchema *key_schema,
                     const TxKey &min_key,
                     const TxKey &max_key) const override
    {
        std::shared_lock<std::shared_mutex> slk(shared_mutex_);

        uint64_t records = 0;

        if (distribution_steps_.Available())
        {
            records = records_.load(std::memory_order_acquire) *
                      distribution_steps_.Selectivity(key_schema,
                                                      *min_key.GetKey<KeyT>(),
                                                      *max_key.GetKey<KeyT>());
        }
        else
        {
            records = records_.load(std::memory_order_acquire) / 100;
        }

        return records;
    }

    void Rebuild(const KeySchema *key_schema,
                 uint64_t records,
                 const std::set<const KeyT *, PtrLessThan<KeyT>> &keys)
    {
        std::unique_lock<std::shared_mutex> ulk(shared_mutex_);
        records_.store(std::max(records, keys.size()),
                       std::memory_order_release);
        distribution_steps_ = DistributionSteps<KeyT>(keys);
        CalRecordsPerKey(key_schema, keys);
    }

    std::vector<double> RecordsPerKey() const override
    {
        std::shared_lock<std::shared_mutex> slk(shared_mutex_);
        return rec_per_key_;
    }

private:
    // The algorithm is modified from rocksdb/Rdb_tbl_card_coll::ProcessKey()
    void CalRecordsPerKey(
        const KeySchema *key_schema,
        const std::set<const KeyT *, PtrLessThan<KeyT>> &sample_keys)
    {
        size_t key_parts = key_schema->ExtendKeyParts();
        std::vector<size_t> distinct_keys_per_prefix(key_parts, 0);
        const KeyT *last_key = nullptr;

        for (const KeyT *key : sample_keys)
        {
            size_t start_column_diff = 0;
            if (last_key == nullptr ||
                key_schema->CompareKeys(
                    TxKey(last_key), TxKey(key), &start_column_diff))
            {
                assert(start_column_diff < key_parts);
                for (size_t i = start_column_diff; i < key_parts; i++)
                {
                    distinct_keys_per_prefix[i]++;
                }
                if (start_column_diff < key_parts)
                {
                    // Keep same with rocksdb's logic.
                    //
                    // This branch is a little confusing.
                    // Tests show that key_parts include pk column and pk column
                    // is always different. It seems that this branch would
                    // always enter.
                    last_key = key;
                }
            }
        }

        // Scale up count of distinct keys from sample set to total set
        std::vector<size_t> distinct_keys_per_prefix_all(key_parts, 1.0);

        uint64_t records = records_.load(std::memory_order_acquire);
        size_t sample_keys_count = sample_keys.size();

        if (sample_keys_count > 1)
        {
            // Define NDV: number of distinct values.
            // 1) For unique keys, sample_pool's NDV == sample_pool's size(S).
            // 2) For completely same keys, sample_pool's NDV == 1.
            //
            // To scale up to total set:
            //   For 1) transformed NDV should be table size(T).
            //   For 2) transformed NDV should be 1.
            //
            // Also, guarantee NDV >= 1 always.
            //
            // Thus:
            //            T - 1
            //   NDV' = --------- * (NDV - 1) + 1
            //            S - 1
            for (size_t i = 0; i < key_parts; i++)
            {
                assert(records >= 1);
                assert(distinct_keys_per_prefix[i] >= 1);
                distinct_keys_per_prefix_all[i] =
                    ((records - 1) * (distinct_keys_per_prefix[i] - 1) /
                     static_cast<double>(sample_keys_count - 1)) +
                    1;
            }
        }

        for (size_t i = 0; i < key_parts; i++)
        {
            rec_per_key_[i] = records > 0 ? static_cast<double>(records) /
                                                distinct_keys_per_prefix_all[i]
                                          : 1;
            assert(rec_per_key_[i] >= 1);
        }
    }

private:
    std::atomic<uint64_t> records_{0};

    mutable std::shared_mutex shared_mutex_;
    DistributionSteps<KeyT> distribution_steps_;

    std::vector<double> rec_per_key_;  // Guarantee records per key >= 1 always.
};

template <typename KeyT>
class TableStatistics : public Statistics
{
public:
    TableStatistics(const TableSchema *table_schema, NodeGroupId cc_ng_id)
        : base_table_name_(table_schema->GetBaseTableName())
    {
        const TableName &base_table_name = table_schema->GetBaseTableName();
        InitSamplePool(base_table_name, cc_ng_id);
        InitDistribution(base_table_name, table_schema->KeySchema());

        for (TableName index_name : table_schema->IndexNames())
        {
            InitSamplePool(index_name, cc_ng_id);
            InitDistribution(index_name,
                             table_schema->IndexKeySchema(index_name));
        }
    }

    TableStatistics(
        const TableSchema *table_schema,
        std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
            sample_pool_map,
        CcShard *ccs,
        NodeGroupId cc_ng_id)
        : base_table_name_(table_schema->GetBaseTableName())
    {
        const TableName &base_table_name = table_schema->GetBaseTableName();

        for (auto &[table_or_index_name, index_sample_pool] : sample_pool_map)
        {
            if (table_or_index_name != base_table_name &&
                table_schema->IndexKeySchema(table_or_index_name) == nullptr)
            {
                continue;  // Skip dropped index
            }

            InitSamplePool(table_or_index_name,
                           index_sample_pool.first,
                           std::move(index_sample_pool.second),
                           ccs,
                           cc_ng_id);
            InitDistribution(
                table_or_index_name,
                table_or_index_name.IsBase()
                    ? table_schema->KeySchema()
                    : table_schema->IndexKeySchema(table_or_index_name));
        }

        // Initialize empty distribution.
        if (index_distribution_map_.find(base_table_name) ==
            index_distribution_map_.end())
        {
            InitSamplePool(base_table_name, cc_ng_id);
            InitDistribution(base_table_name, table_schema->KeySchema());
        }

        for (const TableName &index_name : table_schema->IndexNames())
        {
            if (index_distribution_map_.find(index_name) ==
                index_distribution_map_.end())
            {
                InitSamplePool(index_name, cc_ng_id);
                InitDistribution(index_name,
                                 table_schema->IndexKeySchema(index_name));
            }
        }
    }

    const TableName &BaseTableName() const override
    {
        return base_table_name_;
    }

    // Distribution won't be null.
    const Distribution *GetDistribution(
        const TableName &table_or_index_name) const override
    {
        return &index_distribution_map_.at(table_or_index_name);
    }

    TemplateCcMapSamplePool<KeyT> *MutableSamplePool(
        const TableName &table_or_index_name, NodeGroupId ng_id)
    {
        std::unique_lock<std::shared_mutex> ulk(index_sample_pool_map_mutex_);

        auto iter = index_sample_pool_map_.find(table_or_index_name);
        if (iter == index_sample_pool_map_.end())
        {
            auto [it, insert] = index_sample_pool_map_.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(table_or_index_name),
                std::forward_as_tuple());
            assert(insert);

            it->second.emplace(std::piecewise_construct,
                               std::forward_as_tuple(ng_id),
                               std::forward_as_tuple(&it->first, ng_id, this));
        }
        else
        {
            NodeGroupSamplePoolMap &ng_sample_pool_map = iter->second;
            if (ng_sample_pool_map.find(ng_id) == ng_sample_pool_map.end())
            {
                ng_sample_pool_map.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(ng_id),
                    std::forward_as_tuple(&iter->first, ng_id, this));
            }
        }

        return &index_sample_pool_map_.at(table_or_index_name).at(ng_id);
    }

    void CreateIndex(const TableName &index_name,
                     const KeySchema *key_schema,
                     NodeGroupId cc_ng_id) override
    {
        // Lock is required. Because apply DDL Operation and release catalog
        // lock are interleaved.
        std::unique_lock<std::shared_mutex> ulk(index_sample_pool_map_mutex_);
        InitSamplePool(index_name, cc_ng_id);
        InitDistribution(index_name, key_schema);
    }

    void DropIndex(const TableName &index_name) override
    {
        // Lock is required. Because apply DDL Operation and release catalog
        // lock are interleaved.
        std::unique_lock<std::shared_mutex> ulk(index_sample_pool_map_mutex_);
        index_distribution_map_.erase(index_name);
        index_sample_pool_map_.erase(index_name);
    }

    // This method is called in the sample tx_processor thread
    void OnRemoteStatisticsMessage(
        const TableName &table_or_index_name,
        const TableSchema *table_schema,
        const remote::NodeGroupSamplePool &remote_sample_pool) override
    {
        NodeGroupId ng_id =
            static_cast<NodeGroupId>(remote_sample_pool.ng_id());

        SamplePoolParam<KeyT> param;
        param.records_ = remote_sample_pool.records();

        const KeySchema *key_schema =
            table_or_index_name.IsBase()
                ? table_schema->KeySchema()
                : table_schema->IndexKeySchema(table_or_index_name);
        for (const std::string &sample : remote_sample_pool.samples())
        {
            KeyT key;
            size_t offset = 0;
            key.Deserialize(sample.data(), offset, key_schema);
            param.sample_keys_.push_back(std::move(key));
        }

        std::unique_lock<std::shared_mutex> ulk(index_sample_pool_map_mutex_);
        auto [it, insert] = index_sample_pool_map_.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(table_or_index_name),
            std::forward_as_tuple());

        auto lk_vec = LockSamplePools(table_or_index_name);
        it->second.insert_or_assign(
            ng_id,
            TemplateCcMapSamplePool<KeyT>(&it->first, ng_id, param, this));

        RebuildDistribution(table_or_index_name, key_schema);

        SetUpdatedSinceSync();
    }

    std::unique_ptr<remote::NodeGroupSamplePool> MakeBroadcastSamplePool(
        NodeGroupId ng_id,
        const TableName &table_or_index_name,
        bool *updated_since_sync) const override
    {
        std::shared_lock<std::shared_mutex> slk(index_sample_pool_map_mutex_);

        auto broadcast_sample_pool =
            std::make_unique<remote::NodeGroupSamplePool>();

        auto iter = index_sample_pool_map_.find(table_or_index_name);
        if (iter == index_sample_pool_map_.end())
        {
            *updated_since_sync = false;
            return nullptr;
        }
        const std::unordered_map<NodeGroupId, TemplateCcMapSamplePool<KeyT>>
            &ng_sample_pool_map = iter->second;
        auto it = ng_sample_pool_map.find(ng_id);
        if (it == ng_sample_pool_map.end())
        {
            *updated_since_sync = false;
            return nullptr;
        }

        slk.unlock();
        const TemplateCcMapSamplePool<KeyT> &ccmap_sample_pool = it->second;
        Task task =
            [ccmap_sample_pool = const_cast<TemplateCcMapSamplePool<KeyT> *>(
                 &ccmap_sample_pool),
             broadcast_sample_pool = broadcast_sample_pool.get(),
             updated_since_sync](CcShard &ccs)
        {
            ccmap_sample_pool->To(broadcast_sample_pool);
            *updated_since_sync = ccmap_sample_pool->ResetGetUpdatedSinceSync();
            return true;
        };
        RunOnBindingCcShard(std::move(task));

        return broadcast_sample_pool;
    }

    std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
    MakeStoreStatistics(bool *updated_since_sync) const override
    {
        std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
            sample_pool_map;

        Task task = [this, &sample_pool_map](CcShard &ccs)
        {
            To(sample_pool_map);
            return true;
        };
        RunOnBindingCcShard(std::move(task));

        *updated_since_sync = const_cast<TableStatistics<KeyT> *>(this)
                                  ->ResetGetUpdatedSinceSync();
        return sample_pool_map;
    }

    void SetEstimateRecordSize(size_t size) override
    {
        size_t orig_val = estimate_record_size_.load(std::memory_order_relaxed);
        if (orig_val == UINT64_MAX)
        {
            // If rec size is unset

            estimate_record_size_.store(size, std::memory_order_relaxed);
        }
        else
        {
            // Update size with a 0.1 weight.
            estimate_record_size_.store(size * 0.1 + orig_val * 0.9,
                                        std::memory_order_relaxed);
        }
    }

    size_t EstimateRecordSize() const override
    {
        return estimate_record_size_.load(std::memory_order_relaxed);
    }

    // This method is called in tx_processor thread.
    //
    // Every node group execute this method.
    //
    // Sample keys and records of the old range should be subtracted from old
    // node group. Sample keys and records of every new range should be added to
    // new node group.
    //
    // ng#0 [__1__] --> [__1__] [__2__] [__3__]
    //                     ^
    // ng#1 [__1__] --> [__1__] [__2__] [__3__]
    //                             ^
    // ng#2 [__1__] --> [__1__] [__2__] [__3__]
    //                                     ^
    void OnSplitSamplePool(CcShard *ccs,
                           NodeGroupId cc_ng_id,
                           const TableName &table_or_index_name,
                           const RangeInfo *old_info)
    {
        uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
        if (ng_cnt <= 1)
        {
            return;
        }

        NodeGroupId old_ng_id =
            ccs->GetRangeOwner(old_info->PartitionId(), cc_ng_id)
                ->BucketOwner();

        auto it = index_sample_pool_map_.find(table_or_index_name);
        if (it == index_sample_pool_map_.end())
        {
            // In case store sample pool was failed, and recover range split.
            return;
        }
        NodeGroupSamplePoolMap &ng_sample_pool_map = it->second;

        auto iter = ng_sample_pool_map.find(old_ng_id);
        if (iter == ng_sample_pool_map.end())
        {
            return;
        }

        TemplateCcMapSamplePool<KeyT> &old_sample_pool = iter->second;
        if (old_sample_pool.Records() <= 0)
        {
            // In case store sample pool was failed, and recover range split.
            return;
        }

        DLOG(INFO) << "Before split records and sample for table: "
                   << table_or_index_name.StringView()
                   << " old_ng_id: " << old_ng_id
                   << ", old_range_id: " << old_info->PartitionId()
                   << ", records: " << old_sample_pool.Records()
                   << ", samples: " << old_sample_pool.SampleCount();

        std::unordered_map<NodeGroupId, SamplePoolParam<KeyT>> param_map =
            SamplePoolParamsForSplit(ccs, cc_ng_id, old_sample_pool, old_info);
        if (param_map.empty())
        {
            // Already splitted.
            return;
        }

        for (const auto &[new_ng_id, param] : param_map)
        {
            assert(new_ng_id != old_ng_id);
            auto iter = ng_sample_pool_map.find(new_ng_id);
            if (iter != ng_sample_pool_map.end())
            {
                TemplateCcMapSamplePool<KeyT> &new_sample_pool = iter->second;
                new_sample_pool.Merge(param);
                old_sample_pool.Prune(param);
            }
            else
            {
                ng_sample_pool_map.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(new_ng_id),
                    std::forward_as_tuple(&it->first, new_ng_id, param, this));
                old_sample_pool.Prune(param);
            }
        }

        DLOG(INFO) << "After split records and sample for table: "
                   << table_or_index_name.StringView()
                   << " old_ng_id: " << old_ng_id
                   << ", old_range_id: " << old_info->PartitionId()
                   << ", records: " << old_sample_pool.Records()
                   << ", samples: " << old_sample_pool.SampleCount();

        SetUpdatedSinceSync();
    }

    std::vector<std::unique_lock<std::mutex>> LockSamplePools(
        const TableName &table_or_index_name)
    {
        NodeGroupSamplePoolMap &ng_sample_pool_map =
            index_sample_pool_map_.at(table_or_index_name);

        std::vector<std::unique_lock<std::mutex>> lk_vec;
        lk_vec.reserve(ng_sample_pool_map.size());

        for (auto &[ng_id, ccmap_sample_pool] : ng_sample_pool_map)
        {
            lk_vec.emplace_back(ng_sample_pool_map.at(ng_id).Lock());
        }

        return lk_vec;
    }

    // Assumes tx_processor already holds lock on exclude_ng_id. It will try
    // to acquire lock on other ng. Call try_lock to avoid deadlock.
    std::vector<std::unique_lock<std::mutex>> TryLockSamplePoolsBesides(
        const TableName &table_or_index_name, NodeGroupId exclude_ng_id)
    {
        NodeGroupSamplePoolMap &ng_sample_pool_map =
            index_sample_pool_map_.at(table_or_index_name);

        std::vector<std::unique_lock<std::mutex>> lk_vec;
        lk_vec.reserve(ng_sample_pool_map.size());

        for (auto &[ng_id, ccmap_sample_pool] : ng_sample_pool_map)
        {
            if (ng_id != exclude_ng_id)
            {
                lk_vec.emplace_back(ng_sample_pool_map.at(ng_id).TryLock());
            }
        }

        return lk_vec;
    }

    void RebuildDistribution(const TableName &table_or_index_name,
                             const KeySchema *key_schema)
    {
        uint64_t records = 0;
        std::set<const KeyT *, PtrLessThan<KeyT>> keys;
        for (const auto &[ng_id, ccmap_sample_pool] :
             index_sample_pool_map_.at(table_or_index_name))
        {
            records += ccmap_sample_pool.Records();
            for (const KeyT &key : ccmap_sample_pool.SampleKeys())
            {
                keys.insert(&key);
            }
        }

        index_distribution_map_.at(table_or_index_name)
            .Rebuild(key_schema, records, keys);
    }

    void AccumulateRecords(const TableName &table_or_index_name)
    {
        int64_t records = 0;
        const NodeGroupSamplePoolMap &ng_sample_pool_map =
            index_sample_pool_map_.at(table_or_index_name);
        for (const auto &[ng_id, ccmap_sample_pool] : ng_sample_pool_map)
        {
            records += ccmap_sample_pool.Records();
        }
        index_distribution_map_.at(table_or_index_name).SetRecords(records);
    }

private:
    void InitSamplePool(const TableName &table_or_index_name, NodeGroupId ng_id)
    {
        typename IndexSamplePoolMap::iterator it =
            index_sample_pool_map_.find(table_or_index_name);
        if (it == index_sample_pool_map_.end())
        {
            auto [it, insert] = index_sample_pool_map_.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(table_or_index_name),
                std::forward_as_tuple());
            assert(insert);

            it->second.emplace(std::piecewise_construct,
                               std::forward_as_tuple(ng_id),
                               std::forward_as_tuple(&it->first, ng_id, this));
        }
        else
        {
            NodeGroupSamplePoolMap &ng_sample_pool_map = it->second;

            if (ng_sample_pool_map.find(ng_id) == ng_sample_pool_map.end())
            {
                ng_sample_pool_map.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(ng_id),
                    std::forward_as_tuple(&it->first, ng_id, this));
            }
        }
    }

    void InitSamplePool(const TableName &table_or_index_name,
                        uint64_t records,
                        std::vector<TxKey> samplekeys,
                        CcShard *ccs,
                        NodeGroupId cc_ng_id)
    {
        uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();

        auto [it, insert] = index_sample_pool_map_.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(table_or_index_name),
            std::forward_as_tuple());
        assert(insert);

        NodeGroupSamplePoolMap &ng_sample_pool_map = it->second;

        std::vector<std::vector<KeyT>> sample_pool_vec(ng_cnt);
        for (TxKey &samplekey : samplekeys)
        {
            std::unique_ptr<KeyT> key = samplekey.MoveKey<KeyT>();
            assert(key != nullptr);
#ifdef RANGE_PARTITION_ENABLED
            NodeGroupId dest_ng_id =
                RouteKeyByRange(*ccs, table_or_index_name, cc_ng_id, *key);
#else
            NodeGroupId dest_ng_id = RouteKeyByHash(*ccs, cc_ng_id, *key);
#endif
            sample_pool_vec[dest_ng_id].emplace_back(std::move(*key));
        }

        std::vector<uint64_t> sp_size_vec;
        std::transform(sample_pool_vec.begin(),
                       sample_pool_vec.end(),
                       std::back_inserter(sp_size_vec),
                       [](const std::vector<KeyT> &sample_pool)
                       { return sample_pool.size(); });
        std::vector<uint64_t> records_vec = DivideRecords(
            *ccs, cc_ng_id, table_or_index_name, records, sp_size_vec);
        assert(records_vec.size() == ng_cnt);

        for (NodeGroupId ng_id = 0; ng_id < ng_cnt; ++ng_id)
        {
            std::vector<KeyT> &sample_pool = sample_pool_vec[ng_id];
            uint64_t records = records_vec[ng_id];
            if (records > 0)
            {
                records = std::max(records, sample_pool.size());
                SamplePoolParam<KeyT> param{std::move(sample_pool), records};
                ng_sample_pool_map.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(ng_id),
                    std::forward_as_tuple(&it->first, ng_id, param, this));
            }
        }
    }

    // This method is called in tx_processor thread.
    void InitDistribution(const TableName &table_or_index_name,
                          const KeySchema *key_schema)
    {
        std::set<const KeyT *, PtrLessThan<KeyT>> index_sample_keys;
        uint64_t index_total_keys = 0;

        for (const auto &[ng_id, ccmap_sample_pool] :
             index_sample_pool_map_.at(table_or_index_name))
        {
            const std::vector<KeyT> &sample_keys =
                ccmap_sample_pool.SampleKeys();
            for (const KeyT &key : sample_keys)
            {
                index_sample_keys.insert(&key);
            }
            index_total_keys += ccmap_sample_pool.Records();
        }

        index_distribution_map_.try_emplace(table_or_index_name,
                                            key_schema,
                                            index_total_keys,
                                            index_sample_keys);
    }

    // Get value of updated_since_sync_, and set it to false atomically.
    bool ResetGetUpdatedSinceSync()
    {
        bool updated = true;
        updated_since_sync_.compare_exchange_strong(updated, false);
        return updated;
    }

    void SetUpdatedSinceSync() override
    {
        updated_since_sync_.store(true, std::memory_order_release);
    }

private:
    // For a table or index, we merge its sample pools of node groups together
    // when write them into kv storage. Because we need to scale up/down node
    // groups.
    //
    // After merge sample pool, when rebuild sampel pool of every node from
    // storage, we need to split that merged sample pool.
    //
    // For node group sample keys, it is easy to split them based on their route
    // method.
    //
    // For node group records, information to split table/index records is
    // incomplete. We split table/index records by using node group ranges count
    // as the weight. If the output result is inappropriate, concretely, node
    // group records is less than node group sample keys, then we split
    // table/index records by node group sample keys.
    static std::vector<uint64_t> DivideRecords(
        CcShard &ccs,
        NodeGroupId cc_ng_id,
        const TableName &table_or_index_name,
        uint64_t records,
        const std::vector<uint64_t> &sp_size_vec)
    {
        std::vector<uint64_t> records_vec;

        uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();

        if (records == 0)
        {
            records_vec.resize(ng_cnt, 0UL);
            return records_vec;
        }

        std::vector<uint64_t> ng_weight_vec;
#ifdef RANGE_PARTITION_ENABLED
        for (NodeGroupId ng_id = 0; ng_id < ng_cnt; ++ng_id)
        {
            // Lock has been acquired in LocalCcShards::InitTableStatistic.
            uint64_t range_cnt =
                ccs.CountRangesLockless(table_or_index_name, cc_ng_id, ng_id);
            ng_weight_vec.emplace_back(range_cnt);
        }
#else
        ng_weight_vec.resize(ng_cnt, 1);
#endif

        records_vec = DivideRecordsByNodeGroupWeight(records, ng_weight_vec);
        bool no_conflict =
            std::equal(records_vec.begin(),
                       records_vec.end(),
                       sp_size_vec.begin(),
                       [](uint64_t a, uint64_t b) { return a >= b; });
        if (no_conflict)
        {
            return records_vec;
        }

        records_vec = DivideRecordsBySamplePoolSize(records, sp_size_vec);
        return records_vec;
    }

    static std::vector<uint64_t> DivideRecordsByNodeGroupWeight(
        uint64_t records, const std::vector<uint64_t> &ng_weight_vec)
    {
        assert(Sharder::Instance().NodeGroupCount() == ng_weight_vec.size());
        return DivideRecordsByWeights(records, ng_weight_vec);
    }

    static std::vector<uint64_t> DivideRecordsBySamplePoolSize(
        uint64_t records, const std::vector<uint64_t> &sp_size_vec)
    {
        std::vector<uint64_t> records_vec;

        uint64_t sp_size_total =
            std::accumulate(sp_size_vec.begin(), sp_size_vec.end(), 0);
        assert(records >= sp_size_total);
        if (records <= sp_size_total)
        {
            records_vec = sp_size_vec;
        }
        else
        {
            records_vec = DivideRecordsByWeights(records, sp_size_vec);
            assert(std::equal(records_vec.begin(),
                              records_vec.end(),
                              sp_size_vec.begin(),
                              [](uint64_t a, uint64_t b) { return a >= b; }));
        }

        return records_vec;
    }

    static std::vector<uint64_t> DivideRecordsByWeights(
        uint64_t records, const std::vector<uint64_t> &weights)
    {
        size_t sz = weights.size();
        std::vector<uint64_t> records_vec(sz, 0UL);

        uint64_t total_weight =
            std::accumulate(weights.begin(), weights.end(), 0UL);
        assert(total_weight > 0);

        uint64_t c = 0;
        for (size_t i = 0; i < sz - 1; ++i)
        {
            records_vec[i] = records * (static_cast<double>(weights[i]) /
                                        static_cast<double>(total_weight));
            c += records_vec[i];
        }
        records_vec.back() = records - c;

        return records_vec;
    }

private:
    using Task = std::function<bool(CcShard &ccs)>;

    // Deliver task to tx_processor to avoid lock contention.
    void RunOnBindingCcShard(Task task) const
    {
        auto core_idx = LeaderCore(base_table_name_);

        WaitableCc cc_req(std::move(task));
        Sharder::Instance().GetLocalCcShards()->EnqueueCcRequest(core_idx,
                                                                 &cc_req);
        cc_req.Wait();
    }
    // This method is called in tx_processor thread.
    void To(
        std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
            &sample_pool_map) const
    {
        std::shared_lock<std::shared_mutex> slk(index_sample_pool_map_mutex_);

        for (const auto &[table_or_index_name, index_sample_pool] :
             index_sample_pool_map_)
        {
            sample_pool_map.try_emplace(
                table_or_index_name, 0, std::vector<TxKey>());

            uint64_t &records = sample_pool_map.at(table_or_index_name).first;
            std::vector<TxKey> &sample_keys =
                sample_pool_map.at(table_or_index_name).second;

            for (const auto &[ng_id, ccmap_sample_pool] : index_sample_pool)
            {
                auto [lk, ng_sample_keys] =
                    ccmap_sample_pool.LockedSampleKeys();
                records += ccmap_sample_pool.Records();

                for (const KeyT &sample_key : *ng_sample_keys)
                {
                    sample_keys.emplace_back(
                        std::make_unique<KeyT>(sample_key));
                }
            }

            records = std::max(records, sample_keys.size());
        }
    }

    NodeGroupId RouteKeyByHash(CcShard &ccs,
                               NodeGroupId cc_ng_id,
                               const KeyT &key) const
    {
        uint16_t bucket_id =
            Sharder::Instance().MapKeyHashToBucketId(key.Hash());
        auto *bucket_info =
            ccs.local_shards_.GetBucketInfo(bucket_id, cc_ng_id);
        return bucket_info->BucketOwner();
    }

    NodeGroupId RouteKeyByRange(CcShard &ccs,
                                const TableName &table_or_index_name,
                                NodeGroupId cc_ng_id,
                                const KeyT &key) const
    {
        // Safe to use TableRangeEntry *.
        TxKey tx_key(&key);
        const TableRangeEntry *range_entry = ccs.GetTableRangeEntryNoLocking(
            table_or_index_name, cc_ng_id, tx_key);
        assert(range_entry != nullptr);

        NodeGroupId ng_id =
            ccs.local_shards_
                .GetRangeOwnerNoLocking(
                    range_entry->GetRangeInfo()->PartitionId(), cc_ng_id)
                ->BucketOwner();
        return ng_id;
    }

    std::unordered_map<NodeGroupId, SamplePoolParam<KeyT>>
    SamplePoolParamsForSplit(
        CcShard *ccs,
        NodeGroupId cc_ng_id,
        const TemplateCcMapSamplePool<KeyT> &old_sample_pool,
        const RangeInfo *old_info) const
    {
        auto [lk, old_sample_keys] = old_sample_pool.LockedSampleKeys();
        assert(!old_sample_keys->empty());

        std::unordered_map<NodeGroupId, SamplePoolParam<KeyT>> param_map;

        NodeGroupId old_ng_id = old_sample_pool.GetNodeGroupId();

        for (const KeyT &key : *old_sample_keys)
        {
            TxKey tx_key(&key);
            TemplateTableRangeEntry<KeyT> *range_entry =
                static_cast<TemplateTableRangeEntry<KeyT> *>(
                    ccs->GetTableRangeEntry(
                        old_sample_pool.GetTableOrIndexName(),
                        cc_ng_id,
                        tx_key));
            NodeGroupId new_ng_id =
                ccs->GetRangeOwner(range_entry->TypedRangeInfo()->PartitionId(),
                                   cc_ng_id)
                    ->BucketOwner();
            if (new_ng_id != old_ng_id)
            {
                param_map[new_ng_id].sample_keys_.push_back(key);
            }
        }

        if (!param_map.empty())
        {
            uint64_t avg_range_key_count = AvgRangeKeyCountBeforeRangeSplit(
                ccs, old_sample_pool.GetTableOrIndexName(), cc_ng_id, old_info);
            for (int32_t new_partition_id :
                 old_info->NewPartitionIdUncheckDirty())
            {
                NodeGroupId new_ng_id =
                    ccs->GetRangeOwner(new_partition_id, cc_ng_id)
                        ->BucketOwner();
                if (new_ng_id != old_ng_id)
                {
                    param_map[new_ng_id].records_ += avg_range_key_count;
                }
            }

            for (auto &[new_ng_id, param] : param_map)
            {
                param.records_ =
                    std::max(param.records_, param.sample_keys_.size());
            }
        }

        return param_map;
    }

    uint64_t AvgRangeKeyCountBeforeRangeSplit(
        CcShard *ccs,
        const TableName &table_or_index_name,
        NodeGroupId cc_ng_id,
        const RangeInfo *old_info) const
    {
        NodeGroupId old_ng_id =
            ccs->GetRangeOwner(old_info->PartitionId(), cc_ng_id)
                ->BucketOwner();

        int32_t split_out = 0;
        for (int32_t new_partition_id : old_info->NewPartitionIdUncheckDirty())
        {
            NodeGroupId new_ng_id =
                ccs->GetRangeOwner(new_partition_id, cc_ng_id)->BucketOwner();
            if (new_ng_id != old_ng_id)
            {
                split_out += 1;
            }
        }

        uint64_t ng_range_count =
            ccs->CountRanges(table_or_index_name, cc_ng_id, old_ng_id) +
            split_out;
        uint64_t ng_key_count = index_sample_pool_map_.at(table_or_index_name)
                                    .at(old_ng_id)
                                    .Records();
        return ng_key_count / ng_range_count;
    }

private:
    TableName base_table_name_;

    // index_distribution_map_ is changed when executing PostWriteAllCc on
    // CatalogCcMap. No extra mutex is required here.
    std::unordered_map<TableName, IndexDistribution<KeyT>>
        index_distribution_map_;

    using NodeGroupSamplePoolMap =
        std::unordered_map<NodeGroupId, TemplateCcMapSamplePool<KeyT>>;
    using IndexSamplePoolMap =
        std::unordered_map<TableName, NodeGroupSamplePoolMap>;

    IndexSamplePoolMap index_sample_pool_map_;
    mutable std::shared_mutex index_sample_pool_map_mutex_;

    mutable std::atomic<bool> updated_since_sync_{false};

    mutable std::atomic<size_t> estimate_record_size_{UINT64_MAX};
};

}  // namespace txservice
