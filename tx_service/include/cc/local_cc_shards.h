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
#include <condition_variable>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog_factory.h"
#include "catalog_key_record.h"
#include "cc_entry.h"
#include "cc_page_clean_guard.h"
#include "cc_shard.h"
#include "data_sync_task.h"
#include "eloq_basic_catalog_factory.h"
#include "local_cc_handler.h"
#include "log.pb.h"
#include "meter.h"
#include "range_record.h"
#include "range_slice.h"
#include "store/data_store_handler.h"
#include "system_handler.h"
#include "tx_key.h"
#include "tx_service_common.h"
#include "tx_start_ts_collector.h"
#include "type.h"

#ifdef WITH_JEMALLOC
// we use uint32_t instead of unsigned to make the code more readable
static_assert(std::is_same_v<unsigned, uint32_t>,
              "jemalloc uses 'unsigned' for arena id, but this platform "
              "has unsigned != uint32_t. Please adjust the type alias.");
#endif

namespace txservice
{
namespace remote
{
class RemoteCcHandler;
};
class Checkpointer;
class TxService;
struct ClusterScaleOp;
struct DataMigrationOp;
struct InvalidateTableCacheCompositeOp;
class SkGenerator;
class UploadBatchSlicesClosure;
struct FlushDataTask;

struct DataMigrationStatus
{
public:
    DataMigrationStatus(TxNumber cluster_scale_txn,
                        std::vector<std::vector<uint16_t>> &&bucket_ids,
                        std::vector<std::vector<NodeGroupId>> &&new_owner_ngs,
                        std::vector<TxNumber> &&migration_txns)
        : cluster_scale_txn_(cluster_scale_txn),
          bucket_ids_(std::move(bucket_ids)),
          new_owner_ngs_(std::move(new_owner_ngs)),
          migration_txns_(std::move(migration_txns)),
          next_bucket_idx_(0),
          unfinished_worker_(migration_txns_.size())
    {
    }

    TxNumber cluster_scale_txn_;
    // Each worker should migrate a batch of buckets to speed up the migration.
    std::vector<std::vector<uint16_t>> bucket_ids_;
    std::vector<std::vector<NodeGroupId>> new_owner_ngs_;
    std::vector<TxNumber> migration_txns_;
    std::atomic_size_t next_bucket_idx_;
    std::atomic_size_t unfinished_worker_;
};

struct GenerateSkStatus
{
    enum struct Status
    {
        Ongoing,
        Terminating,
        Finished
    };

    explicit GenerateSkStatus(int64_t tx_term)
        : tx_term_(tx_term),
          task_status_(Status::Finished),
          tx_term_changed_(false)
    {
    }

    GenerateSkStatus(const GenerateSkStatus &rhs) = delete;
    GenerateSkStatus(GenerateSkStatus &&rhs) = delete;

    bool StartGenerateSk(int64_t tx_term)
    {
        std::unique_lock<std::mutex> lk(status_mux_);
        if (task_status_ == Status::Ongoing ||
            task_status_ == Status::Terminating)
        {
            if (tx_term_ <= tx_term)
            {
                // Terminate the current task.
                tx_term_changed_ = true;
                status_cv_.wait(
                    lk, [this]() { return task_status_ == Status::Finished; });

                // update the task status
                task_status_ = Status::Ongoing;
                tx_term_ = tx_term;
                tx_term_changed_ = false;
            }
            else
            {
                // The @@tx_term is expired, terminate it directly.
                return false;
            }
        }
        else
        {
            task_status_ = Status::Ongoing;
            tx_term_ = tx_term;
            tx_term_changed_ = false;
        }
        return true;
    }

    void TerminateGenerateSk()
    {
        std::unique_lock<std::mutex> lk(status_mux_);
        task_status_ = Status::Terminating;
    }

    void FinishGenerateSk()
    {
        std::unique_lock<std::mutex> lk(status_mux_);
        task_status_ = Status::Finished;
        status_cv_.notify_all();
    }

    bool CheckTxTermStatus()
    {
        std::unique_lock<std::mutex> lk(status_mux_);
        return !(tx_term_changed_);
    }

    Status TaskStatus()
    {
        std::unique_lock<std::mutex> lk(status_mux_);
        return task_status_;
    }

private:
    int64_t tx_term_;
    Status task_status_;
    bool tx_term_changed_;
    std::mutex status_mux_;
    std::condition_variable status_cv_;
};
class LocalCcShards
{
public:
    static const size_t DATA_SYNC_SCAN_BATCH_SIZE = 3 * 1024;

    LocalCcShards(
        uint32_t node_id,  // = 0,
        uint32_t ng_id,    // = 0,
        const std::map<std::string, uint32_t> &conf,
        CatalogFactory *catalog_factory[NUM_EXTERNAL_ENGINES],  // = nullptr,
        SystemHandler *system_handler,                          // = nullptr,
        std::unordered_map<uint32_t, std::vector<NodeConfig>>
            *ng_configs,                    // = nullptr,
        uint64_t cluster_config_version,    // = 0,
        store::DataStoreHandler *store_hd,  // = nullptr,
        TxService *tx_service,              // = nullptr,
        bool enable_mvcc = true,
        metrics::MetricsRegistry *metrics_registry = nullptr,
        metrics::CommonLabels common_labels = {},
        std::unordered_map<TableName, std::string> *prebuilt_tables = nullptr,
        std::function<void(std::string_view, std::string_view)> publish_func =
            nullptr);

    ~LocalCcShards();

    LocalCcShards(LocalCcShards const &) = delete;
    void operator=(LocalCcShards const &) = delete;

    CcShard *GetCcShard(size_t core_idx)
    {
        return cc_shards_[core_idx].get();
    }

    void EnqueueCcRequest(uint32_t thd_id,
                          uint32_t shard_code,
                          CcRequestBase *req)
    {
        uint32_t residual = shard_code & 0x3FF;
        size_t core_idx = residual % cc_shards_.size();

        cc_shards_[core_idx]->Enqueue(thd_id, req);
    }

    void EnqueueCcRequest(uint32_t shard_code, CcRequestBase *req)
    {
        uint32_t residual = shard_code & 0x3FF;
        size_t core_idx = residual % cc_shards_.size();
        cc_shards_.at(core_idx)->Enqueue(req);
    }

    size_t ProcessRequests(size_t thd_id)
    {
        return cc_shards_[thd_id]->ProcessRequests();
    }

    size_t ProcessLowPriorityRequests(size_t thd_id)
    {
        return cc_shards_[thd_id]->ProcessLowPriorityRequests();
    }

    size_t QueueSize(size_t thd_id)
    {
        return cc_shards_[thd_id]->QueueSize();
    };

    size_t LowPriorityQueueSize(size_t thd_id)
    {
        return cc_shards_[thd_id]->LowPriorityQueueSize();
    }

    bool IsIdle(uint32_t thd_id) const
    {
        return cc_shards_[thd_id]->IsIdle();
    }

    void SetTxProcNotifier(uint16_t thd_id,
                           std::atomic<TxProcessorStatus> *tx_proc_status,
                           TxProcCoordinator *tx_coordi)
    {
        return cc_shards_[thd_id]->SetTxProcNotifier(tx_proc_status, tx_coordi);
    }

    size_t Count() const
    {
        return cc_shards_.size();
    }

    TxService *GetTxservice()
    {
        return tx_service_;
    }

    // Testing purpose only.
    template <typename KeyT, typename ValueT>
    void CreateCcTable(const TableName &tabname,
                       const KeySchema *key_schema = nullptr,
                       const RecordSchema *rec_schema = nullptr,
                       uint32_t core_id = 0,
                       bool is_all = true)
    {
        for (uint32_t id = 0; id < cc_shards_.size(); id++)
        {
            if (is_all || id == core_id)
            {
                cc_shards_[id]->native_ccms_.try_emplace(
                    tabname,
                    std::make_unique<TemplateCcMap<KeyT, ValueT, true, true>>(
                        cc_shards_[id].get(), key_schema, rec_schema));
            }
        }
    }

    void DropCcTable(const TableName &tabname, uint32_t core_id)
    {
        cc_shards_[core_id]->native_ccms_.erase(tabname);
    }

    template <typename SkT, typename PkT>
    void CreateSkCcTable(const TableName &tabname,
                         const KeySchema *sk_schema = nullptr,
                         const KeySchema *pk_schema = nullptr,
                         uint32_t core_id = 0,
                         bool is_all = true)
    {
        for (uint32_t id = 0; id < cc_shards_.size(); id++)
        {
            if (is_all || id == core_id)
            {
                cc_shards_[id]->native_ccms_.try_emplace(
                    tabname,
                    std::make_unique<SkCcMap<SkT, PkT>>(
                        cc_shards_[id].get(), sk_schema, pk_schema));
            }
        }
    }

    /**
     * @brief Clean the ccentries from the ccmap in each ccshards.
     * Note that this function is not thread safe and should only be used by
     * test case.
     *
     * @param tabname
     */
    void CleanCcTable(const TableName &tabname)
    {
        for (uint32_t i = 0; i < cc_shards_.size(); i++)
        {
            cc_shards_[i]->CleanCcm(tabname);
        }
    }

    void NotifyCheckPointer(bool request_ckpt = true)
    {
        cc_shards_[0]->NotifyCkpt(request_ckpt);
    }

    void PrintCcMap()
    {
        // std::unordered_map<TableName, size_t>
        //     mapsizes;  // not string owner, sv -> native_ccms_
        // for (const auto &cc_shard : cc_shards_)
        // {
        //     CcShard &shard = *cc_shard;

        //     for (auto map_iter = shard.native_ccms_.begin();
        //          map_iter != shard.native_ccms_.end();
        //          ++map_iter)
        //     {
        //         const TableName &tab_name = map_iter->first;
        //         auto find_iter = mapsizes.find(tab_name);
        //         size_t cnt = 0;
        //         if (find_iter != mapsizes.end())
        //         {
        //             cnt = find_iter->second;
        //             cnt += map_iter->second->size();
        //             find_iter->second = cnt;
        //         }
        //         else
        //         {
        //             mapsizes.emplace(
        //                 std::piecewise_construct,
        //                 std::forward_as_tuple(tab_name.StringView(),
        //                                       tab_name.Type(),
        //                                       tab_name.Engine()),
        //                 std::forward_as_tuple(map_iter->second->size()));
        //         }

        //         std::cout << "Table '" << tab_name.StringView() << "' core ID
        //         "
        //                   << shard.core_id_ << ": " <<
        //                   map_iter->second->size()
        //                   << std::endl;

        //         assert(map_iter->second->VerifyOrdering() ==
        //                map_iter->second->size());
        //     }
        // }

        /*for (auto map_iter = mapsizes.begin(); map_iter != mapsizes.end();
             ++map_iter)
        {
            std::cout << map_iter->first << ": " << map_iter->second
                      << std::endl;
        }*/
    }

    uint32_t NodeId() const
    {
        return node_id_;
    }

    void EnqueueToCcShard(uint16_t cc_shard_idx, CcRequestBase *req)
    {
        assert(cc_shard_idx < cc_shards_.size());
        auto &ccs = cc_shards_[cc_shard_idx];
        ccs->Enqueue(req);
    }

    void EnqueueLowPriorityCcRequestToShard(uint16_t cc_shard_idx,
                                            CcRequestBase *req)
    {
        assert(cc_shard_idx < cc_shards_.size());
        auto &ccs = cc_shards_[cc_shard_idx];
        ccs->EnqueueLowPriorityCcRequest(req);
    }

    static uint64_t ClockTs();
    static uint64_t ClockTsInMillseconds();
    uint64_t TsBase();
    uint64_t TsBaseInMillseconds();
    void UpdateTsBase(uint64_t timestamp);
    void StartBackgroudWorkers();

    void BindThreadToFastMetaDataShard(size_t shard_idx);

    /**
     * @brief Create new heap used by table ranges only.
     *
     * Note that this function should only be invoked by threads whose life
     * cycle can last until the end of the process.
     */
    void InitializeTableRangesHeap()
    {
        std::unique_lock<std::mutex> lk(table_ranges_heap_mux_);
        if (!table_ranges_heap_)
        {
            table_ranges_thread_id_ = mi_thread_id();
            table_ranges_heap_ = mi_heap_new();
        }

#if defined(WITH_JEMALLOC)
        // create table ranges arena
        size_t sz = sizeof(uint32_t);
        if (mallctl("arenas.create", &table_ranges_arena_id_, &sz, NULL, 0) !=
            0)
        {
            LOG(FATAL) << "Failed to create jemalloc arena for table ranges";
        }
#endif
    }

    void InitializeHashPartitionCkptHeap()
    {
        hash_partition_ckpt_heap_ = mi_heap_new();
        hash_partition_main_thread_id_ = mi_thread_id();
#if defined(WITH_JEMALLOC)
        // create hash partition ckpt arena
        size_t sz = sizeof(uint32_t);
        if (mallctl("arenas.create",
                    &hash_partition_ckpt_arena_id_,
                    &sz,
                    NULL,
                    0) != 0)
        {
            LOG(FATAL) << "Failed to create jemalloc arena for hash part heap";
        }
#endif
    }

    mi_threadid_t GetTableRangesHeapThreadId() const
    {
        return table_ranges_thread_id_;
    }

    mi_heap_t *GetTableRangesHeap() const
    {
        return table_ranges_heap_;
    }

    uint32_t GetTableRangesArenaId() const
    {
        return table_ranges_arena_id_;
    }

    /**
     * @brief Check whether the table ranges heap reach the limitation.
     * NOTE: Be sure that this function is called in context of table ranges
     * heap.
     */
    bool TableRangesMemoryFull()
    {
#if defined(WITH_JEMALLOC)
        auto table_range_arena_id = GetTableRangesArenaId();
        size_t committed = 0;
        size_t allocated = 0;
        GetJemallocArenaStat(table_range_arena_id, committed, allocated);
        return (static_cast<size_t>(allocated) >= range_slice_memory_limit_);

#else
        if (table_ranges_heap_ != nullptr)
        {
            int64_t allocated, committed;
            mi_thread_stats(&allocated, &committed);
            return (static_cast<size_t>(allocated) >=
                    range_slice_memory_limit_);
        }
        else
        {
            return false;
        }
#endif
    }

    /**
     * @brief Check whether the table ranges heap has enough memory.
     * NOTE: Be sure that this function is called in context of table ranges
     * heap.
     */
    bool HasEnoughTableRangesMemory()
    {
#if defined(WITH_JEMALLOC)
        auto table_range_arena_id = GetTableRangesArenaId();

        size_t committed = 0;
        size_t allocated = 0;
        GetJemallocArenaStat(table_range_arena_id, committed, allocated);
        size_t target_memory_size = range_slice_memory_limit_ / 10 * 9;
        return (static_cast<size_t>(allocated) <= target_memory_size);

#else
        if (table_ranges_heap_ != nullptr)
        {
            size_t target_memory_size = range_slice_memory_limit_ / 10 * 9;
            int64_t allocated, committed;
            mi_thread_stats(&allocated, &committed);
            return (static_cast<size_t>(allocated) <= target_memory_size);
        }
        else
        {
            return false;
        }
#endif
    }

    void TableRangeHeapUsageReport()
    {
#if defined(WITH_JEMALLOC)
        size_t committed = 0;
        size_t allocated = 0;
        GetJemallocArenaStat(GetTableRangesArenaId(), committed, allocated);
        LOG(INFO) << "Table range memory report: allocated " << allocated
                  << ", committed " << committed << ", full: "
                  << (bool) (static_cast<size_t>(allocated) >=
                             range_slice_memory_limit_);
#else
        std::unique_lock<std::mutex> heap_lk(table_ranges_heap_mux_);
        bool is_override_thd = mi_is_override_thread();
        mi_threadid_t prev_thd =
            mi_override_thread(GetTableRangesHeapThreadId());
        mi_heap_t *prev_heap = mi_heap_set_default(GetTableRangesHeap());

        int64_t allocated, committed;
        mi_thread_stats(&allocated, &committed);
        LOG(INFO) << "Table range memory report: allocated " << allocated
                  << ", committed " << committed << ", full: "
                  << (bool) (static_cast<size_t>(allocated) >=
                             range_slice_memory_limit_);

        mi_heap_set_default(prev_heap);
        if (is_override_thd)
        {
            mi_override_thread(prev_thd);
        }
        else
        {
            mi_restore_default_thread_id();
        }
        heap_lk.unlock();
#endif
    }

    /**
     * -------------------------------------
     *
     * Catalog Operation Interface
     *
     * -------------------------------------
     */
    /**
     * Returns false if catalog entry of higher version already exists.
     * @param table_name
     * @param cc_ng_id
     * @param catalog_image
     * @param commit_ts
     * @return
     */
    std::pair<bool, const CatalogEntry *> CreateCatalog(
        const TableName &table_name,
        NodeGroupId cc_ng_id,
        const std::string &catalog_image,
        uint64_t commit_ts);

    TableSchema::uptr CreateTableSchemaFromImage(
        const TableName &table_name,
        const std::string &catalog_image,
        uint64_t version);

    CatalogEntry *CreateDirtyCatalog(const TableName &table_name,
                                     NodeGroupId cc_ng_id,
                                     const std::string &catalog_image,
                                     uint64_t commit_ts);

    void UpdateDirtyCatalog(const TableName &table_name,
                            const std::string &catalog_image,
                            CatalogEntry *catalog_entry);

    /**
     * Returns false if catalog entry of higher version already exists.
     * @param table_name
     * @param cc_ng_id
     * @param old_catalog_image
     * @param new_catalog_image
     * @param commit_ts
     * @return
     */
    std::pair<bool, const CatalogEntry *> CreateReplayCatalog(
        const TableName &table_name,
        NodeGroupId cc_ng_id,
        const std::string &old_catalog_image,
        const std::string &new_catalog_image,
        uint64_t old_schema_ts,
        uint64_t dirty_schema_ts);

    CatalogEntry *GetCatalog(const TableName &table_name, NodeGroupId cc_ng_id);
    CatalogEntry *GetCatalogInternal(const TableName &table_name,
                                     NodeGroupId cc_ng_id);

    /**
     * @brief Drops all tables' catalogs associated with the specified cc node
     * group. The function is called when this node steps down from the leader
     * of the specified cc node group.
     *
     * @param cc_ng_id The cc node group whose leader has transferred to another
     * node.
     */
    void DropCatalogs(NodeGroupId cc_ng_id);

    /**
     * @return Pair of TableName and bool, the bool value is used to sign
     * whether this table is dirty table(such as dirty index table).
     */
    std::unordered_map<TableName, bool> GetCatalogTableNameSnapshot(
        NodeGroupId cc_ng_id, uint64_t snapshot_ts);

    void CreateSchemaRecoveryTx(ReplayLogCc &replay_log_cc,
                                const ::txlog::SchemaOpMessage &schema_op_msg,
                                int64_t tx_term);

    /**
     * ---------------------------------
     *
     * Table Range Operation Interface
     *
     * ---------------------------------
     */
    void CreateSplitRangeRecoveryTx(
        ReplayLogCc &replay_log_cc,
        const ::txlog::SplitRangeOpMessage &ds_split_range_op_msg,
        int32_t partition_id,
        const RangeInfo *range_info,
        std::vector<TxKey> &&new_range_key,
        std::vector<int32_t> &&new_partition_ids,
        uint32_t node_group_id,
        int64_t tx_term);

    template <typename KeyT>
    std::vector<const TemplateTableRangeEntry<KeyT> *> SplitTableRange(
        const TableName &table_name,
        NodeGroupId ng_id,
        TemplateTableRangeEntry<KeyT> *old_entry,
        size_t estimate_rec_size = UINT64_MAX)
    {
        std::vector<const TemplateTableRangeEntry<KeyT> *> new_range_entries;
        assert(old_entry != nullptr);
        TemplateRangeInfo<KeyT> *old_info = old_entry->TypedRangeInfo();
        size_t new_range_cnt = old_info->NewKey()->size();
        NodeGroupId range_owner =
            GetRangeOwner(old_info->PartitionId(), ng_id)->BucketOwner();

        bool has_dml_since_ddl = true;

        if (range_owner == ng_id)
        {
            uint64_t old_last_sync_ts = old_entry->GetLastSyncTs();
            // Store range is pinned during range split so we don't need mutex
            // on range entry.
            TemplateStoreRange<KeyT> *old_store_range =
                old_entry->TypedStoreRange();
            assert(old_store_range);
            has_dml_since_ddl = old_store_range->HasDmlSinceDdl();

            const TxKey &tx_key = old_info->NewKey()->front();
            // Split the StoreRange struct in old TableRangeEntry and get
            // the removed slice keys and sizes. These keys will be reused
            // as the slice keys in the new ranges.
            std::vector<SliceInitInfo> new_slice_info;

            // Acquire meta lock since table_ranges_ is not consistent during
            // split.
            std::unique_lock<FastMetaDataMutex> meta_lk(fast_meta_data_mux_);
            bool split_range_res = old_store_range->SplitRange(
                tx_key.GetKey<KeyT>(), new_slice_info);
            if (!split_range_res)
            {
                // split fails
                assert(new_slice_info.empty());
                return new_range_entries;
            }
            if (new_slice_info.empty())
            {
                // If all current slices should stay in old range,
                // assign empty slice for new range
                for (const TxKey &new_key : *old_info->NewKey())
                {
                    const KeyT *new_range_start = new_key.GetKey<KeyT>();
                    new_slice_info.emplace_back(
                        TxKey(std::make_unique<KeyT>(*new_range_start)),
                        0,
                        SliceStatus::FullyCached);
                }
            }
            // Create new range entries in local cc shard
            size_t cur_slice_idx = 0;
            for (uint new_range_idx = 0; new_range_idx < new_range_cnt;
                 new_range_idx++)
            {
                std::vector<SliceInitInfo> cur_range_slices;
                // First slice start key will reuse the new range start
                // key, so we can just pass in nullptr.
                std::unique_ptr<KeyT> range_start_key =
                    new_slice_info[cur_slice_idx].key_.MoveKey<KeyT>();

                cur_range_slices.emplace_back(
                    std::move(new_slice_info.at(cur_slice_idx)));
                cur_slice_idx++;
                // Move the range slices that falls into the new range.
                while (cur_slice_idx != new_slice_info.size() &&
                       (new_range_idx + 1 == new_range_cnt ||
                        new_slice_info.at(cur_slice_idx).key_ <
                            old_info->NewKey()->at(new_range_idx + 1)))
                {
                    cur_range_slices.emplace_back(
                        std::move(new_slice_info.at(cur_slice_idx++)));
                }

                if (new_range_idx < new_range_cnt - 1 &&
                    cur_slice_idx == new_slice_info.size())
                {
                    // If we run out of slice before we reach last new
                    // range, insert an empty slice for the next new
                    // range
                    for (size_t idx = new_range_idx + 1; idx < new_range_cnt;
                         ++idx)
                    {
                        const TxKey &tx_key = old_info->NewKey()->at(idx);
                        const KeyT *r_start = tx_key.GetKey<KeyT>();

                        new_slice_info.emplace_back(
                            TxKey(std::make_unique<KeyT>(*r_start)),
                            0,
                            SliceStatus::FullyCached);
                    }
                }
                assert(
                    [&]()
                    {
                        const TxKey &tx_key =
                            old_info->NewKey()->at(new_range_idx);
                        return *range_start_key == *tx_key.GetKey<KeyT>();
                    }());

                const TemplateTableRangeEntry<KeyT> *new_range =
                    CreateTableRange(
                        table_name,
                        ng_id,
                        old_info->NewPartitionId()->at(new_range_idx),
                        *range_start_key,
                        old_info->DirtyTs(),
                        &cur_range_slices,
                        false,
                        old_last_sync_ts,
                        estimate_rec_size,
                        has_dml_since_ddl);

                new_range_entries.push_back(new_range);
            }
        }
        else
        {
            std::unique_ptr<std::tuple<
                bool,
                bool,
                std::unordered_map<int32_t, std::vector<SliceInitInfo>>>>
                new_range_slices = old_entry->ReleaseDirtyRangeSlices();

            has_dml_since_ddl = new_range_slices != nullptr
                                    ? std::get<1>(*new_range_slices)
                                    : true;
            // Acquire meta lock since table_ranges_ is not consistent during
            // split.
            std::unique_lock<FastMetaDataMutex> meta_lk(fast_meta_data_mux_);
            for (size_t i = 0; i < new_range_cnt; i++)
            {
                const TxKey &tx_key = old_info->NewKey()->at(i);
                const KeyT *range_start = tx_key.GetKey<KeyT>();

                // During splitting, new ranges' slices info was
                // uploaded on their new owner and cached in
                // TableRangeEntry::dirty_range_slices_.
                // Then, just copy them to the new ranges.
                std::vector<SliceInitInfo> *slices_keys = nullptr;
                if (new_range_slices != nullptr)
                {
                    NodeGroupId new_range_owner =
                        GetRangeOwnerInternal(old_info->NewPartitionId()->at(i),
                                              ng_id)
                            ->BucketOwner();
                    if (new_range_owner == ng_id)
                    {
                        auto &new_store_range = std::get<2>(*new_range_slices);
                        auto tmp_it = new_store_range.find(
                            old_info->NewPartitionId()->at(i));
                        if (tmp_it != new_store_range.end())
                        {
                            slices_keys = &(tmp_it->second);
                        }
                    }
                }

                const TemplateTableRangeEntry<KeyT> *new_range =
                    CreateTableRange(table_name,
                                     ng_id,
                                     old_info->NewPartitionId()->at(i),
                                     *range_start,
                                     old_info->DirtyTs(),
                                     slices_keys,
                                     false,
                                     0,
                                     estimate_rec_size,
                                     has_dml_since_ddl);

                new_range_entries.push_back(new_range);
            }
        }
        if (TableRangesMemoryFull())
        {
            KickoutRangeSlices();
        }
        return new_range_entries;
    }

    void FreeCcShard(size_t idx)
    {
        cc_shards_[idx] = nullptr;
    }

    /**
     * @brief Create a new table range entry and fill current range info with
     * given partition id and start key.
     */
    template <typename KeyT>
    const TemplateTableRangeEntry<KeyT> *CreateTableRange(
        const TableName &table_name,
        const NodeGroupId ng_id,
        int32_t partition_id,
        const KeyT &start_key,
        uint64_t version,
        std::vector<SliceInitInfo> *slice_keys = nullptr,
        bool need_meta_lk = true,
        uint64_t last_sync_ts = 0,
        size_t estimate_rec_size = UINT64_MAX,
        bool has_dml_since_ddl = true)
    {
        std::unique_lock<FastMetaDataMutex> lk(fast_meta_data_mux_,
                                               std::defer_lock);
        if (need_meta_lk)
        {
            lk.lock();
        }
        std::vector<TableRangeEntry *> new_entries;

        std::map<TxKey, TableRangeEntry::uptr> *ranges =
            GetTableRangesForATableInternal(table_name, ng_id);
        std::unordered_map<uint32_t, TableRangeEntry *> *range_ids =
            GetTableRangeIdsForATableInternal(table_name, ng_id);
        std::unique_ptr<TemplateStoreRange<KeyT>> range_slices = nullptr;
        NodeGroupId range_ng =
            GetRangeOwnerInternal(partition_id, ng_id)->BucketOwner();

        std::unique_lock<std::mutex> heap_lk(table_ranges_heap_mux_);
        bool is_override_thd = mi_is_override_thread();
        mi_threadid_t prev_thd = mi_override_thread(table_ranges_thread_id_);
        mi_heap_t *prev_heap = mi_heap_set_default(table_ranges_heap_);

#if defined(WITH_JEMALLOC)
        uint32_t prev_arena_id;
        JemallocArenaSwitcher::ReadCurrentArena(prev_arena_id);
        auto table_range_arena_id = GetTableRangesArenaId();
        JemallocArenaSwitcher::SwitchToArena(table_range_arena_id);
#endif

        TxKey range_tx_key(&start_key);
        auto range_it = ranges->find(range_tx_key);
        if (range_it == ranges->end())
        {
            // The created range entry copies the start key and references the
            // input end key, which points to the containing range's end key.
            std::unique_ptr<TemplateTableRangeEntry<KeyT>> new_range_entry =
                std::make_unique<TemplateTableRangeEntry<KeyT>>(
                    &start_key, version, partition_id);

            TemplateTableRangeEntry<KeyT> *new_range_ptr =
                new_range_entry.get();
            auto [new_range_it, is_insert] =
                ranges->try_emplace(TxKey(new_range_entry->RangeStartKey()),
                                    std::move(new_range_entry));
            assert(is_insert);

            auto next_range_it = std::next(new_range_it);
            if (next_range_it != ranges->end())
            {
                const TemplateTableRangeEntry<KeyT> *next_entry =
                    static_cast<const TemplateTableRangeEntry<KeyT> *>(
                        next_range_it->second.get());
                const KeyT *next_start = next_entry->RangeStartKey();
                new_range_ptr->SetRangeEndKey(next_start);
            }
            else
            {
                new_range_ptr->SetRangeEndKey(KeyT::PositiveInfinity());
            }

            // Update previous range entry's end key if the range is inserted
            // into table_ranges. The new inserted range is always not the
            // smallest range since negative inf is one of the first default
            // range start key.
            auto prev_it = std::prev(new_range_it);
            const KeyT *new_range_start = new_range_ptr->RangeStartKey();
            TemplateTableRangeEntry<KeyT> *prev_range_entry =
                static_cast<TemplateTableRangeEntry<KeyT> *>(
                    prev_it->second.get());
            prev_range_entry->SetRangeEndKey(new_range_start);

            range_ids->try_emplace(partition_id, new_range_ptr);

            if (ng_id == range_ng && slice_keys)
            {
                new_range_ptr->InitRangeSlices(std::move(*slice_keys),
                                               range_ng,
                                               table_name.IsBase(),
                                               false,
                                               estimate_rec_size,
                                               has_dml_since_ddl);
            }

            mi_heap_set_default(prev_heap);
            if (is_override_thd)
            {
                mi_override_thread(prev_thd);
            }
            else
            {
                mi_restore_default_thread_id();
            }

#if defined(WITH_JEMALLOC)
            JemallocArenaSwitcher::SwitchToArena(prev_arena_id);
#endif

            if (last_sync_ts > 0)
            {
                new_range_ptr->UpdateLastDataSyncTS(last_sync_ts);
            }

            return new_range_ptr;
        }
        else if (range_it->second->Version() < version)
        {
            // Update existing range entry's version range slice info if the
            // passed in version is newer.
            TemplateTableRangeEntry<KeyT> *range_entry =
                static_cast<TemplateTableRangeEntry<KeyT> *>(
                    range_it->second.get());

            if (ng_id == range_ng && slice_keys)
            {
                const KeyT *r_start = range_entry->TypedRangeInfo()->StartKey();
                const KeyT *r_end = range_entry->TypedRangeInfo()->EndKey();
                range_slices = std::make_unique<TemplateStoreRange<KeyT>>(
                    r_start,
                    r_end,
                    partition_id,
                    range_ng,
                    *this,
                    table_name.IsBase(),
                    false,
                    UINT64_MAX,
                    has_dml_since_ddl);
                range_slices->InitSlices(std::move(*slice_keys));
            }
            range_entry->UpdateRangeEntry(version, std::move(range_slices));
        }

        if (is_override_thd)
        {
            mi_override_thread(prev_thd);
        }
        else
        {
            mi_restore_default_thread_id();
        }
        mi_heap_set_default(prev_heap);

#if defined(WITH_JEMALLOC)
        JemallocArenaSwitcher::SwitchToArena(prev_arena_id);
#endif

        return static_cast<TemplateTableRangeEntry<KeyT> *>(
            range_it->second.get());
    }

    /**
     * @brief Initialize TableRangeEntry for a table in range_maps_.
     */
    void InitTableRanges(const TableName &range_table_name,
                         std::vector<InitRangeEntry> &init_ranges,
                         const NodeGroupId ng_id,
                         bool empty_table = false);

    /**
     * @brief Get the All Table Ranges for a table.
     */
    std::map<TxKey, TableRangeEntry::uptr> *GetTableRangesForATable(
        const TableName &range_table_name, const NodeGroupId ng_id);

    /**
     * @brief Upload new range info into range_info_ in TableRangeEntry
     * object.
     */
    template <typename KeyT>
    TemplateTableRangeEntry<KeyT> *UploadNewRangeInfo(
        const TableName &table_name,
        const NodeGroupId ng_id,
        const KeyT &key,
        const std::vector<TxKey> &new_key,
        const std::vector<int32_t> &new_partition_id,
        uint64_t commit_ts)
    {
        std::vector<TxKey> new_key_copy;
        new_key_copy.reserve(new_key.size());
        for (const TxKey &key : new_key)
        {
            const KeyT *typed_key = key.GetKey<KeyT>();
            assert(typed_key->Type() == KeyType::Normal);
            new_key_copy.emplace_back(std::make_unique<KeyT>(*typed_key));
        }
        std::vector<int32_t> partition_copy{new_partition_id};

        std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
        TxKey map_key(&key);
        TemplateTableRangeEntry<KeyT> *range_entry =
            static_cast<TemplateTableRangeEntry<KeyT> *>(
                GetTableRangeEntryInternal(table_name, ng_id, map_key));
        assert(range_entry);
        // Set dirty range in local cc shard range entry.
        range_entry->UploadNewRangeInfo(
            std::move(new_key_copy), std::move(partition_copy), commit_ts);

        return range_entry;
    }

    /**
     * @brief Remove all ranges of table_name from local cc shard.
     */
    void CleanTableRange(const TableName &table_name, NodeGroupId ng_id);

    /**
     * @brief Remove all ranges of ng_id from local cc shard.
     */
    void DropTableRanges(NodeGroupId ng_id);

    /**
     * @brief Kickout least recently used range slices info from local cc
     * shards. Note that this function should not be called on txprocessor since
     * it will block for a while(up to milliseconds).
     */
    void KickoutRangeSlices();

    /**
     * @brief Get the TableRangeEntry with given table name and key
     * from local cc shards. This result in a binary search with key in
     * table_ranges_.
     */
    TableRangeEntry *GetTableRangeEntry(const TableName &table_name,
                                        const NodeGroupId ng_id,
                                        const TxKey &key);

    std::optional<std::tuple<uint64_t, TxKey, TxKey>> GetTableRangeKeys(
        const TableName &table_name, const NodeGroupId ng_id, int32_t range_id);

    bool CheckRangeVersion(const TableName &table_name,
                           const NodeGroupId ng_id,
                           int32_t range_id,
                           uint64_t range_version);

    const TableRangeEntry *GetTableRangeEntry(const TableName &table_name,
                                              const NodeGroupId ng_id,
                                              int32_t range_id);

    const TableRangeEntry *GetTableRangeEntryNoLocking(
        const TableName &table_name, const NodeGroupId ng_id, const TxKey &key);

    template <typename KeyT>
    RangeSliceOpStatus AddKeyToKeyCache(const TableName &table_name,
                                        NodeGroupId cc_ng_id,
                                        uint16_t core_id,
                                        const KeyT &key)
    {
        std::shared_lock<std::shared_mutex> lk(meta_data_mux_);

        TableName range_table_name(table_name.StringView(),
                                   TableType::RangePartition,
                                   table_name.Engine());
        TxKey search_key(&key);

        TemplateTableRangeEntry<KeyT> *range_entry =
            static_cast<TemplateTableRangeEntry<KeyT> *>(
                GetTableRangeEntryInternal(
                    range_table_name, cc_ng_id, search_key));
        if (!range_entry)
        {
            // key cache does not exist for this range.
            return RangeSliceOpStatus::Error;
        }
        std::shared_lock<WritePreferSharedMutex> range_lk(range_entry->mux_);
        TemplateStoreRange<KeyT> *store_range = range_entry->TypedStoreRange();
        if (!store_range)
        {
            // key cache does not exist for this range.
            return RangeSliceOpStatus::Error;
        }
        store_range->UpdateLastAccessedTs(ClockTs());
        return store_range->AddKey(key, core_id);
    }

    template <typename KeyT>
    RangeSliceId PinRangeSlice(
        const TableName &table_name,
        NodeGroupId cc_ng_id,
        int64_t cc_ng_term,
        const KeySchema *key_schema,
        const RecordSchema *rec_schema,
        uint64_t schema_ts,
        const KVCatalogInfo *kv_info,
        int32_t range_id,
        const KeyT &key,
        bool inclusive,
        CcRequestBase *cc_request,
        CcShard *cc_shard,
        RangeSliceOpStatus &pin_status,
        bool force_load,
        uint32_t prefetch_size,
        bool check_key_cache = false,
        bool no_load_on_miss = false,
        bool prefetch_force_load = false,
        const std::function<int32_t(int32_t, bool)> &next_prefetch_slice =
            DefaultNextPrefetchSlice)
    {
        std::shared_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);

        TableName range_table_name(table_name.StringView(),
                                   TableType::RangePartition,
                                   table_name.Engine());
        TxKey slice_key(&key);

        TemplateTableRangeEntry<KeyT> *range_entry;
        if (range_id >= 0)
        {
            range_entry = static_cast<TemplateTableRangeEntry<KeyT> *>(
                GetTableRangeEntryInternal(
                    range_table_name, cc_ng_id, range_id));
        }
        else
        {
            range_entry = static_cast<TemplateTableRangeEntry<KeyT> *>(
                GetTableRangeEntryInternal(
                    range_table_name, cc_ng_id, slice_key));
        }

        if (!range_entry)
        {
            // Table range info not initialized, initialize range info first
            cc_shard->FetchTableRanges(
                range_table_name, cc_request, cc_ng_id, cc_ng_term);
            pin_status = RangeSliceOpStatus::BlockedOnLoad;
            return RangeSliceId();
        }
        std::shared_lock<WritePreferSharedMutex> range_lk(range_entry->mux_);
        TemplateStoreRange<KeyT> *store_range = range_entry->TypedStoreRange();
        if (store_range == nullptr)
        {
            // Check if range is owned by cc_ng_id. If so, load range slices
            // from data store
            if (GetBucketInfoInternal(
                    Sharder::Instance().MapRangeIdToBucketId(
                        range_entry->GetRangeInfo()->PartitionId()),
                    cc_ng_id)
                    ->BucketOwner() == cc_ng_id)
            {
                // release shared lock since FetchRangeSlices will acquire
                // unique lock.
                range_lk.unlock();
                range_entry->FetchRangeSlices(range_table_name,
                                              cc_request,
                                              cc_ng_id,
                                              cc_ng_term,
                                              cc_shard);
                pin_status = RangeSliceOpStatus::BlockedOnLoad;
            }
            else
            {
                pin_status = RangeSliceOpStatus::NotOwner;
            }
            return RangeSliceId();
        }

        store_range->UpdateLastAccessedTs(ClockTs());
        const StoreSlice *last_pinned_slice;
        return store_range->PinSlices(table_name,
                                      cc_ng_term,
                                      key,
                                      inclusive,
                                      nullptr,
                                      false,
                                      key_schema,
                                      rec_schema,
                                      schema_ts,
                                      INT64_MAX,
                                      kv_info,
                                      cc_request,
                                      cc_shard,
                                      store_hd_,
                                      force_load,
                                      prefetch_size,
                                      1,
                                      true,
                                      pin_status,
                                      last_pinned_slice,
                                      check_key_cache,
                                      cc_shard->core_id_,
                                      no_load_on_miss,
                                      prefetch_force_load,
                                      next_prefetch_slice);
    }

    template <typename KeyT>
    RangeSliceId PinRangeSlices(const TableName &table_name,
                                NodeGroupId cc_ng_id,
                                int64_t cc_ng_term,
                                const KeySchema *key_schema,
                                const RecordSchema *rec_schema,
                                uint64_t schema_ts,
                                const KVCatalogInfo *kv_info,
                                uint32_t range_id,
                                const KeyT &start_key,
                                bool start_inclusive,
                                const KeyT *end_key,
                                bool end_inclusive,
                                CcRequestBase *cc_request,
                                CcShard *cc_shard,
                                bool force_load,
                                uint32_t prefetch_size,
                                uint32_t max_pin_cnt,
                                bool forward_pin,
                                RangeSliceOpStatus &pin_status,
                                const StoreSlice *&last_pinned_slice)
    {
        std::shared_lock<std::shared_mutex> lk(meta_data_mux_);

        TableName range_table_name(table_name.StringView(),
                                   TableType::RangePartition,
                                   table_name.Engine());

        TemplateTableRangeEntry<KeyT> *range_entry =
            static_cast<TemplateTableRangeEntry<KeyT> *>(
                GetTableRangeEntryInternal(
                    range_table_name, cc_ng_id, range_id));
        if (!range_entry)
        {
            // Table range info not initialized, initialize range info first
            cc_shard->FetchTableRanges(
                range_table_name, cc_request, cc_ng_id, cc_ng_term);
            pin_status = RangeSliceOpStatus::BlockedOnLoad;
            return RangeSliceId();
        }
        std::shared_lock<WritePreferSharedMutex> range_lk(range_entry->mux_);
        TemplateStoreRange<KeyT> *store_range = range_entry->TypedStoreRange();
        if (store_range == nullptr)
        {
            // Check if range is owned by cc_ng_id. If so, load range slices
            // from data store
            if (GetBucketInfoInternal(
                    Sharder::Instance().MapRangeIdToBucketId(
                        range_entry->GetRangeInfo()->PartitionId()),
                    cc_ng_id)
                    ->BucketOwner() == cc_ng_id)
            {
                // release shared lock since FetchRangeSlices will acquire
                // unique lock.
                range_lk.unlock();
                range_entry->FetchRangeSlices(range_table_name,
                                              cc_request,
                                              cc_ng_id,
                                              cc_ng_term,
                                              cc_shard);
                pin_status = RangeSliceOpStatus::BlockedOnLoad;
            }
            else
            {
                pin_status = RangeSliceOpStatus::NotOwner;
            }
            return RangeSliceId();
        }
        uint64_t snapshot_ts = 0;
        if (EnableMvcc())
        {
            snapshot_ts = TxStartTsCollector::Instance().GlobalMinSiTxStartTs();
        }

        store_range->UpdateLastAccessedTs(ClockTs());
        return store_range->PinSlices(table_name,
                                      cc_ng_term,
                                      start_key,
                                      start_inclusive,
                                      end_key,
                                      end_inclusive,
                                      key_schema,
                                      rec_schema,
                                      schema_ts,
                                      snapshot_ts,
                                      kv_info,
                                      cc_request,
                                      cc_shard,
                                      store_hd_,
                                      force_load,
                                      prefetch_size,
                                      max_pin_cnt,
                                      forward_pin,
                                      pin_status,
                                      last_pinned_slice);
    }

    uint64_t CountRanges(const TableName &table_name,
                         const NodeGroupId ng_id,
                         const NodeGroupId key_ng_id) const;

    uint64_t CountRangesLockless(const TableName &table_name,
                                 const NodeGroupId ng_id,
                                 const NodeGroupId key_ng_id) const;

    uint64_t CountSlices(const TableName &table_name,
                         const NodeGroupId ng_id,
                         const NodeGroupId local_ng_id) const;

    std::optional<std::vector<uint32_t>> GetTableRangeIds(
        const TableName &range_table_name, NodeGroupId node_group_id);

    /**
     * @brief add dirty range slices info into
     * TemplateTableRangeEntry::dirty_range_slices_.
     * @return true if "dirty_ts" match, otherwise false.
     */
    template <typename KeyT>
    bool UploadDirtyRangeSlices(const TableName &range_table_name,
                                const NodeGroupId ng_id,
                                int32_t range_id,
                                uint64_t dirty_ts,
                                int32_t new_range_id,
                                bool has_dml_since_ddl,
                                std::vector<SliceInitInfo> &&new_slices)
    {
        std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
        TemplateTableRangeEntry<KeyT> *range_entry =
            static_cast<TemplateTableRangeEntry<KeyT> *>(
                GetTableRangeEntryInternal(range_table_name, ng_id, range_id));
        assert(range_entry != nullptr);
        return range_entry->UploadDirtyRangeSlices(
            new_range_id, dirty_ts, has_dml_since_ddl, std::move(new_slices));
    }

    template <typename KeyT>
    void UpdateDirtyRangeSlice(const TableName &table_name,
                               const NodeGroupId ng_id,
                               uint32_t old_range,
                               uint32_t new_range,
                               const std::vector<uint32_t> &slice_idxs,
                               uint64_t dirty_version,
                               SliceStatus status)
    {
        std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
        TableName range_table_name(table_name.StringView(),
                                   TableType::RangePartition,
                                   table_name.Engine());
        // TxKey tx_key(&slice_key);
        TemplateTableRangeEntry<KeyT> *range_entry =
            static_cast<TemplateTableRangeEntry<KeyT> *>(
                GetTableRangeEntryInternal(range_table_name, ng_id, old_range));
        assert(range_entry != nullptr);

        range_entry->UpdateDirtyRangeSlice(
            new_range, slice_idxs, dirty_version, status);
    }

    /**
     * @brief check whether dirty range data was kicked out
     */
    template <typename KeyT>
    bool AcceptsDirtyRangeData(const TableName &table_name,
                               const NodeGroupId ng_id,
                               uint32_t old_range,
                               uint64_t dirty_version)
    {
        std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
        TableName range_table_name(table_name.StringView(),
                                   TableType::RangePartition,
                                   table_name.Engine());

        TemplateTableRangeEntry<KeyT> *range_entry =
            static_cast<TemplateTableRangeEntry<KeyT> *>(
                GetTableRangeEntryInternal(range_table_name, ng_id, old_range));
        assert(range_entry != nullptr);

        return range_entry->AcceptsDirtyRangeData(dirty_version);
    }

    void SetTxIdent(uint32_t latest_committed_txn_no);

    // Return last succ ckpt timestamp on table.
    void EnqueueDataSyncTaskForTable(
        const TableName &table_name,
        uint32_t ng_id,
        int64_t ng_term,
        uint64_t data_sync_ts,
        uint64_t &last_data_sync_ts,
        bool is_standby_node = false,
        bool is_dirty = false,
        bool can_be_skipped = false,
        std::shared_ptr<DataSyncStatus> status = nullptr,
        CcHandlerResult<Void> *hres = nullptr);

    /**
     * @brief When TxService is stopping, this function will be called.
     *
     * NOTE: This function should be called after Checkpointer::Terminate
     */
    void Terminate();

    uint64_t StatsLocalActiveSiTxs()
    {
        uint64_t min_ts = UINT64_MAX;
        for (auto &ccs : cc_shards_)
        {
            min_ts = std::min(ccs->LocalMinSiTxStartTs(), min_ts);
        }
        return min_ts;
    }

    bool EnableMvcc() const
    {
        return enable_mvcc_;
    }

    TxService *GetTxService() const
    {
        return tx_service_;
    }

    CatalogFactory *GetCatalogFactory(TableEngine table_engine) const
    {
        return catalog_factory_[static_cast<int>(table_engine) - 1];
    }

    SystemHandler *GetSystemHandler()
    {
        return system_handler_;
    }

    std::shared_ptr<TableSchema> GetSharedTableSchema(
        const TableName &table_name, NodeGroupId ng_id);

    std::shared_ptr<TableSchema> GetSharedDirtyTableSchema(
        const TableName &table_name, NodeGroupId ng_id);

    /**
     * @brief Kickout a page assigned by clean_guard.
     *
     * LocalCcShards doesn't want to expose meta_data_mux_, so the kickout
     * detail have to be implemented at here.
     */
    template <typename KeyT,
              typename ValueT,
              bool VersionedRecord,
              bool RangePartitioned>
    void KickoutPage(
        CcPageCleanGuard<KeyT, ValueT, VersionedRecord, RangePartitioned>
            *clean_guard)
    {
        const TableName &table_name = clean_guard->table_name_;
        NodeGroupId cc_ng_id = clean_guard->cc_ng_id_;
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page =
            clean_guard->page_;

        size_t page_size = page->Size();
        if (RangePartitioned)
        {
            // For range partitioned table, we need to verify range and slice
            // info to check if the key is cleanable.
            //
            // A key is not cleanable if the pin count of its belonged slice is
            // not 0.
            //
            // Consider a page that contains three ranges, and each range
            // contains three slices. To clean the page, the method iterate over
            // ranges. And in each range, it iterate over slices. This is to
            // reduce frequently searching for ranges and slices.
            //
            // |______._______._______|_______._______._______|_______._______._______|
            TableName range_table_name(table_name.StringView(),
                                       TableType::RangePartition,
                                       table_name.Engine());
            size_t idx = 0;
            do
            {
                std::shared_lock<std::shared_mutex> meta_data_lk(
                    meta_data_mux_);

                const KeyT &key = page->keys_[idx];

                // Clean next range.
                auto range_entry = static_cast<TemplateTableRangeEntry<KeyT> *>(
                    GetTableRangeEntryInternal(
                        range_table_name, cc_ng_id, TxKey(&key)));
                if (range_entry == nullptr)
                {
                    clean_guard->MarkCleanForOrphanKey(idx);
                    idx++;
                    continue;
                }

                // PinStoreRange: Prevent range_slices_from being kicked out.
                auto store_range = static_cast<TemplateStoreRange<KeyT> *>(
                    range_entry->PinStoreRange());
                if (store_range == nullptr)
                {
                    clean_guard->MarkCleanForOrphanKey(idx);
                    idx++;
                    continue;
                }

                range_entry->SetAcceptsDirtyRangeData(false);

                bool kickout_any = false;
                uint64_t dirty_range_version =
                    range_entry->GetRangeInfo()->DirtyTs();
                uint64_t last_sync_ts = range_entry->GetLastSyncTs();
                idx = clean_guard->MarkCleanInRange(
                    store_range,
                    idx,
                    kickout_any,
                    (range_entry->GetRangeInfo()->IsDirty()
                         ? &dirty_range_version
                         : nullptr),
                    last_sync_ts);
                if (kickout_any && clean_guard->cc_shard_->IsBucketsMigrating())
                {
                    // If the key is kicked out, we need to update the bucket
                    // info to disallow upload batch cc since we might already
                    // have kicked out newer version from cc map.
                    uint32_t partition_id =
                        range_entry->GetRangeInfo()->PartitionId();
                    uint16_t bucket_id =
                        Sharder::Instance().MapRangeIdToBucketId(partition_id);
                    BucketInfo *bucket_info =
                        GetBucketInfoInternal(bucket_id, cc_ng_id);
                    bucket_info->SetAcceptsUploadBatch(false);
                }

                range_entry->UnPinStoreRange();
            } while (idx != page_size);
        }
        else
        {
            // For non-range partitioned table, we just need to check if the cce
            // cleanable.
            for (size_t idx = 0; idx < page_size; idx++)
            {
                clean_guard->MarkCleanForOrphanKey(idx);
            }
            if (clean_guard->cc_shard_->IsBucketsMigrating())
            {
                // This will disallow this bucket from accepting upload batch
                // request during cluster scale since we might already have
                // kicked out newer version from cc map.
                for (size_t idx = 0; idx < page_size; ++idx)
                {
                    const KeyT &key = page->keys_[idx];
                    const auto &cce = page->entries_[idx];
                    if (cce.get() == nullptr)
                    {
                        uint16_t bucket_id =
                            Sharder::MapKeyHashToBucketId(key.Hash());
                        std::shared_lock<std::shared_mutex> s_lk(
                            meta_data_mux_);
                        GetBucketInfoInternal(bucket_id, cc_ng_id)
                            ->SetAcceptsUploadBatch(false);
                    }
                }
            }
        }
    }

    /**
     * Create table statistics and bind it to table_schema
     */
    std::pair<std::shared_ptr<Statistics>, bool> InitTableStatistics(
        TableSchema *table_schema, NodeGroupId ng_id);

    /**
     * Create table statistics and bind it to table_schema and
     * dirty_table_schema if not null.
     */
    std::pair<std::shared_ptr<Statistics>, bool> InitTableStatistics(
        TableSchema *table_schema,
        TableSchema *dirty_table_schema,
        NodeGroupId ng_id,
        std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
            sample_pool_map,
        CcShard *ccs);

    StatisticsEntry *GetTableStatistics(const TableName &table_name,
                                        NodeGroupId ng_id);

    void CleanTableStatistics(const TableName &table_name, NodeGroupId ng_id);

    void DropTableStatistics(NodeGroupId ng_id);

    void BroadcastIndexStatistics(
        TransactionExecution *txm,
        NodeGroupId ng_id,
        const TableName &table_name,
        const TableSchema *table_schema,
        const remote::NodeGroupSamplePool &sample_pool);

    void SetBucketMigrating(bool is_migrating)
    {
        buckets_migrating_.store(is_migrating, std::memory_order_release);
    }

    bool IsBucketsMigrating()
    {
        return buckets_migrating_.load(std::memory_order_relaxed);
    }

    const BucketInfo *GetBucketInfo(const uint16_t bucket_id,
                                    const NodeGroupId ng_id) const;

    BucketInfo *GetBucketInfo(const uint16_t bucket_id,
                              const NodeGroupId ng_id);

    NodeGroupId GetBucketOwner(const uint16_t bucket_id,
                               const NodeGroupId ng_id) const;

    const BucketInfo *GetRangeOwner(const int32_t range_id,
                                    const NodeGroupId ng_id) const;

    const BucketInfo *GetRangeOwnerNoLocking(const int32_t range_id,
                                             const NodeGroupId ng_id) const;

    std::vector<std::pair<uint16_t, NodeGroupId>> GetAllBucketOwners(
        NodeGroupId node_group_id);

    const std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>> *
    GetAllBucketInfos(const NodeGroupId ng_id) const;

    const std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>> *
    GetAllBucketInfosNoLocking(const NodeGroupId ng_id) const;

    void DropBucketInfo(NodeGroupId ng_id);

    void InitRangeBuckets(NodeGroupId ng_id,
                          const std::set<NodeGroupId> &node_groups,
                          uint64_t version);

    bool IsRangeBucketsInitialized(NodeGroupId ng_id);

    const BucketInfo *UploadNewBucketInfo(NodeGroupId ng_id,
                                          uint16_t bucket_id,
                                          NodeGroupId dirty_ng,
                                          uint64_t dirty_version);

    const BucketInfo *UploadBucketInfo(NodeGroupId ng_id,
                                       uint16_t bucket_id,
                                       NodeGroupId owner_ng,
                                       uint64_t version);

    bool DropStoreRangesInBucket(NodeGroupId ng_id, uint16_t bucket_id);

    std::unordered_map<TableName, std::unordered_set<int>> GetRangesInBucket(
        uint16_t bucket_id, NodeGroupId ng_id);

    const BucketInfo *CommitDirtyBucketInfo(NodeGroupId ng_id,
                                            uint16_t bucket_id);

    void EnqueueDataSyncTaskForBucket(
        const std::unordered_map<TableName, std::unordered_set<int32_t>>
            &ranges_in_bucket_snapshot,
        const std::unordered_set<TableName> &hash_partitioned_tables,
        const std::vector<uint16_t> &bucket_id,
        bool send_cache_for_migration,
        uint32_t ng_id,
        int64_t ng_term,
        uint64_t data_sync_ts,
        CcHandlerResult<Void> *hres);

    /**
     * @brief Construct one DataSyncTask for each sub-ranges of this range
     * identified by @@range_entry, and enqueue to the datasync task queue.
     *
     * @param range_entry - The old range entry.
     * @param txn - The tx_number of the split range transaction which is the
     * transaction who launch this split range actually.
     */
    void EnqueueDataSyncTaskForSplittingRange(
        const TableName &table_name,
        uint32_t ng_id,
        int64_t ng_term,
        std::shared_ptr<const TableSchema> table_schema,
        TableRangeEntry *range_entry,
        uint64_t data_sync_ts,
        bool is_dirty,
        uint64_t txn,
        CcHandlerResult<Void> *hres);

    std::pair<TableRangeEntry *, StoreRange *> PinStoreRange(
        const TableName &table_name,
        const NodeGroupId ng_id,
        int64_t ng_term,
        const TxKey &start_key,
        CcRequestBase *cc_request,
        CcShard *cc_shard);

    void InitPrebuiltTables(NodeGroupId ng_id, int64_t term);

    void PublishMessage(const std::string &chan, const std::string &message);

    using PublishArg = std::tuple<LocalCcShards *, std::string, std::string>;

    static void *Publish(void *raw_args)
    {
        std::unique_ptr<PublishArg> args_guard(
            static_cast<PublishArg *>(raw_args));

        auto [self, ch, msg] = *static_cast<PublishArg *>(raw_args);
        self->publish_func_(ch, msg);
        return nullptr;
    }

    void AddHeartbeatTargetNode(uint32_t target_node,
                                int64_t target_node_standby_term);

    void RemoveHeartbeatTargetNode(uint32_t target_node,
                                   int64_t target_node_standby_term);

    /**
     * @brief Generate bucket migration plan based on the new node group config.
     */
    std::unordered_map<NodeGroupId, BucketMigrateInfo>
    GenerateBucketMigrationPlan(const std::set<NodeGroupId> &new_node_groups);

    // Memory limit of heap memory allocated by range slices info.
    // Configurable via rangeSliceMemoryLimitPercent (default 10% of node
    // memory).
    const uint64_t range_slice_memory_limit_;

    GenerateSkStatus *GetGenerateSkStatus(NodeGroupId ng_id,
                                          uint64_t tx_number,
                                          int32_t partition_id,
                                          int64_t tx_term);
    void ClearGenerateSkStatus(NodeGroupId ng_id,
                               uint64_t tx_number,
                               int32_t partition_id);

    metrics::Meter *GetNodeMeter()
    {
        return node_meter_.get();
    }

    void FlushCurrentFlushBuffer();

    store::DataStoreHandler *const store_hd_;

    /*

    table_schema_op_pool_ and split_flush_range_op_pool_ are introduced to
    ensure the CcHandlerResult(stored in UpsertTableOp/SplitFlushRangeOp)
    pointer validation:

    if failover didn't happen, the pointer receivied from remote PostWriteAll
    response CcMessage should always be valid(memory not being freed). This is
    important in the case where network timeout happens and remote response(sent
    by retry) arrives after UpsertTableOp has finished(finished means txm has
    been reset and schema_op_ unique pointer in txm has been set to null).

    To avoid this pointer invalidation, the UpsertTableOp is moved from txm to
    local_cc_shards once finished the last step of the schema op to maintain
    the validity of the pointer. (note: this is different from pointer
    stability, which is guaranteed by checking the node term and whether this
    node is the leader of a node group).

    */
    std::vector<std::unique_ptr<UpsertTableOp>> table_schema_op_pool_;
    std::mutex table_schema_op_pool_mux_;
    std::vector<std::unique_ptr<SplitFlushRangeOp>> split_flush_range_op_pool_;
    std::mutex split_flush_range_op_pool_mux_;
    std::vector<std::unique_ptr<UpsertTableIndexOp>> table_index_op_pool_;
    std::vector<std::unique_ptr<SkGenerator>> sk_generator_pool_;
    std::mutex table_index_op_pool_mux_;

    // Since there's only 1 cluster scale event at a time across the cluster,
    // we don't need a pool for it. We just need to make sure that the op is not
    // invalidated in case late remote cc response comes in.

    std::vector<std::unique_ptr<ClusterScaleOp>> cluster_scale_op_pool_;
    std::mutex cluster_scale_op_mux_;

    std::vector<std::unique_ptr<InvalidateTableCacheCompositeOp>>
        invalidate_table_cache_op_pool_;
    std::mutex invalidate_table_cache_op_mux_;

    std::mutex data_migration_op_pool_mux_;
    std::vector<std::unique_ptr<DataMigrationOp>> migration_op_pool_;

    // protects the table ranges heap to avoid concurrent memory requests by
    // multiple threads.
    std::mutex table_ranges_heap_mux_;

private:
    static inline int32_t DefaultNextPrefetchSlice(int32_t idx, bool forward)
    {
        return forward ? (idx + 1) : (idx - 1);
    }

    void TimerRun();
    // Internal interface that exposes non const return type and does
    // not acquire mutex lock.
    TableRangeEntry *GetTableRangeEntryInternal(
        const TableName &range_table_name,
        const NodeGroupId ng_id,
        const TxKey &key);

    TableRangeEntry *GetTableRangeEntryInternal(
        const TableName &range_table_name,
        const NodeGroupId ng_id,
        int32_t range_id);

    std::unordered_map<uint32_t, TableRangeEntry *> *
    GetTableRangeIdsForATableInternal(const TableName &range_table_name,
                                      const NodeGroupId ng_id);

    std::map<TxKey, TableRangeEntry::uptr> *GetTableRangesForATableInternal(
        const TableName &range_table_name, const NodeGroupId ng_id);

    // These 2 FindRange should only be used when we need to update range and
    // slice spec during data sync of a range. StoreRange should not be accessed
    // without protection. Any access must acquire shared lock on
    // TableRangeEntry.
    StoreRange *FindRange(const TableName &table_name,
                          const NodeGroupId ng_id,
                          const TxKey &key);

    StoreRange *FindRange(const TableName &table_name,
                          const NodeGroupId ng_id,
                          int32_t range_id);

    BucketInfo *GetBucketInfoInternal(const uint16_t bucket_id,
                                      const NodeGroupId ng_id) const;

    BucketInfo *GetRangeOwnerInternal(int32_t range_id,
                                      const NodeGroupId ng_id) const;

    bool EnqueueRangeDataSyncTask(const TableName &table_name,
                                  uint32_t ng_id,
                                  int64_t ng_term,
                                  TableRangeEntry *range_entry,
                                  uint64_t data_sync_ts,
                                  bool is_dirty,
                                  bool can_be_skipped,
                                  uint64_t &last_sync_ts,
                                  std::shared_ptr<DataSyncStatus> status,
                                  CcHandlerResult<Void> *hres);
    bool EnqueueDataSyncTaskToCore(
        const TableName &table_name,
        uint32_t ng_id,
        int64_t ng_term,
        uint64_t data_sync_ts,
        uint16_t core_idx,
        bool is_standby_node,
        bool is_dirty = false,
        bool can_be_skipped = false,
        std::shared_ptr<DataSyncStatus> status = nullptr,
        CcHandlerResult<Void> *hres = nullptr,
        bool send_cache_for_migration = false,
        std::function<bool(size_t)> filter_lambda = nullptr);

    void PopPendingTask(NodeGroupId ng_id,
                        int64_t ng_term,
                        const TableName &table_name,
                        uint32_t id);

    void ClearAllPendingTasks(NodeGroupId ng_id,
                              int64_t ng_term,
                              const TableName &table_name,
                              uint32_t id);

    void PostProcessHashPartitionDataSyncTask(
        std::shared_ptr<DataSyncTask> task,
        TransactionExecution *data_sync_txm,
        DataSyncTask::CkptErrorCode ckpt_err,
        bool is_scan_task = true);

    void PostProcessRangePartitionDataSyncTask(
        std::shared_ptr<DataSyncTask> task,
        TransactionExecution *data_sync_txm,
        DataSyncTask::CkptErrorCode ckpt_err,
        bool is_scan_task = true);

    void PostProcessFlushTaskEntries(
        std::unordered_map<
            std::string_view,
            std::vector<std::unique_ptr<txservice::FlushTaskEntry>>>
            &flush_task_entries,
        DataSyncTask::CkptErrorCode ckpt_err);

    const uint32_t node_id_;
    // Native node group
    const NodeGroupId ng_id_;
    std::vector<std::unique_ptr<CcShard>> cc_shards_;

    // The background thread that periodically advances the timers of the local
    // shards to the current wall clock.
    std::thread timer_thd_;
    bool timer_terminate_;
    std::mutex timer_terminate_mux_;
    std::condition_variable timer_terminate_cv_;
    // std::atomic<bool> timer_terminate_;

    // When ccshard is full and no ccentry can be kicked-out, it will notify
    // checkpointer to do checkpoint and set flag is_wait_ckpt_ to true.
    // Subsequent ccrequest is able to skip checking freeable ccentry when
    // is_wait_ckpt_ is true. After checkpoint done, set is_wait_ckpt_ to false.
    std::atomic<bool> is_waiting_ckpt_;

    // The static variable storing the local time. It is delayed time and
    // refreshed in roughly every 2 seconds by the background thread, so as to
    // reduce the cost of calling system functions to get the wall clock. The
    // local time is used by transaction state machines to determine if a lock
    // has been held too long and if so, invoke lock recovery.
    static std::atomic<uint64_t> local_clock;

    // The base timestamp  which will be adjust by local clock and commit
    // timestamp of transactions on all ccshards to keep it up to date.
    std::atomic<uint64_t> ts_base_;

    // catalog factory for each table engine
    CatalogFactory *catalog_factory_[5]{nullptr,
                                        nullptr,
                                        nullptr,
                                        &hash_catalog_factory_,
                                        &range_catalog_factory_};

    SystemHandler *const system_handler_;

    std::unordered_map<TableName, std::unordered_map<NodeGroupId, CatalogEntry>>
        table_catalogs_;  // string owner

    // map<table name, map<partition id, range record>>
    std::unordered_map<TableName,
                       std::unordered_map<NodeGroupId,
                                          std::map<TxKey,
                                                   TableRangeEntry::uptr>>>
        table_ranges_;  // string owner

    // map from range id to TableRangeEntry. TableRangeEntry* here is the
    // pointer to TableRangeEntry in table_ranges_. This map is used as a
    // fast path from range id to table range in PinRangeSlice so that we
    // can avoid doing a binary search with TxKey.
    std::unordered_map<
        TableName,
        std::unordered_map<NodeGroupId,
                           std::unordered_map<uint32_t, TableRangeEntry *>>>
        table_range_ids_;

    std::unordered_map<TableName,
                       std::unordered_map<NodeGroupId, StatisticsEntry>>
        table_statistics_map_;

    // map to store mapping relationship from bucket id to bucket info
    // that stores the bucket owner of this bucket.
    // TODO(zc) make inner map vector
    std::unordered_map<
        NodeGroupId,
        std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>>>
        bucket_infos_;

    // heap memory resource used by table ranges.
    mi_heap_t *table_ranges_heap_{nullptr};
    mi_threadid_t table_ranges_thread_id_{0};

    uint32_t table_ranges_arena_id_{0};
    uint32_t hash_partition_ckpt_arena_id_{0};

    std::atomic_bool buckets_migrating_{false};

    std::unordered_map<TableName, std::string> prebuilt_tables_;

    TxService *tx_service_;

    bool enable_mvcc_;

    bool realtime_sampling_;

    struct RangeSplitTask
    {
        RangeSplitTask(std::shared_ptr<DataSyncTask> data_sync_task,
                       std::shared_ptr<const TableSchema> schema,
                       std::vector<TxKey> &&split_keys,
                       TableRangeEntry *range_entry,
                       TransactionExecution *data_sync_txm,
                       std::shared_ptr<void> defer_unpin)
            : schema_(schema),
              split_keys_(std::move(split_keys)),
              range_entry_(range_entry),
              data_sync_task_(data_sync_task),
              data_sync_txm_(data_sync_txm),
              defer_unpin_(defer_unpin)
        {
        }

        std::shared_ptr<const TableSchema> schema_;

        std::vector<TxKey> split_keys_;

        TableRangeEntry *range_entry_{nullptr};

        // Increased by worker after finishing the retrieved work.
        std::shared_ptr<DataSyncTask> data_sync_task_{nullptr};
        TransactionExecution *data_sync_txm_{nullptr};
        std::shared_ptr<void> defer_unpin_{nullptr};
    };

    WorkerThreadContext range_split_worker_ctx_;
    std::deque<std::unique_ptr<RangeSplitTask>> pending_range_split_task_;
    void RangeSplitWorker();

    struct UpdateSliceStatus
    {
        void Reset()
        {
            paused_slice_ = nullptr;
            paused_slice_rec_cnt_ = 0;
            paused_split_keys_.clear();
        }

        void SetPausedPos(StoreSlice *slice,
                          size_t record_cnt,
                          std::vector<SliceChangeInfo> &&split_keys)
        {
            paused_slice_ = slice;
            paused_slice_rec_cnt_ = record_cnt;
            paused_split_keys_ = std::move(split_keys);
        }

        StoreSlice *paused_slice_{nullptr};
        size_t paused_slice_rec_cnt_{0};
        std::vector<SliceChangeInfo> paused_split_keys_;
    };

    struct RangeCacheSender
    {
        static constexpr uint32_t BATCH_BYTES_SIZE = 0x100000;
        RangeCacheSender(const TableName &table_name,
                         const TxKey &range_start_key,
                         NodeGroupId ng_id,
                         TableRangeEntry *range_entry)
            : table_name_(table_name), ng_id_(ng_id)
        {
            assert(range_entry);
            new_range_id_ =
                range_entry->GetRangeInfo()->GetKeyNewRangeId(range_start_key);
            old_range_id_ = range_entry->GetRangeInfo()->PartitionId();
            version_ts_ = range_entry->GetRangeInfo()->DirtyTs();
            assert(version_ts_ > 0);
            new_range_owner_ = Sharder::Instance()
                                   .GetLocalCcShards()
                                   ->GetRangeOwner(new_range_id_, ng_id_)
                                   ->BucketOwner();
            assert(new_range_owner_ != ng_id_);

            dest_node_id_ = Sharder::Instance().LeaderNodeId(new_range_owner_);
            channel_ =
                Sharder::Instance().GetCcNodeServiceChannel(dest_node_id_);
            store_range_ = range_entry->RangeSlices();
        }

        /**
         * @brief Find the (sub)slice keys for this batch flush records.
         *
         * @param update_slice_status - If the content of this value is not
         * null, is means that the slice to which the last part of this batch
         * records belong need to be split.
         */
        void FindSliceKeys(const std::vector<FlushRecord> &batch_records,
                           const UpdateSliceStatus &update_slice_status);

        /**
         * @brief Encode the FlushRecord for this batch into an UploadBatchSlice
         * request. The records within a slice (the slice is the new slice, that
         * is, if the old slice needs to be split, the slice is a subslice) are
         * always encoded into one request. Therefore, regardless of whether the
         * slice corresponding to the last part of the batch of records needs to
         * be split, the information of this slice needs to be stored in the
         * paused_pos_, unless the value of @@all_data_exported is true.
         *
         */
        void AppendSliceDataRequest(
            const std::vector<FlushRecord> &batch_records,
            bool all_data_exported);

        void SendRangeCacheRequest(const TxKey &start_key,
                                   const TxKey &end_key);

        struct PausedPosition
        {
            void SetPausedPos(uint32_t batch_size, TxKey &&start_key)
            {
                paused_batch_size_ = batch_size;
                paused_slice_start_key_ = std::move(start_key);
            }

            void Reset()
            {
                paused_batch_size_ = 0;
                paused_slice_start_key_ = TxKey();
            }

            uint32_t paused_batch_size_{0};
            // The start key of the paused slice which is the new slice (i.e.
            // the slice before it was split).
            TxKey paused_slice_start_key_;
        };

        const TableName &table_name_;
        NodeGroupId ng_id_;
        int32_t new_range_id_;
        int32_t old_range_id_;
        uint64_t version_ts_;
        NodeGroupId new_range_owner_;
        uint32_t dest_node_id_;
        StoreRange *store_range_{nullptr};
        std::shared_ptr<brpc::Channel> channel_{nullptr};
        // Slices index in new range.
        uint32_t slice_idx_{0};
        // Because we don't know whether the slice to which the last part of the
        // current batch of data is located has been completely scanned, we need
        // to save the information of this slice using this variable.
        PausedPosition paused_pos_;
        std::vector<TxKey> batch_slice_key_vec_;
        std::vector<const StoreSlice *> slices_vec_;
        std::shared_ptr<std::vector<UploadBatchSlicesClosure *>> closure_vec_{
            nullptr};
    };

    /**
     * DataSync Operation Interface
     */
    WorkerThreadContext data_sync_worker_ctx_;
    std::vector<bool> data_sync_done_;
    std::atomic<size_t> scan_concurrency_{1};
    size_t last_data_sync_worker_idx_{0};

    std::vector<std::deque<std::shared_ptr<DataSyncTask>>>
        data_sync_task_queue_;

    struct DataSyncMemoryController
    {
        explicit DataSyncMemoryController(uint64_t mem_quota)
            : flush_data_mem_usage_(0), flush_data_mem_quota_(mem_quota)
        {
        }

        DataSyncMemoryController(const DataSyncMemoryController &rhs) = delete;
        DataSyncMemoryController(DataSyncMemoryController &&rhs)
            : flush_data_mem_usage_(rhs.flush_data_mem_usage_),
              flush_data_mem_quota_(rhs.flush_data_mem_quota_)
        {
        }

        // Function to allocate memory quota
        uint64_t AllocateFlushDataMemQuota(uint64_t quota)
        {
            std::unique_lock<bthread::Mutex> lk(mem_mutex_);

            // Lambda to check if there's enough available memory
            auto has_enough_memory = [this, quota]()
            {
                // if quota is avaliable
                if ((flush_data_mem_usage_ + quota) <= flush_data_mem_quota_)
                {
                    return true;
                }
                // Or a single object quota is bigger than overall quota which
                // means the object is also bigger than scan heap limit, we have
                // to allow it to be flushed, otherwise it will block ckpt
                if (quota > flush_data_mem_quota_)
                {
                    LOG(WARNING)
                        << "Flush object is too large (size: " << quota
                        << ") which excceds the flush data mem quota (size: "
                        << flush_data_mem_quota_ << ")";
                    return true;
                }

                return false;
            };

            // Wait until enough memory is available
            while (!has_enough_memory())
            {
                DLOG(INFO) << "Flush data memory quota is full "
                           << flush_data_mem_usage_
                           << " ,request quota: " << quota
                           << " total quota: " << flush_data_mem_quota_;
                mem_cv_.wait(lk);
            }

            // Allocate the memory quota
            uint64_t old_usage = flush_data_mem_usage_;
            flush_data_mem_usage_ += quota;
            return old_usage;
        }

        // return the quota to flush data memory usage pool and notify waiting
        // data sync thread
        uint64_t DeallocateFlushMemQuota(uint64_t quota)
        {
            std::lock_guard<bthread::Mutex> lock(mem_mutex_);

            assert(quota <= flush_data_mem_usage_);

            // Deallocate the memory quota
            uint64_t old_usage = flush_data_mem_usage_;
            flush_data_mem_usage_ -= quota;

            // Notify all waiting threads that memory has been freed
            mem_cv_.notify_all();

            return old_usage;
        }

        uint64_t FlushMemoryQuota() const
        {
            return flush_data_mem_quota_;
        }

    private:
        // Accumulated pending flush data memory usage for back pressure the
        // DataSyncScan
        // Synchronization primitives
        bthread::Mutex mem_mutex_;
        bthread::ConditionVariable mem_cv_;
        // Memory usage tracking
        uint64_t flush_data_mem_usage_{0};
        uint64_t flush_data_mem_quota_{0};
    };

    struct DataSyncTaskLimiter
    {
        // `0` means no pending task
        uint64_t latest_pending_task_ts_{0};
        std::queue<std::shared_ptr<DataSyncTask>> pending_tasks_;

        uint64_t UnsetLatestPendingTs()
        {
            uint64_t ts = latest_pending_task_ts_;
            latest_pending_task_ts_ = 0;
            return ts;
        }
    };

    struct TaskLimiterKey
    {
        explicit TaskLimiterKey(NodeGroupId node_group_id,
                                int64_t node_group_term,
                                std::string_view table_name,
                                TableType table_type,
                                TableEngine table_engine,
                                uint32_t id)
            : node_group_id_(node_group_id),
              node_group_term_(node_group_term),
              table_name_(table_name, table_type, table_engine),
              id_(id)
        {
        }

        TaskLimiterKey(const TaskLimiterKey &rhs)
            : node_group_id_(rhs.node_group_id_),
              node_group_term_(rhs.node_group_term_),
              // deep copy
              table_name_(rhs.table_name_.StringView().data(),
                          rhs.table_name_.StringView().size(),
                          rhs.table_name_.Type(),
                          rhs.table_name_.Engine()),
              id_(rhs.id_)
        {
        }

        TaskLimiterKey &operator=(const TaskLimiterKey &) = delete;
        TaskLimiterKey(TaskLimiterKey &&) = delete;
        TaskLimiterKey &operator=(TaskLimiterKey &&) = delete;

        bool operator==(const TaskLimiterKey &other) const
        {
            return node_group_id_ == other.node_group_id_ &&
                   node_group_term_ == other.node_group_term_ &&
                   table_name_ == other.table_name_ && id_ == other.id_;
        }

        NodeGroupId node_group_id_;
        int64_t node_group_term_;
        TableName table_name_;
        uint32_t id_;
    };

    struct LimiterKeyHasher
    {
        size_t operator()(const TaskLimiterKey &key) const
        {
            size_t h1 = std::hash<NodeGroupId>()(key.node_group_id_);
            size_t h2 = std::hash<int64_t>()(key.node_group_term_);
            size_t h3 = std::hash<TableName>()(key.table_name_);
            size_t h4 = std::hash<uint32_t>()(key.id_);
            return h1 ^ (h2 << 1) ^ (h3 << 3) ^ (h4 << 5);
        }
    };

    std::mutex task_limiter_mux_;
    std::unordered_map<TaskLimiterKey,
                       std::shared_ptr<DataSyncTaskLimiter>,
                       LimiterKeyHasher>
        task_limiters_;

    void DataSyncWorker(size_t worker_idx);

#if defined(WITH_JEMALLOC)
    void EpochWorker();
#endif

    void DataSyncForHashPartition(std::shared_ptr<DataSyncTask> data_sync_task,
                                  size_t worker_idx);

    void DataSyncForRangePartition(std::shared_ptr<DataSyncTask> data_sync_task,
                                   size_t worker_idx);

    /**
     * Range & Slice Update Interface
     */

    /**
     * @brief Update the post ckpt size of slices that contain unpersisted data.
     */
    void UpdateSlicePostCkptSize(
        StoreRange *store_range,
        const std::map<TxKey, int64_t> &slices_delta_size);

    /**
     * @brief Decide the range update plan based on the @@slices_delta_size
     * parameter.
     */
    bool CalculateRangeUpdate(const TableName &table_name,
                              NodeGroupId node_group_id,
                              int64_t node_group_term,
                              uint64_t data_sync_ts,
                              StoreRange *store_range,
                              std::vector<TxKey> &splitting_info);

    /**
     * @brief Decide if the slice needs to be updated(merge/split). Update slice
     * info accordingly if all the data of this slice are scanned, but does not
     * update the actual slice size since the data is not flushed yet.
     *
     * @param all_data_exported - True if the scan finished for a DataSync task.
     * @param data_sync_vec - The vector of current batch data sync data.
     * @param status - Only to update a slice spec in case that all data of the
     * slice are exported in the @@data_sync_vec. Otherwise, store the status of
     * this slice, and continue to process it in the next batch data.
     */
    void UpdateSlices(const TableName &table_name,
                      const TableSchema *schema,
                      StoreRange *store_range,
                      bool all_data_exported,
                      const std::vector<FlushRecord> &data_sync_vec,
                      UpdateSliceStatus &status);
    /**
     * @brief Worker thread that split the target range and flush the data into
     * data store in their new partitions. This is called during checkpoint on a
     * table, after this function returns, we can assume the splitting ranges
     * are flushed too.
     */
    void SplitFlushRange(std::unique_lock<std::mutex> &task_worker_lk);

    /**
     * @brief Called after data sync is done. Update data store slice size
     * in memory and in data store. Reset post ckpt size in store slice.
     */
    bool UpdateStoreSlice(const TableName &tbl_name,
                          uint64_t ckpt_ts,
                          TableRangeEntry *range_entry,
                          const TxKey *start_key,
                          const TxKey *end_key,
                          bool flush_res);

    bool UpdateStoreSlices(std::vector<FlushTaskEntry *> &task);

    bool GetNextRangePartitionId(const TableName &tablename,
                                 const TableSchema *table_schema,
                                 uint32_t range_cnt,
                                 int32_t &out_next_partition_id);

    /**
     * @brief Map a data_sync_worker index to the fixed flush_data_worker index.
     * Used when data_sync_worker count != flush_data_worker count so that each
     * data_sync_worker consistently targets one flush_data_worker.
     */
    size_t DataSyncWorkerToFlushDataWorker(size_t data_sync_worker_id) const;

    /**
     * @brief Add a flush task entry to the flush task. If the there's no
     * pending flush task, create a new flush task and add the entry to it.
     * Otherwise, add the entry to the current flush task. If the current flush
     * task is full, append it to pending_flush_work_ and create a new flush
     * task.
     */
    void AddFlushTaskEntry(std::unique_ptr<FlushTaskEntry> &&entry);

    WorkerThreadContext flush_data_worker_ctx_;

    // Per-worker flush buffers. Each DataSyncWorker has its own buffer.
    // New flush task entry will be added to the corresponding buffer. This task
    // will be appended to pending_flush_work_[worker_idx] when it reaches the
    // max pending flush size, which will then be processed by the corresponding
    // flush data worker. Store as pointers because FlushDataTask contains a
    // bthread::Mutex and is non-movable/non-copyable, which cannot be stored
    // directly in a vector that may reallocate.
    std::vector<std::unique_ptr<FlushDataTask>> cur_flush_buffers_;
    // Per-worker flush task queues. Each FlushDataWorker processes its
    // corresponding queue.
    std::vector<std::deque<std::unique_ptr<FlushDataTask>>> pending_flush_work_;

    void FlushDataWorker(size_t worker_idx);
    void FlushData(std::unique_lock<std::mutex> &flush_worker_lk,
                   size_t worker_idx);

    // C++20 coroutine: single FlushDataWorker drives multiple flush tasks.
    struct FlushDataTaskCo
    {
        struct promise_type
        {
            FlushDataTaskCo get_return_object()
            {
                return FlushDataTaskCo{
                    std::coroutine_handle<promise_type>::from_promise(*this)};
            }
            std::suspend_always initial_suspend()
            {
                return {};
            }
            std::suspend_always final_suspend() noexcept
            {
                return {};
            }
            void return_void()
            {
            }
            void unhandled_exception()
            {
                std::terminate();
            }
        };
        std::coroutine_handle<promise_type> handle_;
        explicit FlushDataTaskCo(std::coroutine_handle<promise_type> h)
            : handle_(h)
        {
        }
        FlushDataTaskCo(FlushDataTaskCo &&other) noexcept
            : handle_(other.handle_)
        {
            other.handle_ = nullptr;
        }
        FlushDataTaskCo &operator=(FlushDataTaskCo &&other) noexcept
        {
            if (this != &other)
            {
                if (handle_)
                    handle_.destroy();
                handle_ = other.handle_;
                other.handle_ = nullptr;
            }
            return *this;
        }
        FlushDataTaskCo(const FlushDataTaskCo &) = delete;
        FlushDataTaskCo &operator=(const FlushDataTaskCo &) = delete;
        ~FlushDataTaskCo()
        {
            if (handle_)
                handle_.destroy();
        }
        bool valid() const
        {
            return handle_ != nullptr;
        }
        bool done() const
        {
            return handle_ && handle_.done();
        }
        void resume()
        {
            if (handle_ && !handle_.done())
                handle_.resume();
        }
        // Release ownership of the handle to the scheduler; caller must
        // destroy the handle when the coroutine is done.
        std::coroutine_handle<> ReleaseHandle()
        {
            std::coroutine_handle<> h =
                std::coroutine_handle<>::from_address(handle_.address());
            handle_ = nullptr;
            return h;
        }
    };
    static FlushDataTaskCo FlushDataCoroutine(
        LocalCcShards *self, std::unique_ptr<FlushDataTask> cur_work);

    // Ready queue: store callbacks post coroutine handles here; FlushDataWorker
    // drains and resumes. Only FlushDataWorker thread calls resume()/destroy().
    std::mutex flush_ready_handles_mux_;
    std::deque<std::coroutine_handle<>> flush_ready_handles_;
    void PostFlushReadyHandle(std::coroutine_handle<> h);

    // Awaitable for PutAllAsync: suspends until store callback posts handle.
    struct PutAllAwaitable
    {
        LocalCcShards *self;
        std::unordered_map<std::string_view,
                           std::vector<std::unique_ptr<FlushTaskEntry>>>
            &flush_task_entries;
        std::shared_ptr<std::optional<bool>> result_;

        bool await_ready() const
        {
            return false;
        }
        void await_suspend(std::coroutine_handle<> h)
        {
            result_ = std::make_shared<std::optional<bool>>();
            self->store_hd_->PutAllAsync(
                flush_task_entries,
                [self = this->self, h, result = result_](bool ok)
                {
                    result->emplace(ok);
                    self->PostFlushReadyHandle(h);
                });
        }
        bool await_resume()
        {
            return result_->value_or(false);
        }
    };
    PutAllAwaitable PutAllAsync(
        std::unordered_map<std::string_view,
                           std::vector<std::unique_ptr<FlushTaskEntry>>>
            &flush_task_entries);

    // Memory controller for data sync.
    DataSyncMemoryController data_sync_mem_controller_;

    WorkerThreadContext statistics_worker_ctx_;
    void SyncTableStatisticsWorker();

    absl::flat_hash_map<uint32_t, int64_t> heartbeat_target_nodes_;
    WorkerThreadContext heartbeat_worker_ctx_;
    void HeartbeatWorker();

    void SendHeartbeat(std::unique_lock<std::mutex> &worker_lk);

    WorkerThreadContext purge_deleted_worker_ctx_;
    void PurgeDeletedData();

    WorkerThreadContext kickout_data_test_worker_ctx_;
    void KickoutDataForTest();

#if defined(WITH_JEMALLOC)
    WorkerThreadContext epoch_worker_ctx_;
#endif

    void ReportHashPartitionCkptHeapUsage();

    /**
     * Generate sk from pk
     */
    std::mutex generate_sk_mux_;
    using RangeGenerateSkStatus = std::unordered_map<int32_t, GenerateSkStatus>;
    using TxGenerateSkStatus =
        std::unordered_map<uint64_t, RangeGenerateSkStatus>;
    std::unordered_map<NodeGroupId, TxGenerateSkStatus> generate_sk_status_;

    // For cluster Publish message
    std::function<void(std::string_view, std::string_view)> publish_func_;

    // If enable/disable shard heap defragment
    bool enable_shard_heap_defragment_{false};

    std::unique_ptr<metrics::Meter> node_meter_{nullptr};

    EloqHashCatalogFactory hash_catalog_factory_;
    EloqRangeCatalogFactory range_catalog_factory_;
    // Protects meta data (table_ranges_ and table_catalogs_)
    mutable std::shared_mutex meta_data_mux_;
    mutable struct FastMetaDataMutex
    {
        explicit FastMetaDataMutex(size_t size,
                                   std::shared_mutex &meta_data_mux)
            : mux_ptrs_(size), meta_data_mux_(meta_data_mux)
        {
        }

        void lock();
        bool try_lock() = delete;
        void unlock();
        void lock_shared();
        bool try_lock_shared() = delete;
        void unlock_shared();
        static constexpr int32_t kWriterMask = static_cast<int32_t>(1u << 31);

        std::vector<std::atomic<int32_t> *> mux_ptrs_;
        std::shared_mutex &meta_data_mux_;
    } fast_meta_data_mux_;

    mi_heap_t *hash_partition_ckpt_heap_{nullptr};
    mi_threadid_t hash_partition_main_thread_id_{0};
    std::mutex hash_partition_ckpt_heap_mux_;

    friend class LocalCcHandler;
    friend class remote::RemoteCcHandler;
    friend class Checkpointer;
    friend class txservice::fault::RecoveryService;
    friend class CcShard;
    friend class CcShardHeap;
};
}  // namespace txservice
