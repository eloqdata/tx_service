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

#include <bthread/moodycamelqueue.h>
#include <butil/time.h>
#include <mimalloc-2.1/mimalloc.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "catalog_factory.h"
#include "catalog_key_record.h"
#include "cc/non_blocking_lock.h"
#include "cc_entry.h"
#include "cc_map.h"
#include "cc_req_base.h"
#include "cc_req_misc.h"
#include "cc_req_pool.h"
#include "cc_request.pb.h"
#include "cc_stream_sender.h"
#include "error_messages.h"
#include "meter.h"
#include "metrics.h"
#include "range_bucket_key_record.h"
#include "range_record.h"
#include "reader_writer_cntl.h"
#include "sharder.h"
#include "standby.h"
#include "store/data_store_handler.h"
#include "system_handler.h"
#include "tentry.h"
#include "tx_key.h"
#include "tx_service_common.h"

namespace txservice
{
class SingleShardScanner;
class CcMapScanner;
class TxProcessor;
class TxService;
class Checkpointer;
class LocalCcShards;
struct StatisticsEntry;
struct CheckDeadLockResult;
struct DefragShardHeapCc;
struct RetryFailedStandbyMsgCc;
struct ShardCleanCc;

namespace remote
{
class CcStreamSender;
};

#define LOCK_VECTOR_SHRINK_THRESHOLD 2u
#define RESIZE_LOCK_LIMIT 3u
#define LOCK_ARRAY_INIT_SIZE 8192u

// store table catalog information in ccshard
class TableCatalog
{
public:
    TableCatalog() : table_catalog_info_(""), table_catalog_version_("")
    {
    }

    // content of table catalog, same as frm file
    std::string table_catalog_info_;
    // catalog version
    std::string table_catalog_version_;
};

struct InitCcmResult
{
    bool success{false};
    CcErrorCode error{CcErrorCode::NO_ERROR};
    const TableSchema *schema{nullptr};

    static InitCcmResult Success(const TableSchema *schema_ptr)
    {
        return InitCcmResult{true, CcErrorCode::NO_ERROR, schema_ptr};
    }

    static InitCcmResult Failure(CcErrorCode error_code)
    {
        return InitCcmResult{false, error_code, nullptr};
    }

    static InitCcmResult Retry()
    {
        return InitCcmResult{};
    }
};

struct TxLockInfo
{
    using uptr = std::unique_ptr<TxLockInfo>;

    TxLockInfo() = delete;
    explicit TxLockInfo(int64_t tx_coord_term)
        : tx_coord_term_(tx_coord_term),
          wlock_ts_(0),
          last_recover_ts_(0),
          cce_list_(),
          table_type_(TableType::Primary),
          next_(nullptr)
    {
    }

    ~TxLockInfo()
    {
        // Uses the loop to deallocate the list to avoid recursive deallocation
        // and stack overflow.
        while (next_ != nullptr)
        {
            next_ = std::move(next_->next_);
        }
    }

    void Reset(int64_t tx_term)
    {
        tx_coord_term_ = tx_term;
        wlock_ts_ = 0;
        last_recover_ts_ = 0;
        cce_list_.clear();
        table_type_ = TableType::Primary;
        next_ = nullptr;
    }

    // tx coordinator's term.
    int64_t tx_coord_term_;
    // The timestamp when the tx acquires the first write lock in the cc shard.
    // If tx has not acquired any write lock, set wlock_ts_ to 0.
    uint64_t wlock_ts_;
    // The last time when the tx is recovered or the tx acquired the latest
    // lock.
    uint64_t last_recover_ts_;
    // A list of cc entries on which the tx has acquired write/read locks.
    absl::flat_hash_set<LruEntry *> cce_list_;
    // This cc map type is used to skip the meta table(such as: catalog, range)
    // during get ActiveTxMinTs()
    TableType table_type_;

    std::unique_ptr<TxLockInfo> next_{nullptr};
};

class CcShardHeap
{
    static constexpr double high_water = 0.8;
    static constexpr double utilization = 0.8;

public:
    CcShardHeap(CcShard *cc_shard, size_t limit);
    ~CcShardHeap();

    // Set this heap_ as the default heap for the current thread.
    // return the previous default heap.
    mi_heap_t *SetAsDefaultHeap();

    // Check if this heap is full
    bool Full(int64_t *alloc = nullptr, int64_t *commit = nullptr) const;

    bool NeedCleanShard(int64_t &alloc, int64_t &commit) const;

    bool NeedDefragment(int64_t *alloc, int64_t *commit) const;

    mi_heap_t *Heap() const
    {
        return heap_;
    }

    size_t Threshold() const
    {
        return memory_limit_;
    }

    // Perform defragmentation asynchronously.
    bool AsyncDefragment();

    bool IsDefragHeapCcOnFly() const
    {
        return defrag_heap_cc_on_fly_;
    }

    void SetDefragHeapCcOnFly(bool on_fly)
    {
        defrag_heap_cc_on_fly_ = on_fly;
    }

private:
    CcShard *cc_shard_{nullptr};
    mi_heap_t *heap_{nullptr};
    const size_t memory_limit_{0};
    size_t last_failed_collect_ts_{0};

    // defrag heap cc for this shard
    std::unique_ptr<DefragShardHeapCc> defrag_heap_cc_{nullptr};
    // indicating the per shard defrag heap cc is on fly
    bool defrag_heap_cc_on_fly_{false};
};

class CcShard
{
public:
    CcShard() = delete;
    CcShard(const CcShard &other) = delete;
    ~CcShard();

    CcShard(uint16_t core_id,
            uint32_t core_cnt,
            uint32_t node_memory_limit_mb,
            bool realtime_sampling,
            uint32_t native_ng_id,
            LocalCcShards &local_shards,
            CatalogFactory *catalog_factory[6],
            SystemHandler *system_handler,
            std::unordered_map<uint32_t, std::vector<NodeConfig>> *ng_configs,
            uint64_t cluster_config_version,
            metrics::MetricsRegistry *metrics_registry = nullptr,
            metrics::CommonLabels common_labels = {},
            uint32_t range_slice_memory_limit_percent = 10);

    void Init();

    /**
     * @brief Returns the cc map at this shard given the table name and the cc
     * node group.
     *
     * @param table_name The table name.
     * @param node_group The ID of the cc node group.
     * @return CcMap* The pointer to the cc map.
     */
    CcMap *GetCcm(const TableName &table_name, uint32_t node_group);

    void InitializeShardHeap()
    {
        if (shard_heap_thread_id_ == 0)
        {
            shard_heap_thread_id_ = mi_thread_id();
        }

        if (!shard_heap_)
        {
            shard_heap_ =
                std::make_unique<CcShardHeap>(this, memory_limit_ * 0.9);
        }

        if (!shard_data_sync_scan_heap_)
        {
            // 10% of the shard heap is reserved for ckpt
            // The distribution of ckpt memory
            // 25% for data sync scan cc
            // 75% for flush data task
            shard_data_sync_scan_heap_ =
                std::make_unique<CcShardHeap>(this, memory_limit_ * 0.1 * 0.25);
        }
    }

    void OverrideHeapThread()
    {
        mi_override_thread(shard_heap_thread_id_);
    }

    mi_threadid_t GetShardHeapThreadId()
    {
        return shard_heap_thread_id_;
    }

    CcShardHeap *GetShardHeap()
    {
        return shard_heap_.get();
    }

    CcShardHeap *GetShardDataSyncScanHeap()
    {
        return shard_data_sync_scan_heap_.get();
    }

    /**
     * @brief Puts a cc request into the shard's request queue to be processed.
     *
     * @param thd_id The thread ID of the producer sending the cc request.
     * Providing the thread ID helps reduce contention, as internally the
     * concurrent queue uses it to dispatch the request to an internal storage
     * allocated for the thread.
     * @param req The pointer to the cc request. The request is either owned by
     * a resource pool or a stack object whose owner thread is blocking on the
     * request.
     */
    void Enqueue(uint32_t thd_id, CcRequestBase *req);

    void Enqueue(uint32_t thd_id, uint32_t shard_code, CcRequestBase *req);

    /**
     * @brief Puts a cc request into the shard's request wait list until memory
     * is avaliable.
     * @param req The pointer to the cc request.
     */
    void EnqueueWaitListIfMemoryFull(CcRequestBase *req);
    /**
     * @brief Dequeue cc requests from the shard's request wait list to process.
     * @param deque_all If true, dequeue all the requests in the wait list.
     * @return True if the wait list is empty, false otherwise.
     */
    bool DequeueWaitListAfterMemoryFree(bool deque_all = false);
    /**
     * @brief Abort the cc requests whose tx is holding a range read lock.
     */
    void AbortRequestsAfterMemoryFree();

    size_t WaitListSizeForMemory();

    void WakeUpShardCleanCc();

    /**
     * @brief Puts a cc request into the shard's request queue to be processed.
     *
     * @param req The pointer to the cc request.
     */
    void Enqueue(CcRequestBase *req);

    /**
     * @brief Puts a cc request into the shard's low priority request queue to
     * be processed. This API is specifically for background job requests.
     *
     * @param thd_id The thread ID of the producer sending the cc request.
     * @param req The pointer to the cc request.
     */
    void EnqueueLowPriorityCcRequest(uint32_t thd_id, CcRequestBase *req);

    /**
     * @brief Puts a cc request into the shard's low priority request queue to
     * be processed. This API is specifically for background job requests.
     *
     * @param req The pointer to the cc request.
     */
    void EnqueueLowPriorityCcRequest(CcRequestBase *req);

    void AbortCcRequests(std::vector<CcRequestBase *> &&reqs,
                         CcErrorCode err_code);

    bool IsIdle()
    {
        return cc_queue_size_.load(std::memory_order_relaxed) == 0;
    }

    size_t ProcessRequests();

    /**
     * @brief Find an available TEntry in tranaction array and initialize it.
     *
     */
    TEntry &NewTx(NodeGroupId tx_ng_id, uint32_t log_group_id, int64_t term);

    /**
     * @brief Find an available NonBlockingLock in lock array and initialize it.
     *
     */
    KeyGapLockAndExtraData *NewLock(CcMap *ccm, LruPage *page, LruEntry *entry);

    TEntry *LocateTx(const TxId &tx_id);

    /**
     * @brief Given the tx number, returns the tx entry that describes the tx
     * status.
     *
     * @param tx_number
     * @return TEntry* The pointer to the tx entry.
     */
    TEntry *LocateTx(TxNumber tx_number);

    /**
     * Clean ccentry through the lru list
     * @return A pair, of which the first is the clean count, the second is
     * whether reach to the end of the lru list.
     */
    std::pair<size_t, bool> Clean();

    bool FlushEntryForTest(
        std::unordered_map<std::string_view,
                           std::vector<std::unique_ptr<FlushTaskEntry>>>
            &flush_task_entries,
        bool only_archives);

    /**
     * @brief Notify the checkpoint thread to do checkpoint.
     * @param request_ckpt If true, request a new checkpoint. If false, just
     * notify the checkpoint thread to check whether there is a pending
     * checkpoint request.
     */
    void NotifyCkpt(bool request_ckpt = true);

    /**
     * @brief Dispatch heavy cpu-bound task, e.g. StoreRange::LoadSlice().
     * @param cc_shard_idx Execute the task on which cc_shard.
     * @param task A cpu-bound task.
     */
    void DispatchTask(uint16_t cc_shard_idx,
                      std::function<bool(CcShard &)> task);

    /**
     * @brief Get the number of ccentries in this ccshard
     *
     */
    size_t Size() const
    {
        return size_;
    }

    uint16_t LocalCoreId() const
    {
        return core_id_;
    }

    uint32_t GlobalCoreId(NodeGroupId ng_id) const
    {
        // The global core ID is a combination of node group ID and the local
        // core ID.
        return (ng_id << 10) | core_id_;
    }

    uint64_t Now() const;
    uint64_t NowInMilliseconds() const;
    void UpdateTsBase(uint64_t ts);

    size_t QueueSize()
    {
        return cc_queue_size_.load(std::memory_order_relaxed);
    }

    CatalogFactory *GetCatalogFactory(TableEngine table_engine)
    {
        return catalog_factory_[static_cast<int>(table_engine) - 1];
    }

    /**
     * Insert page at the end of the lru list as the most-recently accessed
     * page.
     * @param page
     */
    void UpdateLruList(LruPage *page, bool is_emplace);

    /**
     * Detaches the page from the double linked list. This function is invoked
     * in UpdateLruList or when the cc page is to be kicked out.
     * @param page
     */
    void DetachLru(LruPage *page);

    /**
     * Replace the old page with new page in Lru list
     */
    void ReplaceLru(LruPage *old_page, LruPage *new_page);

    TxLockInfo *UpsertLockHoldingTx(TxNumber txn,
                                    int64_t tx_term,
                                    LruEntry *cce_ptr,
                                    bool is_key_write_lock,
                                    NodeGroupId cc_ng_id,
                                    TableType table_type);

    void DeleteLockHoldingTx(TxNumber txn,
                             LruEntry *cce_ptr,
                             NodeGroupId cc_ng_id);

    void DropLockHoldingTxs(NodeGroupId cc_ng_id);

    void VerifyOrphanLock(NodeGroupId cc_ng_id, TxNumber txn)
    {
        auto locks_it = lock_holding_txs_.find(cc_ng_id);
        if (locks_it == lock_holding_txs_.end())
        {
            return;
        }

        auto tx_it = locks_it->second.find(txn);
        if (tx_it != locks_it->second.end())
        {
            LOG(ERROR) << "txn #" << txn
                       << " has orphan lock(s) after finishing.";
            assert("Orphan lock detected.");
        }
    }

    /**
     * @brief When a tx fails to acquire a lock, it invokes this method to check
     * how long the conflicting tx has been holding the lock. If the conflicting
     * tx has been holding the lock for an extended period of time, tries to
     * recover the conflicting tx.
     *
     * @param txn Tx number of the conflicting tx
     * @param cc_ng_id ID of the cc node group in which the conflict happens
     * @param cc_ng_term Leader term of the cc node group
     */
    void CheckRecoverTx(TxNumber txn, uint32_t cc_ng_id, int64_t cc_ng_term);
    void CheckRecoverTx(TxNumber txn,
                        TxLockInfo &lk_info,
                        uint32_t cc_ng_id,
                        int64_t cc_ng_term);

    void ClearTx(TxNumber txn);

    uint64_t ActiveTxMinTs(NodeGroupId cc_ng_id)
    {
        uint64_t min_ts = UINT64_MAX;

        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id);
        if (cc_ng_term < 0)
        {
            cc_ng_term = Sharder::Instance().StandbyNodeTerm();
        }

        auto it = lock_holding_txs_.find(cc_ng_id);
        if (it != lock_holding_txs_.end())
        {
            for (auto &tx_pair : it->second)
            {
                // Skip meta table because there is no need to do
                // checkpoint for these type table.
                if (!TableName::IsMeta(tx_pair.second->table_type_) &&
                    tx_pair.second->wlock_ts_ != 0)
                {
                    min_ts = std::min(min_ts, tx_pair.second->wlock_ts_ - 1);
                }

                // check and recover holding write lock transactions.

                CheckRecoverTx(
                    tx_pair.first, *tx_pair.second, cc_ng_id, cc_ng_term);
            }
        }

        if (min_ts == UINT64_MAX)
        {
            // When there is no active tx, since the local ts base is only
            // synced with the clock in every 2 sec, the local ts may fall a
            // little far behind. Re-synced the ts base with the clock to choose
            // an update-to-date ts for checkpoint.
            using namespace std::chrono_literals;

            uint64_t clock_ts =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();

            uint64_t tsb = Now();
            uint64_t max_ts = std::max(tsb, clock_ts);
            UpdateTsBase(max_ts);

            // need to return max_ts - 1 at here, since lock_holding_txs_'s
            // timestamp is read from ts_base_ which could be the same as
            // max_ts. max_ts is possible to be assigned to last_ckpt_ts, while
            // the next ckpt_ts could be read from lock_holding_txs_. Hence it
            // would be possible to trigger assert(ckpt_ts >= last_ckpt_ts_); if
            // we return max_ts directly.
            min_ts = max_ts - 1;
        }

        TryResizeLockArray();

        return min_ts;
    }

    /**
     * Try to reduce the size of lock array if it becomes sparse.
     *
     */
    void TryResizeLockArray();

    std::pair<bool, const CatalogEntry *> CreateCatalog(
        const TableName &table_name,
        NodeGroupId cc_ng_id,
        const std::string &catalog_image,
        uint64_t commit_ts);

    CatalogEntry *CreateDirtyCatalog(const TableName &table_name,
                                     NodeGroupId cc_ng_id,
                                     const std::string &catalog_image,
                                     uint64_t commit_ts);

    void UpdateDirtyCatalog(const TableName &table_name,
                            const std::string &catalog_image,
                            CatalogEntry *catalog_entry);

    std::pair<bool, const CatalogEntry *> CreateReplayCatalog(
        const TableName &table_name,
        NodeGroupId cc_ng_id,
        const std::string &old_schema_image,
        const std::string &new_schema_image,
        uint64_t old_schema_ts,
        uint64_t dirty_schema_ts);

    CatalogEntry *GetCatalog(const TableName &table_name, NodeGroupId cc_ng_id);

    /**
     * @brief Initialize table_ranges_ in local_cc_shard based on the
     * InitRangeEntry. StoreRange and StoreSlice will also be initialized if the
     * range belongs to this ng.
     *
     * @param table_name
     * @param init_ranges
     * @param ng_id
     * @param fully_cached If range is already fully cached. This will affect
     * the StoreSlice status of the created range. Currently set to true on
     * table create so that we don't need to visit data store once when reading
     * a just created table.
     */
    void InitTableRanges(const TableName &table_name,
                         std::vector<InitRangeEntry> &init_ranges,
                         const NodeGroupId ng_id,
                         bool fully_cached = false);

    std::map<TxKey, TableRangeEntry::uptr> *GetTableRangesForATable(
        const TableName &range_table_name, const NodeGroupId ng_id);

    TableRangeEntry *GetTableRangeEntry(const TableName &table_name,
                                        const NodeGroupId ng_id,
                                        const TxKey &key);

    const TableRangeEntry *GetTableRangeEntry(const TableName &table_name,
                                              const NodeGroupId ng_id,
                                              int32_t range_id);

    const TableRangeEntry *GetTableRangeEntryNoLocking(
        const TableName &table_name, const NodeGroupId ng_id, const TxKey &key);

    bool CheckRangeVersion(const TableName &table_name,
                           const NodeGroupId ng_id,
                           int32_t range_id,
                           uint64_t range_version);

    uint64_t CountRanges(const TableName &table_name,
                         const NodeGroupId ng_id,
                         const NodeGroupId key_ng_id);

    uint64_t CountRangesLockless(const TableName &table_name,
                                 const NodeGroupId ng_id,
                                 const NodeGroupId key_ng_id);

    uint64_t CountSlices(const TableName &table_name,
                         const NodeGroupId ng_id,
                         const NodeGroupId local_ng_id) const;

    void CleanTableRange(const TableName &table_name, const NodeGroupId ng_id);

    std::pair<std::shared_ptr<Statistics>, bool> InitTableStatistics(
        TableSchema *table_schema, NodeGroupId ng_id);

    std::pair<std::shared_ptr<Statistics>, bool> InitTableStatistics(
        TableSchema *table_name,
        TableSchema *dirty_table_schema,
        NodeGroupId ng_id,
        std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
            sample_pool_map);

    StatisticsEntry *GetTableStatistics(const TableName &table_name,
                                        NodeGroupId ng_id);

    const StatisticsEntry *LoadRangesAndStatisticsNx(
        const TableSchema *curr_schema,
        NodeGroupId cc_ng_id,
        int64_t cc_ng_term,
        CcRequestBase *requester);

    void CleanTableStatistics(const TableName &table_name, NodeGroupId ng_id);

    void DropBucketInfo(NodeGroupId ng_id);

    const BucketInfo *GetBucketInfo(uint16_t bucket_id,
                                    NodeGroupId ng_id) const;

    BucketInfo *GetBucketInfo(uint16_t bucket_id, NodeGroupId ng_id);

    NodeGroupId GetBucketOwner(const uint16_t bucket_id,
                               const NodeGroupId ng_id) const;

    const std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>> *
    GetAllBucketInfos(NodeGroupId ng_id) const;

    const BucketInfo *GetRangeOwner(int32_t range_id, NodeGroupId ng_id) const;

    void SetBucketMigrating(bool is_migrating);

    bool IsBucketsMigrating();

    uint32_t NakedBucketsRefCnt()
    {
        return tx_cnt_reading_naked_buckets_;
    }

    void IncrNakedBucketReader()
    {
        tx_cnt_reading_naked_buckets_++;
    }

    void DecrNakedBucketReader()
    {
        assert(tx_cnt_reading_naked_buckets_ > 0);
        tx_cnt_reading_naked_buckets_--;
    }

    /**
     * @brief Fetches the table's catalog from the data store and
     * temporarily caches the demanding cc request in the cc shard. After
     * the catalog is fetched and instantiated in this node, re-enqueues the
     * cc request for re-execution.
     *
     * @param table_name The table name
     * @param requester The cc request that needs to access the input
     * table's cc map but the cc map does not exist due to the missing of
     * the catalog.
     */
    void FetchCatalog(const TableName &table_name,
                      NodeGroupId cc_ng_id,
                      int64_t cc_ng_term,
                      CcRequestBase *requester);
    void RemoveFetchRequest(const TableName &table_name);

    void FetchTableStatistics(const TableName &table_name,
                              NodeGroupId cc_ng_id,
                              int64_t cc_ng_term,
                              CcRequestBase *requester);

    void FetchTableRanges(const TableName &range_table_name,
                          CcRequestBase *requester,
                          NodeGroupId cc_ng_id,
                          int64_t cc_ng_term);

    store::DataStoreHandler::DataStoreOpStatus FetchRecord(
        const TableName &table_name,
        const TableSchema *tbl_schema,
        TxKey key,
        LruEntry *cce,
        NodeGroupId cc_ng_id,
        int64_t cc_ng_term,
        CcRequestBase *requester,
        int32_t partition_id,
        bool fetch_from_primary = false,
        uint32_t key_shard_code = 0,
        uint64_t snapshot_read_ts = 0,
        bool only_fetch_archives = false);

    store::DataStoreHandler::DataStoreOpStatus FetchSnapshot(
        const TableName &table_name,
        const TableSchema *tbl_schema,
        TxKey key,
        NodeGroupId cc_ng_id,
        int64_t cc_ng_term,
        uint64_t snapshot_read_ts,
        bool only_fetch_archive,
        CcRequestBase *requester,
        size_t tuple_idx,
        OnFetchedSnapshot backfill_func,
        int32_t partition_id);

    store::DataStoreHandler::DataStoreOpStatus FetchBucketData(
        const TableName *table_name,
        const TableSchema *table_schema,
        NodeGroupId node_group_id,
        int64_t node_group_term,
        CcShard *ccs,
        bool is_local,
        absl::flat_hash_map<uint16_t, bool> &bucket_ids,
        const std::vector<DataStoreSearchCond> *pushdown_cond_,
        std::string_view start_key,
        KeyType start_key_type,
        bool start_key_inclusive,
        std::string_view end_key,
        KeyType end_key_type,
        bool end_key_inclusive,
        size_t batch_size,
        CcRequestBase *requester,
        OnFetchedBucketData backfill_func);

    store::DataStoreHandler::DataStoreOpStatus FetchBucketData(
        FetchBucketDataCc *fetch_bucket_data_cc);

    void RemoveFetchRecordRequest(LruEntry *cce);

    CcMap *CreateOrUpdatePkCcMap(const TableName &table_name,
                                 const TableSchema *table_schema,
                                 NodeGroupId ng_id,
                                 bool is_create = true,
                                 bool ccm_has_full_entries = false);

    CcMap *CreateOrUpdateSkCcMap(const TableName &index_name,
                                 const TableSchema *table_schema,
                                 NodeGroupId ng_id,
                                 bool is_create = true);

    /**
     * @brief Initializes the request's target CC map if the table schema
     * is available and indicates that the table exists.
     *
     * The request is rejected if the table is being modified, or if the
     * requested schema does not match the target schema. If the schema
     * is not cached locally, an asynchronous FetchCatalog() request is
     * issued to retrieve it from the data store.
     *
     * @return InitCcmResult describing the outcome. success=false with
     * error=CcErrorCode::NO_ERROR indicates that the caller should retry
     * after catalog fetching completes.
     */
    InitCcmResult InitCcm(const TableName &table_name,
                          NodeGroupId cc_ng_id,
                          int64_t cc_ng_term,
                          CcRequestBase *requester);

    void DropCcm(const TableName &table_name, NodeGroupId ng_id);

    /**
     * Clean cc map and update its schema and schema ts. For truncate table
     * operation.
     * @param table_name
     * @param ng_id
     * @param schema_ts
     * @param truncate_table: if the clean operation is part of truncate table.
     * If true, ccm will be set to fully cached after cleared.
     */
    bool CleanCcmPages(const TableName &table_name,
                       NodeGroupId ng_id,
                       uint64_t clean_ts,
                       bool truncate_table = false);

    void UpdateCcmSchema(const TableName &table_name,
                         NodeGroupId node_group_id,
                         const TableSchema *table_schema,
                         uint64_t schema_ts);

    void CleanCcm(const TableName &table_name);

    void CleanCcm(const TableName &table_name, NodeGroupId ng_id);

    /**
     * @brief Drops all cc maps associated with a cc node group. The method is
     * called when this node steps down as the leader of the specified cc node
     * group.
     *
     * @param ng_id The cc node group whose leader has been transferred to
     * another node.
     */
    void DropCcms(NodeGroupId ng_id);

    void CreateOrUpdateRangeCcMap(const TableName &range_table_name,
                                  const TableSchema *table_schema,
                                  NodeGroupId ng_id,
                                  uint64_t schema_ts,
                                  bool is_create = true);

    void DecreaseLockCount();

    /**
     * Used for unit test to verify the lru link is complete.
     */
    void VerifyLruList();

    std::unordered_map<TableName, bool> GetCatalogTableNameSnapshot(
        NodeGroupId cc_ng_id);

    bool IsNative(NodeGroupId ng_id) const
    {
        return ng_id == ng_id_;
    }

    // native node group
    const uint16_t core_id_;
    const uint16_t core_cnt_;
    const NodeGroupId ng_id_;
    std::atomic<int32_t> meta_data_mux_{};
    LocalCcShards &local_shards_;

    bool EnableMvcc() const;
    void AddActiveSiTx(TxNumber txn, uint64_t start_ts);
    void RemoveActiveSiTx(TxNumber txn);
    void ClearActvieSiTxs();
    // Scan {active_si_txs_} to update {min_si_tx_start_ts_}
    void UpdateLocalMinSiTxStartTs();
    uint64_t LocalMinSiTxStartTs();
    uint64_t GlobalMinSiTxStartTs() const;

    // Active blocking transaction management functions
    void UpsertActiveBlockingTx(TxNumber txn, uint64_t timestamp);
    bool RemoveActiveBlockingTx(TxNumber txn);
    void ClearActiveBlockingTxs();
    size_t ActiveBlockingTxSize() const;
    void RemoveExpiredActiveBlockingTxs();

    // shard level memory limit.
    uint64_t memory_limit_{0};

    const bool realtime_sampling_{true};

    // Search lock_holding_txs_, find the entrys with waited transactions and
    // save them into CheckDeadLockResult.
    void CollectLockWaitingInfo(CheckDeadLockResult &dlr);
    const std::unordered_map<NodeGroupId,
                             absl::flat_hash_map<TxNumber, TxLockInfo::uptr>> &
    GetLockHoldingTxs() const
    {
        return lock_holding_txs_;
    }

    LruPage *CleanStart() const
    {
        return clean_start_ccp_;
    }

    void ResetCleanStart(LruPage *ccp = nullptr)
    {
        clean_start_ccp_ = ccp;
    }

    bool OutOfMemory()
    {
        return clean_start_ccp_ != nullptr && clean_start_ccp_ == &tail_ccp_;
    }

    SystemHandler *GetSystemHandler()
    {
        return system_handler_;
    }

    uint64_t &LastReadTs()
    {
        return last_read_ts_;
    }

    void UpdateLastReadTs(uint64_t read_ts)
    {
        last_read_ts_ = std::max(last_read_ts_, read_ts);
    }

    metrics::Meter *GetMeter()
    {
        return meter_.get();
    };

    // Called on primary node
    void ForwardStandbyMessage(StandbyForwardEntry *entry);
    void AddCandidateStandby(uint32_t node_id, uint64_t start_seq_id);
    void RemoveCandidateStandby(uint32_t node_id);
    void CheckAndFreeUnneededEntries();
    void AddSubscribedStandby(uint32_t node_id,
                              uint64_t start_seq_id,
                              int64_t standby_node_term)
    {
        LOG(INFO) << "start forwarding to node " << node_id << " from seq "
                  << start_seq_id << ", seq grp " << core_id_;

        auto ins_res = subscribed_standby_nodes_.try_emplace(
            node_id, std::make_pair(start_seq_id - 1, standby_node_term));
        if (!ins_res.second)
        {
            if (ins_res.first->second.second < standby_node_term)
            {
                ins_res.first->second.first = start_seq_id - 1;
                ins_res.first->second.second = standby_node_term;
                CheckAndFreeUnneededEntries();
            }
        }
    }
    uint64_t NextStandbyMessageSequence() const
    {
        return next_forward_sequence_id_;
    }

    // Try to send previous failed message to standby nodes.
    bool ResendFailedForwardMessages();

    void CollectStandbyMetrics();

    uint64_t GetNextForwardSequnceId() const
    {
        return next_forward_sequence_id_;
    }

    void RemoveSubscribedStandby(uint32_t node_id);

    std::vector<uint32_t> GetSubscribedStandbys()
    {
        std::vector<uint32_t> node_ids;
        node_ids.reserve(subscribed_standby_nodes_.size());
        for (auto [node_id, seq_id_and_term] : subscribed_standby_nodes_)
        {
            node_ids.push_back(node_id);
        }
        return node_ids;
    }
    void ResetStandbySequence();

    void DecrInflightStandbyReqCount(uint32_t seq_grp);

    absl::flat_hash_map<uint32_t, StandbySequenceGroup> &
    GetStandbysequenceGrps()
    {
        return standby_sequence_grps_;
    }

    uint64_t GetStandbyLag(uint32_t node_id) const
    {
        auto it = subscribed_standby_nodes_.find(node_id);
        assert(it != subscribed_standby_nodes_.end());
        return it->second.first;
    }

    // called on follower node
    bool UpdateLastReceivedStandbySequenceId(
        const remote::KeyObjectStandbyForwardRequest &msg);
    void SubsribeToPrimaryNode(uint32_t seq_grp, uint64_t seq_id);

    void UpdateStandbyConsistentTs(uint32_t seq_grp,
                                   uint64_t seq_id,
                                   uint64_t consistent_ts,
                                   int64_t standby_node_term);

    uint64_t MinLastStandbyConsistentTs() const;

    void EnqueueWaitListIfSchemaMismatch(CcRequestBase *req);

    void DequeueWaitListAfterSchemaUpdated();

    void UpdateBufferedCommandCnt(int64_t delta);

    void CheckLagAndResubscribe() const;

    bool EnableDefragment() const;

    void OnDirtyDataFlushed()
    {
        ResetCleanStart();
        if (WaitListSizeForMemory() > 0)
        {
            WakeUpShardCleanCc();
        }
    }

    FillStoreSliceCc *NewFillStoreSliceCc()
    {
        return fill_store_slice_cc_pool_.NextRequest();
    }

    InitKeyCacheCc *NewInitKeyCacheCc()
    {
        return init_key_cache_cc_pool_.NextRequest();
    }

    std::shared_ptr<ReaderWriterObject<TableSchema>> FindSchemaCntl(
        const TableName &tbl_name);

    std::shared_ptr<ReaderWriterObject<TableSchema>> FindEmplaceSchemaCntl(
        const TableName &tbl_name);

    void DeleteSchemaCntl(const TableName &tbl_name);

    void ClearNativeSchemaCntl();
    void CollectCacheHit();
    void CollectCacheMiss();

private:
    void SetTxProcNotifier(std::atomic<TxProcessorStatus> *tx_proc_status,
                           TxProcCoordinator *tx_coordi)
    {
        tx_proc_status_ = tx_proc_status;
        tx_coordi_ = tx_coordi;
    }

    void NotifyTxProcessor();

    TxLockInfo::uptr GetTxLockInfo(int64_t tx_term);
    void RecycleTxLockInfo(TxLockInfo::uptr lock_info);

    size_t memory_usage_round_ = 1;

    // heap for cc_map memory allocation
    std::unique_ptr<CcShardHeap> shard_heap_{nullptr};
    // heap only for data sync scan
    std::unique_ptr<CcShardHeap> shard_data_sync_scan_heap_{nullptr};
    mi_threadid_t shard_heap_thread_id_{0};
    size_t last_failed_collect_ts_{0};

    // all the lock acquire/release on this ccshard. It used to reduce the cost
    // of allocation/dellocation of memory.
    std::vector<KeyGapLockAndExtraData::uptr> lock_vec_;
    // pointer to the next slot in lock array.
    uint32_t next_lock_idx_;
    uint32_t used_lock_count_;

    /**
     * @brief A collection of active tx's that have acquired locks/intentions in
     * this shard and the tx's information, including when the tx acquires the
     * latest write lock, the term of the tx node and a list of pointers to the
     * cc entries containing the tx's locks/intentions.
     *
     */
    std::unordered_map<NodeGroupId,
                       absl::flat_hash_map<TxNumber, TxLockInfo::uptr>>
        lock_holding_txs_;

    TxLockInfo tx_lock_info_head_{0};

    // below are all string owners
    absl::flat_hash_map<TableName, CcMap::uptr> native_ccms_;
    std::unordered_map<TableName, std::unordered_map<NodeGroupId, CcMap::uptr>>
        failover_ccms_;

    std::unordered_map<TableName, std::unique_ptr<FetchCc>> fetch_reqs_;

    // For load record from kvstore asynchronously
    std::unordered_map<LruEntry *, FetchRecordCc> fetch_record_reqs_;

    // For load snapshot from kvstore asynchronously
    CcRequestPool<FetchSnapshotCc> fetch_snapshot_cc_pool_;
    // For load bucket data from kvstore asynchronously
    CcRequestPool<FetchBucketDataCc> fetch_bucket_data_cc_pool_;

    // For concurrency execution of cpu-bound tasks.
    CcRequestPool<RunOnTxProcessorCc> run_on_tx_processor_cc_pool_;

    CcRequestPool<FillStoreSliceCc> fill_store_slice_cc_pool_;
    CcRequestPool<InitKeyCacheCc> init_key_cache_cc_pool_;

    // CcRequest queue on this shard/core.
    moodycamel::ConcurrentQueue<CcRequestBase *> cc_queue_;
    std::atomic<uint32_t> cc_queue_size_{0};
    std::array<CcRequestBase *, 64> req_buf_;
    std::vector<moodycamel::ProducerToken> thd_token_;
    // Low priority queue for background job requests
    moodycamel::ConcurrentQueue<CcRequestBase *> low_priority_cc_queue_;
    std::atomic<uint32_t> low_priority_cc_queue_size_{0};
    std::vector<moodycamel::ProducerToken> low_priority_thd_token_;
    // Cc requests waiting for the free memory.
    std::list<CcRequestBase *> cc_wait_list_for_memory_;

    // all the transactions started on this ccshard. Some txs are Ongoing while
    // others are Available, new transaction request has to traverse the array
    // and find an available one.
    std::vector<TEntry> tx_vec_;
    // pointer to the next slot in tx array.
    uint32_t next_tx_idx_;

    // tx identifier inside a CPU core. It's a uint32 value and will become 0
    // after wraparound. Global tx_number is 64 bits: higher 32 bits are
    // global_core_id, while lower 32 bits are tx_ident.
    uint32_t next_tx_ident_;

    // Standby forward msg related members used on primary node.
    // Uses memory-bounded queue instead of fixed-size buffer.
    // Memory-bounded queue for entries still needed (owns entries)
    std::deque<std::unique_ptr<StandbyForwardEntry>> history_standby_msg_;
    // O(1) lookup map: sequence ID -> entry pointer (entries owned by queue)
    std::unordered_map<uint64_t, StandbyForwardEntry *> seq_id_to_entry_map_;
    // Candidate followers: node_id -> start_seq_id
    std::unordered_map<uint32_t, uint64_t> candidate_standby_nodes_;
    // Total memory usage of all entries in history queue
    uint64_t total_standby_buffer_memory_usage_{0};
    // Memory limit for standby buffer (10% of node memory limit per shard)
    uint64_t standby_buffer_memory_limit_{0};
    uint64_t next_forward_sequence_id_{1};
    std::unordered_map<uint32_t, std::pair<uint64_t, int64_t>>
        subscribed_standby_nodes_;
    std::unique_ptr<RetryFailedStandbyMsgCc> retry_fwd_msg_cc_;
    // Shard clean cc
    std::unique_ptr<ShardCleanCc> shard_clean_cc_;

    // Standby forward msg related members used on follower node
    CcRequestPool<KeyObjectStandbyForwardCc> key_obj_standby_msg_cc_pool_;
    absl::flat_hash_map<uint32_t, StandbySequenceGroup> standby_sequence_grps_;
    // requests to execute after schema being modified
    std::vector<CcRequestBase *> waiting_list_for_schema_;

    // The total number of commands buffered on this shard. If standby node has
    // too many commands buffered, it probably has fallen behind. Resubscribe
    // to the master node to get the full and latest snapshot.
    int64_t buffered_cmd_cnt_{0};

    // Reserved head and tail for the double-linked list of cc entries, which
    // simplifies handling of empty and one-element lists.
    LruPage head_ccp_, tail_ccp_;
    /**
     * @brief Each time a page is accessed and moved to the tail of the LRU
     * list, the counter is incremented and assigned to the page. Since in a
     * double-linked list there is no way to determine the relative order of two
     * pages, we use the number to indicate if a page precedes or succeeds the
     * other in the list.
     *
     */
    uint64_t access_counter_{0};

    // Page to start looking for cc entries to kick out on LRU chain.
    LruPage *clean_start_ccp_;

    // The number of ccentry in all the ccmap of this ccshard.
    uint64_t size_;

    Checkpointer *ckpter_;

    /**
     * @brief The variable via which the dedicated processing thread notifies
     * the shard that it enters into the sleep mode.
     *
     */
    std::atomic<TxProcessorStatus> *tx_proc_status_{nullptr};
    TxProcCoordinator *tx_coordi_{nullptr};

    // Catalog handler which is used to execute catalog related callback
    // function at runtime side.
    CatalogFactory *catalog_factory_[5]{
        nullptr, nullptr, nullptr, nullptr, nullptr};

    SystemHandler *const system_handler_;

    absl::flat_hash_map<TableName,
                        std::shared_ptr<ReaderWriterObject<TableSchema>>>
        catalog_rw_cntl_;

    // The max number of cc page to scan in one invocation of Clean().
    static constexpr uint64_t freeBatchSize = 10;
    // The maximum allowed duration(us) of one invocation of Clean().
    static constexpr uint64_t maxDuration = 30;

    // cache all tx info under SI isolation level in this shard,
    // format: {txn->start_ts}
    std::unordered_map<TxNumber, uint64_t> active_si_txs_;
    // min start_ts of tx in "active_si_txs_"
    std::atomic<uint64_t> min_si_tx_start_ts_{1U};
    // last timestamp of updating "min_si_tx_start_ts_"
    uint64_t last_scan_txs_ts_{0U};
    // track the lock sparse number and reduce lock array size if threshold
    // reached.
    uint8_t lock_sparse_num_{0};

    std::unique_ptr<metrics::Meter> meter_;
    size_t standby_metrics_round_{1};

    /**
     * @brief The variable bookkeeps the latest time when any record in this
     * shard is accessed by read tx's. It is used to coordinate with write tx's
     * such that a write tx's commit timestamp is greater than all read tx's
     * that happen before the write tx. To coordinate, the variable is updated
     * in two cases: (1) when a snapshot read is performed with a read ts, the
     * variable is set to max{read_ts, last_read_ts_}, and (2) when PostRead is
     * performed to release read locks or validate version stability, the
     * variable is set to max{commit_ts, last_read_ts_}.
     *
     */
    uint64_t last_read_ts_{0};

    // The number of active tx reading buckets without adding readlock on
    // ccentry in RangeBucketCcMap.
    uint32_t tx_cnt_reading_naked_buckets_{0};

    remote::CcStreamSender *stream_sender_{nullptr};

    // free invalid cces after 2 hours.
    static const uint64_t invalid_cce_expire_time_ = 7200000000;

    // Keep track of all active blocking transactions(e.g. BLMOVE, BLMPOP) whose
    // abort ApplyCc fails to discard the corresponding blocking
    // ApplyCc(exec_rst == ExecResult::Block), uint64_t here is the timestamp
    // when the transaction was inserted into this map.
    std::unordered_map<TxNumber, uint64_t> active_blocking_txs_;

    friend class LocalCcHandler;
    friend class LocalCcShards;
    friend class Checkpointer;
};
}  // namespace txservice
