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
#if defined(WITH_JEMALLOC)
#include <jemalloc/jemalloc.h>
#endif

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <iostream>
#include <iterator>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <tuple>
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
#include "page_key_codec.h"
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
struct TxObject;
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
    // lock. Gates CheckRecoverTx to once per tx per 5s. The ctor/Reset zero
    // is never observable: UpsertLockHoldingTx — the only path that puts an
    // entry into lock_holding_txs_ — stamps this with Now() on every call,
    // including entry creation, so the gate runs from acquisition time.
    uint64_t last_recover_ts_;
    // A list of cc entries on which the tx has acquired write/read locks.
    absl::flat_hash_set<LruEntry *> cce_list_;
    // This cc map type is used to skip the meta table(such as: catalog, range)
    // during get ActiveTxMinTs()
    TableType table_type_;

    std::unique_ptr<TxLockInfo> next_{nullptr};
};

#if defined(WITH_JEMALLOC)
class JemallocArenaSwitcher
{
public:
    explicit JemallocArenaSwitcher() = default;

    static bool ReadCurrentArena(uint32_t &current_arena)
    {
        size_t sz = sizeof(uint32_t);
        if (mallctl("thread.arena", &current_arena, &sz, nullptr, 0) != 0)
        {
            LOG(FATAL) << "Failed to read current arena";
            return false;
        }

        return true;
    }

    static bool SwitchToArena(uint32_t arena)
    {
        if (mallctl(
                "thread.arena", nullptr, nullptr, &arena, sizeof(uint32_t)) !=
            0)
        {
            LOG(FATAL) << "Failed to switch arena " << arena;
            return false;
        }

        return true;
    }
};
#endif

class CcShardHeap
{
    static constexpr double high_water = 0.8;
    static constexpr double utilization = 0.8;
    // A triggered cleaning campaign runs until this much of the budget is
    // free (or until the sweep has nothing left to reclaim). Cleaning walks
    // the LRU and can cascade into a checkpoint request, so it is expensive
    // and must not be tuned to whatever the current allocation happens to
    // need: stopping the instant the heap dips below its threshold leaves
    // zero headroom, so the very next allocation cleans again. Freeing a
    // BATCH instead bounds how OFTEN cleaning runs, which is the cost that
    // matters.
    static constexpr double clean_target_free_ratio = 0.10;

public:
    CcShardHeap(CcShard *cc_shard, size_t limit);
    ~CcShardHeap();

    // Set this heap_ as the default heap for the current thread.
    // return the previous default heap.
    mi_heap_t *SetAsDefaultHeap();

    uint32_t SetAsDefaultArena();

    // Check if this heap is full
    bool Full(int64_t *alloc = nullptr, int64_t *commit = nullptr) const;

    /**
     * @brief Whether `bytes` more can be allocated on this heap right now —
     * the page-admission predicate (eloqkv docs/08 §8).
     *
     * Differs from Full() by asking about a SPECIFIC prospective allocation
     * rather than the current level, which is what lets a large fault set be
     * refused before it is taken rather than after it has already overshot.
     *
     * @return true if the allocation would stay within this heap's limit.
     */
    bool CanAllocate(size_t bytes) const;

    /**
     * @brief PageAdmission::Fn adapter — `ctx` is a CcShardHeap*.
     * @return true if `bytes` may be allocated on that heap now.
     */
    static bool AdmitPageBytes(void *ctx, size_t bytes)
    {
        return static_cast<const CcShardHeap *>(ctx)->CanAllocate(bytes);
    }

    /**
     * @brief PageAdmission::CapFn adapter — `ctx` is a CcShardHeap*.
     * @return that heap's total limit, for "could this ever fit?" questions.
     */
    static size_t PageAdmissionCap(void *ctx)
    {
        return static_cast<const CcShardHeap *>(ctx)->MemoryLimit();
    }

    bool NeedCleanShard(int64_t &alloc, int64_t &commit) const;

    /**
     * @brief Should a cleaning campaign keep going? True until the heap has
     * `clean_target_free_ratio` of its budget free.
     *
     * Distinct from NeedCleanShard, which decides whether to START: that one
     * fires at the threshold, this one keeps the campaign running past it so
     * one campaign yields real headroom rather than the single page the
     * triggering allocation needed.
     */
    bool NeedMoreCleaning(int64_t alloc) const
    {
        // Deterministic hook for the TERMINAL branch of a campaign (the path
        // taken when a sweep reaches the LRU tail without meeting the
        // target). On a test node the heap sits far below the target, so a
        // campaign always takes the target-MET branch and the terminal path
        // — where the waiter-stranding defect lived — is unreachable from a
        // client. Debug-only via CODE_FAULT_INJECTOR.
        CODE_FAULT_INJECTOR("force_clean_target_unmet", {
            LOG_EVERY_N(INFO, 200) << "FAULTLOG force_clean_target_unmet";
            return true;
        });
        return alloc > static_cast<int64_t>(memory_limit_ *
                                            (1.0 - clean_target_free_ratio));
    }

    bool NeedDefragment(int64_t *alloc, int64_t *commit) const;

    mi_heap_t *Heap() const
    {
        return heap_;
    }

    uint32_t ArenaId() const
    {
        return arena_id_;
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

    size_t MemoryLimit() const
    {
        return memory_limit_;
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

    uint32_t arena_id_{0};
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
            uint32_t range_slice_memory_limit_percent = 10,
            uint64_t dirty_memory_check_interval = 1000,
            uint64_t dirty_memory_size_threshold_mb = 0);

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

    void FetchTableRangeSize(const TableName &table_name,
                             int32_t partition_id,
                             NodeGroupId cc_ng_id,
                             int64_t cc_ng_term);

    void AdjustDataKeyStats(const TableName &table_name,
                            int64_t size_delta,
                            int64_t dirty_delta);

    std::pair<size_t, size_t> GetDataKeyStats() const;

    /**
     * @brief Check dirty memory thresholds and trigger checkpoint if exceeded.
     * Called periodically from AdjustDataKeyStats based on sampling interval.
     */
    void CheckAndTriggerCkptByDirtyMemory();

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
     * @brief A page-admission refusal asks for a reclamation CAMPAIGN
     * (eloqkv docs/08 §8).
     *
     * Deliberately a boolean, not a size: the campaign's amount is the fixed
     * 10 % target, never tuned to the refused allocation. What this adds is a
     * START signal independent of Full(), because the two predicates differ —
     * admission refuses at `allocated + requested > limit` while the cleaner
     * fired only at `allocated >= limit`. In the gap (limit 100, allocated
     * 99, requested 2) the cleaner saw a heap that was not full, reclaimed
     * nothing, woke the waiter, and the waiter refused again: a real
     * production spin measured at ~130 k refusals/s and ~200 % CPU. Once
     * requested, the campaign runs to its target even after `allocated`
     * drops back below the hard limit.
     */
    void RequestCleanCampaign()
    {
        clean_campaign_requested_ = true;
    }

    bool CleanCampaignRequested() const
    {
        return clean_campaign_requested_;
    }

    void ClearCleanCampaignRequest()
    {
        clean_campaign_requested_ = false;
    }

    /**
     * @brief Why a cleaning campaign could not reclaim (eloqkv docs/08 §8).
     *
     * When a campaign frees nothing, the right response depends entirely on
     * WHICH obstacle dominated, and the two need opposite handling:
     *
     *   dirty-blocked  the candidates are unflushed. A checkpoint is already
     *                  requested and will make them reclaimable, so this is
     *                  not a deadlock — wait.
     *   pin-blocked    the candidates are pinned by other in-flight
     *                  faulters — a hold-and-wait cycle. The holders are
     *                  ordinary commands that terminate on their own, so
     *                  the waiters are woken to retry until the pins are
     *                  released (identifying and killing the specific
     *                  holding transaction would be a far larger mechanism
     *                  for the same outcome).
     *
     * Counted in CcPageCleanGuard::CanBeCleaned, which already evaluates
     * both clauses on every candidate, so attribution is free. Reset at the
     * start of each campaign.
     */
    void NoteDirtyBlockedCandidate()
    {
        ++clean_dirty_blocked_;
    }

    void NotePinBlockedCandidate()
    {
        ++clean_pin_blocked_;
    }

    void ResetCleanBlockedCounters()
    {
        clean_dirty_blocked_ = 0;
        clean_pin_blocked_ = 0;
    }

    size_t CleanDirtyBlocked() const
    {
        return clean_dirty_blocked_;
    }

    size_t CleanPinBlocked() const
    {
        return clean_pin_blocked_;
    }

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
        return cc_queue_size_.load(std::memory_order_relaxed) == 0 &&
               low_priority_cc_queue_size_.load(std::memory_order_relaxed) ==
                   0 &&
               lazy_free_queue_size_.load(std::memory_order_relaxed) == 0;
    }

    size_t ProcessRequests();

    size_t ProcessLowPriorityRequests();

    void EnqueueLazyFree(std::unique_ptr<TxObject> obj);

    size_t ProcessLazyFreeQueue();

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

    size_t LowPriorityQueueSize()
    {
        return low_priority_cc_queue_size_.load(std::memory_order_relaxed);
    }

    CatalogFactory *GetCatalogFactory(TableEngine table_engine)
    {
        return catalog_factory_[static_cast<int>(table_engine) - 1];
    }

    CacheEvictPolicy GetCacheEvictPolicy() const;

    uint64_t LargeObjThresholdBytes() const;

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

    /**
     * @brief Once per tx per 5s: reconciles the tx's registry entry with the
     * actual lock state in place — references whose lock no longer lists
     * the tx are removed immediately (the check is one SearchLock; such a
     * mismatch is a leak by invariant) — and, if real locks remain, starts
     * orphan-lock recovery: a RecoverDeadTxCc probe for locally-coordinated
     * txs, the log-consulting path for remote ones.
     *
     * @return True when the repair emptied the entry; the caller must
     * recycle and erase it (this function cannot: ActiveTxMinTs calls it
     * while iterating lock_holding_txs_).
     */
    bool CheckRecoverTx(TxNumber txn,
                        TxLockInfo &lk_info,
                        uint32_t cc_ng_id,
                        int64_t cc_ng_term);

    void ClearTx(TxNumber txn);

    /**
     * @brief Resolve phase of RecoverDeadTxCc, reached when the tx's owner
     * shard reported a non-alive verdict. Runs on the shard holding the
     * locks, against freshly-read state.
     *
     * @param verdict The owner shard's finding, which decides how write
     * locks are handled:
     * - Verdict::Dead — the owner will never run post-processing again (its
     *   TEntry slot has been reused, or it is resident as Aborted/Finished;
     *   post-processing always completes before either can happen). No code
     *   path remains that would install pending values or release these
     *   locks, so on non-meta tables without a data WAL the write locks are
     *   abort-cleared here.
     * - Verdict::Committed — the commit is decided but post-processing may
     *   still be running, and post-processing is what installs each pending
     *   value and then releases its write lock. Write locks are therefore
     *   left to it, unless committed_write_recover_window_seconds has
     *   passed since the write lock was granted (a wedged post-processing;
     *   the pending write is then treated as lost per the no-WAL contract).
     *
     * Independent of the verdict: read locks, read intents and data write
     * intents are released (none guard a pending value; the owner's own
     * release, if any, is an idempotent no-op); write locks under a data
     * WAL and all meta write locks/intents are handed to the log-consulting
     * recovery path (Sharder::RecoverTx), which determines the outcome from
     * the log and replays committed records before releasing. References
     * whose lock no longer lists the tx are removed as in CheckRecoverTx.
     */
    void RecoverDeadTxLocks(TxNumber txn,
                            NodeGroupId cc_ng_id,
                            int64_t cc_ng_term,
                            int64_t tx_coord_term,
                            uint64_t wlock_ts,
                            RecoverDeadTxCc::Verdict verdict);

    uint64_t ActiveTxMinTs(NodeGroupId cc_ng_id,
                           TxNumber *pinning_txn,
                           uint64_t *pinning_wlock_ts)
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
            for (auto tx_it = it->second.begin(); tx_it != it->second.end();)
            {
                // Repairs stale registry references in place and launches
                // orphan-lock recovery for locks that are really held. A
                // true return means the entry emptied out and must be
                // erased here: the callee cannot erase while this loop
                // iterates the map. An erased entry stops contributing to
                // the checkpoint watermark immediately.
                if (CheckRecoverTx(
                        tx_it->first, *tx_it->second, cc_ng_id, cc_ng_term))
                {
                    RecycleTxLockInfo(std::move(tx_it->second));
                    auto next_it = std::next(tx_it);
                    it->second.erase(tx_it);
                    tx_it = next_it;
                    continue;
                }

                // Skip meta table because there is no need to do
                // checkpoint for these type table.
                if (!TableName::IsMeta(tx_it->second->table_type_) &&
                    tx_it->second->wlock_ts_ != 0)
                {
                    if (tx_it->second->wlock_ts_ - 1 < min_ts)
                    {
                        min_ts = tx_it->second->wlock_ts_ - 1;
                        *pinning_txn = tx_it->first;
                        *pinning_wlock_ts = tx_it->second->wlock_ts_;
                    }
                }

                ++tx_it;
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

    uint64_t ActiveTxMaxTs(NodeGroupId cc_ng_id) const
    {
        uint64_t max_ts = 0;
        auto it = lock_holding_txs_.find(cc_ng_id);
        if (it != lock_holding_txs_.end())
        {
            for (const auto &tx_pair : it->second)
            {
                if (!TableName::IsMeta(tx_pair.second->table_type_) &&
                    tx_pair.second->wlock_ts_ != 0)
                {
                    max_ts = std::max(max_ts, tx_pair.second->wlock_ts_);
                }
            }
        }
        return max_ts;
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
        bool only_fetch_archives = false,
        bool reopen = false);

    /**
     * @brief Issues (or joins) a page fetch for one page of a paged large
     * object (eloqkv docs/08-paged-objects.md §13). Bypasses
     * fetch_record_reqs_ entirely: the request lives in the entry's FetchHub
     * (cc/page_fetch.h), keyed by page id — the coalescing unit for pages.
     * The object key supplies both the encoded row key's middle bytes and
     * the partition hash (§5 co-location); `waiter_txn` (0 = none) is
     * registered on the fetch and resolved through the paged payload's
     * per-txn contexts at completion.
     * @return Retry when the store is busy — the caller unwinds exactly as
     * for a whole-record fetch; Success otherwise.
     */
    store::DataStoreHandler::DataStoreOpStatus FetchPage(
        const TableName &table_name,
        const TableSchema *tbl_schema,
        const TxKey &object_key,
        PageRowKind kind,
        uint32_t page_id,
        LruEntry *cce,
        NodeGroupId cc_ng_id,
        int64_t cc_ng_term,
        uint64_t waiter_txn,
        int32_t partition_id);

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
    void NotifyStandbyOutOfSync(uint32_t node_id);

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

    std::vector<uint32_t> GetCandidateStandbys()
    {
        std::vector<uint32_t> node_ids;
        node_ids.reserve(candidate_standby_nodes_.size());
        for (const auto &it : candidate_standby_nodes_)
        {
            node_ids.push_back(it.first);
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

    /**
     * @brief Commands buffered on this shard, awaiting a version or a page.
     *
     * @return The count. Recovery gates promotion on this reaching zero as
     * well as on DrainBlockedCount(): when log replay reports a group finished
     * it has only PROCESSED the records, and the record fetches they triggered
     * may still be in flight, so a drain that will stall has not stalled yet.
     * This is shard-wide rather than per node group, which can over-wait in a
     * multi-group deployment -- safe, since it only ever delays serving.
     */
    int64_t BufferedCommandCnt() const
    {
        return buffered_cmd_cnt_;
    }

    /**
     * @brief Whole-record fetches in flight on this shard.
     *
     * @return The count. The third thing recovery must wait for: when replay
     * reports a log group finished, a record fetch it issued may still be in
     * flight, and the replayed commands for that key are not buffered — and so
     * not yet counted anywhere — until the record lands in BackFill.
     */
    size_t InFlightRecordFetchCnt() const
    {
        return fetch_record_reqs_.size();
    }

    /**
     * @brief Records that `entry`'s buffered-command drain is waiting on a page
     * fetch, so log-replay completion must not be declared yet (docs/08 §10).
     *
     * Idempotent: the same entry stalls repeatedly, once per page it needs, and
     * must be counted once. Shard-core only, like every other CcShard member.
     *
     * @param key_desc the key's text, for logging which keys hold up recovery.
     */
    void NoteDrainBlocked(NodeGroupId ng_id,
                          LruEntry *entry,
                          std::string key_desc);

    /**
     * @brief Records that `entry`'s drain is no longer waiting — it applied,
     * its buffer was cleared, or it was abandoned on a term change.
     */
    void NoteDrainUnblocked(NodeGroupId ng_id, LruEntry *entry);

    /**
     * @brief How many entries of `ng_id` have a drain waiting on a page.
     *
     * @return The count, zero if none. Recovery gates the candidate-to-real
     * term promotion on this reaching zero across all shards, so that a node
     * never starts serving a key whose replayed tail has not been applied.
     */
    size_t DrainBlockedCount(NodeGroupId ng_id) const;

    /**
     * @brief Drops all drain-blocked bookkeeping for `ng_id`.
     *
     * Called when the node group's term changes: the pending drains are moot,
     * and a stale entry here would block the next term's promotion forever.
     */
    void ClearDrainBlocked(NodeGroupId ng_id);

    /**
     * @brief Names up to `limit` keys whose drain is blocking `ng_id`.
     *
     * @return A human-readable list for logging. A bare count says recovery is
     * stuck; this says which keys are holding it.
     */
    std::string DescribeDrainBlocked(NodeGroupId ng_id, size_t limit) const;

    /**
     * @brief Describes up to `limit` entries on this shard that hold buffered
     * replayed/standby commands, across every cc map serving `ng_id`.
     *
     * The per-entry version tuple (see CcMap::DescribeBufferedCommands) is
     * the post-mortem evidence for a stalled replay drain: it distinguishes a
     * head waiting on a version hole from an applicable head the drain never
     * drove. Failure-path only; walks whole maps.
     *
     * @return A human-readable list for logging; "" when nothing is buffered.
     */
    std::string DescribeBufferedCommands(NodeGroupId ng_id, size_t limit);

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

    void ResetRangeSplittingStatus(const TableName &table_name,
                                   uint32_t ng_id,
                                   uint32_t range_id);

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

    /**
     * @brief Create a data sync task for triggering the split range operation.
     *
     * @param table_name - The name of the table.
     * @param ng_id - The id of the node group.
     * @param ng_term - The term of the node group.
     * @param range_id - The id of the range.
     * @param data_sync_ts - The timestamp of the data sync.
     * @param is_dirty - Whether the table is dirty (such as the secondary index
     * table that is being built).
     */
    void CreateSplitRangeDataSyncTask(const TableName &table_name,
                                      uint32_t ng_id,
                                      int64_t ng_term,
                                      int32_t range_id,
                                      uint64_t data_sync_ts,
                                      bool is_dirty);

    void ClearNativeSchemaCntl();
    void CollectCacheHit();
    void CollectCacheMiss();

public:
    // native node group
    const uint16_t core_id_;
    const uint16_t core_cnt_;
    const NodeGroupId ng_id_;
    std::atomic<int32_t> meta_data_mux_{};
    LocalCcShards &local_shards_;

    // shard level memory limit.
    uint64_t memory_limit_{0};

    const bool realtime_sampling_{true};

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

    // For orphan-lock recovery probes launched by CheckRecoverTx. Probe
    // volume is bounded by the per-tx 5s gate (last_recover_ts_).
    CcRequestPool<RecoverDeadTxCc> recover_dead_tx_cc_pool_;

    CcRequestPool<FillStoreSliceCc> fill_store_slice_cc_pool_;
    CcRequestPool<InitKeyCacheCc> init_key_cache_cc_pool_;
    CcRequestPool<FetchTableRangeSizeCc> fetch_range_size_cc_pool_;

    // CcRequest queue on this shard/core.
    moodycamel::ConcurrentQueue<CcRequestBase *> cc_queue_;
    std::atomic<uint32_t> cc_queue_size_{0};
    std::array<CcRequestBase *, 64> req_buf_;
    std::vector<moodycamel::ProducerToken> thd_token_;
    // Low priority queue for background job requests
    moodycamel::ConcurrentQueue<CcRequestBase *> low_priority_cc_queue_;
    std::atomic<uint32_t> low_priority_cc_queue_size_{0};
    std::vector<moodycamel::ProducerToken> low_priority_thd_token_;
    std::vector<std::unique_ptr<TxObject>> lazy_free_queue_;
    std::atomic<uint32_t> lazy_free_queue_size_{0};
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

    // Set by a page-admission refusal to start a campaign regardless of
    // Full(); cleared when the campaign ends. See RequestCleanCampaign.
    bool clean_campaign_requested_{false};

    // Reclaim attribution for the current cleaning campaign; shard-core
    // only. See NoteDirtyBlockedCandidate.
    size_t clean_dirty_blocked_{0};
    size_t clean_pin_blocked_{0};

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

    // Entries whose buffered-command drain is parked on a page fetch, keyed by
    // node group because term promotion is per node group while a shard hosts
    // entries of several. A set, not a counter: a drain re-stalls once per page
    // it needs, so membership is what makes the accounting idempotent, and the
    // keys are worth naming when recovery is slow.
    //
    // Entries here are pinned by IssueDrainFetches, so they cannot be freed
    // while listed. The value is the key text, captured at stall time because
    // KeyString() is only meaningful while the key lock is held and only the
    // caller has the concrete CcEntry type.
    absl::flat_hash_map<NodeGroupId,
                        absl::flat_hash_map<LruEntry *, std::string>>
        drain_blocked_entries_;

    // Reserved head and tail for the double-linked list of cc entries, which
    // simplifies handling of empty and one-element lists.
    LruPage head_ccp_, tail_ccp_;

    // head --- [small pages] --- protected_head_ --- [large pages] -- tail
    //
    // Declare as a pointer instead of as a dummy page. Eviction policies(SLRU)
    // might share it.
    LruPage *protected_head_page_;

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

    // The number of keys in data tables only (meta tables excluded).
    size_t data_key_count_{0};
    // The number of committed dirty keys in data tables only.
    size_t dirty_data_key_count_{0};
    // Counter for sampling dirty memory checks in AdjustDataKeyStats.
    uint64_t adjust_stats_call_count_{0};

    // Config for dirty memory checkpoint triggering.
    // The interval (in number of calls to AdjustDataKeyStats) to check whether
    // dirty memory exceeds the threshold.
    uint64_t dirty_memory_check_interval_{1000};
    // Pre-calculated threshold in bytes (0 means use 10% of memory_limit_).
    uint64_t dirty_memory_threshold_bytes_{0};

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
