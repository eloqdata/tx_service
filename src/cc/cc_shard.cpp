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
#include "cc/cc_shard.h"

#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <bthread/remote_task_queue.h>

#include <chrono>  // std::chrono
#include <cstdint>
#include <string>

#include "cc/catalog_cc_map.h"
#include "cc/cc_request.h"
#include "cc/cluster_config_cc_map.h"
#include "cc/non_blocking_lock.h"  // lock_vec_
#include "cc/range_bucket_cc_map.h"
#include "cc_map.h"
#include "cc_req_misc.h"
#include "cc_request.pb.h"
#include "checkpointer.h"
#include "error_messages.h"
#include "metrics.h"
#include "range_slice.h"
#include "remote_type.h"
#include "rpc_closure.h"
#include "sharder.h"  // Sharder
#include "store/data_store_handler.h"
#include "tx_id.h"
#include "tx_service_common.h"
#include "tx_start_ts_collector.h"
#include "type.h"
#include "util.h"

DECLARE_bool(cmd_read_catalog);

namespace txservice
{
CcShard::CcShard(
    uint16_t core_id,
    uint32_t core_cnt,
    uint32_t node_memory_limit_mb,
    uint32_t node_log_limit_mb,
    bool realtime_sampling,
    uint32_t ng_id,
    LocalCcShards &local_shards,
    CatalogFactory *catalog_factory[6],
    SystemHandler *system_handler,
    std::unordered_map<uint32_t, std::vector<NodeConfig>> *ng_configs,
    uint64_t cluster_config_version,
    metrics::MetricsRegistry *metrics_registry,
    metrics::CommonLabels common_labels)
    : ng_id_(ng_id),
      core_id_(core_id),
      core_cnt_(core_cnt),
      local_shards_(local_shards),
      realtime_sampling_(realtime_sampling),
      lock_vec_(),
      next_lock_idx_(0),
      used_lock_count_(0),
      lock_holding_txs_(),
      native_ccms_(),
      failover_ccms_(),
      cc_queue_(256),
      req_buf_(),
      tx_vec_(),
      next_tx_idx_(0),
      next_tx_ident_(0),
      next_foward_idx_(0),
      next_forward_sequence_id_(1),
      head_ccp_(nullptr),
      tail_ccp_(nullptr),
      clean_start_ccp_(nullptr),
      size_(0),
      ckpter_(nullptr),
      catalog_factory_{catalog_factory[0],
                       catalog_factory[1],
                       catalog_factory[2],
                       catalog_factory[3],
                       catalog_factory[4],
                       catalog_factory[5]},
      system_handler_(system_handler),
      active_si_txs_()
{
    // memory_limit_ and log_limit_ are calculated at shard level.
    // reserve 5% for range slice info, 10% for data sync heap, 5% for key cache
    memory_limit_ = (uint64_t) MB(node_memory_limit_mb) *
                    (txservice_enable_key_cache ? 0.9 : 0.95);

    memory_limit_ /= core_cnt_;
    log_limit_ = (uint64_t) MB(node_log_limit_mb);
    log_limit_ /= core_cnt_;

    head_ccp_.lru_prev_ = nullptr;
    head_ccp_.lru_next_ = &tail_ccp_;
    tail_ccp_.lru_prev_ = &head_ccp_;
    tail_ccp_.lru_next_ = nullptr;

    thd_token_.reserve((size_t) core_cnt_ + 1);
    for (size_t idx = 0; idx < core_cnt_; ++idx)
    {
        thd_token_.emplace_back(moodycamel::ProducerToken(cc_queue_));
    }

    // cluster config map is only created on core 0.
    if (core_id_ == 0)
    {
        native_ccms_.try_emplace(cluster_config_ccm_name,
                                 std::make_unique<ClusterConfigCcMap>(
                                     this, ng_id_, cluster_config_version));
    }

    // init meter
    if (metrics::enable_metrics)
    {
        common_labels.emplace("native_ng", std::to_string(ng_id_));
        meter_ =
            std::make_unique<metrics::Meter>(metrics_registry, common_labels);
    }

    if (metrics::enable_metrics)
    {
        meter_->Register(metrics::NAME_MEMORY_LIMIT, metrics::Type::Gauge);
        // 10% memory size is reserved for data sync scan
        meter_->Collect(metrics::NAME_MEMORY_LIMIT, memory_limit_ * 0.9);
    }

    if (metrics::enable_cache_hit_rate)
    {
        meter_->Register(metrics::NAME_CACHE_HIT_OR_MISS_TOTAL,
                         metrics::Type::Counter,
                         {{"type", {"hits", "miss"}}});
    }

    if (metrics::enable_memory_usage)
    {
        meter_->Register(metrics::NAME_MEMORY_USAGE, metrics::Type::Gauge);
        meter_->Register(metrics::NAME_FRAGMENT_RATIO, metrics::Type::Gauge);
    }

    if (metrics::enable_standby_metrics)
    {
        // NOTICE(lzx): If standby member nodes can be adjust dynamically, all
        // nodes must be registered as "standby_node_id" label values at here.
        std::vector<metrics::LabelGroup> labels;
        labels.emplace_back("standby_node_id", std::vector<std::string>());
        auto &node_ids = labels[0].second;
        const std::vector<NodeConfig> &member_nodes = ng_configs->at(ng_id_);
        for (const auto &node : member_nodes)
        {
            node_ids.emplace_back(std::to_string(node.node_id_));
        }

        std::vector<metrics::LabelGroup> max_standby_lag_labels = labels;

        meter_->Register(metrics::NAME_STANDBY_LAGGING_MESGS,
                         metrics::Type::Gauge,
                         std::move(labels));

        meter_->Register(metrics::NAME_MAX_STANDBY_LAG,
                         metrics::Type::Gauge,
                         std::move(max_standby_lag_labels));

        for (const auto &node : member_nodes)
        {
            meter_->Collect(metrics::NAME_MAX_STANDBY_LAG,
                            txservice_max_standby_lag,
                            std::to_string(node.node_id_));
        }

        meter_->Register(metrics::NAME_STANDBY_OUT_OF_SYNC_COUNT,
                         metrics::Type::Counter);
    }

    last_read_ts_ = Now();

    retry_fwd_msg_cc_ = std::make_unique<RetryFailedStandbyMsgCc>();
    shard_clean_cc_ = std::make_unique<ShardCleanCc>();
}

void CcShard::Init()
{
    InitializeShardHeap();
    mi_heap_t *prev_heap = shard_heap_->SetAsDefaultHeap();
    lock_vec_.resize(LOCK_ARRAY_INIT_SIZE);
    for (size_t i = 0; i < LOCK_ARRAY_INIT_SIZE; ++i)
    {
        lock_vec_[i] = std::make_unique<KeyGapLockAndExtraData>();
    }
    standby_fwd_vec_.resize(txservice_max_standby_lag);
    for (size_t i = 0; i < txservice_max_standby_lag; ++i)
    {
        standby_fwd_vec_[i] = std::make_unique<StandbyForwardEntry>();
    }

    assert(standby_fwded_msg_buffer_.empty());
    standby_fwded_msg_buffer_.resize(txservice_max_standby_lag, nullptr);

    tx_vec_.reserve(128);
    for (int idx = 0; idx < 128; ++idx)
    {
        tx_vec_.emplace_back(idx);
    }

    native_ccms_.try_emplace(
        catalog_ccm_name,
        std::make_unique<CatalogCcMap>(this, ng_id_, catalog_ccm_name));

    // range bucket map is replicated on every core
    native_ccms_.try_emplace(range_bucket_ccm_name,
                             std::make_unique<RangeBucketCcMap>(
                                 this, ng_id_, range_bucket_ccm_name));
    mi_heap_set_default(prev_heap);
}

CcMap *CcShard::GetCcm(const TableName &table_name, uint32_t node_group)
{
    if (IsNative(node_group))
    {
        auto table_it = native_ccms_.find(table_name);
        if (table_it == native_ccms_.end())
        {
            if (table_name == range_bucket_ccm_name)
            {
                // Initialize range bucket ccm.
                auto insert_it = native_ccms_.emplace(
                    range_bucket_ccm_name,
                    std::make_unique<RangeBucketCcMap>(
                        this, node_group, range_bucket_ccm_name));
                return insert_it.first->second.get();
            }
            if (table_name == cluster_config_ccm_name)
            {
                // cluster config map should only be initialized on core 0.
                assert(core_id_ == 0);
                auto insert_it = native_ccms_.emplace(
                    cluster_config_ccm_name,
                    std::make_unique<ClusterConfigCcMap>(
                        this,
                        node_group,
                        Sharder::Instance().ClusterConfigVersion()));
                return insert_it.first->second.get();
            }
            return nullptr;
        }
        else
        {
            return table_it->second.get();
        }
    }
    else
    {
        auto table_it = failover_ccms_.try_emplace(table_name);
        std::unordered_map<uint32_t, CcMap::uptr> &ng_ccm =
            table_it.first->second;

        auto ccm_it = ng_ccm.find(node_group);
        if (ccm_it != ng_ccm.end())
        {
            return ccm_it->second.get();
        }
        else if (table_name == catalog_ccm_name)
        {
            // The catalog cc map "___catalog" is initialized as one of native
            // cc maps when the cc shard is initialized. The cc map in failed
            // over cc node is initialized lazily, when the cc node becomes the
            // leader.
            auto catalog_it = ng_ccm.find(node_group);
            if (catalog_it != ng_ccm.end())
            {
                return catalog_it->second.get();
            }
            else
            {
                auto insert_it =
                    ng_ccm.emplace(node_group,
                                   std::make_unique<CatalogCcMap>(
                                       this, node_group, catalog_ccm_name));
                return insert_it.first->second.get();
            }
        }
        else if (table_name == range_bucket_ccm_name)
        {
            // range bucket cc map is the same as catalog cc map. It is
            // initialized lazily on failover.
            auto bucket_it = ng_ccm.find(node_group);
            if (bucket_it != ng_ccm.end())
            {
                return bucket_it->second.get();
            }
            else
            {
                auto insert_it = ng_ccm.emplace(
                    node_group,
                    std::make_unique<RangeBucketCcMap>(
                        this, node_group, range_bucket_ccm_name));
                return insert_it.first->second.get();
            }
        }
        else if (table_name == cluster_config_ccm_name)
        {
            // cluster config map should only be initialized on core 0.
            assert(core_id_ == 0);
            auto bucket_it = ng_ccm.find(node_group);
            if (bucket_it != ng_ccm.end())
            {
                return bucket_it->second.get();
            }
            else
            {
                auto insert_it = ng_ccm.emplace(
                    node_group,
                    std::make_unique<ClusterConfigCcMap>(
                        this,
                        node_group,
                        Sharder::Instance().ClusterConfigVersion()));
                return insert_it.first->second.get();
            }
        }
        else
        {
            return nullptr;
        }
    }
}

void CcShard::Enqueue(uint32_t thd_id, CcRequestBase *req)
{
    // The memory order in enqueue() of the concurrent queue ensures that the
    // queue size update is visible first.
    cc_queue_size_.fetch_add(1, std::memory_order_relaxed);

    assert(thd_id < thd_token_.size());
    bool ret = cc_queue_.enqueue(thd_token_.at(thd_id), req);
    assert(ret == true);
    (void) ret;

    // Wakes up the tx processor dedicated to this shard when it is asleep. The
    // notify function internally uses a std::mutex before notifying via the
    // condition variable. This is to create a barrier such that the prior queue
    // size update and the enqueue of the cc request precedes notify().
    NotifyTxProcessor();
}

void CcShard::Enqueue(uint32_t thd_id, uint32_t shard_code, CcRequestBase *req)
{
    local_shards_.EnqueueCcRequest(thd_id, shard_code, req);
}

void CcShard::EnqueueWaitListIfMemoryFull(CcRequestBase *req)
{
    cc_wait_list_for_memory_.push_back(req);
}

bool CcShard::DequeueWaitListAfterMemoryFree(bool abort, bool deque_all)
{
    if (cc_wait_list_for_memory_.size() == 0)
    {
        return true;
    }

    auto it = cc_wait_list_for_memory_.begin();
    for (uint32_t dequeue_cnt = 0; it != cc_wait_list_for_memory_.end() &&
                                   (abort || deque_all || dequeue_cnt < 20);
         ++it)
    {
        if (abort)
        {
            (*it)->AbortCcRequest(CcErrorCode::OUT_OF_MEMORY);
        }
        else
        {
            this->Enqueue((*it));
            ++dequeue_cnt;
        }
    }

    bool is_empty = it == cc_wait_list_for_memory_.end();
    cc_wait_list_for_memory_.erase(cc_wait_list_for_memory_.begin(), it);

    return is_empty;
}

size_t CcShard::WaitListSizeForMemory()
{
    return cc_wait_list_for_memory_.size();
}

void CcShard::WakeUpShardCleanCc()
{
    if (!shard_clean_cc_->InUse())
    {
        shard_clean_cc_->Use();
        Enqueue(shard_clean_cc_.get());
    }
}

void CcShard::Enqueue(CcRequestBase *req)
{
    cc_queue_size_.fetch_add(1, std::memory_order_relaxed);

    bool ret = cc_queue_.enqueue(req);
    assert(ret == true);
    // This silences the -Wunused-but-set-variable warning without any runtime
    // overhead.
    (void) ret;

    // Wakes up the tx processor dedicated to this shard when it is asleep. The
    // notify function internally uses a std::mutex before notifying via the
    // condition variable. This is to create a barrier such that the prior queue
    // size update and the enqueue of the cc request precedes notify().
    NotifyTxProcessor();
}

size_t CcShard::ProcessRequests()
{
    uint32_t queue_size = cc_queue_size_.load(std::memory_order_relaxed);

    if (metrics::enable_memory_usage)
    {
        if (memory_usage_round_++ % metrics::collect_memory_usage_round == 0)
        {
            int64_t allocated, committed;
            mi_thread_stats(&allocated, &committed);
            meter_->Collect(metrics::NAME_MEMORY_USAGE, allocated);
            meter_->Collect(metrics::NAME_FRAGMENT_RATIO,
                            (100 * (static_cast<float>(committed - allocated) /
                                    committed)));
        }
    }

    if (metrics::enable_standby_metrics)
    {
        if (standby_metrics_round_++ > metrics::collect_standby_metrics_round)
        {
            standby_metrics_round_ = 0;
            CollectStandbyMetrics();
        }
    }

    if (queue_size == 0)
    {
        return 0;
    }

    size_t total = 0;
    size_t req_cnt = 0;
    do
    {
        req_cnt = cc_queue_.try_dequeue_bulk(req_buf_.begin(), req_buf_.size());
        total += req_cnt;
        assert(cc_queue_size_.load(std::memory_order_relaxed) >= req_cnt);
        cc_queue_size_.fetch_sub(req_cnt, std::memory_order_acq_rel);

        for (size_t i = 0; i < req_cnt; ++i)
        {
            bool finish = req_buf_[i]->Execute(*this);
            if (finish)
            {
                req_buf_[i]->Free();
            }
        }
    } while (req_cnt > (req_buf_.size() >> 1) && total < 1000);

    return total;
}

void CcShard::AbortCcRequests(std::vector<CcRequestBase *> &&reqs,
                              CcErrorCode err_code)
{
    // Note: This object ownership is owned by itself. We will delete this
    // object in RequestAborterCc::Free()
    RequestAborterCc *request_aborter =
        new RequestAborterCc(std::move(reqs), err_code);
    Enqueue(request_aborter);
}

TEntry &CcShard::NewTx(NodeGroupId tx_ng_id,
                       uint32_t log_group_id,
                       int64_t term)
{
    // allocate start timestamp.
    uint64_t start_ts = Now();

    // Cicurlar iteration to find an available transaction entry.
    size_t cnt = 0;
    while (cnt < tx_vec_.size())
    {
        TEntry &te = tx_vec_[next_tx_idx_];
        if (te.status_ == TxnStatus::Finished ||
            te.status_ == TxnStatus::Committed ||
            te.status_ == TxnStatus::Aborted ||
            te.status_ == TxnStatus::Unknown)
        {
            break;
        }

        ++next_tx_idx_;
        if (next_tx_idx_ >= tx_vec_.size())
        {
            next_tx_idx_ = 0;
        }

        ++cnt;
    }

    if (cnt == tx_vec_.size())
    {
        uint32_t old_size = (uint32_t) tx_vec_.size();
        // Increases the capacity of the tx vector.
        uint32_t new_size = (uint32_t) (tx_vec_.size() * 1.5);
        tx_vec_.reserve(new_size);

        for (uint32_t idx = old_size; idx < new_size; ++idx)
        {
            tx_vec_.emplace_back(idx);
        }

        // position old_size must be an available slot.
        next_tx_idx_ = old_size;
    }

    if (log_group_id != UINT32_MAX)
    {
        auto txlog = Sharder::Instance().GetLogAgent();
        uint64_t global_core_id = GlobalCoreId(tx_ng_id);
        TxNumber new_txn = (global_core_id << 32L) | next_tx_ident_;
        while (txlog->GetLogGroupId(new_txn) != log_group_id)
        {
            ++next_tx_ident_;
            new_txn = (global_core_id << 32L) | next_tx_ident_;
        }
    }
    TEntry &tentry = tx_vec_.at(next_tx_idx_);
    // Reset() set lower_bound ts and commit ts.
    tentry.Reset(start_ts, next_tx_ident_, term);
    ++next_tx_ident_;
    ++next_tx_idx_;
    next_tx_idx_ = next_tx_idx_ == tx_vec_.size() ? 0 : next_tx_idx_;
    return tentry;
}

KeyGapLockAndExtraData *CcShard::NewLock(CcMap *ccm,
                                         LruPage *page,
                                         LruEntry *entry)
{
    // Circular iteration to find an available lock.
    size_t cnt = 0;
    while (cnt < lock_vec_.size())
    {
        KeyGapLockAndExtraData *lk = lock_vec_[next_lock_idx_].get();

        if (lk->GetUsedStatus() == false)
        {
            break;
        }
        ++next_lock_idx_;
        if (next_lock_idx_ >= lock_vec_.size())
        {
            next_lock_idx_ = 0;
        }

        ++cnt;
    }

    if (cnt == lock_vec_.size())
    {
        uint32_t old_size = (uint32_t) lock_vec_.size();
        // Increases the capacity of the lock vector.
        uint32_t new_size = (uint32_t) (lock_vec_.size() * 2);
        lock_vec_.reserve(new_size);

        DLOG(INFO) << "the size of lock array increased to: " << new_size;

        for (uint32_t idx = old_size; idx < new_size; ++idx)
        {
            lock_vec_.emplace_back(std::make_unique<KeyGapLockAndExtraData>());
        }

        // position old_size must be an available slot.
        next_lock_idx_ = old_size;
    }

    KeyGapLockAndExtraData *lk = lock_vec_.at(next_lock_idx_).get();
    assert(!lk->GetUsedStatus());
    lk->Reset(ccm, page, entry);
    lk->SetUsedStatus(true);
    used_lock_count_++;
    ++next_lock_idx_;
    next_lock_idx_ = next_lock_idx_ == lock_vec_.size() ? 0 : next_lock_idx_;
    return lk;
}

TEntry *CcShard::LocateTx(const TxId &tx_id)
{
    if (tx_id.Empty())
    {
        return nullptr;
    }

    TEntry &tentry = tx_vec_.at(tx_id.vec_idx_);
    return tentry.ident_ == tx_id.ident_ ? &tentry : nullptr;
}

TEntry *CcShard::LocateTx(TxNumber tx_number)
{
    // The lower 4 bytes represent the identity on a core, while the higher
    // 4 bytes represent the global core ID.
    uint32_t identity = tx_number & 0xFFFFFFFF;

    for (TEntry &tx_entry : tx_vec_)
    {
        if (tx_entry.ident_ == identity)
        {
            return &tx_entry;
        }
    }

    return nullptr;
}

void CcShard::DetachLru(LruPage *page)
{
    LruPage *prev = page->lru_prev_;
    LruPage *next = page->lru_next_;
    // If page is the head to start looking for cc entry to kickout, move
    // the clean head to the next page
    if (clean_start_ccp_ == page)
    {
        clean_start_ccp_ = page->lru_next_;
    }
    assert(prev != nullptr && next != nullptr);
    prev->lru_next_ = next;
    next->lru_prev_ = prev;
    page->lru_prev_ = nullptr;
    page->lru_next_ = nullptr;
}

// Replace the old_page with new_page in LRU after defrag recreating the cc page
void CcShard::ReplaceLru(LruPage *old_page, LruPage *new_page)
{
    assert(old_page->lru_prev_ != nullptr && old_page->lru_next_ != nullptr);
    LruPage *lru_prev = old_page->lru_prev_;
    old_page->lru_prev_ = nullptr;
    LruPage *lru_next = old_page->lru_next_;
    old_page->lru_next_ = nullptr;
    // If page is the head to start looking for cc entry to kickout, move
    // the clean head to the next page
    if (clean_start_ccp_ == old_page)
    {
        clean_start_ccp_ = new_page;
    }
    lru_prev->lru_next_ = new_page;
    lru_next->lru_prev_ = new_page;
    new_page->lru_next_ = lru_next;
    new_page->lru_prev_ = lru_prev;
}

void CcShard::UpdateLruList(LruPage *page, bool is_emplace)
{
    // We should not add meta cc map page into lru list since they
    // should never be kicked out of memory.
    TableType tbl_type = page->parent_map_->table_name_.Type();
    if (tbl_type != TableType::Primary && tbl_type != TableType::Secondary &&
        tbl_type != TableType::UniqueSecondary)
    {
        assert(page->lru_next_ == nullptr && page->lru_prev_ == nullptr);
        return;
    }
    // page already at the tail, do nothing
    if (page->lru_next_ == &tail_ccp_ && tail_ccp_.lru_prev_ == page)
    {
        ++access_counter_;
        page->last_access_ts_ = access_counter_;
        return;
    }
    // Removes the page from the list, if it's already in the list. This is
    // used to keep the updated page at the end(tail) of the LRU list. A
    // page's prev and post are both not-null when the page is in the
    // list. This is because we have a reserved head and tail for the list.
    if (page->lru_next_ != nullptr)
    {
        DetachLru(page);
    }
    LruPage *second_tail = tail_ccp_.lru_prev_;
    second_tail->lru_next_ = page;
    tail_ccp_.lru_prev_ = page;
    page->lru_next_ = &tail_ccp_;
    page->lru_prev_ = second_tail;

    ++access_counter_;
    page->last_access_ts_ = access_counter_;

    // If the update is a emplace update, these new loaded data might be
    // kickable from cc map. Usually if the clean_start_page is at tail we're
    // not able to load new data into memory, except some special case where we
    // use force_load. In these cases the new loaded data should be able to be
    // kicked from memory once it is unpinned. Set clean_start_page at the new
    // updated page in this case.
    if (is_emplace && clean_start_ccp_ == &tail_ccp_)
    {
        clean_start_ccp_ = page;
    }
}

TxLockInfo *CcShard::UpsertLockHoldingTx(TxNumber txn,
                                         int64_t tx_term,
                                         LruEntry *cce_ptr,
                                         bool is_key_write_lock,
                                         NodeGroupId cc_ng_id,
                                         TableType table_type)
{
    auto [ng_it, is_new_ng] = lock_holding_txs_.try_emplace(cc_ng_id);
    auto [tx_it, is_new_tx] = ng_it->second.try_emplace(txn);
    if (is_new_tx)
    {
        tx_it->second = GetTxLockInfo(tx_term);
    }
    tx_it->second->cce_list_.emplace(cce_ptr);
    tx_it->second->last_recover_ts_ = Now();
    tx_it->second->table_type_ = table_type;

    if (is_key_write_lock)
    {
        // write lock should update ts if the txn exists, or the CkptTsCc
        // request may get an older ckpt_ts.
        tx_it->second->wlock_ts_ = Now();
    }
    return tx_it->second.get();
}

void CcShard::DeleteLockHoldingTx(TxNumber txn,
                                  LruEntry *cce_ptr,
                                  NodeGroupId cc_ng_id)
{
    auto ng_it = lock_holding_txs_.find(cc_ng_id);
    if (ng_it == lock_holding_txs_.end())
    {
        return;
    }

    auto tx_it = ng_it->second.find(txn);
    if (tx_it == ng_it->second.end())
    {
        return;
    }
    TxLockInfo::uptr &lk_info = tx_it->second;
    lk_info->cce_list_.erase(cce_ptr);

    if (lk_info->cce_list_.empty())
    {
        RecycleTxLockInfo(std::move(lk_info));
        ng_it->second.erase(tx_it);
    }
}

void CcShard::DropLockHoldingTxs(NodeGroupId cc_ng_id)
{
    auto it = lock_holding_txs_.find(cc_ng_id);
    if (it == lock_holding_txs_.end())
    {
        return;
    }
    for (auto &[txn, lk_info] : it->second)
    {
        RecycleTxLockInfo(std::move(lk_info));
    }

    lock_holding_txs_.erase(cc_ng_id);
}

void CcShard::CheckRecoverTx(TxNumber lock_holding_txn,
                             uint32_t cc_ng_id,
                             int64_t cc_ng_term)
{
    auto ng_it = lock_holding_txs_.find(cc_ng_id);
    if (ng_it == lock_holding_txs_.end())
    {
        return;
    }
    auto tx_it = ng_it->second.find(lock_holding_txn);
    if (tx_it == ng_it->second.end())
    {
        return;
    }

    CheckRecoverTx(lock_holding_txn, *tx_it->second, cc_ng_id, cc_ng_term);
}

void CcShard::CheckRecoverTx(TxNumber lock_holding_txn,
                             TxLockInfo &lk_info,
                             uint32_t cc_ng_id,
                             int64_t cc_ng_term)
{
    constexpr uint64_t ts_gap =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::seconds(5))
            .count();

    uint64_t now_ts = Now();

    // If the tx has been holding a lock/intention for an extended period of
    // time (more than 5 seconds), inquires the tx's status. If the tx has
    // failed or committed, recovers the orphan lock/intention. Or, does
    // nothing and waits for the tx to make further actions.
    if (now_ts - lk_info.last_recover_ts_ >= ts_gap)
    {
        // Updates the last_recover_ts field, so that following
        // conflicting tx's will not try recovery immediately,
        // avoiding a flood of recovery requests.
        lk_info.last_recover_ts_ = now_ts;

        uint32_t txn_node_group = lock_holding_txn >> 42L;

        CODE_FAULT_INJECTOR("recover_local_txn", { txn_node_group = 12345; })

        // A local transaction might leave orphan locks when log service is
        // unreachable and write log result is unknown. This rarely happens as
        // we retry write log until this node is not group leader anymore. If
        // this node is not group leader, the data and locks are to be cleared;
        // orphan locks left on data belonging to other node groups that failed
        // over to this node still need be recovered.
        if (txn_node_group == Sharder::Instance().NativeNodeGroup() &&
            cc_ng_id == txn_node_group)
        {
            LOG(WARNING)
                << "orphan lock detected, lock holding txn: "
                << lock_holding_txn
                << ", txn is initiated by this machine, try to recover";

            std::unordered_map<std::string, std::unordered_set<LockType>>
                tbl_set;
            for (const auto &lru : lk_info.cce_list_)
            {
                CcMap *ccm = lru->GetCcMap();
                if (ccm != nullptr)
                {
                    NonBlockingLock *lk = lru->GetKeyLock();
                    assert(lk != nullptr);
                    LockType lk_type = lk->SearchLock(lock_holding_txn);
                    auto [it, is_insert] =
                        tbl_set.try_emplace(ccm->table_name_.Trace());
                    it->second.emplace(lk_type);
                }
            }

            for (const auto &tbl_lk : tbl_set)
            {
                for (LockType lk_type : tbl_lk.second)
                {
                    LOG(INFO)
                        << "Txn #" << lock_holding_txn << " locks "
                        << tbl_lk.first << ", lock type: " << (int) lk_type;
                }
            }
        }

        if (Sharder::Instance().LeaderTerm(cc_ng_id) > 0)
        {
            LOG(WARNING) << "orphan lock detected, lock holding txn: "
                         << lock_holding_txn << ", try to recover";
            Sharder::Instance().RecoverTx(lock_holding_txn,
                                          lk_info.tx_coord_term_,
                                          lk_info.wlock_ts_,
                                          cc_ng_id,
                                          cc_ng_term);
        }
        // else:
        // 1. The cc node group of the participant(where lock resides) has
        // failed over to a new leader. There is no need to recover the orphan
        // lock in the old leader because the cc map will be cleared anyway.

        // 2. This is standby node, all locks are acquired by local transaction.
        // There is no need to recover the orphan lock.
    }
}

void CcShard::ClearTx(TxNumber txn)
{
    for (auto &ng_pair : lock_holding_txs_)
    {
        auto tx_it = ng_pair.second.find(txn);
        if (tx_it == ng_pair.second.end())
        {
            continue;
        }

        TxLockInfo::uptr &lk_info = tx_it->second;
        for (auto &lru_ptr : lk_info->cce_list_)
        {
            // This is only called when a tx is aborted after asking log/tx
            // owner, so the cce is not updated in this case. So we do not need
            // to worry about needing to pop blocking commands from cce.
            NonBlockingLock *lock = lru_ptr->GetKeyLock();
            if (lock != nullptr)
            {
                LockType lt = lock->ClearTx(txn, this);
                if (lt == LockType::WriteLock)
                {
                    lru_ptr->GetKeyGapLockAndExtraData()->ClearTx();
                }

                if (FLAGS_cmd_read_catalog &&
                    lk_info->table_type_ == TableType::Catalog)
                {
                    // Do not recycle the lock on Catalog since it's frequently
                    // accessed.
                    continue;
                }

                lru_ptr->RecycleKeyLock(*this);
            }
        }
        RecycleTxLockInfo(std::move(lk_info));
        ng_pair.second.erase(tx_it);
    }
}

void CcShard::VerifyLruList()
{
    LruPage *pre = &head_ccp_;
    for (LruPage *cur = head_ccp_.lru_next_; cur != nullptr;
         cur = cur->lru_next_)
    {
        assert(pre->lru_next_ == cur && cur->lru_prev_ == pre);
        pre = cur;
    }
    assert(pre == &tail_ccp_);
    // This silences the -Wunused-but-set-variable warning without any runtime
    // overhead.
    (void) pre;
}

/**
 * @brief Kick out freeable entries from ccmap.
 *
 * @return A pair, the first of which is the number of freed entries in ccmap,
 * and the second is a bool value that is true if should yield this shard clean
 * operation.
 */
std::pair<size_t, bool> CcShard::Clean()
{
    LruPage *ccp = !txservice_skip_kv && clean_start_ccp_ ? clean_start_ccp_
                                                          : head_ccp_.lru_next_;

    size_t free_cnt = 0;
    bool yield = false;

#ifndef RUNNING_TXSERVICE_CTEST
    size_t clean_page_cnt = 0, scan_page_cnt = 0;
    uint64_t begin_ts =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();
    while (scan_page_cnt < CcShard::freeBatchSize && ccp != &tail_ccp_)
    {
        // merge and removal might happen during Clean so ccp and ccp->lru_next_
        // might change
        auto [freed, next] = ccp->parent_map_->CleanPageAndReBalance(ccp);
        free_cnt += freed;
        ccp = next;
        ++scan_page_cnt;
        clean_page_cnt = (freed > 0 ? (clean_page_cnt + 1) : clean_page_cnt);
        if (clean_page_cnt % 4 == 0)
        {
            uint64_t current_ts =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now()
                        .time_since_epoch())
                    .count();
            if (current_ts - begin_ts >= CcShard::maxDuration)
            {
                break;
            }
        }
    }

    yield = ccp != &tail_ccp_;
#else
    ccp = head_ccp_.lru_next_;
    while (ccp != &tail_ccp_)
    {
        // merge and removal might happen during Clean so ccp and ccp->lru_next_
        // might change
        auto [freed, next] = ccp->parent_map_->CleanPageAndReBalance(ccp);
        free_cnt += freed;
        ccp = next;
    }
#endif
    clean_start_ccp_ = ccp;

    return {free_cnt, yield};
}

/**
 * @brief Flush Entry to KvStore. Now, only used for test.
 *
 */
bool CcShard::FlushEntryForTest(
    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>
        &flush_task_entries,
    bool only_archives)
{
    // TODO(lzx): Now, only flush archives synchronously for test.
    if (only_archives)
    {
        return ckpter_->FlushArchiveForTest(flush_task_entries);
    }
    else
    {
        return (ckpter_->CkptEntryForTest(flush_task_entries)) &&
               (ckpter_->FlushArchiveForTest(flush_task_entries));
    }
}

void CcShard::NotifyCkpt(bool request_ckpt)
{
    if (ckpter_ != nullptr)
    {
        ckpter_->Notify(request_ckpt);
    }
}

void CcShard::SetWaitingCkpt(bool is_waiting)
{
    local_shards_.SetWaitingCkpt(is_waiting);
}

bool CcShard::IsWaitingCkpt()
{
    return local_shards_.IsWaitingCkpt();
}

void CcShard::DispatchTask(uint16_t cc_shard_idx,
                           std::function<bool(CcShard &)> task)
{
    RunOnTxProcessorCc *cc_req = run_on_tx_processor_cc_pool_.NextRequest();
    cc_req->Reset(std::move(task));

    uint32_t producer_thd_id = core_id_;
    Enqueue(producer_thd_id, cc_shard_idx, cc_req);
}

std::pair<bool, const CatalogEntry *> CcShard::CreateCatalog(
    const TableName &table_name,
    NodeGroupId cc_ng_id,
    const std::string &catalog_image,
    uint64_t commit_ts)
{
    return local_shards_.CreateCatalog(
        table_name, cc_ng_id, catalog_image, commit_ts);
}

std::pair<bool, const CatalogEntry *> CcShard::CreateReplayCatalog(
    const TableName &table_name,
    NodeGroupId cc_ng_id,
    const std::string &old_schema_image,
    const std::string &new_schema_image,
    uint64_t old_schema_ts,
    uint64_t dirty_schema_ts)
{
    return local_shards_.CreateReplayCatalog(table_name,
                                             cc_ng_id,
                                             old_schema_image,
                                             new_schema_image,
                                             old_schema_ts,
                                             dirty_schema_ts);
}

CatalogEntry *CcShard::CreateDirtyCatalog(const TableName &table_name,
                                          NodeGroupId cc_ng_id,
                                          const std::string &catalog_image,
                                          uint64_t commit_ts)
{
    return local_shards_.CreateDirtyCatalog(
        table_name, cc_ng_id, catalog_image, commit_ts);
}

void CcShard::UpdateDirtyCatalog(const TableName &table_name,
                                 const std::string &catalog_image,
                                 CatalogEntry *catalog_entry)
{
    return local_shards_.UpdateDirtyCatalog(
        table_name, catalog_image, catalog_entry);
}

CatalogEntry *CcShard::GetCatalog(const TableName &table_name,
                                  NodeGroupId cc_ng_id)
{
    return local_shards_.GetCatalog(table_name, cc_ng_id);
}

void CcShard::InitTableRanges(const TableName &range_table_name,
                              std::vector<InitRangeEntry> &init_ranges,
                              NodeGroupId ng_id,
                              bool fully_cached)
{
    local_shards_.InitTableRanges(
        range_table_name, init_ranges, ng_id, fully_cached);
}

std::map<TxKey, TableRangeEntry::uptr> *CcShard::GetTableRangesForATable(
    const TableName &range_table_name, const NodeGroupId ng_id)
{
    return local_shards_.GetTableRangesForATable(range_table_name, ng_id);
}

void CcShard::FetchCatalog(const TableName &table_name,
                           NodeGroupId cc_ng_id,
                           int64_t cc_ng_term,
                           CcRequestBase *requester)
{
    assert(cc_ng_term > 0);
    FetchCatalogCc *fetch_req = nullptr;
    auto tab_it = fetch_reqs_.find(table_name);
    if (tab_it != fetch_reqs_.end())
    {
        fetch_req = static_cast<FetchCatalogCc *>(tab_it->second.get());
    }
    else
    {
        assert(!(txservice_skip_kv && (Sharder::Instance().GetPrimaryNodeId() ==
                                       Sharder::Instance().NodeId())));
        // All standby nodes should fetch catalog from primay node.
        bool fetch_from_primary = IsStandbyTx(cc_ng_term);
        std::unique_ptr<FetchCatalogCc> fetch_catalog_cc =
            std::make_unique<FetchCatalogCc>(
                table_name, *this, cc_ng_id, cc_ng_term, fetch_from_primary);

        fetch_req = fetch_catalog_cc.get();
        fetch_reqs_.emplace(table_name, std::move(fetch_catalog_cc));
        if (fetch_from_primary)
        {
            int64_t primary_node_id = Sharder::Instance().GetPrimaryNodeId();
            auto channel =
                Sharder::Instance().GetCcNodeServiceChannel(primary_node_id);
            assert(channel != nullptr);
            if (!channel)
            {
                if (requester)
                {
                    // Renqueue the requester to retry.
                    Enqueue(core_id_, requester);
                }
                RemoveFetchRequest(table_name);
                return;
            }
            FetchCatalogClosure *closure = new FetchCatalogClosure(fetch_req);
            closure->SetChannel(primary_node_id, channel);
            auto req = closure->FetchPayloadRequest();
            CatalogKey key(table_name);
            std::string key_str;
            key.Serialize(key_str);
            req->set_key_str(std::move(key_str));
            req->set_node_group_id(cc_ng_id);
            req->set_table_name_str(catalog_ccm_name.String());
            req->set_table_type(remote::ToRemoteType::ConvertTableType(
                catalog_ccm_name.Type()));
            req->set_table_engine(remote::ToRemoteType::ConvertTableEngine(
                catalog_ccm_name.Engine()));

            int64_t primary_node_term = PrimaryTermFromStandbyTerm(cc_ng_term);
            req->set_primary_leader_term(primary_node_term);
            req->set_key_shard_code(cc_ng_id << 10);
            closure->Controller()->set_timeout_ms(5000);
            closure->Controller()->set_write_to_socket_in_background(true);
            remote::CcRpcService_Stub stub(channel.get());
            stub.FetchCatalog(closure->Controller(),
                              closure->FetchPayloadRequest(),
                              closure->FetchPayloadResponse(),
                              closure);
        }
        else
        {
            local_shards_.store_hd_->FetchTableCatalog(table_name, fetch_req);
        }
    }

    if (requester != nullptr)
    {
        fetch_req->AddRequester(requester);
    }
}

void CcShard::FetchTableStatistics(const TableName &table_name,
                                   NodeGroupId cc_ng_id,
                                   int64_t cc_ng_term,
                                   CcRequestBase *requester)
{
    FetchTableStatisticsCc *fetch_req = nullptr;
    auto tab_it = fetch_reqs_.find(table_name);
    if (tab_it != fetch_reqs_.end())
    {
        fetch_req = static_cast<FetchTableStatisticsCc *>(tab_it->second.get());
    }
    else
    {
        std::unique_ptr<FetchTableStatisticsCc> fetch_statistics_cc =
            std::make_unique<FetchTableStatisticsCc>(
                table_name, *this, cc_ng_id, cc_ng_term);
        fetch_req = fetch_statistics_cc.get();
        fetch_reqs_.emplace(table_name, std::move(fetch_statistics_cc));
    }

    fetch_req->AddRequester(requester);
    if (fetch_req->RequesterCount() == 1)
    {
        local_shards_.store_hd_->FetchCurrentTableStatistics(table_name,
                                                             fetch_req);
    }
}

void CcShard::FetchTableRanges(const TableName &table_name,
                               CcRequestBase *requester,
                               NodeGroupId cc_ng_id,
                               int64_t cc_ng_term)
{
    FetchTableRangesCc *fetch_req = nullptr;
    auto table_it = fetch_reqs_.find(table_name);
    if (table_it != fetch_reqs_.end())
    {
        fetch_req = static_cast<FetchTableRangesCc *>(table_it->second.get());
    }
    else
    {
        auto insert_it = fetch_reqs_.try_emplace(table_name, nullptr);

        std::unique_ptr<FetchTableRangesCc> fetch_range_cc =
            std::make_unique<FetchTableRangesCc>(
                insert_it.first->first, *this, cc_ng_id, cc_ng_term);
        fetch_req = fetch_range_cc.get();

        insert_it.first->second = std::move(fetch_range_cc);
    }

    fetch_req->AddRequester(requester);
    if (fetch_req->RequesterCount() == 1)
    {
        local_shards_.store_hd_->FetchTableRanges(fetch_req);
    }
}

void CcShard::CleanTableRange(const TableName &table_name,
                              const NodeGroupId ng_id)
{
    local_shards_.CleanTableRange(table_name, ng_id);
}

TableRangeEntry *CcShard::GetTableRangeEntry(const TableName &table_name,
                                             const NodeGroupId ng_id,
                                             const TxKey &key)
{
    return local_shards_.GetTableRangeEntry(table_name, ng_id, key);
}

const TableRangeEntry *CcShard::GetTableRangeEntry(const TableName &table_name,
                                                   const NodeGroupId ng_id,
                                                   int32_t range_id)
{
    return local_shards_.GetTableRangeEntry(table_name, ng_id, range_id);
}

const TableRangeEntry *CcShard::GetTableRangeEntryNoLocking(
    const TableName &table_name, const NodeGroupId ng_id, const TxKey &key)
{
    return local_shards_.GetTableRangeEntryNoLocking(table_name, ng_id, key);
}

bool CcShard::CheckRangeVersion(const TableName &table_name,
                                const NodeGroupId ng_id,
                                int32_t range_id,
                                uint64_t range_version)
{
    return local_shards_.CheckRangeVersion(
        table_name, ng_id, range_id, range_version);
}

uint64_t CcShard::CountRanges(const TableName &table_name,
                              const NodeGroupId ng_id,
                              const NodeGroupId key_ng_id)
{
    return local_shards_.CountRanges(table_name, ng_id, key_ng_id);
}

uint64_t CcShard::CountRangesLockless(const TableName &table_name,
                                      const NodeGroupId ng_id,
                                      const NodeGroupId key_ng_id)
{
    return local_shards_.CountRangesLockless(table_name, ng_id, key_ng_id);
}

uint64_t CcShard::CountSlices(const TableName &table_name,
                              const NodeGroupId ng_id,
                              const NodeGroupId local_ng_id) const
{
    return local_shards_.CountSlices(table_name, ng_id, local_ng_id);
}

std::pair<std::shared_ptr<Statistics>, bool> CcShard::InitTableStatistics(
    TableSchema *table_schema, NodeGroupId ng_id)
{
    return local_shards_.InitTableStatistics(table_schema, ng_id);
}

std::pair<std::shared_ptr<Statistics>, bool> CcShard::InitTableStatistics(
    TableSchema *table_schema,
    TableSchema *dirty_table_schema,
    NodeGroupId ng_id,
    std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
        sample_pool_map)
{
    return local_shards_.InitTableStatistics(table_schema,
                                             dirty_table_schema,
                                             ng_id,
                                             std::move(sample_pool_map),
                                             this);
}

StatisticsEntry *CcShard::GetTableStatistics(const TableName &table_name,
                                             NodeGroupId ng_id)
{
    return local_shards_.GetTableStatistics(table_name, ng_id);
}

const StatisticsEntry *CcShard::LoadRangesAndStatisticsNx(
    const TableSchema *curr_schema,
    NodeGroupId cc_ng_id,
    int64_t cc_ng_term,
    CcRequestBase *requester)
{
    const StatisticsEntry *statistics_entry =
        GetTableStatistics(curr_schema->GetBaseTableName(), cc_ng_id);
    if (statistics_entry)
    {
        return statistics_entry;
    }

    if (!curr_schema->GetBaseTableName().IsHashPartitioned())
    {
        // Initialize table ranges before create table
        // statistics.
        TableName base_range_table_name(
            curr_schema->GetBaseTableName().StringView(),
            TableType::RangePartition,
            curr_schema->GetBaseTableName().Engine());
        const auto *ranges =
            GetTableRangesForATable(base_range_table_name, cc_ng_id);
        if (ranges == nullptr)
        {
            FetchTableRanges(
                base_range_table_name, requester, cc_ng_id, cc_ng_term);
            return nullptr;
        }
    }

    std::vector<TableName> index_names = curr_schema->IndexNames();
    for (const TableName &index_name : index_names)
    {
        TableName index_range_table_name(index_name.StringView(),
                                         TableType::RangePartition,
                                         index_name.Engine());
        const auto *ranges =
            GetTableRangesForATable(index_range_table_name, cc_ng_id);
        if (ranges == nullptr)
        {
            FetchTableRanges(
                index_range_table_name, requester, cc_ng_id, cc_ng_term);
            return nullptr;
        }
    }

    statistics_entry =
        GetTableStatistics(curr_schema->GetBaseTableName(), cc_ng_id);
    if (statistics_entry == nullptr)
    {
        FetchTableStatistics(
            curr_schema->GetBaseTableName(), cc_ng_id, cc_ng_term, requester);
        return nullptr;
    }

    return statistics_entry;
}

void CcShard::CleanTableStatistics(const TableName &table_name,
                                   NodeGroupId ng_id)
{
    return local_shards_.CleanTableStatistics(table_name, ng_id);
}

const BucketInfo *CcShard::GetBucketInfo(uint16_t bucket_id,
                                         NodeGroupId ng_id) const
{
    return local_shards_.GetBucketInfo(bucket_id, ng_id);
}

BucketInfo *CcShard::GetBucketInfo(uint16_t bucket_id, NodeGroupId ng_id)
{
    return local_shards_.GetBucketInfo(bucket_id, ng_id);
}

NodeGroupId CcShard::GetBucketOwner(uint16_t bucket_id, NodeGroupId ng_id) const
{
    return local_shards_.GetBucketOwner(bucket_id, ng_id);
}

const BucketInfo *CcShard::GetRangeOwner(int32_t range_id,
                                         NodeGroupId ng_id) const
{
    return local_shards_.GetRangeOwner(range_id, ng_id);
}

const std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>> *
CcShard::GetAllBucketInfos(NodeGroupId ng_id) const
{
    return local_shards_.GetAllBucketInfos(ng_id);
}

void CcShard::SetBucketMigrating(bool is_migrating)
{
    local_shards_.SetBucketMigrating(is_migrating);
}

bool CcShard::IsBucketsMigrating()
{
    return local_shards_.IsBucketsMigrating();
}

void CcShard::DropBucketInfo(NodeGroupId ng_id)
{
    local_shards_.DropBucketInfo(ng_id);
}

void CcShard::RemoveFetchRequest(const TableName &table_name)
{
    fetch_reqs_.erase(table_name);
}

store::DataStoreHandler::DataStoreOpStatus CcShard::FetchRecord(
    const TableName &table_name,
    const TableSchema *tbl_schema,
    TxKey key,
    LruEntry *cce,
    NodeGroupId cc_ng_id,
    int64_t cc_ng_term,
    CcRequestBase *requester,
    int32_t partition_id,
    bool fetch_from_primary,
    uint32_t key_shard_code,
    uint64_t snapshot_read_ts,
    bool only_fetch_archives)
{
    auto tab_it = fetch_record_reqs_.try_emplace(cce,
                                                 &table_name,
                                                 tbl_schema,
                                                 std::move(key),
                                                 cce,
                                                 *this,
                                                 cc_ng_id,
                                                 cc_ng_term,
                                                 partition_id,
                                                 fetch_from_primary,
                                                 snapshot_read_ts,
                                                 only_fetch_archives);
    FetchRecordCc *fetch_req = &(tab_it.first->second);
    fetch_req->AddRequester(requester);

    CODE_FAULT_INJECTOR("disable_fetch_record", {
        LOG(INFO) << "FaultInject disable_fetch_record hitted, mark record as "
                     "deleted";
        fetch_req->rec_status_ = RecordStatus::Deleted;
        fetch_req->rec_ts_ = 1;
        fetch_req->SetFinish(static_cast<int>(CcErrorCode::NO_ERROR));
        return store::DataStoreHandler::DataStoreOpStatus::Success;
    });

    if (fetch_req->RequesterCount() == 1)
    {
        // The responsibility of releasing this pin is the FetchRecordCc.
        cce->GetKeyGapLockAndExtraData()->AddPin();
        if (txservice_skip_kv || fetch_from_primary)
        {
            int64_t primary_node_id = Sharder::Instance().GetPrimaryNodeId();
            auto channel =
                Sharder::Instance().GetCcNodeServiceChannel(primary_node_id);
            if (!channel)
            {
                // channel is not established, retry later.
                // Remove fetch req
                RemoveFetchRecordRequest(cce);
                cce->GetKeyGapLockAndExtraData()->ReleasePin();
                cce->RecycleKeyLock(*this);
                return store::DataStoreHandler::DataStoreOpStatus::Retry;
            }

            FetchRecordClosure *closure = new FetchRecordClosure(fetch_req);
            closure->SetChannel(primary_node_id, channel);
            auto req = closure->FetchPayloadRequest();
            std::string key_str;
            key.Serialize(key_str);
            req->set_key_str(std::move(key_str));
            req->set_node_group_id(cc_ng_id);
            req->set_table_name_str(table_name.String());
            req->set_table_type(
                remote::ToRemoteType::ConvertTableType(table_name.Type()));
            req->set_table_engine(
                remote::ToRemoteType::ConvertTableEngine(table_name.Engine()));

            int64_t primary_leader_term =
                PrimaryTermFromStandbyTerm(cc_ng_term);
            req->set_primary_leader_term(primary_leader_term);
            req->set_key_shard_code(key_shard_code);
            closure->Controller()->set_timeout_ms(5000);
            closure->Controller()->set_write_to_socket_in_background(true);
            remote::CcRpcService_Stub stub(channel.get());
            stub.FetchPayload(closure->Controller(),
                              closure->FetchPayloadRequest(),
                              closure->FetchPayloadResponse(),
                              closure);
        }
        else
        {
            auto res = local_shards_.store_hd_->FetchRecord(fetch_req);
            if (res == store::DataStoreHandler::DataStoreOpStatus::Retry)
            {
                // Remove fetch req
                RemoveFetchRecordRequest(cce);
                cce->GetKeyGapLockAndExtraData()->ReleasePin();
                cce->RecycleKeyLock(*this);

                return store::DataStoreHandler::DataStoreOpStatus::Retry;
            }
        }
    }

    if (requester != nullptr)
    {
        // Use pins to prevent the cce from being recycled before the request
        // be processed. We will release the pin when the request be processed
        // again or be aborted.
        // The responsibility of releasing this pin is the ccrequest.
        cce->GetKeyGapLockAndExtraData()->AddPin();
    }

    return store::DataStoreHandler::DataStoreOpStatus::Success;
}

store::DataStoreHandler::DataStoreOpStatus CcShard::FetchSnapshot(
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
    int32_t partition_id)
{
    FetchSnapshotCc *fetch_cc = fetch_snapshot_cc_pool_.NextRequest();
    fetch_cc->Reset(&table_name,
                    tbl_schema,
                    std::move(key),
                    *this,
                    cc_ng_id,
                    cc_ng_term,
                    snapshot_read_ts,
                    only_fetch_archive,
                    requester,
                    tuple_idx,
                    backfill_func,
                    partition_id);

    return local_shards_.store_hd_->FetchRecord(nullptr, fetch_cc);
}

store::DataStoreHandler::DataStoreOpStatus CcShard::FetchBucketData(
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
    OnFetchedBucketData backfill_func)
{
    std::vector<FetchBucketDataCc *> requests;
    requests.reserve(bucket_ids.size());
    for (const auto &[bucket, kv_is_drained] : bucket_ids)
    {
        if (kv_is_drained)
        {
            continue;
        }

        if (is_local)
        {
            ScanNextBatchCc *scan_next_batch_cc =
                static_cast<ScanNextBatchCc *>(requester);
            // increate counter
            scan_next_batch_cc->IncreaseWaitForFetchBucketCnt(core_id_);
        }
        else
        {
            remote::RemoteScanNextBatch *remote_scan_next_batch_cc =
                static_cast<remote::RemoteScanNextBatch *>(requester);
            remote_scan_next_batch_cc->IncreaseWaitForFetchBucketCnt(core_id_);
        }

        FetchBucketDataCc *fetch_bucket_data_cc =
            fetch_bucket_data_cc_pool_.NextRequest();
        fetch_bucket_data_cc->Reset(table_name,
                                    table_schema,
                                    node_group_id,
                                    node_group_term,
                                    ccs,
                                    is_local,
                                    bucket,
                                    pushdown_cond_,
                                    start_key,
                                    start_key_type,
                                    start_key_inclusive,
                                    end_key,
                                    end_key_type,
                                    end_key_inclusive,
                                    batch_size,
                                    requester,
                                    backfill_func);
        requests.push_back(fetch_bucket_data_cc);
    }

    if (!requests.empty())
    {
        auto err_code = local_shards_.store_hd_->FetchBucketData(requests);
        assert(err_code == store::DataStoreHandler::DataStoreOpStatus::Success);
        return err_code;
    }

    return store::DataStoreHandler::DataStoreOpStatus::Success;
}

store::DataStoreHandler::DataStoreOpStatus CcShard::FetchBucketData(
    FetchBucketDataCc *fetch_bucket_data_cc)
{
    auto err_code =
        local_shards_.store_hd_->FetchBucketData(fetch_bucket_data_cc);
    assert(err_code == store::DataStoreHandler::DataStoreOpStatus::Success);
    return err_code;
}

void CcShard::RemoveFetchRecordRequest(LruEntry *cce)
{
    fetch_record_reqs_.erase(cce);
}

CcMap *CcShard::CreateOrUpdatePkCcMap(const TableName &table_name,
                                      const TableSchema *table_schema,
                                      NodeGroupId ng_id,
                                      bool is_create,
                                      bool ccm_has_full_entries)
{
    if (IsNative(ng_id))
    {
        auto ccm_it =
            native_ccms_.try_emplace(table_name,
                                     GetCatalogFactory(table_name.Engine())
                                         ->CreatePkCcMap(table_name,
                                                         table_schema,
                                                         ccm_has_full_entries,
                                                         this,
                                                         ng_id));
        // update table schema for alter table command.
        if (!is_create && !ccm_it.second)
        {
            CcMap *ccm = ccm_it.first->second.get();
            ccm->SetTableSchema(table_schema);
        }
        assert(ccm_it.first->first.IsStringOwner());
        return ccm_it.first->second.get();
    }
    else
    {
        auto fail_ccm_it = failover_ccms_.try_emplace(table_name).first;
        std::unordered_map<NodeGroupId, CcMap::uptr> &ccms =
            fail_ccm_it->second;
        auto ccm_it = ccms.try_emplace(ng_id,
                                       GetCatalogFactory(table_name.Engine())
                                           ->CreatePkCcMap(table_name,
                                                           table_schema,
                                                           ccm_has_full_entries,
                                                           this,
                                                           ng_id));
        // update table schema for alter table command.
        if (!is_create && !ccm_it.second)
        {
            CcMap *ccm = ccm_it.first->second.get();
            ccm->SetTableSchema(table_schema);
        }
        return ccm_it.first->second.get();
    }
}

CcMap *CcShard::CreateOrUpdateSkCcMap(const TableName &index_name,
                                      const TableSchema *table_schema,
                                      NodeGroupId ng_id,
                                      bool is_create)
{
    if (IsNative(ng_id))
    {
        auto ccm_it = native_ccms_.try_emplace(
            index_name,
            GetCatalogFactory(index_name.Engine())
                ->CreateSkCcMap(index_name, table_schema, this, ng_id));
        // update table schema for current sk cc map
        if (!is_create && !ccm_it.second)
        {
            CcMap *ccm = ccm_it.first->second.get();
            ccm->SetTableSchema(table_schema);
        }
        return ccm_it.first->second.get();
    }
    else
    {
        auto fail_ccm_it = failover_ccms_.try_emplace(index_name).first;
        std::unordered_map<NodeGroupId, CcMap::uptr> &ccms =
            fail_ccm_it->second;
        auto ccm_it = ccms.try_emplace(
            ng_id,
            GetCatalogFactory(index_name.Engine())
                ->CreateSkCcMap(index_name, table_schema, this, ng_id));
        // update table schema for current sk cc map
        if (!is_create && !ccm_it.second)
        {
            CcMap *ccm = ccm_it.first->second.get();
            ccm->SetTableSchema(table_schema);
        }
        return ccm_it.first->second.get();
    }
}

const CatalogEntry *CcShard::InitCcm(const TableName &table_name,
                                     NodeGroupId cc_ng_id,
                                     int64_t cc_ng_term,
                                     CcRequestBase *requester)
{
    const TableName base_table_name{table_name.GetBaseTableNameSV(),
                                    TableType::Primary,
                                    table_name.Engine()};

    const CatalogEntry *catalog_entry = GetCatalog(base_table_name, cc_ng_id);
    if (catalog_entry == nullptr)
    {
        // The local node does not contain the table's schema instance. The
        // FetchCatalog() method sends an async request toward the data
        // store to fetch the catalog. After fetching is finished, this cc
        // request is re-enqueued for re-execution.
        FetchCatalog(base_table_name, cc_ng_id, cc_ng_term, requester);
        return nullptr;
    }

    const TableSchema *curr_schema = catalog_entry->schema_.get();
    uint64_t schema_ts = 0;
    if (curr_schema != nullptr)
    {
        if (table_name.IsBase())
        {
            schema_ts = curr_schema->KeySchema()->SchemaTs();
        }
        else
        {
            schema_ts = curr_schema->IndexKeySchema(table_name)->SchemaTs();
        }
    }

    if (curr_schema != nullptr && schema_ts > 0)
    {
        if (const auto request_schema_version = requester->SchemaVersion();
            request_schema_version != 0 && request_schema_version != schema_ts)
        {
            // For DDL operations (e.g., `flushdb` in Redis protocol), if one tx
            // processor completes the operation earlier, it could potentially
            // read data from other tx processors by simply acquiring a read
            // lock on itself. However, other processors might not have
            // completed the DDL operation yet, meaning their schema version
            // could still be old. Initiating ccm with the old version leads to
            // conflicts. Therefore, we explicitly verify the schema version
            // here to prevent such cases.
            requester->AbortCcRequest(
                CcErrorCode::REQUESTED_TABLE_SCHEMA_MISMATCH);
            return nullptr;
        }
        auto cc_map = GetCcm(catalog_ccm_name, cc_ng_id);
        CatalogCcMap *catalog_ccm = reinterpret_cast<CatalogCcMap *>(cc_map);
        if (catalog_ccm->HasWriteLock(table_name, cc_ng_id))
        {
            // This verification is used in cases where this node receives a
            // request from an old leader with an outdated schema version,
            // which happens to match the current schema version. Such a request
            // should still be aborted. We verify this by checking whether the
            // table is being updated, using the write lock as an indicator,
            // since a table is being changed under write lock.
            requester->AbortCcRequest(
                CcErrorCode::REQUESTED_TABLE_SCHEMA_MISMATCH);
            return nullptr;
        }
#ifdef STATISTICS
        if (!LoadRangesAndStatisticsNx(
                curr_schema, cc_ng_id, cc_ng_term, requester))
        {
            return nullptr;
        }
#endif

        std::vector<TableName> index_names = curr_schema->IndexNames();
        bool ccm_has_full_entries = txservice_skip_kv;
        CreateOrUpdatePkCcMap(
            base_table_name, curr_schema, cc_ng_id, true, ccm_has_full_entries);

        for (const TableName &index_name : index_names)
        {
            CreateOrUpdateSkCcMap(index_name, curr_schema, cc_ng_id);
        }
    }

    return catalog_entry;
}

void CcShard::DropCcm(const TableName &table_name, NodeGroupId ng_id)
{
    if (IsNative(ng_id))
    {
        native_ccms_.erase(table_name);
    }
    else
    {
        auto fail_ccm_it = failover_ccms_.find(table_name);
        if (fail_ccm_it != failover_ccms_.end())
        {
            std::unordered_map<NodeGroupId, CcMap::uptr> &ccms =
                fail_ccm_it->second;
            ccms.erase(ng_id);
            if (ccms.empty())
            {
                failover_ccms_.erase(fail_ccm_it);
            }
        }
    }
}

bool CcShard::CleanCcmPages(const txservice::TableName &table_name,
                            txservice::NodeGroupId ng_id,
                            uint64_t clean_ts,
                            bool truncate_table)
{
    if (IsNative(ng_id))
    {
        auto native_it = native_ccms_.find(table_name);

        if (native_it != native_ccms_.end())
        {
            auto &ccm = native_it->second;

            if (ccm->SchemaTs() >= clean_ts)
            {
                // This request is outdate.
                return true;
            }

            if (!ccm->CleanBatchPages(32))
            {
                // Yield
                return false;
            }

            if (truncate_table)
            {
                ccm->ccm_has_full_entries_ = true;
            }
        }
        // else: 1. This request is outdate, the ccmap was erased.
        // 2. No data in memory
    }
    else
    {
        auto fail_ccm_it = failover_ccms_.find(table_name);
        if (fail_ccm_it != failover_ccms_.end())
        {
            std::unordered_map<NodeGroupId, CcMap::uptr> &ccms =
                fail_ccm_it->second;
            auto it = ccms.find(ng_id);
            if (it != ccms.end())
            {
                auto &ccm = it->second;
                if (ccm->SchemaTs() >= clean_ts)
                {
                    return true;
                }

                if (!ccm->CleanBatchPages(32))
                {
                    // Has more data. Yield to avoid blocking txprocessor
                    return false;
                }

                if (truncate_table)
                {
                    ccm->ccm_has_full_entries_ = true;
                }
            }
        }
    }

    return true;
}

void CcShard::UpdateCcmSchema(const TableName &table_name,
                              NodeGroupId node_group_id,
                              const TableSchema *table_schema,
                              uint64_t schema_ts)
{
    if (IsNative(node_group_id))
    {
        auto native_it = native_ccms_.find(table_name);

        if (native_it != native_ccms_.end())
        {
            auto &ccm = native_it->second;

            ccm->SetTableSchema(table_schema);
            ccm->SetSchemaTs(schema_ts);

            DLOG(INFO) << "Truncate ccm on shard: " << core_id_
                       << ", set new table schema: " << ccm->GetTableSchema()
                       << ", schema ts: " << ccm->SchemaTs();
        }
    }
    else
    {
        auto fail_ccm_it = failover_ccms_.find(table_name);
        if (fail_ccm_it != failover_ccms_.end())
        {
            std::unordered_map<NodeGroupId, CcMap::uptr> &ccms =
                fail_ccm_it->second;
            auto it = ccms.find(node_group_id);
            if (it != ccms.end())
            {
                auto &ccm = it->second;

                ccm->SetTableSchema(table_schema);
                ccm->SetSchemaTs(schema_ts);

                DLOG(INFO) << "Truncate ccm on shard: " << core_id_
                           << ", set new table schema: "
                           << ccm->GetTableSchema()
                           << ", schema ts: " << ccm->SchemaTs();
            }
        }
    }
}

/**
 * @brief Clean ccentries from ccm given a table name.
 *
 * @param table_name
 */
void CcShard::CleanCcm(const TableName &table_name)
{
    auto native_it = native_ccms_.find(table_name);
    if (native_it != native_ccms_.end())
    {
        native_it->second->Clean();
    }

    auto fail_ccm_it = failover_ccms_.find(table_name);
    if (fail_ccm_it != failover_ccms_.end())
    {
        std::unordered_map<NodeGroupId, CcMap::uptr> &ccms =
            fail_ccm_it->second;
        std::unordered_map<NodeGroupId, CcMap::uptr>::iterator it =
            ccms.begin();
        while (it != ccms.end())
        {
            it->second->Clean();
            it++;
        }
    }
}

void CcShard::CleanCcm(const TableName &table_name, NodeGroupId ng_id)
{
    if (IsNative(ng_id))
    {
        auto native_it = native_ccms_.find(table_name);
        if (native_it != native_ccms_.end())
        {
            native_it->second->Clean();
        }
    }
    else
    {
        auto fail_ccm_it = failover_ccms_.find(table_name);
        if (fail_ccm_it != failover_ccms_.end())
        {
            std::unordered_map<NodeGroupId, CcMap::uptr> &ccms =
                fail_ccm_it->second;
            auto it = ccms.find(ng_id);
            if (it != ccms.end())
            {
                it->second->Clean();
            }
        }
    }
}

void CcShard::DropCcms(NodeGroupId ng_id)
{
    if (IsNative(ng_id))
    {
        for (auto &[table_name, ccm] : native_ccms_)
        {
            if (table_name == catalog_ccm_name)
            {
                ccm->Clean();
            }
        }
        absl::erase_if(native_ccms_,
                       [&](const auto &entry)
                       {
                           return entry.first != catalog_ccm_name &&
                                  entry.first != range_bucket_ccm_name;
                       });
        // Drop range bucket ccm last. We might call release cce lock
        // on cce in range bucket ccm in range ccmap desctructor.
        native_ccms_.erase(range_bucket_ccm_name);
    }
    else
    {
        for (auto table_it = failover_ccms_.begin();
             table_it != failover_ccms_.end();)
        {
            if (table_it->first == range_bucket_ccm_name)
            {
                ++table_it;
                continue;
            }
            std::unordered_map<NodeGroupId, CcMap::uptr> &ng_ccm =
                table_it->second;
            ng_ccm.erase(ng_id);
            if (ng_ccm.empty())
            {
                table_it = failover_ccms_.erase(table_it);
            }
            else
            {
                ++table_it;
            }
        }
        auto range_bucket_it = failover_ccms_.find(range_bucket_ccm_name);
        if (range_bucket_it != failover_ccms_.end())
        {
            range_bucket_it->second.erase(ng_id);
            if (range_bucket_it->second.empty())
            {
                failover_ccms_.erase(range_bucket_it);
            }
        }
    }
}

void CcShard::CreateOrUpdateRangeCcMap(const TableName &table_name,
                                       const TableSchema *table_schema,
                                       NodeGroupId ng_id,
                                       uint64_t schema_ts,
                                       bool is_create)
{
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition,
                               table_name.Engine());
    if (IsNative(ng_id))
    {
        auto ccm_it = native_ccms_.try_emplace(
            range_table_name,
            GetCatalogFactory(range_table_name.Engine())
                ->CreateRangeMap(
                    range_table_name, table_schema, schema_ts, this, ng_id));
        // update table schema for current range cc map
        if (!is_create && !ccm_it.second)
        {
            CcMap *ccm = ccm_it.first->second.get();
            ccm->SetTableSchema(table_schema);
            ccm->SetSchemaTs(schema_ts);
        }
    }
    else
    {
        auto fail_range_it = failover_ccms_.try_emplace(range_table_name).first;
        std::unordered_map<NodeGroupId, CcMap::uptr> &range_maps =
            fail_range_it->second;
        auto ccm_it = range_maps.try_emplace(
            ng_id,
            GetCatalogFactory(range_table_name.Engine())
                ->CreateRangeMap(
                    range_table_name, table_schema, schema_ts, this, ng_id));
        // update table schema for current range cc map
        if (!is_create && !ccm_it.second)
        {
            CcMap *ccm = ccm_it.first->second.get();
            ccm->SetTableSchema(table_schema);
            ccm->SetSchemaTs(schema_ts);
        }
    }
}

bool CcShard::EnableMvcc() const
{
    return local_shards_.EnableMvcc();
}

void CcShard::AddActiveSiTx(TxNumber txn, uint64_t start_ts)
{
    active_si_txs_.try_emplace(txn, start_ts);

    if (active_si_txs_.size() == 1)
    {
        min_si_tx_start_ts_.store(start_ts);
        last_scan_txs_ts_ = Now();
        return;
    }

    UpdateLocalMinSiTxStartTs();
}

void CcShard::RemoveActiveSiTx(TxNumber txn)
{
    active_si_txs_.erase(txn);

    UpdateLocalMinSiTxStartTs();
}

void CcShard::ClearActvieSiTxs()
{
    active_si_txs_.clear();
    min_si_tx_start_ts_.store(Now());
}

void CcShard::UpsertActiveBlockingTx(TxNumber txn, uint64_t timestamp)
{
    active_blocking_txs_[txn] = timestamp;
}

bool CcShard::RemoveActiveBlockingTx(TxNumber txn)
{
    auto it = active_blocking_txs_.find(txn);
    if (it != active_blocking_txs_.end())
    {
        active_blocking_txs_.erase(it);
        return true;
    }
    return false;
}

void CcShard::ClearActiveBlockingTxs()
{
    active_blocking_txs_.clear();
}

size_t CcShard::ActiveBlockingTxSize() const
{
    return active_blocking_txs_.size();
}

void CcShard::RemoveExpiredActiveBlockingTxs()
{
    // 5s, tune as needed
    static constexpr uint64_t kCleanupPeriodUs = 5000000;
    static thread_local uint64_t last_run_ts = 0;

    uint64_t now_ts = Now();
    if (now_ts - last_run_ts < kCleanupPeriodUs)
    {
        return;
    }

    last_run_ts = now_ts;

    for (auto it = active_blocking_txs_.begin();
         it != active_blocking_txs_.end();)
    {
        if (now_ts - it->second > kCleanupPeriodUs)
        {
            active_blocking_txs_.erase(it++);
        }
        else
        {
            ++it;
        }
    }
}

void CcShard::UpdateLocalMinSiTxStartTs()
{
    uint64_t now_ts = Now();
    if (active_si_txs_.size() == 0)
    {
        min_si_tx_start_ts_.store(now_ts);
        last_scan_txs_ts_ = now_ts;
        return;
    }

    // Scan "active_si_txs_" to update "min_si_tx_start_ts_".
    if (now_ts - last_scan_txs_ts_ < 5000000)
    {
        return;
    }

    uint64_t min_ts = UINT64_MAX;
    for (auto it = active_si_txs_.begin(); it != active_si_txs_.end(); it++)
    {
        uint64_t start_ts = it->second;
        if (start_ts < min_ts)
        {
            min_ts = start_ts;
        }
    }

    min_si_tx_start_ts_.store(min_ts);
    last_scan_txs_ts_ = now_ts;
}

uint64_t CcShard::LocalMinSiTxStartTs()
{
    if (active_si_txs_.size() > 0)
    {
        return min_si_tx_start_ts_.load();
    }
    else
    {
        return Now();
    }
}

uint64_t CcShard::GlobalMinSiTxStartTs() const
{
    return TxStartTsCollector::Instance().GlobalMinSiTxStartTs();
}

void CcShard::DecreaseLockCount()
{
    used_lock_count_--;
}

void CcShard::TryResizeLockArray()
{
    // shrink the lock vector when it becomes sparse.
    if (lock_vec_.size() > LOCK_ARRAY_INIT_SIZE &&
        used_lock_count_ < (lock_vec_.size() >> LOCK_VECTOR_SHRINK_THRESHOLD))
    {
        // shrink lock array size if and only if the lock used count is low
        // for a period of time.
        if (lock_sparse_num_ < RESIZE_LOCK_LIMIT)
        {
            // mark lock array is sparse, but not to shrink the array now.
            lock_sparse_num_++;
            return;
        }
        lock_sparse_num_ = 0;

        uint32_t high_idx = 0;
        uint32_t low_idx = 0;

        // shrink the capacity of the lock vector.
        uint32_t old_size = (uint32_t) lock_vec_.size();
        uint32_t new_size_expect =
            (uint32_t) (old_size >> (LOCK_VECTOR_SHRINK_THRESHOLD - 1u));

        // recycle the recylable slot and keep the unrecylable
        for (high_idx = old_size - 1;
             high_idx >= new_size_expect && high_idx > low_idx;
             high_idx--)
        {
            if (!lock_vec_[high_idx]->SafeToRecycle())
            {
                // find an unused slot
                while (low_idx < high_idx &&
                       !lock_vec_[low_idx]->SafeToRecycle())
                {
                    low_idx++;
                }
                if (low_idx == high_idx)
                {
                    break;
                }

                lock_vec_[low_idx++] = std::move(lock_vec_[high_idx]);
            }
        }

        lock_vec_.resize(high_idx + 1);
        lock_vec_.shrink_to_fit();
        next_lock_idx_ =
            next_lock_idx_ >= lock_vec_.size() ? 0 : next_lock_idx_;

        DLOG(INFO) << "the size of lock array decreased from: " << old_size
                   << " to: " << lock_vec_.size();
    }
}

uint64_t CcShard::Now() const
{
    return local_shards_.TsBase();
}

uint64_t CcShard::NowInMilliseconds() const
{
    return Now() / 1000;
}

void CcShard::UpdateTsBase(uint64_t ts)
{
    local_shards_.UpdateTsBase(ts);
}

void CcShard::CollectLockWaitingInfo(CheckDeadLockResult &dlr)
{
    std::unordered_map<uint64_t, CheckDeadLockResult::EntryLockInfo>
        &entry_lock_info_map = dlr.entry_lock_info_vec_[core_id_];

    for (auto it_ng = lock_holding_txs_.begin();
         it_ng != lock_holding_txs_.end();
         it_ng++)
    {
        for (auto it_tx = it_ng->second.begin(); it_tx != it_ng->second.end();
             it_tx++)
        {
            dlr.txid_ety_lock_count_[core_id_].insert(
                {it_tx->first, it_tx->second->cce_list_.size()});
            for (auto it_cce = it_tx->second->cce_list_.begin();
                 it_cce != it_tx->second->cce_list_.end();
                 it_cce++)
            {
                NonBlockingLock *key_lock = (*it_cce)->GetKeyLock();
                if (key_lock == nullptr)
                {
                    continue;
                }

                std::vector<uint64_t> vct =
                    key_lock->GetBlockTxIds(it_tx->first);
                if (vct.size() == 0)
                {
                    continue;
                }

                auto itet =
                    entry_lock_info_map.try_emplace((uint64_t) (*it_cce));
                itet.first->second.lock_txids.insert(it_tx->first);

                for (uint64_t id : vct)
                {
                    itet.first->second.wait_txids.insert(id);
                }
            }
        }
    }
}

CcShardHeap::CcShardHeap(CcShard *cc_shard, size_t limit)
    : cc_shard_(cc_shard), memory_limit_(limit)
{
    heap_ = mi_heap_new();

    if (cc_shard_->EnableDefragment())
    {
        // Init defrag heap cc
        defrag_heap_cc_ = std::make_unique<DefragShardHeapCc>(this, 16);
    }
}

CcShardHeap::~CcShardHeap()
{
    // destroy heap_ when shutdown cause core
    // remove it since heap will alway be freed when shutdown
    // if (heap_)
    //{
    // mi_heap_destroy(heap_);
    //}
}

mi_heap_t *CcShardHeap::SetAsDefaultHeap()
{
    return mi_heap_set_default(heap_);
}

bool CcShardHeap::Full(int64_t *alloc, int64_t *commit) const
{
    int64_t allocated, committed;
    mi_thread_stats(&allocated, &committed);
    if (alloc != nullptr)
    {
        *alloc = allocated;
    }

    if (commit != nullptr)
    {
        *commit = committed;
    }

    return allocated >= static_cast<int64_t>(memory_limit_) ||
           (cc_shard_->EnableDefragment() &&
            committed > static_cast<int64_t>(memory_limit_));
}

bool CcShardHeap::NeedCleanShard(int64_t &alloc, int64_t &commit) const
{
    return alloc >= static_cast<int64_t>(memory_limit_) ||
           (cc_shard_->EnableDefragment() &&
            alloc > (memory_limit_ * CcShardHeap::high_water));
}

bool CcShardHeap::NeedDefragment(int64_t *alloc, int64_t *commit) const
{
    int64_t allocated, committed;
    mi_thread_stats(&allocated, &committed);
    if (alloc != nullptr)
    {
        *alloc = allocated;
    }

    if (commit != nullptr)
    {
        *commit = committed;
    }

    return committed > (memory_limit_ * CcShardHeap::high_water) &&
           (static_cast<double>(allocated) / static_cast<double>(committed)) <=
               CcShardHeap::utilization;
}

bool CcShardHeap::AsyncDefragment()
{
    if (!defrag_heap_cc_on_fly_)
    {
        defrag_heap_cc_on_fly_ = true;
        std::vector<std::pair<uint32_t, int64_t>> node_groups_with_term;

        int64_t standby_node_term = Sharder::Instance().StandbyNodeTerm();
        if (standby_node_term < 0)
        {
            std::vector<uint32_t> node_groups =
                Sharder::Instance().LocalNodeGroups();
            for (auto node_group : node_groups)
            {
                int64_t leader_term =
                    Sharder::Instance().LeaderTerm(node_group);
                if (leader_term < 0)
                {
                    continue;
                }
                node_groups_with_term.emplace_back(node_group, leader_term);
            }
        }
        else
        {
            node_groups_with_term.emplace_back(
                Sharder::Instance().NativeNodeGroup(), standby_node_term);
        }

        defrag_heap_cc_->Reset(std::move(node_groups_with_term));
        cc_shard_->Enqueue(defrag_heap_cc_.get());

        LOG(INFO) << "Start defragmentation on shard#" << cc_shard_->core_id_
                  << ", node groups size: "
                  << defrag_heap_cc_->node_groups_.size();
        return true;
    }
    return false;
}

std::unordered_map<TableName, bool> CcShard::GetCatalogTableNameSnapshot(
    NodeGroupId cc_ng_id)
{
    uint64_t ckpt_ts = Now();
    return local_shards_.GetCatalogTableNameSnapshot(cc_ng_id, ckpt_ts);
}

StandbyForwardEntry *CcShard::GetNextStandbyForwardEntry()
{
    size_t cnt = 0;
    bool found = false;
    while (cnt < standby_fwd_vec_.size())
    {
        StandbyForwardEntry *ety = standby_fwd_vec_[next_foward_idx_].get();
        found =
            (ety->IsFree() && (ety->SequenceId() == UINT64_MAX ||
                               next_forward_sequence_id_ - ety->SequenceId() >=
                                   txservice_max_standby_lag));

        if (found)
        {
            break;
        }

        // Only overwrite standby entry if it is already older than the
        // oldest buffered msg.
        ++next_foward_idx_;
        if (next_foward_idx_ >= standby_fwd_vec_.size())
        {
            next_foward_idx_ = 0;
        }

        ++cnt;
    }

    if (!found)
    {
        uint32_t old_size = (uint32_t) standby_fwd_vec_.size();
        // Increases the capacity of the forward vector.
        uint32_t new_size = (uint32_t) (standby_fwd_vec_.size() * 1.5);
        LOG(INFO) << "Resize standby forward entry container, size:"
                  << standby_fwd_vec_.size() << ",new_size:" << new_size;
        standby_fwd_vec_.resize(new_size);
        for (uint32_t idx = old_size; idx < new_size; idx++)
        {
            standby_fwd_vec_[idx] = std::make_unique<StandbyForwardEntry>();
        }

        // position old_size must be an available slot.
        next_foward_idx_ = old_size;
    }

    StandbyForwardEntry *entry = standby_fwd_vec_.at(next_foward_idx_).get();
    entry->Reset();
    ++next_foward_idx_;
    next_foward_idx_ =
        next_foward_idx_ == standby_fwd_vec_.size() ? 0 : next_foward_idx_;

    return entry;
}

void CcShard::ForwardStandbyMessage(StandbyForwardEntry *entry)
{
    assert(entry != nullptr);
    uint64_t seq_id = next_forward_sequence_id_++;
    size_t buf_idx = seq_id % txservice_max_standby_lag;
    entry->SetSequenceId(seq_id);
    standby_fwded_msg_buffer_[buf_idx] = entry;
    auto &req = entry->Request();
    req.set_forward_seq_id(seq_id);
    req.set_forward_seq_grp(core_id_);
    if (!stream_sender_)
    {
        stream_sender_ = Sharder::Instance().GetCcStreamSender();
    }

    for (auto &[node_id, last_sent_seq_id_and_term] : subscribed_standby_nodes_)
    {
        bool write_succ = false;
        uint64_t &last_sent_seq_id = last_sent_seq_id_and_term.first;

        CODE_FAULT_INJECTOR("discard_forward_standby_message", {
            LOG(INFO) << "FaultInject  "
                         "discard_forward_standby_message, seq_id:"
                      << seq_id;
            continue;
        });

        if (last_sent_seq_id == seq_id - 1)
        {
            CODE_FAULT_INJECTOR("forward_standby_message_eagain", {
                if (seq_id % 2 == 0)
                {
                    LOG(INFO) << "FaultInject  "
                                 "forward_standby_message_eagain, seq_id:"
                              << seq_id;
                    if (!retry_fwd_msg_cc_->InUse())
                    {
                        retry_fwd_msg_cc_->Use();
                        Enqueue(retry_fwd_msg_cc_.get());
                    }
                    continue;
                }
            });

            write_succ = stream_sender_->SendMessageToNode(
                node_id, entry->Message(), nullptr, false, false);
            if (write_succ)
            {
                last_sent_seq_id++;
            }
        }
        if (!write_succ && !retry_fwd_msg_cc_->InUse())
        {
            // If the latest message is not successfully written to standby,
            // enqueue retry cc req that will keep retrying to send message from
            // last suceeded seq id.
            retry_fwd_msg_cc_->Use();
            Enqueue(retry_fwd_msg_cc_.get());
        }
    }

    entry->Free();
}

bool CcShard::ResendFailedForwardMessages()
{
    bool all_msgs_sent = true;
    for (auto &[node_id, seq_id_and_term] : subscribed_standby_nodes_)
    {
        uint64_t &seq_id = seq_id_and_term.first;
        if (seq_id == next_forward_sequence_id_ - 1 || seq_id == UINT64_MAX)
        {
            // No failed message
            continue;
        }

        size_t sent_msg = 0;
        // Retry sending buffered messages. Yield if there're too many msgs and
        // continue next round.
        while (seq_id < next_forward_sequence_id_ - 1 && sent_msg < 500)
        {
            // seq id is the last sent msg, start sending from seq id + 1
            size_t pending_buf_idx = (seq_id + 1) % txservice_max_standby_lag;

            if (standby_fwded_msg_buffer_[pending_buf_idx] &&
                standby_fwded_msg_buffer_[pending_buf_idx]->IsFree() &&
                standby_fwded_msg_buffer_[pending_buf_idx]->SequenceId() ==
                    seq_id + 1)
            {
                bool succ = stream_sender_->SendMessageToNode(
                    node_id,
                    standby_fwded_msg_buffer_[pending_buf_idx]->Message(),
                    nullptr,
                    false,
                    false);
                if (!succ)
                {
                    all_msgs_sent = false;
                    break;
                }
                else
                {
                    seq_id++;
                    sent_msg++;
                }
            }
            else
            {
                // this message is already lost and this standby needs to
                // resubscribe to the primary node again. Notify standby that
                // it has alraedy fall behind. Use the latest seq id so that
                // standby knows which epoch this out of sync msg belongs to.
                assert(next_forward_sequence_id_ - seq_id >=
                       txservice_max_standby_lag);
                remote::CcMessage cc_msg;
                cc_msg.set_type(
                    remote::CcMessage_MessageType::
                        CcMessage_MessageType_KeyObjectStandbyForwardRequest);
                auto req = cc_msg.mutable_key_obj_standby_forward_req();
                req->set_forward_seq_grp(core_id_);
                req->set_forward_seq_id(next_forward_sequence_id_ - 1);
                req->set_primary_leader_term(Sharder::Instance().LeaderTerm(
                    Sharder::Instance().NativeNodeGroup()));
                req->set_out_of_sync(true);
                stream_sender_->SendMessageToNode(node_id, cc_msg);

                seq_id = UINT64_MAX;
                // Remove heartbeat target node
                local_shards_.RemoveHeartbeatTargetNode(node_id,
                                                        seq_id_and_term.second);

                for (size_t core_idx = 0; core_idx < core_cnt_; ++core_idx)
                {
                    if (core_idx != core_id_)
                    {
                        DispatchTask(
                            core_idx,
                            [node_id = node_id,
                             unsubscribe_standby_term =
                                 seq_id_and_term.second](CcShard &ccs) -> bool
                            {
                                auto subscribe_node_iter =
                                    ccs.subscribed_standby_nodes_.find(node_id);
                                if (subscribe_node_iter !=
                                    ccs.subscribed_standby_nodes_.end())
                                {
                                    if (subscribe_node_iter->second.second <=
                                        unsubscribe_standby_term)
                                    {
                                        // erase ?
                                        subscribe_node_iter->second.first =
                                            UINT64_MAX;
                                    }
                                }

                                return true;
                            });
                    }
                }
                break;
            }
        }
        if (seq_id < next_forward_sequence_id_ - 1)
        {
            all_msgs_sent = false;
        }
    }

    return all_msgs_sent;
}

void CcShard::CollectStandbyMetrics()
{
    assert(metrics::enable_standby_metrics);
    for (auto &[node_id, seq_id_and_term] : subscribed_standby_nodes_)
    {
        uint64_t unsent_msgs_count = 0;
        uint64_t &seq_id = seq_id_and_term.first;
        if (seq_id != UINT64_MAX)
        {
            unsent_msgs_count = (next_forward_sequence_id_ - 1 - seq_id);
        }

        meter_->Collect(metrics::NAME_STANDBY_LAGGING_MESGS,
                        unsent_msgs_count,
                        std::to_string(node_id));
    }
}

void CcShard::RemoveSubscribedStandby(uint32_t node_id)
{
    auto seq_id_and_term = subscribed_standby_nodes_.find(node_id);
    if (seq_id_and_term != subscribed_standby_nodes_.end())
    {
        local_shards_.RemoveHeartbeatTargetNode(node_id,
                                                seq_id_and_term->second.second);
        subscribed_standby_nodes_.erase(node_id);
    }
}

bool CcShard::UpdateLastReceivedStandbySequenceId(
    const remote::KeyObjectStandbyForwardRequest &msg)
{
    uint16_t sequence_grp_id = msg.forward_seq_grp();
    uint64_t seq_id = msg.forward_seq_id();
    assert(seq_id != UINT64_MAX);

    auto &seq_grp_info = standby_sequence_grps_.at(sequence_grp_id);
    if (!seq_grp_info.subscribed_ &&
        seq_id >= seq_grp_info.last_consistent_standby_sequence_id_)
    {
        return false;
    }

    if (seq_id < seq_grp_info.initial_sequnce_id_)
    {
        // message belongs to an older subscribe epoch
        return false;
    }

    if (msg.out_of_sync())
    {
        // standby has fallen behind too much. Resubscribe to primary node.
        LOG(WARNING) << "Sequence group " << sequence_grp_id
                     << " has fallen behind primary too much. Trying to "
                        "resubscribe.";
        int64_t cur_prim_term = Sharder::Instance().PrimaryNodeTerm();
        if (cur_prim_term > 0)
        {
            NodeGroupId native_ng = Sharder::Instance().NativeNodeGroup();
            Sharder::Instance().OnStartFollowing(
                native_ng,
                cur_prim_term,
                Sharder::Instance().LeaderNodeId(native_ng),
                true);
        }

        // remove this seq grp from subscribed seq grps
        seq_grp_info.Unsubscribe();

        if (metrics::enable_metrics)
        {
            meter_->Collect(metrics::NAME_STANDBY_OUT_OF_SYNC_COUNT, 1);
        }
        return false;
    }

    seq_grp_info.last_consistent_standby_sequence_id_ =
        std::max(seq_id, seq_grp_info.last_consistent_standby_sequence_id_);

    // Update last consistent ts
    while (!seq_grp_info.pending_standby_consistent_ts_.empty() &&
           seq_grp_info.pending_standby_consistent_ts_.front().first <=
               seq_grp_info.last_consistent_standby_sequence_id_)
    {
        DLOG(INFO)
            << "Update last consistent ts for seq grp " << sequence_grp_id
            << ", old ts " << seq_grp_info.last_standby_consistent_ts_
            << ", new ts "
            << seq_grp_info.pending_standby_consistent_ts_.front().second;
        seq_grp_info.last_standby_consistent_ts_ = std::max(
            seq_grp_info.last_standby_consistent_ts_,
            seq_grp_info.pending_standby_consistent_ts_.front().second);
        seq_grp_info.pending_standby_consistent_ts_.pop();
    }

    return true;
}

void CcShard::ResetStandbySequence()
{
    next_forward_sequence_id_ = 1;
    for (auto &entry : standby_fwd_vec_)
    {
        entry->Free();
        entry->SetSequenceId(UINT64_MAX);
    }
    subscribed_standby_nodes_.clear();

    for (auto &[grp_id, grp_info] : standby_sequence_grps_)
    {
        grp_info.Unsubscribe();
    }

    standby_sequence_grps_.clear();
}

uint64_t CcShard::MinLastStandbyConsistentTs() const
{
    uint64_t min_ts = UINT64_MAX;
    for (auto &seq_grp : standby_sequence_grps_)
    {
        if (seq_grp.second.subscribed_)
        {
            min_ts =
                std::min(seq_grp.second.last_standby_consistent_ts_, min_ts);
        }
    }
    return min_ts;
}

void CcShard::UpdateStandbyConsistentTs(uint32_t seq_grp,
                                        uint64_t seq_id,
                                        uint64_t consistent_ts,
                                        int64_t standby_node_term)

{
    auto &seq_grp_info = standby_sequence_grps_.at(seq_grp);
    if (!seq_grp_info.subscribed_)
    {
        return;
    }

    // data before the follower subscribed to primary is always
    // consistent.
    if (seq_grp_info.last_consistent_standby_sequence_id_ >= seq_id)
    {
        // Update ts if seq id is already consistent
        DLOG(INFO) << "Update last consistent ts for seq grp " << seq_grp
                   << ", old ts " << seq_grp_info.last_standby_consistent_ts_
                   << ", new ts " << consistent_ts;
        seq_grp_info.last_standby_consistent_ts_ =
            std::max(seq_grp_info.last_standby_consistent_ts_, consistent_ts);
    }
    else
    {
        DLOG(INFO) << "Have not received the consistent sequence id " << seq_id
                   << " for seq grp " << seq_grp << ", current seq id "
                   << seq_grp_info.last_consistent_standby_sequence_id_;
        seq_grp_info.pending_standby_consistent_ts_.emplace(seq_id,
                                                            consistent_ts);
    }
}

void CcShard::DecrInflightStandbyReqCount(uint32_t seq_grp)
{
    auto iter = standby_sequence_grps_.find(seq_grp);
    if (iter == standby_sequence_grps_.end() ||
        iter->second.subscribed_ == false)
    {
        assert(iter == standby_sequence_grps_.end() ||
               iter->second.finished_stanbdy_req_count_ == 0);
        // update global counter immediately.
        Sharder::Instance().DecrInflightStandbyReqCount(1);
        return;
    }
    else
    {
        // update local counter to reduce modify global atomic variable
        auto &seq_grp_info = iter->second;
        assert(seq_grp_info.subscribed_);
        seq_grp_info.finished_stanbdy_req_count_++;

        if (seq_grp_info.finished_stanbdy_req_count_ >= 10000)
        {
            Sharder::Instance().DecrInflightStandbyReqCount(
                seq_grp_info.finished_stanbdy_req_count_);
            seq_grp_info.finished_stanbdy_req_count_ = 0;
        }
    }
}

void CcShard::SubsribeToPrimaryNode(uint32_t seq_grp, uint64_t seq_id)
{
    auto ins_pair = standby_sequence_grps_.try_emplace(seq_grp);
    ins_pair.first->second.Subscribe(seq_id);

    LOG(INFO) << "subscribed to seq grp " << seq_grp << ", next seq id "
              << seq_id;
}

void CcShard::EnqueueWaitListIfSchemaMismatch(CcRequestBase *req)
{
    waiting_list_for_schema_.push_back(req);
}

void CcShard::DequeueWaitListAfterSchemaUpdated()
{
    if (waiting_list_for_schema_.size() > 0)
    {
        for (auto req : waiting_list_for_schema_)
        {
            this->Enqueue(req);
        }

        waiting_list_for_schema_.clear();
    }
}

void CcShard::UpdateBufferedCommandCnt(int64_t delta)
{
    buffered_cmd_cnt_ += delta;
}

void CcShard::CheckLagAndResubscribe() const
{
    if (buffered_cmd_cnt_ >= (int64_t) txservice_max_standby_lag)
    {
        // Resubscribe to the leader.
        NodeGroupId native_ng = Sharder::Instance().NativeNodeGroup();
        int64_t cur_prim_term = Sharder::Instance().PrimaryNodeTerm();
        LOG(WARNING) << "Resubscribe to the leader at term " << cur_prim_term
                     << ", since buffered cmd cnt has exceeded the threshold: "
                     << buffered_cmd_cnt_;
        Sharder::Instance().OnStartFollowing(
            native_ng,
            cur_prim_term,
            Sharder::Instance().LeaderNodeId(native_ng),
            true);
    }
}

bool CcShard::EnableDefragment() const
{
    return local_shards_.enable_shard_heap_defragment_;
}

void CcShard::NotifyTxProcessor()
{
    if (tx_coordi_ == nullptr)
    {
        return;
    }
    TxProcessor *processor =
        tx_coordi_->tx_processor_.load(std::memory_order_relaxed);
    if (processor != nullptr)
    {
        processor->NotifyTxProcessor();
    }
}

std::shared_ptr<ReaderWriterObject<TableSchema>> CcShard::FindSchemaCntl(
    const TableName &tbl_name)
{
    auto it = catalog_rw_cntl_.find(tbl_name);
    return it == catalog_rw_cntl_.end() ? nullptr : it->second;
}

std::shared_ptr<ReaderWriterObject<TableSchema>> CcShard::FindEmplaceSchemaCntl(
    const TableName &tbl_name)
{
    auto [it, insert] = catalog_rw_cntl_.try_emplace(tbl_name);
    if (insert)
    {
        it->second = std::make_shared<ReaderWriterObject<TableSchema>>(this);
    }

    return it->second;
}

void CcShard::DeleteSchemaCntl(const TableName &tbl_name)
{
    catalog_rw_cntl_.erase(tbl_name);
}

void CcShard::ClearNativeSchemaCntl()
{
    // When the native cc shard fails over, invalidates all schema reader-writer
    // control blocks. Invalidation ensures that future runtime queries will not
    // use cached schema and falls back to reading the schema via concurrency
    // control. Ongoing runtime queries will continue to use the old schema.
    for (auto &[tbl_name, cntl] : catalog_rw_cntl_)
    {
        cntl->Invalidate();
    }
    catalog_rw_cntl_.clear();
}

TxLockInfo::uptr CcShard::GetTxLockInfo(int64_t tx_term)
{
    TxLockInfo::uptr first_lock_info = std::move(tx_lock_info_head_.next_);

    if (first_lock_info == nullptr)
    {
        first_lock_info = std::make_unique<TxLockInfo>(tx_term);
    }
    else
    {
        tx_lock_info_head_.next_ = std::move(first_lock_info->next_);
        first_lock_info->Reset(tx_term);
    }
    assert(first_lock_info->next_ == nullptr);
    return first_lock_info;
}

void CcShard::RecycleTxLockInfo(TxLockInfo::uptr lock_info)
{
    assert(lock_info->next_ == nullptr);
    lock_info->next_ = std::move(tx_lock_info_head_.next_);
    tx_lock_info_head_.next_ = std::move(lock_info);
}

void CcShard::CollectCacheHit()
{
    assert(metrics::enable_cache_hit_rate);
    meter_->Collect(metrics::NAME_CACHE_HIT_OR_MISS_TOTAL, 1, "hits");
}

void CcShard::CollectCacheMiss()
{
    assert(metrics::enable_cache_hit_rate);
    meter_->Collect(metrics::NAME_CACHE_HIT_OR_MISS_TOTAL, 1, "miss");
}

}  // namespace txservice