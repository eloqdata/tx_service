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
#include "cc/local_cc_shards.h"

#include <bthread/mutex.h>
#include <butil/time.h>
#include <malloc.h>
#include <sys/stat.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <shared_mutex>
#include <thread>
#include <tuple>
#include <unordered_map>

#include "catalog_key_record.h"
#include "cc_handler_result.h"
#include "cc_node_service.h"
#include "cc_req_misc.h"
#include "cc_request.h"
#include "cc_request.pb.h"
#include "error_messages.h"
#include "log_type.h"
#include "range_bucket_key_record.h"
#include "range_record.h"
#include "range_slice.h"
#include "sequences/sequences.h"
#include "sharder.h"
#include "sk_generator.h"
#include "standby.h"
#include "store/data_store_handler.h"
#include "tx_execution.h"
#include "tx_key.h"
#include "tx_service.h"
#include "tx_service_common.h"
#include "tx_util.h"
#include "type.h"

namespace txservice
{
DECLARE_bool(report_ckpt);
DEFINE_double(ckpt_buffer_ratio,
              0.125,
              "Fraction of node memory carved out for checkpoint flush "
              "buffers and the data-sync memory controller");
DEFINE_uint32(hash_partition_ckpt_scan_yield_time_us,
              100,
              "Max microseconds a hash partition checkpoint/data-sync scan may "
              "run before yielding a Tx processor thread");
DEFINE_uint64(hash_partition_data_sync_scan_batch_size,
              100,
              "Upper bound on the number of CC pages exported in a single "
              "hash partition data-sync scan batch");
DEFINE_uint64(hash_partition_data_sync_scan_data_size,
              1000 * 1024,
              "Per-batch memory budget (bytes) for data exported during a "
              "hash partition data-sync scan");

std::atomic<uint64_t> LocalCcShards::local_clock(0);
inline thread_local size_t tls_shard_idx = std::numeric_limits<size_t>::max();

LocalCcShards::LocalCcShards(
    uint32_t node_id,
    uint32_t ng_id,
    const std::map<std::string, uint32_t> &conf,
    CatalogFactory *catalog_factory[3],
    SystemHandler *system_handler,
    std::unordered_map<uint32_t, std::vector<NodeConfig>> *ng_configs,
    uint64_t cluster_config_version,
    store::DataStoreHandler *store_hd,
    TxService *tx_service,
    bool enable_mvcc,
    metrics::MetricsRegistry *metrics_registry,
    metrics::CommonLabels common_labels,
    std::unordered_map<TableName, std::string> *prebuilt_tables,
    std::function<void(std::string_view, std::string_view)> publish_func)
    : range_slice_memory_limit_(
          static_cast<uint64_t>(MB(conf.at("node_memory_limit_mb"))) *
          conf.at("range_slice_memory_limit_percent") / 100),
      store_hd_(store_hd),
      node_id_(node_id),
      ng_id_(ng_id),
      timer_terminate_(false),
      is_waiting_ckpt_(false),
      catalog_factory_{catalog_factory[0],
                       catalog_factory[1],
                       catalog_factory[2],
                       &range_catalog_factory_,
                       &hash_catalog_factory_},
      system_handler_(system_handler),
      tx_service_(tx_service),
      enable_mvcc_(enable_mvcc),
      realtime_sampling_(conf.at("realtime_sampling")),

#ifdef EXT_TX_PROC_ENABLED
      range_split_worker_ctx_(
          conf.at("range_split_worker_num") > 0
              ? conf.at("range_split_worker_num")
              : (conf.at("core_num") >= 2 ? (conf.at("core_num") / 2) : 1)),
#else
      range_split_worker_ctx_(conf.at("range_split_worker_num") > 0
                                  ? conf.at("range_split_worker_num")
                                  : conf.at("core_num")),
#endif

      data_sync_worker_ctx_(conf.at("core_num")),
#ifdef EXT_TX_PROC_ENABLED
      flush_data_worker_ctx_(
          conf.at("core_num") >= 2
              ? std::min(conf.at("core_num") / 2, (uint32_t) 10)
              : 1),
#else
      flush_data_worker_ctx_(
          std::min(static_cast<int>(conf.at("core_num")), 10)),
#endif

      cur_flush_buffer_(
          static_cast<uint64_t>(MB(conf.at("node_memory_limit_mb")) *
                                (FLAGS_ckpt_buffer_ratio - 0.025))),
      data_sync_mem_controller_(static_cast<uint64_t>(
          MB(conf.at("node_memory_limit_mb")) * FLAGS_ckpt_buffer_ratio)),
      statistics_worker_ctx_(1),
      heartbeat_worker_ctx_(1),
      purge_deleted_worker_ctx_(1),
      kickout_data_test_worker_ctx_(conf.count("kickout_data_for_test") > 0
                                        ? conf.at("kickout_data_for_test")
                                        : 0),
#if defined(WITH_JEMALLOC)
      epoch_worker_ctx_(1),
#endif
      publish_func_(publish_func),
      enable_shard_heap_defragment_(conf.at("enable_shard_heap_defragment")),
      fast_meta_data_mux_(conf.at("core_num"), meta_data_mux_)

{
    if (conf.find("max_standby_lag") != conf.end())
    {
        txservice_max_standby_lag = conf.at("max_standby_lag");
    }
    if (conf.find("enable_key_cache") != conf.end())
    {
        if (enable_mvcc && conf.at("enable_key_cache"))
        {
            LOG(WARNING) << "Txservice key cache is disabled due to "
                            "incompatibility with MVCC.";
        }
        // Key cache is only available in non-mvcc mode.
        txservice_enable_key_cache =
            conf.at("enable_key_cache") && !enable_mvcc;
    }
    using namespace std::chrono_literals;
    uint64_t ts_base = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    ts_base_.store(ts_base);
    local_clock.store(ts_base);
    timer_thd_ = std::thread([this] { TimerRun(); });
    pthread_setname_np(timer_thd_.native_handle(), "tx_timer");

    // For mariadb, this thread is the main thread of the mariadb process.
    InitializeTableRangesHeap();

    InitializeHashPartitionCkptHeap();

    std::set<NodeGroupId> ng_ids;
    for (auto &[ng_id, _] : *ng_configs)
    {
        ng_ids.emplace(ng_id);
    }

    uint64_t node_memory_limit_mb = conf.at("node_memory_limit_mb");
    uint32_t range_slice_memory_limit_percent =
        conf.at("range_slice_memory_limit_percent");
    uint16_t core_cnt = conf.at("core_num");
    for (uint16_t thd_idx = 0; thd_idx < core_cnt; ++thd_idx)
    {
        common_labels["core_id"] = std::to_string(thd_idx);
        cc_shards_.emplace_back(
            std::make_unique<CcShard>(thd_idx,
                                      core_cnt,
                                      node_memory_limit_mb,
                                      conf.at("realtime_sampling"),
                                      ng_id_,
                                      *this,
                                      catalog_factory_,
                                      system_handler,
                                      ng_configs,
                                      cluster_config_version,
                                      metrics_registry,
                                      common_labels,
                                      range_slice_memory_limit_percent));
    }
    for (size_t i = 0; i < cc_shards_.size(); ++i)
    {
        fast_meta_data_mux_.mux_ptrs_[i] = &cc_shards_[i]->meta_data_mux_;
    }

    InitRangeBuckets(ng_id_, ng_ids, cluster_config_version);

    if (prebuilt_tables)
    {
        for (auto &[table, image] : *prebuilt_tables)
        {
            auto ins_res = prebuilt_tables_.try_emplace(table, image);
            assert(ins_res.second);
            (void) ins_res;
        }
    }

    if (metrics::enable_metrics)
    {
        node_meter_ =
            std::make_unique<metrics::Meter>(metrics_registry, common_labels);

        node_meter_->Register(metrics::NAME_IS_CONTINUOUS_CHECKPOINT_FAILURES,
                              metrics::Type::Gauge);

        std::vector<metrics::LabelGroup> leader_changes_metric_labels;
        std::vector<std::string> ng_id_label_values;
        for (const auto &[ng_id, members] : *ng_configs)
        {
            for (const auto &member : members)
            {
                if (member.node_id_ == node_id_)
                {
                    ng_id_label_values.push_back(std::to_string(ng_id));
                    break;
                }
            }
        }

        leader_changes_metric_labels.emplace_back(
            "ng_id", std::move(ng_id_label_values));
        auto is_leader_metric_labels = leader_changes_metric_labels;
        node_meter_->Register(metrics::NAME_LEADER_CHANGES,
                              metrics::Type::Counter,
                              std::move(leader_changes_metric_labels));
        node_meter_->Register(metrics::NAME_IS_LEADER,
                              metrics::Type::Gauge,
                              std::move(is_leader_metric_labels));
    }
}

LocalCcShards::~LocalCcShards()
{
    {
        std::scoped_lock<std::mutex> lk(timer_terminate_mux_);
        timer_terminate_ = true;
        timer_terminate_cv_.notify_one();
    }
    timer_thd_.join();
    cc_shards_.clear();
}

void LocalCcShards::StartBackgroudWorkers()
{
    // Starts flush worker threads firstly.
    for (int id = 0; id < flush_data_worker_ctx_.worker_num_; id++)
    {
        std::thread &thd = flush_data_worker_ctx_.worker_thd_.emplace_back(
            [this] { FlushDataWorker(); });
        std::string thread_name = "flush_data_" + std::to_string(id);
        pthread_setname_np(thd.native_handle(), thread_name.c_str());
    }

    LOG(INFO) << "Range Split Worker Num: "
              << range_split_worker_ctx_.worker_num_;
    for (int id = 0; id < range_split_worker_ctx_.worker_num_; id++)
    {
        std::thread &thd = range_split_worker_ctx_.worker_thd_.emplace_back(
            [this] { RangeSplitWorker(); });
        std::string thread_name = "range_split_" + std::to_string(id);
        pthread_setname_np(thd.native_handle(), thread_name.c_str());
    }

    data_sync_task_queue_.resize(data_sync_worker_ctx_.worker_num_);

    for (int id = 0; id < data_sync_worker_ctx_.worker_num_; id++)
    {
        std::thread &thd = data_sync_worker_ctx_.worker_thd_.emplace_back(
            [this, id] { DataSyncWorker(id); });
        std::string thread_name = "data_sync_" + std::to_string(id);
        pthread_setname_np(thd.native_handle(), thread_name.c_str());
    }

#if defined(WITH_JEMALLOC)
    for (int id = 0; id < epoch_worker_ctx_.worker_num_; ++id)
    {
        epoch_worker_ctx_.worker_thd_.emplace_back([this] { EpochWorker(); });
    }
#endif

    if (realtime_sampling_)
    {
        std::thread &thd = statistics_worker_ctx_.worker_thd_.emplace_back(
            [this] { SyncTableStatisticsWorker(); });
        std::string thread_name = "stat_sync";
        pthread_setname_np(thd.native_handle(), thread_name.c_str());
    }

    {
        std::thread &thd = heartbeat_worker_ctx_.worker_thd_.emplace_back(
            [this] { HeartbeatWorker(); });
        std::string thread_name = "heartbeat";
        pthread_setname_np(thd.native_handle(), thread_name.c_str());
    }

    if (!txservice_enable_cache_replacement)
    {
        // In this mode we will not try to evict cce when memory is full. We
        // need to periodically clean up deleted cces to avoid memory overflow.
        std::thread &thd = purge_deleted_worker_ctx_.worker_thd_.emplace_back(
            [this] { PurgeDeletedData(); });
        std::string thread_name = "purge_deleted";
        pthread_setname_np(thd.native_handle(), thread_name.c_str());
    }

    if (kickout_data_test_worker_ctx_.worker_num_ > 0)
    {
        assert(kickout_data_test_worker_ctx_.worker_num_ == 1);
        std::thread &thd =
            kickout_data_test_worker_ctx_.worker_thd_.emplace_back(
                [this] { KickoutDataForTest(); });
        std::string thread_name = "kick_data_test";
        pthread_setname_np(thd.native_handle(), thread_name.c_str());
    }
}

uint64_t LocalCcShards::ClockTs()
{
    return LocalCcShards::local_clock.load(std::memory_order_relaxed);
}

uint64_t LocalCcShards::ClockTsInMillseconds()
{
    return ClockTs() / 1000;
}

uint64_t LocalCcShards::TsBase()
{
    return ts_base_.load(std::memory_order_relaxed);
}

uint64_t LocalCcShards::TsBaseInMillseconds()
{
    return TsBase() / 1000;
}

void LocalCcShards::UpdateTsBase(uint64_t timestamp)
{
    uint64_t tsb = ts_base_.load(std::memory_order_relaxed);
    // Update ts_base_ only if new timestamp is bigger. If the CAS fails, tsb
    // will be set to the actual value of ts_base_, keep retrying until success
    // or ts_base_ is already bigger than timestamp.
    while (timestamp > tsb && !ts_base_.compare_exchange_strong(tsb, timestamp))
    {
        // If the CAS fails, since timestamps always roll forward, the ts base
        // must be greater than the current time or the old ts base.
        // ts_base_ can also be updated when calculating commit timestamp, which
        // results in system clock could be smaller than ts_base_. In this case
        // we should not update ts_base_.
    }
}

void LocalCcShards::BindThreadToFastMetaDataShard(size_t shard_idx)
{
    if (BAIDU_UNLIKELY(tls_shard_idx == std::numeric_limits<size_t>::max()))
    {
        tls_shard_idx = shard_idx;
    }
}

void LocalCcShards::TimerRun()
{
    std::unique_lock<std::mutex> lk(timer_terminate_mux_);
    do
    {
        using namespace std::chrono_literals;

        uint64_t clock_ts =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        uint64_t old_clock_ts = LocalCcShards::local_clock.load();
        if (clock_ts > old_clock_ts)
        {
            LocalCcShards::local_clock.compare_exchange_strong(
                old_clock_ts, clock_ts, std::memory_order_relaxed);
        }
        UpdateTsBase(clock_ts);

        timer_terminate_cv_.wait_for(
            lk, 100ms, [this]() { return timer_terminate_ == true; });
    } while (!timer_terminate_);
}

std::pair<bool, const CatalogEntry *> LocalCcShards::CreateCatalog(
    const TableName &table_name,
    NodeGroupId cc_ng_id,
    const std::string &catalog_image,
    uint64_t commit_ts)
{
    assert(table_name.Type() == TableType::Primary);
    std::unique_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);

    auto ng_catalog_it = table_catalogs_.try_emplace(table_name);
    auto catalog_it = ng_catalog_it.first->second.try_emplace(cc_ng_id);
    CatalogEntry &catalog_entry = catalog_it.first->second;

    if (catalog_it.second)
    {
        // A new catalog entry is created in LocalCcShards.
        catalog_entry.InitSchema(
            catalog_image.empty()
                ? nullptr
                : GetCatalogFactory(table_name.Engine())
                      ->CreateTableSchema(table_name, catalog_image, commit_ts),
            commit_ts);
    }
    else
    {
        // If the input schema version is greater than the existing one,
        // replaces the existing scheme with the new one.
        if (catalog_entry.schema_version_ < commit_ts)
        {
            catalog_entry.InitSchema(
                catalog_image.empty()
                    ? nullptr
                    : GetCatalogFactory(table_name.Engine())
                          ->CreateTableSchema(
                              table_name, catalog_image, commit_ts),
                commit_ts);
        }
        else if (catalog_entry.schema_version_ == commit_ts)
        {
            // It is kv_store_failure and is restoring old schema, treat as
            // create successfully.
        }
        else
        {
            return {false, &catalog_entry};
        }
    }

    return {true, &catalog_entry};
}

TableSchema::uptr LocalCcShards::CreateTableSchemaFromImage(
    const TableName &table_name,
    const std::string &catalog_image,
    uint64_t version)
{
    if (catalog_image.empty())
    {
        return nullptr;
    }
    return GetCatalogFactory(table_name.Engine())
        ->CreateTableSchema(table_name, catalog_image, version);
}

std::pair<bool, const CatalogEntry *> LocalCcShards::CreateReplayCatalog(
    const TableName &table_name,
    NodeGroupId cc_ng_id,
    const std::string &old_catalog_image,
    const std::string &new_catalog_image,
    uint64_t old_schema_ts,
    uint64_t dirty_schema_ts)
{
    std::unique_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);

    auto ng_catalog_it = table_catalogs_.try_emplace(table_name);
    auto catalog_it = ng_catalog_it.first->second.try_emplace(cc_ng_id);
    CatalogEntry &catalog_entry = catalog_it.first->second;

    if (catalog_it.second || catalog_entry.schema_version_ == 0)
    {
        // If catalog entry is not initialized yet, use the old schema image
        // stored in prepare log to restore old schema.
        catalog_entry.InitSchema(
            old_catalog_image.empty()
                ? nullptr
                : GetCatalogFactory(table_name.Engine())
                      ->CreateTableSchema(
                          table_name, old_catalog_image, old_schema_ts),
            old_schema_ts);
    }

    if (catalog_entry.schema_version_ < dirty_schema_ts &&
        catalog_entry.dirty_schema_version_ < dirty_schema_ts)
    {
        // For idempotency, only installs the dirty version when the input ts is
        // greater than the existing version and dirty version.
        catalog_entry.SetDirtySchema(
            new_catalog_image.empty()
                ? nullptr
                : GetCatalogFactory(table_name.Engine())
                      ->CreateTableSchema(
                          table_name, new_catalog_image, dirty_schema_ts),
            dirty_schema_ts);
        return {true, &catalog_entry};
    }
    if (catalog_entry.schema_version_ == old_schema_ts &&
        catalog_entry.dirty_schema_version_ == dirty_schema_ts)
    {
        // Rerun ReplayLogCc req.
        return {true, &catalog_entry};
    }
    if (dirty_schema_ts == 0)
    {
        // It is kv_store_failure and is restoring old schema, treat as
        // create successfully.
        return {true, &catalog_entry};
    }
    return {false, &catalog_entry};
}

CatalogEntry *LocalCcShards::CreateDirtyCatalog(
    const TableName &table_name,
    NodeGroupId cc_ng_id,
    const std::string &catalog_image,
    uint64_t commit_ts)
{
    std::unique_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);

    auto ng_catalog_it = table_catalogs_.try_emplace(table_name);
    auto catalog_it = ng_catalog_it.first->second.try_emplace(cc_ng_id);
    CatalogEntry &catalog_entry = catalog_it.first->second;

    if (catalog_entry.schema_version_ < commit_ts &&
        catalog_entry.dirty_schema_version_ < commit_ts)
    {
        // For idempotency, only installs the dirty version when the input ts is
        // greater than the existing version and dirty version.
        catalog_entry.SetDirtySchema(
            catalog_image.empty()
                ? nullptr
                : GetCatalogFactory(table_name.Engine())
                      ->CreateTableSchema(table_name, catalog_image, commit_ts),
            commit_ts);
    }

    return &catalog_entry;
}

void LocalCcShards::UpdateDirtyCatalog(const TableName &table_name,
                                       const std::string &catalog_image,
                                       CatalogEntry *catalog_entry)
{
    assert(catalog_entry && !catalog_image.empty());
    std::unique_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);
    catalog_entry->SetDirtySchema(
        GetCatalogFactory(table_name.Engine())
            ->CreateTableSchema(table_name,
                                catalog_image,
                                catalog_entry->dirty_schema_version_),
        catalog_entry->dirty_schema_version_);
}

CatalogEntry *LocalCcShards::GetCatalog(const TableName &table_name,
                                        NodeGroupId cc_ng_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);

    auto ng_catalog_it = table_catalogs_.find(table_name);
    if (ng_catalog_it == table_catalogs_.end())
    {
        return nullptr;
    }

    auto catalog_it = ng_catalog_it->second.find(cc_ng_id);
    return catalog_it == ng_catalog_it->second.end() ? nullptr
                                                     : &catalog_it->second;
}

CatalogEntry *LocalCcShards::GetCatalogInternal(const TableName &table_name,
                                                NodeGroupId cc_ng_id)
{
    auto ng_catalog_it = table_catalogs_.find(table_name);
    if (ng_catalog_it == table_catalogs_.end())
    {
        return nullptr;
    }

    auto catalog_it = ng_catalog_it->second.find(cc_ng_id);
    return catalog_it == ng_catalog_it->second.end() ? nullptr
                                                     : &catalog_it->second;
}

std::unordered_map<TableName, bool> LocalCcShards::GetCatalogTableNameSnapshot(
    NodeGroupId cc_ng_id, uint64_t snapshot_ts)
{
    std::unordered_map<TableName, bool> tables;
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    for (const auto &[base_table_name, ng_catalog_map] : table_catalogs_)
    {
        auto catalog_it = ng_catalog_map.find(cc_ng_id);
        if (catalog_it != ng_catalog_map.end())
        {
            CatalogEntry &catalog_entry =
                const_cast<CatalogEntry &>(catalog_it->second);
            std::shared_lock<std::shared_mutex> lk(catalog_entry.s_mux_);
            if (catalog_entry.schema_ != nullptr)
            {
                auto ins_it = tables.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(base_table_name.StringView().data(),
                                          base_table_name.StringView().size(),
                                          base_table_name.Type(),
                                          base_table_name.Engine()),
                    std::forward_as_tuple(false));
                assert(ins_it.second);
                (void) ins_it;
                for (const txservice::TableName &index_table_name :
                     catalog_entry.schema_->IndexNames())
                {
                    auto ins_it =
                        tables.emplace(std::piecewise_construct,
                                       std::forward_as_tuple(
                                           index_table_name.StringView().data(),
                                           index_table_name.StringView().size(),
                                           index_table_name.Type(),
                                           index_table_name.Engine()),
                                       std::forward_as_tuple(false));
                    assert(ins_it.second);
                    // This silences the -Wunused-but-set-variable warning
                    // without any runtime overhead.
                    (void) ins_it;
                }

                // For alter table, should include new index tables.
                // For dirty index tables, if the dirty schema version is larger
                // than the `snapshot_ts`, it means there is no data in the
                // dirty index table before the `snapshot_ts`, then, there is no
                // need to do checkpoint or table stats sync.
                if (catalog_entry.dirty_schema_ != nullptr &&
                    catalog_entry.dirty_schema_version_ <= snapshot_ts)
                {
                    // Only search new index table name, because the base table
                    // and the old index have been obtained via above.
                    for (const txservice::TableName &index_table_name :
                         catalog_entry.dirty_schema_->IndexNames())
                    {
                        auto iter = tables.find(index_table_name);
                        if (iter == tables.end())
                        {
                            auto ins_it = tables.emplace(
                                std::piecewise_construct,
                                std::forward_as_tuple(
                                    index_table_name.StringView().data(),
                                    index_table_name.StringView().size(),
                                    index_table_name.Type(),
                                    index_table_name.Engine()),
                                std::forward_as_tuple(true));
                            assert(ins_it.second);
                            // This silences the -Wunused-but-set-variable
                            // warning without any runtime overhead.
                            (void) ins_it;
                        }
                    }
                } /* End of dirty index table schema */
            } /* End of this base table */
        }
    }
    return tables;
}

void LocalCcShards::CreateSchemaRecoveryTx(
    ReplayLogCc &replay_log_cc,
    const ::txlog::SchemaOpMessage &schema_op_msg,
    int64_t tx_term)
{
    auto schema_recover_thd = std::thread(
        [this,
         &replay_log_cc,
         schema_op_msg,  // cannot pass in reference here since msg
                         // will be freed once replay is done
         txn = replay_log_cc.Txn(),
         tx_term,
         commit_ts = replay_log_cc.CommitTs()]
        {
            TransactionExecution *txm = tx_service_->NewTx();
            ClusterConfigRecord rec;
            txm->SetRecoverTxState(txn, tx_term, commit_ts);
            ReadTxRequest read_req(&cluster_config_ccm_name,
                                   0,
                                   VoidKey::NegInfTxKey(),
                                   &rec,
                                   false,
                                   false,
                                   true,
                                   0,
                                   false,
                                   true);
            txm->Execute(&read_req);
            read_req.Wait();
            if (read_req.IsError())
            {
                // leadership transferred away before replay finish.
                replay_log_cc.AbortCcRequest(CcErrorCode::TX_NODE_NOT_LEADER);
                AbortTxRequest abort_req;
                txm->Execute(&abort_req);
                abort_req.Wait();
                return;
            }
            replay_log_cc.SetFinish();
            SchemaRecoveryTxRequest recover_req(schema_op_msg);
            txm->Execute(&recover_req);
            recover_req.Wait();

            if (recover_req.IsError() ||
                recover_req.Result() != UpsertResult::Succeeded)
            {
                txservice::AbortTx(txm);
            }
            else
            {
                txservice::CommitTx(txm);
            }
        });

    schema_recover_thd.detach();
}

void LocalCcShards::CreateSplitRangeRecoveryTx(
    ReplayLogCc &replay_log_cc,
    const ::txlog::SplitRangeOpMessage &ds_split_range_op_msg,
    int32_t partition_id,
    const RangeInfo *range_info,
    std::vector<TxKey> &&new_range_keys,
    std::vector<int32_t> &&new_partition_ids,
    uint32_t node_group_id,
    int64_t tx_term)
{
    auto split_recover_thd = std::thread(
        [this,
         &replay_log_cc,
         ds_split_range_op_msg,  // cannot pass in reference here since
                                 // msg will be freed once replay is
                                 // done
         partition_id,
         txn = replay_log_cc.Txn(),
         tx_term,
         commit_ts = replay_log_cc.CommitTs(),
         new_range_keys = std::move(new_range_keys),
         new_partition_ids = std::move(new_partition_ids),
         node_group_id,
         split_tx_started = replay_log_cc.RangeSplitStarted()]() mutable
        {
            if (Sharder::Instance().TryPinNodeGroupData(node_group_id) !=
                tx_term)
            {
                replay_log_cc.AbortCcRequest(
                    CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                return;
            }
            // guard to unpin node group on finish.
            std::shared_ptr<void> defer_unpin(
                nullptr,
                [node_group_id](void *)
                { Sharder::Instance().UnpinNodeGroupData(node_group_id); });
            // Mark the table as sync in progress to avoid concurrent data sync
            // before range split tx finishes if this is the first started range
            // split.
            TableEngine table_engine = ::txlog::ToLocalType::ConvertTableEngine(
                ds_split_range_op_msg.table_engine());
            TableName table_name =
                TableName(ds_split_range_op_msg.table_name(),
                          TableName::Type(ds_split_range_op_msg.table_name()),
                          table_engine);
            const TableName range_table_name =
                TableName{ds_split_range_op_msg.table_name(),
                          TableType::RangePartition,
                          table_engine};
            const TableName base_table_name =
                TableName{range_table_name.GetBaseTableNameSV(),
                          TableType::Primary,
                          table_engine};
            TableRangeEntry *range_entry =
                const_cast<TableRangeEntry *>(GetTableRangeEntry(
                    range_table_name, node_group_id, partition_id));

            // Checkpoint cannot start until recover is finished, we should be
            // the only one trying to sync the range.

            TransactionExecution *txm = tx_service_->NewTx();
            ClusterConfigRecord rec;
            txm->SetRecoverTxState(txn, tx_term, commit_ts);
            ReadTxRequest read_req(&cluster_config_ccm_name,
                                   0,
                                   VoidKey::NegInfTxKey(),
                                   &rec,
                                   false,
                                   false,
                                   true,
                                   0,
                                   false,
                                   true);
            txm->Execute(&read_req);
            read_req.Wait();
            // Only case we fail here is that leader gone before replay
            // finished.
            bool lock_meta_failed =
                read_req.IsError() ||
                read_req.Result().first != RecordStatus::Normal;

            bool is_dirty = false;
            std::shared_ptr<const TableSchema> table_schema = nullptr;
            if (!lock_meta_failed)
            {
                CatalogKey table_key(base_table_name);
                TxKey tbl_tx_key{&table_key};
                CatalogRecord catalog_rec;

                read_req.Reset();
                read_req.Set(&catalog_ccm_name,
                             0,
                             &tbl_tx_key,
                             &catalog_rec,
                             false,
                             false,
                             true,
                             0,
                             false,
                             true);
                txm->Execute(&read_req);
                read_req.Wait();
                lock_meta_failed =
                    read_req.IsError() ||
                    read_req.Result().first != RecordStatus::Normal;

                if (!lock_meta_failed && catalog_rec.DirtySchema() != nullptr &&
                    table_name.Type() != TableType::Primary)
                {
                    assert(table_name.Type() == TableType::Secondary ||
                           table_name.Type() == TableType::UniqueSecondary);
                    is_dirty =
                        !catalog_rec.Schema()->IndexKeySchema(table_name);
                    assert(
                        !is_dirty ||
                        catalog_rec.DirtySchema()->IndexKeySchema(table_name));
                }
                table_schema = !is_dirty ? catalog_rec.CopySchema()
                                         : catalog_rec.CopyDirtySchema();
                assert(table_schema);
            }

            if (!lock_meta_failed)
            {
                RangeBucketRecord bucket_rec;
                RangeBucketKey bucket_key(
                    Sharder::Instance().MapRangeIdToBucketId(partition_id));
                TxKey bucket_tx_key{&bucket_key};
                read_req.Reset();
                read_req.Set(&range_bucket_ccm_name,
                             0,
                             &bucket_tx_key,
                             &bucket_rec,
                             false,
                             false,
                             true,
                             0,
                             false,
                             true);
                txm->Execute(&read_req);
                read_req.Wait();
                lock_meta_failed =
                    read_req.IsError() ||
                    read_req.Result().first != RecordStatus::Normal;
            }
            if (lock_meta_failed)
            {
                replay_log_cc.AbortCcRequest(
                    CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                txservice::AbortTx(txm);
                return;
            }

            auto task_limiter_key = TaskLimiterKey(node_group_id,
                                                   tx_term,
                                                   table_name.StringView(),
                                                   table_name.Type(),
                                                   table_name.Engine(),
                                                   partition_id);
            // Checkpoint cannot start at `tx_term` until recover is finished,
            // we should be the only one trying to sync the range.
            auto limiter = task_limiters_.emplace(
                task_limiter_key, std::make_shared<DataSyncTaskLimiter>());
            assert(limiter.second == true);
            (void) limiter;

            replay_log_cc.SetFinish();

            StoreRange *store_range = range_entry->PinStoreRange();
            if (!store_range)
            {
                // Fetch range slices from data store if it's not cached.
                WaitableCc cc;
                range_entry->FetchRangeSlices(range_table_name,
                                              &cc,
                                              node_group_id,
                                              tx_term,
                                              cc_shards_[0].get());
                cc.Wait();
                while (cc.IsError())
                {
                    // Failed to fetch range slice. If error is caused by
                    // data store unreachable, retry.
                    if (cc.ErrorCode() == CcErrorCode::DATA_STORE_ERR)
                    {
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(500));
                        cc.Reset();
                        range_entry->FetchRangeSlices(range_table_name,
                                                      &cc,
                                                      node_group_id,
                                                      tx_term,
                                                      cc_shards_[0].get());
                        cc.Wait();
                    }
                    else if (cc.ErrorCode() == CcErrorCode::NG_TERM_CHANGED)
                    {
                        // Term is invalid, we are no longer leader. Abort tx.
                        txservice::AbortTx(txm);
                        PopPendingTask(
                            node_group_id, tx_term, table_name, partition_id);
                        return;
                    }
                    else
                    {
                        assert(false);
                    }
                }
                store_range = range_entry->PinStoreRange();
                assert(store_range != nullptr);
            }

            // Update slice spec in memory for subranges.
            // Because the DataSync tasks for each subrange during a split range
            // transaction are executed in parallel, in oder to reduce conflicts
            // and contention, when calculate the subrange keys, if the post
            // ckpt size of a slice is larger than the average subrange size, we
            // split the slice into multiple subslices in memory, and each
            // subslice belongs to one subrange.
            if (ds_split_range_op_msg.stage() ==
                ::txlog::SplitRangeOpMessage_Stage_PrepareSplit)
            {
                for (size_t idx = 0; idx < new_range_keys.size();)
                {
                    auto slice = store_range->FindSlice(new_range_keys[idx]);
                    if (!(new_range_keys[idx] == slice->StartTxKey()))
                    {
                        assert(slice->StartTxKey() < new_range_keys[idx]);
                        auto it = std::lower_bound(
                            new_range_keys.begin() + idx,
                            new_range_keys.end(),
                            slice->EndTxKey(),
                            [](const TxKey &new_key, const TxKey &end_key)
                            { return new_key < end_key; });

                        size_t subkeys_cnt =
                            it - (new_range_keys.begin() + idx);
                        // Update the current slice into multiple slices.
                        store_range->UpdateSliceSpec(
                            slice, new_range_keys, idx, (subkeys_cnt + 1));

                        idx += subkeys_cnt;
                    }
                    else
                    {
                        ++idx;
                        if (idx < new_range_keys.size() &&
                            new_range_keys[idx] == slice->EndTxKey())
                        {
                            // The new range key equal the next slice's start
                            // key, move to next key.
                            ++idx;
                        }
                    }
                }
            }

            RangeSplitRecoveryTxRequest recover_req(
                ds_split_range_op_msg,
                std::move(table_schema),
                partition_id,
                range_entry,
                std::move(new_range_keys),
                std::move(new_partition_ids),
                node_group_id,
                is_dirty);
            txm->Execute(&recover_req);
            recover_req.Wait();

            if (recover_req.IsError() || !recover_req.Result())
            {
                // Leader transferred away before replay finish. No need
                // to update pending task queue.
                txservice::AbortTx(txm);
            }
            else
            {
                range_entry->UpdateLastDataSyncTS(0);
                range_entry->UnPinStoreRange();
                txservice::CommitTx(txm);
            }

            PopPendingTask(node_group_id, tx_term, table_name, partition_id);
        });

    split_recover_thd.detach();
}

void LocalCcShards::InitTableRanges(const TableName &range_table_name,
                                    std::vector<InitRangeEntry> &init_ranges,
                                    NodeGroupId ng_id,
                                    bool empty_table)
{
    std::unique_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);

    std::unique_lock<std::mutex> heap_lk(table_ranges_heap_mux_);
    bool is_override_thd = mi_is_override_thread();
    mi_threadid_t prev_thd = mi_override_thread(table_ranges_thread_id_);
    mi_heap_t *prev_heap = mi_heap_set_default(table_ranges_heap_);

#if defined(WITH_JEMALLOC)
    uint32_t prev_arena;
    JemallocArenaSwitcher::ReadCurrentArena(prev_arena);
    // override arena id
    auto table_range_arena_id = GetTableRangesArenaId();
    JemallocArenaSwitcher::SwitchToArena(table_range_arena_id);
#endif

    // Init table ranges
    assert(range_table_name.Type() == TableType::RangePartition);
    auto table_it = table_ranges_.try_emplace(range_table_name);
    auto id_table_it = table_range_ids_.try_emplace(range_table_name);
    std::unordered_map<NodeGroupId, std::map<TxKey, TableRangeEntry::uptr>>
        &ranges_of_all_ngs = table_it.first->second;
    auto ngs_it = ranges_of_all_ngs.try_emplace(ng_id);
    std::map<TxKey, TableRangeEntry::uptr> &ranges = ngs_it.first->second;
    auto &ids = id_table_it.first->second.try_emplace(ng_id).first->second;
    assert(init_ranges.size() > 0);

    for (size_t pidx = 0; pidx < init_ranges.size(); ++pidx)
    {
        InitRangeEntry &range_entry = init_ranges[pidx];

        // The first range's start is negative infinity.
        assert(pidx == 0 || range_entry.key_.IsOwner());
        TxKey range_start_key =
            pidx == 0
                ? GetCatalogFactory(range_table_name.Engine())->NegativeInfKey()
                : std::move(range_entry.key_);
        // The tx key in the map is a shallow copy of the start key in the
        // to-be-created range entry.
        TxKey map_key = range_start_key.GetShallowCopy();

        auto range_it = ranges.find(map_key);
        if (range_it == ranges.end())
        {
            TableRangeEntry::uptr new_range =
                GetCatalogFactory(range_table_name.Engine())
                    ->CreateTableRange(std::move(range_start_key),
                                       range_entry.version_ts_,
                                       range_entry.partition_id_);
            range_it = ranges
                           .try_emplace(new_range->RangeStartTxKey(),
                                        std::move(new_range))
                           .first;

            if (range_it != ranges.begin())
            {
                auto prev_range_it = std::prev(range_it);
                const TxKey &range_start = range_it->first;
                prev_range_it->second->SetRangeEndTxKey(
                    range_start.GetShallowCopy());
            }

            auto next_range_it = std::next(range_it);
            if (next_range_it != ranges.end())
            {
                const TxKey &next_start = next_range_it->first;
                range_it->second->SetRangeEndTxKey(next_start.GetShallowCopy());
            }
        }

        ids.insert_or_assign(range_entry.partition_id_, range_it->second.get());
    }
    ranges.rbegin()->second->SetRangeEndTxKey(
        GetCatalogFactory(range_table_name.Engine())->PositiveInfKey());

    if (empty_table)
    {
        assert(init_ranges.size() == 1);
        NodeGroupId range_owner =
            GetRangeOwnerNoLocking(init_ranges.begin()->partition_id_, ng_id)
                ->BucketOwner();
        if (range_owner == ng_id)
        {
            // The store range is only initialized on the node group that owns
            // this range.
            std::vector<SliceInitInfo> slices;
            slices.emplace_back(
                GetCatalogFactory(range_table_name.Engine())->NegativeInfKey(),
                0,
                SliceStatus::FullyCached);
            ranges.begin()->second->InitRangeSlices(std::move(slices),
                                                    ng_id,
                                                    range_table_name.IsBase(),
                                                    true,
                                                    UINT64_MAX,
                                                    false);
        }
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
    JemallocArenaSwitcher::SwitchToArena(prev_arena);
#endif
}

void LocalCcShards::InitPrebuiltTables(NodeGroupId ng_id, int64_t term)
{
    if (txservice_skip_kv)
    {
        for (const auto &[table, image] : prebuilt_tables_)
        {
            auto table_it = table_catalogs_.try_emplace(table);
            auto ng_it = table_it.first->second.try_emplace(ng_id);
            if (ng_it.second)
            {
                ng_it.first->second.InitSchema(
                    GetCatalogFactory(table.Engine())
                        ->CreateTableSchema(table, image, 2),
                    2);
            }
        }
    }
}

void LocalCcShards::PublishMessage(const std::string &chan,
                                   const std::string &message)
{
    assert(publish_func_ != nullptr);

    // start a new bthread to handle message publish as it might block the
    // thread
    bthread_t tid = 0;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;

    // args will be deleted in `publish`
    auto *args_ptr = new PublishArg(this, chan, message);

    if (bthread_start_background(&tid, &attr, Publish, args_ptr) != 0)
    {
        LOG(FATAL) << "Fail to start Publish bthread";
        delete args_ptr;
        publish_func_(chan, message);
    }
}

std::map<TxKey, TableRangeEntry::uptr> *
LocalCcShards::GetTableRangesForATableInternal(
    const TableName &range_table_name, const NodeGroupId ng_id)
{
    auto table_it = table_ranges_.find(range_table_name);
    if (table_it == table_ranges_.end())
    {
        return nullptr;
    }

    auto &ranges_of_all_ngs = table_it->second;
    auto ngs_it = ranges_of_all_ngs.find(ng_id);

    return ngs_it == ranges_of_all_ngs.end() ? nullptr : &ngs_it->second;
}

std::map<TxKey, TableRangeEntry::uptr> *LocalCcShards::GetTableRangesForATable(
    const TableName &range_table_name, const NodeGroupId ng_id)
{
    std::shared_lock<std::shared_mutex> s_lk(meta_data_mux_);
    return GetTableRangesForATableInternal(range_table_name, ng_id);
}

std::optional<std::vector<uint32_t>> LocalCcShards::GetTableRangeIds(
    const TableName &range_table_name, NodeGroupId node_group_id)
{
    // Acquire shared mutex
    std::shared_lock<std::shared_mutex> s_lk(meta_data_mux_);

    std::vector<uint32_t> table_range_ids;
    auto table_it = table_range_ids_.find(range_table_name);
    if (table_it == table_range_ids_.end())
    {
        return std::nullopt;
    }

    auto ng_it = table_it->second.find(node_group_id);
    if (ng_it == table_it->second.end())
    {
        return std::nullopt;
    }

    for (const auto &table_range_id : ng_it->second)
    {
        table_range_ids.push_back(table_range_id.first);
    }

    return table_range_ids;
}

std::unordered_map<uint32_t, TableRangeEntry *> *
LocalCcShards::GetTableRangeIdsForATableInternal(
    const TableName &range_table_name, const NodeGroupId ng_id)
{
    auto table_it = table_range_ids_.find(range_table_name);
    if (table_it == table_range_ids_.end())
    {
        return nullptr;
    }
    auto ng_it = table_it->second.find(ng_id);
    return ng_it == table_it->second.end() ? nullptr : &ng_it->second;
}

void LocalCcShards::CleanTableRange(const TableName &table_name,
                                    NodeGroupId ng_id)
{
    std::unique_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);
    auto table_it = table_ranges_.find(table_name);
    if (table_it != table_ranges_.end())
    {
        table_it->second.erase(ng_id);
    }
    auto id_table_it = table_range_ids_.find(table_name);
    if (id_table_it != table_range_ids_.end())
    {
        id_table_it->second.erase(ng_id);
    }
}

void LocalCcShards::DropTableRanges(NodeGroupId ng_id)
{
    std::unique_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);
    for (auto &table_range : table_ranges_)
    {
        table_range.second.erase(ng_id);
    }
    for (auto &range_id : table_range_ids_)
    {
        range_id.second.erase(ng_id);
    }
}

void LocalCcShards::KickoutRangeSlices()
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    // Since we don't maintain a lru list of store range, we just
    // kickout range slices that have not been accessed for at least
    // 10 mins. If we still haven't cleaned up enough memory. We
    // will sort the range slices by their last accessed time and
    // kickout the least recent accessed range slices.
    std::vector<std::pair<uint64_t, TableRangeEntry *>> scanned_ranges;
    uint64_t current_ts = ClockTs();
    for (auto &[table, table_ranges] : table_ranges_)
    {
        for (auto &[ng_id, ng_ranges] : table_ranges)
        {
            for (auto &[key, range_entry] : ng_ranges)
            {
                std::shared_lock<WritePreferSharedMutex> lk(range_entry->mux_);
                StoreRange *store_range = range_entry->RangeSlices();
                if (store_range)
                {
                    uint64_t last_accessed = store_range->LastAccessedTs();
                    if (current_ts > last_accessed &&
                        current_ts - last_accessed > 600000000)
                    {
                        lk.unlock();
                        std::unique_lock<WritePreferSharedMutex> uniq_lk(
                            range_entry->mux_);
                        if (range_entry->IsStoreRangeFree())
                        {
                            std::unique_lock<std::mutex> heap_lk(
                                table_ranges_heap_mux_);
                            bool is_override_thd = mi_is_override_thread();
                            mi_threadid_t prev_thd = mi_override_thread(
                                GetTableRangesHeapThreadId());
                            mi_heap_t *prev_heap =
                                mi_heap_set_default(GetTableRangesHeap());

#if defined(WITH_JEMALLOC)
                            uint32_t prev_arena;
                            JemallocArenaSwitcher::ReadCurrentArena(prev_arena);
                            // override arena id
                            auto table_range_arena_id = GetTableRangesArenaId();
                            JemallocArenaSwitcher::SwitchToArena(
                                table_range_arena_id);
#endif

                            range_entry->DropStoreRange();

                            bool has_enough_mem = HasEnoughTableRangesMemory();
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
                            JemallocArenaSwitcher::SwitchToArena(prev_arena);
#endif
                            if (has_enough_mem)
                            {
                                // We've cleaned up enough memory space.
                                return;
                            }
                        }
                    }
                    else if (current_ts > last_accessed &&
                             current_ts - last_accessed > 4000000)
                    {
                        // Put the store range into buffer for sort.
                        // Only kickout range slices that are not
                        // accessed for more than 4 seconds.
                        scanned_ranges.emplace_back(last_accessed,
                                                    range_entry.get());
                    }
                }
            }
        }
    }

    // We should not need to reach here most of the time.
    std::sort(scanned_ranges.begin(),
              scanned_ranges.end(),
              [](const std::pair<uint64_t, TableRangeEntry *> &a,
                 const std::pair<uint64_t, TableRangeEntry *> &b) -> bool
              { return a.first < b.first; });
    for (auto &[time, entry] : scanned_ranges)
    {
        std::unique_lock<WritePreferSharedMutex> uniq_lk(entry->mux_);
        if (entry->IsStoreRangeFree())
        {
            std::unique_lock<std::mutex> heap_lk(table_ranges_heap_mux_);
            bool is_override_thd = mi_is_override_thread();
            mi_threadid_t prev_thd =
                mi_override_thread(GetTableRangesHeapThreadId());
            mi_heap_t *prev_heap = mi_heap_set_default(GetTableRangesHeap());
#if defined(WITH_JEMALLOC)
            uint32_t prev_arena;
            JemallocArenaSwitcher::ReadCurrentArena(prev_arena);
            // override arena id
            auto table_range_arena_id = GetTableRangesArenaId();
            JemallocArenaSwitcher::SwitchToArena(table_range_arena_id);
#endif

            entry->DropStoreRange();

            bool has_enough_mem = HasEnoughTableRangesMemory();
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
            JemallocArenaSwitcher::SwitchToArena(prev_arena);
#endif

            if (has_enough_mem)
            {
                // We've cleaned up enough memory space.
                return;
            }
        }
    }
}

TableRangeEntry *LocalCcShards::GetTableRangeEntry(const TableName &table_name,
                                                   const NodeGroupId ng_id,
                                                   const TxKey &key)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition,
                               table_name.Engine());
    return GetTableRangeEntryInternal(range_table_name, ng_id, key);
}

std::optional<std::tuple<uint64_t, TxKey, TxKey>>
LocalCcShards::GetTableRangeKeys(const TableName &table_name,
                                 const NodeGroupId ng_id,
                                 int32_t range_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition,
                               table_name.Engine());
    TableRangeEntry *range_entry =
        GetTableRangeEntryInternal(range_table_name, ng_id, range_id);

    if (range_entry == nullptr)
    {
        return std::nullopt;
    }

    uint64_t version_ts = range_entry->Version();
    auto start_key = range_entry->GetRangeInfo()->StartTxKey().Clone();
    auto end_key = range_entry->GetRangeInfo()->EndTxKey().Clone();

    return std::make_tuple(
        version_ts, std::move(start_key), std::move(end_key));
}

bool LocalCcShards::CheckRangeVersion(const TableName &table_name,
                                      const NodeGroupId ng_id,
                                      int32_t range_id,
                                      uint64_t range_version)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition,
                               table_name.Engine());
    TableRangeEntry *range_entry =
        GetTableRangeEntryInternal(range_table_name, ng_id, range_id);

    if (range_entry == nullptr)
    {
        return false;
    }

    return range_entry->Version() == range_version;
}

const TableRangeEntry *LocalCcShards::GetTableRangeEntry(
    const TableName &table_name, const NodeGroupId ng_id, int32_t range_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition,
                               table_name.Engine());
    return GetTableRangeEntryInternal(range_table_name, ng_id, range_id);
}

const TableRangeEntry *LocalCcShards::GetTableRangeEntryNoLocking(
    const TableName &table_name, const NodeGroupId ng_id, const TxKey &key)
{
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition,
                               table_name.Engine());
    return GetTableRangeEntryInternal(range_table_name, ng_id, key);
}

StoreRange *LocalCcShards::FindRange(const TableName &table_name,
                                     const NodeGroupId ng_id,
                                     const TxKey &key)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition,
                               table_name.Engine());

    TableRangeEntry *entry =
        GetTableRangeEntryInternal(range_table_name, ng_id, key);
    if (!entry)
    {
        DLOG(ERROR) << " The range table of " << table_name.StringView()
                    << " does not exist.";
        return nullptr;
    }

    return entry->RangeSlices();
}

StoreRange *LocalCcShards::FindRange(const TableName &table_name,
                                     const NodeGroupId ng_id,
                                     int32_t range_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition,
                               table_name.Engine());

    TableRangeEntry *entry =
        GetTableRangeEntryInternal(range_table_name, ng_id, range_id);
    if (!entry)
    {
        DLOG(ERROR) << " The range table of " << table_name.StringView()
                    << " does not exist.";
        return nullptr;
    }

    return entry->RangeSlices();
}

uint64_t LocalCcShards::CountRanges(const TableName &table_name,
                                    const NodeGroupId ng_id,
                                    const NodeGroupId key_ng_id) const
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    return CountRangesLockless(table_name, ng_id, key_ng_id);
}

uint64_t LocalCcShards::CountRangesLockless(const TableName &table_name,
                                            const NodeGroupId ng_id,
                                            const NodeGroupId key_ng_id) const
{
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition,
                               table_name.Engine());
    auto range_ngs = table_ranges_.find(range_table_name);
    if (range_ngs == table_ranges_.end())
    {
        return 0;
    }
    auto ranges = range_ngs->second.find(ng_id);
    if (ranges == range_ngs->second.end())
    {
        return 0;
    }
    uint64_t counts = std::accumulate(
        ranges->second.begin(),
        ranges->second.end(),
        0UL,
        [key_ng_id, ng_id, this](
            uint64_t a, const std::pair<const TxKey, TableRangeEntry::uptr> &b)
        {
            NodeGroupId range_ng =
                GetRangeOwnerInternal(b.second->GetRangeInfo()->PartitionId(),
                                      ng_id)
                    ->BucketOwner();
            if (range_ng == key_ng_id)
            {
                return a + 1;
            }
            else
            {
                return a;
            }
        });
    return counts;
}

uint64_t LocalCcShards::CountSlices(const TableName &table_name,
                                    const NodeGroupId ng_id,
                                    const NodeGroupId local_ng_id) const
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition,
                               table_name.Engine());
    assert(Sharder::Instance().LeaderTerm(local_ng_id) >= 0 ||
           Sharder::Instance().CandidateLeaderTerm(local_ng_id) >= 0);

    uint64_t slices = 0;

    const std::map<TxKey, TableRangeEntry::uptr> &ranges =
        table_ranges_.at(range_table_name).at(ng_id);

    for (auto &[range_start_key, range_entry] : ranges)
    {
        NodeGroupId range_ng =
            GetRangeOwnerInternal(range_entry->GetRangeInfo()->PartitionId(),
                                  ng_id)
                ->BucketOwner();
        if (range_ng == local_ng_id)
        {
            const StoreRange *store_range = range_entry->RangeSlices();
            assert(store_range != nullptr);
            slices += store_range->Slices().size();
        }
    }

    return slices;
}

void LocalCcShards::SetTxIdent(uint32_t latest_committed_tx_no)
{
    // Each cc_shard's `next_tx_ident_` is concurrently accessed by log replay
    // thread in this func when native cc node finishes log replay from its
    // bound log group, and tx_processor thread in CcShard::NewTx().
    // They are coordinated by the point when native cc node's `leader_term_`
    // atomic variable becomes positive so no lock is needed.
    uint32_t next_tx_ident = latest_committed_tx_no + 1;
    for (const auto &cc_shard : cc_shards_)
    {
        if (cc_shard->next_tx_ident_ < next_tx_ident)
        {
            cc_shard->next_tx_ident_ = latest_committed_tx_no + 1;
        }
    }
}

void LocalCcShards::DropCatalogs(NodeGroupId cc_ng_id)
{
    std::unique_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);

    for (auto node_catalog_it = table_catalogs_.begin();
         node_catalog_it != table_catalogs_.end();
         ++node_catalog_it)
    {
        node_catalog_it->second.erase(cc_ng_id);
    }
}

std::shared_ptr<TableSchema> LocalCcShards::GetSharedTableSchema(
    const TableName &table_name, NodeGroupId ng_id)
{
    std::shared_lock<std::shared_mutex> shards_lk(meta_data_mux_);

    auto ng_catalog_it = table_catalogs_.find(table_name);
    if (ng_catalog_it == table_catalogs_.end())
    {
        return nullptr;
    }

    auto catalog_it = ng_catalog_it->second.find(ng_id);
    if (catalog_it == ng_catalog_it->second.end())
    {
        return nullptr;
    }

    CatalogEntry &catalog_entry = catalog_it->second;

    std::shared_lock<std::shared_mutex> catalog_s_lk(catalog_entry.s_mux_);
    return catalog_it->second.schema_;
}

std::shared_ptr<TableSchema> LocalCcShards::GetSharedDirtyTableSchema(
    const TableName &table_name, NodeGroupId ng_id)
{
    TableName base_table_name(table_name.GetBaseTableNameSV(),
                              TableType::Primary,
                              table_name.Engine());
    std::shared_lock<std::shared_mutex> shards_lk(meta_data_mux_);

    auto ng_catalog_it = table_catalogs_.find(base_table_name);
    if (ng_catalog_it == table_catalogs_.end())
    {
        return nullptr;
    }

    auto catalog_it = ng_catalog_it->second.find(ng_id);
    if (catalog_it == ng_catalog_it->second.end())
    {
        return nullptr;
    }

    CatalogEntry &catalog_entry = catalog_it->second;

    std::shared_lock<std::shared_mutex> catalog_s_lk(catalog_entry.s_mux_);
    return catalog_entry.dirty_schema_;
}

TableRangeEntry *LocalCcShards::GetTableRangeEntryInternal(
    const TableName &range_tbl_name, const NodeGroupId ng_id, const TxKey &key)
{
    // The caller of the method must have acquired a shared lock. The table name
    // must be the range table name.

    auto range_map = GetTableRangesForATableInternal(range_tbl_name, ng_id);
    if (!range_map)
    {
        return nullptr;
    }
    TableRangeEntry *entry = nullptr;

    auto lower_it = range_map->lower_bound(key);

    if (lower_it == range_map->end())
    {
        // The input key is greater than the last entry of the range map, so
        // it falls into the last range.
        --lower_it;
        entry = lower_it->second.get();
    }
    else if (lower_it->first == key)
    {
        entry = lower_it->second.get();
    }
    else
    {
        --lower_it;
        entry = lower_it->second.get();
    }

    return entry;
}

TableRangeEntry *LocalCcShards::GetTableRangeEntryInternal(
    const TableName &range_tbl_name, const NodeGroupId ng_id, int32_t range_id)
{
    // The caller of the method must have acquired a shared lock. The table name
    // must be the range table name.
    auto table_it = table_range_ids_.find(range_tbl_name);
    if (table_it == table_range_ids_.end())
    {
        return nullptr;
    }
    auto ng_it = table_it->second.find(ng_id);
    if (ng_it == table_it->second.end())
    {
        return nullptr;
    }

    auto range_it = ng_it->second.find(range_id);
    return range_it == ng_it->second.end() ? nullptr : range_it->second;
}

std::pair<std::shared_ptr<Statistics>, bool> LocalCcShards::InitTableStatistics(
    TableSchema *table_schema, NodeGroupId ng_id)
{
    std::unique_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);

    auto ng_statistics_it =
        table_statistics_map_.try_emplace(table_schema->GetBaseTableName());
    auto statistics_it = ng_statistics_it.first->second.try_emplace(ng_id);
    if (statistics_it.second)
    {
        StatisticsEntry &statistics_entry = statistics_it.first->second;

        statistics_entry.statistics_ =
            GetCatalogFactory(table_schema->GetBaseTableName().Engine())
                ->CreateTableStatistics(table_schema, ng_id);

        table_schema->BindStatistics(statistics_entry.statistics_);
    }

    return {statistics_it.first->second.statistics_, statistics_it.second};
}

std::pair<std::shared_ptr<Statistics>, bool> LocalCcShards::InitTableStatistics(
    TableSchema *table_schema,
    TableSchema *dirty_table_schema,
    NodeGroupId ng_id,
    std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
        sample_pool_map,
    CcShard *ccs)
{
    std::unique_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);

    auto ng_statistics_it =
        table_statistics_map_.try_emplace(table_schema->GetBaseTableName());
    auto statistics_it = ng_statistics_it.first->second.try_emplace(ng_id);
    if (statistics_it.second)
    {
        StatisticsEntry &statistics_entry = statistics_it.first->second;

        statistics_entry.statistics_ =
            GetCatalogFactory(table_schema->GetBaseTableName().Engine())
                ->CreateTableStatistics(
                    table_schema, std::move(sample_pool_map), ccs, ng_id);

        table_schema->BindStatistics(statistics_entry.statistics_);
        if (dirty_table_schema)
        {
            dirty_table_schema->BindStatistics(statistics_entry.statistics_);
        }
    }

    return {statistics_it.first->second.statistics_, statistics_it.second};
}

StatisticsEntry *LocalCcShards::GetTableStatistics(const TableName &table_name,
                                                   NodeGroupId ng_id)
{
    std::shared_lock<std::shared_mutex> s_lk(meta_data_mux_);

    auto ng_statistics_it = table_statistics_map_.find(table_name);
    if (ng_statistics_it == table_statistics_map_.end())
    {
        return nullptr;
    }

    auto statistics_it = ng_statistics_it->second.find(ng_id);
    return statistics_it == ng_statistics_it->second.end()
               ? nullptr
               : &statistics_it->second;
}

void LocalCcShards::CleanTableStatistics(const TableName &table_name,
                                         NodeGroupId ng_id)
{
    std::unique_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);

    auto ng_statistics_it = table_statistics_map_.find(table_name);
    if (ng_statistics_it != table_statistics_map_.end())
    {
        ng_statistics_it->second.erase(ng_id);
    }
}

void LocalCcShards::DropTableStatistics(NodeGroupId ng_id)
{
    std::unique_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);
    for (auto ng_statistics_it = table_statistics_map_.begin();
         ng_statistics_it != table_statistics_map_.end();
         ++ng_statistics_it)
    {
        ng_statistics_it->second.erase(ng_id);
    }
}

void LocalCcShards::BroadcastIndexStatistics(
    TransactionExecution *txm,
    NodeGroupId ng_id,
    const TableName &table_name,
    const TableSchema *table_schema,
    const remote::NodeGroupSamplePool &sample_pool)
{
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();

    if (ng_cnt > 1)
    {
        BroadcastStatisticsTxRequest broadcast_req(
            &table_name, table_schema->Version(), &sample_pool);
        txm->Execute(&broadcast_req);
        broadcast_req.Wait();
        if (broadcast_req.IsError())
        {
            LOG(ERROR) << "Broadcast table statistics for table: "
                       << table_name.StringView()
                       << ", error: " << broadcast_req.ErrorMsg();
        }
    }
}

const BucketInfo *LocalCcShards::GetBucketInfo(const uint16_t bucket_id,
                                               const NodeGroupId ng_id) const
{
    if (!buckets_migrating_.load(std::memory_order_relaxed))
    {
        return GetBucketInfoInternal(bucket_id, ng_id);
    }
    else
    {
        std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
        return GetBucketInfoInternal(bucket_id, ng_id);
    }
}

BucketInfo *LocalCcShards::GetBucketInfo(const uint16_t bucket_id,
                                         const NodeGroupId ng_id)
{
    return const_cast<BucketInfo *>(
        std::as_const(*this).GetBucketInfo(bucket_id, ng_id));
}

NodeGroupId LocalCcShards::GetBucketOwner(const uint16_t bucket_id,
                                          const NodeGroupId ng_id) const
{
    const BucketInfo *bucket_info = GetBucketInfo(bucket_id, ng_id);
    if (bucket_info != nullptr)
    {
        return bucket_info->BucketOwner();
    }
    return UINT32_MAX;
}

BucketInfo *LocalCcShards::GetBucketInfoInternal(const uint16_t bucket_id,
                                                 const NodeGroupId ng_id) const
{
    assert(bucket_id < total_range_buckets);
    auto ng_bucket_it = bucket_infos_.find(ng_id);
    if (ng_bucket_it == bucket_infos_.end() || ng_bucket_it->second.empty())
    {
        return nullptr;
    }

    return ng_bucket_it->second.at(bucket_id).get();
}

const BucketInfo *LocalCcShards::GetRangeOwner(const int32_t range_id,
                                               const NodeGroupId ng_id) const
{
    return GetBucketInfo(Sharder::MapRangeIdToBucketId(range_id), ng_id);
}

const BucketInfo *LocalCcShards::GetRangeOwnerNoLocking(
    const int32_t range_id, const NodeGroupId ng_id) const
{
    return GetBucketInfoInternal(Sharder::MapRangeIdToBucketId(range_id),
                                 ng_id);
}

BucketInfo *LocalCcShards::GetRangeOwnerInternal(const int32_t range_id,
                                                 const NodeGroupId ng_id) const
{
    return GetBucketInfoInternal(Sharder::MapRangeIdToBucketId(range_id),
                                 ng_id);
}

std::vector<std::pair<uint16_t, NodeGroupId>> LocalCcShards::GetAllBucketOwners(
    NodeGroupId node_group_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    auto ng_bucket_it = bucket_infos_.find(node_group_id);
    if (ng_bucket_it == bucket_infos_.end())
    {
        return {};
    }

    std::vector<std::pair<uint16_t, NodeGroupId>> bucket_owners;
    auto &ng_buckets = ng_bucket_it->second;

    for (const auto &pair : ng_buckets)
    {
        bucket_owners.emplace_back(pair.first, pair.second->BucketOwner());
    }

    return bucket_owners;
}

const std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>> *
LocalCcShards::GetAllBucketInfos(NodeGroupId ng_id) const
{
    std::shared_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);
    auto ng_bucket_it = bucket_infos_.find(ng_id);
    if (ng_bucket_it == bucket_infos_.end())
    {
        return nullptr;
    }
    return &ng_bucket_it->second;
}

const std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>> *
LocalCcShards::GetAllBucketInfosNoLocking(const NodeGroupId ng_id) const
{
    auto ng_bucket_it = bucket_infos_.find(ng_id);
    if (ng_bucket_it == bucket_infos_.end())
    {
        return nullptr;
    }
    return &ng_bucket_it->second;
}

void LocalCcShards::DropBucketInfo(NodeGroupId ng_id)
{
    std::unique_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);
    // bucket_infos_.erase(ng_id);
    assert(bucket_infos_.find(ng_id) != bucket_infos_.end());
    bucket_infos_.at(ng_id).clear();
}

bool LocalCcShards::IsRangeBucketsInitialized(NodeGroupId ng_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    auto bucket_info = bucket_infos_.find(ng_id);
    return bucket_info != bucket_infos_.end() &&
           bucket_info->second.size() == total_range_buckets;
}

void LocalCcShards::InitRangeBuckets(NodeGroupId ng_id,
                                     const std::set<NodeGroupId> &node_groups,
                                     uint64_t version)
{
    std::unique_lock<FastMetaDataMutex> lk(fast_meta_data_mux_);
    size_t ng_cnt = node_groups.size();
    if (bucket_infos_.size() != ng_cnt)
    {
        // Init bucket_info container for all node groups.
        // Case one node group failover to other machine, we just clear
        // bucket_info container instead of erase it from bucket_infos_.
        // Case other node group failover to this machine, we can just fetch
        // bucket_info container from bucket_infos_ instead of insert into
        // bucket_infos_ during failover.
        //
        // Then, the buckte_infos_ never be modified if there is no cluster
        // scaling. So, we can safely read buckte_infos_ without locking
        // meta_data_mux_ when cluster is not migrating.
        for (auto &ng : node_groups)
        {
            bucket_infos_.try_emplace(ng);
        }
    }

    // Construct bucket info map on startup
    // Generate 64 random numbers for each node group as virtual nodes on
    // hashing ring. Each bucket id belongs to the first virtual node that is
    // larger than the bucket id.
    std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>> &ng_bucket_infos =
        bucket_infos_.at(ng_id);
    ng_bucket_infos.clear();
    std::map<uint16_t, NodeGroupId> rand_num_to_ng;
    // use ng id as seed to generate random numbers
    for (auto ng : node_groups)
    {
        // Thread-safe and deterministic random generator
        std::mt19937 rng(ng);
        std::uniform_int_distribution<uint16_t> dist(0,
                                                     total_range_buckets - 1);
        size_t generated = 0;
        while (generated < 64)
        {
            uint16_t rand_num = dist(rng);
            if (rand_num_to_ng.find(rand_num) == rand_num_to_ng.end())
            {
                generated++;
                rand_num_to_ng.emplace(rand_num, ng);
            }
            if (rand_num_to_ng.size() >= total_range_buckets)
            {
                LOG(WARNING)
                    << "Cluster has too many node groups, need to reduce the "
                       "number of buckets held by each node group";
                break;
            }
        }
    }

    // Insert bucket ids into the map.
    auto it = rand_num_to_ng.begin();
    for (uint16_t bucket_id = 0; bucket_id < total_range_buckets; bucket_id++)
    {
        // The buckets larger than the last random number belongs to the
        // first virtual node on the ring.
        if (it != rand_num_to_ng.end() && bucket_id >= it->first)
        {
            it++;
        }
        NodeGroupId ng_id = it == rand_num_to_ng.end()
                                ? rand_num_to_ng.begin()->second
                                : it->second;
        auto insert_res = ng_bucket_infos.try_emplace(
            bucket_id, std::make_unique<BucketInfo>(ng_id, version));
        if (insert_res.second)
        {
            insert_res.first->second->Set(ng_id, version);
        }
    }
}

const BucketInfo *LocalCcShards::UploadNewBucketInfo(NodeGroupId ng_id,
                                                     uint16_t bucket_id,
                                                     NodeGroupId dirty_ng,
                                                     uint64_t dirty_version)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    BucketInfo *bucket_info = GetBucketInfoInternal(bucket_id, ng_id);
    bucket_info->SetDirty(dirty_ng, dirty_version);
    if (dirty_ng == ng_id)
    {
        // Allow this bucket to accept upload batch request from original
        // bucket owner to warm up cache.
        bucket_info->SetAcceptsUploadBatch(true);
    }
    return bucket_info;
}

const BucketInfo *LocalCcShards::UploadBucketInfo(NodeGroupId ng_id,
                                                  uint16_t bucket_id,
                                                  NodeGroupId owner_ng,
                                                  uint64_t version)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    BucketInfo *bucket_info = GetBucketInfoInternal(bucket_id, ng_id);
    assert(version > bucket_info->Version() &&
           version >= bucket_info->DirtyVersion());
    bucket_info->ClearDirty();
    bucket_info->Set(owner_ng, version);
    return bucket_info;
}

bool LocalCcShards::DropStoreRangesInBucket(NodeGroupId ng_id,
                                            uint16_t bucket_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    for (auto &[tbl_name, ng_ranges] : table_ranges_)
    {
        auto tbl_ranges = ng_ranges.find(ng_id);
        if (tbl_ranges != ng_ranges.end())
        {
            for (auto &[key, entry] : tbl_ranges->second)
            {
                if (Sharder::MapRangeIdToBucketId(
                        entry->GetRangeInfo()->PartitionId()) == bucket_id)
                {
                    std::unique_lock<WritePreferSharedMutex> uniq_lk(
                        entry->mux_);
                    if (entry->IsStoreRangeFree(true))
                    {
                        std::unique_lock<std::mutex> heap_lk(
                            table_ranges_heap_mux_);
                        bool is_override_thd = mi_is_override_thread();
                        mi_threadid_t prev_thd =
                            mi_override_thread(GetTableRangesHeapThreadId());
                        mi_heap_t *prev_heap =
                            mi_heap_set_default(GetTableRangesHeap());

#if defined(WITH_JEMALLOC)
                        uint32_t prev_arena;
                        JemallocArenaSwitcher::ReadCurrentArena(prev_arena);
                        // override arena id
                        auto table_range_arena_id = GetTableRangesArenaId();
                        JemallocArenaSwitcher::SwitchToArena(
                            table_range_arena_id);
#endif

                        entry->DropStoreRange();

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
                        JemallocArenaSwitcher::SwitchToArena(prev_arena);
#endif
                    }
                    else
                    {
                        return false;
                    }
                }
            }
        }
    }
    return true;
}

std::unordered_map<TableName, std::unordered_set<int>>
LocalCcShards::GetRangesInBucket(uint16_t bucket_id, NodeGroupId ng_id)
{
    std::unordered_map<TableName, std::unordered_set<int>> snapshot;
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    for (auto &[tbl_name, ng_ranges] : table_ranges_)
    {
        auto tbl_ranges = ng_ranges.find(ng_id);
        if (tbl_ranges != ng_ranges.end())
        {
            std::unordered_set<int> tbl_snapshot;
            for (auto &[key, entry] : tbl_ranges->second)
            {
                if (Sharder::MapRangeIdToBucketId(
                        entry->GetRangeInfo()->PartitionId()) == bucket_id)
                {
                    tbl_snapshot.insert(entry->GetRangeInfo()->PartitionId());
                }
            }
            if (!tbl_snapshot.empty())
            {
                assert(!tbl_name.IsHashPartitioned());
                snapshot.try_emplace(TableName{tbl_name.StringView(),
                                               tbl_name.Type(),
                                               tbl_name.Engine()},
                                     std::move(tbl_snapshot));
            }
        }
    }
    return snapshot;
}

const BucketInfo *LocalCcShards::CommitDirtyBucketInfo(NodeGroupId ng_id,
                                                       uint16_t bucket_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    BucketInfo *bucket_info = GetBucketInfoInternal(bucket_id, ng_id);
    bucket_info->CommitDirty();
    return bucket_info;
}

std::unordered_map<NodeGroupId, BucketMigrateInfo>
LocalCcShards::GenerateBucketMigrationPlan(
    const std::set<NodeGroupId> &new_node_groups)
{
    // Construct bucket info map on startup
    // Generate 64 random numbers for each node group as virtual nodes on
    // hashing ring. Each bucket id belongs to the first virtual node that is
    // larger than the bucket id.
    std::map<uint16_t, NodeGroupId> rand_num_to_ng;
    // use ng id as seed to generate random numbers
    for (auto ng : new_node_groups)
    {
        std::mt19937 rng(ng);
        std::uniform_int_distribution<uint16_t> dist(0,
                                                     total_range_buckets - 1);
        size_t generated = 0;
        while (generated < 64)
        {
            uint16_t rand_num = dist(rng);
            if (rand_num_to_ng.find(rand_num) == rand_num_to_ng.end())
            {
                generated++;
                rand_num_to_ng.emplace(rand_num, ng);
            }
            if (rand_num_to_ng.size() >= total_range_buckets)
            {
                LOG(WARNING)
                    << "Cluster has too many node groups, need to reduce the "
                       "number of buckets held by each node group";
                break;
            }
        }
    }

    std::unordered_map<NodeGroupId, BucketMigrateInfo> migrate_plan;

    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    auto it = rand_num_to_ng.begin();
    for (uint16_t bucket_id = 0; bucket_id < total_range_buckets; bucket_id++)
    {
        // The buckets larger than the last random number belongs to the
        // first virtual node on the ring.
        if (it != rand_num_to_ng.end() && bucket_id >= it->first)
        {
            it++;
        }
        NodeGroupId ng_id = it == rand_num_to_ng.end()
                                ? rand_num_to_ng.begin()->second
                                : it->second;
        // This function should only be called as preferred leader of node
        // group.
        NodeGroupId cur_owner =
            GetBucketInfoInternal(bucket_id,
                                  Sharder::Instance().NativeNodeGroup())
                ->BucketOwner();
        if (cur_owner != ng_id)
        {
            auto ins_pair = migrate_plan.try_emplace(cur_owner);
            BucketMigrateInfo &bucket_migrate_info = ins_pair.first->second;
            bucket_migrate_info.bucket_ids_.push_back(bucket_id);
            bucket_migrate_info.new_owner_ngs_.push_back(ng_id);
        }
    }
    return migrate_plan;
}

std::pair<TableRangeEntry *, StoreRange *> LocalCcShards::PinStoreRange(
    const TableName &table_name,
    const NodeGroupId ng_id,
    int64_t ng_term,
    const TxKey &start_key,
    CcRequestBase *cc_request,
    CcShard *cc_shard)
{
    // This is invoked during the DataSync process, during which the node group
    // is pinned, range entry will not be dropped by ClearCcNodeGroup. And No
    // other process will update table ranges for this table, so we don't need
    // meta data shared lock here.
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition,
                               table_name.Engine());

    TableRangeEntry *range_entry =
        GetTableRangeEntryInternal(range_table_name, ng_id, start_key);
    assert(range_entry);

    StoreRange *store_range = range_entry->PinStoreRange();
    if (store_range == nullptr)
    {
        // The range must be owned by ng_id because we have acquired read lock
        // on catalog and bucket.
        assert(
            [&]()
            {
                return GetBucketInfoInternal(
                           Sharder::Instance().MapRangeIdToBucketId(
                               range_entry->GetRangeInfo()->PartitionId()),
                           ng_id)
                           ->BucketOwner() == ng_id;
            }());

        // Fetch range slices info from data store if it's not loaded yet.
        range_entry->FetchRangeSlices(
            range_table_name, cc_request, ng_id, ng_term, cc_shard);
        return {range_entry, nullptr};
    }
    return {range_entry, store_range};
}

bool LocalCcShards::EnqueueRangeDataSyncTask(
    const TableName &table_name,
    uint32_t ng_id,
    int64_t ng_term,
    TableRangeEntry *range_entry,
    uint64_t data_sync_ts,
    bool is_dirty,
    bool can_be_skipped,
    uint64_t &last_sync_ts,
    std::shared_ptr<DataSyncStatus> status,
    CcHandlerResult<Void> *hres)
{
    const RangeInfo *range_info = range_entry->GetRangeInfo();
    NodeGroupId range_ng =
        GetRangeOwnerInternal(range_info->PartitionId(), ng_id)->BucketOwner();
    if (range_ng == ng_id)
    {
        last_sync_ts = std::min(range_entry->GetLastSyncTs(), last_sync_ts);
        auto task_limiter_key = TaskLimiterKey(ng_id,
                                               ng_term,
                                               table_name.StringView(),
                                               table_name.Type(),
                                               table_name.Engine(),
                                               range_info->PartitionId());

        std::unique_lock<std::mutex> task_limiter_lk(task_limiter_mux_);
        auto iter = task_limiters_.find(task_limiter_key);
        if (iter == task_limiters_.end())
        {
            // Create task limiter
            auto limiter = task_limiters_.emplace(
                task_limiter_key, std::make_shared<DataSyncTaskLimiter>());
            // Update `latest_pending_task_ts` to higher ts if this task can be
            // skipped(also means it's `data_sync_ts_` can be adjusted).
            if (can_be_skipped)
            {
                limiter.first->second->latest_pending_task_ts_ = data_sync_ts;
            }
            // Relase `task_limiter_mux_`
            task_limiter_lk.unlock();

            // Push task to worker task queue.
            std::lock_guard<std::mutex> task_worker_lk(
                data_sync_worker_ctx_.mux_);
            data_sync_task_queue_[range_info->PartitionId() %
                                  data_sync_task_queue_.size()]
                .emplace_back(
                    std::make_shared<DataSyncTask>(table_name,
                                                   range_info->PartitionId(),
                                                   range_info->VersionTs(),
                                                   ng_id,
                                                   ng_term,
                                                   data_sync_ts,
                                                   status,
                                                   is_dirty,
                                                   can_be_skipped,
                                                   hres));
            return true;
        }
        else
        {
            if (can_be_skipped)
            {
                assert(hres == nullptr);
                // '0' means have no pending task on queue.
                if (iter->second->latest_pending_task_ts_ == 0)
                {
                    iter->second->latest_pending_task_ts_ = data_sync_ts;
                    iter->second->pending_tasks_.push(
                        std::make_shared<DataSyncTask>(
                            table_name,
                            range_info->PartitionId(),
                            range_info->VersionTs(),
                            ng_id,
                            ng_term,
                            data_sync_ts,
                            status,
                            is_dirty,
                            can_be_skipped,
                            hres));
                    return true;
                }
                else
                {
                    // Already has one pending task on the pending queue. We
                    // just update `latest_pending_task_ts_`.
                    iter->second->latest_pending_task_ts_ = std::max(
                        iter->second->latest_pending_task_ts_, data_sync_ts);
                    status->SetNoTruncateLog();
                    return false;
                }
            }
            else
            {
                // This task can't be skipped(DataMigration, CraeteIndex,
                // LastCheckpoint). So we push this task to the pending task
                // queue of `Limiter`
                iter->second->pending_tasks_.push(
                    std::make_shared<DataSyncTask>(table_name,
                                                   range_info->PartitionId(),
                                                   range_info->VersionTs(),
                                                   ng_id,
                                                   ng_term,
                                                   data_sync_ts,
                                                   status,
                                                   is_dirty,
                                                   can_be_skipped,
                                                   hres));
                return true;
            }
        }
    }
    else
    {
        auto new_range_ids = range_info->NewPartitionId();
        if (new_range_ids &&
            (is_dirty || range_info->DirtyTs() <= data_sync_ts))
        {
            assert(range_info->IsDirty());
            for (int32_t new_range : *new_range_ids)
            {
                NodeGroupId new_range_ng =
                    GetRangeOwnerInternal(new_range, ng_id)->BucketOwner();
                if (new_range_ng == ng_id)
                {
                    // If range is splitting and the new range falls on
                    // current node group, we might receive forwarded
                    // messages. These messages cannot be flushed into
                    // data store yet since we cannot update their slice
                    // specs. Thus the log cannot be truncated for this
                    // round of checkpoint.
                    LOG(INFO)
                        << "Unable to truncate log since " << table_name.Trace()
                        << ", range " << range_info->PartitionId()
                        << " is forwarding message to ng " << ng_id
                        << " during range split.";

                    last_sync_ts = 0;
                    // Mark the task as failed since we cannot gaurantee all
                    // data before data sync ts is flushed.
                    std::lock_guard<std::mutex> status_lk(status->mux_);
                    status->err_code_ = CcErrorCode::PIN_RANGE_SLICE_FAILED;
                    break;
                }
            }
        }

        return false;
    }
}

void LocalCcShards::EnqueueDataSyncTaskForSplittingRange(
    const TableName &table_name,
    uint32_t ng_id,
    int64_t ng_term,
    std::shared_ptr<const TableSchema> table_schema,
    TableRangeEntry *range_entry,
    uint64_t data_sync_ts,
    bool is_dirty,
    uint64_t txn,
    CcHandlerResult<Void> *hres)
{
    std::shared_ptr<DataSyncStatus> status =
        std::make_shared<DataSyncStatus>(ng_id, ng_term, false);
    const std::vector<TxKey> *new_keys = range_entry->GetRangeInfo()->NewKey();
    const std::vector<int32_t> *new_range_ids =
        range_entry->GetRangeInfo()->NewPartitionId();
    assert(new_keys->size() == new_range_ids->size());

    // Push task to worker task queue.
    // There is no need to construct DataSyncTaskLimiter for those subrange
    // tasks, due to the following reasons:
    // 1. For the first subrange which is the old range, already exists one. And
    // the ongoing DataSync task for this range is the one that launch this
    // split range transaction.
    // 2. For the remaining subranges, there is only one task for them. So there
    // is no need to construct a limiter.
    std::unique_lock<std::mutex> task_worker_lk(data_sync_worker_ctx_.mux_);

    TxKey old_start_key = range_entry->GetRangeInfo()->StartTxKey();
    TxKey old_end_key = range_entry->GetRangeInfo()->EndTxKey();
    // The old range
    data_sync_task_queue_[range_entry->GetRangeInfo()->PartitionId() %
                          data_sync_task_queue_.size()]
        .emplace_back(std::make_shared<DataSyncTask>(
            table_name,
            ng_id,
            ng_term,
            table_schema,
            range_entry,
            range_entry->GetRangeInfo()->StartTxKey(),
            *new_keys->begin(),
            data_sync_ts,
            is_dirty,
            false,
            txn,
            status,
            hres));

    bool need_copy_range = store_hd_->NeedCopyRange();

    for (size_t i = 0; i < new_keys->size(); ++i)
    {
        int32_t new_range_id = (*new_range_ids)[i];
        TxKey end_key =
            (i == new_keys->size() - 1 ? range_entry->GetRangeInfo()->EndTxKey()
                                       : (*new_keys)[i + 1].GetShallowCopy());
        data_sync_task_queue_[new_range_id % data_sync_task_queue_.size()]
            .emplace_back(std::make_shared<DataSyncTask>(table_name,
                                                         ng_id,
                                                         ng_term,
                                                         table_schema,
                                                         range_entry,
                                                         (*new_keys)[i],
                                                         end_key,
                                                         data_sync_ts,
                                                         is_dirty,
                                                         need_copy_range,
                                                         txn,
                                                         status,
                                                         hres));
    }

    data_sync_worker_ctx_.cv_.notify_all();
    task_worker_lk.unlock();

    {
        std::lock_guard<std::mutex> status_lk(status->mux_);

        status->unfinished_tasks_ += (new_keys->size() + 1);
        status->unfinished_scan_tasks_ += (new_keys->size() + 1);
        status->all_task_started_ = true;
        if (status->unfinished_tasks_ == 0)
        {
            hres->SetFinished();
            return;
        }
        else if (status->unfinished_scan_tasks_ == 0)
        {
            FlushCurrentFlushBuffer();
            return;
        }
    }
}
bool LocalCcShards::EnqueueDataSyncTaskToCore(
    const TableName &table_name,
    uint32_t ng_id,
    int64_t ng_term,
    uint64_t data_sync_ts,
    uint16_t core_idx,
    bool is_standby_node,
    bool is_dirty,
    bool can_be_skipped,
    std::shared_ptr<DataSyncStatus> status,
    CcHandlerResult<Void> *hres,
    bool send_cache_for_migration,
    std::function<bool(size_t)> filter_lambda)
{
    auto task_limiter_key = TaskLimiterKey(ng_id,
                                           ng_term,
                                           table_name.StringView(),
                                           table_name.Type(),
                                           table_name.Engine(),
                                           core_idx);
    std::unique_lock<std::mutex> task_limiter_lk(task_limiter_mux_);
    auto iter = task_limiters_.find(task_limiter_key);
    bool enqueued_task = false;
    if (iter == task_limiters_.end())
    {
        // Create task limiter
        auto limiter = task_limiters_.emplace(
            task_limiter_key, std::make_shared<DataSyncTaskLimiter>());
        // Update `latest_pending_task_ts` to higher ts if this task can be
        // skipped(also means it's `data_sync_ts_` can be adjusted).
        if (can_be_skipped)
        {
            limiter.first->second->latest_pending_task_ts_ = data_sync_ts;
        }
        // Relase `task_limiter_mux_`
        task_limiter_lk.unlock();

        auto task = std::make_shared<DataSyncTask>(table_name,
                                                   core_idx,
                                                   0,
                                                   ng_id,
                                                   ng_term,
                                                   data_sync_ts,
                                                   status,
                                                   is_dirty,
                                                   can_be_skipped,
                                                   hres,
                                                   filter_lambda,
                                                   send_cache_for_migration,
                                                   is_standby_node);

        // Push task to worker task queue.
        {
            std::lock_guard<std::mutex> task_worker_lk(
                data_sync_worker_ctx_.mux_);
            data_sync_task_queue_[core_idx].emplace_back(task);
            data_sync_worker_ctx_.cv_.notify_all();
        }
        enqueued_task = true;
    }
    else
    {
        if (can_be_skipped)
        {
            assert(hres == nullptr);
            // '0' means have no pending task on queue. so we push this task
            // to PendingTaskQueue
            if (iter->second->latest_pending_task_ts_ == 0)
            {
                iter->second->latest_pending_task_ts_ = data_sync_ts;
                iter->second->pending_tasks_.push(
                    std::make_shared<DataSyncTask>(table_name,
                                                   core_idx,
                                                   0,
                                                   ng_id,
                                                   ng_term,
                                                   data_sync_ts,
                                                   status,
                                                   is_dirty,
                                                   can_be_skipped,
                                                   hres,
                                                   filter_lambda,
                                                   send_cache_for_migration,
                                                   is_standby_node));
                enqueued_task = true;
            }
            else
            {
                // Already has one pending task on the PendingTaskQueue. We
                // just update `latest_pending_task_ts_` to higher ts.
                iter->second->latest_pending_task_ts_ = std::max(
                    iter->second->latest_pending_task_ts_, data_sync_ts);
                status->SetNoTruncateLog();
            }
        }
        else
        {
            // This task can't be skipped(DataMigration, CraeteIndex,
            // LastCheckpoint). Because these operations need to explicitly
            // flush data into storage, rather than relying on other
            // checkpoint tasks.
            iter->second->pending_tasks_.push(
                std::make_shared<DataSyncTask>(table_name,
                                               core_idx,
                                               0,
                                               ng_id,
                                               ng_term,
                                               data_sync_ts,
                                               status,
                                               is_dirty,
                                               can_be_skipped,
                                               hres,
                                               filter_lambda,
                                               send_cache_for_migration,
                                               is_standby_node));
            enqueued_task = true;
        }
    }
    return enqueued_task;
}

void LocalCcShards::EnqueueDataSyncTaskForTable(
    const TableName &table_name,
    uint32_t ng_id,
    int64_t ng_term,
    uint64_t data_sync_ts,
    uint64_t &last_data_sync_ts,
    bool is_standby_node,
    bool is_dirty,
    bool can_be_skipped,
    std::shared_ptr<DataSyncStatus> status,
    CcHandlerResult<Void> *hres)
{
    assert(status != nullptr);

    std::shared_lock<std::shared_mutex> meta_lk(meta_data_mux_);
    last_data_sync_ts = UINT64_MAX;
    uint32_t unfinished_task_cnt = 0;

    if (table_name.IsHashPartitioned())
    {
        LOG(INFO) << "EnqueueDataSyncTaskForTable: " << table_name.StringView()
                  << ", data_sync_ts: " << data_sync_ts;
        CatalogEntry *catalog_entry = GetCatalogInternal(table_name, ng_id);
        if (!catalog_entry)
        {
            if (hres)
            {
                hres->SetFinished();
            }

            return;
        }

        last_data_sync_ts = catalog_entry->GetMinLastSyncTs();
        // Release `meta_data_mux_`
        meta_lk.unlock();

        auto core_count = cc_shards_.size();

        for (size_t core_idx = 0; core_idx < core_count; ++core_idx)
        {
            if (EnqueueDataSyncTaskToCore(table_name,
                                          ng_id,
                                          ng_term,
                                          data_sync_ts,
                                          core_idx,
                                          is_standby_node,
                                          is_dirty,
                                          can_be_skipped,
                                          status,
                                          hres))
            {
                unfinished_task_cnt++;
            }
        }
    }
    else
    {
        TableName range_table_name(table_name.StringView(),
                                   TableType::RangePartition,
                                   table_name.Engine());
        auto ranges = GetTableRangesForATableInternal(range_table_name, ng_id);
        if (ranges == nullptr)
        {
            if (hres)
            {
                hres->SetFinished();
            }
            return;
        }

        for (auto &range : *ranges)
        {
            if (EnqueueRangeDataSyncTask(table_name,
                                         ng_id,
                                         ng_term,
                                         range.second.get(),
                                         data_sync_ts,
                                         is_dirty,
                                         can_be_skipped,
                                         last_data_sync_ts,
                                         status,
                                         hres))
            {
                // Increment local variable to reduce lock contention.
                unfinished_task_cnt++;
            }
        }

        meta_lk.unlock();
    }

    {
        std::lock_guard<std::mutex> status_lk(status->mux_);
        status->unfinished_tasks_ += unfinished_task_cnt;
        status->unfinished_scan_tasks_ += unfinished_task_cnt;
        if (hres)
        {
            status->all_task_started_ = true;
            if (status->unfinished_tasks_ == 0)
            {
                hres->SetFinished();
                return;
            }
        }

        if (status->unfinished_scan_tasks_ == 0 &&
            status->unfinished_tasks_ != 0 && status->all_task_started_)
        {
            FlushCurrentFlushBuffer();
            return;
        }
    }
    data_sync_worker_ctx_.cv_.notify_all();
}

void LocalCcShards::EnqueueDataSyncTaskForBucket(
    const std::unordered_map<TableName, std::unordered_set<int32_t>>
        &ranges_in_bucket_snapshot,
    const std::unordered_set<TableName> &hash_partitioned_tables,
    const std::vector<uint16_t> &bucket_ids,
    bool send_cache_for_migration,
    uint32_t ng_id,
    int64_t ng_term,
    uint64_t data_sync_ts,
    CcHandlerResult<Void> *hres)
{
    std::shared_lock<std::shared_mutex> meta_lk(meta_data_mux_);
    std::shared_ptr<DataSyncStatus> status =
        std::make_shared<DataSyncStatus>(ng_id, ng_term, false);
    uint32_t unfinished_task_cnt = 0;
    for (auto &[range_table_name, range_ids] : ranges_in_bucket_snapshot)
    {
        TableType type;
        if (TableName::IsBase(range_table_name.StringView()))
        {
            type = TableType::Primary;
        }
        else if (TableName::IsUniqueSecondary(range_table_name.StringView()))
        {
            type = TableType::UniqueSecondary;
        }
        else
        {
            type = TableType::Secondary;
        }
        TableName table_name(
            range_table_name.StringView(), type, range_table_name.Engine());
        for (int32_t range_id : range_ids)
        {
            uint64_t last_sync_ts = 0;
            auto range_entry =
                GetTableRangeEntryInternal(range_table_name, ng_id, range_id);
            if (range_entry && EnqueueRangeDataSyncTask(table_name,
                                                        ng_id,
                                                        ng_term,
                                                        range_entry,
                                                        data_sync_ts,
                                                        false,
                                                        false,
                                                        last_sync_ts,
                                                        status,
                                                        hres))
            {
                unfinished_task_cnt++;
            }
        }
    }

    assert(!bucket_ids.empty());
    for (auto &table_name : hash_partitioned_tables)
    {
        if (EnqueueDataSyncTaskToCore(
                table_name,
                ng_id,
                ng_term,
                data_sync_ts,
                Sharder::Instance().ShardBucketIdToCoreIdx(
                    bucket_ids[0]),  // all buckets passed in should land on the
                                     // same core
                false,
                false,
                false,
                status,
                hres,
                send_cache_for_migration,
                [bucket_ids](size_t key_hash) -> bool
                {
                    uint16_t bucket_id =
                        Sharder::MapKeyHashToBucketId(key_hash);
                    for (uint16_t target_id : bucket_ids)
                    {
                        if (bucket_id == target_id)
                        {
                            return true;
                        }
                    }
                    return false;
                }))
        {
            unfinished_task_cnt++;
        }
    }

    {
        std::lock_guard<std::mutex> status_lk(status->mux_);
        status->all_task_started_ = true;
        status->unfinished_tasks_ += unfinished_task_cnt;
        status->unfinished_scan_tasks_ += unfinished_task_cnt;
        if (status->unfinished_tasks_ == 0)
        {
            hres->SetFinished();
            return;
        }
        if (status->unfinished_scan_tasks_ == 0)
        {
            FlushCurrentFlushBuffer();
            return;
        }
    }
    data_sync_worker_ctx_.cv_.notify_all();
}

void LocalCcShards::Terminate()
{
    // Terminate the data sync task worker thds.
    data_sync_worker_ctx_.Terminate();

    // Terminate the flush worker thds.
    flush_data_worker_ctx_.Terminate();

    range_split_worker_ctx_.Terminate();

    if (realtime_sampling_)
    {
        statistics_worker_ctx_.Terminate();
    }

    heartbeat_worker_ctx_.Terminate();

    if (!txservice_enable_cache_replacement)
    {
        purge_deleted_worker_ctx_.Terminate();
    }

    if (kickout_data_test_worker_ctx_.worker_num_ > 0)
    {
        kickout_data_test_worker_ctx_.Terminate();
    }

#if defined(WITH_JEMALLOC)
    epoch_worker_ctx_.Terminate();
#endif
}

#if defined(WITH_JEMALLOC)
void LocalCcShards::EpochWorker()
{
    std::unique_lock<std::mutex> lk(epoch_worker_ctx_.mux_);
    using clock = std::chrono::steady_clock;
    auto next = clock::now() + std::chrono::seconds(1);

    while (epoch_worker_ctx_.status_ == WorkerStatus::Active)
    {
        if (epoch_worker_ctx_.cv_.wait_until(lk, next) ==
            std::cv_status::timeout)
        {
            uint64_t epoch = 1;
            mallctl("epoch", nullptr, nullptr, &epoch, sizeof(epoch));
            next += std::chrono::seconds(1);
        }
    }
}
#endif

void LocalCcShards::DataSyncWorker(size_t worker_idx)
{
    std::unique_lock<std::mutex> task_worker_lk(data_sync_worker_ctx_.mux_);
    auto &task_queue = data_sync_task_queue_[worker_idx];
    const size_t core_number = cc_shards_.size();

    if (data_sync_done_.empty())
    {
        data_sync_done_.resize(core_number, false);
    }

    while (true)
    {
        if (data_sync_worker_ctx_.status_ != WorkerStatus::Active)
        {
            break;
        }

        data_sync_worker_ctx_.cv_.wait(
            task_worker_lk,
            [this, &task_queue]
            {
                return !task_queue.empty() ||
                       data_sync_worker_ctx_.status_ != WorkerStatus::Active;
            });

        if (task_queue.empty())
        {
            data_sync_done_[worker_idx] = true;

            bool need_notify = false;
            while (data_sync_done_[last_data_sync_worker_idx_])
            {
                data_sync_done_[last_data_sync_worker_idx_] = false;
                last_data_sync_worker_idx_ =
                    (last_data_sync_worker_idx_ + 1) % core_number;
                need_notify = true;
            }

            if (need_notify)
            {
                data_sync_worker_ctx_.cv_.notify_all();
            }
            continue;
        }

        std::shared_ptr<DataSyncTask> task = task_queue.front();
        task_queue.pop_front();

        if (task->table_name_.IsHashPartitioned())
        {
            data_sync_worker_ctx_.cv_.wait(
                task_worker_lk,
                [this, worker_idx, core_number]
                {
                    return ((worker_idx + core_number -
                             last_data_sync_worker_idx_) %
                            core_number) <
                               scan_concurrency_.load(
                                   std::memory_order_relaxed) ||
                           data_sync_worker_ctx_.status_ !=
                               WorkerStatus::Active;
                });
            task_worker_lk.unlock();
            DataSyncForHashPartition(std::move(task), worker_idx);
            if (FLAGS_report_ckpt)
            {
                ReportHashPartitionCkptHeapUsage();
            }
            task_worker_lk.lock();

            data_sync_done_[worker_idx] = true;
            bool need_notify = false;
            while (data_sync_done_[last_data_sync_worker_idx_])
            {
                data_sync_done_[last_data_sync_worker_idx_] = false;
                last_data_sync_worker_idx_ =
                    (last_data_sync_worker_idx_ + 1) % core_number;
                need_notify = true;
            }

            if (need_notify)
            {
                data_sync_worker_ctx_.cv_.notify_all();
            }
        }
        else
        {
            task_worker_lk.unlock();
            DataSyncForRangePartition(std::move(task), worker_idx);
            task_worker_lk.lock();
        }
    }

    while (!task_queue.empty())
    {
        std::shared_ptr<DataSyncTask> task = task_queue.front();
        task_queue.pop_front();
        task_worker_lk.unlock();
        if (task->table_name_.IsHashPartitioned())
        {
            DataSyncForHashPartition(std::move(task), worker_idx);
        }
        else
        {
            DataSyncForRangePartition(std::move(task), worker_idx);
        }
        task_worker_lk.lock();
    }
}

void LocalCcShards::PostProcessFlushTaskEntries(
    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<txservice::FlushTaskEntry>>>
        &flush_task_entries,
    DataSyncTask::CkptErrorCode ckpt_err)
{
    assert(ckpt_err != DataSyncTask::CkptErrorCode::SCAN_ERROR);

    // collect all tasks that require update slices
    std::vector<FlushTaskEntry *> pending_update_entries;
    // cache node group term to avoid multiple calls to TryPinNodeGroupData
    std::map<NodeGroupId, int64_t> pinned_node_group_terms_;

    for (auto &[table_name, entries] : flush_task_entries)
    {
        for (auto &entry : entries)
        {
            if (entry->data_sync_task_->table_name_.IsHashPartitioned())
            {
                PostProcessHashPartitionDataSyncTask(
                    std::move(entry->data_sync_task_),
                    entry->data_sync_txm_,
                    ckpt_err,
                    false);
            }
            else
            {
                auto &task = entry->data_sync_task_;

                // check node group term
                int64_t ng_term = -1;
                if (pinned_node_group_terms_.count(task->node_group_id_) > 0)
                {
                    ng_term = pinned_node_group_terms_.at(task->node_group_id_);
                }
                else
                {
                    ng_term = Sharder::Instance().TryPinNodeGroupData(
                        task->node_group_id_);
                    pinned_node_group_terms_[task->node_group_id_] = ng_term;
                }

                auto task_ckpt_err = ckpt_err;
                if (ng_term != task->node_group_term_)
                {
                    LOG(ERROR)
                        << "PostProcessDataSyncTask: term mismatch ng#"
                        << task->node_group_id_ << ", leader term: " << ng_term
                        << ", expected term: " << task->node_group_term_;
                    task_ckpt_err = DataSyncTask::CkptErrorCode::TERM_MISMATCH;
                }

                std::unique_lock<bthread::Mutex> flight_task_lk(
                    task->flight_task_mux_);
                int64_t flight_task_cnt = --task->flight_task_cnt_;

                if (task->ckpt_err_ == DataSyncTask::CkptErrorCode::NO_ERROR)
                {
                    task->ckpt_err_ = task_ckpt_err;
                }

                task_ckpt_err = task->ckpt_err_;
                flight_task_lk.unlock();

                // All flush tasks of this datasync task are finished
                // (flight_task_cnt == 0)
                if (flight_task_cnt == 0)
                {
                    if (task_ckpt_err ==
                        DataSyncTask::CkptErrorCode::TERM_MISMATCH)
                    {
                        if (!task->during_split_range_)
                        {
                            // Abort the data sync txm
                            txservice::AbortTx(entry->data_sync_txm_);

                            PopPendingTask(task->node_group_id_,
                                           task->node_group_term_,
                                           task->table_name_,
                                           task->id_);
                        }
                        task->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                        continue;
                    }

                    if (task_ckpt_err == DataSyncTask::CkptErrorCode::NO_ERROR)
                    {
                        pending_update_entries.push_back(entry.get());
                        continue;
                    }
                    else if (task_ckpt_err ==
                             DataSyncTask::CkptErrorCode::FLUSH_ERROR)
                    {
                        assert(task_ckpt_err ==
                               DataSyncTask::CkptErrorCode::FLUSH_ERROR);

                        TableRangeEntry *range_entry = nullptr;
                        const TxKey *start_key = nullptr;
                        const TxKey *end_key = nullptr;
                        if (!task->during_split_range_)
                        {
                            range_entry = const_cast<TableRangeEntry *>(
                                GetTableRangeEntry(task->table_name_,
                                                   task->node_group_id_,
                                                   task->id_));
                        }
                        else
                        {
                            range_entry = task->range_entry_;
                            start_key = &task->start_key_;
                            end_key = &task->end_key_;
                        }
                        assert(range_entry);
                        CcErrorCode err_code = CcErrorCode::DATA_STORE_ERR;
                        // Reset the post ckpt size if flush failed
                        UpdateStoreSlice(task->table_name_,
                                         task->data_sync_ts_,
                                         range_entry,
                                         start_key,
                                         end_key,
                                         false);

                        if (!task->during_split_range_)
                        {
                            // Abort the data sync txm
                            range_entry->UnPinStoreRange();
                            txservice::AbortTx(entry->data_sync_txm_);

                            PopPendingTask(task->node_group_id_,
                                           task->node_group_term_,
                                           task->table_name_,
                                           task->id_);
                        }

                        if (!Sharder::Instance().CheckLeaderTerm(
                                task->node_group_id_, task->node_group_term_))
                        {
                            err_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
                        }

                        task->SetError(err_code);
                    }
                    else
                    {
                        assert(task_ckpt_err ==
                               DataSyncTask::CkptErrorCode::PERSIST_KV_ERROR);
                        if (!task->during_split_range_)
                        {
                            // Abort the data sync txm
                            txservice::AbortTx(entry->data_sync_txm_);
                        }
                        task->SetError(CcErrorCode::DATA_STORE_ERR);
                    }
                }
            }
        }
    }

    if (!pending_update_entries.empty())
    {
        bool success = UpdateStoreSlices(pending_update_entries);

        for (auto &entry : pending_update_entries)
        {
            auto &task = entry->data_sync_task_;
            TableRangeEntry *range_entry = nullptr;
            const TxKey *start_key = nullptr;
            const TxKey *end_key = nullptr;
            if (!task->during_split_range_)
            {
                range_entry = const_cast<TableRangeEntry *>(GetTableRangeEntry(
                    task->table_name_, task->node_group_id_, task->id_));
            }
            else
            {
                range_entry = task->range_entry_;
                start_key = &task->start_key_;
                end_key = &task->end_key_;
            }
            assert(range_entry);

            if (success)
            {
                if (!task->during_split_range_)
                {
                    range_entry->UpdateLastDataSyncTS(task->data_sync_ts_);
                    range_entry->UnPinStoreRange();
                    // Commit the data sync txm
                    txservice::CommitTx(entry->data_sync_txm_);
                    PopPendingTask(task->node_group_id_,
                                   task->node_group_term_,
                                   task->table_name_,
                                   task->id_);
                }

                task->SetFinish();
            }
            else
            {
                CcErrorCode err_code = CcErrorCode::DATA_STORE_ERR;
                // Reset the post ckpt size if flush failed
                UpdateStoreSlice(task->table_name_,
                                 task->data_sync_ts_,
                                 range_entry,
                                 start_key,
                                 end_key,
                                 false);

                if (!task->during_split_range_)
                {
                    // Abort the data sync txm
                    range_entry->UnPinStoreRange();
                    txservice::AbortTx(entry->data_sync_txm_);

                    PopPendingTask(task->node_group_id_,
                                   task->node_group_term_,
                                   task->table_name_,
                                   task->id_);
                }

                if (!Sharder::Instance().CheckLeaderTerm(
                        task->node_group_id_, task->node_group_term_))
                {
                    err_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
                }

                task->SetError(err_code);
            }
        }
    }

    // unpin node group data
    for (const auto &[node_group, term] : pinned_node_group_terms_)
    {
        if (term >= 0)
        {
            Sharder::Instance().UnpinNodeGroupData(node_group);
        }
    }
}

void LocalCcShards::PostProcessRangePartitionDataSyncTask(
    std::shared_ptr<DataSyncTask> task,
    TransactionExecution *data_sync_txm,
    DataSyncTask::CkptErrorCode err,
    bool is_scan_task)
{
    int64_t ng_term =
        Sharder::Instance().TryPinNodeGroupData(task->node_group_id_);
    if (ng_term != task->node_group_term_)
    {
        LOG(ERROR) << "PostProcessDataSyncTask: term mismatch ng#"
                   << task->node_group_id_ << ", leader term: " << ng_term
                   << ", expected term: " << task->node_group_term_;
        err = DataSyncTask::CkptErrorCode::TERM_MISMATCH;
    }
    std::unique_lock<bthread::Mutex> flight_task_lk(task->flight_task_mux_);
    int64_t flight_task_cnt = --task->flight_task_cnt_;

    if (task->ckpt_err_ == DataSyncTask::CkptErrorCode::NO_ERROR)
    {
        task->ckpt_err_ = err;
    }

    auto task_ckpt_err = task->ckpt_err_;
    flight_task_lk.unlock();

    // All flush tasks of this datasync task are finished (flight_task_cnt == 0)
    if (flight_task_cnt == 0)
    {
        if (task_ckpt_err == DataSyncTask::CkptErrorCode::TERM_MISMATCH)
        {
            if (!task->during_split_range_)
            {
                // Abort the data sync txm
                txservice::AbortTx(data_sync_txm);

                PopPendingTask(task->node_group_id_,
                               task->node_group_term_,
                               task->table_name_,
                               task->id_);
            }
            task->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            if (ng_term >= 0)
            {
                Sharder::Instance().UnpinNodeGroupData(task->node_group_id_);
            }
            return;
        }

        TableRangeEntry *range_entry = nullptr;
        const TxKey *start_key = nullptr;
        const TxKey *end_key = nullptr;
        if (!task->during_split_range_)
        {
            range_entry = const_cast<TableRangeEntry *>(GetTableRangeEntry(
                task->table_name_, task->node_group_id_, task->id_));
        }
        else
        {
            range_entry = task->range_entry_;
            start_key = &task->start_key_;
            end_key = &task->end_key_;
        }
        assert(range_entry);

        if (task_ckpt_err == DataSyncTask::CkptErrorCode::NO_ERROR)
        {
            // update Update the slice size.
            while (!UpdateStoreSlice(task->table_name_,
                                     task->data_sync_ts_,
                                     range_entry,
                                     start_key,
                                     end_key,
                                     true))
            {
                // Keep retrying here since we've finished the flush
                // already, it's too expensive to start from the beginning
                // all over again.
                LOG(ERROR) << "DataSync failed to update store slice of range#"
                           << task->id_ << " on table "
                           << task->table_name_.Trace() << ", retrying.";
                std::this_thread::sleep_for(1s);
                if (!Sharder::Instance().CheckLeaderTerm(
                        task->node_group_id_, task->node_group_term_))
                {
                    LOG(ERROR)
                        << "Leader term changed during store slice update";
                    // task->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);

                    task_ckpt_err = DataSyncTask::CkptErrorCode::FLUSH_ERROR;
                    break;
                }
            }
        }

        if (task_ckpt_err == DataSyncTask::CkptErrorCode::NO_ERROR)
        {
            if (!task->during_split_range_)
            {
                range_entry->UpdateLastDataSyncTS(task->data_sync_ts_);
                range_entry->UnPinStoreRange();
                // Commit the data sync txm
                txservice::CommitTx(data_sync_txm);
                PopPendingTask(task->node_group_id_,
                               task->node_group_term_,
                               task->table_name_,
                               task->id_);
            }

            task->SetFinish();
        }
        else if (task_ckpt_err == DataSyncTask::CkptErrorCode::SCAN_ERROR)
        {
            if (!task->during_split_range_)
            {
                range_entry->UnPinStoreRange();
                txservice::AbortTx(data_sync_txm);
            }

            // Should not decrease unfinished scan task in this case since the
            // task is re-added to the queue.
            is_scan_task = false;

            task->ckpt_err_ = DataSyncTask::CkptErrorCode::NO_ERROR;
            std::lock_guard<std::mutex> task_worker_lk(
                data_sync_worker_ctx_.mux_);
            data_sync_task_queue_[task->id_ % data_sync_task_queue_.size()]
                .emplace_front(task);
            data_sync_worker_ctx_.cv_.notify_all();
        }
        else if (task_ckpt_err == DataSyncTask::CkptErrorCode::FLUSH_ERROR)
        {
            assert(task_ckpt_err == DataSyncTask::CkptErrorCode::FLUSH_ERROR);
            CcErrorCode err_code = CcErrorCode::DATA_STORE_ERR;
            // Reset the post ckpt size if flush failed
            UpdateStoreSlice(task->table_name_,
                             task->data_sync_ts_,
                             range_entry,
                             start_key,
                             end_key,
                             false);

            if (!task->during_split_range_)
            {
                // Abort the data sync txm
                range_entry->UnPinStoreRange();
                txservice::AbortTx(data_sync_txm);

                PopPendingTask(task->node_group_id_,
                               task->node_group_term_,
                               task->table_name_,
                               task->id_);
            }

            if (!Sharder::Instance().CheckLeaderTerm(task->node_group_id_,
                                                     task->node_group_term_))
            {
                err_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
            }

            task->SetError(err_code);
        }
        else
        {
            assert(task_ckpt_err ==
                   DataSyncTask::CkptErrorCode::PERSIST_KV_ERROR);
            if (!task->during_split_range_)
            {
                // Abort the data sync txm
                txservice::AbortTx(data_sync_txm);
            }
            task->SetError(CcErrorCode::DATA_STORE_ERR);
        }
    }

    if (is_scan_task)
    {
        task->SetScanTaskFinished();
    }

    if (ng_term > 0)
    {
        Sharder::Instance().UnpinNodeGroupData(task->node_group_id_);
    }
}

void LocalCcShards::DataSyncForRangePartition(
    std::shared_ptr<DataSyncTask> data_sync_task, size_t worker_idx)
{
    // Whether other task worker is processing this table.
    const TableName &table_name = data_sync_task->table_name_;
    uint32_t ng_id = data_sync_task->node_group_id_;
    int64_t expected_ng_term = data_sync_task->node_group_term_;
    int32_t range_id = data_sync_task->id_;
    bool is_dirty = data_sync_task->is_dirty_;
    bool during_split_range = data_sync_task->during_split_range_;
    bool export_base_table_items = data_sync_task->export_base_table_items_;
    uint64_t last_sync_ts = 0;
    int64_t ng_term = -1;
    uint64_t tx_number = 0;
    bool need_process = false;
    TxKey start_tx_key, end_tx_key;
    std::shared_ptr<const TableSchema> table_schema = nullptr;
    uint64_t schema_version = 0;
    TableRangeEntry *range_entry = nullptr;
    TransactionExecution *data_sync_txm = nullptr;
    StoreRange *store_range = nullptr;
    // Guard to unpin node group on finish.
    std::shared_ptr<void> defer_unpin = nullptr;

    if (!during_split_range)
    {
        std::shared_lock<std::shared_mutex> meta_lk(meta_data_mux_);

        TableName range_tbl_name{table_name.StringView(),
                                 TableType::RangePartition,
                                 table_name.Engine()};
        range_entry =
            GetTableRangeEntryInternal(range_tbl_name, ng_id, range_id);
        if (range_entry == nullptr)
        {
            // table dropped
            data_sync_task->SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
            data_sync_task->SetScanTaskFinished();
            ClearAllPendingTasks(ng_id, expected_ng_term, table_name, range_id);
        }
        else
        {
            NodeGroupId range_ng =
                GetRangeOwnerInternal(range_id, ng_id)->BucketOwner();
            if (range_ng == ng_id)
            {
                if (data_sync_task->SyncTsAdjustable())
                {
                    auto task_limiter_key =
                        TaskLimiterKey(ng_id,
                                       expected_ng_term,
                                       table_name.StringView(),
                                       table_name.Type(),
                                       table_name.Engine(),
                                       range_id);
                    std::lock_guard<std::mutex> task_limiter_lk(
                        task_limiter_mux_);
                    auto iter = task_limiters_.find(task_limiter_key);
                    assert(iter != task_limiters_.end());
                    uint64_t latest_pending_task_ts =
                        iter->second->UnsetLatestPendingTs();
                    data_sync_task->data_sync_ts_ = std::max(
                        latest_pending_task_ts, data_sync_task->data_sync_ts_);
                    data_sync_task->UnsetSyncTsAdjustable();
                }

                // For dirty tables (create index in process), data older than
                // last sync ts will be continously written into memory. We
                // cannot rely on last sync ts to determin if there's dirty data
                // that needs to be flushed.
                last_sync_ts = is_dirty ? 0 : range_entry->GetLastSyncTs();
                if (data_sync_task->data_sync_ts_ <= last_sync_ts && !is_dirty)
                {
                    data_sync_task->SetFinish();
                    data_sync_task->SetScanTaskFinished();
                    PopPendingTask(
                        ng_id, expected_ng_term, table_name, range_id);
                    assert(need_process == false);
                }
                else
                {
                    need_process = true;
                }
            }
            else
            {
                // range no longer belong to this ng.
                data_sync_task->SetError(
                    CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                data_sync_task->SetScanTaskFinished();
                PopPendingTask(ng_id, expected_ng_term, table_name, range_id);
            }
        }

        if (!need_process)
        {
            return;
        }

        meta_lk.unlock();

        // Check the leader
        // In order to avoid range entry be dropped by ClearCcNodeGroup, should
        // pin the node group.
        ng_term = Sharder::Instance().TryPinNodeGroupData(ng_id);
        if (ng_term < 0 || ng_term != expected_ng_term)
        {
            LOG(ERROR) << "DataSync: node is not the leader of ng#" << ng_id
                       << " with leader term: " << ng_term
                       << ", and the expected leader term: "
                       << expected_ng_term;

            // Finish this task and notify the caller.
            data_sync_task->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            data_sync_task->SetScanTaskFinished();
            PopPendingTask(ng_id, expected_ng_term, table_name, range_id);

            if (ng_term >= 0)
            {
                Sharder::Instance().UnpinNodeGroupData(ng_id);
            }

            return;
        }

        assert(ng_term == expected_ng_term);
        if (Sharder::Instance().LeaderTerm(ng_id) < 0)
        {
            // node is still candidate leader of node group. Log replay is not
            // finished yet. In this case we can flush data and kickout cce, but
            // we cannot truncate redo log based on this data sync ts since we
            // might miss the data that has not been recovered yet.
            data_sync_task->SetErrorCode(
                CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }

        defer_unpin = std::shared_ptr<void>(
            nullptr,
            [ng_id](void *) { Sharder::Instance().UnpinNodeGroupData(ng_id); });

        // Process this task.
        // 1. Get a new txm and init
        data_sync_txm = txservice::NewTxInit(tx_service_,
                                             IsolationLevel::RepeatableRead,
                                             CcProtocol::Locking,
                                             ng_id);

        if (data_sync_txm == nullptr)
        {
            LOG(ERROR) << "DataSync init data sync transaction failed.";

            std::lock_guard<std::mutex> task_worker_lk(
                data_sync_worker_ctx_.mux_);
            data_sync_task_queue_[worker_idx].emplace_front(data_sync_task);
            data_sync_worker_ctx_.cv_.notify_one();
            return;
        }

        tx_number = data_sync_txm->TxNumber();
        // 2. Issue read catalog tx_request to acquire read lock on catalog
        // cc_entry using base table name, and acquire read lock in one
        // shard is good enough to block schema change.
        // If table_name has been dropped at this point, read lock would
        // not be acquired.
        const TableName base_table_name{table_name.GetBaseTableNameSV(),
                                        TableType::Primary,
                                        table_name.Engine()};

        CatalogKey table_key(base_table_name);
        TxKey tbl_tx_key{&table_key};
        CatalogRecord catalog_rec;

        ReadTxRequest read_req;
        read_req.Set(&catalog_ccm_name,
                     0,
                     &tbl_tx_key,
                     &catalog_rec,
                     false,
                     false,
                     true);
        data_sync_txm->Execute(&read_req);
        read_req.Wait();

        RecordStatus rec_status = read_req.Result().first;

        if (read_req.IsError() || rec_status != RecordStatus::Normal)
        {
            // Use AbortTxRequest to release read lock.
            txservice::AbortTx(data_sync_txm);

            if (rec_status != RecordStatus::Normal)
            {
                LOG(ERROR) << "DataSync try to add read lock on deleted table, "
                              "table name: "
                           << table_key.Name().StringView();
                // If table is deleted(!Normal), skip the table. Return finish
                // directly.
                data_sync_task->SetError();
                data_sync_task->SetScanTaskFinished();

                ClearAllPendingTasks(
                    ng_id, expected_ng_term, table_name, range_id);
            }
            else
            {
                LOG(ERROR) << "DataSync add read lock on table failed, "
                              "table name: "
                           << table_key.Name().StringView();

                // If read lock acquire failed, retry next time.
                // Put back into the beginning.

                std::lock_guard<std::mutex> task_worker_lk(
                    data_sync_worker_ctx_.mux_);
                data_sync_task_queue_[worker_idx].emplace_front(data_sync_task);
                data_sync_worker_ctx_.cv_.notify_one();
            }
            return;
        }

        // Get the table schema. The basic strategy is that, 1) for pk table,
        // must use the current table schema, 2) for the [Unique]secondary
        // table, only in the case that there is no key schema corresponding to
        // the index table in the current table schema, should use the dirty
        // table schema.
        table_schema = catalog_rec.CopySchema();
        if (table_name.Type() == TableType::Secondary ||
            table_name.Type() == TableType::UniqueSecondary)
        {
            if (is_dirty)
            {
                // At the moment when the ckpt worker obtains the table name
                // snapshot, the index table is in dirty state (new index).
                // However, if the alter table transaction has been committed,
                // then at this moment, the index table is no longer in a dirty
                // state.
                if (table_schema->IndexKeySchema(table_name))
                {
                    // The index table is no longer in dirty state.
                    is_dirty = false;
                }
                else if (catalog_rec.DirtySchema())
                {
                    // The base table is in dirty state yet. Try to use the
                    // dirty schema.
                    table_schema = catalog_rec.CopyDirtySchema();
                }
            }
            // For index table, if this table has been dropped, skip it.
            if (!table_schema->IndexKeySchema(table_name))
            {
                // Use CommitTx to release read lock.
                txservice::CommitTx(data_sync_txm);
                LOG(INFO) << "DataSync on the deleted table: "
                          << table_name.Trace() << ". Return finish directly.";

                data_sync_task->SetFinish();
                data_sync_task->SetScanTaskFinished();
                PopPendingTask(ng_id, expected_ng_term, table_name, range_id);

                return;
            }
        }

        // Lock bucket so that bucket cannot be migrated away during data sync.
        uint64_t expected_range_version = data_sync_task->range_version_;
        RangeBucketRecord bucket_rec;
        RangeBucketKey bucket_key(
            Sharder::Instance().MapRangeIdToBucketId(range_id));
        TxKey bucket_tx_key{&bucket_key};
        read_req.Reset();
        read_req.Set(&range_bucket_ccm_name,
                     0,
                     &bucket_tx_key,
                     &bucket_rec,
                     false,
                     false,
                     true);
        data_sync_txm->Execute(&read_req);
        read_req.Wait();

        if (read_req.IsError())
        {
            // Use AbortTxRequest to release read lock.
            LOG(ERROR) << "DataSync add read lock on bucket failed, "
                          "bucket id: "
                       << Sharder::Instance().MapRangeIdToBucketId(range_id);

            txservice::AbortTx(data_sync_txm);
            // If read lock acquire failed, retry next time.
            // Put back into the beginning.
            std::lock_guard<std::mutex> task_worker_lk(
                data_sync_worker_ctx_.mux_);
            data_sync_task_queue_[worker_idx].emplace_front(data_sync_task);
            data_sync_worker_ctx_.cv_.notify_one();
            return;
        }

        // Now that we have acquired read lock on catalog and bucket, there
        // won't be any ddl on this range. Update store_range and check if this
        // range is still owned by this node group.
        range_entry = const_cast<TableRangeEntry *>(
            GetTableRangeEntry(range_tbl_name, ng_id, range_id));
        if (bucket_rec.GetBucketInfo()->BucketOwner() != ng_id)
        {
            assert(range_entry);
            // Skip the range, it might be dropped or migrated away.
            // Use AbortTxRequest to release read lock.
            txservice::AbortTx(data_sync_txm);

            data_sync_task->SetError();
            data_sync_task->SetScanTaskFinished();
            PopPendingTask(ng_id, expected_ng_term, table_name, range_id);

            return;
        }
        else if (range_entry->Version() != expected_range_version)
        {
            // If the range spec has been updated since we create the task,
            // we might miss the data in the new range during data sync scan.
            // So we need to mark this round of data sync as failed.
            LOG(WARNING)
                << "DataSync range version mismatch with data sync ts: "
                << data_sync_task->data_sync_ts_;
            data_sync_task->SetErrorCode(CcErrorCode::GET_RANGE_ID_ERR);
            data_sync_task->SetScanTaskFinished();
        }

        assert(!store_range);
        start_tx_key = range_entry->GetRangeInfo()->StartTxKey();
        end_tx_key = range_entry->GetRangeInfo()->EndTxKey();
    }
    else
    {
        ng_term = data_sync_task->node_group_term_;
        tx_number = data_sync_task->tx_number_;
        start_tx_key = data_sync_task->start_key_.GetShallowCopy();
        end_tx_key = data_sync_task->end_key_.GetShallowCopy();
        table_schema = data_sync_task->table_schema_;
        range_entry = data_sync_task->range_entry_;
        assert(range_entry);
        store_range = range_entry->RangeSlices();
        assert(store_range);

        last_sync_ts = is_dirty ? 0 : range_entry->GetLastSyncTs();
    }

    if (table_name.IsBase())
    {
        schema_version = table_schema->KeySchema()->SchemaTs();
    }
    else
    {
        schema_version = table_schema->IndexKeySchema(table_name)->SchemaTs();
    }

    // Scan the delta slice size
    std::map<TxKey, int64_t> slices_delta_size;
    ScanSliceDeltaSizeCcForRangePartition scan_delta_size_cc(
        table_name,
        last_sync_ts,
        data_sync_task->data_sync_ts_,
        ng_id,
        ng_term,
        cc_shards_.size(),
        tx_number,
        start_tx_key,
        end_tx_key,
        store_range,
        is_dirty,
        schema_version);

    for (size_t i = 0; i < cc_shards_.size(); i++)
    {
        EnqueueLowPriorityCcRequestToShard(i, &scan_delta_size_cc);
    }
    scan_delta_size_cc.Wait();

    if (scan_delta_size_cc.IsError())
    {
        LOG(ERROR) << "DataSync scan slice delta size failed on table "
                   << table_name.StringView() << " with error code: "
                   << static_cast<uint32_t>(scan_delta_size_cc.ErrorCode());

        if (!during_split_range)
        {
            txservice::AbortTx(data_sync_txm);
            if (scan_delta_size_cc.StoreRangePtr())
            {
                range_entry->UnPinStoreRange();
            }
        }
        std::lock_guard<std::mutex> task_worker_lk(data_sync_worker_ctx_.mux_);
        data_sync_task_queue_[worker_idx].emplace_front(data_sync_task);
        data_sync_worker_ctx_.cv_.notify_one();
        return;
    }

    for (size_t i = 0; i < cc_shards_.size(); ++i)
    {
        auto &delta_size = scan_delta_size_cc.SliceDeltaSize(i);
        for (size_t j = 0; j < delta_size.size(); ++j)
        {
            slices_delta_size[std::move(delta_size[j].first)] +=
                delta_size[j].second;
        }
    }

    if (!export_base_table_items && slices_delta_size.size() == 0)
    {
        DLOG(INFO)
            << "No items need to be sync in this round data sync of range#"
            << range_id << " for table: " << table_name.StringView()
            << ". Finish this task directly.";
        if (!during_split_range)
        {
            txservice::CommitTx(data_sync_txm);

            // Update the task status for this range.
            range_entry->UpdateLastDataSyncTS(data_sync_task->data_sync_ts_);
            // Generally, the StoreRange will be pinned only when there are data
            // items in the range that needs to be ckpted.
            if (scan_delta_size_cc.StoreRangePtr() != nullptr)
            {
                // This will happen if ng failover during split range, the
                // recovering split range transaction is finished on the
                // failover node. The dirty data already been persisted, but the
                // before the checkpointer is able to truncate the data log, the
                // prefer leader node is launched, and the data log is replayed
                // on the prefer leader. When the prefer leader node try to
                // persist those dirty data, it need to check the data store
                // size of these data, after load slice, it find that these data
                // are not really dirty, so do not need to export them anymore.
                range_entry->UnPinStoreRange();
            }

            PopPendingTask(ng_id, expected_ng_term, table_name, range_id);
        }
        data_sync_task->SetFinish();
        data_sync_task->SetScanTaskFinished();
        return;
    }
    assert(slices_delta_size.size() > 0 || export_base_table_items);
    store_range = scan_delta_size_cc.StoreRangePtr();
    assert(store_range);

    if (is_dirty && scan_delta_size_cc.HasDmlSinceDdl())
    {
        store_range->SetHasDmlSinceDdl();
    }

    // Update slice post ckpt size.
    UpdateSlicePostCkptSize(store_range, slices_delta_size);

    if (!during_split_range)
    {
        // If the task comes from split range transaction, it is assumed that
        // there will be no further splitting.
        std::vector<TxKey> split_keys;
        bool ret = CalculateRangeUpdate(table_name,
                                        ng_id,
                                        ng_term,
                                        data_sync_task->data_sync_ts_,
                                        store_range,
                                        split_keys);
        if (!ret)
        {
            LOG(ERROR) << "Calculate subranges key failed on table "
                       << table_name.StringView();

            data_sync_task->SetError();
            data_sync_task->SetScanTaskFinished();
            // Handle the pending tasks for the same range
            PopPendingTask(ng_id, expected_ng_term, table_name, range_id);

            range_entry->UnPinStoreRange();
            txservice::AbortTx(data_sync_txm);
            return;
        }

        if (!split_keys.empty())
        {
            std::lock_guard<std::mutex> range_split_worker_lk(
                range_split_worker_ctx_.mux_);

            auto range_split_task =
                std::make_unique<RangeSplitTask>(data_sync_task,
                                                 table_schema,
                                                 std::move(split_keys),
                                                 range_entry,
                                                 data_sync_txm,
                                                 defer_unpin);

            pending_range_split_task_.push_back(std::move(range_split_task));
            range_split_worker_ctx_.cv_.notify_one();
            return;
        }
    }

    // 3. Scan records.
    // The data sync worker thread is the owner of those vectors.

    // Sort output vectors in key sorting order.
    auto key_greater = [](const std::pair<TxKey, int32_t> &r1,
                          const std::pair<TxKey, int32_t> &r2) -> bool
    { return r2.first < r1.first; };
    auto rec_greater = [](const FlushRecord &r1, const FlushRecord &r2) -> bool
    { return r2.Key() < r1.Key(); };

    std::vector<std::vector<FlushRecord>> data_sync_vecs;
    std::vector<std::vector<FlushRecord>> archive_vecs;
    std::vector<std::vector<std::pair<TxKey, int32_t>>> mv_base_vecs;

    // Add an extra vector as a remaining vector to store the remaining keys
    // of the current batch of FlushRecords.
    // DataSyncScanCc request is executed in parallel on all cores. For a
    // batch of scan results, the end keys among the cores are different.
    // In order to ensure the accuracy of the calculated subslice keys, for
    // this batch of FlushRecords, the minimum end key of all cores's scan
    // result is obtained, and the FlushRecords after this key is placed in
    // this remaining vector, which will be merged with the next batch of
    // FlushRecords. For example: core1[10,15,20], core2[8,16,24,32], only
    // [8,10,15,16,20] will be flushed into data store in this round，and
    // the remaining vector stores [24,32]
    for (size_t i = 0; i < (cc_shards_.size() + 1); ++i)
    {
        data_sync_vecs.emplace_back();
        data_sync_vecs.back().reserve(DATA_SYNC_SCAN_BATCH_SIZE);
        archive_vecs.emplace_back();
        archive_vecs.back().reserve(DATA_SYNC_SCAN_BATCH_SIZE);
        mv_base_vecs.emplace_back();
        mv_base_vecs.back().reserve(DATA_SYNC_SCAN_BATCH_SIZE);
    }

    // Scan the FlushRecords.
    // Paused position
    UpdateSliceStatus update_slice_status;
    std::unique_ptr<RangeCacheSender> range_cache_sender = nullptr;
    bool need_send_range_cache = false;
    if (during_split_range)
    {
        int32_t old_range_id = range_entry->GetRangeInfo()->PartitionId();
        NodeGroupId old_range_owner =
            GetRangeOwner(old_range_id, ng_id)->BucketOwner();
        NodeGroupId new_range_owner =
            GetRangeOwner(range_id, ng_id)->BucketOwner();

        need_send_range_cache = new_range_owner != old_range_owner;
        if (need_send_range_cache)
        {
            range_cache_sender = std::make_unique<RangeCacheSender>(
                table_name, start_tx_key, ng_id, range_entry);
            range_cache_sender->closure_vec_ =
                std::make_shared<std::vector<UploadBatchSlicesClosure *>>();
        }
    }

    bool scan_data_drained = false;
    // Note: `DataSyncScanCc` needs to ensure that no two ckpt_rec with the
    // same Key can be generated. Our subsequent algorithms are based on this
    // assumption.

    RangePartitionDataSyncScanCc scan_cc(table_name,
                                         data_sync_task->data_sync_ts_,
                                         ng_id,
                                         ng_term,
                                         cc_shards_.size(),
                                         DATA_SYNC_SCAN_BATCH_SIZE,
                                         tx_number,
                                         &start_tx_key,
                                         &end_tx_key,
                                         last_sync_ts,
                                         export_base_table_items,
                                         false,
                                         store_range,
                                         &slices_delta_size,
                                         schema_version);

    {
        // DataSync Worker will call PostProcessDataSyncTask() to decrement
        // flight task count
        std::lock_guard<bthread::Mutex> flight_task_lk(
            data_sync_task->flight_task_mux_);
        data_sync_task->flight_task_cnt_ += 1;
    }

    while (!scan_data_drained)
    {
        uint32_t core_rand = butil::fast_rand();
        // The scan slice request is dispatched to the first core. The first
        // core tries to pin the slice if necessary and if succeeds, further
        // dispatches the request to remaining cores for parallel scans.
        EnqueueLowPriorityCcRequestToShard(core_rand % cc_shards_.size(),
                                           &scan_cc);
        scan_cc.Wait();

        if (scan_cc.IsError())
        {
            LOG(ERROR) << "DataSync scan FlushRecords failed of range#"
                       << range_id << " on table: " << table_name.StringView()
                       << " with error code: "
                       << static_cast<uint32_t>(scan_cc.ErrorCode());

            PostProcessRangePartitionDataSyncTask(
                std::move(data_sync_task),
                data_sync_txm,
                DataSyncTask::CkptErrorCode::SCAN_ERROR);
            return;
        }
        else
        {
            scan_data_drained = true;
            assert(scan_cc.accumulated_flush_data_size_.size() ==
                   cc_shards_.size());
            uint64_t flush_data_size = 0;
            for (size_t flush_data_size_per_core :
                 scan_cc.accumulated_flush_data_size_)
            {
                flush_data_size += flush_data_size_per_core;
            }

            // The cost of FlushRecord also needs to be considered.
            for (size_t i = 0; i < cc_shards_.size(); ++i)
            {
#ifdef WITH_JEMALLOC
                flush_data_size +=
                    (scan_cc.DataSyncVec(i).size() * sizeof(FlushRecord) +
                     scan_cc.ArchiveVec(i).size() * sizeof(FlushRecord) +
                     scan_cc.MoveBaseIdxVec(i).size() *
                         sizeof(std::pair<TxKey, int32_t>));
#else
                // Check if vectors are empty before calling malloc_usable_size
                // to avoid SEGV on nullptr or invalid pointers.
                // Use malloc_usable_size when ASan is enabled (vectors may be
                // allocated by ASan's allocator), otherwise use
                // mi_malloc_usable_size for mimalloc-allocated memory.
                auto &data_sync_vec_ref = scan_cc.DataSyncVec(i);
                auto &archive_vec_ref = scan_cc.ArchiveVec(i);
                auto &move_base_idx_vec_ref = scan_cc.MoveBaseIdxVec(i);

#ifdef __SANITIZE_ADDRESS__
                // When ASan is enabled, use standard malloc_usable_size
                flush_data_size +=
                    (data_sync_vec_ref.empty()
                         ? 0
                         : malloc_usable_size(data_sync_vec_ref.data())) +
                    (archive_vec_ref.empty()
                         ? 0
                         : malloc_usable_size(archive_vec_ref.data())) +
                    (move_base_idx_vec_ref.empty()
                         ? 0
                         : malloc_usable_size(move_base_idx_vec_ref.data()));
#else
                // When ASan is not enabled, use mimalloc's API
                flush_data_size +=
                    (data_sync_vec_ref.empty()
                         ? 0
                         : mi_malloc_usable_size(data_sync_vec_ref.data())) +
                    (archive_vec_ref.empty()
                         ? 0
                         : mi_malloc_usable_size(archive_vec_ref.data())) +
                    (move_base_idx_vec_ref.empty()
                         ? 0
                         : mi_malloc_usable_size(move_base_idx_vec_ref.data()));
#endif
#endif
            }

            // This thread will wait in AllocatePendingFlushDataMemQuota if
            // quota is not available
            uint64_t old_usage =
                data_sync_mem_controller_.AllocateFlushDataMemQuota(
                    flush_data_size);

            DLOG(INFO) << "AllocateFlushDataMemQuota old_usage: " << old_usage
                       << " new_usage: " << old_usage + flush_data_size
                       << " quota: "
                       << data_sync_mem_controller_.FlushMemoryQuota()
                       << " flight_tasks: " << data_sync_task->flight_task_cnt_
                       << " of range: " << range_id
                       << " for table: " << table_name.StringView();

            // The minimum end key of this batch data between all the cores.
            TxKey min_scanned_end_key =
                GetCatalogFactory(table_name.Engine())->PositiveInfKey();
            for (size_t i = 0; i < cc_shards_.size(); ++i)
            {
                for (size_t j = 0; j < scan_cc.accumulated_scan_cnt_[i]; ++j)
                {
                    auto &rec = scan_cc.DataSyncVec(i)[j];
                    // Clone key
                    data_sync_vecs[i].emplace_back(
                        rec.Key().Clone(),
                        rec.ReleaseVersionedPayload(),
                        rec.payload_status_,
                        rec.commit_ts_,
                        rec.cce_,
                        rec.post_flush_size_,
                        range_id);
                }

                // Get the minimum end key.
                if (!data_sync_vecs[i].empty() &&
                    data_sync_vecs[i].back().Key() < min_scanned_end_key)
                {
                    min_scanned_end_key = data_sync_vecs[i].back().Key();
                }

                for (size_t j = 0; j < scan_cc.ArchiveVec(i).size(); ++j)
                {
                    auto &rec = scan_cc.ArchiveVec(i)[j];
                    rec.SetKey(data_sync_vecs[i][rec.GetKeyIndex()].Key());
                }

                for (size_t j = 0; j < scan_cc.MoveBaseIdxVec(i).size(); ++j)
                {
                    size_t key_idx = scan_cc.MoveBaseIdxVec(i)[j];
                    TxKey key_raw = data_sync_vecs[i][key_idx].Key();
                    mv_base_vecs[i].emplace_back(std::move(key_raw), range_id);
                }

                // Move the bucket into the tank
                std::move(scan_cc.ArchiveVec(i).begin(),
                          scan_cc.ArchiveVec(i).end(),
                          std::back_inserter(archive_vecs.at(i)));

                scan_data_drained = scan_cc.IsDrained(i) && scan_data_drained;
            }

            std::unique_ptr<std::vector<FlushRecord>> data_sync_vec =
                std::make_unique<std::vector<FlushRecord>>();
            std::unique_ptr<std::vector<FlushRecord>> archive_vec =
                std::make_unique<std::vector<FlushRecord>>();
            std::unique_ptr<std::vector<std::pair<TxKey, int32_t>>>
                mv_base_vec =
                    std::make_unique<std::vector<std::pair<TxKey, int32_t>>>();

            MergeSortedVectors(
                std::move(mv_base_vecs), *mv_base_vec, key_greater, false);

            // Set the ckpt_ts_ of a cc entry repeatedly, which might cause the
            // ccentry become invalid in between. But, there should be no
            // duplication here. we don't need to remove duplicate record.
            MergeSortedVectors(
                std::move(data_sync_vecs), *data_sync_vec, rec_greater, false);

            // For archive vec we don't need to worry about duplicate causing
            // issue since we're not visiting their cc entry. Also we cannot
            // rely on key compare to dedup archive vec since a key could have
            // multiple version of archive versions.
            MergeSortedVectors(
                std::move(archive_vecs), *archive_vec, rec_greater, false);

            data_sync_vecs.resize(cc_shards_.size() + 1);
            archive_vecs.resize(cc_shards_.size() + 1);
            mv_base_vecs.resize(cc_shards_.size() + 1);
            for (size_t i = 0; i <= cc_shards_.size(); ++i)
            {
                data_sync_vecs.at(i).clear();
                archive_vecs.at(i).clear();
                mv_base_vecs.at(i).clear();
            }

            size_t data_sync_vec_size = data_sync_vec->size();
            // Fix the vector of FlushRecords.
            if (!scan_data_drained)
            {
                // Only flush the keys that are not greater than the
                // min_scanned_end_key
                auto iter = std::upper_bound(
                    data_sync_vec->begin(),
                    data_sync_vec->end(),
                    min_scanned_end_key,
                    [](const TxKey &key, const FlushRecord &rec)
                    { return key < rec.Key(); });

                auto &remaining_vec = data_sync_vecs[cc_shards_.size()];
                remaining_vec.clear();
                remaining_vec.insert(
                    remaining_vec.begin(),
                    std::make_move_iterator(iter),
                    std::make_move_iterator(data_sync_vec->end()));
                data_sync_vec->erase(iter, data_sync_vec->end());

                // archive vector
                auto archive_iter = std::upper_bound(
                    archive_vec->begin(),
                    archive_vec->end(),
                    min_scanned_end_key,
                    [](const TxKey &key, const FlushRecord &rec)
                    { return key < rec.Key(); });
                auto &archive_remaining_vec = archive_vecs[cc_shards_.size()];
                archive_remaining_vec.clear();
                archive_remaining_vec.insert(
                    archive_remaining_vec.begin(),
                    std::make_move_iterator(archive_iter),
                    std::make_move_iterator(archive_vec->end()));
                archive_vec->erase(archive_iter, archive_vec->end());

                // mv base vector
                auto mv_base_iter = std::upper_bound(
                    mv_base_vec->begin(),
                    mv_base_vec->end(),
                    min_scanned_end_key,
                    [](const TxKey &t_key,
                       const std::pair<TxKey, int32_t> &key_and_partition_id)
                    { return t_key < key_and_partition_id.first; });
                auto &mv_base_remaining_vec = mv_base_vecs[cc_shards_.size()];
                mv_base_remaining_vec.clear();
                mv_base_remaining_vec.insert(
                    mv_base_remaining_vec.begin(),
                    std::make_move_iterator(mv_base_iter),
                    std::make_move_iterator(mv_base_vec->end()));
                mv_base_vec->erase(mv_base_iter, mv_base_vec->end());
            }

            if (data_sync_vec->empty())
            {
                LOG(WARNING) << "data_sync_vec becomes empty after erase, old "
                                "size of data_sync_vec_size: "
                             << data_sync_vec_size;
                // Reset
                scan_cc.Reset();
                // Return the quota to flush data memory usage pool since the
                // flush data task is not put into flush worker.
                data_sync_mem_controller_.DeallocateFlushMemQuota(
                    flush_data_size);
                continue;
            }

            // Updata slices for this batch records.
            UpdateSlices(table_name,
                         table_schema.get(),
                         store_range,
                         scan_data_drained,
                         *data_sync_vec,
                         update_slice_status);

            if (need_send_range_cache)
            {
                range_cache_sender->FindSliceKeys(*data_sync_vec,
                                                  update_slice_status);
                // Upload this batch records of split out ranges to their future
                // owner for caching.
                range_cache_sender->AppendSliceDataRequest(*data_sync_vec,
                                                           scan_data_drained);
            }

            // Remove the keys that no need to be flush. This is the case that
            // the slice need to split, and we just use those keys to calculate
            // the subslice keys.
            if (!export_base_table_items)
            {
                auto it = data_sync_vec->begin();
                auto flush_it = data_sync_vec->begin();
                for (; it != data_sync_vec->end(); ++it)
                {
                    if (it->cce_)
                    {
                        *flush_it = std::move(*it);
                        ++flush_it;
                    }
                }
                data_sync_vec->erase(flush_it, data_sync_vec->end());
            }

            // Send this flush task to flush worker.
            {
                std::unique_lock<bthread::Mutex> flight_task_lk(
                    data_sync_task->flight_task_mux_);
                if (data_sync_task->ckpt_err_ ==
                    DataSyncTask::CkptErrorCode::FLUSH_ERROR)
                {
                    // Terminate this datasync task immediately if the former
                    // flush task failed.
                    // Return the quota to flush data memory usage pool since
                    // the flush data task is not put into flush worker.
                    data_sync_mem_controller_.DeallocateFlushMemQuota(
                        flush_data_size);
                    break;
                }

                // Flush worker will call PostProcessDataSyncTask() to decrement
                // flight task count.
                data_sync_task->flight_task_cnt_ += 1;
            }

            AddFlushTaskEntry(
                std::make_unique<FlushTaskEntry>(std::move(data_sync_vec),
                                                 std::move(archive_vec),
                                                 std::move(mv_base_vec),
                                                 data_sync_txm,
                                                 data_sync_task,
                                                 table_schema,
                                                 flush_data_size));

            for (size_t i = 0; i < cc_shards_.size(); ++i)
            {
                if (scan_cc.scan_heap_is_full_[i] == 1)
                {
                    // Clear the FlushRecords' memory of scan cc since the
                    // DataSyncScan heap is full.
                    auto &data_sync_vec_ref = scan_cc.DataSyncVec(i);
                    auto &archive_vec_ref = scan_cc.ArchiveVec(i);
                    ReleaseDataSyncScanHeapCc release_scan_heap_cc(
                        &data_sync_vec_ref, &archive_vec_ref);
                    EnqueueLowPriorityCcRequestToShard(i,
                                                       &release_scan_heap_cc);
                    release_scan_heap_cc.Wait();
                }
            }
            // Reset
            scan_cc.Reset();
        }
    }

    if (need_send_range_cache)
    {
        assert(range_cache_sender);
        // Send the cache sending request.
        range_cache_sender->SendRangeCacheRequest(start_tx_key, end_tx_key);
    }

    // Release scan heap memory after scan finish.
    std::list<ReleaseDataSyncScanHeapCc> req_vec;
    for (size_t core_idx = 0; core_idx < Count(); ++core_idx)
    {
        auto &data_sync_vec_ref = scan_cc.DataSyncVec(core_idx);
        auto &archive_vec_ref = scan_cc.ArchiveVec(core_idx);
        req_vec.emplace_back(&data_sync_vec_ref, &archive_vec_ref);
        EnqueueLowPriorityCcRequestToShard(core_idx, &req_vec.back());
    }
    while (req_vec.size() > 0)
    {
        req_vec.back().Wait();
        req_vec.pop_back();
    }

    PostProcessRangePartitionDataSyncTask(std::move(data_sync_task),
                                          data_sync_txm,
                                          DataSyncTask::CkptErrorCode::NO_ERROR,
                                          true);
}

void LocalCcShards::PostProcessHashPartitionDataSyncTask(
    std::shared_ptr<DataSyncTask> task,
    TransactionExecution *data_sync_txm,
    DataSyncTask::CkptErrorCode err,
    bool is_scan_task)
{
    std::unique_lock<bthread::Mutex> flight_task_lk(task->flight_task_mux_);
    int64_t flight_task_cnt = --task->flight_task_cnt_;

    if (task->ckpt_err_ == DataSyncTask::CkptErrorCode::NO_ERROR)
    {
        task->ckpt_err_ = err;
    }

    auto task_ckpt_err = task->ckpt_err_;

    flight_task_lk.unlock();

    // All flush tasks of this task are finished (flight_task_cnt == 0)
    if (flight_task_cnt == 0)
    {
        if (task_ckpt_err == DataSyncTask::CkptErrorCode::NO_ERROR)
        {
            CcErrorCode err_code = CcErrorCode::NO_ERROR;
            if (task->data_sync_ts_ != UINT64_MAX && !task->filter_lambda_)
            {
                std::shared_lock<std::shared_mutex> meta_data_lk(
                    meta_data_mux_);
                // Make sure that the term has not changed so that catalog entry
                // is still valid.
                if (!Sharder::Instance().CheckLeaderTerm(
                        task->node_group_id_, task->node_group_term_))
                {
                    err_code = CcErrorCode::NG_TERM_CHANGED;
                }
                else
                {
                    bool has_skipped_entries = false;
                    {
                        std::lock_guard status_lk(task->status_->mux_);
                        has_skipped_entries =
                            task->status_->has_skipped_entries_;
                    }
                    if (!has_skipped_entries)
                    {
                        LOG(INFO)
                            << "DataSyncScan with ts: " << task->data_sync_ts_
                            << " flushed all entries, "
                               "catalog_entry->UpdateLastDataSyncTS";
                        const TableName base_table_name{
                            task->table_name_.GetBaseTableNameSV(),
                            TableType::Primary,
                            task->table_name_.Engine()};
                        CatalogEntry *catalog_entry = GetCatalogInternal(
                            base_table_name, task->node_group_id_);
                        // We're still holding catalog read lock here, and the
                        // term has not changed. catalog entry should not be
                        // nullptr.
                        assert(catalog_entry);
                        catalog_entry->UpdateLastDataSyncTS(task->data_sync_ts_,
                                                            task->id_);
                    }
                    else
                    {
                        LOG(INFO)
                            << "DataSyncScan with ts: " << task->data_sync_ts_
                            << " has skipped entries, do not "
                               "UpdateLastDataSyncTS";
                    }
                }
            }

            if (err_code != CcErrorCode::NO_ERROR)
            {
                AbortTx(data_sync_txm);
            }
            else
            {
                // Commit the data sync txm
                CommitTx(data_sync_txm);
            }

            PopPendingTask(task->node_group_id_,
                           task->node_group_term_,
                           task->table_name_,
                           task->id_);

            if (err_code == CcErrorCode::NO_ERROR)
            {
                task->SetFinish();
            }
            else
            {
                task->SetError(err_code);
            }
        }
        else if (task_ckpt_err == DataSyncTask::CkptErrorCode::SCAN_ERROR)
        {
            txservice::AbortTx(data_sync_txm);

            // reset error code
            task->ckpt_err_ = DataSyncTask::CkptErrorCode::NO_ERROR;
            std::lock_guard<std::mutex> task_worker_lk(
                data_sync_worker_ctx_.mux_);
            data_sync_task_queue_[task->id_].emplace_front(task);
            data_sync_worker_ctx_.cv_.notify_all();
        }
        else if (task_ckpt_err == DataSyncTask::CkptErrorCode::FLUSH_ERROR)
        {
            assert(task_ckpt_err == DataSyncTask::CkptErrorCode::FLUSH_ERROR);
            CcErrorCode err_code = CcErrorCode::DATA_STORE_ERR;
            if (task->is_standby_node_ckpt_)
            {
                if (Sharder::Instance().LeaderTerm(task->node_group_id_) !=
                    task->node_group_term_)
                {
                    err_code = CcErrorCode::NG_TERM_CHANGED;
                }
            }
            else
            {
                if (Sharder::Instance().StandbyNodeTerm() !=
                    task->node_group_term_)
                {
                    err_code = CcErrorCode::NG_TERM_CHANGED;
                }
            }

            txservice::AbortTx(data_sync_txm);
            task->SetError(err_code);

            PopPendingTask(task->node_group_id_,
                           task->node_group_term_,
                           task->table_name_,
                           task->id_);
        }
        else
        {
            assert(task_ckpt_err ==
                   DataSyncTask::CkptErrorCode::PERSIST_KV_ERROR);
            txservice::AbortTx(data_sync_txm);
            task->SetError(CcErrorCode::DATA_STORE_ERR);
        }
    }
    if (is_scan_task)
    {
        task->SetScanTaskFinished();
    }
}

void LocalCcShards::DataSyncForHashPartition(
    std::shared_ptr<DataSyncTask> data_sync_task, size_t worker_idx)
{
    // Whether other task worker is processing this table.
    const TableName &table_name = data_sync_task->table_name_;
    uint32_t ng_id = data_sync_task->node_group_id_;
    int64_t expected_ng_term = data_sync_task->node_group_term_;
    bool is_dirty = data_sync_task->is_dirty_;

    std::shared_lock<std::shared_mutex> meta_lk(meta_data_mux_);
    uint64_t last_sync_ts = 0;
    bool need_process = false;

    const TableName primary_base_table_name{table_name.GetBaseTableNameSV(),
                                            TableType::Primary,
                                            table_name.Engine()};
    CatalogEntry *catalog_entry =
        GetCatalogInternal(primary_base_table_name, ng_id);

    if (catalog_entry == nullptr)
    {
        data_sync_task->SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
        data_sync_task->SetScanTaskFinished();
        ClearAllPendingTasks(ng_id, expected_ng_term, table_name, worker_idx);
    }
    else
    {
        if (data_sync_task->SyncTsAdjustable())
        {
            auto task_limiter_key = TaskLimiterKey(ng_id,
                                                   expected_ng_term,
                                                   table_name.StringView(),
                                                   table_name.Type(),
                                                   table_name.Engine(),
                                                   worker_idx);
            std::lock_guard<std::mutex> task_limiter_lk(task_limiter_mux_);
            auto iter = task_limiters_.find(task_limiter_key);
            assert(iter != task_limiters_.end());
            // Now, This task is runing. we call `UnsetLatestPendingTs` to make
            // anothe task could push to PendingTaskQueue.
            uint64_t lateset_pending_task_ts =
                iter->second->UnsetLatestPendingTs();
            data_sync_task->data_sync_ts_ = std::max(
                lateset_pending_task_ts, data_sync_task->data_sync_ts_);
            data_sync_task->UnsetSyncTsAdjustable();
        }

        // For dirty tables (create index in process), data older than
        // last sync ts will be continously written into memory. We cannot
        // rely on last sync ts to determin if there's dirty data that needs
        // to be flushed.
        last_sync_ts = is_dirty ? 0 : catalog_entry->GetLastSyncTs(worker_idx);
        if (data_sync_task->data_sync_ts_ <= last_sync_ts && !is_dirty)
        {
            data_sync_task->SetFinish();
            data_sync_task->SetScanTaskFinished();

            PopPendingTask(ng_id, expected_ng_term, table_name, worker_idx);

            assert(need_process == false);
        }
        else
        {
            need_process = true;
        }
    }

    if (!need_process)
    {
        return;
    }

    meta_lk.unlock();

    int64_t ng_term = -1;
    if (data_sync_task->is_standby_node_ckpt_)
    {
        assert(data_sync_task->node_group_id_ ==
               Sharder::Instance().NativeNodeGroup());
        ng_term = Sharder::Instance().StandbyNodeTerm();
    }
    else
    {
        assert(!data_sync_task->is_standby_node_ckpt_);
        int64_t ng_candidate_leader_term =
            Sharder::Instance().CandidateLeaderTerm(ng_id);
        int64_t ng_leader_term = Sharder::Instance().LeaderTerm(ng_id);
        ng_term = std::max(ng_candidate_leader_term, ng_leader_term);

        if (ng_term >= 0 && ng_leader_term < 0)
        {
            // node is still candidate leader of node group. Log replay is not
            // finished yet. In this case we can flush data and kickout cce, but
            // we cannot truncate redo log based on this data sync ts since we
            // might miss the data that has not been recovered yet.
            data_sync_task->SetErrorCode(
                CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }
    }

    if (ng_term < 0 || ng_term != expected_ng_term)
    {
        LOG(ERROR) << "DataSync: node is not the leader of ng#" << ng_id
                   << " with leader term: " << ng_term
                   << ", and the expected leader term: " << expected_ng_term;
        // Finish this task and notify the caller.
        data_sync_task->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        data_sync_task->SetScanTaskFinished();

        ClearAllPendingTasks(ng_id, expected_ng_term, table_name, worker_idx);

        return;
    }

    assert(ng_term == expected_ng_term);

    // Process this task.
    // 1. Get a new txm and init
    TransactionExecution *data_sync_txm =
        txservice::NewTxInit(tx_service_,
                             IsolationLevel::RepeatableRead,
                             CcProtocol::Locking,
                             ng_id);

    if (data_sync_txm == nullptr)
    {
        LOG(ERROR) << "DataSync init data sync transaction failed.";

        std::lock_guard<std::mutex> task_worker_lk(data_sync_worker_ctx_.mux_);
        data_sync_task_queue_[worker_idx].emplace_front(data_sync_task);
        data_sync_worker_ctx_.cv_.notify_all();
        return;
    }

    // 2. Issue read catalog tx_request to acquire read lock on catalog
    // cc_entry using base table name, and acquire read lock in one
    // shard is good enough to block schema change.
    // If table_name has been dropped at this point, read lock would
    // not be acquired.
    const TableName base_table_name{table_name.GetBaseTableNameSV(),
                                    TableType::Primary,
                                    table_name.Engine()};

    CatalogKey table_key(base_table_name);
    TxKey tbl_tx_key(&table_key);
    CatalogRecord catalog_rec;

    ReadTxRequest read_req;
    read_req.Set(
        &catalog_ccm_name, 0, &tbl_tx_key, &catalog_rec, false, false, true);
    data_sync_txm->Execute(&read_req);
    read_req.Wait();

    RecordStatus rec_status = read_req.Result().first;

    if (read_req.IsError() || rec_status != RecordStatus::Normal)
    {
        // Use AbortTxRequest to release read lock.
        txservice::AbortTx(data_sync_txm);

        if (rec_status != RecordStatus::Normal)
        {
            LOG(ERROR) << "DataSync try to add read lock on deleted table, "
                          "table name: "
                       << table_key.Name().StringView();

            // If table is deleted(!Normal), skip the table. Return finish
            // directly.
            data_sync_task->SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
            data_sync_task->SetScanTaskFinished();

            ClearAllPendingTasks(
                ng_id, expected_ng_term, table_name, worker_idx);
        }
        else
        {
            LOG(ERROR) << "DataSync add read lock on table failed, "
                          "table name: "
                       << table_key.Name().StringView();

            // If read lock acquire failed, retry next time.
            // Put back into the beginning.
            std::lock_guard<std::mutex> task_worker_lk(
                data_sync_worker_ctx_.mux_);
            data_sync_task_queue_[worker_idx].emplace_front(data_sync_task);
            data_sync_worker_ctx_.cv_.notify_all();
        }
        return;
    }

    assert(table_name.Type() == TableType::Primary);

    auto core_number = cc_shards_.size();
    auto approximate_partition_number_this_node = std::max(
        1U, total_hash_partitions / Sharder::Instance().NodeGroupCount());
    auto approximate_partition_number_this_core =
        std::max(1UL, approximate_partition_number_this_node / core_number);
    const size_t flush_buffer_size = cur_flush_buffer_.GetFlushBufferSize();
    constexpr size_t default_data_file_size = 8ULL * 1024 * 1024;
    const size_t scan_concurrency =
        flush_buffer_size /
        (default_data_file_size * approximate_partition_number_this_core);
    if (scan_concurrency > 0)
    {
        bool need_notify = scan_concurrency >
                           scan_concurrency_.load(std::memory_order_acquire);
        scan_concurrency_.store(scan_concurrency, std::memory_order_relaxed);
        if (need_notify)
        {
            std::lock_guard<std::mutex> lk(data_sync_worker_ctx_.mux_);
            data_sync_worker_ctx_.cv_.notify_all();
        }
    }

    // 3. Scan records.
    {
        std::lock_guard<bthread::Mutex> flight_task_lk(
            data_sync_task->flight_task_mux_);
        data_sync_task->flight_task_cnt_ += 1;
    }

    HashPartitionDataSyncScanCc scan_cc(table_name,
                                        data_sync_task->data_sync_ts_,
                                        ng_id,
                                        ng_term,
                                        DATA_SYNC_SCAN_BATCH_SIZE,
                                        data_sync_txm->TxNumber(),
                                        data_sync_task->forward_cache_,
                                        last_sync_ts,
                                        data_sync_task->filter_lambda_,
                                        catalog_rec.Schema()->Version());
    bool scan_data_drained = false;

    auto data_sync_vec = std::make_unique<std::vector<FlushRecord>>();
    auto archive_vec = std::make_unique<std::vector<FlushRecord>>();
    auto mv_base_vec =
        std::make_unique<std::vector<std::pair<TxKey, int32_t>>>();
    uint64_t vec_mem_usage = 0;

    // Note: `DataSyncScanCc` needs to ensure that no two ckpt_rec with the
    // same Key can be generated. Our subsequent algorithms are based on
    // this assumption.

    assert(worker_idx < cc_shards_.size());

    while (!scan_data_drained)
    {
        EnqueueLowPriorityCcRequestToShard(worker_idx, &scan_cc);
        scan_cc.Wait();

        if (scan_cc.IsError() &&
            scan_cc.ErrorCode() != CcErrorCode::LOG_NOT_TRUNCATABLE)
        {
            LOG(ERROR) << "DataSync scan failed on table "
                       << table_name.StringView() << " with error code: "
                       << static_cast<int>(scan_cc.ErrorCode());

            PostProcessHashPartitionDataSyncTask(
                std::move(data_sync_task),
                data_sync_txm,
                DataSyncTask::CkptErrorCode::SCAN_ERROR);

            return;
        }
        if (scan_cc.ErrorCode() == CcErrorCode::LOG_NOT_TRUNCATABLE)
        {
            data_sync_task->status_->SetEntriesSkippedAndNoTruncateLog();
        }

        scan_data_drained = true;

        // Send cache to target node group if needed.
        if (data_sync_task->forward_cache_)
        {
            std::shared_lock<std::shared_mutex> meta_lk(meta_data_mux_);
            const auto bucket_infos = GetAllBucketInfosNoLocking(ng_id);
            if (bucket_infos == nullptr)
            {
                // no longer node group owner, abort the task
                LOG(ERROR) << "DataSync: Failed to get bucket infos for "
                              "ng#"
                           << ng_id;
                PostProcessHashPartitionDataSyncTask(
                    std::move(data_sync_task),
                    data_sync_txm,
                    DataSyncTask::CkptErrorCode::SCAN_ERROR);
                return;
            }

            std::unordered_map<NodeGroupId, UploadBatchClosure *>
                send_cache_closures;
            for (size_t idx = 0; idx < scan_cc.accumulated_scan_cnt_; idx++)
            {
                FlushRecord &ref = scan_cc.DataSyncVec()[idx];
                uint16_t bucket_id =
                    Sharder::MapKeyHashToBucketId(ref.Key().Hash());
                NodeGroupId dest_ng =
                    bucket_infos->at(bucket_id)->DirtyBucketOwner();
                assert(dest_ng != UINT32_MAX);

                // Put the record into the request for this node group.
                auto ins_res = send_cache_closures.try_emplace(dest_ng);
                remote::UploadBatchRequest *req_ptr = nullptr;
                if (ins_res.second)
                {
                    uint32_t node_id =
                        Sharder::Instance().LeaderNodeId(dest_ng);
                    std::shared_ptr<brpc::Channel> channel =
                        Sharder::Instance().GetCcNodeServiceChannel(node_id);
                    if (channel == nullptr)
                    {
                        // Fail to establish the channel to the tx node.
                        // Just skip the cache sending since it is a best
                        // effort try to performance improvement.
                        LOG(ERROR) << "UploadBatch: Failed to init the "
                                      "channel of ng#"
                                   << dest_ng;
                        send_cache_closures.erase(ins_res.first);
                    }
                    else
                    {
                        // Create a closure for the first time.
                        UploadBatchClosure *upload_batch_closure =
                            new UploadBatchClosure(
                                [this, data_sync_task, data_sync_txm](
                                    CcErrorCode res_code, int32_t dest_ng_term)
                                {
                                    // We don't care if
                                    // the cache send
                                    // was succeed or
                                    // not since it's a
                                    // best effort
                                    // move. Just pass in no error
                                    // so that it won't cause data sync
                                    // failure.
                                    PostProcessHashPartitionDataSyncTask(
                                        std::move(data_sync_task),
                                        data_sync_txm,
                                        DataSyncTask::CkptErrorCode::NO_ERROR);
                                },
                                10000,
                                false);

                        upload_batch_closure->SetChannel(node_id, channel);

                        ins_res.first->second = upload_batch_closure;
                        req_ptr = upload_batch_closure->UploadBatchRequest();
                        req_ptr->set_node_group_id(dest_ng);
                        req_ptr->set_node_group_term(-1);
                        req_ptr->set_table_name_str(table_name.String());
                        req_ptr->set_table_type(
                            remote::ToRemoteType::ConvertTableType(
                                table_name.Type()));
                        req_ptr->set_table_engine(
                            remote::ToRemoteType::ConvertTableEngine(
                                table_name.Engine()));
                        req_ptr->set_kind(
                            remote::UploadBatchKind::DIRTY_BUCKET_DATA);
                        req_ptr->set_batch_size(0);
                        // keys
                        req_ptr->clear_keys();
                        // records
                        req_ptr->clear_records();
                        // commit_ts
                        req_ptr->clear_commit_ts();
                        // rec_status
                        req_ptr->clear_rec_status();
                    }
                }
                else
                {
                    req_ptr = ins_res.first->second->UploadBatchRequest();
                }

                if (req_ptr)
                {
                    std::string *keys_str = req_ptr->mutable_keys();
                    std::string *rec_status_str = req_ptr->mutable_rec_status();
                    std::string *commit_ts_str = req_ptr->mutable_commit_ts();
                    size_t len_sizeof = sizeof(uint64_t);
                    const char *val_ptr = nullptr;
                    ref.Key().Serialize(*keys_str);
                    if (ref.payload_status_ == RecordStatus::Normal)
                    {
                        std::string *recs_str = req_ptr->mutable_records();
                        ref.Payload()->Serialize(*recs_str);
                    }
                    const char *status_ptr =
                        reinterpret_cast<const char *>(&(ref.payload_status_));
                    rec_status_str->append(status_ptr, sizeof(RecordStatus));
                    val_ptr = reinterpret_cast<const char *>(&(ref.commit_ts_));
                    commit_ts_str->append(val_ptr, len_sizeof);
                    req_ptr->set_batch_size(req_ptr->batch_size() + 1);
                }
            }
            meta_lk.unlock();

            {
                std::unique_lock<bthread::Mutex> flight_lk(
                    data_sync_task->flight_task_mux_);
                data_sync_task->flight_task_cnt_ += send_cache_closures.size();
            }
            // Send cache to target node groups.
            for (auto &[ng, upload_batch_closure] : send_cache_closures)
            {
                remote::CcRpcService_Stub stub(upload_batch_closure->Channel());
                brpc::Controller *cntl_ptr = upload_batch_closure->Controller();
                cntl_ptr->set_timeout_ms(10000);
                // Asynchronous mode
                stub.UploadBatch(upload_batch_closure->Controller(),
                                 upload_batch_closure->UploadBatchRequest(),
                                 upload_batch_closure->UploadBatchResponse(),
                                 upload_batch_closure);
            }
        }

        uint64_t flush_data_size = scan_cc.accumulated_flush_data_size_;

        // nothing to flush
        if (scan_cc.accumulated_scan_cnt_ == 0)
        {
            scan_cc.Reset();
            continue;
        }

        // The cost of FlushRecord also needs to be considered.
#ifdef WITH_JEMALLOC
        flush_data_size += (scan_cc.DataSyncVec().size() * sizeof(FlushRecord) +
                            scan_cc.ArchiveVec().size() * sizeof(FlushRecord) +
                            scan_cc.MoveBaseIdxVec().size() *
                                sizeof(std::pair<TxKey, int32_t>));
#else
        // Check if vectors are empty before calling malloc_usable_size
        // to avoid SEGV on nullptr or invalid pointers.
        // Use malloc_usable_size when ASan is enabled (vectors may be
        // allocated by ASan's allocator), otherwise use
        // mi_malloc_usable_size for mimalloc-allocated memory.
        {
            auto &data_sync_vec_ref = scan_cc.DataSyncVec();
            auto &archive_vec_ref = scan_cc.ArchiveVec();
            auto &move_base_idx_vec_ref = scan_cc.MoveBaseIdxVec();

#ifdef __SANITIZE_ADDRESS__
            // When ASan is enabled, use standard malloc_usable_size
            flush_data_size +=
                (data_sync_vec_ref.empty()
                     ? 0
                     : malloc_usable_size(data_sync_vec_ref.data())) +
                (archive_vec_ref.empty()
                     ? 0
                     : malloc_usable_size(archive_vec_ref.data())) +
                (move_base_idx_vec_ref.empty()
                     ? 0
                     : malloc_usable_size(move_base_idx_vec_ref.data()));
#else
            // When ASan is not enabled, use mimalloc's API
            flush_data_size +=
                (data_sync_vec_ref.empty()
                     ? 0
                     : mi_malloc_usable_size(data_sync_vec_ref.data())) +
                (archive_vec_ref.empty()
                     ? 0
                     : mi_malloc_usable_size(archive_vec_ref.data())) +
                (move_base_idx_vec_ref.empty()
                     ? 0
                     : mi_malloc_usable_size(move_base_idx_vec_ref.data()));
#endif
        }
#endif

        // this thread will wait in AllocatePendingFlushDataMemQuota if
        // quota is not available
        uint64_t old_usage =
            data_sync_mem_controller_.AllocateFlushDataMemQuota(
                flush_data_size);
        DLOG(INFO) << "AllocateFlushDataMemQuota old_usage: " << old_usage
                   << " new_usage: " << old_usage + flush_data_size
                   << " quota: " << data_sync_mem_controller_.FlushMemoryQuota()
                   << " flight_tasks: " << data_sync_task->flight_task_cnt_
                   << " record count: " << scan_cc.accumulated_scan_cnt_;

        std::unique_lock<std::mutex> heap_lk(hash_partition_ckpt_heap_mux_);
        mi_threadid_t prev_thd =
            mi_override_thread(hash_partition_main_thread_id_);
        mi_heap_t *prev_heap = mi_heap_set_default(hash_partition_ckpt_heap_);

#if defined(WITH_JEMALLOC)
        uint32_t prev_arena;
        JemallocArenaSwitcher::ReadCurrentArena(prev_arena);
        JemallocArenaSwitcher::SwitchToArena(hash_partition_ckpt_arena_id_);
#endif

        data_sync_vec->reserve(scan_cc.accumulated_scan_cnt_);
        for (size_t j = 0; j < scan_cc.accumulated_scan_cnt_; ++j)
        {
            auto &rec = scan_cc.DataSyncVec()[j];
            // Note. Clone key instead of move key. The memory of
            // rec.Key() will be reused to avoid memory allocation.
            if (rec.cce_)
            {
                // cce_ is null means the key is already persisted on
                // kv, so we don't need to put it into the flush vec.
                int32_t part_id =
                    Sharder::MapKeyHashToHashPartitionId(rec.Key().Hash());
                if (table_name.Engine() == TableEngine::EloqKv)
                {
                    data_sync_vec->emplace_back(rec.Key().Clone(),
                                                rec.GetNonVersionedPayload(),
                                                rec.payload_status_,
                                                rec.commit_ts_,
                                                rec.cce_,
                                                rec.post_flush_size_,
                                                part_id);
                }
                else
                {
                    data_sync_vec->emplace_back(rec.Key().Clone(),
                                                rec.ReleaseVersionedPayload(),
                                                rec.payload_status_,
                                                rec.commit_ts_,
                                                rec.cce_,
                                                rec.post_flush_size_,
                                                part_id);
                }
            }
        }

        for (size_t j = 0; j < scan_cc.ArchiveVec().size(); ++j)
        {
            auto &rec = scan_cc.ArchiveVec()[j];
            // Note. We need to ensure the copy constructor of
            // FlushRecord could not be called.
            rec.SetKey((*data_sync_vec)[rec.GetKeyIndex()].Key());
        }

        for (size_t j = 0; j < scan_cc.MoveBaseIdxVec().size(); ++j)
        {
            size_t key_idx = scan_cc.MoveBaseIdxVec()[j];
            TxKey key_raw = (*data_sync_vec)[key_idx].Key();
            int32_t part_id =
                Sharder::MapKeyHashToHashPartitionId(key_raw.Hash());
            mv_base_vec->emplace_back(std::move(key_raw), part_id);
        }
        mi_override_thread(prev_thd);
        mi_heap_set_default(prev_heap);

#if defined(WITH_JEMALLOC)
        // override arena id
        JemallocArenaSwitcher::SwitchToArena(prev_arena);
#endif

        heap_lk.unlock();

        std::move(scan_cc.ArchiveVec().begin(),
                  scan_cc.ArchiveVec().end(),
                  std::back_inserter(*archive_vec));

        vec_mem_usage += flush_data_size;

        scan_data_drained = scan_cc.IsDrained() && scan_data_drained;

        {
            std::unique_lock<bthread::Mutex> flight_task_lk(
                data_sync_task->flight_task_mux_);
            if (data_sync_task->ckpt_err_ ==
                DataSyncTask::CkptErrorCode::FLUSH_ERROR)
            {
                flight_task_lk.unlock();

                LOG(WARNING)
                    << "There are error during flush for this data sync: "
                    << data_sync_txm->TxNumber() << " on worker#" << worker_idx
                    << ". Terminal this datasync task.";
                // 1. Release read intent on paused key
                if (!scan_cc.IsDrained())
                {
                    scan_cc.Reset(
                        HashPartitionDataSyncScanCc::OpType::Terminated);
                    EnqueueLowPriorityCcRequestToShard(worker_idx, &scan_cc);
                    scan_cc.Wait();
                }
                // 2. Release memory usage on this datasync worker.
                data_sync_vec->clear();
                archive_vec->clear();
                mv_base_vec->clear();
                data_sync_mem_controller_.DeallocateFlushMemQuota(
                    vec_mem_usage);
                break;
            }

            // Flush worker will call PostProcessDataSyncTask() to
            // decrement flight task count.
            data_sync_task->flight_task_cnt_ += 1;
        }

        AddFlushTaskEntry(
            std::make_unique<FlushTaskEntry>(std::move(data_sync_vec),
                                             std::move(archive_vec),
                                             std::move(mv_base_vec),
                                             data_sync_txm,
                                             data_sync_task,
                                             catalog_rec.CopySchema(),
                                             vec_mem_usage));

        data_sync_vec = std::make_unique<std::vector<FlushRecord>>();

        archive_vec = std::make_unique<std::vector<FlushRecord>>();

        mv_base_vec =
            std::make_unique<std::vector<std::pair<TxKey, int32_t>>>();

        vec_mem_usage = 0;

        if (scan_cc.scan_heap_is_full_ == 1)
        {
            // Clear the FlushRecords' memory of scan cc since the
            // DataSyncScan heap is full.
            auto &data_sync_vec_ref = scan_cc.DataSyncVec();
            auto &archive_vec_ref = scan_cc.ArchiveVec();
            ReleaseDataSyncScanHeapCc release_scan_heap_cc(&data_sync_vec_ref,
                                                           &archive_vec_ref);
            EnqueueLowPriorityCcRequestToShard(worker_idx,
                                               &release_scan_heap_cc);
            release_scan_heap_cc.Wait();
        }

        scan_cc.Reset();
    }

    // release scan heap memory after scan finish
    auto &data_sync_vec_ref = scan_cc.DataSyncVec();
    auto &archive_vec_ref = scan_cc.ArchiveVec();
    ReleaseDataSyncScanHeapCc release_scan_heap_cc(&data_sync_vec_ref,
                                                   &archive_vec_ref);
    EnqueueLowPriorityCcRequestToShard(worker_idx, &release_scan_heap_cc);
    release_scan_heap_cc.Wait();

    if (!data_sync_vec->empty() || !archive_vec->empty() ||
        !mv_base_vec->empty())
    {
        std::unique_lock<bthread::Mutex> flight_task_lk(
            data_sync_task->flight_task_mux_);
        if (data_sync_task->ckpt_err_ == DataSyncTask::CkptErrorCode::NO_ERROR)
        {
            data_sync_task->flight_task_cnt_ += 1;
            flight_task_lk.unlock();

            AddFlushTaskEntry(
                std::make_unique<FlushTaskEntry>(std::move(data_sync_vec),
                                                 std::move(archive_vec),
                                                 std::move(mv_base_vec),
                                                 data_sync_txm,
                                                 data_sync_task,
                                                 catalog_rec.CopySchema(),
                                                 vec_mem_usage));
        }
        else
        {
            // There are error during flush, and if we do not put the
            // current batch data into flush worker, should release the
            // memory usage.
            data_sync_mem_controller_.DeallocateFlushMemQuota(vec_mem_usage);
        }
    }

    PostProcessHashPartitionDataSyncTask(std::move(data_sync_task),
                                         data_sync_txm,
                                         DataSyncTask::CkptErrorCode::NO_ERROR);
}

void LocalCcShards::PopPendingTask(NodeGroupId ng_id,
                                   int64_t ng_term,
                                   const TableName &table_name,
                                   uint32_t id)
{
    assert(!table_name.IsMeta());
    auto task_limiter_key = TaskLimiterKey(ng_id,
                                           ng_term,
                                           table_name.StringView(),
                                           table_name.Type(),
                                           table_name.Engine(),
                                           id);

    std::unique_lock<std::mutex> task_limiter_lk(task_limiter_mux_);
    auto iter = task_limiters_.find(task_limiter_key);
    assert(iter != task_limiters_.end());

    if (!iter->second->pending_tasks_.empty())
    {
        std::shared_ptr<DataSyncTask> task =
            iter->second->pending_tasks_.front();
        iter->second->pending_tasks_.pop();
        task_limiter_lk.unlock();

        std::lock_guard<std::mutex> task_worker_lk(data_sync_worker_ctx_.mux_);
        data_sync_task_queue_[id % data_sync_task_queue_.size()].push_back(
            std::move(task));
        data_sync_worker_ctx_.cv_.notify_all();
    }
    else
    {
        task_limiters_.erase(iter);
    }
}

void LocalCcShards::ClearAllPendingTasks(NodeGroupId ng_id,
                                         int64_t ng_term,
                                         const TableName &table_name,
                                         uint32_t id)
{
    assert(!table_name.IsMeta());

    auto task_limiter_key = TaskLimiterKey(ng_id,
                                           ng_term,
                                           table_name.StringView(),
                                           table_name.Type(),
                                           table_name.Engine(),
                                           id);

    std::lock_guard<std::mutex> task_limiter_lk(task_limiter_mux_);
    auto iter = task_limiters_.find(task_limiter_key);
    assert(iter != task_limiters_.end());

    while (!iter->second->pending_tasks_.empty())
    {
        auto &task = iter->second->pending_tasks_.front();
        task->SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
        task->SetScanTaskFinished();
        iter->second->pending_tasks_.pop();
    }

    task_limiters_.erase(iter);
}

void LocalCcShards::UpdateSlicePostCkptSize(
    StoreRange *store_range, const std::map<TxKey, int64_t> &slices_delta_size)
{
    for (auto it = slices_delta_size.cbegin(); it != slices_delta_size.cend();
         ++it)
    {
        StoreSlice *curr_slice = store_range->FindSlice(it->first);
        int64_t sum = curr_slice->Size() + it->second;
        uint64_t slice_size = sum > 0 ? sum : 0;
        curr_slice->SetPostCkptSize(slice_size);
    }
}

bool LocalCcShards::CalculateRangeUpdate(const TableName &table_name,
                                         NodeGroupId node_group_id,
                                         int64_t node_group_term,
                                         uint64_t data_sync_ts,
                                         StoreRange *store_range,
                                         std::vector<TxKey> &splitting_info)
{
    size_t store_range_post_ckpt_size = store_range->PostCkptSize();
    if (store_range_post_ckpt_size > StoreRange::range_max_size)
    {
        return store_range->CalculateRangeSplitKeys(table_name,
                                                    node_group_id,
                                                    node_group_term,
                                                    data_sync_ts,
                                                    store_range_post_ckpt_size,
                                                    splitting_info);
    }
    return true;
}

void LocalCcShards::UpdateSlices(const TableName &table_name,
                                 const TableSchema *schema,
                                 StoreRange *store_range,
                                 bool all_data_exported,
                                 const std::vector<FlushRecord> &data_sync_vec,
                                 UpdateSliceStatus &status)
{
    auto lower_bound_cmp = [](const FlushRecord &rec, const TxKey &key)
    { return rec.Key() < key; };

    TxKey range_end_tx_key = store_range->RangeEndTxKey();

    auto flush_record_it = data_sync_vec.begin();
    while (flush_record_it != data_sync_vec.end())
    {
        TxKey search_key = flush_record_it->Key();
        StoreSlice *curr_slice = store_range->FindSlice(search_key);
        TxKey slice_start_key = curr_slice->StartTxKey();
        TxKey slice_end_key = curr_slice->EndTxKey();

        if (status.paused_slice_ &&
            (status.paused_slice_->StartTxKey().KeyPtr() !=
             slice_start_key.KeyPtr()))
        {
            if (table_name.IsBase() && status.paused_slice_rec_cnt_ != 0)
            {
                // Set estimate record size for base table
                uint64_t post_ckpt_size = status.paused_slice_->PostCkptSize();
                auto stats_obj = schema->StatisticsObject();
                if (stats_obj)
                {
                    stats_obj->SetEstimateRecordSize(
                        post_ckpt_size / status.paused_slice_rec_cnt_);
                }
            }

            // The paused slice in the last round has already finished. Just
            // update the slice.
            if (status.paused_split_keys_.size() > 1)
            {
                // Update the current slice
                store_range->UpdateSliceSpec(status.paused_slice_,
                                             status.paused_split_keys_);
            }
            else if (status.paused_split_keys_.size() == 1)
            {
                // Fix the post ckpt size of this slice
                status.paused_slice_->SetPostCkptSize(
                    status.paused_split_keys_[0].post_update_size_);
            }
            status.Reset();
        }
        else if (status.paused_slice_)
        {
            assert(status.paused_slice_ == curr_slice);
        }

        auto slice_end_it = slice_end_key.KeyPtr() == range_end_tx_key.KeyPtr()
                                ? data_sync_vec.end()
                                : std::lower_bound(flush_record_it,
                                                   data_sync_vec.end(),
                                                   slice_end_key,
                                                   lower_bound_cmp);

        // Whether all items of the current slice are exported. If the current
        // slice need to be split, only in case that this variable is true, can
        // update the slice spec.
        bool all_slice_item_exported =
            (slice_end_it != data_sync_vec.end() || all_data_exported) ? true
                                                                       : false;

        uint64_t slice_post_ckpt_size = curr_slice->PostCkptSize();
        // If post ckpt size of the slice is less than or equal to the current
        // size, there is no need to split the slice.
        if (slice_post_ckpt_size == UINT64_MAX ||
            slice_post_ckpt_size <= StoreSlice::slice_upper_bound ||
            slice_post_ckpt_size <= curr_slice->Size())
        {
            // Case 1: There is no unpersisted data in the current slice, so no
            // need to split the slice. Only to migrate this data from the old
            // range to the new range.
            // Case 2: The current slice does not need to be split.
            assert(!status.paused_slice_);
            // Move to the next slice
            flush_record_it = slice_end_it;
            continue;
        }

        // Calculate subslice keys based on the data in the vector of
        // FlushRecord
        uint32_t subslice_cnt =
            slice_post_ckpt_size / StoreSlice::slice_upper_bound;
        subslice_cnt = subslice_cnt < 2 ? 2 : subslice_cnt;
        uint32_t avg_subslice_size = slice_post_ckpt_size / subslice_cnt;

        uint32_t subslice_size = curr_slice->Size() / subslice_cnt;
        size_t record_cnt = status.paused_slice_rec_cnt_;
        uint32_t subslice_post_ckpt_size = 0;
        std::vector<SliceChangeInfo> slice_split_keys =
            std::move(status.paused_split_keys_);
        if (status.paused_slice_)
        {
            uint32_t paused_subslice_post_ckpt_size =
                slice_split_keys.back().post_update_size_;
            subslice_post_ckpt_size =
                paused_subslice_post_ckpt_size < avg_subslice_size
                    ? paused_subslice_post_ckpt_size
                    : 0;
        }
        else
        {
            // Process the current slice for the first time.
            slice_split_keys.reserve(subslice_cnt);
        }

        auto update_new_subslices = [curr_slice,
                                     &search_key,
                                     &subslice_size,
                                     &avg_subslice_size,
                                     &subslice_post_ckpt_size,
                                     &all_slice_item_exported,
                                     &slice_split_keys]()
        {
            if (slice_split_keys.empty())
            {
                // The first sub-slice's start key re-uses the old slice's
                // start key, so there is no need to allocate a new key.
                slice_split_keys.emplace_back(curr_slice->StartTxKey(),
                                              subslice_size,
                                              subslice_post_ckpt_size);
            }
            else if (slice_split_keys.back().post_update_size_ <
                     avg_subslice_size)
            {
                assert(slice_split_keys.back().cur_size_ == subslice_size);
                // Update the subslice info that was unfinished last round.
                slice_split_keys.back().post_update_size_ =
                    subslice_post_ckpt_size;
            }
            else
            {
                // Clone the key if not all the data of this slice have been
                // exported.
                TxKey slice_start_key = all_slice_item_exported
                                            ? std::move(search_key)
                                            : search_key.Clone();

                slice_split_keys.emplace_back(std::move(slice_start_key),
                                              subslice_size,
                                              subslice_post_ckpt_size);
            }
        };

        while (flush_record_it != slice_end_it)
        {
            subslice_post_ckpt_size += flush_record_it->post_flush_size_;

            record_cnt = (flush_record_it->post_flush_size_ != 0)
                             ? (record_cnt + 1)
                             : record_cnt;

            if (subslice_post_ckpt_size >= avg_subslice_size)
            {
                update_new_subslices();

                subslice_post_ckpt_size = 0;
                search_key = std::next(flush_record_it) != slice_end_it
                                 ? std::next(flush_record_it)->Key()
                                 : TxKey();
            }

            // Forward to the next iterator
            ++flush_record_it;
        }

        if (subslice_post_ckpt_size > 0)
        {
            update_new_subslices();
        }

        if (all_slice_item_exported)
        {
            if (table_name.IsBase() && record_cnt != 0)
            {
                // Set estimate record size for base table
                auto stats_obj = schema->StatisticsObject();
                if (stats_obj)
                {
                    stats_obj->SetEstimateRecordSize(slice_post_ckpt_size /
                                                     record_cnt);
                }
            }

            // Update the current slice spec if all the slice data have been
            // exported.
            // In some case, the size of a flush record is larger than the slice
            // upper bound.
            if (slice_split_keys.size() > 1)
            {
                // Split StoreSlice in memory. Slice info in KV store will be
                // updated after checkpoint.
                store_range->UpdateSliceSpec(curr_slice, slice_split_keys);
            }
            else if (slice_split_keys.size() == 1)
            {
                // Fix the post ckpt size for this slice
                // If some keys in this slice are deleted between the scan delta
                // size and scan keys, it may actually cause the slice to not
                // need to be split.
                curr_slice->SetPostCkptSize(
                    slice_split_keys[0].post_update_size_);
            }

            if (status.paused_slice_)
            {
                status.Reset();
            }
        }
        else
        {
            assert(flush_record_it == data_sync_vec.end());
            // The slice_split_keys may be empty if the first few keys of this
            // slice are DELETED.
            status.SetPausedPos(
                curr_slice, record_cnt, std::move(slice_split_keys));
        }
    }
}

void LocalCcShards::SplitFlushRange(
    std::unique_lock<std::mutex> &task_worker_lk)
{
    std::shared_ptr<void> lock_task_worker(nullptr,
                                           [&task_worker_lk](void *)
                                           {
                                               if (!task_worker_lk.owns_lock())
                                               {
                                                   // Need to gain ownership
                                                   // before returnning back to
                                                   // caller.
                                                   task_worker_lk.lock();
                                               }
                                           });

    std::unique_ptr<RangeSplitTask> range_split_task =
        std::move(pending_range_split_task_.front());
    pending_range_split_task_.pop_front();

    task_worker_lk.unlock();

    TableRangeEntry *range_entry = range_split_task->range_entry_;
    std::shared_ptr<DataSyncTask> &data_sync_task =
        range_split_task->data_sync_task_;
    const TableName &table_name = data_sync_task->table_name_;
    const TableName range_table_name{
        table_name.String(), TableType::RangePartition, table_name.Engine()};
    auto &split_keys = range_split_task->split_keys_;
    std::shared_ptr<const TableSchema> &table_schema =
        range_split_task->schema_;
    TransactionExecution *split_txm = range_split_task->data_sync_txm_;
    NodeGroupId node_group = data_sync_task->node_group_id_;

    std::string log_output(
        "Splitting table " + table_name.String() + " range " +
        std::to_string(range_entry->GetRangeInfo()->PartitionId()) + " into " +
        std::to_string(split_keys.size() + 1) + " ranges. New range ids ");
    // Request for new range ids from data store. The new range ids returned
    // by data store are always unique.
    std::vector<std::pair<TxKey, int32_t>> new_range_ids;
    int32_t new_part_id;
    if (!GetNextRangePartitionId(
            table_name, table_schema.get(), split_keys.size(), new_part_id))
    {
        LOG(ERROR) << "Split range failed due to unable to get next "
                      "partition id. table_name = "
                   << table_name.StringView();

        range_entry->UnPinStoreRange();
        data_sync_task->SetError(CcErrorCode::DATA_STORE_ERR);
        data_sync_task->SetScanTaskFinished();

        PopPendingTask(node_group,
                       data_sync_task->node_group_term_,
                       table_name,
                       data_sync_task->id_);
        txservice::AbortTx(split_txm);

        return;
    }
    for (TxKey &new_key : split_keys)
    {
        log_output.append(std::to_string(new_part_id) + ",");
        new_range_ids.emplace_back(std::move(new_key), new_part_id);
        new_part_id++;
    }

    // Start the SplitFlush tx. This would split the range, flush the data
    // and update slice metadata.
    log_output.append(" txn: " + std::to_string(split_txm->TxNumber()));
    LOG(INFO) << log_output;
    if (realtime_sampling_)
    {
        // We should consider again whether should we broadcast statistics here.
        bool updated_since_sync = false;
        std::unique_ptr<remote::NodeGroupSamplePool> sample_pool =
            table_schema->StatisticsObject()->MakeBroadcastSamplePool(
                node_group, table_name, &updated_since_sync);
        if (updated_since_sync)
        {
            BroadcastIndexStatistics(split_txm,
                                     node_group,
                                     table_name,
                                     table_schema.get(),
                                     *sample_pool);
        }
    }

    DLOG(INFO) << "Begin to do range split flush, range_id:"
               << range_entry->GetRangeInfo()->PartitionId();

    SplitFlushTxRequest split_req(table_name,
                                  std::move(table_schema),
                                  range_entry,
                                  std::move(new_range_ids),
                                  data_sync_task->is_dirty_);

    split_txm->Execute(&split_req);
    split_req.Wait();
    if (split_req.IsError() || !split_req.Result())
    {
        LOG(ERROR) << "Split range on table " << table_name.StringView()
                   << " partition "
                   << range_entry->GetRangeInfo()->PartitionId() << " failed.";

        range_entry->UnPinStoreRange();

        data_sync_task->SetError();
        data_sync_task->SetScanTaskFinished();

        PopPendingTask(node_group,
                       data_sync_task->node_group_term_,
                       table_name,
                       data_sync_task->id_);
        txservice::AbortTx(split_txm);

        return;
    }

    range_entry->UpdateLastDataSyncTS(data_sync_task->data_sync_ts_);
    range_entry->UnPinStoreRange();

    data_sync_task->SetFinish();
    data_sync_task->SetScanTaskFinished();
    PopPendingTask(node_group,
                   data_sync_task->node_group_term_,
                   table_name,
                   data_sync_task->id_);

    LOG(INFO) << "Split range on table " << range_table_name.StringView()
              << " partition " << range_entry->GetRangeInfo()->PartitionId()
              << " succeeded.";
    txservice::CommitTx(split_txm);
}

void LocalCcShards::AddFlushTaskEntry(std::unique_ptr<FlushTaskEntry> &&entry)
{
    cur_flush_buffer_.AddFlushTaskEntry(std::move(entry));

    std::unique_ptr<FlushDataTask> flush_data_task =
        cur_flush_buffer_.MoveFlushData(false);
    if (flush_data_task != nullptr)
    {
        std::unique_lock<std::mutex> worker_lk(flush_data_worker_ctx_.mux_);

        // Try to merge with the last task if queue is not empty
        if (!pending_flush_work_.empty())
        {
            auto &last_task = pending_flush_work_.back();
            if (last_task->MergeFrom(std::move(flush_data_task)))
            {
                // Merge successful, task was merged into last_task
                flush_data_worker_ctx_.cv_.notify_one();
                return;
            }
        }

        // Could not merge, wait if queue is full
        while (pending_flush_work_.size() >=
               static_cast<size_t>(flush_data_worker_ctx_.worker_num_))
        {
            flush_data_worker_ctx_.cv_.wait(worker_lk);
        }

        // Add as new task
        pending_flush_work_.emplace_back(std::move(flush_data_task));
        flush_data_worker_ctx_.cv_.notify_one();
    }
}

void LocalCcShards::FlushCurrentFlushBuffer()
{
    std::unique_ptr<FlushDataTask> flush_data_task =
        cur_flush_buffer_.MoveFlushData(true);
    if (flush_data_task != nullptr)
    {
        std::unique_lock<std::mutex> worker_lk(flush_data_worker_ctx_.mux_);

        // Try to merge with the last task if queue is not empty
        if (!pending_flush_work_.empty())
        {
            auto &last_task = pending_flush_work_.back();
            if (last_task->MergeFrom(std::move(flush_data_task)))
            {
                // Merge successful, task was merged into last_task
                flush_data_worker_ctx_.cv_.notify_one();
                return;
            }
        }

        // Could not merge, wait if queue is full
        while (pending_flush_work_.size() >=
               static_cast<size_t>(flush_data_worker_ctx_.worker_num_))
        {
            flush_data_worker_ctx_.cv_.wait(worker_lk);
        }

        // Add as new task
        pending_flush_work_.emplace_back(std::move(flush_data_task));
        flush_data_worker_ctx_.cv_.notify_one();
    }
}

void LocalCcShards::FlushData(std::unique_lock<std::mutex> &flush_worker_lk)
{
    // Retrieve first pending work and pop it (FIFO).
    std::unique_ptr<FlushDataTask> cur_work =
        std::move(pending_flush_work_.front());
    pending_flush_work_.pop_front();

    // Notify any threads waiting for queue space
    flush_data_worker_ctx_.cv_.notify_all();

    flush_worker_lk.unlock();

    auto &flush_task_entries = cur_work->flush_task_entries_;

    bool succ = true;
    // Flushes to the data store
    if (EnableMvcc())
    {
        succ = store_hd_->CopyBaseToArchive(flush_task_entries);
        if (!succ)
        {
            LOG(ERROR) << "DataSync CopyBaseToArchive flush to kv "
                          "storage failed";
        }
    }

    if (succ)
    {
        succ = store_hd_->PutAll(flush_task_entries);
        if (!succ)
        {
            LOG(ERROR) << "DataSync PutAll flush to kv "
                          "storage failed";
        }
    }

    if (succ && EnableMvcc())
    {
        succ = store_hd_->PutArchivesAll(flush_task_entries);
        if (!succ)
        {
            LOG(ERROR) << "DataSync PutArchivesAll flush to "
                          "kv storage failed";
        }
    }

    // Persist data in kv store if needed
    if (succ && store_hd_->NeedPersistKV())
    {
        std::vector<std::string> kv_table_names;
        for (auto &[table_name, entries] : flush_task_entries)
        {
            kv_table_names.push_back(table_name.data());
        }
        succ = store_hd_->PersistKV(kv_table_names);
    }

    std::unordered_set<uint16_t> updated_ckpt_ts_core_ids;
    // Update cce ckpt ts in memory
    if (succ)
    {
        for (auto &[kv_table_name, entries] : flush_task_entries)
        {
            for (auto &entry : entries)
            {
                if (!entry->data_sync_task_->need_update_ckpt_ts_)
                {
                    continue;
                }

                absl::flat_hash_map<size_t,
                                    std::vector<UpdateCceCkptTsCc::CkptTsEntry>>
                    cce_entries_map;

                auto &table_name =
                    entries.front()->data_sync_task_->table_name_;

                for (auto &rec : *(entry->data_sync_vec_))
                {
                    auto cce = rec.cce_;
                    if (cce != nullptr)
                    {
                        size_t key_core_idx = 0;
                        if (!table_name.IsHashPartitioned())
                        {
                            key_core_idx = (rec.Key().Hash() & 0x3FF) % Count();
                        }
                        else
                        {
                            key_core_idx = entry->data_sync_task_->id_;
                        }
                        auto insert_it = cce_entries_map.try_emplace(
                            key_core_idx,
                            std::vector<UpdateCceCkptTsCc::CkptTsEntry>());
                        insert_it.first->second.emplace_back(
                            cce, rec.commit_ts_, rec.post_flush_size_);
                    }
                }

                if (cce_entries_map.size() > 0)
                {
                    UpdateCceCkptTsCc update_cce_req(
                        entry->data_sync_task_->node_group_id_,
                        entry->data_sync_task_->node_group_term_,
                        table_name,
                        cce_entries_map);
                    for (auto &[core_idx, cce_entries] : cce_entries_map)
                    {
                        updated_ckpt_ts_core_ids.insert(core_idx);
                        EnqueueToCcShard(core_idx, &update_cce_req);
                    }
                    update_cce_req.Wait();
                }
            }
        }
    }

    // Notify cc shards that dirty data has been flushed. This will re-enqueue
    // kickout data cc reqs if there are any.
    WaitableCc reset_cc(
        [&](CcShard &ccs)
        {
            ccs.OnDirtyDataFlushed();
            return true;
        },
        updated_ckpt_ts_core_ids.size());
    for (uint16_t core_idx : updated_ckpt_ts_core_ids)
    {
        EnqueueToCcShard(core_idx, &reset_cc);
    }
    reset_cc.Wait();

    auto ckpt_err = succ ? DataSyncTask::CkptErrorCode::NO_ERROR
                         : DataSyncTask::CkptErrorCode::FLUSH_ERROR;

    // notify waiting data sync scan thread
    uint64_t old_usage = data_sync_mem_controller_.DeallocateFlushMemQuota(
        cur_work->pending_flush_size_);

    DLOG(INFO) << "DelocateFlushDataMemQuota old_usage: " << old_usage
               << " new_usage: " << old_usage - cur_work->pending_flush_size_
               << " quota: " << data_sync_mem_controller_.FlushMemoryQuota();

    PostProcessFlushTaskEntries(flush_task_entries, ckpt_err);
    flush_worker_lk.lock();
}

void LocalCcShards::FlushDataWorker()
{
    size_t previous_flush_size = 0;
    auto previous_size_update_time = std::chrono::steady_clock::now();
    std::unique_lock<std::mutex> flush_worker_lk(flush_data_worker_ctx_.mux_);
    while (flush_data_worker_ctx_.status_ == WorkerStatus::Active)
    {
        flush_data_worker_ctx_.cv_.wait_for(
            flush_worker_lk,
            10s,
            [this, &previous_flush_size, &previous_size_update_time]
            {
                if (!pending_flush_work_.empty() ||
                    flush_data_worker_ctx_.status_ == WorkerStatus::Terminated)
                {
                    return true;
                }
                auto current_time = std::chrono::steady_clock::now();
                if (current_time - previous_size_update_time > 10s)
                {
                    size_t current_flush_size =
                        cur_flush_buffer_.GetPendingFlushSize();
                    bool flush_size_changed =
                        current_flush_size != previous_flush_size;
                    previous_flush_size = current_flush_size;
                    previous_size_update_time = current_time;
                    if (!flush_size_changed && current_flush_size > 0)
                    {
                        // data sync might be stuck due to lock conflict with
                        // DDL. Flush current flush buffer to release catalog
                        // read lock held by ongoing data sync tx, which might
                        // block the DDL.
                        std::unique_ptr<FlushDataTask> flush_data_task =
                            cur_flush_buffer_.MoveFlushData(true);
                        if (flush_data_task != nullptr)
                        {
                            // Try to merge with the last task if queue is not
                            // empty Note: flush_worker_lk is already held here
                            // (inside condition variable predicate)
                            if (!pending_flush_work_.empty())
                            {
                                auto &last_task = pending_flush_work_.back();
                                if (last_task->MergeFrom(
                                        std::move(flush_data_task)))
                                {
                                    // Merge successful, task was merged into
                                    // last_task
                                    return true;
                                }
                            }

                            // Add as new task. We just checked that
                            // pending_flush_work_ is empty,
                            pending_flush_work_.emplace_back(
                                std::move(flush_data_task));
                            return true;
                        }
                    }
                }

                return false;
            });

        if (pending_flush_work_.empty())
        {
            continue;
        }

        FlushData(flush_worker_lk);
    }

    while (!pending_flush_work_.empty())
    {
        FlushData(flush_worker_lk);
    }
}

void LocalCcShards::RangeSplitWorker()
{
    std::unique_lock<std::mutex> range_split_worker_lk(
        range_split_worker_ctx_.mux_);
    while (range_split_worker_ctx_.status_ == WorkerStatus::Active)
    {
        range_split_worker_ctx_.cv_.wait(
            range_split_worker_lk,
            [this]
            {
                return !pending_range_split_task_.empty() ||
                       range_split_worker_ctx_.status_ ==
                           WorkerStatus::Terminated;
            });

        if (pending_range_split_task_.empty())
        {
            continue;
        }

        SplitFlushRange(range_split_worker_lk);
    }

    while (!pending_range_split_task_.empty())
    {
        SplitFlushRange(range_split_worker_lk);
    }
}

bool LocalCcShards::UpdateStoreSlices(
    std::vector<FlushTaskEntry *> &flush_tasks)
{
    std::vector<UpdateRangeSlicesReq> update_range_slice_reqs;

    for (auto &flush_task : flush_tasks)
    {
        auto &task = flush_task->data_sync_task_;
        TableRangeEntry *range_entry = nullptr;
        const TxKey *start_key = nullptr;
        const TxKey *end_key = nullptr;
        if (!task->during_split_range_)
        {
            range_entry = const_cast<TableRangeEntry *>(GetTableRangeEntry(
                task->table_name_, task->node_group_id_, task->id_));
        }
        else
        {
            range_entry = task->range_entry_;
            start_key = &task->start_key_;
            end_key = &task->end_key_;
        }

        StoreRange *range = range_entry->RangeSlices();
        // Update in-memory slice size
        bool range_updated =
            range->UpdateSliceSizeAfterFlush(start_key, end_key, true);

        // Update data store slice size
        // start_key != nullptr, means that it is during split range, and for
        // this case, this job will be done in the split range operation.
        if (start_key == nullptr && range_updated)
        {
            update_range_slice_reqs.emplace_back(&task->table_name_,
                                                 task->data_sync_ts_,
                                                 range->RangeStartTxKey(),
                                                 range->Slices(),
                                                 range->PartitionId(),
                                                 range_entry->Version());
        }
    }

    if (!update_range_slice_reqs.empty())
    {
        bool success = store_hd_->UpdateRangeSlices(update_range_slice_reqs);
        return success;
    }

    return true;
}

bool LocalCcShards::UpdateStoreSlice(const TableName &table_name,
                                     uint64_t ckpt_ts,
                                     TableRangeEntry *range_entry,
                                     const TxKey *start_key,
                                     const TxKey *end_key,
                                     bool flush_res)
{
    bool success = true;
    assert(range_entry);
    uint64_t range_version = range_entry->Version();
    StoreRange *range = range_entry->RangeSlices();
    assert(range);

    // Update in-memory slice size
    bool range_updated =
        range->UpdateSliceSizeAfterFlush(start_key, end_key, flush_res);

    // Update data store slice size
    // start_key != nullptr, means that it is during split range, and for this
    // case, this job will be done in the split range operation.
    if (start_key == nullptr && flush_res && range_updated)
    {
        success = range->UpdateRangeSlicesInStore(
            table_name, ckpt_ts, range_version, store_hd_);
    }
    // else: SplitFlushRangeOp will update range slice in store
    return success;
}

void LocalCcShards::SyncTableStatisticsWorker()
{
    std::unique_lock<std::mutex> worker_lk(statistics_worker_ctx_.mux_);
    std::unordered_map<NodeGroupId, uint64_t> ng_sync_ts;
    while (statistics_worker_ctx_.status_ == WorkerStatus::Active)
    {
        // Wake up every 10s to sync table statistics with other nodes.
        statistics_worker_ctx_.cv_.wait_for(
            worker_lk,
            10s,
            [this]
            {
                return statistics_worker_ctx_.status_ ==
                       WorkerStatus::Terminated;
            });

        CODE_FAULT_INJECTOR("skip_sync_table_statistics", { continue; });

        worker_lk.unlock();

        std::vector<uint32_t> node_groups =
            Sharder::Instance().LocalNodeGroups();
        for (uint32_t node_group : node_groups)
        {
            // check whether this node is group leader, pin its data if it
            // is
            int64_t leader_term =
                Sharder::Instance().TryPinNodeGroupData(node_group);
            if (leader_term < 0)
            {
                continue;
            }
            CkptTsCc ckpt_req(cc_shards_.size(), node_group);

            // Use ckpt ts as sync ts. It will be used next round to decide
            // if table has any updates since last sync.
            for (auto &ccs : cc_shards_)
            {
                ccs->Enqueue(&ckpt_req);
            }
            ckpt_req.Wait();

            uint64_t sync_ts = ckpt_req.GetCkptTs();
            bool succ = true;

            // Get table names in this node group, stats sync worker should
            // be TableName string owner.
            std::unordered_map<TableName, bool> tables =
                GetCatalogTableNameSnapshot(node_group, sync_ts);

            // Loop over all tables and sync stats.
            for (auto it = tables.begin(); it != tables.end(); ++it)
            {
                if (Sharder::Instance().LeaderTerm(node_group) != leader_term)
                {
                    // Skip the node groups that are no longer on this node.
                    break;
                }

                const TableName &table_name = it->first;
                bool is_dirty = it->second;
                if (table_name.Engine() == TableEngine::EloqSql ||
                    table_name.Engine() == TableEngine::EloqDoc)
                {
                    // Set isolation level to RepeatableRead to ensure the
                    // readlock will be set during the execution of the
                    // following ReadTxRequest.
                    TransactionExecution *txm =
                        NewTxInit(tx_service_,
                                  IsolationLevel::RepeatableRead,
                                  CcProtocol::Locking,
                                  node_group);
                    if (txm == nullptr)
                    {
                        succ = false;
                        continue;
                    }
                    const TableName base_table_name{
                        table_name.GetBaseTableNameSV(),
                        TableType::Primary,
                        table_name.Engine()};

                    CatalogKey table_key(base_table_name);
                    TxKey tbl_tx_key{&table_key};
                    CatalogRecord catalog_rec;

                    ReadTxRequest read_req;
                    read_req.Set(&catalog_ccm_name,
                                 0,
                                 &tbl_tx_key,
                                 &catalog_rec,
                                 false,
                                 false,
                                 true);
                    txm->Execute(&read_req);
                    read_req.Wait();

                    RecordStatus rec_status = read_req.Result().first;
                    if (read_req.IsError() ||
                        rec_status != RecordStatus::Normal)
                    {
                        // Use AbortTxRequest to release read lock.
                        txservice::AbortTx(txm);

                        if (read_req.IsError())
                        {
                            succ = false;
                        }
                        continue;
                    }

                    const TableSchema *table_schema = catalog_rec.Schema();
                    if (is_dirty && catalog_rec.DirtySchema() &&
                        !table_schema->IndexKeySchema(table_name))
                    {
                        assert(table_name.Type() == TableType::Secondary ||
                               table_name.Type() == TableType::UniqueSecondary);
                        table_schema = catalog_rec.DirtySchema();
                    }

                    assert(table_schema != nullptr);

                    // For index table, if this table has been dropped, skip it.
                    if ((table_name.Type() == TableType::Secondary ||
                         table_name.Type() == TableType::UniqueSecondary) &&
                        table_schema->IndexKeySchema(table_name) == nullptr)
                    {
                        txservice::AbortTx(txm);
                        continue;
                    }

                    bool updated_since_sync = false;
                    std::unique_ptr<remote::NodeGroupSamplePool> sample_pool =
                        table_schema->StatisticsObject()
                            ->MakeBroadcastSamplePool(
                                node_group, table_name, &updated_since_sync);
                    if (updated_since_sync)
                    {
                        BroadcastIndexStatistics(txm,
                                                 node_group,
                                                 table_name,
                                                 table_schema,
                                                 *sample_pool);
                        table_schema->StatisticsObject()->SetUpdatedSinceSync();
                    }

                    if (Statistics::LeaderNodeGroup(table_name) == node_group)
                    {
                        // To prevent statistics in storage broken, only
                        // one node is allowed to write.
                        for (int i = 0; i < 10; i++)
                        {
                            std::unordered_map<
                                TableName,
                                std::pair<uint64_t, std::vector<TxKey>>>
                                sample_pool_map =
                                    table_schema->StatisticsObject()
                                        ->MakeStoreStatistics(
                                            &updated_since_sync);
                            if (updated_since_sync)
                            {
                                succ = store_hd_->UpsertTableStatistics(
                                    base_table_name, sample_pool_map, sync_ts);
                                if (succ)
                                {
                                    break;
                                }
                                else
                                {
                                    LOG(ERROR)
                                        << "Failed to update statistics of "
                                           "table "
                                        << table_name.Trace() << ", retrying.";
                                    std::this_thread::sleep_for(1s);
                                    // Check leader term in infinite while
                                    // loop.
                                    if (!Sharder::Instance().CheckLeaderTerm(
                                            node_group, leader_term))
                                    {
                                        LOG(ERROR)
                                            << "Leader term changed during "
                                               "table statistics update";
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                break;
                            }
                        }
                        if (!succ)
                        {
                            // Set updated_since_sync_ to true, and next sync
                            // loop will retry it.
                            table_schema->StatisticsObject()
                                ->SetUpdatedSinceSync();
                        }
                    }

                    txservice::CommitTx(txm);
                }
            }

            // finish table stats sync on this node group, unpin its data
            // and clear its ccmaps and catalogs if it is no longer leader
            Sharder::Instance().UnpinNodeGroupData(node_group);
        }
        worker_lk.lock();
    }
}

GenerateSkStatus *LocalCcShards::GetGenerateSkStatus(NodeGroupId ng_id,
                                                     uint64_t tx_number,
                                                     int32_t partition_id,
                                                     int64_t tx_term)
{
    std::unique_lock<std::mutex> lk(generate_sk_mux_);

    auto ng_it = generate_sk_status_.find(ng_id);
    if (ng_it == generate_sk_status_.end())
    {
        auto insert_it = generate_sk_status_.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(ng_id),
            std::forward_as_tuple(TxGenerateSkStatus()));

        ng_it = insert_it.first;
    }

    TxGenerateSkStatus &tx_status = ng_it->second;
    auto tx_it = tx_status.find(tx_number);
    if (tx_it == tx_status.end())
    {
        auto insert_it =
            tx_status.emplace(std::piecewise_construct,
                              std::forward_as_tuple(tx_number),
                              std::forward_as_tuple(RangeGenerateSkStatus()));

        tx_it = insert_it.first;
    }

    RangeGenerateSkStatus &range_status = tx_it->second;
    auto range_it = range_status.find(partition_id);
    if (range_it == range_status.end())
    {
        auto insert_it =
            range_status.emplace(std::piecewise_construct,
                                 std::forward_as_tuple(partition_id),
                                 std::forward_as_tuple(tx_term));
        range_it = insert_it.first;
    }

    return &(range_it->second);
}

void LocalCcShards::ClearGenerateSkStatus(NodeGroupId ng_id,
                                          uint64_t tx_number,
                                          int32_t partition_id)
{
    std::unique_lock<std::mutex> lk(generate_sk_mux_);

    auto ng_it = generate_sk_status_.find(ng_id);
    assert(ng_it != generate_sk_status_.end());

    TxGenerateSkStatus &tx_status = ng_it->second;
    auto tx_it = tx_status.find(tx_number);
    assert(tx_it != tx_status.end());

    RangeGenerateSkStatus &range_status = tx_it->second;
    range_status.erase(partition_id);
}

void LocalCcShards::HeartbeatWorker()
{
    std::unique_lock<std::mutex> heartbeat_worker_lk(
        heartbeat_worker_ctx_.mux_);

    while (heartbeat_worker_ctx_.status_ == WorkerStatus::Active)
    {
        bool wait_res = heartbeat_worker_ctx_.cv_.wait_for(
            heartbeat_worker_lk,
            std::chrono::seconds(1),
            [this]
            {
                return heartbeat_worker_ctx_.status_ ==
                       WorkerStatus::Terminated;
            });

        if (!wait_res)
        {
            SendHeartbeat(heartbeat_worker_lk);
        }
    }
}

void LocalCcShards::SendHeartbeat(std::unique_lock<std::mutex> &worker_lk)
{
    auto sender = Sharder::Instance().GetCcStreamSender();
    if (sender)
    {
        std::vector<uint32_t> target_nodes;
        target_nodes.reserve(heartbeat_target_nodes_.size());

        for (const auto &node : heartbeat_target_nodes_)
        {
            target_nodes.push_back(node.first);
        }

        worker_lk.unlock();

        for (const auto &target_node : target_nodes)
        {
            remote::CcMessage message;
            message.set_type(remote::CcMessage::MessageType::
                                 CcMessage_MessageType_StandbyHeartbeatRequest);
            sender->SendMessageToNode(target_node, message);
        }

        worker_lk.lock();
    }
}

void LocalCcShards::AddHeartbeatTargetNode(uint32_t target_node,
                                           int64_t target_node_standby_term)
{
    std::lock_guard<std::mutex> heartbeat_lk(heartbeat_worker_ctx_.mux_);
    auto iter = heartbeat_target_nodes_.try_emplace(target_node,
                                                    target_node_standby_term);
    if (!iter.second)
    {
        if (iter.first->second < target_node_standby_term)
        {
            iter.first->second = target_node_standby_term;
        }
    }
}

void LocalCcShards::RemoveHeartbeatTargetNode(uint32_t target_node,
                                              int64_t target_node_standby_term)
{
    std::lock_guard<std::mutex> heartbeat_lk(heartbeat_worker_ctx_.mux_);
    auto iter = heartbeat_target_nodes_.find(target_node);
    if (iter != heartbeat_target_nodes_.end())
    {
        if (iter->second <= target_node_standby_term)
        {
            heartbeat_target_nodes_.erase(target_node);
        }
    }
}

void LocalCcShards::PurgeDeletedData()
{
    std::unique_lock<std::mutex> worker_lk(purge_deleted_worker_ctx_.mux_);
    KickoutCcEntryCc purge_cc;
    CcHandlerResult<Void> res(nullptr);
    std::mutex mux;
    std::condition_variable cv;
    bool done = false;
    res.post_lambda_ = [&done, &mux, &cv](CcHandlerResult<Void> *res)
    {
        std::unique_lock<std::mutex> lk(mux);
        done = true;
        cv.notify_one();
    };

    while (purge_deleted_worker_ctx_.status_ == WorkerStatus::Active)
    {
        bool wait_res = purge_deleted_worker_ctx_.cv_.wait_for(
            worker_lk,
            std::chrono::seconds(10),
            [this]
            {
                return purge_deleted_worker_ctx_.status_ ==
                       WorkerStatus::Terminated;
            });

        if (wait_res)
        {
            return;
        }

        int64_t candidate_standby_node_term =
            Sharder::Instance().CandidateStandbyNodeTerm();
        int64_t standby_node_term = Sharder::Instance().StandbyNodeTerm();
        bool is_standby_node =
            standby_node_term > 0 || candidate_standby_node_term > 0;
        std::vector<uint32_t> node_groups;
        if (is_standby_node)
        {
            node_groups.push_back(Sharder::Instance().NativeNodeGroup());
        }
        else
        {
            node_groups = Sharder::Instance().LocalNodeGroups();
        }
        // Send purge cc req to each core.
        for (uint32_t node_group : node_groups)
        {
            int64_t ng_term;
            if (is_standby_node)
            {
                ng_term =
                    std::max(standby_node_term, candidate_standby_node_term);
            }
            else
            {
                ng_term = Sharder::Instance().LeaderTerm(node_group);
            }

            if (ng_term < 0)
            {
                continue;
            }
            std::unordered_map<TableName, bool> tables =
                GetCatalogTableNameSnapshot(ng_id_, ClockTs());

            std::vector<TableName> table_names;
            for (auto &table : tables)
            {
                if (table.first.IsMeta())
                {
                    continue;
                }
                table_names.push_back(table.first);
            }

            for (auto &table_name : table_names)
            {
                res.Reset();
                {
                    std::unique_lock<std::mutex> lk(mux);
                    done = false;
                }
                purge_cc.Reset(table_name,
                               node_group,
                               &res,
                               Count(),
                               CleanType::CleanDeletedData);
                for (auto &shard : cc_shards_)
                {
                    shard->Enqueue(&purge_cc);
                }
                std::unique_lock<std::mutex> lk(mux);
                cv.wait(lk, [&done] { return done; });
            }
        }
    }
}

void LocalCcShards::KickoutDataForTest()
{
    KickoutCcEntryCc kickout_cc;
    CcHandlerResult<Void> res(nullptr);
    std::mutex mux;
    std::condition_variable cv;
    bool done = false;
    res.post_lambda_ = [&done, &mux, &cv](CcHandlerResult<Void> *res)
    {
        std::unique_lock<std::mutex> lk(mux);
        done = true;
        cv.notify_one();
    };

    std::unique_lock<std::mutex> worker_lk(kickout_data_test_worker_ctx_.mux_);

    while (kickout_data_test_worker_ctx_.status_ == WorkerStatus::Active)
    {
        worker_lk.unlock();

        int64_t candidate_standby_node_term =
            Sharder::Instance().CandidateStandbyNodeTerm();
        int64_t standby_node_term = Sharder::Instance().StandbyNodeTerm();
        bool is_standby_node =
            standby_node_term > 0 || candidate_standby_node_term > 0;
        std::vector<uint32_t> node_groups;
        if (is_standby_node)
        {
            node_groups.push_back(Sharder::Instance().NativeNodeGroup());
        }
        else
        {
            node_groups = Sharder::Instance().LocalNodeGroups();
        }
        // Send kickout cc req to each core.
        for (uint32_t node_group : node_groups)
        {
            int64_t ng_term;
            if (is_standby_node)
            {
                ng_term =
                    std::max(standby_node_term, candidate_standby_node_term);
            }
            else
            {
                ng_term = Sharder::Instance().LeaderTerm(node_group);
            }

            if (ng_term < 0)
            {
                continue;
            }
            std::unordered_map<TableName, bool> tables =
                GetCatalogTableNameSnapshot(ng_id_, ClockTs());

            std::vector<TableName> table_names;
            for (auto &table : tables)
            {
                if (table.first.IsMeta())
                {
                    continue;
                }
                table_names.push_back(table.first);
            }

            for (auto &table_name : table_names)
            {
                res.Reset();
                {
                    std::unique_lock<std::mutex> lk(mux);
                    done = false;
                }

                kickout_cc.Reset(table_name,
                                 node_group,
                                 &res,
                                 Count(),
                                 CleanType::CleanDataForTest);
                for (auto &shard : cc_shards_)
                {
                    shard->Enqueue(&kickout_cc);
                }

                std::unique_lock<std::mutex> lk(mux);
                cv.wait(lk, [&done] { return done; });
            }
        }

        worker_lk.lock();
    }
}

void LocalCcShards::ReportHashPartitionCkptHeapUsage()
{
#if defined(WITH_JEMALLOC)
    size_t committed = 0;
    size_t allocated = 0;
    GetJemallocArenaStat(hash_partition_ckpt_arena_id_, committed, allocated);
    if (committed != 0)
    {
        LOG(INFO) << "ckpt hash partition heap memory usage report, committed "
                  << committed << ", allocated " << allocated << ", frag ratio "
                  << std::setprecision(2)
                  << 100 * (static_cast<float>(committed - allocated) /
                            static_cast<float>(committed));
    }
#else
    std::unique_lock<std::mutex> heap_lk(hash_partition_ckpt_heap_mux_);
    mi_threadid_t prev_thd = mi_override_thread(hash_partition_main_thread_id_);
    mi_heap_t *prev_heap = mi_heap_set_default(hash_partition_ckpt_heap_);
    int64_t allocated, committed;
    mi_thread_stats(&allocated, &committed);
    if (committed != 0)
    {
        LOG(INFO) << "ckpt hash partition heap memory usage report, committed "
                  << committed << ", allocated " << allocated << ", frag ratio "
                  << std::setprecision(2)
                  << 100 * (static_cast<float>(committed - allocated) /
                            static_cast<float>(committed));
    }
    mi_override_thread(prev_thd);
    mi_heap_set_default(prev_heap);
#endif
}

bool LocalCcShards::GetNextRangePartitionId(const TableName &tablename,
                                            const TableSchema *table_schema,
                                            uint32_t range_cnt,
                                            int32_t &out_next_partition_id)
{
    int64_t reserved_cnt;
    uint64_t key_schema_ts;
    if (tablename.IsBase())
    {
        key_schema_ts = table_schema->KeySchema()->SchemaTs();
    }
    else
    {
        key_schema_ts = table_schema->IndexKeySchema(tablename)->SchemaTs();
    }

    int64_t first_reserved_id = -1;
    bool res = Sequences::ApplyIdOfTableRangePartition(
        tablename, range_cnt, first_reserved_id, reserved_cnt, key_schema_ts);

    if (!res)
    {
        LOG(ERROR) << "GetNextRangePartitionId failed for not assigned "
                      "enough range ids, first_reserved_id:"
                   << first_reserved_id << ", reserved_cnt: " << reserved_cnt;
        return false;
    }
    assert(reserved_cnt == range_cnt);

    assert(first_reserved_id + reserved_cnt < INT32_MAX);

    out_next_partition_id = static_cast<int32_t>(first_reserved_id);
    return true;
}

void LocalCcShards::RangeCacheSender::FindSliceKeys(
    const std::vector<FlushRecord> &batch_records,
    const UpdateSliceStatus &update_slice_status)
{
    if (channel_ == nullptr)
    {
        // Fail to establish the channel to the tx node. Just skip the cache
        // sending.
        LOG(WARNING) << "FindSliceKeys: Failed to init the channel of ng#"
                     << new_range_owner_ << ". Skip sending cache.";
        return;
    }

    TxKey first_key = batch_records[0].Key();
    TxKey last_key = batch_records.at(batch_records.size() - 1).Key();
    std::vector<const StoreSlice *> batch_slices =
        store_range_->SubSlices(first_key, last_key, true);

    batch_slice_key_vec_.clear();
    batch_slice_key_vec_.reserve(batch_slices.size());

    for (size_t i = 0; i < batch_slices.size() - 1; ++i)
    {
        batch_slice_key_vec_.emplace_back(batch_slices[i]->StartTxKey());
    }

    if (!update_slice_status.paused_slice_)
    {
        batch_slice_key_vec_.emplace_back(batch_slices.back()->StartTxKey());
    }
    else
    {
        // The slice corresponding to the last part of this batch items need to
        // be split, but because it is not sure whether all the data has been
        // scanned, the slice spec in memory has not been actually updated.
        // Therefore, we cannot use FindSlice to find a sub-slice spec, but use
        // update_slice_status.paused_split_keys_ to determine the boundaries of
        // each sub-slice.
        auto &dirty_slices = update_slice_status.paused_split_keys_;
        auto first_it =
            std::lower_bound(dirty_slices.begin(),
                             dirty_slices.end(),
                             first_key,
                             [](const SliceChangeInfo &slice, const TxKey &key)
                             { return slice.key_ < key; });
        assert(first_it != dirty_slices.end());
        if (first_it != dirty_slices.begin())
        {
            --first_it;
        }

        while (first_it != dirty_slices.end())
        {
            batch_slice_key_vec_.emplace_back(first_it->key_.GetShallowCopy());
            ++first_it;
        }
    }
}

void LocalCcShards::RangeCacheSender::AppendSliceDataRequest(
    const std::vector<FlushRecord> &batch_records, bool all_data_exported)
{
    if (channel_ == nullptr)
    {
        // Fail to establish the channel to the tx node. Just skip the cache
        // sending.
        LOG(WARNING) << "AppendSliceRequest: Failed to init the channel of ng#"
                     << new_range_owner_ << ". Skip sending cache.";
        return;
    }

    // Encode this batch FlushRecord into rpc request.
    UploadBatchSlicesClosure *upload_batch_closure = nullptr;
    remote::UploadBatchSlicesRequest *batch_req_ptr = nullptr;
    std::string *keys_str = nullptr;
    std::string *recs_str = nullptr;
    std::string *rec_status_str = nullptr;
    std::string *commit_ts_str = nullptr;
    uint32_t batch_size = 0;

    assert(batch_slice_key_vec_.size() > 0);
    if (paused_pos_.paused_batch_size_ != 0)
    {
        assert(!closure_vec_->empty());
        upload_batch_closure = closure_vec_->back();
        batch_req_ptr = upload_batch_closure->UploadBatchRequest();
        assert(upload_batch_closure);
        keys_str = batch_req_ptr->mutable_keys();
        recs_str = batch_req_ptr->mutable_records();
        rec_status_str = batch_req_ptr->mutable_rec_status();
        commit_ts_str = batch_req_ptr->mutable_commit_ts();

        batch_size = paused_pos_.paused_batch_size_;

        if (!(paused_pos_.paused_slice_start_key_ == batch_slice_key_vec_[0]))
        {
            assert(paused_pos_.paused_slice_start_key_ <
                   batch_slice_key_vec_[0]);
            // The paused (sub)slice has already finished.
            batch_req_ptr->set_batch_size(batch_size);
            batch_req_ptr->add_slices_idxs(slice_idx_++);

            paused_pos_.Reset();

            upload_batch_closure =
                batch_req_ptr->ByteSizeLong() > BATCH_BYTES_SIZE
                    ? nullptr
                    : closure_vec_->back();
        }
        // else: Continue to encoding the items of the paused (sub)slice.
    }
    // else: The first batch flush records of this range.

    auto slice_key_it = batch_slice_key_vec_.begin();
    auto slice_data_it = batch_records.begin();
    while (slice_data_it != batch_records.end())
    {
        assert(*slice_key_it <= slice_data_it->Key());
        // Find the final slice boundary.
        TxKey subslice_end_key =
            std::next(slice_key_it) != batch_slice_key_vec_.end()
                ? std::next(slice_key_it)->GetShallowCopy()
                : store_range_->RangeEndTxKey();

        auto slice_end_it =
            std::lower_bound(slice_data_it,
                             batch_records.end(),
                             subslice_end_key,
                             [](const FlushRecord &rec, const TxKey &key)
                             { return rec.Key() < key; });

        if (upload_batch_closure == nullptr ||
            (batch_req_ptr->ByteSizeLong() > BATCH_BYTES_SIZE &&
             paused_pos_.paused_batch_size_ == 0))
        {
            // New batch
            upload_batch_closure =
                new UploadBatchSlicesClosure(nullptr, 10000, false);
            closure_vec_->push_back(upload_batch_closure);
            upload_batch_closure->SetChannel(dest_node_id_, channel_);
            batch_req_ptr = upload_batch_closure->UploadBatchRequest();

            // Init this request
            batch_req_ptr->set_table_name_str(table_name_.String());
            batch_req_ptr->set_table_type(
                remote::ToRemoteType::ConvertTableType(table_name_.Type()));
            batch_req_ptr->set_table_engine(
                remote::ToRemoteType::ConvertTableEngine(table_name_.Engine()));
            batch_req_ptr->set_node_group_id(new_range_owner_);
            // The term will be fixed when send the request.
            batch_req_ptr->set_node_group_term(INIT_TERM);

            // range, dirty range, dirty version
            batch_req_ptr->set_partition_id(old_range_id_);
            batch_req_ptr->set_new_partition_id(new_range_id_);
            batch_req_ptr->set_version_ts(version_ts_);

            batch_req_ptr->clear_keys();
            batch_req_ptr->clear_rec_status();
            batch_req_ptr->clear_commit_ts();
            batch_req_ptr->clear_records();

            keys_str = batch_req_ptr->mutable_keys();
            recs_str = batch_req_ptr->mutable_records();
            rec_status_str = batch_req_ptr->mutable_rec_status();
            commit_ts_str = batch_req_ptr->mutable_commit_ts();

            batch_size = 0;
        }

        // The slice (the slice is the final slice, that is to say, if one old
        // slice need to be split, the slice is a sub-slice) data.
        for (; slice_data_it != slice_end_it; ++slice_data_it)
        {
            slice_data_it->Key().Serialize(*keys_str);
            if (slice_data_it->payload_status_ == RecordStatus::Normal)
            {
                slice_data_it->Payload()->Serialize(*recs_str);
            }
            size_t len_sizeof = sizeof(RecordStatus);
            const char *val_ptr =
                reinterpret_cast<const char *>(&slice_data_it->payload_status_);
            rec_status_str->append(val_ptr, len_sizeof);
            len_sizeof = sizeof(uint64_t);
            val_ptr =
                reinterpret_cast<const char *>(&slice_data_it->commit_ts_);
            commit_ts_str->append(val_ptr, len_sizeof);

            ++batch_size;
        }

        // Reset the paused position
        paused_pos_.Reset();

        if (slice_end_it != batch_records.end() || all_data_exported)
        {
            // All items of this slice (the new slice) have been exported.
            batch_req_ptr->set_batch_size(batch_size);
            batch_req_ptr->add_slices_idxs(slice_idx_++);

            // Forward the slice key iterator
            ++slice_key_it;
        }
        else
        {
            assert(slice_data_it == batch_records.end() &&
                   std::next(slice_key_it) == batch_slice_key_vec_.end());
            // The pause slice start key, the slice is the one to which the last
            // part of this batch items belong to.
            paused_pos_.SetPausedPos(batch_size, std::move(*slice_key_it));
        }
    }
}

void LocalCcShards::RangeCacheSender::SendRangeCacheRequest(
    const TxKey &start_key, const TxKey &end_key)
{
    if (channel_ == nullptr)
    {
        // Fail to establish the channel to the tx node. Just skip the cache
        // sending.
        LOG(WARNING)
            << "SendRangeCacheRequest: Failed to init the channel of ng#"
            << new_range_owner_ << ". Skip sending cache.";
        return;
    }

    slices_vec_ = store_range_->SubSlices(start_key, end_key);
    assert(slices_vec_.size() == slice_idx_);

    LOG(INFO)
        << "Begin upload new range slices and records to future owner, range#"
        << old_range_id_ << ", new_range#" << new_range_id_
        << ", new_range_owner:" << new_range_owner_;

    // 1- upload dirty range slices info (with PartiallyCached)
    int64_t ng_term = INIT_TERM;
    remote::CcRpcService_Stub stub(channel_.get());

    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);
    cntl.set_write_to_socket_in_background(true);
    // cntl.ignore_eovercrowded(true);
    remote::UploadRangeSlicesRequest req;
    remote::UploadRangeSlicesResponse resp;

    req.set_node_group_id(new_range_owner_);
    req.set_ng_term(ng_term);
    req.set_table_name_str(table_name_.String());
    req.set_table_engine(
        remote::ToRemoteType::ConvertTableEngine(table_name_.Engine()));
    req.set_old_partition_id(old_range_id_);
    req.set_version_ts(version_ts_);
    req.set_new_partition_id(new_range_id_);
    req.set_new_slices_num(slices_vec_.size());
    std::string *keys_str = req.mutable_new_slices_keys();
    std::string *sizes_str = req.mutable_new_slices_sizes();
    std::string *status_str = req.mutable_new_slices_status();
    for (const StoreSlice *slice : slices_vec_)
    {
        // key
        TxKey slice_key = slice->StartTxKey();
        slice_key.Serialize(*keys_str);
        // size
        // If post ckpt size of the slice is UINT64_MAX, it means that there is
        // no item need to be ckpt in this slice, so should use the current size
        // of the slice.
        uint32_t slice_size =
            (slice->PostCkptSize() == UINT64_MAX ? slice->Size()
                                                 : slice->PostCkptSize());
        const char *slice_size_ptr =
            reinterpret_cast<const char *>(&slice_size);
        sizes_str->append(slice_size_ptr, sizeof(slice_size));
        // status
        int8_t slice_status = static_cast<int8_t>(SliceStatus::PartiallyCached);
        const char *slice_status_ptr =
            reinterpret_cast<const char *>(&slice_status);
        status_str->append(slice_status_ptr, sizeof(slice_status));
    }
    req.set_has_dml_since_ddl(store_range_->HasDmlSinceDdl());
    stub.UploadRangeSlices(&cntl, &req, &resp, nullptr);

    if (cntl.Failed())
    {
        LOG(WARNING) << "SendRangeCacheRequest: Fail to upload dirty range "
                        "slices RPC ng#"
                     << new_range_owner_ << ". Error code: " << cntl.ErrorCode()
                     << ". Msg: " << cntl.ErrorText();
        return;
    }

    if (remote::ToLocalType::ConvertCcErrorCode(resp.error_code()) !=
        CcErrorCode::NO_ERROR)
    {
        LOG(WARNING) << "SendRangeCacheRequest: New owner ng#"
                     << new_range_owner_
                     << " reject to receive dirty range data";
        return;
    }

    ng_term = resp.ng_term();
    LOG(INFO) << "SendRangeCacheRequest: Uploaded new range slices info to "
                 "future owner, range#"
              << old_range_id_ << ", new_range#" << new_range_id_;

    // 2- upload records belongs to dirty range
    assert(closure_vec_->size() > 0);
    LOG(INFO) << "SendRangeCacheRequest: Sending range data, old_range_id: "
              << old_range_id_ << ", to upload " << closure_vec_->size()
              << " batches to ng#" << new_range_owner_;

    uint32_t sender_cnt = 5;
    auto closures_idx = std::make_shared<std::atomic_uint64_t>(sender_cnt);
    for (size_t i = 0; i < sender_cnt && i < closure_vec_->size(); ++i)
    {
        UploadBatchSlicesClosure *head = closure_vec_->at(i);
        if (sender_cnt < closure_vec_->size())
        {
            head->SetPostLambda(
                [range_id = old_range_id_,
                 closures_idx,
                 vec_sptr = closure_vec_,
                 ng_term](CcErrorCode res_code, int32_t dest_ng_term)
                {
                    auto &vec = *vec_sptr;
                    size_t begin_idx = closures_idx->fetch_add(5);
                    size_t vec_size = vec.size();
                    size_t end_idx = std::min(begin_idx + 5, vec_size);
                    bool rejected = false;
                    while (begin_idx < end_idx)
                    {
                        std::unique_ptr<UploadBatchSlicesClosure> closure(
                            vec[begin_idx]);
                        begin_idx++;

                        if (begin_idx == end_idx && begin_idx < vec_size)
                        {
                            begin_idx = closures_idx->fetch_add(5);
                            end_idx = std::min(begin_idx + 5, vec_size);
                        }

                        if (rejected)
                        {
                            // Must continue to delete left closures in
                            // closures_sptr.
                            continue;
                        }

                        remote::CcRpcService_Stub stub(closure->Channel());
                        brpc::Controller *cntl_ptr = closure->Controller();
                        cntl_ptr->Reset();
                        cntl_ptr->set_max_retry(3);
                        cntl_ptr->set_timeout_ms(closure->TimeoutValue());
                        // Fix the term
                        closure->UploadBatchRequest()->set_node_group_term(
                            ng_term);
                        stub.UploadBatchSlices(cntl_ptr,
                                               closure->UploadBatchRequest(),
                                               closure->UploadBatchResponse(),
                                               nullptr);

                        auto *resp = closure->UploadBatchResponse();
                        if (cntl_ptr->Failed())
                        {
                            DLOG(INFO) << "UploadBatch rpc to node#"
                                       << closure->NodeId() << " failed.";
                        }
                        else if (resp->error_code() ==
                                 static_cast<uint32_t>(
                                     CcErrorCode::UPLOAD_BATCH_REJECTED))
                        {
                            rejected = true;
                            DLOG(INFO) << "UploadBatch rpc to node#"
                                       << closure->NodeId()
                                       << " is reject for no free memory";
                        }
                    }

                    LOG(INFO) << "Old_Range#" << range_id
                              << " finished upload_batch_closure (" << begin_idx
                              << ", all_rpc_cnt" << vec_size << ").";
                });
        }

        remote::CcRpcService_Stub stub(head->Channel());
        brpc::Controller *cntl_ptr = head->Controller();
        cntl_ptr->set_timeout_ms(head->TimeoutValue());
        // Fix the term
        head->UploadBatchRequest()->set_node_group_term(ng_term);
        stub.UploadBatchSlices(cntl_ptr,
                               head->UploadBatchRequest(),
                               head->UploadBatchResponse(),
                               head);
    }
}

void LocalCcShards::FastMetaDataMutex::lock()
{
    meta_data_mux_.lock();

    for (auto *ptr : mux_ptrs_)
    {
        assert(ptr != nullptr);
        int32_t expected = ptr->load(std::memory_order_relaxed);
        for (;;)
        {
            // Wait for writers that already hold the bit.
            while ((expected & kWriterMask) != 0)
            {
                expected = ptr->load(std::memory_order_acquire);
            }

            const int32_t desired = expected | kWriterMask;
            if (ptr->compare_exchange_weak(expected,
                                           desired,
                                           std::memory_order_acq_rel,
                                           std::memory_order_relaxed))
            {
                break;
            }
        }

        // Wait until all readers drain so only the writer bit remains.
        while ((ptr->load(std::memory_order_acquire) & ~kWriterMask) != 0)
        {
        }
    }
}

void LocalCcShards::FastMetaDataMutex::lock_shared()
{
    auto *ptr = mux_ptrs_[tls_shard_idx];
    int32_t expected = ptr->load(std::memory_order_acquire);
    for (;;)
    {
        if ((expected & kWriterMask) != 0)
        {
            expected = ptr->load(std::memory_order_acquire);
            continue;
        }

        const int32_t desired = expected + 1;
        if (ptr->compare_exchange_weak(expected,
                                       desired,
                                       std::memory_order_acq_rel,
                                       std::memory_order_relaxed))
        {
            break;
        }
    }
}

void LocalCcShards::FastMetaDataMutex::unlock()
{
    meta_data_mux_.unlock();

    for (auto *ptr : mux_ptrs_)
    {
        assert(ptr != nullptr);
        ptr->fetch_and(~kWriterMask, std::memory_order_release);
    }
}

void LocalCcShards::FastMetaDataMutex::unlock_shared()
{
    auto *ptr = mux_ptrs_[tls_shard_idx];
    assert(ptr != nullptr);

    ptr->fetch_sub(1, std::memory_order_release);
}

}  // namespace txservice
