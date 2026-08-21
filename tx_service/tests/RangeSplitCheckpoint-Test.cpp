/**
 * Regression coverage for checkpoint completion while a range is split.
 */
#include <catch2/catch_all.hpp>

#define protected public
#define private public

#include "cc_entry.h"
#include "cc_req_misc.h"
#include "cc_shard.h"
#include "data_sync_task.h"
#include "include/mock/mock_catalog_factory.h"
#include "local_cc_shards.h"
#include "range_record.h"
#include "template_cc_map.h"
#include "tx_key.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
namespace
{
using TestKey = CompositeKey<std::string, int>;
using TestRecord = CompositeRecord<int>;
using TestCcMap = TemplateCcMap<TestKey, TestRecord, true, true>;
using TestCcEntry = CcEntry<TestKey, TestRecord, true, true>;

struct SplitCheckpointFixture
{
    std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs{
        {0, {NodeConfig(0, "127.0.0.1", 8600)}}};
    std::map<std::string, uint32_t> tx_cnf{
        {"node_memory_limit_mb", 1000},
        {"enable_key_cache", 0},
        {"reltime_sampling", 0},
        {"range_split_worker_num", 1},
        {"range_slice_memory_limit_percent", 20},
        {"core_num", 2},
        {"realtime_sampling", 0},
        {"checkpointer_interval", 10},
        {"checkpointer_delay_seconds", 0},
        {"checkpointer_min_ckpt_request_interval", 5},
        {"enable_shard_heap_defragment", 0},
        {"node_log_limit_mb", 1000},
        {"collect_active_tx_ts_interval_seconds", 2},
        {"rep_group_cnt", 1},
    };
    MockCatalogFactory mock_catalog_factory;
    CatalogFactory *catalog_factory[5] = {
        &mock_catalog_factory,
        &mock_catalog_factory,
        &mock_catalog_factory,
        &mock_catalog_factory,
        &mock_catalog_factory,
    };
    LocalCcShards local_cc_shards;
    std::string raft_path;

    SplitCheckpointFixture()
        : local_cc_shards(0,
                          0,
                          tx_cnf,
                          catalog_factory,
                          nullptr,
                          &ng_configs,
                          2,
                          nullptr,
                          nullptr,
                          true)
    {
        local_cc_shards.BindThreadToFastMetaDataShard(0);
        local_cc_shards.GetCcShard(0)->Init();
        local_cc_shards.GetCcShard(1)->Init();
        auto &sharder = Sharder::Instance(0,
                                           &ng_configs,
                                           0,
                                           nullptr,
                                           nullptr,
                                           &local_cc_shards,
                                           nullptr,
                                           &raft_path);
        // The lightweight fixture does not call Sharder::Init(), which is
        // responsible for enabling leader-term lookups in a server process.
        // Enable that gate explicitly so UpdateCceCkptTsCc takes its normal
        // leader-term path.
        sharder.cc_nodes_init_.store(true, std::memory_order_release);
        sharder.SetStandbyNodeTerm(-1);
        sharder.SetLeaderTerm(0, 1);
    }

    ~SplitCheckpointFixture()
    {
        local_cc_shards.Terminate();
    }
};
}  // namespace

TEST_CASE("split-range checkpoint updates source CCE shard", "[checkpoint]")
{
    SplitCheckpointFixture fixture;
    TableName table_name(std::string("split_checkpoint"),
                         TableType::Primary,
                         TableEngine::EloqSql);
    CcShard *source_shard = fixture.local_cc_shards.GetCcShard(0);
    source_shard->native_ccms_.try_emplace(
        table_name,
        std::make_unique<TestCcMap>(
            source_shard, 0, table_name, 1, nullptr, true));

    // Source range 0 splits at this key into destination range 1.  With two
    // local shards, the child ID maps to core 1 while its CCE remains in the
    // source CcMap on core 0 until split cleanup.
    TestKey source_start = std::make_tuple(std::string("split_checkpoint"), 0);
    TestKey child_start =
        std::make_tuple(std::string("split_checkpoint"), 100);
    TemplateTableRangeEntry<TestKey> source_range(
        &source_start, 10, 0);
    std::vector<TxKey> split_keys;
    split_keys.emplace_back(&child_start);
    source_range.UploadNewRangeInfo(std::move(split_keys), {1}, 20);

    TxKey child_start_key(&child_start);
    TxKey source_end_key(&child_start);
    DataSyncTask split_child_task(table_name,
                                  0,
                                  1,
                                  nullptr,
                                  &source_range,
                                  child_start_key,
                                  source_end_key,
                                  30,
                                  true,
                                  false,
                                  0,
                                  nullptr,
                                  nullptr);
    REQUIRE(split_child_task.id_ == 1);

    // The key hash maps to core 1, but range data is owned by the source
    // range's core (core 0), not by the key hash or child range ID.
    int record_key_suffix = 0;
    TestKey record_key = std::make_tuple(
        std::string("split_checkpoint"), record_key_suffix);
    while ((TxKey(&record_key).Hash() & 0x3FF) % 2 != 1)
    {
        record_key = std::make_tuple(std::string("split_checkpoint"),
                                     ++record_key_suffix);
    }
    size_t cce_owner_core = split_child_task.CheckpointCceOwnerCore(2);
    REQUIRE(cce_owner_core == 0);
    REQUIRE(cce_owner_core != static_cast<size_t>(split_child_task.id_));

    TableName hash_table_name(std::string("split_checkpoint_hash"),
                              TableType::Primary,
                              TableEngine::EloqKv);
    DataSyncTask hash_task(hash_table_name,
                           3,
                           0,
                           0,
                           1,
                           0,
                           nullptr,
                           false,
                           false,
                           nullptr);
    REQUIRE(hash_task.CheckpointCceOwnerCore(2) == 1);

    auto *source_map =
        static_cast<TestCcMap *>(source_shard->GetCcm(table_name, 0));
    REQUIRE(source_map != nullptr);
    bool emplace = false;
    auto it = source_map->FindEmplace(record_key, &emplace, false, false);
    REQUIRE(emplace);
    TestCcEntry *cce = it->second;
    bool was_dirty = cce->IsDirty();
    cce->SetCommitTsPayloadStatus(40, RecordStatus::Normal);
    source_map->OnCommittedUpdate(cce, was_dirty);
    cce->SetBeingCkpt();
    REQUIRE(cce->IsDirty());
    REQUIRE(cce->GetBeingCkpt());
    REQUIRE(source_map->dirty_data_key_count_ == 1);

    absl::flat_hash_map<size_t, std::vector<UpdateCceCkptTsCc::CkptTsEntry>>
        updates;
    updates[cce_owner_core].emplace_back(cce, 40, 321);
    UpdateCceCkptTsCc update_req(0, 1, table_name, updates);
    update_req.Execute(*source_shard);

    REQUIRE(update_req.IsFinished());
    REQUIRE(cce->CkptTs() == 40);
    REQUIRE_FALSE(cce->GetBeingCkpt());
    REQUIRE_FALSE(cce->IsDirty());
    REQUIRE(cce->entry_info_.DataStoreSize() == 321);
    REQUIRE(source_map->dirty_data_key_count_ == 0);
}

}  // namespace txservice
