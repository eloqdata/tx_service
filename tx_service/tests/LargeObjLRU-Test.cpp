/**
 * Basic migration consistency test for moving an entry between pages.
 */
#include <catch2/catch_all.hpp>

#define protected public
#define private public

#include "cc/cc_entry.h"
#include "eloq_string_key_record.h"
#include "include/mock/mock_catalog_factory.h"
#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{

static MockCatalogFactory mock_catalog_factory{};

TEST_CASE("Large object migration basic consistency", "[large-obj-lru]")
{
    using KeyT = CompositeKey<std::string, int>;
    using ValT = CompositeRecord<int>;

    // Create sentinel pages and two normal pages linked between them so that
    // both prev_page_ and next_page_ are non-null (not sentinels).
    CcPage<KeyT, ValT, true, true> neg_inf(nullptr);
    CcPage<KeyT, ValT, true, true> pos_inf(nullptr);

    CcPage<KeyT, ValT, true, true> old_page(nullptr, &neg_inf, &pos_inf);
    CcPage<KeyT, ValT, true, true> new_page(nullptr, &old_page, &pos_inf);

    // Prepare two keys and emplace them into old_page
    KeyT k1(std::make_tuple(std::string("t"), 1));
    KeyT k2(std::make_tuple(std::string("t"), 2));

    size_t idx1 = old_page.Emplace(k1);
    size_t idx2 = old_page.Emplace(k2);

    REQUIRE(old_page.Size() == 2);

    // Populate payloads for entries to simulate normal objects
    old_page.Entry(idx1)->payload_.cur_payload_ = std::make_unique<ValT>(10);
    old_page.Entry(idx2)->payload_.cur_payload_ = std::make_unique<ValT>(20);

    // Move the second entry to new_page using MoveEntry + Emplace(unique_ptr)
    size_t move_idx = old_page.Find(k2);
    REQUIRE(move_idx < old_page.Size());

    auto moved_uptr = old_page.MoveEntry(move_idx);
    old_page.Remove(move_idx);
    new_page.Emplace(k2, std::move(moved_uptr));
    new_page.large_obj_page_ = true;

    // After move, old page size should have decreased and key k2 no longer
    // be present in old_page.
    REQUIRE(old_page.Size() == 1);
    REQUIRE(old_page.Find(k2) == old_page.Size());

    // New page contains the moved key
    const KeyT *nk = new_page.Key(0);
    REQUIRE(nk != nullptr);
    REQUIRE(*nk == k2);
}

// Helper to create shard and local shards configured for LO_LRU.
static std::pair<std::unique_ptr<LocalCcShards>, std::unique_ptr<CcShard>>
make_shard()
{
    std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs{
        {0, {NodeConfig(0, "127.0.0.1", 8600)}}};
    std::map<std::string, uint32_t> tx_cnf{
        {"node_memory_limit_mb", 1000},
        {"enable_key_cache", 0},
        {"reltime_sampling", 0},
        {"range_split_worker_num", 1},
        {"range_slice_memory_limit_percent", 20},
        {"core_num", 1},
        {"realtime_sampling", 0},
        {"checkpointer_interval", 10},
        {"checkpointer_delay_seconds", 0},
        {"checkpointer_min_ckpt_request_interval", 5},
        {"enable_shard_heap_defragment", 0},
        {"node_log_limit_mb", 1000},
        {"collect_active_tx_ts_interval_seconds", 2},
        {"rep_group_cnt", 1},
    };

    CatalogFactory *catalog_factory[5] = {&mock_catalog_factory,
                                          &mock_catalog_factory,
                                          &mock_catalog_factory,
                                          &mock_catalog_factory,
                                          &mock_catalog_factory};

    auto local = std::make_unique<LocalCcShards>(0,
                                                 0,
                                                 tx_cnf,
                                                 catalog_factory,
                                                 nullptr,
                                                 &ng_configs,
                                                 2,
                                                 nullptr,
                                                 nullptr,
                                                 true);
    // Use LO_LRU for this shard
    local->SetupPolicyLoLRU(1);

    auto shard = std::make_unique<CcShard>(0,
                                           1,
                                           10000,
                                           false,
                                           0,
                                           *local,
                                           catalog_factory,
                                           nullptr,
                                           &ng_configs,
                                           2);
    shard->Init();
    std::string raft_path("");
    Sharder::Instance(
        0, &ng_configs, 0, nullptr, nullptr, local.get(), nullptr, &raft_path);
    return {std::move(local), std::move(shard)};
}

// Configurable shard factory for integration test
static std::pair<std::unique_ptr<LocalCcShards>, std::unique_ptr<CcShard>>
make_shard_config(uint32_t node_memory_limit_mb,
                  uint32_t large_obj_threshold_kb)
{
    std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs{
        {0, {NodeConfig(0, "127.0.0.1", 8600)}}};
    std::map<std::string, uint32_t> tx_cnf{
        {"node_memory_limit_mb", node_memory_limit_mb},
        {"enable_key_cache", 0},
        {"reltime_sampling", 0},
        {"range_split_worker_num", 1},
        {"range_slice_memory_limit_percent", 20},
        {"core_num", 1},
        {"realtime_sampling", 0},
        {"checkpointer_interval", 10},
        {"checkpointer_delay_seconds", 0},
        {"checkpointer_min_ckpt_request_interval", 5},
        {"enable_shard_heap_defragment", 0},
        {"node_log_limit_mb", 1000},
        {"collect_active_tx_ts_interval_seconds", 2},
        {"rep_group_cnt", 1},
    };

    CatalogFactory *catalog_factory[5] = {&mock_catalog_factory,
                                          &mock_catalog_factory,
                                          &mock_catalog_factory,
                                          &mock_catalog_factory,
                                          &mock_catalog_factory};

    auto local = std::make_unique<LocalCcShards>(0,
                                                 0,
                                                 tx_cnf,
                                                 catalog_factory,
                                                 nullptr,
                                                 &ng_configs,
                                                 2,
                                                 nullptr,
                                                 nullptr,
                                                 true);
    // Use LO_LRU for this shard with provided threshold
    local->SetupPolicyLoLRU(large_obj_threshold_kb);

    auto shard = std::make_unique<CcShard>(0,
                                           1,
                                           10000,
                                           false,
                                           0,
                                           *local,
                                           catalog_factory,
                                           nullptr,
                                           &ng_configs,
                                           2);
    shard->Init();
    std::string raft_path("");
    Sharder::Instance(
        0, &ng_configs, 0, nullptr, nullptr, local.get(), nullptr, &raft_path);
    return {std::move(local), std::move(shard)};
}

// Helper to create a TemplateCcMap and four pages (neg_inf, pos_inf, p1, p2).
template <typename KeyT, typename ValT>
static std::tuple<CcPage<KeyT, ValT, true, true> *,
                  CcPage<KeyT, ValT, true, true> *,
                  CcPage<KeyT, ValT, true, true> *,
                  CcPage<KeyT, ValT, true, true> *>
make_ccmap_and_pages(
    CcShard &sh,
    std::unique_ptr<TemplateCcMap<KeyT, ValT, true, true>> &ccmap_uptr)
{
    TableName tname(
        std::string("tbl"), TableType::Primary, TableEngine::EloqSql);
    ccmap_uptr = std::make_unique<TemplateCcMap<KeyT, ValT, true, true>>(
        &sh, 0, tname, 1, nullptr, true);
    auto *cc_map = ccmap_uptr.get();
    auto *neg_inf = new CcPage<KeyT, ValT, true, true>(cc_map);
    auto *pos_inf = new CcPage<KeyT, ValT, true, true>(cc_map);
    auto *p1 = new CcPage<KeyT, ValT, true, true>(cc_map, neg_inf, pos_inf);
    auto *p2 = new CcPage<KeyT, ValT, true, true>(cc_map, p1, pos_inf);
    return {neg_inf, pos_inf, p1, p2};
}

TEST_CASE("LRU scenario A: small then large", "[large-obj-lru][lru-A]")
{
    auto [local, shard_uptr] = make_shard();
    CcShard &shard = *shard_uptr;

    std::unique_ptr<TemplateCcMap<CompositeKey<std::string, int>,
                                  CompositeRecord<int>,
                                  true,
                                  true>>
        ccmap_uptr;
    CcPage<CompositeKey<std::string, int>, CompositeRecord<int>, true, true>
        *neg_inf, *pos_inf, *p1, *p2;
    std::tie(neg_inf, pos_inf, p1, p2) =
        make_ccmap_and_pages<CompositeKey<std::string, int>,
                             CompositeRecord<int>>(shard, ccmap_uptr);

    p2->large_obj_page_ = true;  // mark p2 as large

    shard.UpdateLruList(p1, false);
    uint64_t t1 = p1->last_access_ts_;
    REQUIRE(t1 > 0);
    shard.UpdateLruList(p2, false);
    uint64_t t2 = p2->last_access_ts_;
    REQUIRE(t2 > t1);
    REQUIRE(p2->lru_prev_ == p1);

    delete neg_inf;
    delete pos_inf;
    delete p1;
    delete p2;
}

TEST_CASE("LRU scenario B: large then small", "[large-obj-lru][lru-B]")
{
    auto [local, shard_uptr] = make_shard();
    CcShard &shard = *shard_uptr;

    std::unique_ptr<TemplateCcMap<CompositeKey<std::string, int>,
                                  CompositeRecord<int>,
                                  true,
                                  true>>
        ccmap_uptr;
    CcPage<CompositeKey<std::string, int>, CompositeRecord<int>, true, true>
        *neg_inf, *pos_inf, *p1, *p2;
    std::tie(neg_inf, pos_inf, p1, p2) =
        make_ccmap_and_pages<CompositeKey<std::string, int>,
                             CompositeRecord<int>>(shard, ccmap_uptr);
    p2->large_obj_page_ = true;  // p2 large

    shard.UpdateLruList(p2, false);
    uint64_t tL = p2->last_access_ts_;
    REQUIRE(tL > 0);

    shard.UpdateLruList(p1, false);
    uint64_t tS = p1->last_access_ts_;
    REQUIRE(tS > tL);

    // small page should be inserted before protected head (which is p2)
    REQUIRE(p1->lru_next_ == p2);

    delete neg_inf;
    delete pos_inf;
    delete p1;
    delete p2;
}

TEST_CASE("LRU scenario C: two larges then small", "[large-obj-lru][lru-C]")
{
    auto [local, shard_uptr] = make_shard();
    CcShard &shard = *shard_uptr;

    std::unique_ptr<TemplateCcMap<CompositeKey<std::string, int>,
                                  CompositeRecord<int>,
                                  true,
                                  true>>
        ccmap_uptr;
    CcPage<CompositeKey<std::string, int>, CompositeRecord<int>, true, true>
        *neg_inf, *pos_inf, *l1, *l2;
    std::tie(neg_inf, pos_inf, l1, l2) =
        make_ccmap_and_pages<CompositeKey<std::string, int>,
                             CompositeRecord<int>>(shard, ccmap_uptr);
    l1->large_obj_page_ = true;
    l2->large_obj_page_ = true;

    shard.UpdateLruList(l1, false);
    shard.UpdateLruList(l2, false);

    // now insert small page s
    CcPage<CompositeKey<std::string, int>, CompositeRecord<int>, true, true>
        *s = new CcPage<CompositeKey<std::string, int>,
                        CompositeRecord<int>,
                        true,
                        true>(ccmap_uptr.get(), neg_inf, pos_inf);
    s->large_obj_page_ = false;
    shard.UpdateLruList(s, false);

    // expected order: s -> l1 -> l2 (s before protected_head which is l1)
    REQUIRE(s->lru_next_ == l1);
    REQUIRE(l1->lru_next_ == l2);

    delete neg_inf;
    delete pos_inf;
    delete s;
    delete l1;
    delete l2;
}

TEST_CASE("Test A: EnsureLargeObjOccupyPageAlone basic migration",
          "[large-obj-lru][ensure-migrate]")
{
    auto [local, shard_uptr] = make_shard_config(64, 1);  // 1KB threshold
    CcShard &shard = *shard_uptr;

    using KeyT = EloqStringKey;
    using ValT = EloqStringRecord;

    std::unique_ptr<TemplateCcMap<KeyT, ValT, true, true>> ccmap_uptr;
    TableName tname(
        std::string("tbl_mig"), TableType::Primary, TableEngine::EloqSql);
    ccmap_uptr = std::make_unique<TemplateCcMap<KeyT, ValT, true, true>>(
        &shard, 0, tname, 1, nullptr, false);
    auto *cc_map = ccmap_uptr.get();

    // Create first key via FindEmplace to ensure page is present in ccmp_
    EloqStringKey k1("ka", 2);
    auto it1 = cc_map->FindEmplace(k1);
    auto *cce1 = it1->second;
    auto *page = it1.GetPage();
    cce1->payload_.cur_payload_ = std::make_unique<ValT>();
    cce1->SetCommitTsPayloadStatus(1, RecordStatus::Normal);
    cce1->SetCkptTs(100);

    // Emplace a second key via cc_map->FindEmplace to ensure map bookkeeping
    EloqStringKey k2("kb", 2);
    auto it_k2 = cc_map->FindEmplace(k2);
    auto *cce2 = it_k2->second;
    auto *page2 = it_k2.GetPage();
    REQUIRE(page2 == page);
    REQUIRE(page->Size() > 1);
    REQUIRE(cce2 != nullptr);
    auto rec = std::make_unique<ValT>();
    std::string big(shard.LargeObjThresholdBytes() + 512, 'X');
    rec->SetEncodedBlob(reinterpret_cast<const unsigned char *>(big.data()),
                        big.size());
    cce2->payload_.cur_payload_ = std::move(rec);
    cce2->SetCommitTsPayloadStatus(1, RecordStatus::Normal);
    cce1->SetCkptTs(100);

    // Call EnsureLargeObjOccupyPageAlone to migrate the large entry
    ccmap_uptr->EnsureLargeObjOccupyPageAlone(page, cce2);
    // original page should have only one entry after migration
    REQUIRE(page->Size() == 1);
    REQUIRE(page->Entry(0)->PayloadStatus() == RecordStatus::Normal);

    // After migration, FindEmplace on k2 should return a page marked large
    auto it2 = cc_map->FindEmplace(k2);
    auto *new_page = it2.GetPage();
    REQUIRE(new_page->Size() == 1);
    REQUIRE(new_page->Entry(0)->PayloadStatus() == RecordStatus::Normal);
    REQUIRE(new_page->large_obj_page_ == true);

    // Ensure k2 is no longer present in original page
    size_t found = page->Find(k2);
    REQUIRE(found == page->Size());

    // New page should contain the key
    const KeyT *nk = new_page->Key(0);
    REQUIRE(nk != nullptr);
    REQUIRE(*nk == k2);
}

// Test C: tail short-circuit sets protected head
TEST_CASE("LRU tail short-circuit sets protected head",
          "[large-obj-lru][tail-short]")
{
    auto [local, shard_uptr] = make_shard();
    CcShard &shard = *shard_uptr;

    std::unique_ptr<TemplateCcMap<CompositeKey<std::string, int>,
                                  CompositeRecord<int>,
                                  true,
                                  true>>
        ccmap_uptr;
    CcPage<CompositeKey<std::string, int>, CompositeRecord<int>, true, true>
        *neg_inf, *pos_inf, *p1, *p2;
    std::tie(neg_inf, pos_inf, p1, p2) =
        make_ccmap_and_pages<CompositeKey<std::string, int>,
                             CompositeRecord<int>>(shard, ccmap_uptr);

    // Mark tail page as large and update LRU. UpdateLruList should
    // short-circuit and set protected_head_page_ to this page.
    p2->large_obj_page_ = true;
    shard.UpdateLruList(p2, false);

    REQUIRE(shard.protected_head_page_ == p2);

    delete neg_inf;
    delete pos_inf;
    delete p1;
    delete p2;
}

TEST_CASE("Test D: LRU partition invariant", "[large-obj-lru][partition-inv]")
{
    auto [local, shard_uptr] = make_shard();
    CcShard &shard = *shard_uptr;

    std::unique_ptr<TemplateCcMap<CompositeKey<std::string, int>,
                                  CompositeRecord<int>,
                                  true,
                                  true>>
        ccmap_uptr;
    CcPage<CompositeKey<std::string, int>, CompositeRecord<int>, true, true>
        *neg_inf, *pos_inf, *p1, *p2;
    std::tie(neg_inf, pos_inf, p1, p2) =
        make_ccmap_and_pages<CompositeKey<std::string, int>,
                             CompositeRecord<int>>(shard, ccmap_uptr);

    // Create additional pages: two smalls and two larges
    auto *s1 = new CcPage<CompositeKey<std::string, int>,
                          CompositeRecord<int>,
                          true,
                          true>(ccmap_uptr.get(), neg_inf, pos_inf);
    auto *s2 = new CcPage<CompositeKey<std::string, int>,
                          CompositeRecord<int>,
                          true,
                          true>(ccmap_uptr.get(), neg_inf, pos_inf);
    auto *l1 = new CcPage<CompositeKey<std::string, int>,
                          CompositeRecord<int>,
                          true,
                          true>(ccmap_uptr.get(), neg_inf, pos_inf);
    auto *l2 = new CcPage<CompositeKey<std::string, int>,
                          CompositeRecord<int>,
                          true,
                          true>(ccmap_uptr.get(), neg_inf, pos_inf);
    auto VerifyLruList = [&shard]
    {
        // protected_head_page_ must be either tail or a large page
        REQUIRE(((shard.protected_head_page_ == &shard.tail_ccp_) ||
                 (shard.protected_head_page_->large_obj_page_)));

        // Verify partition invariant: pages before protected head are small,
        // pages from protected head to tail are large.
        bool reached_prot = false;
        for (LruPage *cur = shard.head_ccp_.lru_next_; cur != &shard.tail_ccp_;
             cur = cur->lru_next_)
        {
            if (cur == shard.protected_head_page_)
            {
                reached_prot = true;
            }
            if (!reached_prot)
            {
                REQUIRE(cur->large_obj_page_ == false);
            }
            else
            {
                REQUIRE(cur->large_obj_page_ == true);
            }
        }
    };

    s1->large_obj_page_ = false;
    s2->large_obj_page_ = false;
    l1->large_obj_page_ = true;
    l2->large_obj_page_ = true;

    // Build LRU order: s1 -> s2 -> l1 -> l2
    shard.UpdateLruList(s1, false);
    shard.UpdateLruList(s2, false);
    shard.UpdateLruList(l1, false);
    shard.UpdateLruList(l2, false);
    VerifyLruList();

    // Shuffle LRU order
    shard.UpdateLruList(l2, false);
    shard.UpdateLruList(l1, false);
    shard.UpdateLruList(s2, false);
    shard.UpdateLruList(s1, false);

    VerifyLruList();

    delete neg_inf;
    delete pos_inf;
    delete s1;
    delete s2;
    delete l1;
    delete l2;
    delete p1;
    delete p2;
}

}  // namespace txservice

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    int ret = Catch::Session().run(argc, argv);
    return ret;
}
