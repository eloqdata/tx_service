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

// ---------------------------------------------------------------------------
// Helper: verify LO_LRU partition invariant for a shard.
//
// Checks:
//   1. protected_head_page_ is either &tail_ccp_ or a large page.
//   2. Every page in [head, protected_head_page_) has large_obj_page_==false.
//   3. Every page in [protected_head_page_, tail_ccp_) has large_obj_page_==true.
//   4. The list is not empty and forward/backward links are consistent
//      (no dangling next/prev pointers).
// ---------------------------------------------------------------------------
static void VerifyLruListInvariant(CcShard &shard)
{
    // 1. protected_head_page_ is tail or a large page.
    REQUIRE(((shard.protected_head_page_ == &shard.tail_ccp_) ||
             (shard.protected_head_page_->large_obj_page_ == true)));

    // 2 & 3. Walk forward and verify partition.
    bool reached_prot = false;
    size_t count = 0;
    LruPage *prev = &shard.head_ccp_;
    for (LruPage *cur = shard.head_ccp_.lru_next_; cur != &shard.tail_ccp_;
         cur = cur->lru_next_)
    {
        // Backward link must be consistent.
        REQUIRE(cur->lru_prev_ == prev);

        if (cur == shard.protected_head_page_)
        {
            reached_prot = true;
        }
        if (!reached_prot)
        {
            INFO("Page at position " << count << " expected small");
            REQUIRE(cur->large_obj_page_ == false);
        }
        else
        {
            INFO("Page at position " << count << " expected large");
            REQUIRE(cur->large_obj_page_ == true);
        }
        prev = cur;
        ++count;
    }
    // Tail's backward link must also be consistent.
    REQUIRE(shard.tail_ccp_.lru_prev_ == prev);
}

// ---------------------------------------------------------------------------
// State-flip test infrastructure.
//
// All flip tests reuse the same KeyT/ValT and call make_ccmap_and_pages()
// to obtain a ccmap whose parent_map_ is of type Primary (required for
// UpdateLruList to actually update the LRU list rather than early-return).
// ---------------------------------------------------------------------------
using FlipKeyT = CompositeKey<std::string, int>;
using FlipValT = CompositeRecord<int>;

// Convenience: allocate a fresh small page and return it.
static CcPage<FlipKeyT, FlipValT, true, true> *NewPage(
    TemplateCcMap<FlipKeyT, FlipValT, true, true> *cc_map,
    CcPage<FlipKeyT, FlipValT, true, true> *neg_inf,
    CcPage<FlipKeyT, FlipValT, true, true> *pos_inf)
{
    auto *p = new CcPage<FlipKeyT, FlipValT, true, true>(cc_map, neg_inf,
                                                         pos_inf);
    p->large_obj_page_ = false;
    return p;
}

// ---------------------------------------------------------------------------
// Scenario A1:
//   - LRU list has two small pages and one large page (the protected_head).
//   - The large page is flipped to small (large→small).
//   - UpdateLruList is called on it.
//   - Expected: protected_head_page_ resets to &tail_ccp_ (empty protected
//     segment), and partition invariant holds.
// ---------------------------------------------------------------------------
TEST_CASE("Flip A1: large→small for sole protected_head resets to tail",
          "[large-obj-lru][flip-A1]")
{
    auto [local, shard_uptr] = make_shard();
    CcShard &shard = *shard_uptr;

    std::unique_ptr<TemplateCcMap<FlipKeyT, FlipValT, true, true>> ccmap_uptr;
    CcPage<FlipKeyT, FlipValT, true, true> *neg_inf, *pos_inf, *dummy1,
        *dummy2;
    std::tie(neg_inf, pos_inf, dummy1, dummy2) =
        make_ccmap_and_pages<FlipKeyT, FlipValT>(shard, ccmap_uptr);

    // Allocate two small and one large page.
    auto *s1 = NewPage(ccmap_uptr.get(), neg_inf, pos_inf);
    auto *s2 = NewPage(ccmap_uptr.get(), neg_inf, pos_inf);
    auto *ph = NewPage(ccmap_uptr.get(), neg_inf, pos_inf);
    ph->large_obj_page_ = true;

    // Build LRU: s1 → s2 → ph(large, protected_head) → tail
    shard.UpdateLruList(s1, false);
    shard.UpdateLruList(s2, false);
    shard.UpdateLruList(ph, false);

    REQUIRE(shard.protected_head_page_ == ph);
    VerifyLruListInvariant(shard);

    // -- Flip ph from large to small --
    ph->large_obj_page_ = false;
    shard.UpdateLruList(ph, false);

    // Protected segment must now be empty.
    REQUIRE(shard.protected_head_page_ == &shard.tail_ccp_);
    // Partition invariant: all pages are small.
    VerifyLruListInvariant(shard);

    delete neg_inf;
    delete pos_inf;
    delete dummy1;
    delete dummy2;
    delete s1;
    delete s2;
    delete ph;
}

// ---------------------------------------------------------------------------
// Scenario A2:
//   - LRU list has one small, one large protected_head, and one more large.
//   - The protected_head is flipped to small (large→small).
//   - UpdateLruList is called on it.
//   - Expected: protected_head_page_ advances to l2, partition invariant holds.
// ---------------------------------------------------------------------------
TEST_CASE("Flip A2: large→small for protected_head advances to next large page",
          "[large-obj-lru][flip-A2]")
{
    auto [local, shard_uptr] = make_shard();
    CcShard &shard = *shard_uptr;

    std::unique_ptr<TemplateCcMap<FlipKeyT, FlipValT, true, true>> ccmap_uptr;
    CcPage<FlipKeyT, FlipValT, true, true> *neg_inf, *pos_inf, *dummy1,
        *dummy2;
    std::tie(neg_inf, pos_inf, dummy1, dummy2) =
        make_ccmap_and_pages<FlipKeyT, FlipValT>(shard, ccmap_uptr);

    auto *s1 = NewPage(ccmap_uptr.get(), neg_inf, pos_inf);
    auto *ph = NewPage(ccmap_uptr.get(), neg_inf, pos_inf);
    auto *l2 = NewPage(ccmap_uptr.get(), neg_inf, pos_inf);
    ph->large_obj_page_ = true;
    l2->large_obj_page_ = true;

    // Build LRU: s1(small) → ph(large, protected_head) → l2(large) → tail
    shard.UpdateLruList(s1, false);
    shard.UpdateLruList(ph, false);
    shard.UpdateLruList(l2, false);

    REQUIRE(shard.protected_head_page_ == ph);
    VerifyLruListInvariant(shard);

    // -- Flip ph from large to small --
    ph->large_obj_page_ = false;
    shard.UpdateLruList(ph, false);

    // protected_head_page_ must have advanced to l2.
    REQUIRE(shard.protected_head_page_ == l2);
    VerifyLruListInvariant(shard);

    // ph (now small) should be positioned before l2 (the new protected_head).
    REQUIRE(ph->lru_next_ == l2);

    delete neg_inf;
    delete pos_inf;
    delete dummy1;
    delete dummy2;
    delete s1;
    delete ph;
    delete l2;
}

// ---------------------------------------------------------------------------
// Scenario B1:
//   - LRU list has only small pages; protected segment is empty
//     (protected_head_page_ == &tail_ccp_).
//   - A small page that is already at the "just-before-tail" position is
//     flipped to large (small→large).
//   - UpdateLruList is called on it.
//   - Expected: protected_head_page_ is set to that page (no longer tail),
//     and partition invariant holds.
//
// This scenario exposes the early-return bug: without the fix, the early
// return in UpdateLruList (page already at correct insertion point) would
// skip the protected_head_page_ update.
// ---------------------------------------------------------------------------
TEST_CASE(
    "Flip B1: small→large for page already at tail sets protected_head_page_",
    "[large-obj-lru][flip-B1]")
{
    auto [local, shard_uptr] = make_shard();
    CcShard &shard = *shard_uptr;

    std::unique_ptr<TemplateCcMap<FlipKeyT, FlipValT, true, true>> ccmap_uptr;
    CcPage<FlipKeyT, FlipValT, true, true> *neg_inf, *pos_inf, *dummy1,
        *dummy2;
    std::tie(neg_inf, pos_inf, dummy1, dummy2) =
        make_ccmap_and_pages<FlipKeyT, FlipValT>(shard, ccmap_uptr);

    auto *s1 = NewPage(ccmap_uptr.get(), neg_inf, pos_inf);
    auto *s2 = NewPage(ccmap_uptr.get(), neg_inf, pos_inf);

    // Build LRU: s1 → s2 → tail, protected_head == tail (empty large segment)
    shard.UpdateLruList(s1, false);
    shard.UpdateLruList(s2, false);

    REQUIRE(shard.protected_head_page_ == &shard.tail_ccp_);
    // s2 is the most-recently-used small page, positioned just before tail.
    REQUIRE(s2->lru_next_ == &shard.tail_ccp_);

    // -- Flip s2 from small to large (s2 already sits just before tail) --
    s2->large_obj_page_ = true;
    shard.UpdateLruList(s2, false);

    // protected_head_page_ must now point to s2 (not tail).
    REQUIRE(shard.protected_head_page_ == s2);
    VerifyLruListInvariant(shard);

    delete neg_inf;
    delete pos_inf;
    delete dummy1;
    delete dummy2;
    delete s1;
    delete s2;
}

// ---------------------------------------------------------------------------
// Scenario B2:
//   - LRU list has one small page and one large page (non-empty protected
//     segment).
//   - The small page is flipped to large (small→large).
//   - UpdateLruList is called on it.
//   - Expected: protected_head_page_ remains at the original large page (l1),
//     the flipped page is moved after l1 into the large segment, partition
//     invariant holds.
// ---------------------------------------------------------------------------
TEST_CASE("Flip B2: small→large with non-empty protected segment moves page",
          "[large-obj-lru][flip-B2]")
{
    auto [local, shard_uptr] = make_shard();
    CcShard &shard = *shard_uptr;

    std::unique_ptr<TemplateCcMap<FlipKeyT, FlipValT, true, true>> ccmap_uptr;
    CcPage<FlipKeyT, FlipValT, true, true> *neg_inf, *pos_inf, *dummy1,
        *dummy2;
    std::tie(neg_inf, pos_inf, dummy1, dummy2) =
        make_ccmap_and_pages<FlipKeyT, FlipValT>(shard, ccmap_uptr);

    auto *s1 = NewPage(ccmap_uptr.get(), neg_inf, pos_inf);
    auto *l1 = NewPage(ccmap_uptr.get(), neg_inf, pos_inf);
    l1->large_obj_page_ = true;

    // Build LRU: s1(small) → l1(large, protected_head) → tail
    shard.UpdateLruList(s1, false);
    shard.UpdateLruList(l1, false);

    REQUIRE(shard.protected_head_page_ == l1);
    VerifyLruListInvariant(shard);

    // -- Flip s1 from small to large --
    s1->large_obj_page_ = true;
    shard.UpdateLruList(s1, false);

    // protected_head_page_ must still be l1 (the original first large page).
    REQUIRE(shard.protected_head_page_ == l1);
    // s1 must now reside in the large segment (after l1).
    REQUIRE(l1->lru_next_ == s1);
    REQUIRE(s1->lru_next_ == &shard.tail_ccp_);
    VerifyLruListInvariant(shard);

    delete neg_inf;
    delete pos_inf;
    delete dummy1;
    delete dummy2;
    delete s1;
    delete l1;
}

// ---------------------------------------------------------------------------
// Scenario A via EnsureLargeObjOccupyPageAlone:
//   - A single-entry page is marked large (large_obj_page_=true) and is the
//     protected_head in the LRU list.
//   - EnsureLargeObjOccupyPageAlone is called when the entry is Deleted.
//   - Expected: page.large_obj_page_ becomes false, protected_head_page_
//     resets to tail, and partition invariant holds.
// ---------------------------------------------------------------------------
TEST_CASE("Flip A via ELOOPA: deleted entry resets large page to small",
          "[large-obj-lru][flip-A-eloopa]")
{
    auto [local, shard_uptr] = make_shard_config(64, 1);  // 1 KB threshold
    CcShard &shard = *shard_uptr;

    using KeyT = EloqStringKey;
    using ValT = EloqStringRecord;

    TableName tname(
        std::string("tbl_flip_a"), TableType::Primary, TableEngine::EloqSql);
    auto ccmap_uptr =
        std::make_unique<TemplateCcMap<KeyT, ValT, true, true>>(
            &shard, 0, tname, 1, nullptr, false);

    // Insert one entry that is large (exceeds threshold).
    EloqStringKey k1("key_large", 9);
    auto it1 = ccmap_uptr->FindEmplace(k1);
    auto *cce = it1->second;
    auto *ccp = static_cast<CcPage<KeyT, ValT, true, true> *>(it1.GetPage());

    auto rec = std::make_unique<ValT>();
    std::string big(shard.LargeObjThresholdBytes() + 256, 'L');
    rec->SetEncodedBlob(reinterpret_cast<const unsigned char *>(big.data()),
                        big.size());
    cce->payload_.cur_payload_ = std::move(rec);
    cce->SetCommitTsPayloadStatus(1, RecordStatus::Normal);

    // Make the page large and register it in the LRU protected segment.
    ccp->large_obj_page_ = true;
    shard.UpdateLruList(ccp, false);
    REQUIRE(shard.protected_head_page_ == ccp);
    VerifyLruListInvariant(shard);

    // -- Flip: mark the entry as deleted --
    cce->SetCommitTsPayloadStatus(2, RecordStatus::Deleted);
    REQUIRE(cce->PayloadStatus() == RecordStatus::Deleted);

    // EnsureLargeObjOccupyPageAlone should set large_obj_page_=false and
    // update the LRU list.
    ccmap_uptr->EnsureLargeObjOccupyPageAlone(ccp, cce);

    REQUIRE(ccp->large_obj_page_ == false);
    // Protected segment must now be empty.
    REQUIRE(shard.protected_head_page_ == &shard.tail_ccp_);
    VerifyLruListInvariant(shard);
}

// ---------------------------------------------------------------------------
// Scenario B via EnsureLargeObjOccupyPageAlone:
//   - A single-entry page is small (large_obj_page_=false) and is already in
//     the LRU list just before tail (protected segment is empty).
//   - EnsureLargeObjOccupyPageAlone is called with an entry whose serialized
//     size exceeds the threshold.
//   - Expected: page.large_obj_page_ becomes true, protected_head_page_ is
//     updated to that page (not left at tail), and partition invariant holds.
//
// Without the UpdateLruList early-return fix this test would fail because
// the early return would skip the protected_head_page_ update.
// ---------------------------------------------------------------------------
TEST_CASE("Flip B via ELOOPA: single large entry sets page large and updates "
          "protected_head",
          "[large-obj-lru][flip-B-eloopa]")
{
    auto [local, shard_uptr] = make_shard_config(64, 1);  // 1 KB threshold
    CcShard &shard = *shard_uptr;

    using KeyT = EloqStringKey;
    using ValT = EloqStringRecord;

    TableName tname(
        std::string("tbl_flip_b"), TableType::Primary, TableEngine::EloqSql);
    auto ccmap_uptr =
        std::make_unique<TemplateCcMap<KeyT, ValT, true, true>>(
            &shard, 0, tname, 1, nullptr, false);

    // Insert one entry that is initially small.
    EloqStringKey k1("key_small", 9);
    auto it1 = ccmap_uptr->FindEmplace(k1);
    auto *cce = it1->second;
    auto *ccp = static_cast<CcPage<KeyT, ValT, true, true> *>(it1.GetPage());

    auto small_rec = std::make_unique<ValT>();
    std::string small_blob("tiny", 4);
    small_rec->SetEncodedBlob(
        reinterpret_cast<const unsigned char *>(small_blob.data()),
        small_blob.size());
    cce->payload_.cur_payload_ = std::move(small_rec);
    cce->SetCommitTsPayloadStatus(1, RecordStatus::Normal);

    // Page starts as small; put it in the LRU list.
    ccp->large_obj_page_ = false;
    shard.UpdateLruList(ccp, false);
    REQUIRE(shard.protected_head_page_ == &shard.tail_ccp_);
    // Page must be just before tail (only page in list).
    REQUIRE(ccp->lru_next_ == &shard.tail_ccp_);

    // -- Flip: entry grows beyond the threshold --
    auto big_rec = std::make_unique<ValT>();
    std::string big(shard.LargeObjThresholdBytes() + 512, 'B');
    big_rec->SetEncodedBlob(reinterpret_cast<const unsigned char *>(big.data()),
                            big.size());
    cce->payload_.cur_payload_ = std::move(big_rec);
    cce->SetCommitTsPayloadStatus(2, RecordStatus::Normal);

    // ccp still has size == 1, so EnsureLargeObjOccupyPageAlone should mark it
    // large in-place (no migration).
    REQUIRE(ccp->Size() == 1);
    ccmap_uptr->EnsureLargeObjOccupyPageAlone(ccp, cce);

    REQUIRE(ccp->large_obj_page_ == true);
    // protected_head_page_ must now point to ccp (not remain at tail).
    REQUIRE(shard.protected_head_page_ == ccp);
    VerifyLruListInvariant(shard);
}

}  // namespace txservice

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    int ret = Catch::Session().run(argc, argv);
    return ret;
}
