/**
 * Tests for Large Object LRU (LO-LRU) cache eviction policy.
 *
 * LO-LRU divides the LRU list into two logical sections:
 *
 *   [head_ccp] <-> [small pages] <-> [protected_head_page_]
 *                                          <-> [large pages] <-> [tail_ccp]
 *
 * Large objects (payload > lolru_large_obj_threshold_kb) occupy one CcPage
 * exclusively (large_obj_page_ == true) and reside in the tail section.
 * Small objects reside in the head section and are evicted first.
 */
#include <catch2/catch_all.hpp>
#include <chrono>
#include <random>

#define protected public
#define private public

#include "cc_entry.h"
#include "cc_shard.h"
#include "include/mock/mock_catalog_factory.h"
#include "local_cc_shards.h"
#include "template_cc_map.h"
#include "tx_key.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{

static MockCatalogFactory mock_catalog_factory{};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// Convenience type aliases used throughout tests.
using TestKey = CompositeKey<std::string, int>;
using TestRecord = CompositeRecord<int>;
using TestCcMap = TemplateCcMap<TestKey,
                                TestRecord,
                                /*Versioned=*/true,
                                /*RangePartitioned=*/true>;
using TestCcPage = CcPage<TestKey, TestRecord, true, true>;
using TestCcEntry = CcEntry<TestKey, TestRecord, true, true>;

/**
 * Walk the entire LRU doubly-linked chain from head to tail and verify that
 * every forward and backward pointer is consistent.  Returns the number of
 * non-dummy pages in the chain (excludes head_ccp_ and tail_ccp_).
 *
 * Uses Catch2 REQUIRE so failures are reported as test failures rather than
 * hard aborts.  Also calls shard.VerifyLruList() (assert-based) for an
 * independent sanity check when running in debug mode.
 */
static size_t CheckLruListIntegrity(CcShard &shard)
{
    size_t page_count = 0;
    LruPage *pre = &shard.head_ccp_;
    for (LruPage *cur = shard.head_ccp_.lru_next_; cur != nullptr;
         cur = cur->lru_next_)
    {
        REQUIRE(pre->lru_next_ == cur);
        REQUIRE(cur->lru_prev_ == pre);
        pre = cur;
        ++page_count;
    }
    // The loop exits when cur reaches tail_ccp_ and steps to its nullptr next.
    // At that point pre must equal &tail_ccp_.
    REQUIRE(pre == &shard.tail_ccp_);

    // Also run the assert-based verification (effective in debug builds).
    shard.VerifyLruList();

    return page_count - 1;  // Subtract the tail_ccp_ dummy page itself.
}

/**
 * Shared fixture construction used across LO-LRU tests.
 * Mirrors the setup in CcPage-Test.cpp.
 */
struct LoLruFixture
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

    CatalogFactory *catalog_factory[5] = {
        &mock_catalog_factory,
        &mock_catalog_factory,
        &mock_catalog_factory,
        &mock_catalog_factory,
        &mock_catalog_factory,
    };

    LocalCcShards local_cc_shards;
    CcShard shard;
    std::string raft_path;

    explicit LoLruFixture(uint32_t lolru_threshold_kb = 1)
        : local_cc_shards(0,
                          0,
                          tx_cnf,
                          catalog_factory,
                          nullptr,
                          &ng_configs,
                          2,
                          nullptr,
                          nullptr,
                          true),
          shard(0,
                1,
                10000,
                false,
                0,
                local_cc_shards,
                catalog_factory,
                nullptr,
                &ng_configs,
                2)
    {
        local_cc_shards.SetupPolicyLoLRU(lolru_threshold_kb);
        shard.Init();
        Sharder::Instance(0,
                          &ng_configs,
                          0,
                          nullptr,
                          nullptr,
                          &local_cc_shards,
                          nullptr,
                          &raft_path);
    }

    /**
     * Insert `cnt` keys into `cc_map` using FindEmplace, then commit each
     * entry as a non-dirty, already-flushed Normal record.  All entries are
     * left in a state where IsFree() can return true (no locks, ckpt_ts >
     * commit_ts).
     */
    void InsertKeys(TestCcMap &cc_map, const std::string &table_name, int cnt)
    {
        for (int i = 0; i < cnt; i++)
        {
            TestKey key = std::make_tuple(table_name, i);
            bool emplace = false;
            auto it = cc_map.FindEmplace(key, &emplace, false, false);
            REQUIRE(emplace == true);

            TestCcEntry *cce = it->second;
            TestCcPage *ccp = it.GetPage();

            bool was_dirty = cce->IsDirty();
            // commit_ts=100, ckpt_ts=200 -> non-dirty, IsFree()-eligible.
            cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
            cce->SetCkptTs(200);
            cc_map.OnFlushed(cce, was_dirty);
            cc_map.OnCommittedUpdate(cce, was_dirty);
            ccp->last_dirty_commit_ts_ =
                std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
        }
    }

    /**
     * Collect at most `max_count` LruPage pointers from the small-object
     * section [head_ccp_.lru_next_, protected_head_page_).
     */
    std::vector<LruPage *> CollectSmallPages(size_t max_count = SIZE_MAX)
    {
        std::vector<LruPage *> pages;
        for (LruPage *cur = shard.head_ccp_.lru_next_;
             cur != shard.protected_head_page_ && pages.size() < max_count;
             cur = cur->lru_next_)
        {
            pages.push_back(cur);
        }
        return pages;
    }

    /**
     * Collect at most `max_count` LruPage pointers from the large-object
     * section [protected_head_page_, tail_ccp_).
     */
    std::vector<LruPage *> CollectLargePages(size_t max_count = SIZE_MAX)
    {
        std::vector<LruPage *> pages;
        for (LruPage *cur = shard.protected_head_page_;
             cur != &shard.tail_ccp_ && pages.size() < max_count;
             cur = cur->lru_next_)
        {
            pages.push_back(cur);
        }
        return pages;
    }
};

/**
 * Verify the LO-LRU partition invariant:
 *
 *   - Every page in [head_ccp_.lru_next_, protected_head_page_) has
 *     large_obj_page_ == false  (small-object section).
 *   - Every page in [protected_head_page_, tail_ccp_) has
 *     large_obj_page_ == true   (large-object section).
 *
 * The boundary node protected_head_page_ itself is included in the large
 * section.  When it equals &tail_ccp_ the large section is empty and the
 * second loop body never executes, which is correct.
 *
 * Uses Catch2 REQUIRE so failures are reported as test failures.
 */
static void CheckPartitionInvariant(CcShard &shard)
{
    // Small section: [head_ccp_.lru_next_, protected_head_page_)
    for (LruPage *cur = shard.head_ccp_.lru_next_;
         cur != shard.protected_head_page_;
         cur = cur->lru_next_)
    {
        REQUIRE(cur->large_obj_page_ == false);
    }

    // Large section: [protected_head_page_, tail_ccp_)
    for (LruPage *cur = shard.protected_head_page_; cur != &shard.tail_ccp_;
         cur = cur->lru_next_)
    {
        REQUIRE(cur->large_obj_page_ == true);
    }
}

// ---------------------------------------------------------------------------
// TC-INV-01: LRU chain doubly-linked pointer integrity
// ---------------------------------------------------------------------------
//
// Verifies that after each mutating LO-LRU operation the doubly-linked
// lru_prev_ / lru_next_ chain is fully consistent.  Operations covered:
//
//   Phase 1 – Insert small objects       -> UpdateLruList (small path)
//   Phase 2 – Promote pages to large     -> UpdateLruList (large path, first
//                                           promotion also sets
//                                           protected_head_page_)
//   Phase 3 – Re-access a small page     -> UpdateLruList re-order (small)
//   Phase 4 – Re-access a large page     -> UpdateLruList re-order (large)
//   Phase 5 – Demote a large page back   -> UpdateLruList (small path after
//                                           clearing large_obj_page_)
//   Phase 6 – Clean (eviction)           -> CleanPageAndReBalance / DetachLru
// ---------------------------------------------------------------------------
TEST_CASE("TC-INV-01: LRU chain doubly-linked pointer integrity under LO-LRU",
          "[lolru][invariant]")
{
    const std::string TABLE = "inv01";
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    TestCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    // -----------------------------------------------------------------------
    // Phase 1: Insert 400 small objects.
    // Expected: chain is intact; protected_head_page_ still equals tail_ccp_
    //           because no large pages have been created yet.
    // -----------------------------------------------------------------------
    // 400 keys @ split_threshold=64 produces ~8 pages, enough for Phase 2.
    LOG(INFO) << "[TC-INV-01] Phase 1: inserting 400 small objects";
    f.InsertKeys(cc_map, TABLE, 400);

    size_t page_count_after_insert = CheckLruListIntegrity(f.shard);
    REQUIRE(page_count_after_insert > 0);
    // In LO-LRU mode with no large objects, protected_head_ == tail.
    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);

    // -----------------------------------------------------------------------
    // Phase 2: Promote 5 pages to large-object pages.
    //
    // Pages are collected from the oldest end of the small section (HEAD side),
    // so they are NOT at the MRU position.  UpdateLruList takes the non-early-
    // return path: DetachLru removes the page, then it is re-inserted before
    // tail_ccp_, and protected_head_page_ is updated after insertion.
    //
    // The production path (EnsureLargeObjOccupyPageAlone) promotes a page that
    // IS at the MRU position (tail->lru_prev_), which triggers the early-return
    // in UpdateLruList.  That path is covered by the fix inside the
    // early-return block of UpdateLruList which also updates
    // protected_head_page_.
    // -----------------------------------------------------------------------
    std::vector<LruPage *> pages_to_promote = f.CollectSmallPages(5);
    // Need at least 2 pages to exercise both the "first large page sets
    // protected_head_" branch and subsequent large-page re-insertions.
    REQUIRE(pages_to_promote.size() >= 2);
    LOG(INFO) << "[TC-INV-01] Phase 2: promoting " << pages_to_promote.size()
              << " pages to large-object pages";

    for (LruPage *page : pages_to_promote)
    {
        page->large_obj_page_ = true;
        f.shard.UpdateLruList(page, /*is_emplace=*/false);
        CheckLruListIntegrity(f.shard);
    }

    // After promotions the protected head must point into the large section.
    REQUIRE(f.shard.protected_head_page_ != &f.shard.tail_ccp_);
    REQUIRE(f.shard.protected_head_page_->large_obj_page_ == true);

    // -----------------------------------------------------------------------
    // Phase 3: Re-access a small-object page (UpdateLruList small path,
    // re-ordering within [head, protected_head)).
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-INV-01] Phase 3: re-accessing a small page";
    {
        std::vector<LruPage *> small_pages = f.CollectSmallPages(1);
        if (!small_pages.empty())
        {
            f.shard.UpdateLruList(small_pages[0], /*is_emplace=*/false);
            CheckLruListIntegrity(f.shard);
        }
    }

    // -----------------------------------------------------------------------
    // Phase 4: Re-access a large-object page (UpdateLruList large path,
    // re-ordering within [protected_head, tail)).
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-INV-01] Phase 4: re-accessing a large page";
    {
        std::vector<LruPage *> large_pages = f.CollectLargePages(1);
        REQUIRE(!large_pages.empty());
        f.shard.UpdateLruList(large_pages[0], /*is_emplace=*/false);
        CheckLruListIntegrity(f.shard);
    }

    // -----------------------------------------------------------------------
    // Phase 5: Demote one large-object page back to a small-object page.
    // Clear large_obj_page_ and call UpdateLruList (small path).
    // This exercises the branch in UpdateLruList that advances
    // protected_head_page_ when the demoted page IS protected_head_page_.
    // -----------------------------------------------------------------------
    LOG(INFO)
        << "[TC-INV-01] Phase 5: demoting protected_head_page_ back to small";
    {
        // protected_head_page_ is the first large page; demote it.
        LruPage *demote_target = f.shard.protected_head_page_;
        REQUIRE(demote_target != &f.shard.tail_ccp_);
        REQUIRE(demote_target->large_obj_page_ == true);

        demote_target->large_obj_page_ = false;
        f.shard.UpdateLruList(demote_target, /*is_emplace=*/false);
        CheckLruListIntegrity(f.shard);

        // After demotion protected_head_ must still satisfy the invariant:
        // either it is tail or it points to a large-object page.
        REQUIRE((f.shard.protected_head_page_ == &f.shard.tail_ccp_ ||
                 f.shard.protected_head_page_->large_obj_page_ == true));
    }

    // -----------------------------------------------------------------------
    // Phase 6: Run Clean until no more pages can be freed.
    // Clean internally calls CleanPageAndReBalance -> DetachLru, which is
    // the third major mutator of the LRU chain.  Verify integrity after
    // every Clean pass.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-INV-01] Phase 6: running Clean until exhausted";
    {
        size_t total_freed = 0;
        while (true)
        {
            auto [freed, yield] = f.shard.Clean();
            CheckLruListIntegrity(f.shard);
            if (freed == 0)
            {
                break;
            }
            total_freed += freed;
        }
        LOG(INFO) << "[TC-INV-01] total freed: " << total_freed;
    }

    // Final integrity check.
    CheckLruListIntegrity(f.shard);
    LOG(INFO) << "[TC-INV-01] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-INV-02: protected_head_page_ partition invariant
// ---------------------------------------------------------------------------
//
// Verifies that after each mutating LO-LRU operation the partition invariant
// holds:
//   - Every page left of protected_head_page_ has large_obj_page_ == false.
//   - Every page right of (and including) protected_head_page_ (up to but
//     not including tail_ccp_) has large_obj_page_ == true.
//
// Covers the same six phases as TC-INV-01 so that both invariants are
// confirmed to hold simultaneously over identical state transitions.
//
//   Phase 1 – Insert small objects       -> small section only
//   Phase 2 – Promote pages to large     -> large section populated
//   Phase 3 – Re-access a small page     -> small section re-ordered
//   Phase 4 – Re-access a large page     -> large section re-ordered
//   Phase 5 – Demote protected_head_page_ back to small
//   Phase 6 – Clean (eviction)           -> pages removed from both sections
// ---------------------------------------------------------------------------
TEST_CASE("TC-INV-02: protected_head_page_ partition invariant under LO-LRU",
          "[lolru][invariant]")
{
    const std::string TABLE = "inv02";
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    TestCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    // -----------------------------------------------------------------------
    // Phase 1: Insert 400 small objects.
    // Expected: all pages are in the small section; protected_head_page_
    //           equals &tail_ccp_ (large section is empty).
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-INV-02] Phase 1: inserting 400 small objects";
    f.InsertKeys(cc_map, TABLE, 400);

    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);
    CheckPartitionInvariant(f.shard);

    // -----------------------------------------------------------------------
    // Phase 2: Promote 5 pages to large-object pages.
    // Each promotion moves the page to the large section.  After all
    // promotions the partition invariant must still hold.
    // -----------------------------------------------------------------------
    std::vector<LruPage *> pages_to_promote = f.CollectSmallPages(5);
    REQUIRE(pages_to_promote.size() >= 2);
    LOG(INFO) << "[TC-INV-02] Phase 2: promoting " << pages_to_promote.size()
              << " pages to large-object pages";

    for (LruPage *page : pages_to_promote)
    {
        page->large_obj_page_ = true;
        f.shard.UpdateLruList(page, /*is_emplace=*/false);
        CheckPartitionInvariant(f.shard);
    }

    REQUIRE(f.shard.protected_head_page_ != &f.shard.tail_ccp_);
    REQUIRE(f.shard.protected_head_page_->large_obj_page_ == true);

    // -----------------------------------------------------------------------
    // Phase 3: Re-access a small-object page.
    // Page is moved to the MRU end of the small section (just before
    // protected_head_page_).  Partition invariant must hold after move.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-INV-02] Phase 3: re-accessing a small page";
    {
        std::vector<LruPage *> small_pages = f.CollectSmallPages(1);
        if (!small_pages.empty())
        {
            f.shard.UpdateLruList(small_pages[0], /*is_emplace=*/false);
            CheckPartitionInvariant(f.shard);
        }
    }

    // -----------------------------------------------------------------------
    // Phase 4: Re-access a large-object page.
    // Page is moved to the MRU end of the large section (just before
    // tail_ccp_).  Partition invariant must hold after move.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-INV-02] Phase 4: re-accessing a large page";
    {
        std::vector<LruPage *> large_pages = f.CollectLargePages(1);
        REQUIRE(!large_pages.empty());
        f.shard.UpdateLruList(large_pages[0], /*is_emplace=*/false);
        CheckPartitionInvariant(f.shard);
    }

    // -----------------------------------------------------------------------
    // Phase 5: Demote protected_head_page_ back to a small-object page.
    // After demotion the demoted page must appear in the small section and
    // protected_head_page_ must have advanced to the next large page (or to
    // tail_ccp_ if none remain).
    // -----------------------------------------------------------------------
    LOG(INFO)
        << "[TC-INV-02] Phase 5: demoting protected_head_page_ back to small";
    {
        LruPage *demote_target = f.shard.protected_head_page_;
        REQUIRE(demote_target != &f.shard.tail_ccp_);
        REQUIRE(demote_target->large_obj_page_ == true);

        demote_target->large_obj_page_ = false;
        f.shard.UpdateLruList(demote_target, /*is_emplace=*/false);
        CheckPartitionInvariant(f.shard);

        // protected_head_ must still satisfy TC-INV-04.
        REQUIRE((f.shard.protected_head_page_ == &f.shard.tail_ccp_ ||
                 f.shard.protected_head_page_->large_obj_page_ == true));
    }

    // -----------------------------------------------------------------------
    // Phase 6: Run Clean until no more pages can be freed.
    // CleanPageAndReBalance -> DetachLru removes pages from both sections.
    // Verify the partition invariant after each Clean pass.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-INV-02] Phase 6: running Clean until exhausted";
    {
        size_t total_freed = 0;
        while (true)
        {
            auto [freed, yield] = f.shard.Clean();
            CheckPartitionInvariant(f.shard);
            if (freed == 0)
            {
                break;
            }
            total_freed += freed;
        }
        LOG(INFO) << "[TC-INV-02] total freed: " << total_freed;
    }

    // Final partition check.
    CheckPartitionInvariant(f.shard);
    LOG(INFO) << "[TC-INV-02] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-INV-03 support: FakeLargeRecord
// ---------------------------------------------------------------------------
//
// CompositeRecord<int>::SerializedLength() always returns 0, so it can never
// trigger the "large object" path in EnsureLargeObjOccupyPageAlone.  We
// therefore define FakeLargeRecord – a minimal TxObject whose
// SerializedLength() returns a caller-supplied value – and use it as the
// ValueT for TC-INV-03's CcMap so that EnsureLargeObjOccupyPageAlone can be
// exercised along its real code paths.
// ---------------------------------------------------------------------------

struct FakeLargeRecord : public TxObject
{
    size_t fake_size_{0};
    FakeLargeRecord() = default;
    explicit FakeLargeRecord(size_t size) : fake_size_(size)
    {
    }

    size_t SerializedLength() const override
    {
        return fake_size_;
    }
    size_t MemUsage() const override
    {
        return fake_size_;
    }

    void Serialize(std::vector<char> &, size_t &) const override
    {
    }
    void Serialize(std::string &) const override
    {
    }
    void Deserialize(const char *, size_t &) override
    {
    }
    TxRecord::Uptr Clone() const override
    {
        return std::make_unique<FakeLargeRecord>(fake_size_);
    }
    void Copy(const TxRecord &rhs) override
    {
        fake_size_ = static_cast<const FakeLargeRecord &>(rhs).fake_size_;
    }
    std::string ToString() const override
    {
        return "FakeLargeRecord";
    }
};

// Type aliases for TC-INV-03.
using FLRecord = FakeLargeRecord;
using FLCcMap = TemplateCcMap<TestKey,
                              FLRecord,
                              /*Versioned=*/true,
                              /*RangePartitioned=*/true>;
using FLCcPage = CcPage<TestKey, FLRecord, true, true>;
using FLCcEntry = CcEntry<TestKey, FLRecord, true, true>;

/**
 * Verify the exclusivity invariant: every page with large_obj_page_ == true
 * contains exactly one entry (Size() == 1).
 *
 * Safe to call when the shard contains only FLCcPage instances (i.e. when
 * exactly one FLCcMap was created on the shard and no TestCcMap pages exist).
 */
static void CheckExclusivityInvariant(CcShard &shard)
{
    for (LruPage *cur = shard.head_ccp_.lru_next_; cur != &shard.tail_ccp_;
         cur = cur->lru_next_)
    {
        if (cur->large_obj_page_)
        {
            // Safe cast: TC-INV-03 uses only FLCcPage pages in the shard.
            FLCcPage *page = static_cast<FLCcPage *>(cur);
            REQUIRE(page->Size() == 1u);
        }
    }
}

// ---------------------------------------------------------------------------
// TC-INV-03: large-object page exclusivity invariant
// ---------------------------------------------------------------------------
//
// Verifies that every page with large_obj_page_ == true holds exactly one
// entry (Size() == 1) after EnsureLargeObjOccupyPageAlone() is called.
//
// Two sub-scenarios are exercised:
//
//   Scenario A – Single-entry page + large payload:
//       EnsureLargeObjOccupyPageAlone marks the existing page as a large-
//       object page in-place.  Size() remains 1.
//
//   Scenario B – Multi-entry page + one large-payload entry:
//       EnsureLargeObjOccupyPageAlone migrates the large entry to a newly
//       created exclusive page.  The new page gets large_obj_page_=true with
//       Size()==1; the original page keeps large_obj_page_=false with
//       Size() == (original size – 1).
//
// After each call the following invariants are checked:
//   - CheckExclusivityInvariant  (all large pages have Size()==1)
//   - CheckLruListIntegrity      (doubly-linked chain is intact)
//   - CheckPartitionInvariant    (small/large section boundary is correct)
//
// A final Clean() phase checks that eviction preserves the invariant.
// ---------------------------------------------------------------------------
TEST_CASE("TC-INV-03: large-object page exclusivity invariant under LO-LRU",
          "[lolru][invariant]")
{
    const std::string TABLE = "inv03";
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    FLCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    // Threshold is 1 KB = 1024 bytes (configured in LoLruFixture).
    // large_payload_size > threshold  → triggers large-object classification.
    // small_payload_size <= threshold → treated as a normal page.
    constexpr size_t large_payload_size = 2048;

    // Helper: commit an FLCcEntry and inject a FakeLargeRecord payload.
    // The payload must be set AFTER OnCommittedUpdate so that any internal
    // payload reset performed by the commit path does not overwrite our value.
    auto PrepareEntry =
        [](FLCcEntry *cce, FLCcPage *ccp, FLCcMap &map, size_t payload_size)
    {
        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        map.OnFlushed(cce, was_dirty);
        map.OnCommittedUpdate(cce, was_dirty);
        ccp->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
        // Inject the fake payload AFTER all other setup so commit helpers
        // cannot overwrite it.  #define private public makes cur_payload_
        // directly accessible.
        cce->payload_.cur_payload_ =
            std::make_shared<FakeLargeRecord>(payload_size);
    };

    // -----------------------------------------------------------------------
    // Scenario A: single-entry page + large payload.
    // Insert exactly 1 key so that the resulting page has Size()==1.  Then
    // call EnsureLargeObjOccupyPageAlone – expected: page marked as large
    // in-place, Size() stays 1.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-INV-03] Scenario A: single-entry page, large payload";
    {
        TestKey key = std::make_tuple(TABLE, 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(key, &emplace, false, false);
        REQUIRE(emplace == true);

        FLCcEntry *cce = it->second;
        FLCcPage *ccp = it.GetPage();
        REQUIRE(ccp->Size() == 1u);

        PrepareEntry(cce, ccp, cc_map, large_payload_size);

        cc_map.EnsureLargeObjOccupyPageAlone(ccp, cce);

        // The page should now be a large-object page with exactly 1 entry.
        REQUIRE(ccp->large_obj_page_ == true);
        REQUIRE(ccp->Size() == 1u);

        CheckExclusivityInvariant(f.shard);
        CheckLruListIntegrity(f.shard);
        CheckPartitionInvariant(f.shard);
        LOG(INFO) << "[TC-INV-03] Scenario A PASSED";
    }

    // -----------------------------------------------------------------------
    // Scenario B: multi-entry page + one large-payload entry → migration.
    // Insert several more keys (all with small payloads) so they share a page
    // with the Scenario-A key, then update one of them to a large payload and
    // call EnsureLargeObjOccupyPageAlone.
    //
    // NOTE: inserting keys 1..N into "inv03" creates a separate page
    // (split_threshold=64).  We use a different table name to get a fresh
    // multi-entry page without disturbing the Scenario-A page.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-INV-03] Scenario B: multi-entry page, one large entry";
    {
        const std::string TABLE_B = "inv03b";
        TableName tn_b(TABLE_B, TableType::Primary, TableEngine::EloqSql);
        FLCcMap cc_map_b(&f.shard, 0, tn_b, 1, nullptr, true);

        // Insert 10 keys with small payloads → 1 page with 10 entries.
        constexpr int N = 10;
        for (int i = 0; i < N; i++)
        {
            TestKey key = std::make_tuple(TABLE_B, i);
            bool emplace = false;
            auto it = cc_map_b.FindEmplace(key, &emplace, false, false);
            REQUIRE(emplace == true);
            PrepareEntry(it->second, it.GetPage(), cc_map_b, 0 /*small*/);
        }

        // Grab the first entry and its page (all 10 share one page).
        TestKey first_key = std::make_tuple(TABLE_B, 0);
        bool dummy = false;
        auto it = cc_map_b.FindEmplace(first_key, &dummy, false, false);
        REQUIRE(dummy == false);  // key already exists

        FLCcEntry *cce = it->second;
        FLCcPage *ccp = it.GetPage();
        size_t original_size = ccp->Size();
        REQUIRE(original_size >= 2u);

        // Upgrade the first entry to a large payload.
        cce->payload_.cur_payload_ =
            std::make_shared<FakeLargeRecord>(large_payload_size);

        // EnsureLargeObjOccupyPageAlone should create a new exclusive page
        // for this entry.
        cc_map_b.EnsureLargeObjOccupyPageAlone(ccp, cce);

        // Original page must NOT be a large-object page; it lost 1 entry.
        REQUIRE(ccp->large_obj_page_ == false);
        REQUIRE(ccp->Size() == original_size - 1);

        // The exclusivity invariant covers the newly created large page too.
        CheckExclusivityInvariant(f.shard);
        CheckLruListIntegrity(f.shard);
        CheckPartitionInvariant(f.shard);
        LOG(INFO) << "[TC-INV-03] Scenario B PASSED";
    }

    // -----------------------------------------------------------------------
    // Phase: run Clean() until no more pages can be freed and verify that
    // the exclusivity invariant is maintained throughout eviction.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-INV-03] Clean phase: verifying invariant under eviction";
    {
        size_t total_freed = 0;
        while (true)
        {
            auto [freed, yield] = f.shard.Clean();
            CheckExclusivityInvariant(f.shard);
            CheckLruListIntegrity(f.shard);
            CheckPartitionInvariant(f.shard);
            if (freed == 0)
            {
                break;
            }
            total_freed += freed;
        }
        LOG(INFO) << "[TC-INV-03] total freed: " << total_freed;
    }

    // Final invariant check.
    CheckExclusivityInvariant(f.shard);
    LOG(INFO) << "[TC-INV-03] PASSED";

    f.local_cc_shards.Terminate();
}

/**
 * Verify the protected_head_page_ self-type invariant:
 *
 *   protected_head_page_ == &tail_ccp_             (empty large section)
 *   OR
 *   protected_head_page_->large_obj_page_ == true  (points to large page)
 *
 * This is a focused point-check that does not require walking the whole
 * chain.  It is a strict subset of CheckPartitionInvariant, kept separate
 * so TC-INV-04 can name and exercise exactly this property in isolation.
 */
static void CheckProtectedHeadInvariant(CcShard &shard)
{
    REQUIRE((shard.protected_head_page_ == &shard.tail_ccp_ ||
             shard.protected_head_page_->large_obj_page_ == true));
}

// ---------------------------------------------------------------------------
// TC-INV-04: protected_head_page_ self-type invariant
// ---------------------------------------------------------------------------
//
// Verifies that protected_head_page_ always satisfies:
//
//   protected_head_page_ == &tail_ccp_   (empty large section)
//   OR
//   protected_head_page_->large_obj_page_ == true
//
// The test drives the pointer through all four states of its lifecycle:
//
//   State 1 – Initialized:
//       protected_head_page_ == &tail_ccp_  (no large pages)
//   State 2 – First large page created:
//       protected_head_page_ points to the sole large-object page.
//   State 3 – Large pages reordered / additional promotions:
//       protected_head_page_ still points to a large-object page (possibly
//       a different one after DetachLru advances it).
//   State 4 – All large pages demoted:
//       protected_head_page_ returns to &tail_ccp_.
//
// Additional micro-checks:
//   - Re-accessing a small page must NOT move protected_head_page_.
//   - Re-accessing protected_head_page_ itself (DetachLru advances the
//     pointer) must leave it invariant-compliant.
// ---------------------------------------------------------------------------
TEST_CASE("TC-INV-04: protected_head_page_ self-type invariant under LO-LRU",
          "[lolru][invariant]")
{
    const std::string TABLE = "inv04";
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    TestCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    // -----------------------------------------------------------------------
    // Phase 1: Insert 400 small objects.
    // State 1: protected_head_page_ == &tail_ccp_ (large section empty).
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-INV-04] Phase 1: inserting 400 small objects";
    f.InsertKeys(cc_map, TABLE, 400);

    // State 1 check.
    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);
    CheckProtectedHeadInvariant(f.shard);
    CheckLruListIntegrity(f.shard);

    // -----------------------------------------------------------------------
    // Phase 2: Promote pages to large-object pages one by one.
    // After the first promotion: State 2 (protected_head_page_ leaves tail).
    // After each subsequent promotion: State 3 (pointer stable or advanced).
    // -----------------------------------------------------------------------
    std::vector<LruPage *> pages_to_promote = f.CollectSmallPages(5);
    REQUIRE(pages_to_promote.size() >= 2);
    LOG(INFO) << "[TC-INV-04] Phase 2: promoting " << pages_to_promote.size()
              << " pages to large-object pages";

    for (LruPage *page : pages_to_promote)
    {
        page->large_obj_page_ = true;
        f.shard.UpdateLruList(page, /*is_emplace=*/false);
        CheckProtectedHeadInvariant(f.shard);
        CheckLruListIntegrity(f.shard);
    }

    // State 2/3: pointer is in the large section.
    REQUIRE(f.shard.protected_head_page_ != &f.shard.tail_ccp_);
    REQUIRE(f.shard.protected_head_page_->large_obj_page_ == true);

    // -----------------------------------------------------------------------
    // Phase 3: Re-access a small page.
    // Re-ordering within the small section must not move protected_head_page_.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-INV-04] Phase 3: re-accessing a small page";
    {
        LruPage *saved = f.shard.protected_head_page_;
        std::vector<LruPage *> small_pages = f.CollectSmallPages(1);
        if (!small_pages.empty())
        {
            f.shard.UpdateLruList(small_pages[0], /*is_emplace=*/false);
            REQUIRE(f.shard.protected_head_page_ == saved);
            CheckProtectedHeadInvariant(f.shard);
            CheckLruListIntegrity(f.shard);
        }
    }

    // -----------------------------------------------------------------------
    // Phase 4: Re-access protected_head_page_ itself.
    // DetachLru advances protected_head_page_ to the next large page (or to
    // tail if it was the only one).  After re-insertion the invariant must
    // still hold.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-INV-04] Phase 4: re-accessing protected_head_page_";
    {
        LruPage *old_head = f.shard.protected_head_page_;
        REQUIRE(old_head != &f.shard.tail_ccp_);

        f.shard.UpdateLruList(old_head, /*is_emplace=*/false);

        // protected_head_page_ has advanced (old head moved to MRU end).
        REQUIRE(f.shard.protected_head_page_ != old_head);
        CheckProtectedHeadInvariant(f.shard);
        CheckLruListIntegrity(f.shard);
    }

    // -----------------------------------------------------------------------
    // Phase 5: Demote ALL remaining large pages back to small, one at a time.
    // Each demotion of protected_head_page_ advances it; when the last large
    // page is demoted (State 4) it must equal &tail_ccp_ again.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-INV-04] Phase 5: demoting all large pages to small";
    {
        std::vector<LruPage *> large_pages = f.CollectLargePages();
        REQUIRE(!large_pages.empty());

        for (LruPage *page : large_pages)
        {
            page->large_obj_page_ = false;
            f.shard.UpdateLruList(page, /*is_emplace=*/false);
            CheckProtectedHeadInvariant(f.shard);
            CheckLruListIntegrity(f.shard);
        }

        // State 4: large section is empty again.
        REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);
    }

    // -----------------------------------------------------------------------
    // Phase 6: Clean() loop.
    // Eviction via DetachLru must maintain the invariant.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-INV-04] Phase 6: running Clean until exhausted";
    {
        size_t total_freed = 0;
        while (true)
        {
            auto [freed, yield] = f.shard.Clean();
            CheckProtectedHeadInvariant(f.shard);
            CheckLruListIntegrity(f.shard);
            if (freed == 0)
            {
                break;
            }
            total_freed += freed;
        }
        LOG(INFO) << "[TC-INV-04] total freed: " << total_freed;
    }

    // Final invariant check.
    CheckProtectedHeadInvariant(f.shard);
    LOG(INFO) << "[TC-INV-04] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-ENSURE-01: EnsureLargeObjOccupyPageAlone – single-entry page, payload
//               exceeds threshold → in-place promotion, no new page created
// ---------------------------------------------------------------------------
//
// When a page holds exactly 1 entry whose SerializedLength() > threshold,
// EnsureLargeObjOccupyPageAlone must:
//   1. Mark the existing page as large_obj_page_ = true in-place.
//   2. NOT create a new CcPage (ccmp_.size() is unchanged).
//   3. Call UpdateLruList so the page moves to the MRU end of the large
//      section (just before tail_ccp_) and protected_head_page_ is updated.
//
// Steps follow the test plan:
//   a. FindEmplace a single key K so it lands on an isolated page (Size()==1).
//   b. Inject payload with SerializedLength() > LargeObjThresholdBytes().
//   c. Record ccmp_.size() before the call.
//   d. Call EnsureLargeObjOccupyPageAlone(ccp, cce).
//   e. Verify all acceptance criteria.
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-ENSURE-01: single-entry page, payload exceeds threshold – in-place "
    "promotion under LO-LRU",
    "[lolru][ensure]")
{
    const std::string TABLE = "ensure01";
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    FLCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    constexpr size_t large_payload_size = 2048;  // 2 KB > 1 KB threshold.

    // -----------------------------------------------------------------------
    // Step a: insert exactly 1 key → isolated page with Size() == 1.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-ENSURE-01] inserting 1 key";
    TestKey key = std::make_tuple(TABLE, 0);
    bool emplace = false;
    auto it = cc_map.FindEmplace(key, &emplace, false, false);
    REQUIRE(emplace == true);

    FLCcEntry *cce = it->second;
    FLCcPage *ccp = it.GetPage();
    REQUIRE(ccp->Size() == 1u);

    // -----------------------------------------------------------------------
    // Step b: commit the entry and inject a large payload.
    // cur_payload_ is set AFTER commit helpers to avoid it being overwritten.
    // -----------------------------------------------------------------------
    bool was_dirty = cce->IsDirty();
    cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
    cce->SetCkptTs(200);
    cc_map.OnFlushed(cce, was_dirty);
    cc_map.OnCommittedUpdate(cce, was_dirty);
    ccp->last_dirty_commit_ts_ =
        std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
    cce->payload_.cur_payload_ =
        std::make_shared<FakeLargeRecord>(large_payload_size);

    // -----------------------------------------------------------------------
    // Step c: record state before the call.
    // -----------------------------------------------------------------------
    size_t pages_before = cc_map.ccmp_.size();
    REQUIRE(pages_before == 1u);  // sanity: only the one page so far.

    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);

    // -----------------------------------------------------------------------
    // Step d: call EnsureLargeObjOccupyPageAlone.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-ENSURE-01] calling EnsureLargeObjOccupyPageAlone";
    cc_map.EnsureLargeObjOccupyPageAlone(ccp, cce);

    // -----------------------------------------------------------------------
    // Step e: verify acceptance criteria.
    // -----------------------------------------------------------------------

    // 1. Page is now a large-object page.
    REQUIRE(ccp->large_obj_page_ == true);

    // 2. Entry count is unchanged – no migration occurred.
    REQUIRE(ccp->Size() == 1u);

    // 3. No new page was created in the B-tree map.
    REQUIRE(cc_map.ccmp_.size() == pages_before);

    // 4. protected_head_page_ was updated to point to this page (first and
    //    only large page → pointer leaves tail_ccp_).
    REQUIRE(f.shard.protected_head_page_ == ccp);

    // 5. The page sits at the MRU end of the large section (just before tail).
    REQUIRE(ccp->lru_next_ == &f.shard.tail_ccp_);

    // 6. Full chain integrity and partition invariant.
    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    LOG(INFO) << "[TC-ENSURE-01] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-ENSURE-02: EnsureLargeObjOccupyPageAlone – multi-entry page, one entry
//               exceeds threshold → migration to a new exclusive page
// ---------------------------------------------------------------------------
//
// When a page holds more than 1 entry and one entry's SerializedLength() >
// threshold, EnsureLargeObjOccupyPageAlone must:
//   1. Create a new CcPage (ccmp_.size() increases by 1).
//   2. Move the large entry to the new page; the new page has
//      large_obj_page_ = true and Size() == 1.
//   3. Leave the original page with large_obj_page_ = false and
//      Size() == original_size - 1.
//   4. Call UpdateLruList so the new page enters the large section and
//      protected_head_page_ / chain integrity are preserved.
//
// Steps follow the test plan:
//   a. FindEmplace N keys with small payloads → single page with Size() == N.
//   b. Upgrade entry[0]'s payload to SerializedLength() > threshold.
//   c. Record ccmp_.size() before the call.
//   d. Call EnsureLargeObjOccupyPageAlone(ccp, cce[0]).
//   e. Verify all acceptance criteria.
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-ENSURE-02: multi-entry page, one entry exceeds threshold – migration "
    "under LO-LRU",
    "[lolru][ensure]")
{
    const std::string TABLE = "ensure02";
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    FLCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    constexpr size_t large_payload_size = 2048;  // 2 KB > 1 KB threshold.
    constexpr int N = 5;

    // -----------------------------------------------------------------------
    // Step a: insert N keys with small payloads.
    // split_threshold = 64, so 5 keys fit on a single page.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-ENSURE-02] inserting " << N << " small-payload keys";

    FLCcEntry *entry0 = nullptr;
    FLCcPage *page0 = nullptr;

    for (int i = 0; i < N; i++)
    {
        TestKey key = std::make_tuple(TABLE, i);
        bool emplace = false;
        auto it = cc_map.FindEmplace(key, &emplace, false, false);
        REQUIRE(emplace == true);

        FLCcEntry *cce = it->second;
        FLCcPage *ccp = it.GetPage();

        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cc_map.OnFlushed(cce, was_dirty);
        cc_map.OnCommittedUpdate(cce, was_dirty);
        ccp->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
        // Keep payload small (size 0).
        cce->payload_.cur_payload_ =
            std::make_shared<FakeLargeRecord>(0 /*small*/);

        if (i == 0)
        {
            entry0 = cce;
            page0 = ccp;
        }
    }

    REQUIRE(entry0 != nullptr);
    REQUIRE(page0 != nullptr);

    // All N keys must reside on the same page (split_threshold = 64).
    size_t original_size = page0->Size();
    REQUIRE(original_size >= 2u);

    // Confirm all N entries share page0.
    for (int i = 1; i < N; i++)
    {
        TestKey key = std::make_tuple(TABLE, i);
        bool dummy = false;
        auto it = cc_map.FindEmplace(key, &dummy, false, false);
        REQUIRE(dummy == false);  // key already exists
        REQUIRE(it.GetPage() == page0);
    }

    // -----------------------------------------------------------------------
    // Step b: upgrade entry[0] to a large payload.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-ENSURE-02] upgrading entry[0] payload to "
              << large_payload_size << " bytes";
    entry0->payload_.cur_payload_ =
        std::make_shared<FakeLargeRecord>(large_payload_size);

    // -----------------------------------------------------------------------
    // Step c: record state before the call.
    // -----------------------------------------------------------------------
    size_t pages_before = cc_map.ccmp_.size();
    REQUIRE(pages_before == 1u);  // sanity: only the one page so far.
    REQUIRE(page0->large_obj_page_ == false);

    // -----------------------------------------------------------------------
    // Step d: call EnsureLargeObjOccupyPageAlone.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-ENSURE-02] calling EnsureLargeObjOccupyPageAlone";
    cc_map.EnsureLargeObjOccupyPageAlone(page0, entry0);

    // -----------------------------------------------------------------------
    // Step e: verify acceptance criteria.
    // -----------------------------------------------------------------------

    // 1. Original page is NOT a large-object page and lost exactly 1 entry.
    REQUIRE(page0->large_obj_page_ == false);
    REQUIRE(page0->Size() == original_size - 1);

    // 2. A new page was created in the B-tree map.
    REQUIRE(cc_map.ccmp_.size() == pages_before + 1);

    // 3. Find the new large-object page in the LRU chain.
    //    It must be the one just before tail_ccp_ (MRU end of large section).
    LruPage *new_lru_page = f.shard.tail_ccp_.lru_prev_;
    REQUIRE(new_lru_page != &f.shard.head_ccp_);
    REQUIRE(new_lru_page->large_obj_page_ == true);

    FLCcPage *new_page = static_cast<FLCcPage *>(new_lru_page);
    REQUIRE(new_page->Size() == 1u);

    // 4. protected_head_page_ was updated (first large page).
    REQUIRE(f.shard.protected_head_page_ != &f.shard.tail_ccp_);
    REQUIRE(f.shard.protected_head_page_->large_obj_page_ == true);

    // 5. Full chain integrity, partition invariant, and exclusivity invariant.
    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);
    CheckExclusivityInvariant(f.shard);

    LOG(INFO) << "[TC-ENSURE-02] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-ENSURE-03: EnsureLargeObjOccupyPageAlone – entry status is Deleted →
//               large_obj_page_ flag cleared, page demoted to small section
// ---------------------------------------------------------------------------
//
// When a page is already a large-object page (large_obj_page_ == true) and
// the entry's RecordStatus is Deleted, EnsureLargeObjOccupyPageAlone must:
//   1. Clear large_obj_page_ (set it to false).
//   2. Move the page from the large section to the small section by calling
//      UpdateLruList with the small path.
//   3. Update protected_head_page_ if the demoted page was the boundary.
//
// Steps follow the test plan:
//   a. Create a large-object page (single entry, large payload, large_obj_page_
//      == true) using the same in-place promotion path as TC-ENSURE-01.
//   b. Set the entry's RecordStatus to Deleted and shrink the payload to 0.
//   c. Call EnsureLargeObjOccupyPageAlone(ccp, cce) again.
//   d. Verify all acceptance criteria.
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-ENSURE-03: entry status Deleted clears large_obj_page_ flag under "
    "LO-LRU",
    "[lolru][ensure]")
{
    const std::string TABLE = "ensure03";
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    FLCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    constexpr size_t large_payload_size = 2048;  // 2 KB > 1 KB threshold.

    // -----------------------------------------------------------------------
    // Step a: create a large-object page.
    // Insert 1 key, commit it with a large payload, call
    // EnsureLargeObjOccupyPageAlone to trigger in-place promotion.
    // After this step: ccp->large_obj_page_ == true, Size() == 1.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-ENSURE-03] creating large-object page (in-place "
                 "promotion)";
    TestKey key = std::make_tuple(TABLE, 0);
    bool emplace = false;
    auto it = cc_map.FindEmplace(key, &emplace, false, false);
    REQUIRE(emplace == true);

    FLCcEntry *cce = it->second;
    FLCcPage *ccp = it.GetPage();
    REQUIRE(ccp->Size() == 1u);

    bool was_dirty = cce->IsDirty();
    cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
    cce->SetCkptTs(200);
    cc_map.OnFlushed(cce, was_dirty);
    cc_map.OnCommittedUpdate(cce, was_dirty);
    ccp->last_dirty_commit_ts_ =
        std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
    cce->payload_.cur_payload_ =
        std::make_shared<FakeLargeRecord>(large_payload_size);

    // Promote in-place.
    cc_map.EnsureLargeObjOccupyPageAlone(ccp, cce);
    REQUIRE(ccp->large_obj_page_ == true);
    REQUIRE(ccp->Size() == 1u);
    REQUIRE(f.shard.protected_head_page_ == ccp);

    // Sanity: chain integrity before the Deleted step.
    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);

    // -----------------------------------------------------------------------
    // Step b: mark the entry as Deleted and clear its payload.
    // After deletion the object logically no longer exists; payload shrinks
    // to 0 so SerializedLength() <= threshold.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-ENSURE-03] marking entry as Deleted";
    cce->SetCommitTsPayloadStatus(150, RecordStatus::Deleted);
    cce->payload_.cur_payload_ = std::make_shared<FakeLargeRecord>(0);

    // -----------------------------------------------------------------------
    // Step c: call EnsureLargeObjOccupyPageAlone again.
    // Expected: it detects the non-large payload (or Deleted status) and
    // demotes the page back to the small section.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-ENSURE-03] calling EnsureLargeObjOccupyPageAlone on "
                 "Deleted entry";
    cc_map.EnsureLargeObjOccupyPageAlone(ccp, cce);

    // -----------------------------------------------------------------------
    // Step d: verify acceptance criteria.
    // -----------------------------------------------------------------------

    // 1. Page is no longer a large-object page.
    REQUIRE(ccp->large_obj_page_ == false);

    // 2. protected_head_page_ advanced (the only large page was demoted).
    //    With no large pages remaining it must equal &tail_ccp_.
    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);

    // 3. The demoted page now resides in the small section (left of
    //    protected_head_page_ == tail_ccp_), verified by partition invariant.
    CheckPartitionInvariant(f.shard);

    // 4. Full chain integrity.
    CheckLruListIntegrity(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    LOG(INFO) << "[TC-ENSURE-03] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-ENSURE-04: EnsureLargeObjOccupyPageAlone – payload shrinks below
//               threshold → large_obj_page_ cleared, no new page created
// ---------------------------------------------------------------------------
//
// When a page is already a large-object page (large_obj_page_ == true) and
// the entry's RecordStatus remains Normal but its SerializedLength() drops
// to ≤ threshold, EnsureLargeObjOccupyPageAlone must:
//   1. Clear large_obj_page_ (set it to false).
//   2. NOT create a new CcPage (ccmp_.size() is unchanged).
//   3. Move the page to the small section via UpdateLruList (small path).
//   4. Update protected_head_page_ to the next large page (or &tail_ccp_ if
//      none remain).
//
// Steps follow the test plan:
//   a. Create a large-object page (single entry, 2 KB payload, promoted
//      in-place) – same setup as TC-ENSURE-01.
//   b. Shrink the payload to 512 B (≤ 1 KB threshold), keeping Normal status.
//   c. Record ccmp_.size() before the second call.
//   d. Call EnsureLargeObjOccupyPageAlone(ccp, cce) again.
//   e. Verify all acceptance criteria.
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-ENSURE-04: payload shrinks below threshold – large_obj_page_ cleared "
    "under LO-LRU",
    "[lolru][ensure]")
{
    const std::string TABLE = "ensure04";
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    FLCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    constexpr size_t large_payload_size = 2048;  // 2 KB > 1 KB threshold.
    constexpr size_t small_payload_size = 512;   // 512 B < 1 KB threshold.

    // -----------------------------------------------------------------------
    // Step a: create a large-object page (in-place promotion).
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-ENSURE-04] creating large-object page";
    TestKey key = std::make_tuple(TABLE, 0);
    bool emplace = false;
    auto it = cc_map.FindEmplace(key, &emplace, false, false);
    REQUIRE(emplace == true);

    FLCcEntry *cce = it->second;
    FLCcPage *ccp = it.GetPage();
    REQUIRE(ccp->Size() == 1u);

    bool was_dirty = cce->IsDirty();
    cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
    cce->SetCkptTs(200);
    cc_map.OnFlushed(cce, was_dirty);
    cc_map.OnCommittedUpdate(cce, was_dirty);
    ccp->last_dirty_commit_ts_ =
        std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
    cce->payload_.cur_payload_ =
        std::make_shared<FakeLargeRecord>(large_payload_size);

    cc_map.EnsureLargeObjOccupyPageAlone(ccp, cce);
    REQUIRE(ccp->large_obj_page_ == true);
    REQUIRE(f.shard.protected_head_page_ == ccp);

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);

    // -----------------------------------------------------------------------
    // Step b: shrink the payload below threshold (RecordStatus stays Normal).
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-ENSURE-04] shrinking payload to " << small_payload_size
              << " bytes (below threshold)";
    cce->payload_.cur_payload_ =
        std::make_shared<FakeLargeRecord>(small_payload_size);

    // -----------------------------------------------------------------------
    // Step c: record ccmp_.size() – must stay 1 after demotion (no new page).
    // -----------------------------------------------------------------------
    size_t pages_before = cc_map.ccmp_.size();
    REQUIRE(pages_before == 1u);

    // -----------------------------------------------------------------------
    // Step d: call EnsureLargeObjOccupyPageAlone again.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-ENSURE-04] calling EnsureLargeObjOccupyPageAlone";
    cc_map.EnsureLargeObjOccupyPageAlone(ccp, cce);

    // -----------------------------------------------------------------------
    // Step e: verify acceptance criteria.
    // -----------------------------------------------------------------------

    // 1. Page is no longer a large-object page.
    REQUIRE(ccp->large_obj_page_ == false);

    // 2. No new page was created.
    REQUIRE(cc_map.ccmp_.size() == pages_before);

    // 3. Large section is now empty – pointer returned to tail_ccp_.
    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);

    // 4. Full chain integrity and partition invariant.
    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    LOG(INFO) << "[TC-ENSURE-04] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-ENSURE-05: EnsureLargeObjOccupyPageAlone – payload == threshold (boundary)
//               → NOT promoted (comparison is strict >)
// ---------------------------------------------------------------------------
//
// The large-object classification condition is SerializedLength() > threshold,
// not >=.  When the payload equals the threshold exactly, the page must NOT
// be marked as a large-object page.
//
// Steps follow the test plan:
//   a. Insert 1 key; commit with a payload of exactly LargeObjThresholdBytes().
//   b. Record ccmp_.size() before the call.
//   c. Call EnsureLargeObjOccupyPageAlone(ccp, cce).
//   d. Verify acceptance criteria.
// ---------------------------------------------------------------------------
TEST_CASE("TC-ENSURE-05: payload == threshold – NOT promoted under LO-LRU",
          "[lolru][ensure]")
{
    const std::string TABLE = "ensure05";
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    FLCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    // Obtain the exact threshold value from the shard so the test is not
    // hard-coded to the fixture's 1 KB setting.
    const size_t threshold_bytes = f.shard.LargeObjThresholdBytes();
    REQUIRE(threshold_bytes > 0);

    // -----------------------------------------------------------------------
    // Step a: insert 1 key and set its payload to exactly threshold_bytes.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-ENSURE-05] inserting 1 key with payload == threshold ("
              << threshold_bytes << " bytes)";
    TestKey key = std::make_tuple(TABLE, 0);
    bool emplace = false;
    auto it = cc_map.FindEmplace(key, &emplace, false, false);
    REQUIRE(emplace == true);

    FLCcEntry *cce = it->second;
    FLCcPage *ccp = it.GetPage();
    REQUIRE(ccp->Size() == 1u);

    bool was_dirty = cce->IsDirty();
    cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
    cce->SetCkptTs(200);
    cc_map.OnFlushed(cce, was_dirty);
    cc_map.OnCommittedUpdate(cce, was_dirty);
    ccp->last_dirty_commit_ts_ =
        std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
    // Payload == threshold (not strictly greater).
    cce->payload_.cur_payload_ =
        std::make_shared<FakeLargeRecord>(threshold_bytes);

    // -----------------------------------------------------------------------
    // Step b: record state before the call.
    // -----------------------------------------------------------------------
    size_t pages_before = cc_map.ccmp_.size();
    REQUIRE(pages_before == 1u);
    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);

    // -----------------------------------------------------------------------
    // Step c: call EnsureLargeObjOccupyPageAlone.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-ENSURE-05] calling EnsureLargeObjOccupyPageAlone";
    cc_map.EnsureLargeObjOccupyPageAlone(ccp, cce);

    // -----------------------------------------------------------------------
    // Step d: verify acceptance criteria.
    // -----------------------------------------------------------------------

    // 1. Page must NOT be a large-object page (payload == threshold, not >).
    REQUIRE(ccp->large_obj_page_ == false);

    // 2. No new page created.
    REQUIRE(cc_map.ccmp_.size() == pages_before);

    // 3. protected_head_page_ unchanged (still &tail_ccp_).
    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);

    // 4. Full chain integrity and partition invariant.
    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    LOG(INFO) << "[TC-ENSURE-05] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-LRU-01: Accessing a large-object page moves it to the MRU end of the
//            large section (just before tail_ccp_)
// ---------------------------------------------------------------------------
//
// After LO-LRU UpdateLruList is called on a large-object page P that is NOT
// the current protected_head_page_, the following must hold:
//   - P moves to tail_ccp_.lru_prev_ (newest/MRU end of large section).
//   - protected_head_page_ is unchanged (P was not the boundary page).
//   - VerifyLruList() and the partition invariant hold.
//
// Setup: promote 3 pages into the large section in sequence (A, B, C).
//   A = protected_head_page_ (oldest/LRU)
//   B = protected_head_page_->lru_next_   <- target P (second oldest)
//   C = tail_ccp_.lru_prev_               (newest/MRU before the test)
//
// After UpdateLruList(B, false):
//   expected order: [A] <-> [C] <-> [B] <-> tail
//   protected_head_page_ == A (unchanged)
//   tail_ccp_.lru_prev_  == B
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-LRU-01: accessing a large-object page moves it to large-section MRU "
    "under LO-LRU",
    "[lolru][lru]")
{
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    constexpr size_t large_payload_size = 2048;  // 2 KB > 1 KB threshold.

    // Helper: insert 1 key with a large payload into cc_map, commit it, and
    // promote the page in-place.  Returns the promoted FLCcPage pointer.
    auto PromoteLargePage = [&](FLCcMap &cc_map,
                                const std::string &table_name) -> FLCcPage *
    {
        TestKey key = std::make_tuple(table_name, 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(key, &emplace, false, false);
        REQUIRE(emplace == true);

        FLCcEntry *cce = it->second;
        FLCcPage *ccp = it.GetPage();
        REQUIRE(ccp->Size() == 1u);

        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cc_map.OnFlushed(cce, was_dirty);
        cc_map.OnCommittedUpdate(cce, was_dirty);
        ccp->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
        cce->payload_.cur_payload_ =
            std::make_shared<FakeLargeRecord>(large_payload_size);

        cc_map.EnsureLargeObjOccupyPageAlone(ccp, cce);
        REQUIRE(ccp->large_obj_page_ == true);
        return ccp;
    };

    // -----------------------------------------------------------------------
    // Promote 3 pages A, B, C into the large section (in that order).
    // After all 3 promotions the large section order from LRU to MRU is:
    //   protected_head=A  <->  B  <->  C  <->  tail
    // -----------------------------------------------------------------------
    TableName tn_a(
        std::string("lru01_a"), TableType::Primary, TableEngine::EloqSql);
    TableName tn_b(
        std::string("lru01_b"), TableType::Primary, TableEngine::EloqSql);
    TableName tn_c(
        std::string("lru01_c"), TableType::Primary, TableEngine::EloqSql);

    FLCcMap cc_map_a(&f.shard, 0, tn_a, 1, nullptr, true);
    FLCcMap cc_map_b(&f.shard, 0, tn_b, 1, nullptr, true);
    FLCcMap cc_map_c(&f.shard, 0, tn_c, 1, nullptr, true);

    FLCcPage *page_a = PromoteLargePage(cc_map_a, "lru01_a");
    FLCcPage *page_b = PromoteLargePage(cc_map_b, "lru01_b");
    FLCcPage *page_c = PromoteLargePage(cc_map_c, "lru01_c");

    // Verify the expected initial order.
    REQUIRE(f.shard.protected_head_page_ == page_a);
    REQUIRE(f.shard.protected_head_page_->lru_next_ == page_b);
    REQUIRE(f.shard.tail_ccp_.lru_prev_ == page_c);

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);

    // -----------------------------------------------------------------------
    // Access page B (target P: 2nd oldest, NOT the protected_head).
    // -----------------------------------------------------------------------
    LruPage *saved_head = f.shard.protected_head_page_;    // page_a
    LruPage *P = f.shard.protected_head_page_->lru_next_;  // page_b
    REQUIRE(P != saved_head);
    REQUIRE(P != &f.shard.tail_ccp_);
    REQUIRE(P->large_obj_page_ == true);

    LOG(INFO) << "[TC-LRU-01] calling UpdateLruList on the 2nd oldest large "
                 "page (B)";
    f.shard.UpdateLruList(P, /*is_emplace=*/false);

    // -----------------------------------------------------------------------
    // Verify acceptance criteria.
    // -----------------------------------------------------------------------

    // 1. P moved to the MRU end of the large section (just before tail).
    REQUIRE(f.shard.tail_ccp_.lru_prev_ == P);

    // 2. protected_head_page_ is unchanged (P was not the boundary page).
    REQUIRE(f.shard.protected_head_page_ == saved_head);

    // 3. The new LRU order of the large section is: A <-> C <-> B <-> tail.
    REQUIRE(f.shard.protected_head_page_->lru_next_ == page_c);
    REQUIRE(page_c->lru_next_ == P);
    REQUIRE(P->lru_next_ == &f.shard.tail_ccp_);

    // 4. Full chain integrity, partition invariant.
    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    LOG(INFO) << "[TC-LRU-01] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-LRU-02: Accessing a small-object page moves it to the MRU end of the
//            small section (just before protected_head_page_)
// ---------------------------------------------------------------------------
//
// After LO-LRU UpdateLruList is called on a small-object page P, the
// following must hold:
//   - P moves to protected_head_page_->lru_prev_ (newest/MRU end of small
//     section).
//   - protected_head_page_ is unchanged.
//   - VerifyLruList() and the partition invariant hold.
//
// Two phases are exercised:
//
//   Phase A – No large pages (protected_head_page_ == &tail_ccp_):
//       Insert 400 small objects (8+ pages).  Access the oldest small page
//       P = head_ccp_.lru_next_.  After UpdateLruList(P), P must be at
//       protected_head_page_->lru_prev_ == tail_ccp_.lru_prev_ (MRU of chain
//       when no large pages exist).
//
//   Phase B – With large pages (protected_head_page_ != &tail_ccp_):
//       Promote 3 pages into the large section so that
//       protected_head_page_ != &tail_ccp_.  Access the oldest remaining
//       small page P.  After UpdateLruList(P), P must be at
//       protected_head_page_->lru_prev_ (just before the boundary), NOT at
//       tail_ccp_.lru_prev_ (which is inside the large section).
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-LRU-02: accessing a small-object page moves it to small-section MRU "
    "under LO-LRU",
    "[lolru][lru]")
{
    const std::string TABLE = "lru02";
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    TestCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    // -----------------------------------------------------------------------
    // Phase A: No large pages present.
    // Insert 400 small objects → several pages in the small section;
    // protected_head_page_ == &tail_ccp_.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-LRU-02] Phase A: inserting 400 small objects (no large "
                 "pages)";
    f.InsertKeys(cc_map, TABLE, 400);

    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);
    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);

    // Oldest small page (LRU end of the chain, first after head_ccp_).
    LruPage *P_a = f.shard.head_ccp_.lru_next_;
    REQUIRE(P_a != &f.shard.tail_ccp_);
    REQUIRE(P_a->large_obj_page_ == false);

    // Record the pointer to what will become P_a's predecessor after the move.
    // With no large pages, MRU end = tail_ccp_.lru_prev_.
    LruPage *expected_mru_a = f.shard.protected_head_page_;  // == &tail_ccp_
    // The page just before protected_head_ is the future position of P_a.
    LruPage *future_prev_a = expected_mru_a->lru_prev_;  // current MRU page
    (void) future_prev_a;  // used in the REQUIRE below via pointer check

    LruPage *saved_protected_a = f.shard.protected_head_page_;

    LOG(INFO) << "[TC-LRU-02] Phase A: calling UpdateLruList on oldest small "
                 "page";
    f.shard.UpdateLruList(P_a, /*is_emplace=*/false);

    // 1. P_a is now at the MRU end of the small section
    //    = just before protected_head_page_ (== &tail_ccp_ here).
    REQUIRE(f.shard.protected_head_page_->lru_prev_ == P_a);

    // 2. protected_head_page_ is unchanged.
    REQUIRE(f.shard.protected_head_page_ == saved_protected_a);

    // 3. Since protected_head_ == &tail_ccp_, the small MRU == chain MRU.
    REQUIRE(f.shard.tail_ccp_.lru_prev_ == P_a);

    // 4. Full chain integrity and partition invariant.
    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    LOG(INFO) << "[TC-LRU-02] Phase A PASSED";

    // -----------------------------------------------------------------------
    // Phase B: With large pages present.
    // Promote 3 existing small pages into the large section so that
    // protected_head_page_ != &tail_ccp_.  Then access the oldest remaining
    // small page and confirm it lands just before protected_head_page_.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-LRU-02] Phase B: promoting 3 pages to large section";
    {
        std::vector<LruPage *> to_promote = f.CollectSmallPages(3);
        REQUIRE(to_promote.size() >= 3);

        for (LruPage *page : to_promote)
        {
            page->large_obj_page_ = true;
            f.shard.UpdateLruList(page, /*is_emplace=*/false);
        }

        REQUIRE(f.shard.protected_head_page_ != &f.shard.tail_ccp_);
        CheckLruListIntegrity(f.shard);
        CheckPartitionInvariant(f.shard);
    }

    // Oldest small page (LRU end of small section = head_ccp_.lru_next_).
    LruPage *P_b = f.shard.head_ccp_.lru_next_;
    REQUIRE(P_b != f.shard.protected_head_page_);
    REQUIRE(P_b->large_obj_page_ == false);

    LruPage *saved_protected_b = f.shard.protected_head_page_;

    LOG(INFO) << "[TC-LRU-02] Phase B: calling UpdateLruList on oldest small "
                 "page";
    f.shard.UpdateLruList(P_b, /*is_emplace=*/false);

    // 1. P_b is now at the MRU end of the small section (just before
    //    protected_head_page_, which is NOT &tail_ccp_).
    REQUIRE(f.shard.protected_head_page_->lru_prev_ == P_b);

    // 2. protected_head_page_ is unchanged.
    REQUIRE(f.shard.protected_head_page_ == saved_protected_b);

    // 3. P_b must NOT be at tail_ccp_.lru_prev_ (large section is there).
    REQUIRE(f.shard.tail_ccp_.lru_prev_ != P_b);

    // 4. Full chain integrity, partition invariant.
    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    LOG(INFO) << "[TC-LRU-02] Phase B PASSED";
    LOG(INFO) << "[TC-LRU-02] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-LRU-03: DetachLru on protected_head_page_ advances the pointer correctly
// ---------------------------------------------------------------------------
//
// When a LruPage that IS protected_head_page_ is detached from the LRU chain
// (via DetachLru), the pointer must advance to the original P->lru_next_.
//
// Three detach steps are exercised in sequence:
//
//   Initial:    protected_head = A  <->  B  <->  C  <->  tail
//
//   Step 1: DetachLru(A)  → protected_head = B  (B is the next large page)
//   Step 2: DetachLru(B)  → protected_head = C
//   Step 3: DetachLru(C)  → protected_head = &tail_ccp_ (large section empty)
//
// After each detach, CheckProtectedHeadInvariant and CheckLruListIntegrity
// are run.  The test uses pages that satisfy IsFree() (committed, flushed)
// so that DetachLru proceeds without skipping them.
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-LRU-03: DetachLru on protected_head_page_ advances the pointer "
    "under LO-LRU",
    "[lolru][lru]")
{
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    constexpr size_t large_payload_size = 2048;  // 2 KB > 1 KB threshold.

    // Helper: insert 1 free-eligible entry and promote the page in-place.
    auto PromoteLargePage = [&](FLCcMap &cc_map,
                                const std::string &table_name) -> FLCcPage *
    {
        TestKey key = std::make_tuple(table_name, 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(key, &emplace, false, false);
        REQUIRE(emplace == true);

        FLCcEntry *cce = it->second;
        FLCcPage *ccp = it.GetPage();

        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cc_map.OnFlushed(cce, was_dirty);
        cc_map.OnCommittedUpdate(cce, was_dirty);
        ccp->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
        cce->payload_.cur_payload_ =
            std::make_shared<FakeLargeRecord>(large_payload_size);

        cc_map.EnsureLargeObjOccupyPageAlone(ccp, cce);
        REQUIRE(ccp->large_obj_page_ == true);
        return ccp;
    };

    // -----------------------------------------------------------------------
    // Setup: promote 3 pages A, B, C into the large section (in that order).
    //   protected_head = A  <->  B  <->  C  <->  tail
    // -----------------------------------------------------------------------
    TableName tn_a(
        std::string("lru03_a"), TableType::Primary, TableEngine::EloqSql);
    TableName tn_b(
        std::string("lru03_b"), TableType::Primary, TableEngine::EloqSql);
    TableName tn_c(
        std::string("lru03_c"), TableType::Primary, TableEngine::EloqSql);

    FLCcMap cc_map_a(&f.shard, 0, tn_a, 1, nullptr, true);
    FLCcMap cc_map_b(&f.shard, 0, tn_b, 1, nullptr, true);
    FLCcMap cc_map_c(&f.shard, 0, tn_c, 1, nullptr, true);

    FLCcPage *page_a = PromoteLargePage(cc_map_a, "lru03_a");
    FLCcPage *page_b = PromoteLargePage(cc_map_b, "lru03_b");
    FLCcPage *page_c = PromoteLargePage(cc_map_c, "lru03_c");

    // Verify initial ordering.
    REQUIRE(f.shard.protected_head_page_ == page_a);
    REQUIRE(f.shard.protected_head_page_->lru_next_ == page_b);
    REQUIRE(page_b->lru_next_ == page_c);
    REQUIRE(f.shard.tail_ccp_.lru_prev_ == page_c);

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    // -----------------------------------------------------------------------
    // Step 1: DetachLru(page_a) — detach protected_head_page_.
    // Expected: protected_head advances from A to B.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-LRU-03] Step 1: DetachLru(page_a) – protected_head A";
    {
        LruPage *expected_next = page_a->lru_next_;  // B
        REQUIRE(expected_next == page_b);

        f.shard.DetachLru(page_a);

        REQUIRE(f.shard.protected_head_page_ == expected_next);
        REQUIRE(f.shard.protected_head_page_ == page_b);
        REQUIRE(f.shard.protected_head_page_->large_obj_page_ == true);

        CheckProtectedHeadInvariant(f.shard);
        CheckLruListIntegrity(f.shard);
        CheckPartitionInvariant(f.shard);
    }

    // -----------------------------------------------------------------------
    // Step 2: DetachLru(page_b) — detach the new protected_head_page_.
    // Expected: protected_head advances from B to C.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-LRU-03] Step 2: DetachLru(page_b) – protected_head B";
    {
        REQUIRE(f.shard.protected_head_page_ == page_b);
        LruPage *expected_next = page_b->lru_next_;  // C
        REQUIRE(expected_next == page_c);

        f.shard.DetachLru(page_b);

        REQUIRE(f.shard.protected_head_page_ == expected_next);
        REQUIRE(f.shard.protected_head_page_ == page_c);
        REQUIRE(f.shard.protected_head_page_->large_obj_page_ == true);

        CheckProtectedHeadInvariant(f.shard);
        CheckLruListIntegrity(f.shard);
        CheckPartitionInvariant(f.shard);
    }

    // -----------------------------------------------------------------------
    // Step 3: DetachLru(page_c) — detach the last large page.
    // Expected: protected_head returns to &tail_ccp_ (large section empty).
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-LRU-03] Step 3: DetachLru(page_c) – last large page";
    {
        REQUIRE(f.shard.protected_head_page_ == page_c);
        LruPage *expected_next = page_c->lru_next_;  // &tail_ccp_
        REQUIRE(expected_next == &f.shard.tail_ccp_);

        f.shard.DetachLru(page_c);

        REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);

        CheckProtectedHeadInvariant(f.shard);
        CheckLruListIntegrity(f.shard);
        CheckPartitionInvariant(f.shard);
    }

    LOG(INFO) << "[TC-LRU-03] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-LRU-04: No large objects → protected_head_page_ always equals &tail_ccp_
// ---------------------------------------------------------------------------
//
// When no large-object pages are present in a shard, protected_head_page_
// must always equal &tail_ccp_ regardless of how many small pages exist or
// how many UpdateLruList calls are made on them.
//
// Three checkpoints are exercised:
//
//   Checkpoint 1 – Empty shard:
//       protected_head_page_ == &tail_ccp_  (trivially true after fixture init)
//
//   Checkpoint 2 – After inserting 400 small objects (many pages):
//       protected_head_page_ == &tail_ccp_  (no promotion happened)
//
//   Checkpoint 3 – After 10 UpdateLruList accesses on small pages:
//       protected_head_page_ == &tail_ccp_  (accesses never move large ptr)
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-LRU-04: no large objects – protected_head_page_ == &tail_ccp_ "
    "under LO-LRU",
    "[lolru][lru]")
{
    const std::string TABLE = "lru04";
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    TestCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    // -----------------------------------------------------------------------
    // Checkpoint 1: empty shard.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-LRU-04] Checkpoint 1: empty shard";
    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);
    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    // -----------------------------------------------------------------------
    // Checkpoint 2: after inserting 400 small objects (no large payloads).
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-LRU-04] Checkpoint 2: after InsertKeys(400)";
    f.InsertKeys(cc_map, TABLE, 400);

    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);
    REQUIRE(f.CollectLargePages().empty());

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    // -----------------------------------------------------------------------
    // Checkpoint 3: after 10 UpdateLruList accesses on small pages.
    // Walk from head to tail, calling UpdateLruList on each small page found.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-LRU-04] Checkpoint 3: after 10 UpdateLruList accesses";
    {
        int accesses = 0;
        LruPage *cur = f.shard.head_ccp_.lru_next_;
        while (cur != &f.shard.tail_ccp_ && accesses < 10)
        {
            REQUIRE(cur->large_obj_page_ == false);
            LruPage *next = cur->lru_next_;  // save before the move
            f.shard.UpdateLruList(cur, /*is_emplace=*/false);
            // protected_head_page_ must still be &tail_ccp_ after every call.
            REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);
            cur = next;
            ++accesses;
        }
        REQUIRE(accesses == 10);
    }

    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);
    REQUIRE(f.CollectLargePages().empty());

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    LOG(INFO) << "[TC-LRU-04] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-LRU-05: Only large objects → small section empty, entire page chain
//            consists of large-object pages
// ---------------------------------------------------------------------------
//
// When every page in the shard is a large-object page, the small section must
// be completely empty:
//
//   head_ccp_.lru_next_ == protected_head_page_
//
// and every page between protected_head_page_ and tail_ccp_ must have
// large_obj_page_ == true.
//
// Steps:
//   1. Verify initial state (empty shard): head.lru_next_ == &tail_ccp_.
//   2. Promote N=5 pages into the large section one by one.
//   3. After each promotion verify head.lru_next_ == protected_head_page_
//      (small section remains empty throughout).
//   4. Final full-chain scan: every page is large_obj_page_ == true.
//   5. CollectSmallPages() returns nothing.
//   6. All invariant helpers pass.
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-LRU-05: only large-object pages – small section always empty "
    "under LO-LRU",
    "[lolru][lru]")
{
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    constexpr size_t large_payload_size = 2048;  // 2 KB > 1 KB threshold.
    constexpr int N = 5;  // number of large pages to create.

    // Helper: insert 1 free-eligible entry and promote the page in-place.
    auto PromoteLargePage = [&](FLCcMap &cc_map,
                                const std::string &key_prefix) -> FLCcPage *
    {
        TestKey key = std::make_tuple(key_prefix, 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(key, &emplace, false, false);
        REQUIRE(emplace == true);

        FLCcEntry *cce = it->second;
        FLCcPage *ccp = it.GetPage();

        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cc_map.OnFlushed(cce, was_dirty);
        cc_map.OnCommittedUpdate(cce, was_dirty);
        ccp->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
        cce->payload_.cur_payload_ =
            std::make_shared<FakeLargeRecord>(large_payload_size);

        cc_map.EnsureLargeObjOccupyPageAlone(ccp, cce);
        REQUIRE(ccp->large_obj_page_ == true);
        return ccp;
    };

    // -----------------------------------------------------------------------
    // Step 1: empty shard – trivially no small pages.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-LRU-05] Step 1: empty shard";
    REQUIRE(f.shard.head_ccp_.lru_next_ == &f.shard.tail_ccp_);
    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);
    // head.lru_next_ == protected_head_page_ (both point to tail).
    REQUIRE(f.shard.head_ccp_.lru_next_ == f.shard.protected_head_page_);

    // -----------------------------------------------------------------------
    // Steps 2+3: promote N pages one by one; after each, the small section
    //            must still be empty.
    // -----------------------------------------------------------------------
    std::vector<FLCcMap *> maps;  // keep alive for the test duration.
    maps.reserve(N);

    for (int i = 0; i < N; ++i)
    {
        std::string table_name = "lru05_" + std::to_string(i);
        TableName tn(table_name, TableType::Primary, TableEngine::EloqSql);
        auto *cm = new FLCcMap(&f.shard, 0, tn, 1, nullptr, true);
        maps.push_back(cm);

        PromoteLargePage(*cm, table_name);

        LOG(INFO) << "[TC-LRU-05] After promoting page " << (i + 1)
                  << ": checking small section empty";

        // Core invariant: small section is empty.
        REQUIRE(f.shard.head_ccp_.lru_next_ == f.shard.protected_head_page_);
        REQUIRE(f.shard.protected_head_page_ != &f.shard.tail_ccp_);
        REQUIRE(f.shard.protected_head_page_->large_obj_page_ == true);

        CheckLruListIntegrity(f.shard);
        CheckPartitionInvariant(f.shard);
        CheckProtectedHeadInvariant(f.shard);
    }

    // -----------------------------------------------------------------------
    // Step 4: full chain scan – every page must be large_obj_page_ == true.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-LRU-05] Step 4: full chain scan";
    {
        int large_count = 0;
        LruPage *cur = f.shard.head_ccp_.lru_next_;
        while (cur != &f.shard.tail_ccp_)
        {
            REQUIRE(cur->large_obj_page_ == true);
            ++large_count;
            cur = cur->lru_next_;
        }
        REQUIRE(large_count == N);
    }

    // -----------------------------------------------------------------------
    // Step 5: CollectSmallPages() returns nothing.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-LRU-05] Step 5: CollectSmallPages() empty";
    REQUIRE(f.CollectSmallPages(1000).empty());

    // -----------------------------------------------------------------------
    // Step 6: final invariant sweep.
    // -----------------------------------------------------------------------
    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    LOG(INFO) << "[TC-LRU-05] PASSED";

    for (auto *cm : maps)
    {
        delete cm;
    }
    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-FE-01: FindEmplace on key K whose target page is a large-object page
//           routes to the next normal page (page B).
// ---------------------------------------------------------------------------
//
// ccmp_ is keyed by each page's first key; upper_bound(K) followed by
// --target_it locates the target page. When that page has large_obj_page_==true
// FindEmplace advances target_it++ and uses the next page instead.
//
// Setup:
//   page A – large_obj_page_==true, 1 entry "fe01_a", first key = "fe01_a"
//   page B – large_obj_page_==false, 1 entry "fe01_z", first key = "fe01_z"
//
// Key K = "fe01_m" satisfies  "fe01_a" <= "fe01_m" < "fe01_z", so
// upper_bound("fe01_m") -> page B, --target_it -> page A.
// Page A being large routes to page B.
//
// Acceptance:
//   emplace == true
//   K is placed into page B        (page_k == page_b)
//   page A's Size() unchanged      (still 1)
//   no new page is created         (ccmp_.size() unchanged)
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-FE-01: FindEmplace routes away from large-object page to next normal "
    "page under LO-LRU",
    "[lolru][fe]")
{
    const std::string TABLE = "fe01";
    LoLruFixture f;

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    FLCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    // -----------------------------------------------------------------------
    // Step 1: Insert "fe01_a", promote its page to large (in-place, size==1).
    // -----------------------------------------------------------------------
    TestKey key_lo = std::make_tuple(std::string("fe01_a"), 0);
    {
        bool emplace = false;
        auto it = cc_map.FindEmplace(key_lo, &emplace, false, false);
        REQUIRE(emplace == true);
        FLCcEntry *cce = it->second;
        FLCcPage *ccp = it.GetPage();

        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cc_map.OnFlushed(cce, was_dirty);
        cc_map.OnCommittedUpdate(cce, was_dirty);
        ccp->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
        cce->payload_.cur_payload_ =
            std::make_shared<FakeLargeRecord>(2048);  // > 1 KB threshold

        cc_map.EnsureLargeObjOccupyPageAlone(ccp, cce);
        REQUIRE(ccp->large_obj_page_ == true);
        REQUIRE(ccp->Size() == 1u);
    }

    // Locate page A for later assertions.
    FLCcPage *page_a = cc_map.ccmp_.begin()->second.get();
    REQUIRE(page_a->large_obj_page_ == true);
    REQUIRE(page_a->Size() == 1u);

    // -----------------------------------------------------------------------
    // Step 2: Insert "fe01_z" (> "fe01_a").  FindEmplace routes away from
    // page A and creates normal page B keyed at "fe01_z" in ccmp_.
    // -----------------------------------------------------------------------
    TestKey key_hi = std::make_tuple(std::string("fe01_z"), 0);
    FLCcPage *page_b = nullptr;
    {
        bool emplace = false;
        auto it = cc_map.FindEmplace(key_hi, &emplace, false, false);
        REQUIRE(emplace == true);
        page_b = it.GetPage();
        REQUIRE(page_b != page_a);
        REQUIRE(page_b->large_obj_page_ == false);
        REQUIRE(page_b->Size() == 1u);
        REQUIRE(page_a->Size() == 1u);  // A untouched
    }
    REQUIRE(cc_map.ccmp_.size() == 2u);

    // -----------------------------------------------------------------------
    // Step 3 (main operation): Insert K = "fe01_m" (between lo and hi).
    //   upper_bound("fe01_m") -> page B (first key "fe01_z" > "fe01_m")
    //   --target_it             -> page A (first key "fe01_a" <= "fe01_m")
    //   page A large_obj_page_  -> target_it++ -> page B
    //   page B: not large, not full -> insert into B.
    // -----------------------------------------------------------------------
    const size_t size_a_before = page_a->Size();  // 1
    const size_t size_b_before = page_b->Size();  // 1

    TestKey key_mid = std::make_tuple(std::string("fe01_m"), 0);
    bool emplace_k = false;
    auto it_k = cc_map.FindEmplace(key_mid, &emplace_k, false, false);

    REQUIRE(emplace_k == true);

    FLCcPage *page_k = it_k.GetPage();
    REQUIRE(page_k == page_b);                      // K routed to page B
    REQUIRE(page_a->Size() == size_a_before);       // A unchanged
    REQUIRE(page_b->Size() == size_b_before + 1u);  // B gained K
    REQUIRE(cc_map.ccmp_.size() == 2u);             // no new page created

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    LOG(INFO) << "[TC-FE-01] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-FE-02: FindEmplace on key K whose target page is large AND the next page
//           is also large → creates a new normal page C for K.
// ---------------------------------------------------------------------------
//
// When the routing code advances from page A (large) to page B (also large),
// emplace_page is set to true and a brand-new normal page C is inserted into
// ccmp_ between A and B.
//
// Setup:
//   page A – large_obj_page_==true, 1 entry "fe02_a", ccmp_ key = "fe02_a"
//   page B – large_obj_page_==true, 1 entry "fe02_z", ccmp_ key = "fe02_z"
//
// Key K = "fe02_m" satisfies  "fe02_a" <= "fe02_m" < "fe02_z", so
//   upper_bound("fe02_m") -> page B
//   --target_it            -> page A  (large)
//   target_it++            -> page B  (also large)  => emplace_page = true
//   try_emplace at page B  -> new page C keyed "fe02_m" inserted before B
//   K inserted into C
//
// Acceptance:
//   emplace == true
//   K is in new page C (large_obj_page_==false)
//   page A Size() unchanged (1)
//   page B Size() unchanged (1)
//   ccmp_.size() grows from 2 to 3
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-FE-02: FindEmplace creates new page when both target and next pages "
    "are large-object pages under LO-LRU",
    "[lolru][fe]")
{
    const std::string TABLE = "fe02";
    LoLruFixture f;

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    FLCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    // Helper: commit an entry and promote its page in-place (Size()==1).
    auto PromoteInPlace = [&](FLCcPage *ccp, FLCcEntry *cce)
    {
        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cc_map.OnFlushed(cce, was_dirty);
        cc_map.OnCommittedUpdate(cce, was_dirty);
        ccp->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
        cce->payload_.cur_payload_ = std::make_shared<FakeLargeRecord>(2048);
        cc_map.EnsureLargeObjOccupyPageAlone(ccp, cce);
        REQUIRE(ccp->large_obj_page_ == true);
    };

    // -----------------------------------------------------------------------
    // Step 1: Insert "fe02_a" and promote → page A (large).
    // -----------------------------------------------------------------------
    TestKey key_lo = std::make_tuple(std::string("fe02_a"), 0);
    FLCcPage *page_a = nullptr;
    {
        bool emplace = false;
        auto it = cc_map.FindEmplace(key_lo, &emplace, false, false);
        REQUIRE(emplace == true);
        page_a = it.GetPage();
        PromoteInPlace(page_a, it->second);
    }
    REQUIRE(page_a->Size() == 1u);
    REQUIRE(cc_map.ccmp_.size() == 1u);

    // -----------------------------------------------------------------------
    // Step 2: Insert "fe02_z" — routes away from A (large), creates page B.
    //         Then promote page B to large as well.
    // -----------------------------------------------------------------------
    TestKey key_hi = std::make_tuple(std::string("fe02_z"), 0);
    FLCcPage *page_b = nullptr;
    {
        bool emplace = false;
        auto it = cc_map.FindEmplace(key_hi, &emplace, false, false);
        REQUIRE(emplace == true);
        page_b = it.GetPage();
        REQUIRE(page_b != page_a);
        // Promote page B to large.
        PromoteInPlace(page_b, it->second);
    }
    REQUIRE(page_b->Size() == 1u);
    REQUIRE(page_a->Size() == 1u);  // A unchanged
    REQUIRE(cc_map.ccmp_.size() == 2u);

    // Both pages are now large.
    REQUIRE(page_a->large_obj_page_ == true);
    REQUIRE(page_b->large_obj_page_ == true);

    // -----------------------------------------------------------------------
    // Step 3 (main operation): Insert K = "fe02_m".
    //   upper_bound("fe02_m") -> page B; --target_it -> page A (large)
    //   target_it++ -> page B (also large) -> emplace_page = true
    //   New page C inserted before B with key "fe02_m".
    // -----------------------------------------------------------------------
    const size_t size_a_before = page_a->Size();  // 1
    const size_t size_b_before = page_b->Size();  // 1

    TestKey key_mid = std::make_tuple(std::string("fe02_m"), 0);
    bool emplace_k = false;
    auto it_k = cc_map.FindEmplace(key_mid, &emplace_k, false, false);

    REQUIRE(emplace_k == true);

    FLCcPage *page_c = it_k.GetPage();
    REQUIRE(page_c != page_a);
    REQUIRE(page_c != page_b);
    REQUIRE(page_c->large_obj_page_ == false);  // new page is normal
    REQUIRE(page_c->Size() == 1u);              // only K

    REQUIRE(page_a->Size() == size_a_before);  // A unchanged
    REQUIRE(page_b->Size() == size_b_before);  // B unchanged
    REQUIRE(cc_map.ccmp_.size() == 3u);        // one new page created

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    LOG(INFO) << "[TC-FE-02] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-FE-03: FindEmplace on key K whose target page is large AND the next page
//           is a normal page that is Full() → creates new page C.
// ---------------------------------------------------------------------------
//
// Routing code path:
//   A (large) -> target_it++ -> B (normal, Full()) -> emplace_page = true
//   -> new page C created before B, K inserted into C.
//
// Setup:
//   page A – large_obj_page_==true, 1 entry, ccmp_ key = ("fe03_a", 0)
//   page B – large_obj_page_==false, Full() == true (64 entries),
//             ccmp_ key = ("fe03_b000", 0)
//   Keys in B: ("fe03_b000", 0) … ("fe03_b063", 0)
//
// K = ("fe03_b", 0) satisfies "fe03_a" <= "fe03_b" < "fe03_b000", so:
//   upper_bound(K) -> B (ccmp_ key "fe03_b000" > "fe03_b")
//   --target_it    -> A (ccmp_ key "fe03_a"   <= "fe03_b")
//   A: large       -> target_it++ -> B
//   B: Full()      -> emplace_page = true -> new page C created before B
//
// Acceptance:
//   emplace == true
//   K is in new page C (large_obj_page_==false)
//   page A Size() == 1      (unchanged)
//   page B Size() == 64     (unchanged, Full())
//   ccmp_.size() == 3       (was 2, grew by 1)
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-FE-03: FindEmplace creates new page when target page is large and "
    "next page is full under LO-LRU",
    "[lolru][fe]")
{
    const std::string TABLE = "fe03";
    LoLruFixture f;

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    FLCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    // -----------------------------------------------------------------------
    // Step 1: Insert ("fe03_a", 0) and promote page A to large (in-place).
    // -----------------------------------------------------------------------
    TestKey key_a = std::make_tuple(std::string("fe03_a"), 0);
    FLCcPage *page_a = nullptr;
    {
        bool emplace = false;
        auto it = cc_map.FindEmplace(key_a, &emplace, false, false);
        REQUIRE(emplace == true);
        page_a = it.GetPage();
        FLCcEntry *cce = it->second;

        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cc_map.OnFlushed(cce, was_dirty);
        cc_map.OnCommittedUpdate(cce, was_dirty);
        page_a->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), page_a->last_dirty_commit_ts_);
        cce->payload_.cur_payload_ = std::make_shared<FakeLargeRecord>(2048);
        cc_map.EnsureLargeObjOccupyPageAlone(page_a, cce);
        REQUIRE(page_a->large_obj_page_ == true);
        REQUIRE(page_a->Size() == 1u);
    }

    // -----------------------------------------------------------------------
    // Step 2: Fill page B to exactly split_threshold_ (64) entries.
    //
    // Keys "fe03_b000" … "fe03_b063" are all lexicographically greater than
    // "fe03_a" and greater than K = "fe03_b".
    // The first key "fe03_b000" routes through A (large, end-of-map path)
    // and creates page B.  Subsequent keys route directly to page B.
    // -----------------------------------------------------------------------
    constexpr size_t split_threshold = FLCcPage::split_threshold_;
    FLCcPage *page_b = nullptr;

    for (size_t i = 0; i < split_threshold; ++i)
    {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "fe03_b%03zu", i);
        TestKey ki = std::make_tuple(std::string(buf), 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(ki, &emplace, false, false);
        REQUIRE(emplace == true);
        FLCcPage *p = it.GetPage();
        if (i == 0)
        {
            page_b = p;
            REQUIRE(page_b != page_a);
            REQUIRE(page_b->large_obj_page_ == false);
        }
        else
        {
            // All subsequent keys land on page_b (not on page_a or a new page).
            REQUIRE(p == page_b);
        }
    }

    REQUIRE(page_b->Size() == split_threshold);
    REQUIRE(page_b->Full() == true);
    REQUIRE(page_a->Size() == 1u);       // A untouched
    REQUIRE(cc_map.ccmp_.size() == 2u);  // still only A and B

    // -----------------------------------------------------------------------
    // Step 3 (main operation): Insert K = ("fe03_b", 0).
    //   "fe03_a" <= "fe03_b" < "fe03_b000" so routing:
    //     upper_bound -> B;  --target_it -> A (large)
    //     advance to B (Full()) -> emplace_page = true -> new page C
    // -----------------------------------------------------------------------
    TestKey key_k = std::make_tuple(std::string("fe03_b"), 0);
    bool emplace_k = false;
    auto it_k = cc_map.FindEmplace(key_k, &emplace_k, false, false);

    REQUIRE(emplace_k == true);

    FLCcPage *page_c = it_k.GetPage();
    REQUIRE(page_c != page_a);
    REQUIRE(page_c != page_b);
    REQUIRE(page_c->large_obj_page_ == false);  // new page is normal
    REQUIRE(page_c->Size() == 1u);              // only K

    REQUIRE(page_a->Size() == 1u);               // A unchanged
    REQUIRE(page_b->Size() == split_threshold);  // B unchanged, still full
    REQUIRE(cc_map.ccmp_.size() == 3u);          // one new page created

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    LOG(INFO) << "[TC-FE-03] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-MERGE-01: Small-object page S does NOT merge with adjacent large-object
//              page L even when S.Size() drops below merge_threshold_.
// ---------------------------------------------------------------------------
//
// Merge guard in RebalancePage (LO-LRU):
//   merge_with_prev = page->large_obj_page_==false &&
//   prev->large_obj_page_==false merge_with_next = page->large_obj_page_==false
//   && next->large_obj_page_==false
//
// If either side is a large-object page, merge is blocked regardless of size.
//
// Page linked-list order (ccmp_ sorted by first key):
//   neg_inf <-> L ("merge01_a") <-> S ("merge01_s0") <-> pos_inf
//   => S.prev_page_ = L, L.next_page_ = S
//
// Setup:
//   L – large_obj_page_==true, 1 non-free entry (partial commit: no SetCkptTs)
//   S – large_obj_page_==false, 5 entries: 4 free + 1 dirty (non-free)
//
// After f.shard.Clean():
//   CleanPage(S)  removes 4 free entries → S.Size()=1 < merge_threshold_(32)
//   RebalancePage(S): prev=L (large) → merge_with_prev=false, no next → blocked
//   CleanPage(L)  removes 0 entries  → L.Size()=1 < 32
//   RebalancePage(L): prev=neg_inf → blocked, next=S → merge_with_next check:
//     L.large_obj_page_==true → merge_with_next=false → blocked
//
// Acceptance:
//   S.Size() == 1   (dirty entry survived, no merge)
//   L.Size() == 1   (large page survives unchanged)
//   ccmp_.size() == 2  (both pages still present)
//   All invariant checks pass
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-MERGE-01: small page not merged with adjacent large-object page under "
    "LO-LRU",
    "[lolru][merge]")
{
    const std::string TABLE = "merge01";
    LoLruFixture f;

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    FLCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    // -----------------------------------------------------------------------
    // Step 1: Insert ("merge01_a", 0), partially commit
    // (SetCommitTsPayloadStatus only — no
    // SetCkptTs/OnFlushed/OnCommittedUpdate), inject large payload, promote
    // in-place.  ckpt_ts stays 0 < commit_ts 100 → IsFree()==false → CleanPage
    // will NOT evict this entry.
    // -----------------------------------------------------------------------
    FLCcPage *page_L = nullptr;
    {
        TestKey key_a = std::make_tuple(std::string("merge01_a"), 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(key_a, &emplace, false, false);
        REQUIRE(emplace == true);
        page_L = it.GetPage();
        FLCcEntry *cce = it->second;

        // Partial commit: set PayloadStatus=Normal but skip ckpt/flush/commit.
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        // Inject large payload (needed by EnsureLargeObjOccupyPageAlone).
        cce->payload_.cur_payload_ = std::make_shared<FakeLargeRecord>(2048);
        cc_map.EnsureLargeObjOccupyPageAlone(page_L, cce);

        REQUIRE(page_L->large_obj_page_ == true);
        REQUIRE(page_L->Size() == 1u);
    }
    // Pre-register L's entry as dirty so Terminate() can safely decrement.
    f.shard.AdjustDataKeyStats(tn, 0, 1);

    // -----------------------------------------------------------------------
    // Step 2: Insert 5 entries into page S.
    // All keys "merge01_s0"…"merge01_s4" are > "merge01_a", so routing skips
    // page L (large) and lands in page S.
    // -----------------------------------------------------------------------
    FLCcPage *page_S = nullptr;
    for (int i = 0; i < 5; ++i)
    {
        TestKey ki =
            std::make_tuple(std::string("merge01_s") + std::to_string(i), 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(ki, &emplace, false, false);
        REQUIRE(emplace == true);
        FLCcPage *p = it.GetPage();
        if (i == 0)
        {
            page_S = p;
            REQUIRE(page_S != page_L);
            REQUIRE(page_S->large_obj_page_ == false);
        }
        else
        {
            REQUIRE(p == page_S);
        }
    }
    REQUIRE(page_S->Size() == 5u);
    REQUIRE(cc_map.ccmp_.size() == 2u);

    // -----------------------------------------------------------------------
    // Step 3: Make entries "merge01_s0"…"merge01_s3" free (full commit dance).
    // Entry "merge01_s4" is partially committed (SetCommitTsPayloadStatus only,
    // no SetCkptTs/OnFlushed/OnCommittedUpdate):
    //   commit_ts=100, ckpt_ts=0  →  ckpt_ts < commit_ts  →  IsFree()==false
    // A freshly-inserted uncommitted entry has both ts==0, so
    // ckpt_ts>=commit_ts and is treated as free.  Partial commit is the minimum
    // to make it non-free.
    // -----------------------------------------------------------------------
    for (int i = 0; i < 4; ++i)
    {
        TestKey ki =
            std::make_tuple(std::string("merge01_s") + std::to_string(i), 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(ki, &emplace, false, false);
        REQUIRE(emplace == false);
        REQUIRE(it.GetPage() == page_S);
        FLCcEntry *cce = it->second;

        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cc_map.OnFlushed(cce, was_dirty);
        cc_map.OnCommittedUpdate(cce, was_dirty);
        page_S->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), page_S->last_dirty_commit_ts_);
    }

    // Partially commit s4: commit_ts=100, ckpt_ts=0 → IsFree()==false.
    {
        TestKey k4 = std::make_tuple(std::string("merge01_s4"), 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(k4, &emplace, false, false);
        REQUIRE(emplace == false);
        REQUIRE(it.GetPage() == page_S);
        it->second->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        // No SetCkptTs/OnFlushed/OnCommittedUpdate → stays non-free.
    }
    // Pre-register s4 as dirty so Terminate() can safely decrement.
    f.shard.AdjustDataKeyStats(tn, 0, 1);

    // Sanity: S has 5 entries, L has 1, two pages total, LRU intact.
    REQUIRE(page_S->Size() == 5u);
    REQUIRE(page_L->Size() == 1u);
    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    // -----------------------------------------------------------------------
    // Step 4: Clean only page S via CleanPageAndReBalance.
    //   CleanPage removes 4 free entries (s0-s3) → S.Size()=1 <
    //   merge_threshold_(32) RebalancePage: S.prev_page_=L (large) →
    //   merge_with_prev=false (blocked)
    //                  no next page         → merge_with_next not attempted
    // We do NOT clean page L here; its non-free entry stays untouched and the
    // large_obj_page_ flag is unaffected.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-MERGE-01] Calling CleanPageAndReBalance on page_S";
    cc_map.CleanPageAndReBalance(page_S, nullptr, nullptr);

    // -----------------------------------------------------------------------
    // Verify: no merge occurred; both pages survive independently.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-MERGE-01] Verifying post-Clean state";

    // S survived with its one dirty entry.
    REQUIRE(page_S->Size() == 1u);

    // L survived unchanged.
    REQUIRE(page_L->Size() == 1u);
    REQUIRE(page_L->large_obj_page_ == true);

    // Both pages still in ccmp_.
    REQUIRE(cc_map.ccmp_.size() == 2u);

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    LOG(INFO) << "[TC-MERGE-01] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-MERGE-02: Small-object page S does NOT borrow from adjacent large-object
//              page L even when L has a surplus of entries.
// ---------------------------------------------------------------------------
//
// Borrow guard in RebalancePage (LO-LRU):
//   borrow_from_prev = page->large_obj_page_==false &&
//   prev->large_obj_page_==false borrow_from_next =
//   page->large_obj_page_==false && next->large_obj_page_==false
//
// When the prev (or next) page is a large-object page, borrow is blocked even
// if that page holds more than merge_threshold_ entries.  Without the guard,
// borrow would succeed and entries would be moved from L to S.
//
// To isolate the flag as the sole reason borrow is blocked, L is manually
// promoted (large_obj_page_ = true) AFTER inserting L_COUNT=35 entries.
// This ensures L has surplus (35 > merge_threshold_=32) that borrow WOULD
// use if not for the flag.
//
// Page linked-list order (ccmp_ sorted by first key):
//   neg_inf <-> L ("merge02_a*") <-> S ("merge02_s*") <-> pos_inf
//
// Setup:
//   L – 35 non-free entries (partial commit only), large_obj_page_=true
//        (set manually + UpdateLruList to move to the large section)
//   S – 5 entries: 4 free (full commit) + 1 non-free (partial commit)
//
// After CleanPageAndReBalance(S):
//   CleanPage(S) removes 4 free entries → S.Size()=1 < merge_threshold_(32)
//   RebalancePage(S): prev=L (large) →
//     borrow_from_prev = false  (blocked by large flag)
//     merge_with_prev  = false  (same guard)
//   S has no next page → no borrow_from_next or merge_with_next.
//
// Acceptance:
//   S.Size() == 1   (only the non-free s4 entry survives; no merge)
//   L.Size() == 35  (no entries borrowed from L)
//   ccmp_.size() == 2  (both pages still present)
//   All LRU invariant checks pass
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-MERGE-02: small page not borrowing from adjacent large-object page "
    "under LO-LRU",
    "[lolru][merge]")
{
    const std::string TABLE = "merge02";
    LoLruFixture f;

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    FLCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    // -----------------------------------------------------------------------
    // Step 1: Insert L_COUNT=35 non-free entries into page L.
    // "merge02_a00"…"merge02_a34" all fit on one page (split_threshold_=64).
    // Partial commit: SetCommitTsPayloadStatus(100,Normal) only, no SetCkptTs.
    //   commit_ts=100 > ckpt_ts=0  →  IsFree()==false  →  won't be evicted.
    // Then manually set large_obj_page_=true and call UpdateLruList to move L
    // into the large section (updating protected_head_page_).
    // -----------------------------------------------------------------------
    constexpr int L_COUNT =
        35;  // > merge_threshold_(32): borrow WOULD succeed normally.

    FLCcPage *page_L = nullptr;
    for (int i = 0; i < L_COUNT; ++i)
    {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "merge02_a%02d", i);
        TestKey ki = std::make_tuple(std::string(buf), 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(ki, &emplace, false, false);
        REQUIRE(emplace == true);
        FLCcPage *p = it.GetPage();
        if (i == 0)
        {
            page_L = p;
            REQUIRE(page_L->large_obj_page_ == false);
        }
        else
        {
            REQUIRE(p == page_L);
        }
        // Partial commit → IsFree()==false (commit_ts=100 > ckpt_ts=0).
        it->second->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
    }
    REQUIRE(page_L != nullptr);
    REQUIRE(page_L->Size() == static_cast<size_t>(L_COUNT));

    // Pre-register all L entries as dirty so Terminate() can safely decrement.
    f.shard.AdjustDataKeyStats(tn, 0, L_COUNT);

    // Manually promote L to large-object page so the borrow/merge guard fires.
    // This bypasses EnsureLargeObjOccupyPageAlone's exclusivity enforcement
    // intentionally: the goal is to prove the flag alone blocks borrow.
    page_L->large_obj_page_ = true;
    f.shard.UpdateLruList(page_L, /*is_emplace=*/false);

    REQUIRE(f.shard.protected_head_page_ == page_L);
    REQUIRE(page_L->large_obj_page_ == true);
    REQUIRE(page_L->Size() == static_cast<size_t>(L_COUNT));  // surplus intact

    CheckLruListIntegrity(f.shard);
    // Note: CheckExclusivityInvariant would fail (L has 35 entries, not 1).
    // That is intentional — this test is exclusively about the borrow guard.

    // -----------------------------------------------------------------------
    // Step 2: Insert 5 entries into page S.
    // Keys "merge02_s0"…"merge02_s4" are lexicographically greater than all L
    // keys so FindEmplace routes past L (large) to a brand-new page S.
    // -----------------------------------------------------------------------
    FLCcPage *page_S = nullptr;
    for (int i = 0; i < 5; ++i)
    {
        TestKey ki =
            std::make_tuple(std::string("merge02_s") + std::to_string(i), 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(ki, &emplace, false, false);
        REQUIRE(emplace == true);
        FLCcPage *p = it.GetPage();
        if (i == 0)
        {
            page_S = p;
            REQUIRE(page_S != page_L);
            REQUIRE(page_S->large_obj_page_ == false);
        }
        else
        {
            REQUIRE(p == page_S);
        }
    }
    REQUIRE(page_S->Size() == 5u);
    REQUIRE(cc_map.ccmp_.size() == 2u);

    // -----------------------------------------------------------------------
    // Step 3: Fully commit s0-s3 (IsFree()=true → evicted by CleanPage).
    //         Partially commit s4 (IsFree()==false → survives).
    // -----------------------------------------------------------------------
    for (int i = 0; i < 4; ++i)
    {
        TestKey ki =
            std::make_tuple(std::string("merge02_s") + std::to_string(i), 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(ki, &emplace, false, false);
        REQUIRE(emplace == false);
        REQUIRE(it.GetPage() == page_S);
        FLCcEntry *cce = it->second;

        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cc_map.OnFlushed(cce, was_dirty);
        cc_map.OnCommittedUpdate(cce, was_dirty);
        page_S->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), page_S->last_dirty_commit_ts_);
    }
    // Partial commit s4: commit_ts=100, ckpt_ts=0 → IsFree()==false.
    {
        TestKey k4 = std::make_tuple(std::string("merge02_s4"), 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(k4, &emplace, false, false);
        REQUIRE(emplace == false);
        REQUIRE(it.GetPage() == page_S);
        it->second->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
    }
    // Pre-register s4 as dirty so Terminate() can safely decrement.
    f.shard.AdjustDataKeyStats(tn, 0, 1);

    REQUIRE(page_S->Size() == 5u);
    REQUIRE(page_L->Size() == static_cast<size_t>(L_COUNT));
    CheckLruListIntegrity(f.shard);

    // -----------------------------------------------------------------------
    // Step 4: CleanPageAndReBalance on page S only.
    //   CleanPage: 4 free entries (s0-s3) evicted → S.Size()=1 < 32
    //   RebalancePage: prev=L (large_obj_page_=true)
    //     → borrow_from_prev=false (flag blocks borrow)
    //     → merge_with_prev=false  (same guard)
    //   No next page for S → no borrow_from_next or merge_with_next.
    //   Result: S keeps 1 entry; L unchanged (35 entries, 0 borrowed).
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-MERGE-02] Calling CleanPageAndReBalance on page_S";
    cc_map.CleanPageAndReBalance(page_S, nullptr, nullptr);

    // -----------------------------------------------------------------------
    // Verify: borrow was blocked; both pages survive independently.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-MERGE-02] Verifying post-Clean state";

    // S survived with its one non-free entry.
    REQUIRE(page_S->Size() == 1u);

    // L is completely unchanged — no entries were borrowed.
    REQUIRE(page_L->Size() == static_cast<size_t>(L_COUNT));
    REQUIRE(page_L->large_obj_page_ == true);

    // Both pages still in ccmp_.
    REQUIRE(cc_map.ccmp_.size() == 2u);

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    LOG(INFO) << "[TC-MERGE-02] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-MERGE-03: Two small-object pages CAN merge normally (control group)
// ---------------------------------------------------------------------------
//
// Verifies that the large-object merge guards in RebalancePage do NOT break
// the normal merge path when both pages have large_obj_page_ == false.
//
// Merge guard condition (LO-LRU):
//   merge_with_prev = page->large_obj_page_==false &&
//   prev->large_obj_page_==false merge_with_next = page->large_obj_page_==false
//   && next->large_obj_page_==false
//
// Both conditions evaluate to true when both pages are small, so merge
// proceeds exactly as it would without LO-LRU.
//
// Page layout (ccmp_ sorted by first key):
//   neg_inf <-> A ("merge03_a*") <-> B ("merge03_b*") <-> pos_inf
//
// Setup:
//   A – 5 entries: a0-a3 fully committed (IsFree=true),
//                  a4 partially committed (IsFree=false)
//   B – 5 entries: b0-b4 fully committed (IsFree=true)
//   Both pages: large_obj_page_==false
//
// Since B's keys ("merge03_b*") are greater than A's keys ("merge03_a*"),
// inserting B's keys normally routes into page A (not full, not large).
// To force B onto a separate page, A is temporarily promoted to large before
// B's keys are inserted, then demoted back to small.
//
// After CleanPageAndReBalance(A):
//   CleanPage(A): removes a0-a3 (IsFree=true) → A.Size()=1
//   RebalancePage(A): A.Size()=1 < merge_threshold_(32)
//     prev: neg_inf boundary → cannot merge
//     next: B (large_obj_page_==false)
//       merge_with_next = true (both small)
//       A.Size()+B.Size()=6 ≤ split_threshold_(64) → MERGE B into A
//   A absorbs all 5 entries from B → A.Size()=6; B removed from ccmp_
//
// Acceptance:
//   page_A->Size() == 6   (a4 + b0-b4)
//   cc_map.ccmp_.size() == 1  (B was merged and removed)
//   page_A->large_obj_page_ == false
//   All LRU invariant checks pass
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-MERGE-03: two small-object pages merge normally (control group) under "
    "LO-LRU",
    "[lolru][merge]")
{
    const std::string TABLE = "merge03";
    LoLruFixture f;

    TableName tn(TABLE, TableType::Primary, TableEngine::EloqSql);
    FLCcMap cc_map(&f.shard, 0, tn, 1, nullptr, true);

    // -----------------------------------------------------------------------
    // Step 1a: Insert a0-a4 into page A (5 entries, all on one page since
    // split_threshold_=64).  A is small and holds all five keys.
    // -----------------------------------------------------------------------
    FLCcPage *page_A = nullptr;
    for (int i = 0; i < 5; ++i)
    {
        TestKey ki =
            std::make_tuple(std::string("merge03_a") + std::to_string(i), 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(ki, &emplace, false, false);
        REQUIRE(emplace == true);
        FLCcPage *p = it.GetPage();
        if (i == 0)
        {
            page_A = p;
            REQUIRE(page_A->large_obj_page_ == false);
        }
        else
        {
            REQUIRE(p == page_A);
        }
    }
    REQUIRE(page_A != nullptr);
    REQUIRE(page_A->Size() == 5u);
    REQUIRE(cc_map.ccmp_.size() == 1u);

    // -----------------------------------------------------------------------
    // Step 1b: Temporarily promote A to large so that B's keys don't route
    // into A (large pages are skipped by FindEmplace, forcing a new page).
    // -----------------------------------------------------------------------
    page_A->large_obj_page_ = true;
    f.shard.UpdateLruList(page_A, /*is_emplace=*/false);
    REQUIRE(f.shard.protected_head_page_ == page_A);

    // -----------------------------------------------------------------------
    // Step 1c: Insert b0-b4 → routes past A (large) → new page B.
    // -----------------------------------------------------------------------
    FLCcPage *page_B = nullptr;
    for (int i = 0; i < 5; ++i)
    {
        TestKey ki =
            std::make_tuple(std::string("merge03_b") + std::to_string(i), 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(ki, &emplace, false, false);
        REQUIRE(emplace == true);
        FLCcPage *p = it.GetPage();
        if (i == 0)
        {
            page_B = p;
            REQUIRE(page_B != page_A);
            REQUIRE(page_B->large_obj_page_ == false);
        }
        else
        {
            REQUIRE(p == page_B);
        }
    }
    REQUIRE(page_B->Size() == 5u);
    REQUIRE(cc_map.ccmp_.size() == 2u);

    // -----------------------------------------------------------------------
    // Step 1d: Demote A back to small so that both A and B are small pages.
    // After demotion protected_head_page_ returns to &tail_ccp_ (no large
    // pages).
    // -----------------------------------------------------------------------
    page_A->large_obj_page_ = false;
    f.shard.UpdateLruList(page_A, /*is_emplace=*/false);

    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);
    REQUIRE(page_A->large_obj_page_ == false);
    REQUIRE(page_B->large_obj_page_ == false);

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    // -----------------------------------------------------------------------
    // Step 2: Set commit states.
    //   a0-a3: full commit (IsFree=true → removed by CleanPage)
    //   a4:    partial commit (IsFree=false → survives as the sole A entry)
    //   b0-b4: full commit (IsFree=true, but not cleaned since we only call
    //          CleanPageAndReBalance on A; entries survive into the merged
    //          page)
    // -----------------------------------------------------------------------
    for (int i = 0; i < 4; ++i)
    {
        TestKey ki =
            std::make_tuple(std::string("merge03_a") + std::to_string(i), 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(ki, &emplace, false, false);
        REQUIRE(emplace == false);
        REQUIRE(it.GetPage() == page_A);
        FLCcEntry *cce = it->second;

        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cc_map.OnFlushed(cce, was_dirty);
        cc_map.OnCommittedUpdate(cce, was_dirty);
        page_A->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), page_A->last_dirty_commit_ts_);
    }

    // a4: partial commit → IsFree()==false.
    {
        TestKey k4 = std::make_tuple(std::string("merge03_a4"), 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(k4, &emplace, false, false);
        REQUIRE(emplace == false);
        REQUIRE(it.GetPage() == page_A);
        it->second->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
    }
    // Pre-register a4 as dirty so Terminate() can safely decrement.
    f.shard.AdjustDataKeyStats(tn, 0, 1);

    // b0-b4: full commit → IsFree()=true (not cleaned since we only target A).
    for (int i = 0; i < 5; ++i)
    {
        TestKey ki =
            std::make_tuple(std::string("merge03_b") + std::to_string(i), 0);
        bool emplace = false;
        auto it = cc_map.FindEmplace(ki, &emplace, false, false);
        REQUIRE(emplace == false);
        REQUIRE(it.GetPage() == page_B);
        FLCcEntry *cce = it->second;

        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cc_map.OnFlushed(cce, was_dirty);
        cc_map.OnCommittedUpdate(cce, was_dirty);
        page_B->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), page_B->last_dirty_commit_ts_);
    }

    REQUIRE(page_A->Size() == 5u);
    REQUIRE(page_B->Size() == 5u);
    CheckLruListIntegrity(f.shard);

    // -----------------------------------------------------------------------
    // Step 3: CleanPageAndReBalance on page A.
    //   CleanPage(A): removes a0-a3 (IsFree=true) → A.Size()=1
    //   RebalancePage(A): A.Size()=1 < 32
    //     no prev (boundary) → skip merge_with_prev
    //     next = B (large_obj_page_==false)
    //       merge_with_next = true (both small)
    //       A.Size()+B.Size()=1+5=6 ≤ 64 → MERGE: absorb B into A
    //   B removed from ccmp_ and LRU; A now holds 6 entries.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-MERGE-03] Calling CleanPageAndReBalance on page_A";
    cc_map.CleanPageAndReBalance(page_A, nullptr, nullptr);

    // -----------------------------------------------------------------------
    // Verify: merge occurred; only one page remains with combined entries.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-MERGE-03] Verifying post-merge state";

    // A survived and absorbed all 5 entries from B.
    REQUIRE(page_A->Size() == 6u);  // 1 (a4) + 5 (b0-b4)
    REQUIRE(page_A->large_obj_page_ == false);

    // B was merged into A and removed from ccmp_.
    REQUIRE(cc_map.ccmp_.size() == 1u);

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    LOG(INFO) << "[TC-MERGE-03] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-EVICT-01: Small-object pages are evicted before large-object pages
// ---------------------------------------------------------------------------
//
// Verifies the core LO-LRU eviction ordering invariant:
//
//   "At no point is a large-object page evicted while a small-object page
//    still resides in the LRU chain."
//
// LRU chain layout after setup:
//   [head(LRU)] <-> [s1] <-> [s2] <-> [s3] <-> [l1=protected_head] <-> [l2] <->
//   [l3] <-> [tail(MRU)]
//
// Clean() walks from head toward tail, so small pages (head-side) are
// processed first.  All 6 pages carry IsFree()==true entries (fully
// committed after the full commit dance) so they are genuinely evictable.
//
// Setup:
//   3 small pages:  evict01_s1/s2/s3 – one free entry each,
//   large_obj_page_=false 3 large pages:  evict01_l1/l2/l3 – one free entry
//   each, large_obj_page_=true
//
// After each Clean() call the invariant is checked:
//   if   CollectSmallPages() is non-empty
//   then CollectLargePages().size() == initial_large_count  (all large survive)
//
// After the Clean() loop terminates (freed==0):
//   CollectSmallPages().empty()
//   CollectLargePages().empty()
//   head_ccp_.lru_next_ == &tail_ccp_  (chain is empty)
//   protected_head_page_ == &tail_ccp_
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-EVICT-01: small-object pages evicted before large-object pages under "
    "LO-LRU",
    "[lolru][evict]")
{
    LoLruFixture f;

    constexpr size_t large_payload_size = 2048;  // 2 KB > 1 KB threshold

    // Keep all cc_maps alive until their pages have been evicted so that the
    // shard does not hold dangling pointers; deleted before Terminate().
    std::vector<FLCcMap *> maps;
    maps.reserve(6);

    // -----------------------------------------------------------------------
    // Helper: insert one free-eligible entry into a new table and return the
    // resulting page.  large_obj_page_ == false (small path).
    // -----------------------------------------------------------------------
    auto MakeSmallPage = [&](const std::string &table_name) -> FLCcPage *
    {
        TableName tn(table_name, TableType::Primary, TableEngine::EloqSql);
        auto *cm = new FLCcMap(&f.shard, 0, tn, 1, nullptr, true);
        maps.push_back(cm);

        TestKey key = std::make_tuple(table_name, 0);
        bool emplace = false;
        auto it = cm->FindEmplace(key, &emplace, false, false);
        REQUIRE(emplace == true);

        FLCcEntry *cce = it->second;
        FLCcPage *ccp = it.GetPage();

        // Full commit → IsFree()==true (ckpt_ts=200 >= commit_ts=100).
        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cm->OnFlushed(cce, was_dirty);
        cm->OnCommittedUpdate(cce, was_dirty);
        ccp->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);

        REQUIRE(ccp->large_obj_page_ == false);
        return ccp;
    };

    // -----------------------------------------------------------------------
    // Helper: insert one free-eligible entry, inject a large payload, promote
    // the page via EnsureLargeObjOccupyPageAlone.  large_obj_page_ == true.
    // -----------------------------------------------------------------------
    auto MakeLargePage = [&](const std::string &table_name) -> FLCcPage *
    {
        TableName tn(table_name, TableType::Primary, TableEngine::EloqSql);
        auto *cm = new FLCcMap(&f.shard, 0, tn, 1, nullptr, true);
        maps.push_back(cm);

        TestKey key = std::make_tuple(table_name, 0);
        bool emplace = false;
        auto it = cm->FindEmplace(key, &emplace, false, false);
        REQUIRE(emplace == true);

        FLCcEntry *cce = it->second;
        FLCcPage *ccp = it.GetPage();

        // Full commit → IsFree()==true.
        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cm->OnFlushed(cce, was_dirty);
        cm->OnCommittedUpdate(cce, was_dirty);
        ccp->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);

        // Inject large payload AFTER commit helpers (so they cannot reset it).
        cce->payload_.cur_payload_ =
            std::make_shared<FakeLargeRecord>(large_payload_size);

        // Promote in-place → large_obj_page_ = true, UpdateLruList called.
        cm->EnsureLargeObjOccupyPageAlone(ccp, cce);

        REQUIRE(ccp->large_obj_page_ == true);
        return ccp;
    };

    // -----------------------------------------------------------------------
    // Setup: insert 3 small pages then 3 large pages.
    // Insertion order determines LRU position:
    //   most-recently-inserted page is at the MRU end of its section.
    //
    // Expected chain:
    //   [head] <-> [s1(LRU)] <-> [s2] <-> [s3(MRU-small)]
    //          <-> [l1=protected_head(LRU-large)] <-> [l2] <-> [l3(MRU-large)]
    //          <-> [tail]
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-EVICT-01] Setting up 3 small + 3 large pages";

    MakeSmallPage("evict01_s1");
    MakeSmallPage("evict01_s2");
    MakeSmallPage("evict01_s3");

    FLCcPage *page_l1 = MakeLargePage("evict01_l1");
    MakeLargePage("evict01_l2");
    MakeLargePage("evict01_l3");

    // Sanity checks on initial configuration.
    REQUIRE(f.CollectSmallPages().size() == 3u);
    REQUIRE(f.CollectLargePages().size() == 3u);
    // l1 was promoted first → it is the LRU end of the large section
    // = protected_head_page_.
    REQUIRE(f.shard.protected_head_page_ == page_l1);

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    // -----------------------------------------------------------------------
    // Run Clean() in a loop.  After each pass enforce the eviction-order
    // invariant:
    //
    //   if CollectSmallPages() is non-empty:
    //       CollectLargePages().size() == initial_large_count
    //       (no large page may be evicted while any small page remains)
    // -----------------------------------------------------------------------
    const size_t initial_large_count = 3u;
    size_t total_freed = 0;

    while (true)
    {
        auto [freed, yield] = f.shard.Clean();

        auto smalls_now = f.CollectSmallPages();
        auto larges_now = f.CollectLargePages();

        // Core invariant: large pages only evicted after ALL small pages gone.
        if (!smalls_now.empty())
        {
            REQUIRE(larges_now.size() == initial_large_count);
        }

        CheckLruListIntegrity(f.shard);
        CheckPartitionInvariant(f.shard);
        CheckProtectedHeadInvariant(f.shard);

        if (freed == 0)
        {
            break;
        }
        total_freed += freed;
    }

    LOG(INFO) << "[TC-EVICT-01] total_freed=" << total_freed;

    // -----------------------------------------------------------------------
    // After the loop: all 6 pages (3 small + 3 large) must be evicted since
    // every entry was IsFree()==true.
    // -----------------------------------------------------------------------
    REQUIRE(f.CollectSmallPages().empty());
    REQUIRE(f.CollectLargePages().empty());

    // LRU chain is now empty (only the two dummy sentinels remain).
    REQUIRE(f.shard.head_ccp_.lru_next_ == &f.shard.tail_ccp_);
    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);

    CheckLruListIntegrity(f.shard);

    LOG(INFO) << "[TC-EVICT-01] PASSED";

    for (auto *cm : maps)
    {
        delete cm;
    }
    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-EVICT-02: Eviction order follows LRU position, not object size (control)
// ---------------------------------------------------------------------------
//
// Verifies that without calling EnsureLargeObjOccupyPageAlone both pages stay
// in the small section (large_obj_page_==false) and are evicted strictly in
// LRU insertion order regardless of payload size.
//
// Two pages are created in LO-LRU mode but neither is promoted:
//   P1 – inserted FIRST (occupies LRU end of chain), payload = 2048 B (large)
//   P2 – inserted SECOND (occupies MRU end of chain), payload = 0 B (small)
//
// Because no EnsureLargeObjOccupyPageAlone is called:
//   - Both pages have large_obj_page_ == false.
//   - protected_head_page_ == &tail_ccp_ (large section empty throughout).
//   - Clean() walks from head → tail, so P1 (LRU end) is processed first.
//
// Core invariant checked after every Clean() pass:
//   if P2 still exists, P1 must still exist           (P1 never evicted first)
//   equivalently: p2_present → p1_present
//   (contrapositive: !p1_present → !p2_present)
//
// After the Clean() loop terminates:
//   - Both ccmp_ maps are empty (both pages evicted).
//   - LRU chain contains only the two sentinel nodes.
//   - protected_head_page_ == &tail_ccp_.
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-EVICT-02: eviction order follows LRU position regardless of payload "
    "size under LO-LRU",
    "[lolru][evict]")
{
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    // P1: inserted first (LRU), large payload — NOT promoted to large section.
    TableName tn_p1(
        std::string("evict02_p1"), TableType::Primary, TableEngine::EloqSql);
    FLCcMap cm_p1(&f.shard, 0, tn_p1, 1, nullptr, true);

    // P2: inserted second (MRU), small payload — NOT promoted.
    TableName tn_p2(
        std::string("evict02_p2"), TableType::Primary, TableEngine::EloqSql);
    FLCcMap cm_p2(&f.shard, 0, tn_p2, 1, nullptr, true);

    constexpr size_t large_payload_size =
        2048;  // > 1 KB threshold, but no promotion.

    // -----------------------------------------------------------------------
    // Insert P1 first: large payload, full commit (IsFree=true), no promotion.
    // -----------------------------------------------------------------------
    FLCcPage *page_p1 = nullptr;
    {
        TestKey key = std::make_tuple(std::string("evict02_p1"), 0);
        bool emplace = false;
        auto it = cm_p1.FindEmplace(key, &emplace, false, false);
        REQUIRE(emplace == true);

        FLCcEntry *cce = it->second;
        FLCcPage *ccp = it.GetPage();

        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cm_p1.OnFlushed(cce, was_dirty);
        cm_p1.OnCommittedUpdate(cce, was_dirty);
        ccp->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
        // Inject large payload AFTER commit helpers.
        cce->payload_.cur_payload_ =
            std::make_shared<FakeLargeRecord>(large_payload_size);

        // Deliberately NO EnsureLargeObjOccupyPageAlone call.
        REQUIRE(ccp->large_obj_page_ == false);
        page_p1 = ccp;
    }

    // -----------------------------------------------------------------------
    // Insert P2 second: small payload (0 B), full commit (IsFree=true).
    // -----------------------------------------------------------------------
    {
        TestKey key = std::make_tuple(std::string("evict02_p2"), 0);
        bool emplace = false;
        auto it = cm_p2.FindEmplace(key, &emplace, false, false);
        REQUIRE(emplace == true);

        FLCcEntry *cce = it->second;
        FLCcPage *ccp = it.GetPage();

        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cm_p2.OnFlushed(cce, was_dirty);
        cm_p2.OnCommittedUpdate(cce, was_dirty);
        ccp->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
        // Small payload: size 0.
        cce->payload_.cur_payload_ = std::make_shared<FakeLargeRecord>(0);

        REQUIRE(ccp->large_obj_page_ == false);
    }

    // -----------------------------------------------------------------------
    // Verify initial state.
    // P1 was inserted first → LRU end: head_ccp_.lru_next_ == page_p1.
    // Both pages in small section; no large section.
    // -----------------------------------------------------------------------
    REQUIRE(f.shard.head_ccp_.lru_next_ == page_p1);
    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);
    REQUIRE(cm_p1.ccmp_.size() == 1u);
    REQUIRE(cm_p2.ccmp_.size() == 1u);

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    // -----------------------------------------------------------------------
    // Run Clean() loop.  After each pass enforce:
    //   P2 still present  →  P1 still present
    //   (P2 must never be evicted before P1)
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-EVICT-02] running Clean() loop";
    size_t total_freed = 0;

    while (true)
    {
        auto [freed, yield] = f.shard.Clean();

        bool p1_present = (cm_p1.ccmp_.size() == 1u);
        bool p2_present = (cm_p2.ccmp_.size() == 1u);

        // Core invariant: P2 cannot vanish before P1.
        REQUIRE((p2_present || !p1_present));

        // protected_head_page_ must stay equal to &tail_ccp_ throughout
        // because neither page was ever promoted.
        REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);

        CheckLruListIntegrity(f.shard);
        CheckPartitionInvariant(f.shard);
        CheckProtectedHeadInvariant(f.shard);

        if (freed == 0)
        {
            break;
        }
        total_freed += freed;
    }

    LOG(INFO) << "[TC-EVICT-02] total_freed=" << total_freed;

    // -----------------------------------------------------------------------
    // After the loop: both pages must be evicted (all entries were IsFree).
    // -----------------------------------------------------------------------
    REQUIRE(cm_p1.ccmp_.empty());
    REQUIRE(cm_p2.ccmp_.empty());

    // LRU chain is now empty (only the two sentinel dummies remain).
    REQUIRE(f.shard.head_ccp_.lru_next_ == &f.shard.tail_ccp_);
    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);

    CheckLruListIntegrity(f.shard);

    LOG(INFO) << "[TC-EVICT-02] PASSED";

    f.local_cc_shards.Terminate();
}

// ---------------------------------------------------------------------------
// TC-EVICT-03: Re-accessing a large-object page extends its residence time
// ---------------------------------------------------------------------------
//
// Verifies that calling UpdateLruList on a large-object page moves it to the
// MRU end of the large section, so it is evicted LAST among large pages.
//
// Setup: promote 3 pages L1, L2, L3 into the large section in that order.
//   Initial large-section order (LRU → MRU):
//     protected_head = L1  <->  L2  <->  L3  <->  tail
//
// Re-access L1 (call UpdateLruList(L1)):
//   L1 moves to MRU end; protected_head advances to L2.
//   New order:  protected_head = L2  <->  L3  <->  L1  <->  tail
//
// Run Clean() until all pages are evicted (all entries are IsFree()==true).
// Track eviction order via per-map ccmp_.size():
//   When L2 disappears: L3 and L1 must still be present.
//   When L3 disappears: L1 must still be present.
//   L1 is evicted last (moved to MRU by the re-access).
//
// After the loop:
//   All three maps' ccmp_ empty; LRU chain contains only sentinels.
// ---------------------------------------------------------------------------
TEST_CASE(
    "TC-EVICT-03: re-accessing a large-object page extends its residence time "
    "under LO-LRU",
    "[lolru][evict]")
{
    LoLruFixture f;  // LO-LRU, 1 KB threshold.

    constexpr size_t large_payload_size = 2048;  // 2 KB > 1 KB threshold.

    // Helper: insert one free-eligible entry, inject large payload, promote
    // in-place.  Returns {FLCcMap*, FLCcPage*}; map is heap-allocated.
    auto MakeLargePage =
        [&](const std::string &table_name) -> std::pair<FLCcMap *, FLCcPage *>
    {
        TableName tn(
            std::string(table_name), TableType::Primary, TableEngine::EloqSql);
        auto *cm = new FLCcMap(&f.shard, 0, tn, 1, nullptr, true);

        TestKey key = std::make_tuple(table_name, 0);
        bool emplace = false;
        auto it = cm->FindEmplace(key, &emplace, false, false);
        REQUIRE(emplace == true);

        FLCcEntry *cce = it->second;
        FLCcPage *ccp = it.GetPage();

        bool was_dirty = cce->IsDirty();
        cce->SetCommitTsPayloadStatus(100, RecordStatus::Normal);
        cce->SetCkptTs(200);
        cm->OnFlushed(cce, was_dirty);
        cm->OnCommittedUpdate(cce, was_dirty);
        ccp->last_dirty_commit_ts_ =
            std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
        cce->payload_.cur_payload_ =
            std::make_shared<FakeLargeRecord>(large_payload_size);

        cm->EnsureLargeObjOccupyPageAlone(ccp, cce);
        REQUIRE(ccp->large_obj_page_ == true);
        return {cm, ccp};
    };

    // -----------------------------------------------------------------------
    // Setup: promote L1, L2, L3 in order.
    // Initial large-section order: protected_head=L1 <-> L2 <-> L3 <-> tail
    // -----------------------------------------------------------------------
    auto [cm_l1, page_l1] = MakeLargePage("evict03_l1");
    auto [cm_l2, page_l2] = MakeLargePage("evict03_l2");
    auto [cm_l3, page_l3] = MakeLargePage("evict03_l3");

    // Verify initial ordering.
    REQUIRE(f.shard.protected_head_page_ == page_l1);
    REQUIRE(page_l1->lru_next_ == page_l2);
    REQUIRE(page_l2->lru_next_ == page_l3);
    REQUIRE(f.shard.tail_ccp_.lru_prev_ == page_l3);

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    // -----------------------------------------------------------------------
    // Re-access L1: moves from LRU end to MRU end of the large section.
    // Expected new order: protected_head=L2 <-> L3 <-> L1 <-> tail
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-EVICT-03] re-accessing L1 (LRU → MRU)";
    f.shard.UpdateLruList(page_l1, /*is_emplace=*/false);

    REQUIRE(f.shard.protected_head_page_ == page_l2);
    REQUIRE(f.shard.protected_head_page_->lru_next_ == page_l3);
    REQUIRE(page_l3->lru_next_ == page_l1);
    REQUIRE(f.shard.tail_ccp_.lru_prev_ == page_l1);

    CheckLruListIntegrity(f.shard);
    CheckPartitionInvariant(f.shard);
    CheckProtectedHeadInvariant(f.shard);

    // -----------------------------------------------------------------------
    // Clean() loop.  Track eviction order via ccmp_.size():
    //   When L1 is first observed as empty, both L2 and L3 must already be
    //   empty — L1 (the re-accessed page, now at MRU) is evicted last.
    // -----------------------------------------------------------------------
    LOG(INFO) << "[TC-EVICT-03] running Clean() loop";
    bool l1_evicted_seen = false;
    size_t total_freed = 0;

    while (true)
    {
        auto [freed, yield] = f.shard.Clean();

        // Detect first eviction of L1 (the re-accessed, MRU page).
        if (!l1_evicted_seen && cm_l1->ccmp_.empty())
        {
            l1_evicted_seen = true;
            // L2 and L3 must already be gone before L1.
            REQUIRE(cm_l2->ccmp_.empty());
            REQUIRE(cm_l3->ccmp_.empty());
        }

        CheckLruListIntegrity(f.shard);
        CheckPartitionInvariant(f.shard);
        CheckProtectedHeadInvariant(f.shard);

        if (freed == 0)
        {
            break;
        }
        total_freed += freed;
    }

    LOG(INFO) << "[TC-EVICT-03] total_freed=" << total_freed;

    // -----------------------------------------------------------------------
    // All 3 large pages evicted; L1 was confirmed to be evicted last.
    // -----------------------------------------------------------------------
    REQUIRE(l1_evicted_seen);
    REQUIRE(cm_l2->ccmp_.empty());
    REQUIRE(cm_l3->ccmp_.empty());

    // LRU chain is now empty.
    REQUIRE(f.shard.head_ccp_.lru_next_ == &f.shard.tail_ccp_);
    REQUIRE(f.shard.protected_head_page_ == &f.shard.tail_ccp_);

    CheckLruListIntegrity(f.shard);

    LOG(INFO) << "[TC-EVICT-03] PASSED";

    delete cm_l1;
    delete cm_l2;
    delete cm_l3;
    f.local_cc_shards.Terminate();
}

}  // namespace txservice

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    int ret = Catch::Session().run(argc, argv);
    return ret;
}
