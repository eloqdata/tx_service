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
// Let Catch provide main():
#include <catch2/catch_all.hpp>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "cc/cc_req_base.h"
#include "cc/non_blocking_lock.h"
#include "cc_entry.h"
#include "cc_protocol.h"
#include "cc_shard.h"
#include "include/mock/mock_catalog_factory.h"
#include "local_cc_shards.h"
#include "template_cc_map.h"
#include "tx_key.h"     // CompositeKey
#include "tx_record.h"  // CompositeRecord
#include "type.h"

/**
 * @file
 * @brief Which write locks hold the checkpoint back (CcShard::ActiveTxMinTs /
 * ActiveTxMaxTs), and for how long.
 *
 * Write locks on data tables and catalog entries count, until release or
 * downgrade to a write intent. Range, bucket and cluster-config write locks,
 * intents and read locks never count, and no later registration of another
 * lock changes either. While a tx counts, the bound is its latest write-lock
 * grant in the shard, on any table: the tx commits above every grant.
 */

namespace txservice
{
namespace
{
constexpr NodeGroupId kNg = 0;
constexpr int64_t kTerm = 1;
/** @brief Marks an ActiveTxMinTs out-parameter the call did not set. */
constexpr TxNumber kNoTxn = UINT64_MAX;

MockCatalogFactory mock_catalog_factory{};

using TestKey = CompositeKey<std::string, int>;
using TestRec = CompositeRecord<int>;
using TestBaseMap = TemplateCcMap<TestKey, TestRec, true, true>;

/** @brief Every (tx, entry) lock the test has taken and not yet released. */
std::vector<std::pair<TxNumber, LruEntry *>> &Held()
{
    static std::vector<std::pair<TxNumber, LruEntry *>> held;
    return held;
}

/**
 * @brief Minimal CcRequestBase carrying a tx number. NonBlockingLock only
 * reads Txn() off the requests it is given, so Execute() is never reached.
 */
struct StubCcRequest : public CcRequestBase
{
    explicit StubCcRequest(TxNumber txn)
    {
        tx_number_ = txn;
        proto_ = CcProtocol::Locking;
        isolation_level_ = IsolationLevel::RepeatableRead;
    }

    bool Execute(CcShard &) override
    {
        return false;
    }
};

/**
 * @brief A cc map of a chosen table type whose entries the test locks
 * directly, registering each lock the way CcMap::AcquireCceKeyLock does.
 */
class RegistryTestMap : public TestBaseMap
{
public:
    RegistryTestMap(CcShard *shard, TableType type)
        : TestBaseMap(shard,
                      kNg,
                      TableName(std::string("b1_") +
                                    std::to_string(static_cast<int>(type)),
                                type,
                                TableEngine::EloqSql),
                      1,
                      nullptr,
                      true)
    {
    }

    /**
     * @brief Takes a lock on a key of this map and registers it in the
     * shard's lock registry.
     *
     * @param k The key's integer part.
     * @param txn The tx taking the lock.
     * @param type The lock type: WriteLock, WriteIntent or ReadLock.
     * @return The locked entry and the tx's registry entry.
     */
    std::pair<LruEntry *, TxLockInfo *> Lock(int k, TxNumber txn, LockType type)
    {
        TestKey key(std::make_tuple(std::string("k"), k));
        auto it = FindEmplace(key);
        auto *cce = it->second;
        NonBlockingLock &lock =
            cce->GetOrCreateKeyLock(shard_, this, it.GetPage());
        StubCcRequest req{txn};
        // The entry point CcMap::AcquireCceKeyLock uses for every lock type.
        REQUIRE(lock.AcquireLock(&req, CcProtocol::Locking, type) ==
                LockOpStatus::Successful);
        Held().emplace_back(txn, cce);
        TxLockInfo *info =
            shard_->UpsertLockHoldingTx(txn,
                                        kTerm,
                                        cce,
                                        type == LockType::WriteLock,
                                        cc_ng_id_,
                                        table_name_.Type());
        return {cce, info};
    }

    using CcMap::DowngradeCceKeyWriteLock;
};

/**
 * @brief One shard with a map per table type, built on first use and leaked,
 * so it outlives every test case the way a TxProcessor's shard does.
 */
struct Fixture
{
    Fixture()
        : ng_configs{{kNg, {NodeConfig(0, "127.0.0.1", 8600)}}},
          tx_cnf{
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
          },
          catalog_factory{&mock_catalog_factory,
                          &mock_catalog_factory,
                          &mock_catalog_factory,
                          &mock_catalog_factory,
                          &mock_catalog_factory},
          local_cc_shards(0,
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
        // As TxProcessor::Run does before touching its shard.
        local_cc_shards.BindThreadToFastMetaDataShard(0);
        shard.Init();

        for (TableType type : {TableType::Primary,
                               TableType::Secondary,
                               TableType::Catalog,
                               TableType::RangePartition,
                               TableType::RangeBucket,
                               TableType::ClusterConfig})
        {
            maps.emplace(type, std::make_unique<RegistryTestMap>(&shard, type));
        }
    }

    RegistryTestMap &Map(TableType type)
    {
        return *maps.at(type);
    }

    std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs;
    std::map<std::string, uint32_t> tx_cnf;
    CatalogFactory *catalog_factory[5];
    LocalCcShards local_cc_shards;
    CcShard shard;
    std::map<TableType, std::unique_ptr<RegistryTestMap>> maps;
};

Fixture &F()
{
    static Fixture *fixture = new Fixture();
    return *fixture;
}

/**
 * @brief Releases every lock the test took for a tx and drops its registry
 * entry.
 *
 * Does what CcShard::ClearTx does, minus recycling the lock objects: that
 * stamps them from Sharder's LocalCcShards, which only Sharder::Init sets.
 *
 * @param txn The tx whose locks to release.
 */
void Release(TxNumber txn)
{
    auto &held = Held();
    for (auto it = held.begin(); it != held.end();)
    {
        if (it->first != txn)
        {
            ++it;
            continue;
        }
        it->second->GetKeyLock()->ClearTx(txn, &F().shard);
        F().shard.DeleteLockHoldingTx(txn, it->second, kNg);
        it = held.erase(it);
    }
}

/**
 * @brief Releases a tx's locks when a test case exits, including when a
 * failed REQUIRE throws, so one failing case cannot pin the checkpoint for
 * the cases after it.
 */
struct ReleaseOnExit
{
    ~ReleaseOnExit()
    {
        Release(txn);
    }

    TxNumber txn;  ///< The tx whose locks to release.
};

/**
 * @brief Moves the shard clock forward.
 *
 * @return The new shard time.
 */
uint64_t Tick()
{
    uint64_t t = F().shard.Now() + 1000;
    F().shard.UpdateTsBase(t);
    return t;
}

/** @brief One ActiveTxMinTs round and what it reported. */
struct MinTs
{
    uint64_t ts{0};
    TxNumber pinning_txn{kNoTxn};
    uint64_t pinning_wlock_ts{UINT64_MAX};
};

MinTs ActiveMin()
{
    MinTs r;
    r.ts = F().shard.ActiveTxMinTs(kNg, &r.pinning_txn, &r.pinning_wlock_ts);
    return r;
}

/** @brief Requires that no lock of the tx holds the checkpoint back. */
void RequireNotHoldingBack()
{
    MinTs r = ActiveMin();
    REQUIRE(r.pinning_txn == kNoTxn);
    REQUIRE(F().shard.ActiveTxMaxTs(kNg) == 0);
}

/**
 * @brief Requires that a tx holds the checkpoint below its latest write-lock
 * grant.
 *
 * @param txn The tx expected to pin the checkpoint.
 * @param lock_ts When it last acquired a write lock in the shard.
 */
void RequireHoldingBack(TxNumber txn, uint64_t lock_ts)
{
    MinTs r = ActiveMin();
    REQUIRE(r.pinning_txn == txn);
    REQUIRE(r.pinning_wlock_ts == lock_ts);
    REQUIRE(r.ts == lock_ts - 1);
    REQUIRE(r.ts < lock_ts);
    REQUIRE(F().shard.ActiveTxMaxTs(kNg) == lock_ts);
}

}  // namespace

TEST_CASE("a data write lock holds the checkpoint below its acquisition",
          "[ckpt-lock-registry]")
{
    constexpr TxNumber kTxn = 101;
    const ReleaseOnExit release_on_exit{kTxn};
    for (TableType type : {TableType::Primary, TableType::Secondary})
    {
        uint64_t t = Tick();
        F().Map(type).Lock(1, kTxn, LockType::WriteLock);
        RequireHoldingBack(kTxn, t);

        Release(kTxn);
        RequireNotHoldingBack();
    }
}

TEST_CASE("each write-lock grant moves the bound up, reads and intents do not",
          "[ckpt-lock-registry]")
{
    // The tx chooses its commit timestamp after its last write lock, so the
    // latest grant bounds the commit, and the checkpoint may advance to it.
    constexpr TxNumber kTxn = 151;
    const ReleaseOnExit release_on_exit{kTxn};
    RegistryTestMap &data = F().Map(TableType::Primary);

    uint64_t t1 = Tick();
    data.Lock(1, kTxn, LockType::WriteLock);
    RequireHoldingBack(kTxn, t1);

    Tick();
    data.Lock(2, kTxn, LockType::ReadLock);
    Tick();
    data.Lock(3, kTxn, LockType::WriteIntent);
    RequireHoldingBack(kTxn, t1);

    uint64_t t4 = Tick();
    data.Lock(4, kTxn, LockType::WriteLock);
    RequireHoldingBack(kTxn, t4);

    Release(kTxn);
    RequireNotHoldingBack();
}

TEST_CASE("a catalog write lock holds the checkpoint below its acquisition",
          "[ckpt-lock-registry]")
{
    // What EloqDoc's change data capture relies on: a checkpoint computed
    // while a DDL holds its catalog write lock stays below the lock time,
    // so the DDL's log record lands above the log group's checkpoint.
    constexpr TxNumber kTxn = 201;
    const ReleaseOnExit release_on_exit{kTxn};
    uint64_t t = Tick();
    F().Map(TableType::Catalog).Lock(1, kTxn, LockType::WriteLock);
    RequireHoldingBack(kTxn, t);

    Release(kTxn);
    RequireNotHoldingBack();
}

TEST_CASE("range, bucket and cluster-config write locks do not hold it back",
          "[ckpt-lock-registry]")
{
    constexpr TxNumber kTxn = 301;
    const ReleaseOnExit release_on_exit{kTxn};
    for (TableType type : {TableType::RangePartition,
                           TableType::RangeBucket,
                           TableType::ClusterConfig})
    {
        Tick();
        TxLockInfo *info =
            F().Map(type).Lock(1, kTxn, LockType::WriteLock).second;
        // Still recorded for orphan-lock recovery.
        REQUIRE(info->wlock_ts_ != 0);
        RequireNotHoldingBack();

        Release(kTxn);
    }
}

TEST_CASE("intents and read locks do not hold the checkpoint back",
          "[ckpt-lock-registry]")
{
    constexpr TxNumber kTxn = 401;
    const ReleaseOnExit release_on_exit{kTxn};
    Tick();
    F().Map(TableType::Primary).Lock(1, kTxn, LockType::WriteIntent);
    F().Map(TableType::Primary).Lock(2, kTxn, LockType::ReadLock);
    F().Map(TableType::Catalog).Lock(1, kTxn, LockType::WriteIntent);
    F().Map(TableType::Catalog).Lock(2, kTxn, LockType::ReadLock);
    RequireNotHoldingBack();

    Release(kTxn);
}

TEST_CASE("a later meta lock does not hide a data or catalog write lock",
          "[ckpt-lock-registry]")
{
    // The registry once kept one table type per tx, overwritten by every
    // registration, and the checkpoint read it: a data or catalog write lock
    // followed by any lock on a range, bucket or cluster-config entry dropped
    // out of the checkpoint. A later meta write lock is still a write-lock
    // grant, so it moves the bound up to its own time.
    constexpr TxNumber kTxn = 501;
    const ReleaseOnExit release_on_exit{kTxn};
    for (TableType type : {TableType::Primary, TableType::Catalog})
    {
        uint64_t t = Tick();
        F().Map(type).Lock(1, kTxn, LockType::WriteLock);

        Tick();
        F().Map(TableType::ClusterConfig).Lock(1, kTxn, LockType::ReadLock);
        RequireHoldingBack(kTxn, t);

        uint64_t t_range = Tick();
        F().Map(TableType::RangePartition).Lock(1, kTxn, LockType::WriteLock);
        RequireHoldingBack(kTxn, t_range);

        Release(kTxn);
        RequireNotHoldingBack();
    }
}

TEST_CASE("a meta write lock alone does not count, a later data lock does",
          "[ckpt-lock-registry]")
{
    constexpr TxNumber kTxn = 601;
    const ReleaseOnExit release_on_exit{kTxn};
    uint64_t t_range = Tick();
    F().Map(TableType::RangePartition).Lock(1, kTxn, LockType::WriteLock);
    RequireNotHoldingBack();

    uint64_t t_data = Tick();
    TxLockInfo *info =
        F().Map(TableType::Primary).Lock(1, kTxn, LockType::WriteLock).second;
    REQUIRE(t_range < t_data);
    REQUIRE(info->wlock_ts_ == t_data);
    RequireHoldingBack(kTxn, t_data);

    Release(kTxn);
}

TEST_CASE("a downgraded catalog write lock stops holding the checkpoint back",
          "[ckpt-lock-registry]")
{
    // A DDL's prepare stage downgrades its catalog write lock to a write
    // intent (PostWriteType::PrepareCommit) and its commit stage upgrades it
    // again. The checkpoint is not held while only the intent remains, and
    // is held below the upgrade once it is taken.
    constexpr TxNumber kTxn = 701;
    const ReleaseOnExit release_on_exit{kTxn};
    RegistryTestMap &catalog = F().Map(TableType::Catalog);

    uint64_t t_first = Tick();
    LruEntry *cce = catalog.Lock(1, kTxn, LockType::WriteLock).first;
    RequireHoldingBack(kTxn, t_first);

    Tick();
    catalog.DowngradeCceKeyWriteLock(cce, kTxn);
    REQUIRE(cce->GetKeyLock()->SearchLock(kTxn) == LockType::WriteIntent);
    RequireNotHoldingBack();

    uint64_t t_upgrade = Tick();
    catalog.Lock(1, kTxn, LockType::WriteLock);
    RequireHoldingBack(kTxn, t_upgrade);

    Release(kTxn);
    RequireNotHoldingBack();
}

TEST_CASE("a downgrade keeps the tx counted while a counting lock remains",
          "[ckpt-lock-registry]")
{
    constexpr TxNumber kTxn = 801;
    const ReleaseOnExit release_on_exit{kTxn};
    RegistryTestMap &catalog = F().Map(TableType::Catalog);
    RegistryTestMap &range = F().Map(TableType::RangePartition);

    Tick();
    LruEntry *first = catalog.Lock(1, kTxn, LockType::WriteLock).first;
    Tick();
    LruEntry *second = catalog.Lock(2, kTxn, LockType::WriteLock).first;
    uint64_t t_last = Tick();
    LruEntry *range_entry = range.Lock(1, kTxn, LockType::WriteLock).first;

    // Downgrading a lock that never counted changes nothing.
    range.DowngradeCceKeyWriteLock(range_entry, kTxn);
    RequireHoldingBack(kTxn, t_last);

    // Another counting write lock remains; a downgrade acquires nothing, so
    // the bound stays at the latest grant.
    catalog.DowngradeCceKeyWriteLock(first, kTxn);
    RequireHoldingBack(kTxn, t_last);

    catalog.DowngradeCceKeyWriteLock(second, kTxn);
    RequireNotHoldingBack();

    Release(kTxn);
}

}  // namespace txservice
