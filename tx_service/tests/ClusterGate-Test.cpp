/**
 *    Copyright (C) 2026 EloqData Inc.
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
// Before Catch2, whose CHECK replaces the glog CHECK that brpc headers use.
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/stream.h>
#include <bthread/mutex.h>

// Let Catch provide main():
#include <atomic>
#include <catch2/catch_all.hpp>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

// For Sharder's local shards and candidate terms, which only Sharder::Init
// sets (see Fixture).
#define private public
#include "cc/cc_handler_result.h"
#include "cc/cc_req_base.h"
#include "cc/cc_request.h"
#include "cc/cluster_config_cc_map.h"
#include "cc/non_blocking_lock.h"
#include "cc_shard.h"
#include "cluster_config_record.h"
#include "fault/log_replay_service.h"
#include "include/mock/mock_catalog_factory.h"
#include "local_cc_shards.h"
#include "log.pb.h"
#include "sharder.h"
#undef private

/**
 * @file
 * @brief The cluster gate's carrier and replay (EloqDoc
 * docs/cdc-change-stream.md §6.2; plan D1, D2, D6): the optional gate a
 * ClusterConfigRecord carries through every copy and its serialization, the
 * cluster config cc map's gate-only post write all, which stores the gate only
 * while the request's tx owns the lock and never touches Sharder, and the
 * map's replay of log group 0's gate slot, which installs the value in place.
 */

namespace txservice
{
namespace
{
constexpr NodeGroupId kNg = 0;
constexpr int64_t kTerm = 1;

MockCatalogFactory mock_catalog_factory{};

/**
 * @brief A gate value.
 *
 * @param paused Whether it is closed.
 * @param mode What it rejects.
 * @return The gate.
 */
ClusterGate Gate(bool paused, ClusterGate::Mode mode = ClusterGate::Mode::kDdl)
{
    ClusterGate gate;
    gate.paused = paused;
    gate.mode = mode;
    return gate;
}

/**
 * @brief A record round-tripped through Serialize and Deserialize, as a
 * remote post write all carries it.
 *
 * @param rec The record.
 * @return The decoded record.
 */
std::unique_ptr<ClusterConfigRecord> RoundTrip(const ClusterConfigRecord &rec)
{
    std::string str;
    rec.Serialize(str);
    REQUIRE(str.size() == rec.SerializedLength());
    auto decoded = std::make_unique<ClusterConfigRecord>();
    size_t offset = 0;
    decoded->Deserialize(str.data(), offset);
    REQUIRE(offset == str.size());
    return decoded;
}

TEST_CASE("a record without a gate round-trips without one", "[ClusterGate]")
{
    ClusterConfigRecord rec;
    rec.SetVersion(42);
    auto decoded = RoundTrip(rec);
    CHECK(decoded->Version() == 42);
    CHECK_FALSE(decoded->GetClusterGate().has_value());
    CHECK_FALSE(decoded->GatesSchemaOps());
}

TEST_CASE("a gate round-trips, a resume as a present value", "[ClusterGate]")
{
    for (const ClusterGate &gate :
         {Gate(true), Gate(false), Gate(true, ClusterGate::Mode::kDdlDml)})
    {
        ClusterConfigRecord rec;
        rec.SetVersion(7);
        rec.SetClusterGate(gate);
        auto decoded = RoundTrip(rec);
        CHECK(decoded->Version() == 7);
        REQUIRE(decoded->GetClusterGate().has_value());
        CHECK(*decoded->GetClusterGate() == gate);
        CHECK(decoded->GatesSchemaOps() == gate.paused);
    }
}

TEST_CASE("every copy carries the gate", "[ClusterGate]")
{
    ClusterConfigRecord rec;
    rec.SetVersion(9);
    rec.SetClusterGate(Gate(true));

    ClusterConfigRecord constructed(rec);
    CHECK(*constructed.GetClusterGate() == Gate(true));

    ClusterConfigRecord assigned;
    assigned = rec;
    CHECK(*assigned.GetClusterGate() == Gate(true));

    TxRecord::Uptr cloned = rec.Clone();
    CHECK(*static_cast<ClusterConfigRecord &>(*cloned).GetClusterGate() ==
          Gate(true));

    ClusterConfigRecord copied;
    copied.Copy(rec);
    CHECK(*copied.GetClusterGate() == Gate(true));

    copied.Reset();
    CHECK_FALSE(copied.GetClusterGate().has_value());
    CHECK(copied.Version() == 0);
}

TEST_CASE("a decoded gate record, which owns no configs, copies safely",
          "[ClusterGate]")
{
    // Deserialize makes a record the owner of its configs even when it has
    // none, which every gate record is; copying one used to dereference null.
    ClusterConfigRecord rec;
    rec.SetClusterGate(Gate(false));
    auto decoded = RoundTrip(rec);

    ClusterConfigRecord constructed(*decoded);
    ClusterConfigRecord assigned;
    assigned = *decoded;
    ClusterConfigRecord copied;
    copied.Copy(*decoded);
    for (const ClusterConfigRecord *copy : {&constructed, &assigned, &copied})
    {
        REQUIRE(copy->GetClusterGate().has_value());
        CHECK(*copy->GetClusterGate() == Gate(false));
    }
}

/**
 * @brief Minimal CcRequestBase carrying a tx number, for taking the map's
 * write lock directly. NonBlockingLock only reads Txn() off it.
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
 * @brief The cluster config cc map, with a way to take its write lock as a
 * gate op's acquire-all does.
 */
class GateTestMap : public ClusterConfigCcMap
{
public:
    explicit GateTestMap(CcShard *shard) : ClusterConfigCcMap(shard, kNg, 1)
    {
    }

    /**
     * @brief Takes the entry's write lock for a tx and registers it.
     *
     * @param txn The tx.
     */
    void WriteLock(TxNumber txn)
    {
        NonBlockingLock &lock =
            neg_inf_.GetOrCreateKeyLock(shard_, this, &neg_inf_page_);
        StubCcRequest req{txn};
        REQUIRE(
            lock.AcquireLock(&req, CcProtocol::Locking, LockType::WriteLock) ==
            LockOpStatus::Successful);
        shard_->UpsertLockHoldingTx(
            txn, kTerm, &neg_inf_, true, cc_ng_id_, TableType::ClusterConfig);
    }

    /**
     * @brief Which tx holds the entry's write lock.
     *
     * @return The tx, or 0 when none does.
     */
    TxNumber WriteLockHolder()
    {
        NonBlockingLock *lock = neg_inf_.GetKeyLock();
        if (lock == nullptr)
        {
            return 0;
        }
        auto [txn, type] = lock->WriteTx();
        return type == NonBlockingLock::WriteLockType::NoWritelock ? 0 : txn;
    }

    /**
     * @brief The gate the entry stores.
     *
     * @return The gate, or none.
     */
    std::optional<ClusterGate> StoredGate()
    {
        return neg_inf_.payload_.cur_payload_->GetClusterGate();
    }

    /**
     * @brief The record a schema op's cluster config read gets: a copy of
     * the entry's payload, as Execute(ReadCc&) makes it.
     *
     * @return The copy.
     */
    ClusterConfigRecord ReadCopy()
    {
        ClusterConfigRecord rec;
        rec = *neg_inf_.payload_.cur_payload_;
        return rec;
    }

    /**
     * @brief The entry's commit timestamp.
     *
     * @return The timestamp.
     */
    uint64_t EntryCommitTs()
    {
        return neg_inf_.CommitTs();
    }

    /**
     * @brief Sets the entry's commit timestamp, as a topology change leaves
     * it.
     *
     * @param ts The timestamp.
     */
    void SetEntryCommitTs(uint64_t ts)
    {
        neg_inf_.SetCommitTsPayloadStatus(ts, RecordStatus::Normal);
    }

    /**
     * @brief Caches node group configs in the payload, as cluster scale's
     * replay does for its asynchronous Sharder update.
     *
     * @param ng_cnt How many node groups the configs name.
     */
    void CacheConfigs(uint32_t ng_cnt)
    {
        auto configs = std::make_unique<
            std::unordered_map<NodeGroupId, std::vector<NodeConfig>>>();
        for (uint32_t ng = 0; ng < ng_cnt; ++ng)
        {
            configs->try_emplace(
                ng, std::vector<NodeConfig>{NodeConfig(ng, "127.0.0.1", 8600)});
        }
        neg_inf_.payload_.cur_payload_->SetNodeGroupConfigs(std::move(configs));
    }

    /**
     * @brief How many node groups the payload's cached configs name.
     *
     * @return The count.
     */
    size_t CachedConfigCount()
    {
        return neg_inf_.payload_.cur_payload_->GetNodeGroupConfigs().size();
    }
};

/**
 * @brief One shard and the map, built on first use and leaked, so they
 * outlive every test case the way a TxProcessor's shard does.
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
        // Releasing a lock recycles its object, which stamps it with the time
        // from Sharder's LocalCcShards. Only Sharder::Init sets that, and it
        // also starts the node's streams and host manager.
        Sharder::Instance().local_shards_ = &local_cc_shards;
        // A replay runs while its node group is a candidate leader. The
        // terms are read only once the node's cc nodes are up.
        Sharder::Instance().candidate_leader_term_cache_[kNg].store(kTerm);
        Sharder::Instance().cc_nodes_init_.store(true);
        map = std::make_unique<GateTestMap>(&shard);
    }

    std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs;
    std::map<std::string, uint32_t> tx_cnf;
    CatalogFactory *catalog_factory[5];
    LocalCcShards local_cc_shards;
    CcShard shard;
    std::unique_ptr<GateTestMap> map;
};

Fixture &F()
{
    static Fixture *fixture = new Fixture();
    return *fixture;
}

/**
 * @brief Runs a gate op's commit post write all on the map, from the same
 * node, as stage (4) of a ClusterGateOp sends it.
 *
 * @param txn The gate tx.
 * @param commit_ts Its commit timestamp, the gate's version.
 * @param gate The value to install.
 * @return Whether the request finished.
 */
bool PostWriteGate(TxNumber txn, uint64_t commit_ts, const ClusterGate &gate)
{
    ClusterConfigRecord rec;
    rec.SetClusterGate(gate);
    CcHandlerResult<PostProcessResult> result(nullptr);
    PostWriteAllCc req;
    req.Reset(&cluster_config_ccm_name,
              VoidKey::NegInfTxKey(),
              kNg,
              txn,
              commit_ts,
              &rec,
              OperationType::Update,
              &result,
              PostWriteType::Commit,
              kTerm,
              false);
    REQUIRE(F().map->Execute(req));
    return result.IsFinished() && !result.IsError();
}

TEST_CASE(
    "a gate's post write all stores the gate and releases the lock, without "
    "Sharder",
    "[ClusterGate]")
{
    const uint64_t topology_version =
        Sharder::Instance().ClusterConfigVersion();
    const TxNumber txn = 101;
    F().map->WriteLock(txn);
    REQUIRE(F().map->WriteLockHolder() == txn);

    // Above the topology version, where a record without a gate would update
    // Sharder from node group configs this record does not carry.
    REQUIRE(PostWriteGate(txn, topology_version + 1000, Gate(true)));
    CHECK(Sharder::Instance().ClusterConfigVersion() == topology_version);
    CHECK(F().map->WriteLockHolder() == 0);
    REQUIRE(F().map->StoredGate().has_value());
    CHECK(*F().map->StoredGate() == Gate(true));
    // What the next schema op's cluster config read lock reads.
    CHECK(F().map->ReadCopy().GatesSchemaOps());

    // A resume clears it the same way.
    F().map->WriteLock(txn + 1);
    REQUIRE(PostWriteGate(txn + 1, topology_version + 2000, Gate(false)));
    CHECK(*F().map->StoredGate() == Gate(false));
    CHECK_FALSE(F().map->ReadCopy().GatesSchemaOps());
}

TEST_CASE("a post write all whose tx no longer holds the lock stores nothing",
          "[ClusterGate]")
{
    const uint64_t base = Sharder::Instance().ClusterConfigVersion() + 10000;

    // A newer pause takes the lock and installs its value.
    const TxNumber stale_resume = 201;
    const TxNumber newer_pause = 202;
    F().map->WriteLock(newer_pause);
    REQUIRE(PostWriteGate(newer_pause, base + 2, Gate(true)));
    REQUIRE(*F().map->StoredGate() == Gate(true));

    // An older resume's post write all, retried after its tx completed
    // elsewhere, finds the lock free: it must not reopen the gate.
    REQUIRE(PostWriteGate(stale_resume, base + 1, Gate(false)));
    CHECK(*F().map->StoredGate() == Gate(true));

    // Nor while a third gate tx holds the lock.
    const TxNumber holder = 203;
    F().map->WriteLock(holder);
    REQUIRE(PostWriteGate(stale_resume, base + 1, Gate(false)));
    CHECK(*F().map->StoredGate() == Gate(true));
    CHECK(F().map->WriteLockHolder() == holder);
    REQUIRE(PostWriteGate(holder, base + 3, Gate(true)));
    CHECK(F().map->WriteLockHolder() == 0);
}
/**
 * @brief A gate tx coordinated by another node group, whose replay here only
 * installs the value: the coordinator's replay also starts a recovery tx,
 * which needs a running TxService.
 *
 * @param seq The tx's sequence number within its node group.
 * @return The tx number.
 */
TxNumber RemoteGateTxn(uint32_t seq)
{
    return (static_cast<TxNumber>(kNg + 1) << 42) | seq;
}

/**
 * @brief Replays a cluster gate slot into the map, as RecoveryService does
 * for the item log group 0 ships, and waits for it.
 *
 * @param txn The gate tx that set the slot.
 * @param commit_ts The slot's version.
 * @param blob The slot's value, a serialized ClusterGateLogMessage.
 * @return false if the replay reported a recovery error.
 */
bool ReplayGate(TxNumber txn, uint64_t commit_ts, const std::string &blob)
{
    bthread::Mutex mux;
    std::atomic<fault::RecoveryService::WaitingStatus> status{
        fault::RecoveryService::WaitingStatus::Active};
    std::atomic<size_t> on_fly_cnt{1};
    bool recovery_error = false;
    ReplayLogCc req;
    req.Reset(kNg,
              kTerm,
              cluster_config_ccm_name_sv,
              TableType::ClusterConfig,
              TableEngine::None,
              blob,
              commit_ts,
              txn,
              mux,
              status,
              on_fly_cnt,
              recovery_error);
    req.SetClusterGate();
    F().map->Execute(req);
    CHECK(on_fly_cnt.load() == 0);
    return !recovery_error;
}

/**
 * @brief A slot's value as the log service ships it.
 *
 * @param paused Whether the gate is paused.
 * @return The serialized ClusterGateLogMessage.
 */
std::string SlotBlob(bool paused)
{
    ::txlog::ClusterGateLogMessage msg;
    msg.set_mode(::txlog::ClusterGateLogMessage_GateMode_GateDdl);
    msg.set_paused(paused);
    std::string blob;
    msg.SerializeToString(&blob);
    return blob;
}

TEST_CASE(
    "a replayed slot installs its gate in place, keeping the configs scale's "
    "replay cached and the larger commit timestamp",
    "[ClusterGate]")
{
    const uint64_t base = F().map->EntryCommitTs() + 100000;
    // What cluster scale's replay, which runs first, leaves behind.
    F().map->CacheConfigs(2);
    F().map->SetEntryCommitTs(base + 100);

    // A pause logged before that topology change.
    REQUIRE(ReplayGate(RemoteGateTxn(301), base + 50, SlotBlob(true)));
    REQUIRE(F().map->StoredGate().has_value());
    CHECK(*F().map->StoredGate() == Gate(true));
    CHECK(F().map->ReadCopy().GatesSchemaOps());
    CHECK(F().map->CachedConfigCount() == 2);
    CHECK(F().map->EntryCommitTs() == base + 100);

    // A resume logged after it raises the commit timestamp.
    REQUIRE(ReplayGate(RemoteGateTxn(302), base + 200, SlotBlob(false)));
    CHECK(*F().map->StoredGate() == Gate(false));
    CHECK_FALSE(F().map->ReadCopy().GatesSchemaOps());
    CHECK(F().map->CachedConfigCount() == 2);
    CHECK(F().map->EntryCommitTs() == base + 200);

    // Replay takes no lock: the next gate op or schema op finds it free.
    CHECK(F().map->WriteLockHolder() == 0);
}

TEST_CASE("a corrupt slot fails the replay rather than install nothing",
          "[ClusterGate]")
{
    const std::optional<ClusterGate> before = F().map->StoredGate();
    // A length-delimited field whose length runs past the end.
    CHECK_FALSE(ReplayGate(RemoteGateTxn(303),
                           F().map->EntryCommitTs() + 1,
                           std::string("\x12\x05"
                                       "ab")));
    CHECK(F().map->StoredGate() == before);
}
}  // namespace
}  // namespace txservice
