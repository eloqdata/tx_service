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

/**
 * @file
 * @brief The log state's side of the cluster gate (EloqDoc
 * docs/cdc-change-stream.md §6.2; plan D5): the gate slot, which keeps the
 * latest entry, is never purged, survives a snapshot and is replayed as one
 * item.
 */

#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "log.pb.h"
#include "log_state.h"

// butil's logging, which the log state pulls in, defines a one-argument CHECK
// that would replace Catch's; nothing here uses butil's. Catch comes last so
// that its definition is the one in force.
#undef CHECK
#include <catch2/catch_all.hpp>

using namespace txlog;

namespace
{
class TestLogState : public LogState
{
public:
    using LogState::CleanSnapshotState;
    using LogState::GetClusterGateList;
    using LogState::GetClusterScaleOpList;
    using LogState::LoadClusterGateFrom;
    using LogState::MakeCopyOfNgInfoAndCatalogOps;
    using LogState::WriteSnapshotClusterGateTo;

    void AddLogItem(uint32_t, uint64_t, uint64_t, const std::string &) override
    {
    }
    std::pair<bool, std::unique_ptr<ItemIterator>> GetLogReplayList(
        uint32_t, uint64_t) override
    {
        return {false, nullptr};
    }
    std::pair<bool, Item::Pointer> SearchTxDataLog(uint64_t,
                                                   uint32_t,
                                                   uint64_t) override
    {
        return {false, nullptr};
    }
    void BeginSnapshot() override
    {
    }
    int ReadSnapshot(const std::string &,
                     const std::vector<std::string> &) override
    {
        return 0;
    }
    std::vector<std::string> WriteSnapshot(const std::string &) override
    {
        return {};
    }
};

/**
 * @brief A gate log entry's value.
 *
 * @param paused Whether the gate is closed.
 * @return The value.
 */
ClusterGateLogMessage Gate(bool paused)
{
    ClusterGateLogMessage msg;
    msg.set_mode(ClusterGateLogMessage_GateMode_GateDdl);
    msg.set_paused(paused);
    return msg;
}

/**
 * @brief The slot's value, version and transaction.
 */
struct Slot
{
    bool paused;
    uint64_t version;
    uint64_t txn;

    bool operator==(const Slot &o) const
    {
        return paused == o.paused && version == o.version && txn == o.txn;
    }
};

/**
 * @param state The log state.
 * @return The slot, or none when it is empty.
 */
std::optional<Slot> Read(const LogState &state)
{
    ClusterGateLogMessage msg;
    uint64_t version = 0;
    uint64_t txn = 0;
    if (!state.GetClusterGate(&msg, &version, &txn))
    {
        return std::nullopt;
    }
    return Slot{msg.paused(), version, txn};
}
}  // namespace

TEST_CASE("The gate slot keeps the newest entry", "[log-state][cluster-gate]")
{
    TestLogState state;
    CHECK_FALSE(Read(state).has_value());

    state.UpdateClusterGate(11, 100, Gate(true));
    CHECK(Read(state) == Slot{true, 100, 11});

    // A resume replaces the pause: the slot is overwritten in place.
    state.UpdateClusterGate(12, 200, Gate(false));
    CHECK(Read(state) == Slot{false, 200, 12});

    // An older entry, applied late, does not bring the pause back; nor does
    // one at the same version.
    state.UpdateClusterGate(11, 100, Gate(true));
    state.UpdateClusterGate(13, 200, Gate(true));
    CHECK(Read(state) == Slot{false, 200, 12});

    // A retried write log finds whether its entry was applied.
    CHECK(state.SearchTxClusterGate(12));
    CHECK_FALSE(state.SearchTxClusterGate(11));
}

TEST_CASE("The gate slot is never purged", "[log-state][cluster-gate]")
{
    TestLogState state;
    state.UpdateNgTerm(0, 1, "127.0.0.1", 1);
    state.UpdateClusterGate(21, 1'000, Gate(true));

    // Checkpoints far past the entry, over an hour beyond it, which erase
    // finished schema and cluster scale ops, and a purge's trim, leave a
    // pause in place however old it is.
    const uint64_t two_hours = 2ULL * 3600 * 1'000'000;
    state.UpdateCkptTs(0, two_hours);
    state.UpdateCkptTs(0, 2 * two_hours);
    state.TrimCdcDdlHistory(2 * two_hours);
    CHECK(Read(state) == Slot{true, 1'000, 21});
}

TEST_CASE("The gate slot is one replay item, after cluster scale's",
          "[log-state][cluster-gate]")
{
    TestLogState state;
    std::vector<Item::Pointer> items;
    state.GetClusterGateList(items);
    CHECK(items.empty());

    state.UpdateClusterGate(31, 300, Gate(true));
    state.UpdateClusterGate(32, 400, Gate(false));
    // The replay lists call the cluster scale list first; it asserts it holds
    // at most one item, so the gate's comes after.
    state.GetClusterScaleOpList(items);
    state.GetClusterGateList(items);
    REQUIRE(items.size() == 1);
    const Item &item = *items[0];
    CHECK(item.item_type_ == LogItemType::ClusterGateLog);
    CHECK(item.tx_number_ == 32);
    CHECK(item.timestamp_ == 400);
    ClusterGateLogMessage msg;
    REQUIRE(msg.ParseFromString(item.log_message_));
    CHECK_FALSE(msg.paused());
}

TEST_CASE("The gate slot survives a snapshot", "[log-state][cluster-gate]")
{
    const std::filesystem::path file =
        std::filesystem::temp_directory_path() /
        ("cluster_gate_test_" + std::to_string(::getpid()));

    SECTION("a held pause")
    {
        TestLogState state;
        state.UpdateClusterGate(41, 500, Gate(true));
        state.MakeCopyOfNgInfoAndCatalogOps();
        // Applied after the snapshot began: not in it.
        state.UpdateClusterGate(42, 600, Gate(false));
        {
            std::ofstream os(file, std::ios::binary);
            state.WriteSnapshotClusterGateTo(os);
        }

        TestLogState loaded;
        // Whatever the loading state held is replaced.
        loaded.UpdateClusterGate(99, 9'999, Gate(false));
        {
            std::ifstream is(file, std::ios::binary);
            REQUIRE(loaded.LoadClusterGateFrom(is));
        }
        CHECK(Read(loaded) == Slot{true, 500, 41});

        // A truncated file is refused, and the slot kept as it was.
        std::filesystem::resize_file(file,
                                     std::filesystem::file_size(file) - 1);
        {
            std::ifstream is(file, std::ios::binary);
            CHECK_FALSE(loaded.LoadClusterGateFrom(is));
        }
        CHECK(Read(loaded) == Slot{true, 500, 41});
    }

    SECTION("an empty slot")
    {
        TestLogState state;
        state.MakeCopyOfNgInfoAndCatalogOps();
        {
            std::ofstream os(file, std::ios::binary);
            state.WriteSnapshotClusterGateTo(os);
        }
        TestLogState loaded;
        loaded.UpdateClusterGate(51, 700, Gate(true));
        {
            std::ifstream is(file, std::ios::binary);
            REQUIRE(loaded.LoadClusterGateFrom(is));
        }
        CHECK_FALSE(Read(loaded).has_value());
    }

    SECTION("a value whose length runs past the file's end")
    {
        {
            std::ofstream os(file, std::ios::binary);
            const uint8_t present = 1;
            const uint64_t version = 1;
            const uint64_t txn = 1;
            const uint32_t length = 0xfffffff0;
            os.write(reinterpret_cast<const char *>(&present), sizeof(present));
            os.write(reinterpret_cast<const char *>(&version), sizeof(version));
            os.write(reinterpret_cast<const char *>(&txn), sizeof(txn));
            os.write(reinterpret_cast<const char *>(&length), sizeof(length));
        }
        TestLogState loaded;
        loaded.UpdateClusterGate(61, 800, Gate(true));
        {
            std::ifstream is(file, std::ios::binary);
            CHECK_FALSE(loaded.LoadClusterGateFrom(is));
        }
        CHECK(Read(loaded) == Slot{true, 800, 61});
    }

    std::filesystem::remove(file);
}

TEST_CASE("Clearing the state empties the gate slot",
          "[log-state][cluster-gate]")
{
    TestLogState state;
    state.UpdateClusterGate(71, 900, Gate(true));
    state.MakeCopyOfNgInfoAndCatalogOps();
    state.CleanSnapshotState();
    CHECK(Read(state) == Slot{true, 900, 71});
    state.Clear();
    CHECK_FALSE(Read(state).has_value());
}
