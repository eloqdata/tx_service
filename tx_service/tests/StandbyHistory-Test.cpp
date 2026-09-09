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
#include <catch2/catch_all.hpp>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cc/cc_shard.h"
#include "cc/local_cc_shards.h"
#include "standby.h"

namespace txservice
{
// Inspect the actual owning queue, its lookup index, and its accounting
// together; serialized byte counts alone cannot establish entry reclamation.
class StandbyHistoryTestPeer
{
public:
    static std::vector<uint64_t> Sequences(const CcShard &shard)
    {
        std::vector<uint64_t> sequences;
        for (const auto &entry : shard.history_standby_msg_)
        {
            sequences.push_back(entry->SequenceId());
        }
        return sequences;
    }

    static uint64_t MemoryUsage(const CcShard &shard)
    {
        return shard.total_standby_buffer_memory_usage_;
    }

    static const remote::CcMessage &Message(const CcShard &shard,
                                            uint64_t sequence)
    {
        return shard.seq_id_to_entry_map_.at(sequence)->Message();
    }

    static bool Consistent(const CcShard &shard)
    {
        if (shard.history_standby_msg_.size() !=
            shard.seq_id_to_entry_map_.size())
        {
            return false;
        }
        uint64_t memory_usage = 0;
        for (const auto &entry : shard.history_standby_msg_)
        {
            auto it = shard.seq_id_to_entry_map_.find(entry->SequenceId());
            if (it == shard.seq_id_to_entry_map_.end() ||
                it->second != entry.get())
            {
                return false;
            }
            memory_usage += entry->MemorySize();
        }
        return memory_usage == MemoryUsage(shard);
    }

    static void SetMemoryLimit(CcShard &shard, uint64_t limit)
    {
        shard.standby_buffer_memory_limit_ = limit;
    }

    static std::pair<uint64_t, int64_t> Subscriber(const CcShard &shard,
                                                   uint32_t node_id)
    {
        return shard.subscribed_standby_nodes_.at(node_id);
    }
};

namespace
{
struct HistoryFixture
{
    HistoryFixture()
        : shards(0,
                 0,
                 config,
                 factories,
                 nullptr,
                 &node_groups,
                 1,
                 nullptr,
                 nullptr,
                 false)
    {
    }

    CcShard &Shard()
    {
        return *shards.GetCcShard(0);
    }

    void Forward(size_t count, size_t bytes = 1024 * 1024)
    {
        // There are no subscribers during capture, so this executes the real
        // candidate buffering/cap path without starting any RPC services.
        for (size_t i = 0; i < count; ++i)
        {
            auto entry = std::make_unique<StandbyForwardEntry>();
            entry->Request().set_key("key");
            entry->Request().add_cmd_list(std::string(bytes, 'x'));
            Shard().ForwardStandbyMessage(entry.release());
        }
    }

    const std::map<std::string, uint32_t> config{
        {"core_num", 1},
        {"node_memory_limit_mb", 1024},
        {"range_slice_memory_limit_percent", 1},
        {"realtime_sampling", 0},
        {"range_split_worker_num", 1},
        {"enable_shard_heap_defragment", 0}};
    CatalogFactory *factories[NUM_EXTERNAL_ENGINES]{};
    std::unordered_map<uint32_t, std::vector<NodeConfig>> node_groups{
        {0, {NodeConfig(0, "127.0.0.1", 0)}}};
    LocalCcShards shards;
};

void RequireHistory(CcShard &shard, const std::vector<uint64_t> &expected)
{
    REQUIRE(StandbyHistoryTestPeer::Sequences(shard) == expected);
    REQUIRE(StandbyHistoryTestPeer::Consistent(shard));
    for (uint64_t sequence : expected)
    {
        const auto &request = StandbyHistoryTestPeer::Message(shard, sequence)
                                  .key_obj_standby_forward_req();
        REQUIRE(request.forward_seq_id() == sequence);
        REQUIRE(request.forward_seq_grp() == shard.core_id_);
        REQUIRE(request.cmd_list_size() == 1);
        REQUIRE(request.cmd_list(0) == std::string(1024 * 1024, 'x'));
    }
    if (expected.empty())
    {
        REQUIRE(StandbyHistoryTestPeer::MemoryUsage(shard) == 0);
    }
}
}  // namespace

TEST_CASE("Cancelling the final standby candidate releases its history",
          "[standby-history][memory]")
{
    HistoryFixture fixture;
    auto &shard = fixture.Shard();
    shard.AddCandidateStandby(1, 1);
    fixture.Forward(3);
    RequireHistory(shard, {1, 2, 3});
    REQUIRE(StandbyHistoryTestPeer::MemoryUsage(shard) >= 3 * 1024 * 1024);

    shard.RemoveCandidateStandby(1);
    RequireHistory(shard, {});
    REQUIRE(shard.GetCandidateStandbys().empty());

    // No retry, further write, or unsubscribe is needed to reclaim the data.
    shard.RemoveCandidateStandby(1);
    RequireHistory(shard, {});
}

TEST_CASE("Candidate cancellation retains other consumers' history",
          "[standby-history]")
{
    HistoryFixture fixture;
    auto &shard = fixture.Shard();
    shard.AddCandidateStandby(1, 1);
    shard.AddCandidateStandby(2, 3);
    fixture.Forward(4);

    shard.AddSubscribedStandby(3, 2, 5);
    shard.RemoveCandidateStandby(1);
    RequireHistory(shard, {2, 3, 4});
    shard.RemoveSubscribedStandby(3);
    RequireHistory(shard, {3, 4});
    shard.AddSubscribedStandby(3, 4, 6);
    shard.RemoveCandidateStandby(2);
    RequireHistory(shard, {4});
    REQUIRE(StandbyHistoryTestPeer::Subscriber(shard, 3).first == 3);

    shard.RemoveSubscribedStandby(3);
    RequireHistory(shard, {});
}

TEST_CASE("Candidate promotion protects the subscriber's bootstrap history",
          "[standby-history]")
{
    HistoryFixture fixture;
    auto &shard = fixture.Shard();
    shard.AddCandidateStandby(1, 1);
    fixture.Forward(4);

    shard.PromoteCandidateStandby(1, 2, 5);
    REQUIRE(shard.GetCandidateStandbys().empty());
    REQUIRE(shard.GetSubscribedStandbys() == std::vector<uint32_t>{1});
    RequireHistory(shard, {2, 3, 4});
    REQUIRE(StandbyHistoryTestPeer::Subscriber(shard, 1) ==
            std::make_pair(uint64_t{1}, int64_t{5}));

    // A renewed subscription may advance the watermark, but a stale term
    // must retain the existing subscriber state and its still-needed history.
    shard.AddCandidateStandby(1, 2);
    shard.PromoteCandidateStandby(1, 3, 6);
    RequireHistory(shard, {3, 4});
    shard.AddCandidateStandby(1, 3);
    shard.PromoteCandidateStandby(1, 4, 5);
    RequireHistory(shard, {3, 4});
    REQUIRE(StandbyHistoryTestPeer::Subscriber(shard, 1) ==
            std::make_pair(uint64_t{2}, int64_t{6}));
}

TEST_CASE("History cap eviction collects abandoned candidate messages",
          "[standby-history][memory]")
{
    HistoryFixture fixture;
    auto &shard = fixture.Shard();
    constexpr uint64_t image_size = 1024 * 1024;
    StandbyHistoryTestPeer::SetMemoryLimit(shard, 2 * image_size + 1024);
    shard.AddCandidateStandby(1, 1);
    fixture.Forward(2, image_size);
    RequireHistory(shard, {1, 2});

    fixture.Forward(1, image_size);
    REQUIRE(shard.GetCandidateStandbys().empty());
    RequireHistory(shard, {});
}

TEST_CASE("History cap eviction retains a later candidate's messages",
          "[standby-history]")
{
    HistoryFixture fixture;
    auto &shard = fixture.Shard();
    constexpr uint64_t image_size = 1024 * 1024;
    StandbyHistoryTestPeer::SetMemoryLimit(shard, 2 * image_size + 1024);
    shard.AddCandidateStandby(1, 1);
    shard.AddCandidateStandby(2, 3);
    fixture.Forward(3, image_size);

    REQUIRE(shard.GetCandidateStandbys() == std::vector<uint32_t>{2});
    RequireHistory(shard, {3});
    shard.RemoveCandidateStandby(2);
    RequireHistory(shard, {});
}
}  // namespace txservice
