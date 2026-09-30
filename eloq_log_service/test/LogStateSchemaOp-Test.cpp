/**
 * @file LogStateSchemaOp-Test.cpp
 * @brief The commit timestamp the log state keeps for a schema op, which is
 * the timestamp a node that recovers the op resumes with.
 *
 * CREATE, DROP and TRUNCATE TABLE choose their commit timestamp at the commit
 * stage, above the prepare log's. The commit log's timestamp must therefore
 * replace the prepare log's, or an op recovered from its commit log would
 * resume at the prepare-stage timestamp.
 */
#include <catch2/catch_all.hpp>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "log.pb.h"
#include "log_state.h"

using namespace txlog;

namespace
{
/**
 * @brief A log state that keeps no data log: the schema op bookkeeping under
 * test lives in the LogState base, whatever the log state type.
 */
class TestLogState : public LogState
{
public:
    using LogState::GetSchemaOpList;

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
 * @brief A schema op message for one stage of a CREATE TABLE.
 *
 * @param stage The stage.
 * @return The message.
 */
SchemaOpMessage CreateTableOp(SchemaOpMessage_Stage stage)
{
    SchemaOpMessage msg;
    msg.set_table_name_str("./db/coll");
    msg.set_table_type(CcTableType::Primary);
    msg.set_stage(stage);
    msg.mutable_table_op()->set_op_type(0);
    return msg;
}

/**
 * @brief The commit timestamp a node recovering the op would resume with.
 *
 * @param state The log state.
 * @param txn The op's tx.
 * @return The timestamp, or nothing if the op is not replayed.
 */
std::optional<uint64_t> ReplayedCommitTs(TestLogState &state, uint64_t txn)
{
    std::vector<Item::Pointer> ops;
    state.GetSchemaOpList(ops);
    for (const Item::Pointer &op : ops)
    {
        if (op->tx_number_ == txn)
        {
            return op->timestamp_;
        }
    }
    return std::nullopt;
}
}  // namespace

TEST_CASE("A schema op recovers with its commit log's timestamp",
          "[log-state][schema-op]")
{
    TestLogState state;
    constexpr uint64_t kTxn = 7;
    constexpr uint64_t kPrepareTs = 100;
    constexpr uint64_t kCommitTs = 105;

    state.UpsertSchemaOp(
        kTxn, kPrepareTs, CreateTableOp(SchemaOpMessage_Stage_PrepareSchema));
    REQUIRE(ReplayedCommitTs(state, kTxn) == kPrepareTs);

    // The commit stage re-assigned the timestamp above the prepare log's.
    state.UpsertSchemaOp(
        kTxn, kCommitTs, CreateTableOp(SchemaOpMessage_Stage_CommitSchema));
    REQUIRE(ReplayedCommitTs(state, kTxn) == kCommitTs);

    // A retried prepare log that arrives late changes nothing.
    state.UpsertSchemaOp(
        kTxn, kPrepareTs, CreateTableOp(SchemaOpMessage_Stage_PrepareSchema));
    REQUIRE(ReplayedCommitTs(state, kTxn) == kCommitTs);

    // Once cleaned, the op is no longer replayed.
    state.UpsertSchemaOp(
        kTxn, 0, CreateTableOp(SchemaOpMessage_Stage_CleanSchema));
    REQUIRE_FALSE(ReplayedCommitTs(state, kTxn).has_value());
}

TEST_CASE("A rolled-back add index recovers at its timestamp with its mark",
          "[log-state][schema-op]")
{
    // An add index whose build broke a constraint commits at a real
    // timestamp; recovery tells it from a successful one by the mark.
    TestLogState state;
    constexpr uint64_t kTxn = 9;
    state.UpsertSchemaOp(
        kTxn, 300, CreateTableOp(SchemaOpMessage_Stage_PrepareSchema));
    SchemaOpMessage commit = CreateTableOp(SchemaOpMessage_Stage_CommitSchema);
    commit.set_rolled_back(true);
    state.UpsertSchemaOp(kTxn, 305, commit);

    REQUIRE(ReplayedCommitTs(state, kTxn) == 305);
    std::vector<Item::Pointer> ops;
    state.GetSchemaOpList(ops);
    REQUIRE(ops.size() == 1);
    SchemaOpMessage replayed;
    REQUIRE(replayed.ParseFromString(ops[0]->log_message_));
    REQUIRE(replayed.rolled_back());
    REQUIRE(replayed.stage() == SchemaOpMessage_Stage_CommitSchema);
}

TEST_CASE("A schema op that failed after its prepare log recovers at 0",
          "[log-state][schema-op]")
{
    // A create that gave up on its kv tables, or an ALTER TABLE whose catalog
    // write failed, is rolled back at timestamp 0: no change event, and
    // recovery discards the dirty schema.
    TestLogState state;
    constexpr uint64_t kTxn = 8;
    state.UpsertSchemaOp(
        kTxn, 200, CreateTableOp(SchemaOpMessage_Stage_PrepareSchema));
    SchemaOpMessage commit = CreateTableOp(SchemaOpMessage_Stage_CommitSchema);
    commit.set_rolled_back(true);
    state.UpsertSchemaOp(kTxn, 0, commit);

    REQUIRE(ReplayedCommitTs(state, kTxn) == 0);
    std::vector<Item::Pointer> ops;
    state.GetSchemaOpList(ops);
    REQUIRE(ops.size() == 1);
    SchemaOpMessage replayed;
    REQUIRE(replayed.ParseFromString(ops[0]->log_message_));
    REQUIRE(replayed.rolled_back());
}
