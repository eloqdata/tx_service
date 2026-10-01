/**
 * @file LogStateSchemaOp-Test.cpp
 * @brief The log state's schema op bookkeeping: the commit timestamp it keeps
 * for recovery, and the CDC DDL history it appends to.
 *
 * CREATE, DROP and TRUNCATE TABLE choose their commit timestamp at the commit
 * stage, above the prepare log's. The commit log's timestamp must therefore
 * replace the prepare log's, or an op recovered from its commit log would
 * resume at the prepare-stage timestamp.
 *
 * The CDC DDL history (docs/cdc-change-stream.md in the EloqDoc repo, §4.3)
 * holds one record per EloqDoc create, drop and index op, at the timestamp of
 * the log written under the op's catalog write lock.
 */
#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <ostream>
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
/**
 * @brief A log state that keeps no data log: the schema op bookkeeping under
 * test lives in the LogState base, whatever the log state type.
 */
class TestLogState : public LogState
{
public:
    using LogState::GetSchemaOpList;
    using LogState::LoadCdcDdlHistoryFrom;
    using LogState::MakeCopyOfNgInfoAndCatalogOps;
    using LogState::WriteSnapshotCdcDdlHistoryTo;

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

/** @brief txservice::OperationType values. */
constexpr uint32_t kUpdate = 1, kCreateTable = 5, kDropTable = 6,
                   kTruncateTable = 7, kAddIndex = 8, kDropIndex = 9;

/**
 * @brief A schema op message as EloqDoc logs it.
 *
 * @param table The table name.
 * @param op_type The txservice::OperationType.
 * @param stage The stage.
 * @param engine The table engine.
 * @return The message.
 */
SchemaOpMessage DocOp(const std::string &table,
                      uint32_t op_type,
                      SchemaOpMessage_Stage stage,
                      TableEngine engine = TableEngine::EloqDoc)
{
    SchemaOpMessage msg;
    msg.set_table_name_str(table);
    msg.set_table_engine(engine);
    msg.set_table_type(CcTableType::Primary);
    msg.set_stage(stage);
    msg.mutable_table_op()->set_op_type(op_type);
    return msg;
}

/**
 * @brief Logs a schema op's prepare and commit logs.
 *
 * @param state The log state.
 * @param txn The op's tx.
 * @param msg The op's prepare message; its commit message differs only in
 *        stage and mark.
 * @param prepare_ts The prepare log's timestamp, the op's catalog version.
 * @param commit_ts The commit log's timestamp.
 * @param rolled_back The commit log's rolled-back mark.
 */
void LogOp(TestLogState &state,
           uint64_t txn,
           SchemaOpMessage msg,
           uint64_t prepare_ts,
           uint64_t commit_ts,
           bool rolled_back = false)
{
    msg.set_stage(SchemaOpMessage_Stage_PrepareSchema);
    state.UpsertSchemaOp(txn, prepare_ts, msg);
    msg.set_stage(SchemaOpMessage_Stage_CommitSchema);
    msg.set_rolled_back(rolled_back);
    state.UpsertSchemaOp(txn, commit_ts, msg);
}

/**
 * @param state The log state.
 * @return The whole CDC DDL history, in position order.
 */
std::vector<CdcDdlRecord> History(const LogState &state)
{
    std::vector<CdcDdlRecord> records;
    state.CopyCdcDdlHistory({}, UINT64_MAX, &records);
    return records;
}

/**
 * @brief A DDL record reduced to what the tests compare.
 */
struct Ddl
{
    uint64_t ddl_ts;
    uint64_t catalog_ts;
    uint64_t txn;
    std::string table;
    uint32_t op_type;
    bool rolled_back;

    bool operator==(const Ddl &o) const
    {
        return ddl_ts == o.ddl_ts && catalog_ts == o.catalog_ts &&
               txn == o.txn && table == o.table && op_type == o.op_type &&
               rolled_back == o.rolled_back;
    }
};

std::ostream &operator<<(std::ostream &os, const Ddl &d)
{
    return os << "{" << d.ddl_ts << ", " << d.catalog_ts << ", " << d.txn
              << ", " << d.table << ", " << d.op_type << ", " << d.rolled_back
              << "}";
}

/**
 * @param records DDL records.
 * @return Their reduced form.
 */
std::vector<Ddl> Reduced(const std::vector<CdcDdlRecord> &records)
{
    std::vector<Ddl> out;
    for (const CdcDdlRecord &r : records)
    {
        out.push_back({r.ddl_ts(),
                       r.catalog_ts(),
                       r.txn_number(),
                       r.schema_op().table_name_str(),
                       r.schema_op().table_op().op_type(),
                       r.schema_op().rolled_back()});
    }
    return out;
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

TEST_CASE("CREATE and DROP enter the CDC DDL history at their commit log",
          "[log-state][cdc-ddl]")
{
    TestLogState state;
    const SchemaOpMessage create =
        DocOp("db.a", kCreateTable, SchemaOpMessage_Stage_PrepareSchema);

    // A create holds only an intent across its prepare log: no record yet.
    state.UpsertSchemaOp(11, 100, create);
    CHECK(History(state).empty());

    // The commit log re-assigned the timestamp under the write lock.
    SchemaOpMessage commit = create;
    commit.set_stage(SchemaOpMessage_Stage_CommitSchema);
    state.UpsertSchemaOp(11, 105, commit);
    const std::vector<CdcDdlRecord> records = History(state);
    REQUIRE(records.size() == 1);
    CHECK(Reduced(records) ==
          std::vector<Ddl>{{105, 100, 11, "db.a", kCreateTable, false}});
    CHECK(records[0].schema_op().stage() == SchemaOpMessage_Stage_CommitSchema);

    // A retried commit log, the clean log and a late prepare log change
    // nothing.
    state.UpsertSchemaOp(11, 105, commit);
    commit.set_stage(SchemaOpMessage_Stage_CleanSchema);
    state.UpsertSchemaOp(11, 0, commit);
    state.UpsertSchemaOp(11, 100, create);
    CHECK(Reduced(History(state)) ==
          std::vector<Ddl>{{105, 100, 11, "db.a", kCreateTable, false}});

    LogOp(state,
          12,
          DocOp("db.a", kDropTable, SchemaOpMessage_Stage_PrepareSchema),
          200,
          207);
    CHECK(Reduced(History(state)) ==
          std::vector<Ddl>{{105, 100, 11, "db.a", kCreateTable, false},
                           {207, 200, 12, "db.a", kDropTable, false}});
}

TEST_CASE("An index op enters the CDC DDL history at its prepare log",
          "[log-state][cdc-ddl]")
{
    TestLogState state;
    // One op adding two indexes is one record.
    SchemaOpMessage add =
        DocOp("db.a", kAddIndex, SchemaOpMessage_Stage_PrepareSchema);
    add.set_alter_table_info_blob("two index names");
    state.UpsertSchemaOp(21, 300, add);
    std::vector<CdcDdlRecord> records = History(state);
    REQUIRE(records.size() == 1);
    CHECK(Reduced(records) ==
          std::vector<Ddl>{{300, 300, 21, "db.a", kAddIndex, false}});
    CHECK(records[0].schema_op().alter_table_info_blob() == "two index names");

    // Its build and its commit log, at the same timestamp, add nothing.
    SchemaOpMessage data = add;
    data.set_stage(SchemaOpMessage_Stage_PrepareData);
    state.UpsertSchemaOp(21, 300, data);
    SchemaOpMessage commit = add;
    commit.set_stage(SchemaOpMessage_Stage_CommitSchema);
    state.UpsertSchemaOp(21, 300, commit);
    CHECK(Reduced(History(state)) ==
          std::vector<Ddl>{{300, 300, 21, "db.a", kAddIndex, false}});

    LogOp(state,
          22,
          DocOp("db.a", kDropIndex, SchemaOpMessage_Stage_PrepareSchema),
          400,
          400);
    CHECK(Reduced(History(state)) ==
          std::vector<Ddl>{{300, 300, 21, "db.a", kAddIndex, false},
                           {400, 400, 22, "db.a", kDropIndex, false}});
}

TEST_CASE("A rolled-back add index has a record at each of its two logs",
          "[log-state][cdc-ddl]")
{
    TestLogState state;
    LogOp(state,
          31,
          DocOp("db.a", kAddIndex, SchemaOpMessage_Stage_PrepareSchema),
          500,
          505,
          true);
    // createIndexes at the prepare log's timestamp, then the mark at the
    // commit log's, from which the runtime makes dropIndexes.
    CHECK(Reduced(History(state)) ==
          std::vector<Ddl>{{500, 500, 31, "db.a", kAddIndex, false},
                           {505, 500, 31, "db.a", kAddIndex, true}});
}

TEST_CASE("An op rolled back at timestamp 0 leaves no CDC DDL record",
          "[log-state][cdc-ddl]")
{
    TestLogState state;
    // A create that gave up on its kv tables.
    LogOp(state,
          41,
          DocOp("db.a", kCreateTable, SchemaOpMessage_Stage_PrepareSchema),
          600,
          0,
          true);
    CHECK(History(state).empty());

    // An index op's record, appended at its prepare log, is taken out again.
    LogOp(state,
          42,
          DocOp("db.a", kAddIndex, SchemaOpMessage_Stage_PrepareSchema),
          700,
          0,
          true);
    CHECK(History(state).empty());
}

TEST_CASE("Only EloqDoc's create, drop and index ops are recorded",
          "[log-state][cdc-ddl]")
{
    TestLogState state;
    // collMod reaches the tx service as an Update.
    LogOp(state,
          51,
          DocOp("db.a", kUpdate, SchemaOpMessage_Stage_PrepareSchema),
          800,
          805);
    LogOp(state,
          52,
          DocOp("db.a", kTruncateTable, SchemaOpMessage_Stage_PrepareSchema),
          810,
          815);
    LogOp(state,
          53,
          DocOp("t",
                kCreateTable,
                SchemaOpMessage_Stage_PrepareSchema,
                TableEngine::EloqSql),
          820,
          825);
    LogOp(state,
          54,
          DocOp("t",
                kAddIndex,
                SchemaOpMessage_Stage_PrepareSchema,
                TableEngine::EloqKv),
          830,
          830);
    SchemaOpMessage column_op =
        DocOp("db.a", kCreateTable, SchemaOpMessage_Stage_PrepareSchema);
    column_op.mutable_column_op()->set_column_name("c");
    LogOp(state, 55, column_op, 840, 845);
    CHECK(History(state).empty());
}

TEST_CASE("The CDC DDL history is copied by position", "[log-state][cdc-ddl]")
{
    TestLogState state;
    // Two creates on different tables commit at one timestamp: the
    // transaction number orders them.
    LogOp(state,
          62,
          DocOp("db.b", kCreateTable, SchemaOpMessage_Stage_PrepareSchema),
          900,
          950);
    LogOp(state,
          61,
          DocOp("db.a", kCreateTable, SchemaOpMessage_Stage_PrepareSchema),
          901,
          950);
    LogOp(state,
          63,
          DocOp("db.c", kCreateTable, SchemaOpMessage_Stage_PrepareSchema),
          960,
          970);

    const auto txns = [&](const std::string &after, uint64_t upper_ts)
    {
        std::vector<CdcDdlRecord> records;
        state.CopyCdcDdlHistory(after, upper_ts, &records);
        std::vector<uint64_t> out;
        for (const CdcDdlRecord &r : records)
        {
            out.push_back(r.txn_number());
        }
        return out;
    };
    CHECK(txns({}, UINT64_MAX) == std::vector<uint64_t>{61, 62, 63});
    // After a timestamp position: every record at that timestamp.
    CHECK(txns(CdcTimestampPosition(950), UINT64_MAX) ==
          std::vector<uint64_t>{61, 62, 63});
    // After a record's position: strictly after it.
    CHECK(txns(CdcDdlPosition(950, 61), UINT64_MAX) ==
          std::vector<uint64_t>{62, 63});
    CHECK(txns(CdcDdlPosition(950, 62), UINT64_MAX) ==
          std::vector<uint64_t>{63});
    // Nothing above the upper bound.
    CHECK(txns({}, 969) == std::vector<uint64_t>{61, 62});
    CHECK(txns({}, 949).empty());
}

TEST_CASE("The CDC DDL history is trimmed by a purge, not by a checkpoint",
          "[log-state][cdc-ddl]")
{
    TestLogState state;
    state.UpdateNgTerm(0, 1, "127.0.0.1", 1);
    // Records at 1 s, 5 s and 9 s, in microseconds.
    for (uint64_t s : {1, 5, 9})
    {
        LogOp(state,
              s,
              DocOp("db.a" + std::to_string(s),
                    kCreateTable,
                    SchemaOpMessage_Stage_PrepareSchema),
              s * 1'000'000 - 1,
              s * 1'000'000);
    }
    const auto ddl_ts = [&]
    {
        std::vector<uint64_t> out;
        for (const CdcDdlRecord &r : History(state))
        {
            out.push_back(r.ddl_ts());
        }
        return out;
    };

    // Checkpoints past every record, with no retention window, trim nothing:
    // a scan returns a record only once the checkpoint passes it.
    state.UpdateCkptTs(0, 8'000'000);
    state.UpdateCkptTs(0, 10'000'000);
    CHECK(ddl_ts() == std::vector<uint64_t>{1'000'000, 5'000'000, 9'000'000});

    // A purge trims below its floor; a record exactly at the floor stays.
    state.TrimCdcDdlHistory(5'000'000);
    CHECK(ddl_ts() == std::vector<uint64_t>{5'000'000, 9'000'000});
    state.TrimCdcDdlHistory(9'000'001);
    CHECK(ddl_ts().empty());
}

TEST_CASE("The CDC DDL history survives a snapshot", "[log-state][cdc-ddl]")
{
    TestLogState state;
    LogOp(state,
          71,
          DocOp("db.a", kCreateTable, SchemaOpMessage_Stage_PrepareSchema),
          1000,
          1005);
    LogOp(state,
          72,
          DocOp("db.a", kAddIndex, SchemaOpMessage_Stage_PrepareSchema),
          1010,
          1015,
          true);
    state.MakeCopyOfNgInfoAndCatalogOps();
    // Appended after the snapshot began: not in it.
    LogOp(state,
          73,
          DocOp("db.b", kCreateTable, SchemaOpMessage_Stage_PrepareSchema),
          1020,
          1025);

    const std::filesystem::path file =
        std::filesystem::temp_directory_path() /
        ("cdc_ddl_history_test_" + std::to_string(::getpid()));
    {
        std::ofstream os(file, std::ios::binary);
        state.WriteSnapshotCdcDdlHistoryTo(os);
    }

    TestLogState loaded;
    // Whatever the loading state held is replaced.
    LogOp(loaded,
          99,
          DocOp("db.z", kCreateTable, SchemaOpMessage_Stage_PrepareSchema),
          1,
          2);
    {
        std::ifstream is(file, std::ios::binary);
        REQUIRE(loaded.LoadCdcDdlHistoryFrom(is));
    }
    std::vector<CdcDdlRecord> expected = History(state);
    expected.pop_back();
    CHECK(Reduced(History(loaded)) == Reduced(expected));
    CHECK(Reduced(History(loaded)).size() == 3);
    // Positions are rebuilt: a copy after a record's position still works.
    std::vector<CdcDdlRecord> after;
    loaded.CopyCdcDdlHistory(CdcDdlPosition(1010, 72), UINT64_MAX, &after);
    CHECK(Reduced(after) ==
          std::vector<Ddl>{{1015, 1010, 72, "db.a", kAddIndex, true}});

    // A truncated file is refused.
    std::filesystem::resize_file(file, std::filesystem::file_size(file) - 1);
    TestLogState truncated;
    {
        std::ifstream is(file, std::ios::binary);
        CHECK_FALSE(truncated.LoadCdcDdlHistoryFrom(is));
    }
    CHECK(History(truncated).empty());

    // So is a record whose length runs past the file's end, before a buffer
    // of that length is sized; the loading state keeps what it had.
    {
        std::ofstream os(file, std::ios::binary | std::ios::trunc);
        const uint32_t cnt = 1;
        const uint32_t length = 0xfffffff0;
        os.write(reinterpret_cast<const char *>(&cnt), sizeof(cnt));
        os.write(reinterpret_cast<const char *>(&length), sizeof(length));
        os.write("abc", 3);
    }
    {
        std::ifstream is(file, std::ios::binary);
        CHECK_FALSE(loaded.LoadCdcDdlHistoryFrom(is));
    }
    CHECK(Reduced(History(loaded)).size() == 3);
    std::filesystem::remove(file);
}
