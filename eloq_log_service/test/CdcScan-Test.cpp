/**
 * @file
 * Tests for the CDC log scan (cdc_scan.h): the continuation codec, the page
 * merge over the data and DDL streams, the scan served by the RocksDB log
 * state over its data log and its DDL history, and the CDC retention floor in
 * its purge.
 */

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cdc_scan.h"
#include "log.pb.h"

#if defined(LOG_STATE_TYPE_RKDB)
#include "log_state_rocksdb_impl.h"
#endif

// butil's logging, which the log state pulls in, defines a one-argument CHECK
// that would replace Catch's; nothing here uses butil's. Catch comes last so
// that its definition is the one in force.
#undef CHECK
#include <catch2/catch_all.hpp>

using namespace txlog;

namespace
{
/**
 * @brief A storage key laid out the way both stores lay theirs out.
 *
 * The big-endian timestamp first, then what tells records at one timestamp
 * apart, padded to the 20 bytes of a data key so that the same keys serve
 * both streams.
 *
 * @param ts The record's timestamp.
 * @param suffix What tells it apart from others at `ts`.
 * @return The key.
 */
std::string KeyAt(uint64_t ts, const std::string &suffix)
{
    std::string key = CdcTimestampPosition(ts) + suffix;
    key.resize(std::max<size_t>(key.size(), 20), '\0');
    return key;
}

struct FakeRecord
{
    uint64_t ts;
    std::string key;
    size_t size;
};

FakeRecord Rec(uint64_t ts, const std::string &suffix, size_t size = 10)
{
    return {ts, KeyAt(ts, suffix), size};
}

/**
 * A stream over records held in memory, in key order, as a store would hold
 * them. It appends data records with the key as the blob and DDL records with
 * the key as the table name, so a test can tell every returned record apart.
 */
class VectorCursor final : public CdcStreamCursor
{
public:
    VectorCursor(std::vector<FakeRecord> records, bool is_ddl)
        : records_(std::move(records)), is_ddl_(is_ddl)
    {
        std::sort(records_.begin(),
                  records_.end(),
                  [](const FakeRecord &a, const FakeRecord &b)
                  { return a.key < b.key; });
    }

    void Seek(const std::string &after, uint64_t upper_ts) override
    {
        upper_ts_ = upper_ts;
        idx_ = 0;
        while (idx_ < records_.size() && !(records_[idx_].key > after))
        {
            ++idx_;
        }
    }
    bool Valid() const override
    {
        return idx_ < records_.size() && records_[idx_].ts <= upper_ts_;
    }
    bool Ok() const override
    {
        return true;
    }
    std::string ErrorText() const override
    {
        return {};
    }
    uint64_t Timestamp() const override
    {
        return records_[idx_].ts;
    }
    std::string_view Key() const override
    {
        return records_[idx_].key;
    }
    size_t ByteSize() const override
    {
        return records_[idx_].size;
    }
    void AppendTo(CdcScanResponse *response) const override
    {
        const FakeRecord &r = records_[idx_];
        if (is_ddl_)
        {
            CdcDdlRecord *ddl = response->add_ddl();
            ddl->set_ddl_ts(r.ts);
            ddl->mutable_schema_op()->set_table_name_str(r.key);
        }
        else
        {
            CdcDataRecord *data = response->add_data();
            data->set_commit_ts(r.ts);
            data->set_blob(r.key);
        }
    }
    void Next() override
    {
        ++idx_;
    }

private:
    std::vector<FakeRecord> records_;
    bool is_ddl_;
    uint64_t upper_ts_{0};
    size_t idx_{0};
};

struct Returned
{
    bool ddl;
    uint64_t ts;
    std::string key;

    bool operator==(const Returned &o) const
    {
        return ddl == o.ddl && ts == o.ts && key == o.key;
    }
};

/**
 * @brief The records of one page in merged order, rebuilt the way a caller
 * would: both lists are ascending, and at one timestamp data comes first.
 *
 * @param page The page.
 * @return Its records in merged order.
 */
std::vector<Returned> MergedPage(const CdcScanResponse &page)
{
    std::vector<Returned> out;
    int d = 0, l = 0;
    while (d < page.data_size() || l < page.ddl_size())
    {
        const bool take_data =
            d < page.data_size() &&
            (l == page.ddl_size() ||
             page.data(d).commit_ts() <= page.ddl(l).ddl_ts());
        if (take_data)
        {
            out.push_back(
                {false, page.data(d).commit_ts(), page.data(d).blob()});
            ++d;
        }
        else
        {
            out.push_back({true,
                           page.ddl(l).ddl_ts(),
                           page.ddl(l).schema_op().table_name_str()});
            ++l;
        }
    }
    return out;
}

/**
 * @brief The whole merged sequence at or below a bound, as the scan should
 * page it.
 *
 * @param data The data stream's records.
 * @param ddl The DDL stream's records.
 * @param bound The arrival_complete_ts of the scan.
 * @return The records at or below `bound` in merged order.
 */
std::vector<Returned> Expected(const std::vector<FakeRecord> &data,
                               const std::vector<FakeRecord> &ddl,
                               uint64_t bound)
{
    std::vector<Returned> out;
    for (const FakeRecord &r : data)
    {
        if (r.ts <= bound)
        {
            out.push_back({false, r.ts, r.key});
        }
    }
    for (const FakeRecord &r : ddl)
    {
        if (r.ts <= bound)
        {
            out.push_back({true, r.ts, r.key});
        }
    }
    std::sort(out.begin(),
              out.end(),
              [](const Returned &a, const Returned &b)
              {
                  if (a.ts != b.ts)
                  {
                      return a.ts < b.ts;
                  }
                  if (a.ddl != b.ddl)
                  {
                      return !a.ddl;  // data first
                  }
                  return a.key < b.key;
              });
    return out;
}

CdcScanResponse ScanPage(const std::vector<FakeRecord> &data,
                         const std::vector<FakeRecord> &ddl,
                         const CdcScanPosition &start,
                         uint64_t bound,
                         size_t page_bytes)
{
    VectorCursor data_cursor(data, false);
    VectorCursor ddl_cursor(ddl, true);
    CdcScanResponse page;
    std::string error;
    REQUIRE(RunCdcScan(
        data_cursor, ddl_cursor, start, bound, page_bytes, &page, &error));
    return page;
}

CdcScanPosition Decode(const std::string &continuation)
{
    CdcScanPosition position;
    std::string error;
    REQUIRE(DecodeCdcContinuation(continuation, &position, &error));
    return position;
}
}  // namespace

TEST_CASE("A continuation round-trips and is laid out big-endian", "[cdc_scan]")
{
    CdcScanPosition position;
    position.data = KeyAt(7, std::string(12, '\x01'));   // a 20-byte data key
    position.ddl = CdcTimestampPosition(9) + "ns.coll";  // a 15-byte DDL key
    const std::string bytes = EncodeCdcContinuation(position);

    REQUIRE(bytes.size() == 2 + 20 + 2 + 15);
    CHECK(bytes.substr(0, 2) == std::string("\x00\x14", 2));
    CHECK(bytes.substr(2, 20) == position.data);
    CHECK(bytes.substr(22, 2) == std::string("\x00\x0f", 2));
    CHECK(bytes.substr(24) == position.ddl);

    const CdcScanPosition decoded = Decode(bytes);
    CHECK(decoded.data == position.data);
    CHECK(decoded.ddl == position.ddl);

    // Every legal shape, including the empty and the timestamp positions.
    for (const auto &[data, ddl] :
         std::vector<std::pair<std::string, std::string>>{
             {"", ""},
             {CdcTimestampPosition(5), ""},
             {"", CdcTimestampPosition(5)},
             {CdcTimestampPosition(5), CdcTimestampPosition(6)}})
    {
        const CdcScanPosition p = Decode(EncodeCdcContinuation({data, ddl}));
        CHECK(p.data == data);
        CHECK(p.ddl == ddl);
    }
}

TEST_CASE("A continuation this service could not have issued is rejected",
          "[cdc_scan]")
{
    const auto rejected = [](const std::string &bytes)
    {
        CdcScanPosition position;
        std::string error;
        const bool ok = DecodeCdcContinuation(bytes, &position, &error);
        CHECK_FALSE(error.empty());
        return !ok;
    };
    CHECK(rejected(""));
    CHECK(rejected(std::string("\x00", 1)));
    // A length running past the end.
    CHECK(rejected(std::string("\x00\x08", 2) + "1234"));
    // Only the data position present.
    CHECK(rejected(std::string("\x00\x00", 2)));
    // Trailing bytes after both positions.
    CHECK(rejected(EncodeCdcContinuation({}) + "x"));
    // A data position that is neither a timestamp nor a data key.
    CHECK(rejected(EncodeCdcContinuation({"12345", ""})));
    CHECK(rejected(EncodeCdcContinuation({std::string(21, 'k'), ""})));
    // A DDL position shorter than a timestamp.
    CHECK(rejected(EncodeCdcContinuation({"", "abc"})));
}

TEST_CASE("A scan starts at its continuation, at from_ts, or at the beginning",
          "[cdc_scan]")
{
    CdcScanPosition start;
    std::string error;

    CdcScanRequest from_ts;
    from_ts.set_from_ts(1234);
    REQUIRE(ParseCdcScanStart(from_ts, &start, &error));
    CHECK(start.data == CdcTimestampPosition(1234));
    CHECK(start.ddl == CdcTimestampPosition(1234));

    // Timestamp 0 precedes every record: the canonical empty position.
    CdcScanRequest from_zero;
    from_zero.set_from_ts(0);
    REQUIRE(ParseCdcScanStart(from_zero, &start, &error));
    CHECK(start.data.empty());
    CHECK(start.ddl.empty());

    CdcScanRequest neither;
    start = {"stale", "stale"};
    REQUIRE(ParseCdcScanStart(neither, &start, &error));
    CHECK(start.data.empty());
    CHECK(start.ddl.empty());

    CdcScanRequest resumed;
    const CdcScanPosition issued{KeyAt(3, std::string(12, 'k')),
                                 CdcTimestampPosition(2)};
    resumed.set_continuation(EncodeCdcContinuation(issued));
    REQUIRE(ParseCdcScanStart(resumed, &start, &error));
    CHECK(start.data == issued.data);
    CHECK(start.ddl == issued.ddl);

    CdcScanRequest garbled;
    garbled.set_continuation("garbled");
    CHECK_FALSE(ParseCdcScanStart(garbled, &start, &error));
}

TEST_CASE("The page size defaults and is capped", "[cdc_scan]")
{
    CHECK(CdcScanPageBytes(0) == 4 * 1024 * 1024);
    CHECK(CdcScanPageBytes(1) == 1);
    CHECK(CdcScanPageBytes(1 << 20) == 1 << 20);
    CHECK(CdcScanPageBytes(UINT32_MAX) == 32 * 1024 * 1024);
}

TEST_CASE("Every page size returns the whole merged sequence exactly once",
          "[cdc_scan]")
{
    // Both streams share timestamps 3 and 5; timestamp 9 lies above the
    // bound; sizes vary so that cuts land in every possible place.
    const std::vector<FakeRecord> data{Rec(1, "a", 7),
                                       Rec(1, "b", 13),
                                       Rec(3, "a", 5),
                                       Rec(5, "a", 11),
                                       Rec(5, "b", 3),
                                       Rec(5, "c", 17),
                                       Rec(7, "a", 9),
                                       Rec(9, "a", 4)};
    const std::vector<FakeRecord> ddl{Rec(3, "x", 6),
                                      Rec(5, "x", 12),
                                      Rec(5, "y", 8),
                                      Rec(6, "x", 10),
                                      Rec(9, "x", 2)};
    const uint64_t bound = 8;
    const std::vector<Returned> expected = Expected(data, ddl, bound);
    size_t total = 0;
    for (const FakeRecord &r : data)
    {
        total += r.size;
    }
    for (const FakeRecord &r : ddl)
    {
        total += r.size;
    }

    for (size_t page_bytes = 1; page_bytes <= total + 1; ++page_bytes)
    {
        CAPTURE(page_bytes);
        std::vector<Returned> got;
        CdcScanPosition position;
        for (int pages = 0;; ++pages)
        {
            REQUIRE(pages <= static_cast<int>(expected.size()));
            const CdcScanResponse page =
                ScanPage(data, ddl, position, bound, page_bytes);
            CHECK(page.arrival_complete_ts() == bound);
            CHECK(page.retention_floor_ts() == 0);
            const std::vector<Returned> records = MergedPage(page);
            // A page is never empty while records remain.
            CHECK(records.empty() == (got.size() == expected.size()));
            got.insert(got.end(), records.begin(), records.end());
            // Every page ends on a prefix of the merged sequence.
            REQUIRE(got.size() <= expected.size());
            CHECK(std::equal(got.begin(), got.end(), expected.begin()));
            position = Decode(page.continuation());
            CHECK(page.has_more() == (got.size() < expected.size()));
            if (!page.has_more())
            {
                break;
            }
        }
        CHECK(got == expected);
    }
}

TEST_CASE("A page cut inside one timestamp resumes without gap or duplicate",
          "[cdc_scan]")
{
    const std::vector<FakeRecord> data{Rec(5, "a"), Rec(5, "b")};
    const std::vector<FakeRecord> ddl{Rec(5, "x"), Rec(5, "y")};

    // Three records fit: both data records, then the first DDL record.
    CdcScanResponse page = ScanPage(data, ddl, {}, 10, 30);
    REQUIRE(page.data_size() == 2);
    REQUIRE(page.ddl_size() == 1);
    CHECK(page.ddl(0).schema_op().table_name_str() == KeyAt(5, "x"));
    CHECK(page.has_more());
    CdcScanPosition position = Decode(page.continuation());
    CHECK(position.data == KeyAt(5, "b"));
    CHECK(position.ddl == KeyAt(5, "x"));

    page = ScanPage(data, ddl, position, 10, 30);
    REQUIRE(page.data_size() == 0);
    REQUIRE(page.ddl_size() == 1);
    CHECK(page.ddl(0).schema_op().table_name_str() == KeyAt(5, "y"));
    CHECK_FALSE(page.has_more());

    // Cut after the first data record: the DDL stream has returned nothing
    // at 5, so it resumes there.
    page = ScanPage(data, ddl, {}, 10, 10);
    REQUIRE(page.data_size() == 1);
    REQUIRE(page.ddl_size() == 0);
    position = Decode(page.continuation());
    CHECK(position.data == KeyAt(5, "a"));
    CHECK(position.ddl.empty());
    page = ScanPage(data, ddl, position, 10, 1000);
    CHECK(MergedPage(page) == std::vector<Returned>{{false, 5, KeyAt(5, "b")},
                                                    {true, 5, KeyAt(5, "x")},
                                                    {true, 5, KeyAt(5, "y")}});
}

TEST_CASE("Records above arrival_complete_ts wait for a later bound",
          "[cdc_scan]")
{
    const std::vector<FakeRecord> data{Rec(2, "a"), Rec(6, "a")};
    const std::vector<FakeRecord> ddl{Rec(3, "x"), Rec(8, "x")};

    CdcScanResponse page = ScanPage(data, ddl, {}, 4, 1000);
    CHECK(MergedPage(page) == std::vector<Returned>{{false, 2, KeyAt(2, "a")},
                                                    {true, 3, KeyAt(3, "x")}});
    // Nothing at or below the bound remains, so the page was not cut.
    CHECK_FALSE(page.has_more());

    page = ScanPage(data, ddl, Decode(page.continuation()), 10, 1000);
    CHECK(MergedPage(page) == std::vector<Returned>{{false, 6, KeyAt(6, "a")},
                                                    {true, 8, KeyAt(8, "x")}});
    CHECK(page.arrival_complete_ts() == 10);
}

TEST_CASE("An empty page carries the bound and keeps the position",
          "[cdc_scan]")
{
    const std::vector<FakeRecord> data{Rec(20, "a")};
    const CdcScanPosition start{KeyAt(4, std::string(12, 'k')),
                                CdcTimestampPosition(3)};
    const CdcScanResponse page = ScanPage(data, {}, start, 10, 1000);
    CHECK(page.data_size() == 0);
    CHECK(page.ddl_size() == 0);
    CHECK(page.arrival_complete_ts() == 10);
    CHECK_FALSE(page.has_more());
    CHECK(page.continuation() == EncodeCdcContinuation(start));

    // No bound at all yet: still an empty, well-formed page.
    const CdcScanResponse unbounded = ScanPage(data, {}, {}, 0, 1000);
    CHECK(unbounded.data_size() == 0);
    CHECK(unbounded.arrival_complete_ts() == 0);
    CHECK_FALSE(unbounded.has_more());
}

TEST_CASE("A record larger than the page is returned on its own", "[cdc_scan]")
{
    const std::vector<FakeRecord> data{Rec(1, "big", 1000), Rec(2, "a", 1)};
    CdcScanResponse page = ScanPage(data, {}, {}, 10, 100);
    REQUIRE(page.data_size() == 1);
    CHECK(page.data(0).blob() == KeyAt(1, "big"));
    CHECK(page.has_more());
    page = ScanPage(data, {}, Decode(page.continuation()), 10, 100);
    REQUIRE(page.data_size() == 1);
    CHECK(page.data(0).blob() == KeyAt(2, "a"));
    CHECK_FALSE(page.has_more());
}

TEST_CASE("A stream that returns nothing keeps its from_ts position",
          "[cdc_scan]")
{
    const CdcScanPosition start{CdcTimestampPosition(4),
                                CdcTimestampPosition(4)};

    // Below from_ts nothing is returned; above the bound nothing yet.
    std::vector<FakeRecord> data{Rec(2, "a"), Rec(6, "a")};
    CdcScanResponse page = ScanPage(data, {}, start, 5, 1000);
    CHECK(page.data_size() == 0);
    CdcScanPosition position = Decode(page.continuation());
    CHECK(position.data == CdcTimestampPosition(4));
    CHECK(position.ddl == CdcTimestampPosition(4));

    // Records at from_ts itself are included; the DDL stream, still empty,
    // keeps the eight-byte position.
    data.push_back(Rec(4, "b"));
    page = ScanPage(data, {}, position, 10, 1000);
    CHECK(MergedPage(page) == std::vector<Returned>{{false, 4, KeyAt(4, "b")},
                                                    {false, 6, KeyAt(6, "a")}});
    position = Decode(page.continuation());
    CHECK(position.data == KeyAt(6, "a"));
    CHECK(position.ddl == CdcTimestampPosition(4));

    // A DDL record that arrives later, above the bound the scan saw, is
    // found from the eight-byte position.
    page = ScanPage(data, {Rec(11, "x")}, position, 12, 1000);
    CHECK(MergedPage(page) ==
          std::vector<Returned>{{true, 11, KeyAt(11, "x")}});
}

#if defined(LOG_STATE_TYPE_RKDB)
namespace
{
/** @brief A RocksDB log state in a fresh directory, removed afterwards. */
class ScratchLogState
{
public:
    explicit ScratchLogState(size_t sst_files_size_limit = size_t{1} << 40)
        : path_(std::filesystem::temp_directory_path() /
                ("cdc_scan_test_" + std::to_string(::getpid()) + "_" +
                 std::to_string(counter_++)))
    {
        std::filesystem::remove_all(path_);
        state_ = std::make_unique<LogStateRocksDBImpl>(
            (path_ / "rocksdb").string(), sst_files_size_limit, 1);
        REQUIRE(state_->Start() == 0);
    }

    /**
     * Flushes the memtables into a new SST file, which also wakes the purge
     * thread. The state offers no flush of its own; a snapshot's RocksDB
     * checkpoint always flushes first.
     */
    void Flush()
    {
        Snapshot();
    }

    /**
     * @brief Writes a snapshot into a new directory, as a leader does for a
     * follower that lags: copies the in-memory state, then writes it.
     * Flushes the memtables first, like Flush().
     *
     * @return The snapshot directory, and its files relative to it.
     */
    std::pair<std::string, std::vector<std::string>> Snapshot()
    {
        const std::filesystem::path snapshot =
            path_ / ("snapshot_" + std::to_string(snapshots_++));
        std::filesystem::create_directories(snapshot);
        state_->BeginSnapshot();
        return {snapshot.string(), state_->WriteSnapshot(snapshot.string())};
    }

    ~ScratchLogState()
    {
        state_.reset();
        std::filesystem::remove_all(path_);
    }

    LogStateRocksDBImpl &operator*()
    {
        return *state_;
    }
    LogStateRocksDBImpl *operator->()
    {
        return state_.get();
    }

private:
    static inline int counter_ = 0;
    std::filesystem::path path_;
    int snapshots_{0};
    std::unique_ptr<LogStateRocksDBImpl> state_;
};

CdcScanResponse StateScan(LogStateRocksDBImpl &state,
                          const CdcScanRequest &request)
{
    CdcScanResponse page;
    std::string error;
    REQUIRE(state.CdcScan(request, &page, &error) == CdcScanStatus::Ok);
    CHECK(error.empty());
    return page;
}

struct DataRow
{
    uint64_t ts;
    uint32_t ng;
    uint64_t txn;
    std::string blob;

    bool operator==(const DataRow &o) const
    {
        return ts == o.ts && ng == o.ng && txn == o.txn && blob == o.blob;
    }
};

std::vector<DataRow> Rows(const CdcScanResponse &page)
{
    std::vector<DataRow> out;
    for (const CdcDataRecord &r : page.data())
    {
        out.push_back({r.commit_ts(), r.cc_ng_id(), r.txn_number(), r.blob()});
    }
    return out;
}
}  // namespace

TEST_CASE("The RocksDB log state scans its data log through min_ckpt_ts",
          "[cdc_scan][rocksdb]")
{
    ScratchLogState state;
    state->UpdateNgTerm(0, 1, "127.0.0.1", 1);
    state->UpdateNgTerm(1, 1, "127.0.0.1", 2);

    // Binary blobs, two node groups sharing a timestamp.
    const std::string zero_byte("a\0z", 3);
    const std::vector<DataRow> rows{{100, 0, 11, zero_byte},
                                    {100, 1, 12, "b100"},
                                    {150, 0, 13, "a150"},
                                    {200, 1, 14, "b200"},
                                    {300, 0, 15, "a300"}};
    for (const DataRow &r : rows)
    {
        state->AddLogItem(r.ng, r.txn, r.ts, r.blob);
    }

    CdcScanRequest from_start;
    from_start.set_from_ts(0);

    // No checkpoint yet: nothing is known complete.
    CdcScanResponse page = StateScan(*state, from_start);
    CHECK(page.data_size() == 0);
    CHECK(page.arrival_complete_ts() == 0);
    CHECK_FALSE(page.has_more());

    // arrival_complete_ts is the minimum over node groups.
    state->UpdateCkptTs(0, 250);
    state->UpdateCkptTs(1, 200);
    page = StateScan(*state, from_start);
    CHECK(page.arrival_complete_ts() == 200);
    CHECK(Rows(page) == std::vector<DataRow>(rows.begin(), rows.begin() + 4));
    CHECK_FALSE(page.has_more());

    // One record per page, through the continuation.
    std::vector<DataRow> paged;
    CdcScanRequest request = from_start;
    request.set_max_bytes(1);
    for (int i = 0; i < 4; ++i)
    {
        page = StateScan(*state, request);
        REQUIRE(page.data_size() == 1);
        paged.push_back(Rows(page).front());
        CHECK(page.has_more() == (i < 3));
        request.set_continuation(page.continuation());
    }
    CHECK(paged == std::vector<DataRow>(rows.begin(), rows.begin() + 4));

    // Exhausted: the same continuation returns nothing until the bound moves.
    page = StateScan(*state, request);
    CHECK(page.data_size() == 0);
    state->UpdateCkptTs(0, 400);
    state->UpdateCkptTs(1, 350);
    page = StateScan(*state, request);
    CHECK(page.arrival_complete_ts() == 350);
    CHECK(Rows(page) == std::vector<DataRow>{rows[4]});

    // from_ts starts at the first record at or above it.
    CdcScanRequest from_150;
    from_150.set_from_ts(150);
    page = StateScan(*state, from_150);
    CHECK(Rows(page) == std::vector<DataRow>(rows.begin() + 2, rows.end()));
}

namespace
{
/**
 * @brief Logs an EloqDoc schema op's prepare and commit logs.
 *
 * @param state The log state.
 * @param txn The op's tx.
 * @param table The table name.
 * @param op_type The txservice::OperationType.
 * @param prepare_ts The prepare log's timestamp, the op's catalog version.
 * @param commit_ts The commit log's timestamp.
 */
void LogSchemaOp(LogStateRocksDBImpl &state,
                 uint64_t txn,
                 const std::string &table,
                 uint32_t op_type,
                 uint64_t prepare_ts,
                 uint64_t commit_ts)
{
    SchemaOpMessage msg;
    msg.set_table_name_str(table);
    msg.set_table_engine(TableEngine::EloqDoc);
    msg.set_table_type(CcTableType::Primary);
    msg.mutable_table_op()->set_op_type(op_type);
    msg.set_stage(SchemaOpMessage_Stage_PrepareSchema);
    state.UpsertSchemaOp(txn, prepare_ts, msg);
    msg.set_stage(SchemaOpMessage_Stage_CommitSchema);
    state.UpsertSchemaOp(txn, commit_ts, msg);
}

/**
 * @param page A scan page.
 * @return (ddl_ts, txn) of its DDL records.
 */
std::vector<std::pair<uint64_t, uint64_t>> DdlRows(const CdcScanResponse &page)
{
    std::vector<std::pair<uint64_t, uint64_t>> out;
    for (const CdcDdlRecord &r : page.ddl())
    {
        out.emplace_back(r.ddl_ts(), r.txn_number());
    }
    return out;
}
}  // namespace

TEST_CASE("The RocksDB log state scans its DDL history with its data log",
          "[cdc_scan][rocksdb]")
{
    ScratchLogState state;
    state->UpdateNgTerm(0, 1, "127.0.0.1", 1);
    // A window keeps the DDL records a checkpoint passes.
    state->SetCdcRetentionSeconds(1);
    for (uint64_t ts : {100, 200, 300})
    {
        state->AddLogItem(0, ts, ts, "d" + std::to_string(ts));
    }
    // A create committed at 200, the data record's timestamp, and an add
    // index at 250 (txservice::OperationType CreateTable and AddIndex).
    LogSchemaOp(*state, 50, "db.a", 5, 150, 200);
    LogSchemaOp(*state, 51, "db.a", 8, 250, 250);
    state->UpdateCkptTs(0, 260);

    CdcScanRequest from_start;
    from_start.set_from_ts(0);
    CdcScanResponse page = StateScan(*state, from_start);
    CHECK(page.data_size() == 2);
    CHECK(DdlRows(page) ==
          std::vector<std::pair<uint64_t, uint64_t>>{{200, 50}, {250, 51}});
    CHECK(page.ddl(0).catalog_ts() == 150);
    CHECK(page.ddl(0).schema_op().table_name_str() == "db.a");
    CHECK_FALSE(page.has_more());

    // One record per page: at 200 the data record comes first.
    std::vector<std::pair<bool, uint64_t>> merged;
    CdcScanRequest request = from_start;
    request.set_max_bytes(1);
    for (int i = 0; i < 4; ++i)
    {
        page = StateScan(*state, request);
        REQUIRE(page.data_size() + page.ddl_size() == 1);
        merged.emplace_back(page.ddl_size() == 1,
                            page.ddl_size() == 1 ? page.ddl(0).ddl_ts()
                                                 : page.data(0).commit_ts());
        CHECK(page.has_more() == (i < 3));
        request.set_continuation(page.continuation());
    }
    CHECK(merged == std::vector<std::pair<bool, uint64_t>>{
                        {false, 100}, {false, 200}, {true, 200}, {true, 250}});
    CdcScanPosition position;
    std::string error;
    REQUIRE(DecodeCdcContinuation(page.continuation(), &position, &error));
    CHECK(position.ddl == CdcDdlPosition(250, 51));
    page = StateScan(*state, request);
    CHECK(page.data_size() + page.ddl_size() == 0);

    // A follower that installs the snapshot has the same history.
    auto [snapshot_dir, files] = state.Snapshot();
    ScratchLogState follower;
    follower->SetCdcRetentionSeconds(1);
    REQUIRE(follower->ReadSnapshot(snapshot_dir, files) == 0);
    // The bound is rebuilt at the next checkpoint.
    follower->UpdateCkptTs(0, 261);
    page = StateScan(*follower, from_start);
    CHECK(page.data_size() == 2);
    CHECK(DdlRows(page) ==
          std::vector<std::pair<uint64_t, uint64_t>>{{200, 50}, {250, 51}});
}

TEST_CASE("The CDC retention floor is the checkpoint less the window",
          "[cdc_scan][rocksdb]")
{
    ScratchLogState state;
    CHECK(state->CdcRetentionFloor(8'000'000) == 8'000'000);
    state->SetCdcRetentionSeconds(4);
    CHECK(state->CdcRetentionFloor(8'000'000) == 4'000'000);
    CHECK(state->CdcRetentionFloor(4'000'000) == 0);
    CHECK(state->CdcRetentionFloor(3'000'000) == 0);
}

namespace
{
/**
 * @brief Writes one batch of records and flushes it into its own SST file.
 *
 * @param state The log state.
 * @param base_ts The first record's timestamp; the others follow at
 *        `base_ts + 1`, `base_ts + 2`, ...
 * @param records How many records to write.
 */
void WriteBatchFile(ScratchLogState &state, uint64_t base_ts, int records)
{
    for (int i = 0; i < records; ++i)
    {
        state->AddLogItem(0, base_ts + i, base_ts + i, "r");
    }
    state.Flush();
}

/**
 * @param state The log state.
 * @return The commit timestamps a full scan through the checkpoint returns.
 */
std::vector<uint64_t> ScannedTimestamps(LogStateRocksDBImpl &state)
{
    CdcScanRequest request;
    request.set_from_ts(0);
    std::vector<uint64_t> out;
    for (const DataRow &row : Rows(StateScan(state, request)))
    {
        out.push_back(row.ts);
    }
    return out;
}

/**
 * @brief Polls until a condition holds for the scanned timestamps, or ten
 * seconds pass.
 *
 * @param state The log state.
 * @param done The condition on ScannedTimestamps().
 * @return The last timestamps scanned.
 */
template <typename Pred>
std::vector<uint64_t> WaitForScan(LogStateRocksDBImpl &state, Pred done)
{
    std::vector<uint64_t> ts = ScannedTimestamps(state);
    for (int i = 0; i < 100 && !done(ts); ++i)
    {
        usleep(100 * 1000);
        ts = ScannedTimestamps(state);
    }
    return ts;
}
}  // namespace

TEST_CASE("The RocksDB purge keeps the CDC retention window",
          "[cdc_scan][rocksdb]")
{
    // A one-byte size limit makes every flush trigger a purge.
    ScratchLogState state(1);
    state->UpdateNgTerm(0, 1, "127.0.0.1", 1);
    state->SetCdcRetentionSeconds(4);

    // Three files, timestamps in microseconds: A at 1 s, B at 5 s, C at 9 s.
    WriteBatchFile(state, 1'000'000, 3);
    WriteBatchFile(state, 5'000'000, 3);
    WriteBatchFile(state, 9'000'000, 3);

    // Checkpoint at 8 s. The floor is 8 s - 4 s = 4 s: A lies wholly below
    // it, B does not. The next flush wakes the purge.
    state->UpdateCkptTs(0, 8'000'000);
    WriteBatchFile(state, 9'500'000, 1);
    std::vector<uint64_t> ts =
        WaitForScan(*state,
                    [](const std::vector<uint64_t> &t)
                    { return !t.empty() && t.front() >= 5'000'000; });
    CHECK(ts == std::vector<uint64_t>{5'000'000, 5'000'001, 5'000'002});

    // With no window the purge takes everything checkpointed, B included.
    // The checkpoint must move for the purge to run again.
    state->SetCdcRetentionSeconds(0);
    state->UpdateCkptTs(0, 8'100'000);
    WriteBatchFile(state, 9'600'000, 1);
    ts = WaitForScan(*state,
                     [](const std::vector<uint64_t> &t) { return t.empty(); });
    CHECK(ts.empty());

    // Everything above the checkpoint survived both purges. No flush follows,
    // so no purge runs after the checkpoint moves past it.
    state->UpdateCkptTs(0, 9'700'000);
    CHECK(ScannedTimestamps(*state) ==
          std::vector<uint64_t>{
              9'000'000, 9'000'001, 9'000'002, 9'500'000, 9'600'000});
}

namespace
{
/**
 * @param state The log state.
 * @return The ddl_ts of every record in its CDC DDL history.
 */
std::vector<uint64_t> HistoryTs(LogStateRocksDBImpl &state)
{
    std::vector<CdcDdlRecord> records;
    state.CopyCdcDdlHistory({}, UINT64_MAX, &records);
    std::vector<uint64_t> out;
    for (const CdcDdlRecord &r : records)
    {
        out.push_back(r.ddl_ts());
    }
    return out;
}

/**
 * @brief Polls until the CDC DDL history holds the given timestamps, or ten
 * seconds pass.
 *
 * @param state The log state.
 * @param expected The ddl_ts of every record, in order.
 * @return The last history seen.
 */
std::vector<uint64_t> WaitForHistory(LogStateRocksDBImpl &state,
                                     const std::vector<uint64_t> &expected)
{
    std::vector<uint64_t> ts = HistoryTs(state);
    for (int i = 0; i < 100 && ts != expected; ++i)
    {
        usleep(100 * 1000);
        ts = HistoryTs(state);
    }
    return ts;
}
}  // namespace

TEST_CASE("The RocksDB purge trims the CDC DDL history by its floor",
          "[cdc_scan][rocksdb]")
{
    // A one-byte size limit makes every flush trigger a purge.
    ScratchLogState state(1);
    state->UpdateNgTerm(0, 1, "127.0.0.1", 1);
    // Creates committed at 1 s, 5 s and 9 s, in microseconds.
    for (uint64_t s : {1, 5, 9})
    {
        LogSchemaOp(*state,
                    50 + s,
                    "db.a" + std::to_string(s),
                    5,
                    s * 1'000'000 - 1,
                    s * 1'000'000);
    }

    // With no retention window a checkpoint trims nothing: the records it
    // passes are what a scan returns.
    state->UpdateCkptTs(0, 8'000'000);
    CdcScanRequest from_start;
    from_start.set_from_ts(0);
    CHECK(DdlRows(StateScan(*state, from_start)) ==
          std::vector<std::pair<uint64_t, uint64_t>>{{1'000'000, 51},
                                                     {5'000'000, 55}});

    // A purge by the floor 8 s - 4 s = 4 s, woken by the next flush, takes
    // the record at 1 s.
    state->SetCdcRetentionSeconds(4);
    WriteBatchFile(state, 9'500'000, 1);
    CHECK(WaitForHistory(*state, {5'000'000, 9'000'000}) ==
          std::vector<uint64_t>{5'000'000, 9'000'000});

    // With no window the next purge's floor is just below the checkpoint.
    state->SetCdcRetentionSeconds(0);
    state->UpdateCkptTs(0, 8'100'000);
    WriteBatchFile(state, 9'600'000, 1);
    CHECK(WaitForHistory(*state, {9'000'000}) ==
          std::vector<uint64_t>{9'000'000});
}

TEST_CASE("A CDC scan never reads the RocksDB state while it is reopened",
          "[cdc_scan][rocksdb]")
{
    ScratchLogState state;
    state->UpdateNgTerm(0, 1, "127.0.0.1", 1);
    constexpr int kRecords = 200;
    for (uint64_t i = 1; i <= kRecords; ++i)
    {
        state->AddLogItem(0, i, i, std::string(100, 'v'));
    }
    state->UpdateCkptTs(0, kRecords);

    // A snapshot to install again and again, as a follower installs the
    // leader's: each install closes db_, deletes its files, links the
    // snapshot's in and reopens. The ng-info file is left out so that the
    // bound the scans read stays put.
    auto [snapshot_dir, files] = state.Snapshot();
    files.erase(std::remove(files.begin(), files.end(), "data"), files.end());

    std::atomic<bool> stop{false};
    std::atomic<int> complete_pages{0};
    std::atomic<int> not_ready{0};
    std::atomic<int> bad{0};
    std::vector<std::thread> scanners;
    for (int t = 0; t < 3; ++t)
    {
        scanners.emplace_back(
            [&]
            {
                CdcScanRequest request;
                request.set_from_ts(0);
                while (!stop.load())
                {
                    CdcScanResponse page;
                    std::string error;
                    switch (state->CdcScan(request, &page, &error))
                    {
                    case CdcScanStatus::Ok:
                        // A scan that runs sees the whole, intact store.
                        if (page.data_size() == kRecords &&
                            page.data(kRecords - 1).commit_ts() == kRecords)
                        {
                            ++complete_pages;
                        }
                        else
                        {
                            ++bad;
                        }
                        break;
                    case CdcScanStatus::NotReady:
                        ++not_ready;
                        break;
                    default:
                        ++bad;
                    }
                }
            });
    }

    for (int i = 0; i < 30; ++i)
    {
        REQUIRE(state->ReadSnapshot(snapshot_dir, files) == 0);
    }
    stop.store(true);
    for (std::thread &t : scanners)
    {
        t.join();
    }

    CHECK(bad.load() == 0);
    CHECK(complete_pages.load() > 0);
    INFO("scans refused while the state was reopened: " << not_ready.load());
    // The store is readable again once the last install has finished.
    CdcScanRequest request;
    request.set_from_ts(0);
    CHECK(Rows(StateScan(*state, request)).size() == kRecords);
}

TEST_CASE("The RocksDB log state rejects a malformed continuation",
          "[cdc_scan][rocksdb]")
{
    ScratchLogState state;
    CdcScanRequest request;
    request.set_continuation("not a continuation");
    CdcScanResponse page;
    std::string error;
    CHECK(state->CdcScan(request, &page, &error) ==
          CdcScanStatus::InvalidArgument);
    CHECK_FALSE(error.empty());
}
#endif
