#define CATCH_CONFIG_PREFIX_ALL

#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include "log_state.h"
#include "log_state_memory_impl.h"
#include "log_state_rocksdb_impl.h"

namespace
{
namespace fs = std::filesystem;

/**
 * A log state of the configured backend in a temporary directory, optionally
 * seeded with a node-group term, a data log and a schema op.
 */
class SnapshotFixture
{
public:
    /**
     * @param payload_size Size of the seeded data log and table name; zero
     * leaves the state empty.
     */
    explicit SnapshotFixture(size_t payload_size = 0)
    {
        char name[] = "/tmp/eloq-log-stream-XXXXXX";
        char *created = mkdtemp(name);
        CATCH_REQUIRE(created != nullptr);
        path_ = created;
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
        // Exercise the follower's local snapshot streams without opening a
        // cloud DB or contacting object storage. The leader term reference
        // outlives the state, including its background purger threads.
        txlog::RocksDBCloudConfig config{};
        state_ = std::make_unique<txlog::LogStateRocksDBCloudImpl>(
            (path_ / "db").string(),
            config,
            term_,
            nullptr,
            1024,
            8,
            8,
            64 * 1024 * 1024,
            1);
        files_ = {"nginfo_and_catalog",
                  "cdc_ddl_history",
                  "cluster_gate",
                  "in_mem_data_log"};
#elif defined(LOG_STATE_TYPE_MEM)
        state_ = std::make_unique<txlog::LogStateMemoryImpl>();
        files_ = {"data"};
#else
#error "Stream tests require MEMORY or a RocksDB Cloud log state"
#endif
        fs::create_directory(path_ / "snapshot");
        if (payload_size == 0)
        {
            return;
        }
        CATCH_REQUIRE(state_->UpdateNgTerm(1, 7, "127.0.0.1", 9000));
        const std::string payload(payload_size, 'x');
        state_->AddLogItem(1, 99, 100, payload);
        txlog::SchemaOpMessage schema;
        schema.set_table_name_str(payload);
        schema.set_table_type(txlog::CcTableType::Primary);
        schema.set_stage(txlog::SchemaOpMessage::PrepareSchema);
        state_->UpsertSchemaOp(123, 50, schema);
    }

    ~SnapshotFixture()
    {
        state_.reset();
        std::error_code error;
        fs::remove_all(path_, error);
    }

    /**
     * Runs a full snapshot cycle outside braft.
     * @param path Directory to write into; created if missing.
     * @return The relative file names WriteSnapshot reported, empty on failure.
     */
    std::vector<std::string> Save(const fs::path &path)
    {
        fs::create_directories(path);
        state_->BeginSnapshot();
        auto files = state_->WriteSnapshot(path);
        state_->CleanSnapshotState();
        return files;
    }

    fs::path path_;                           ///< temporary directory
    std::atomic<int64_t> term_{-1};           ///< leader term for cloud
    std::unique_ptr<txlog::LogState> state_;  ///< state under test
    std::vector<std::string> files_;          ///< expected snapshot files
};

/**
 * @param path File to read.
 * @return The whole file as bytes; fails the test if it cannot be opened.
 */
std::string ReadFile(const fs::path &path)
{
    std::ifstream input(path, std::ios::binary);
    CATCH_REQUIRE(input.is_open());
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

CATCH_TEST_CASE("Snapshot stream output preserves metadata and data logs")
{
    SnapshotFixture source(GENERATE(size_t{4}, size_t{16384}));
    auto files = source.Save(source.path_ / "snapshot");
    CATCH_REQUIRE(files == source.files_);
    // A fresh follower can restore and serialize the same complete file set.
    SnapshotFixture restored;
    CATCH_REQUIRE(
        restored.state_->ReadSnapshot(source.path_ / "snapshot", files) == 0);
    CATCH_REQUIRE(restored.state_->GetNgLeaderTerm(1) == 7);
    CATCH_REQUIRE(restored.state_->SearchTxSchemaLog(123).first);
    CATCH_REQUIRE(restored.Save(restored.path_ / "roundtrip") == files);
    for (const auto &file : files)
    {
        auto original = ReadFile(source.path_ / "snapshot" / file);
        CATCH_REQUIRE_FALSE(original.empty());
        CATCH_REQUIRE(ReadFile(restored.path_ / "roundtrip" / file) ==
                      original);
    }
}

CATCH_TEST_CASE(
    "Snapshot stream errors reject the entire file set and allow retry")
{
    SnapshotFixture fixture(GENERATE(size_t{4}, size_t{16384}));
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    const auto file = GENERATE("nginfo_and_catalog",
                               "cdc_ddl_history",
                               "cluster_gate",
                               "in_mem_data_log");
#else
    const auto file = "data";
#endif
    const auto failed_path = fixture.path_ / "snapshot" / file;
    CATCH_SECTION("File cannot be opened")
    {
        fs::create_directory(failed_path);
    }
    CATCH_SECTION("Write or buffered close fails with ENOSPC")
    {
        // /dev/full opens successfully but rejects output. Small payloads
        // exercise buffered close; large payloads also fail during writing.
        // A symlink confines this injection to this fixture's output path.
        fs::create_symlink("/dev/full", failed_path);
    }
    CATCH_REQUIRE(fixture.Save(fixture.path_ / "snapshot").empty());
    fs::remove(failed_path);
    CATCH_REQUIRE(fixture.Save(fixture.path_ / "snapshot") == fixture.files_);
    for (const auto &name : fixture.files_)
    {
        CATCH_REQUIRE(fs::file_size(fixture.path_ / "snapshot" / name) > 0);
    }
}
}  // namespace
