#define CATCH_CONFIG_PREFIX_ALL

#include <braft/cli.h>
#include <braft/raft.h>
#include <brpc/server.h>
#include <butil/fd_guard.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <cerrno>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <future>
#include <mutex>
#include <string>
#include <thread>

#include "log_instance.h"
#include "log_state_rocksdb_impl.h"

/**
 * Makes the next pthread_create issued by this test thread fail with EAGAIN.
 * Thread-local so that brpc and RocksDB threads are unaffected by the injected
 * resource exhaustion.
 */
thread_local bool fail_snapshot_pthread = false;
namespace
{
/**
 * Handshake that stalls the snapshot worker inside the fsync of the snapshot
 * metadata file until the test releases it.
 *
 * Matches the metadata inode as well as the worker thread name: static braft
 * and RocksDB libraries also call the wrapped fsync, and they must not consume
 * this stall or make the shutdown test pass by blocking unrelated I/O.
 */
struct SnapshotSyncGate
{
    /**
     * @param path Path of the snapshot metadata file whose fsync is gated.
     */
    explicit SnapshotSyncGate(std::string path) : metadata_path(std::move(path))
    {
    }

    /**
     * @param fd Descriptor passed to fsync.
     * @return true when @p fd refers to the gated metadata file.
     */
    bool Matches(int fd) const
    {
        struct stat actual;
        struct stat expected;
        return fstat(fd, &actual) == 0 &&
               stat(metadata_path.c_str(), &expected) == 0 &&
               actual.st_dev == expected.st_dev &&
               actual.st_ino == expected.st_ino;
    }

    /** Blocks the calling worker until Release(), recording its thread kind. */
    void Pause()
    {
        std::unique_lock lock(mutex);
        on_pthread = bthread_self() == 0;
        paused = true;
        changed.notify_all();
        changed.wait(lock, [&] { return released; });
    }

    /**
     * Waits up to ten seconds for a worker to reach Pause().
     * @return true if a worker is paused, false on timeout.
     */
    bool WaitUntilPaused()
    {
        std::unique_lock lock(mutex);
        return changed.wait_for(
            lock, std::chrono::seconds(10), [&] { return paused; });
    }

    /** Lets a paused worker, or any later one, continue past the gate. */
    void Release()
    {
        std::lock_guard lock(mutex);
        released = true;
        changed.notify_all();
    }

    const std::string metadata_path;  ///< gated snapshot metadata file
    std::mutex mutex;                 ///< guards the flags below
    std::condition_variable changed;  ///< signalled on pause and release
    bool paused = false;              ///< a worker reached Pause()
    bool released = false;            ///< Release() has been called
    bool on_pthread = false;          ///< the paused worker was not a bthread
    std::atomic<bool> sync_completed{false};  ///< the gated fsync returned
};

/** Gate consulted by the fsync wrapper; null when no test is gating. */
std::atomic<SnapshotSyncGate *> snapshot_sync_gate{nullptr};

/**
 * While set, every fsync issued from the snapshot worker thread fails with
 * EIO. This makes LogStateRocksDBImpl::WriteSnapshot fail on the metadata
 * file after braft has already created the temp snapshot directory, which a
 * fault planted on disk beforehand cannot do: braft wipes that directory on
 * each attempt. fsyncs from braft's own threads are unaffected.
 */
std::atomic<bool> fail_snapshot_fsync{false};
}  // namespace

/** The real fsync, reached through the linker's --wrap=fsync. */
extern "C" int __real_fsync(int);

/**
 * Link-time replacement for fsync: on the snapshot worker thread it applies
 * the injected failure or the pause gate, otherwise it forwards unchanged.
 * @param fd Descriptor being synced.
 * @return fsync's result, or -1 with errno EIO while the failure is injected.
 */
extern "C" int __wrap_fsync(int fd)
{
    char name[16]{};
    pthread_getname_np(pthread_self(), name, sizeof(name));
    if (std::string(name) == "log_snapshot")
    {
        if (fail_snapshot_fsync.load())
        {
            errno = EIO;
            return -1;
        }
        auto *gate = snapshot_sync_gate.load();
        if (gate != nullptr && gate->Matches(fd))
        {
            gate->Pause();
            const int rc = __real_fsync(fd);
            gate->sync_completed.store(true);
            return rc;
        }
    }
    return __real_fsync(fd);
}
/** The real pthread_create, reached through the linker's --wrap. */
extern "C" int __real_pthread_create(pthread_t *,
                                     const pthread_attr_t *,
                                     void *(*) (void *),
                                     void *);

/**
 * Link-time replacement for pthread_create that fails once with EAGAIN when
 * fail_snapshot_pthread is set on the calling thread.
 * @return EAGAIN for the injected failure, otherwise pthread_create's result.
 */
extern "C" int __wrap_pthread_create(pthread_t *tid,
                                     const pthread_attr_t *attr,
                                     void *(*fn)(void *),
                                     void *arg)
{
    if (fail_snapshot_pthread)
    {
        fail_snapshot_pthread = false;
        return EAGAIN;
    }
    return __real_pthread_create(tid, attr, fn, arg);
}

namespace
{
namespace fs = std::filesystem;
using txlog::LogStateRocksDBImpl;
using namespace std::chrono_literals;
constexpr size_t kSstLimit = 64 * 1024 * 1024;

/** A fresh directory under /tmp that is removed with the object. */
struct TempDirectory
{
    TempDirectory()
    {
        char name[] = "/tmp/eloq-log-snapshot-XXXXXX";
        char *created = mkdtemp(name);
        CATCH_REQUIRE(created != nullptr);
        path = created;
    }
    ~TempDirectory()
    {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    fs::path path;  ///< the created directory
};

/**
 * A brpc server on an ephemeral loopback port with braft's services added.
 *
 * braft needs the concrete endpoint before brpc starts listening. The
 * constructor asks the kernel for an ephemeral port, then retries if another
 * process claims it in the short interval between releasing the reservation
 * and starting brpc.
 */
struct LoopbackRaftServer
{
    LoopbackRaftServer()
    {
        for (int attempt = 0; attempt < 10; ++attempt)
        {
            butil::fd_guard reservation(socket(AF_INET, SOCK_STREAM, 0));
            CATCH_REQUIRE(reservation >= 0);
            sockaddr_in address{};
            address.sin_family = AF_INET;
            address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            CATCH_REQUIRE(bind(reservation,
                               reinterpret_cast<sockaddr *>(&address),
                               sizeof(address)) == 0);
            socklen_t size = sizeof(address);
            CATCH_REQUIRE(getsockname(reservation,
                                      reinterpret_cast<sockaddr *>(&address),
                                      &size) == 0);
            port = ntohs(address.sin_port);
            endpoint = "127.0.0.1:" + std::to_string(port);
            server = std::make_unique<brpc::Server>();
            CATCH_REQUIRE(braft::add_service(server.get(), endpoint.c_str()) ==
                          0);
            reservation.reset(-1);
            if (server->Start(endpoint.c_str(), nullptr) == 0)
            {
                return;
            }
            server.reset();
        }
        CATCH_FAIL(
            "Could not start a loopback Raft server on an ephemeral port");
    }

    std::unique_ptr<brpc::Server> server;  ///< the running server
    std::string endpoint;                  ///< "127.0.0.1:<port>"
    int port = 0;                          ///< the bound port
};

/**
 * Runs a full snapshot cycle on a log state outside braft.
 * @param state State to snapshot.
 * @param path Directory to write into; created if missing.
 * @return The relative file names WriteSnapshot reported, empty on failure.
 */
std::vector<std::string> Save(LogStateRocksDBImpl &state, const fs::path &path)
{
    fs::create_directories(path);
    state.BeginSnapshot();
    auto files = state.WriteSnapshot(path);
    state.CleanSnapshotState();
    return files;
}

/**
 * @param files Snapshot file list.
 * @param name Relative file name to look for.
 * @return true if @p name is in @p files.
 */
bool Has(const std::vector<std::string> &files, const std::string &name)
{
    return std::find(files.begin(), files.end(), name) != files.end();
}

CATCH_TEST_CASE(
    "Empty log snapshot preserves metadata and replaces stale records")
{
    TempDirectory dir;
    LogStateRocksDBImpl source(dir.path / "source", kSstLimit, 1);
    CATCH_REQUIRE(source.Start() == 0);
    CATCH_REQUIRE(source.UpdateNgTerm(1, 7, "127.0.0.1", 9000));
    source.UpdateCkptTs(1, 40);
    txlog::SchemaOpMessage schema;
    schema.set_table_name_str("snapshot-test");
    schema.set_table_type(txlog::CcTableType::Primary);
    schema.set_stage(txlog::SchemaOpMessage::PrepareSchema);
    source.UpsertSchemaOp(123, 50, schema);
    auto files = Save(source, dir.path / "snapshot");
    CATCH_REQUIRE(Has(files, "data"));
    CATCH_REQUIRE(Has(files, "rocksdb_ckpt/CURRENT"));

    LogStateRocksDBImpl fresh(dir.path / "fresh", kSstLimit, 1);
    CATCH_REQUIRE(fresh.Start() == 0);
    CATCH_REQUIRE(fresh.ReadSnapshot(dir.path / "snapshot", files) == 0);
    CATCH_REQUIRE(fresh.GetNgLeaderTerm(1) == 7);

    {
        LogStateRocksDBImpl restored(dir.path / "restored", kSstLimit, 1);
        CATCH_REQUIRE(restored.Start() == 0);
        restored.AddLogItem(1, 99, 100, "stale record");
        CATCH_REQUIRE(restored.ReadSnapshot(dir.path / "snapshot", files) == 0);
        CATCH_REQUIRE(restored.GetNgLeaderTerm(1) == 7);
        CATCH_REQUIRE(restored.LastCkptTimestamp(1) == 40);
        CATCH_REQUIRE(restored.SearchTxSchemaLog(123).first);
        CATCH_REQUIRE(restored.SearchTxDataLog(99, 1).second == nullptr);
        auto second = Save(restored, dir.path / "second");
        CATCH_REQUIRE(Has(second, "rocksdb_ckpt/CURRENT"));
    }
    // Reopen the rebuilt DB and restore metadata just as Raft restart does.
    LogStateRocksDBImpl reopened(dir.path / "restored", kSstLimit, 1);
    CATCH_REQUIRE(reopened.Start() == 0);
    CATCH_REQUIRE(reopened.ReadSnapshot(dir.path / "snapshot", files) == 0);
    CATCH_REQUIRE(reopened.GetNgLeaderTerm(1) == 7);
    CATCH_REQUIRE(reopened.SearchTxSchemaLog(123).first);
    CATCH_REQUIRE(reopened.SearchTxDataLog(99, 1).second == nullptr);
}

CATCH_TEST_CASE("Nonempty log snapshots retain the legacy checkpoint format")
{
    TempDirectory dir;
    LogStateRocksDBImpl source(dir.path / "source", kSstLimit, 1);
    CATCH_REQUIRE(source.Start() == 0);
    source.AddLogItemBatch(1, 99, 100, "committed log");
    source.FlushLogItemBatch();
    auto files = Save(source, dir.path / "snapshot");
    CATCH_REQUIRE(Has(files, "rocksdb_ckpt/CURRENT"));
    LogStateRocksDBImpl restored(dir.path / "restored", kSstLimit, 1);
    CATCH_REQUIRE(restored.Start() == 0);
    CATCH_REQUIRE(restored.ReadSnapshot(dir.path / "snapshot", files) == 0);
    auto item = restored.SearchTxDataLog(99, 1).second;
    CATCH_REQUIRE(item != nullptr);
    CATCH_REQUIRE(item->log_message_ == "committed log");
}

CATCH_TEST_CASE(
    "Snapshot output failures return failure instead of partial success")
{
    TempDirectory dir;
    LogStateRocksDBImpl state(dir.path / "db", kSstLimit, 1);
    CATCH_REQUIRE(state.Start() == 0);
    fs::create_directories(dir.path / "snapshot");
    CATCH_SECTION("Metadata cannot be opened")
    {
        fs::create_directory(dir.path / "snapshot/data");
    }
    CATCH_SECTION("Checkpoint cannot be created, including for an empty DB")
    {
        // RocksDB uses this staging directory; an existing nonempty staging
        // subdirectory makes CreateCheckpoint fail without depending on chmod.
        fs::create_directories(dir.path / "snapshot/rocksdb_ckpt.tmp/blocker");
        std::ofstream(dir.path / "snapshot/rocksdb_ckpt.tmp/blocker/child")
            << "x";
    }
    CATCH_REQUIRE(Save(state, dir.path / "snapshot").empty());
    CATCH_REQUIRE_FALSE(Save(state, dir.path / "retry").empty());
}

/**
 * A braft snapshot writer that records registered files and the thread that
 * registered them, with an optional injected registration failure.
 */
class RecordingWriter : public braft::SnapshotWriter
{
public:
    explicit RecordingWriter(const fs::path &path) : path_(path)
    {
        fs::create_directories(path_);
    }
    std::string get_path() override
    {
        return path_;
    }
    void list_files(std::vector<std::string> *files) override
    {
        *files = files_;
    }
    int save_meta(const braft::SnapshotMeta &) override
    {
        return 0;
    }
    int remove_file(const std::string &) override
    {
        return 0;
    }
    int add_file(const std::string &file,
                 const google::protobuf::Message *) override
    {
        on_pthread &= bthread_self() == 0;
        char name[16]{};
        pthread_getname_np(pthread_self(), name, sizeof(name));
        named_worker &= std::string(name) == "log_snapshot";
        if (fail_registration)
        {
            return -1;
        }
        files_.push_back(file);
        return 0;
    }
    bool fail_registration = false;   ///< make add_file fail
    bool on_pthread = true;           ///< no add_file came from a bthread
    bool named_worker = true;         ///< every add_file came from the worker
    std::vector<std::string> files_;  ///< files registered so far

private:
    std::string path_;  ///< snapshot directory handed to the state machine
};

/** A braft closure that publishes its final status through a promise. */
class Completion : public braft::Closure
{
public:
    void Run() override
    {
        result.set_value(status());
    }
    std::promise<butil::Status> result;  ///< fulfilled with the status
};

CATCH_TEST_CASE(
    "LogInstance completes snapshots on a dedicated pthread and retries "
    "failures")
{
    TempDirectory dir;
    LoopbackRaftServer server;
    const auto &endpoint = server.endpoint;
    txlog::LogInstance instance(0,
                                0,
                                "127.0.0.1",
                                server.port,
                                endpoint + ":0",
                                "snapshot-test",
                                "local://" + (dir.path / "raft").string(),
#ifdef WITH_CLOUD_AZ_INFO
                                "",
                                "",
#endif
                                dir.path / "db",
                                1,
                                0,
                                8,
                                12,
                                64 * 1024 * 1024,
                                0,
                                false,
                                10,
                                128 * 1024 * 1024,
                                /*cdc_retention_seconds=*/0);
    CATCH_REQUIRE(instance.Start() == 0);
    braft::StateMachine &fsm = instance;
    for (int attempt = 0; attempt != 4; ++attempt)
    {
        RecordingWriter writer(dir.path / std::to_string(attempt));
        Completion done;
        auto result = done.result.get_future();
        if (attempt == 0)
        {
            fail_snapshot_pthread = true;
        }
        writer.fail_registration = attempt == 1;
        if (attempt == 2)
        {
            fs::create_directory(dir.path / "2/data");
        }
        fsm.on_snapshot_save(&writer, &done);
        CATCH_REQUIRE(result.wait_for(10s) == std::future_status::ready);
        auto status = result.get();
        if (attempt < 3)
        {
            CATCH_REQUIRE_FALSE(status.ok());
            // Only a pthread start failure reports its errno; write and
            // registration failures use EPERM so braft keeps the node alive.
            CATCH_REQUIRE(status.error_code() ==
                          (attempt == 0 ? EAGAIN : EPERM));
        }
        else
        {
            CATCH_REQUIRE(status.ok());
            CATCH_REQUIRE(writer.on_pthread);
            CATCH_REQUIRE(writer.named_worker);
            CATCH_REQUIRE(Has(writer.files_, "rocksdb_ckpt/CURRENT"));
        }
    }
    instance.shutdown();
    instance.join();
}

CATCH_TEST_CASE(
    "Raft shutdown waits for snapshot I/O and restart loads the empty snapshot")
{
    TempDirectory dir;
    LoopbackRaftServer server;
    const auto &endpoint = server.endpoint;
    auto make_instance = [&]
    {
        return std::make_unique<txlog::LogInstance>(
            0,
            0,
            "127.0.0.1",
            server.port,
            endpoint + ":0",
            "snapshot-shutdown-test",
            "local://" + (dir.path / "raft").string(),
#ifdef WITH_CLOUD_AZ_INFO
            "",
            "",
#endif
            dir.path / "db",
            1,
            0,
            8,
            12,
            64 * 1024 * 1024,
            0,
            false,
            10,
            128 * 1024 * 1024,
            /*cdc_retention_seconds=*/0);
    };
    auto instance = make_instance();
    CATCH_REQUIRE(instance->Start() == 0);
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!instance->is_leader() &&
           std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(10ms);
    }
    CATCH_REQUIRE(instance->is_leader());

    braft::PeerId peer;
    CATCH_REQUIRE(peer.parse(endpoint + ":0") == 0);
    braft::cli::CliOptions options;
    options.timeout_ms = 10000;
    options.max_retry = 0;
    SnapshotSyncGate gate((dir.path / "raft/snapshot/temp/data").string());
    snapshot_sync_gate.store(&gate);
    auto snapshot = std::async(std::launch::async,
                               [&] {
                                   return braft::cli::snapshot(
                                       "snapshot-shutdown-test", peer, options);
                               });
    const bool observed_pause = gate.WaitUntilPaused();
    instance->shutdown();
    std::promise<void> join_started;
    auto joining = join_started.get_future();
    auto joined = std::async(
        std::launch::async,
        [&]
        {
            join_started.set_value();
            instance->join();
            // Check the state at join's return, before waiting for the snapshot
            // RPC. Publication and metadata I/O must already be complete then.
            bool published = false;
            for (const auto &entry :
                 fs::directory_iterator(dir.path / "raft/snapshot"))
            {
                if (entry.path().filename() != "temp" &&
                    fs::exists(entry.path() / "rocksdb_ckpt/CURRENT"))
                {
                    published = true;
                }
            }
            return gate.sync_completed.load() && published;
        });
    joining.wait();
    // The handshake arranges shutdown with snapshot I/O in flight. Assert
    // completion ordering, without requiring join to take a minimum duration.
    // Release before assertions or future cleanup, including on a pause
    // timeout.
    gate.Release();
    const bool completed_before_join_returned = joined.get();
    auto status = snapshot.get();
    snapshot_sync_gate.store(nullptr);
    CATCH_REQUIRE(observed_pause);
    CATCH_REQUIRE(gate.on_pthread);
    CATCH_REQUIRE(completed_before_join_returned);
    CATCH_REQUIRE(status.ok());
    instance.reset();

    bool has_checkpoint_snapshot = false;
    for (const auto &entry : fs::directory_iterator(dir.path / "raft/snapshot"))
    {
        if (fs::exists(entry.path() / "rocksdb_ckpt/CURRENT"))
        {
            has_checkpoint_snapshot = true;
            CATCH_REQUIRE(fs::exists(entry.path() / "data"));
        }
    }
    CATCH_REQUIRE(has_checkpoint_snapshot);
    // Start goes through braft's actual on_snapshot_load callback.
    instance = make_instance();
    CATCH_REQUIRE(instance->Start() == 0);
    instance->shutdown();
    instance->join();
}

constexpr uint32_t kNgId = 1;   ///< cc node group used by the write tests
constexpr int64_t kNgTerm = 7;  ///< term registered for kNgId via raft

/**
 * Proposes a committed data log through the leader's public WriteLog path.
 * The write passes the term checks in LogInstance::execute only while the
 * node is still the leader and kNgId's term is kNgTerm in the log state.
 * @param instance The log group leader.
 * @param txn Low bits of the tx number; also used as the commit timestamp.
 * @return The response status reported by the raft-applied write.
 */
txlog::LogResponse::ResponseStatus WriteDataLog(txlog::LogInstance &instance,
                                                uint64_t txn)
{
    txlog::LogRequest request;
    txlog::WriteLogRequest *write = request.mutable_write_log_request();
    // execute() derives the sending ng id from bits 42.. of the tx number.
    write->set_txn_number((uint64_t{kNgId} << 42) | txn);
    write->set_commit_timestamp(txn);
    write->set_tx_term(kNgTerm);
    (*write->mutable_node_terms())[kNgId] = kNgTerm;
    (*write->mutable_log_content()
          ->mutable_data_log()
          ->mutable_node_txn_logs())[kNgId] =
        "committed " + std::to_string(txn);
    write->set_log_group_id(0);
    txlog::LogResponse response;
    instance.WriteLog(&request, &response, nullptr);
    return response.response_status();
}

/**
 * Counts the published (non-temp) braft snapshots that carry a RocksDB
 * checkpoint.
 * @param raft_path The braft storage directory of the instance.
 * @return The number of published snapshots with rocksdb_ckpt/CURRENT.
 */
int PublishedCheckpointSnapshots(const fs::path &raft_path)
{
    int count = 0;
    std::error_code error;
    for (const auto &entry :
         fs::directory_iterator(raft_path / "snapshot", error))
    {
        if (entry.path().filename() != "temp" &&
            fs::exists(entry.path() / "rocksdb_ckpt/CURRENT"))
        {
            ++count;
        }
    }
    return count;
}

CATCH_TEST_CASE(
    "A failed snapshot leaves the raft node serving writes and a later "
    "snapshot succeeds without restart")
{
    TempDirectory dir;
    LoopbackRaftServer server;
    const auto &endpoint = server.endpoint;
    const std::string group = "snapshot-retry-test";
    txlog::LogInstance instance(0,
                                0,
                                "127.0.0.1",
                                server.port,
                                endpoint + ":0",
                                group,
                                "local://" + (dir.path / "raft").string(),
#ifdef WITH_CLOUD_AZ_INFO
                                "",
                                "",
#endif
                                dir.path / "db",
                                1,
                                0,
                                8,
                                12,
                                64 * 1024 * 1024,
                                0,
                                false,
                                10,
                                128 * 1024 * 1024,
                                /*cdc_retention_seconds=*/0);
    CATCH_REQUIRE(instance.Start() == 0);
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!instance.is_leader() && std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(10ms);
    }
    CATCH_REQUIRE(instance.is_leader());

    // Register kNgId's term through raft so that data-log writes pass the
    // term checks; no_replay skips shipping logs to a cc node.
    {
        txlog::LogRequest request;
        txlog::ReplayLogRequest *replay = request.mutable_replay_log_request();
        replay->set_cc_node_group_id(kNgId);
        replay->set_term(kNgTerm);
        replay->set_source_ip("127.0.0.1");
        replay->set_source_port(9000);
        replay->set_log_group_id(0);
        replay->set_no_replay(true);
        txlog::LogResponse response;
        instance.ReplayLog(&request, &response, nullptr);
        CATCH_REQUIRE(response.response_status() ==
                      txlog::LogResponse::Success);
    }
    CATCH_REQUIRE(WriteDataLog(instance, 1) == txlog::LogResponse::Success);

    braft::PeerId peer;
    CATCH_REQUIRE(peer.parse(endpoint + ":0") == 0);
    braft::cli::CliOptions options;
    options.timeout_ms = 10000;
    options.max_retry = 0;
    const fs::path raft_path = dir.path / "raft";
    CATCH_REQUIRE(PublishedCheckpointSnapshots(raft_path) == 0);

    // Inject the fault for one attempt. braft runs the real on_snapshot_save,
    // on_snapshot_save_done and writer close paths; the CLI returns the
    // status the save reported.
    fail_snapshot_fsync.store(true);
    const butil::Status failed = braft::cli::snapshot(group, peer, options);
    fail_snapshot_fsync.store(false);
    CATCH_REQUIRE_FALSE(failed.ok());
    CATCH_REQUIRE(failed.error_code() == EPERM);
    CATCH_REQUIRE(PublishedCheckpointSnapshots(raft_path) == 0);

    // EIO would have made braft call on_error, step the node down and reject
    // this write with NotLeader. EPERM must leave the leader serving.
    CATCH_REQUIRE(instance.is_leader());
    CATCH_REQUIRE(WriteDataLog(instance, 2) == txlog::LogResponse::Success);

    // With the fault removed, the next attempt publishes a full snapshot on
    // the same node, and writes keep succeeding afterwards.
    const butil::Status retried = braft::cli::snapshot(group, peer, options);
    CATCH_REQUIRE(retried.ok());
    CATCH_REQUIRE(PublishedCheckpointSnapshots(raft_path) == 1);
    CATCH_REQUIRE(WriteDataLog(instance, 3) == txlog::LogResponse::Success);

    instance.shutdown();
    instance.join();
}
}  // namespace
