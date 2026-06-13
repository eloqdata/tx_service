#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <catch2/catch_all.hpp>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "eloq_data_store_service/data_store.h"
#include "eloq_data_store_service/data_store_service.h"
#include "eloq_data_store_service/data_store_service_util.h"
#include "eloq_data_store_service/internal_request.h"
#include "google/protobuf/service.h"
#include "harness/mem_data_store.h"
#include "harness/mem_data_store_factory.h"

using namespace EloqDS;

namespace
{
// Picks an ephemeral TCP port by binding to 0, reading back the assigned port,
// then closing the socket. DataStoreService::StartService() binds brpc to
// cluster_manager_.GetThisNode().port_, so we hand it a port that was free a
// moment ago instead of a hard-coded one (avoids collisions across the suite).
uint16_t PickFreePort()
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(fd >= 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;  // let the kernel choose
    REQUIRE(::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0);
    socklen_t len = sizeof(addr);
    REQUIRE(::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) == 0);
    uint16_t port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

// MemDataStore is synchronous: it calls SetFinish (and therefore done->Run())
// inline before the Read/BatchWriteRecords/ScanNext call returns. The
// *LocalRequest::SetFinish wraps done_ in a brpc::ClosureGuard, so done_ may be
// null, but we pass a real closure anyway to mirror production.
class NoopClosure : public google::protobuf::Closure
{
public:
    void Run() override
    {
        ran_ = true;
    }
    bool ran_{false};
};

// Stands up a single-shard, in-process DataStoreService backed by MemDataStore
// and drives it through the production local request path. A single-node
// topology (Topology::InitWithSingleNode via
// DataStoreServiceClusterManager::Initialize) owns shard 0 in ReadWrite status;
// the default ShardingAlgorithm maps every partition to shard 0.
//
// The test deliberately goes through DataStoreService's *public local
// overloads* (Read/BatchWriteRecords/ScanNext). Those allocate the matching
// *LocalRequest from a thread_local ObjectPool and hand it to the backend,
// which is responsible for releasing it. MemDataStore now frees the request via
// PoolableGuard (mirroring RocksDBDataStoreCommon); if it did not, the pool's
// destructor would spin forever at thread exit. Exercising this path is exactly
// what proves the fix.
struct ServiceFixture
{
    static constexpr uint32_t kShardId = 0;

    ServiceFixture()
    {
        dir_ = std::filesystem::temp_directory_path() /
               ("memds_test_" + std::to_string(::getpid()));
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);

        // A probed-free port can be lost to a concurrent process between the
        // probe and the actual bind (TOCTOU). Retry StartService with a fresh
        // port instead of failing the test, so parallel runs don't flake.
        constexpr int kMaxBindRetries = 16;
        bool started = false;
        for (int attempt = 0; attempt < kMaxBindRetries && !started; ++attempt)
        {
            cluster_mgr_.Initialize("127.0.0.1", PickFreePort());
            auto factory = std::make_unique<MemDataStoreFactory>();
            service_ = std::make_unique<DataStoreService>(
                cluster_mgr_,
                (dir_ / "dss_config.ini").string(),
                (dir_ / "DSMigrateLog").string(),
                std::move(factory));
            started = service_->StartService(/*create_db_if_missing=*/true);
            if (!started)
            {
                service_.reset();
            }
        }
        REQUIRE(started);
        REQUIRE(service_->GetClusterManager().IsOwnerOfShard(kShardId));
    }

    ~ServiceFixture()
    {
        // DataStoreService has no public Shutdown(); teardown happens in its
        // destructor (stops the brpc server, joins, shuts down each shard).
        service_.reset();
        std::filesystem::remove_all(dir_);
    }

    DataStoreService &service()
    {
        return *service_;
    }

    std::filesystem::path dir_;
    DataStoreServiceClusterManager cluster_mgr_;
    std::unique_ptr<DataStoreService> service_;
};

constexpr int kNoError = static_cast<int>(remote::DataStoreError::NO_ERROR);
constexpr int kKeyNotFound =
    static_cast<int>(remote::DataStoreError::KEY_NOT_FOUND);

// Writes one record through the production DataStoreService::BatchWriteRecords
// local overload. The overload handles shard ownership / write-counter
// bookkeeping and allocates a pooled WriteRecordsLocalRequest that MemDataStore
// frees.
void Put(ServiceFixture &fx,
         std::string_view table,
         int32_t partition,
         std::string_view key,
         std::string_view value,
         uint64_t ts,
         remote::CommonResult &result)
{
    std::vector<std::string_view> key_parts{key};
    std::vector<std::string_view> rec_parts{value};
    std::vector<uint64_t> tss{ts};
    std::vector<uint64_t> ttls{0};
    std::vector<WriteOpType> ops{WriteOpType::PUT};
    NoopClosure done;

    fx.service().BatchWriteRecords(table,
                                   partition,
                                   ServiceFixture::kShardId,
                                   key_parts,
                                   rec_parts,
                                   tss,
                                   ttls,
                                   ops,
                                   /*skip_wal=*/true,
                                   result,
                                   &done,
                                   /*parts_cnt_per_key=*/1,
                                   /*parts_cnt_per_record=*/1);
}
}  // namespace

TEST_CASE("MemDataStore write then read round-trip", "[mem-data-store]")
{
    ServiceFixture fx;

    const std::string table = "test_table";
    const int32_t partition = 0;
    const std::string key = "k1";
    const std::string value = "v1";
    const uint64_t ts = 100;

    // 1. Write one record.
    {
        remote::CommonResult result;
        Put(fx, table, partition, key, value, ts, result);
        REQUIRE(result.error_code() == kNoError);
    }

    // 2. Read it back: value + ts must round-trip.
    {
        std::string out_record;
        uint64_t out_ts = 0;
        uint64_t out_ttl = 0;
        remote::CommonResult result;
        NoopClosure done;

        fx.service().Read(table,
                          partition,
                          ServiceFixture::kShardId,
                          key,
                          &out_record,
                          &out_ts,
                          &out_ttl,
                          &result,
                          &done);

        REQUIRE(result.error_code() == kNoError);
        REQUIRE(out_record == value);
        REQUIRE(out_ts == ts);
    }

    // 3. Missing key -> KEY_NOT_FOUND (table/partition already exist, so this
    //    proves the NOT_FOUND is key-specific).
    {
        std::string out_record = "sentinel";
        uint64_t out_ts = 999;
        uint64_t out_ttl = 0;
        remote::CommonResult result;
        NoopClosure done;

        fx.service().Read(table,
                          partition,
                          ServiceFixture::kShardId,
                          "missing",
                          &out_record,
                          &out_ts,
                          &out_ttl,
                          &result,
                          &done);

        REQUIRE(result.error_code() == kKeyNotFound);
    }
}

TEST_CASE("MemDataStore forward scan returns keys in order", "[mem-data-store]")
{
    ServiceFixture fx;

    const std::string table = "scan_table";
    const int32_t partition = 0;

    // Insert out of order; the store is a sorted map, so a forward scan must
    // return ascending.
    const std::vector<std::pair<std::string, std::string>> kvs = {
        {"b", "vb"}, {"d", "vd"}, {"a", "va"}, {"c", "vc"}};
    for (const auto &[k, v] : kvs)
    {
        remote::CommonResult result;
        Put(fx, table, partition, k, v, /*ts=*/200, result);
        REQUIRE(result.error_code() == kNoError);
    }

    std::vector<ScanTuple> items;
    std::string session_id;
    remote::CommonResult result;
    NoopClosure done;

    fx.service().ScanNext(table,
                          partition,
                          ServiceFixture::kShardId,
                          /*start_key=*/"",
                          /*end_key=*/"",
                          /*inclusive_start=*/true,
                          /*inclusive_end=*/true,
                          /*scan_forward=*/true,
                          /*batch_size=*/100,
                          /*search_conditions=*/nullptr,
                          &items,
                          &session_id,
                          /*generate_session_id=*/false,
                          &result,
                          &done);

    REQUIRE(result.error_code() == kNoError);
    REQUIRE(items.size() == kvs.size());
    REQUIRE(items[0].key_ == "a");
    REQUIRE(items[1].key_ == "b");
    REQUIRE(items[2].key_ == "c");
    REQUIRE(items[3].key_ == "d");
    REQUIRE(items[0].value_ == "va");
    REQUIRE(items[3].value_ == "vd");
}

// NOTE: no gflags::ParseCommandLineFlags here. catch_discover_tests runs this
// binary with Catch's own flags at build time; gflags strict-parse would abort
// on them and delete the executable.
int main(int argc, char **argv)
{
    return Catch::Session().run(argc, argv);
}
