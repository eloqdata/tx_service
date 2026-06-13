#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>

#include "eloq_data_store_service/data_store.h"

namespace EloqDS
{
// In-memory EloqDS::DataStore backend for unit tests. Executes all requests
// synchronously. Replaces the assert(false) stub IntMemoryStore by going
// through the real DataStoreService/DataStoreServiceClient path.
class MemDataStore : public DataStore
{
public:
    MemDataStore(uint32_t shard_id, DataStoreService *data_store_service)
        : DataStore(shard_id, data_store_service)
    {
    }

    bool Initialize() override;

#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
    bool StartDB(int64_t term, uint32_t shard_id) override;
#else
    bool StartDB(int64_t term) override;
#endif

    void Shutdown() override;

    void Read(ReadRequest *read_req) override;
    void BatchWriteRecords(WriteRecordsRequest *batch_write_req) override;
    void FlushData(FlushDataRequest *flush_data_req) override;
    void DeleteRange(DeleteRangeRequest *delete_range_req) override;
    void CreateTable(CreateTableRequest *create_table_req) override;
    void DropTable(DropTableRequest *drop_table_req) override;
    void ScanNext(ScanRequest *scan_req) override;
    void ScanClose(ScanRequest *scan_req) override;
    void CreateSnapshotForBackup(CreateSnapshotForBackupRequest *req) override;
    void SwitchToReadOnly() override;
    void SwitchToReadWrite() override;

    uint64_t ApproxStoreKeyCount() override;

private:
    struct Value
    {
        std::string record_;
        uint64_t ts_{0};
        uint64_t ttl_{0};
    };

    // table_name -> partition_id -> key -> Value
    using PartitionMap = std::map<std::string, Value>;
    using TableMap = std::map<int32_t, PartitionMap>;

    std::mutex mux_;
    std::map<std::string, TableMap> store_;
    bool read_only_{false};
};
}  // namespace EloqDS
