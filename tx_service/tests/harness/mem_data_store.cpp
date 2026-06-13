#include "mem_data_store.h"

#include <brpc/closure_guard.h>

#include "eloq_data_store_service/data_store_service.h"
#include "eloq_data_store_service/internal_request.h"

namespace EloqDS
{
bool MemDataStore::Initialize()
{
    return true;
}

#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
bool MemDataStore::StartDB(int64_t, uint32_t)
{
    return true;
}
#else
bool MemDataStore::StartDB(int64_t)
{
    return true;
}
#endif

void MemDataStore::Shutdown()
{
}

uint64_t MemDataStore::ApproxStoreKeyCount()
{
    std::lock_guard<std::mutex> lk(mux_);
    uint64_t count = 0;
    for (const auto &[table, parts] : store_)
    {
        for (const auto &[pid, keys] : parts)
        {
            count += keys.size();
        }
    }
    return count;
}

void MemDataStore::Read(ReadRequest *req)
{
    // Release the pooled request to its thread_local ObjectPool on every return
    // path, after SetFinish has run (PoolableGuard destructs at method exit).
    // Mirrors RocksDBDataStoreCommon::Read. Without this the pool's destructor
    // spins forever at thread exit because the request stays InUse().
    PoolableGuard poolable_guard(req);

    std::string table(req->GetTableName());
    int32_t pid = req->GetPartitionId();
    std::string key(req->GetKey());

    std::lock_guard<std::mutex> lk(mux_);
    auto t_it = store_.find(table);
    if (t_it != store_.end())
    {
        auto p_it = t_it->second.find(pid);
        if (p_it != t_it->second.end())
        {
            auto k_it = p_it->second.find(key);
            if (k_it != p_it->second.end())
            {
                req->SetRecord(std::string(k_it->second.record_));
                req->SetRecordTs(k_it->second.ts_);
                req->SetRecordTtl(k_it->second.ttl_);
                req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
                return;
            }
        }
    }
    req->SetRecord("");
    req->SetRecordTs(0);
    req->SetRecordTtl(0);
    req->SetFinish(::EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
}

void MemDataStore::BatchWriteRecords(WriteRecordsRequest *req)
{
    PoolableGuard poolable_guard(req);

    // Set the result under the lock but call SetFinish (which runs the request
    // closure) only after releasing mux_, so the closure never executes under
    // the store mutex (a re-entrancy / lock-ordering hazard).
    ::EloqDS::remote::CommonResult result;
    {
        std::lock_guard<std::mutex> lk(mux_);
        if (read_only_)
        {
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
        }
        else
        {
            std::string table(req->GetTableName());
            int32_t pid = req->GetPartitionId();
            const uint16_t parts_per_key = req->PartsCountPerKey();
            const uint16_t parts_per_rec = req->PartsCountPerRecord();

            auto &partition = store_[table][pid];
            for (size_t i = 0; i < req->RecordsCount(); i++)
            {
                std::string key;
                for (uint16_t kp = 0; kp < parts_per_key; kp++)
                {
                    key.append(req->GetKeyPart(i * parts_per_key + kp));
                }

                if (req->KeyOpType(i) == WriteOpType::DELETE)
                {
                    partition.erase(key);
                }
                else
                {
                    std::string rec;
                    for (uint16_t rp = 0; rp < parts_per_rec; rp++)
                    {
                        rec.append(req->GetRecordPart(i * parts_per_rec + rp));
                    }
                    Value &v = partition[key];
                    v.record_ = std::move(rec);
                    v.ts_ = req->GetRecordTs(i);
                    v.ttl_ = req->GetRecordTtl(i);
                }
            }
            result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
        }
    }
    req->SetFinish(result);
}

void MemDataStore::FlushData(FlushDataRequest *req)
{
    PoolableGuard poolable_guard(req);

    ::EloqDS::remote::CommonResult result;
    result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
    req->SetFinish(result);
}

void MemDataStore::DeleteRange(DeleteRangeRequest *req)
{
    PoolableGuard poolable_guard(req);

    // Set the result under the lock but call SetFinish only after releasing
    // mux_, so the request closure never runs under the store mutex.
    ::EloqDS::remote::CommonResult result;
    {
        std::string table(req->GetTableName());
        int32_t pid = req->GetPartitionId();
        std::string start(req->GetStartKey());
        std::string end(req->GetEndKey());

        std::lock_guard<std::mutex> lk(mux_);
        if (read_only_)
        {
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
        }
        else
        {
            auto t_it = store_.find(table);
            if (t_it != store_.end())
            {
                auto p_it = t_it->second.find(pid);
                if (p_it != t_it->second.end())
                {
                    auto &keys = p_it->second;
                    auto lo =
                        start.empty() ? keys.begin() : keys.lower_bound(start);
                    auto hi = end.empty() ? keys.end() : keys.lower_bound(end);
                    keys.erase(lo, hi);
                }
            }
            result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
        }
    }
    req->SetFinish(result);
}

void MemDataStore::CreateTable(CreateTableRequest *req)
{
    PoolableGuard poolable_guard(req);

    // Set the result under the lock but call SetFinish only after releasing
    // mux_, so the request closure never runs under the store mutex.
    ::EloqDS::remote::CommonResult result;
    {
        std::lock_guard<std::mutex> lk(mux_);
        if (read_only_)
        {
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
        }
        else
        {
            store_.try_emplace(std::string(req->GetTableName()));
            result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
        }
    }
    req->SetFinish(result);
}

void MemDataStore::DropTable(DropTableRequest *req)
{
    PoolableGuard poolable_guard(req);

    // Set the result under the lock but call SetFinish only after releasing
    // mux_, so the request closure never runs under the store mutex.
    ::EloqDS::remote::CommonResult result;
    {
        std::lock_guard<std::mutex> lk(mux_);
        if (read_only_)
        {
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
        }
        else
        {
            store_.erase(std::string(req->GetTableName()));
            result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
        }
    }
    req->SetFinish(result);
}

void MemDataStore::ScanNext(ScanRequest *req)
{
    // The scan is stateless: each ScanNext is self-contained (the client
    // re-issues with a new start key for the next batch), so there is no
    // persistent iterator to keep alive and the request is freed per-call,
    // after SetFinish. RocksDBDataStoreCommon::ScanNext frees its scan request
    // the same way (PoolableGuard at the top of the worker lambda); it merely
    // keeps a separate rocksdb iterator session in the service, which the
    // stateless MemDataStore does not have.
    PoolableGuard poolable_guard(req);

    std::string table(req->GetTableName());
    int32_t pid = req->GetPartitionId();
    std::string start(req->GetStartKey());
    std::string end(req->GetEndKey());
    bool inclusive_start = req->InclusiveStart();
    bool inclusive_end = req->InclusiveEnd();
    bool scan_forward = req->ScanForward();
    uint32_t batch = req->BatchSize();

    // Continuation differs from production. The real DataStoreService keeps a
    // server-side iterator alive across batches and resumes it via a session_id
    // (stored in scan_iter_cache_), so it does not re-seek per call. This
    // MemDataStore instead clears the session and re-positions by key each
    // call: when a batch returns BatchSize() items, the client re-issues
    // ScanNext with start_key set to the last returned key
    // (inclusive_start=false), and the backend re-seeks the ordered map from
    // there. The scan is treated as drained once fewer than BatchSize() items
    // come back. Re-seeking an in-memory std::map per batch is a valid
    // simplification (no persistent iterator/session to keep alive).
    req->ClearSessionId();

    std::lock_guard<std::mutex> lk(mux_);
    auto t_it = store_.find(table);
    if (t_it == store_.end())
    {
        req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
        return;
    }
    auto p_it = t_it->second.find(pid);
    if (p_it == t_it->second.end())
    {
        req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
        return;
    }

    auto &keys = p_it->second;
    uint32_t emitted = 0;

    if (scan_forward)
    {
        // Forward scan: start is the lower bound, end is the upper bound.
        auto it = start.empty() ? keys.begin() : keys.lower_bound(start);
        if (!inclusive_start && it != keys.end() && it->first == start)
        {
            ++it;
        }
        for (; it != keys.end() && emitted < batch; ++it)
        {
            if (!end.empty())
            {
                if (it->first > end || (it->first == end && !inclusive_end))
                {
                    break;
                }
            }
            std::string k = it->first;
            std::string v = it->second.record_;
            req->AddItem(
                std::move(k), std::move(v), it->second.ts_, it->second.ttl_);
            ++emitted;
        }
    }
    else
    {
        // Backward scan: start is the upper bound, end is the lower bound.
        // upper_bound(start) is the first key > start; the reverse_iterator
        // built from it points at the greatest key <= start.
        std::map<std::string, Value>::reverse_iterator it;
        if (start.empty())
        {
            it = keys.rbegin();
        }
        else
        {
            it = std::map<std::string, Value>::reverse_iterator(
                keys.upper_bound(start));
        }
        if (!inclusive_start && it != keys.rend() && it->first == start)
        {
            ++it;
        }
        for (; it != keys.rend() && emitted < batch; ++it)
        {
            if (!end.empty())
            {
                if (it->first < end || (it->first == end && !inclusive_end))
                {
                    break;
                }
            }
            std::string k = it->first;
            std::string v = it->second.record_;
            req->AddItem(
                std::move(k), std::move(v), it->second.ts_, it->second.ttl_);
            ++emitted;
        }
    }

    req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
}

void MemDataStore::ScanClose(ScanRequest *req)
{
    PoolableGuard poolable_guard(req);

    req->ClearSessionId();
    req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
}

void MemDataStore::CreateSnapshotForBackup(CreateSnapshotForBackupRequest *req)
{
    PoolableGuard poolable_guard(req);

    // No snapshot artifacts for the in-memory store; backup paths are out of
    // Phase 1 scope. Still finish the request so the closure runs and the
    // write-request counter is decremented.
    req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
}

void MemDataStore::SwitchToReadOnly()
{
    std::lock_guard<std::mutex> lk(mux_);
    read_only_ = true;
}

void MemDataStore::SwitchToReadWrite()
{
    std::lock_guard<std::mutex> lk(mux_);
    read_only_ = false;
}
}  // namespace EloqDS
