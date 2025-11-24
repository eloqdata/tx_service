#include "rocksdb_data_store_common.h"

#include <filesystem>

#include "internal_request.h"

namespace EloqDS
{

bool TTLCompactionFilter::Filter(int level,
                                 const rocksdb::Slice &key,
                                 const rocksdb::Slice &existing_value,
                                 std::string *new_value,
                                 bool *value_changed) const
{
    assert(existing_value.size() >= sizeof(uint64_t));
    bool has_ttl = false;
    uint64_t ts = *(reinterpret_cast<const uint64_t *>(existing_value.data() +
                                                       sizeof(uint64_t)));

    // Check if the MSB is set
    if (ts & MSB)
    {
        has_ttl = true;
        ts &= MSB_MASK;  // Clear the MSB
    }
    else
    {
        has_ttl = false;
    }

    if (has_ttl)
    {
        assert(existing_value.size() >= sizeof(uint64_t) * 2);
        uint64_t rec_ttl = *(reinterpret_cast<const uint64_t *>(
            existing_value.data() + sizeof(uint64_t)));
        // Get the current timestamp in microseconds
        // auto current_timestamp =
        // txservice::LocalCcShards::ClockTsInMillseconds();
        // FIXME(lzx): only fetch the time at the begin of compaction.
        uint64_t current_timestamp =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();

        // Check if the timestamp is smaller than the current timestamp
        if (rec_ttl < current_timestamp)
        {
            return true;  // Mark the key for deletion
        }
    }

    return false;  // Keep the key
}

const char *TTLCompactionFilter::Name() const
{
    return "TTLCompactionFilter";
}

void RocksDBEventListener::OnCompactionBegin(
    rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci)
{
    DLOG(INFO) << "Compaction begin, job_id: " << ci.job_id
               << " ,thread: " << ci.thread_id
               << " ,output_level: " << ci.output_level
               << " ,input_files_size: " << ci.input_files.size()
               << " ,compaction_reason: "
               << static_cast<int>(ci.compaction_reason);
}

void RocksDBEventListener::OnCompactionCompleted(
    rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci)
{
    DLOG(INFO) << "Compaction end, job_id: " << ci.job_id
               << " ,thread: " << ci.thread_id
               << " ,output_level: " << ci.output_level
               << " ,input_files_size: " << ci.input_files.size()
               << " ,compaction_reason: "
               << static_cast<int>(ci.compaction_reason);
}

void RocksDBEventListener::OnFlushBegin(
    rocksdb::DB *db, const rocksdb::FlushJobInfo &flush_job_info)
{
    if (flush_job_info.triggered_writes_slowdown ||
        flush_job_info.triggered_writes_stop)
    {
        LOG(INFO) << "Flush begin, file: " << flush_job_info.file_path
                  << " ,job_id: " << flush_job_info.job_id
                  << " ,thread: " << flush_job_info.thread_id
                  << " ,file_number: " << flush_job_info.file_number
                  << " ,triggered_writes_slowdown: "
                  << flush_job_info.triggered_writes_slowdown
                  << " ,triggered_writes_stop: "
                  << flush_job_info.triggered_writes_stop
                  << " ,smallest_seqno: " << flush_job_info.smallest_seqno
                  << " ,largest_seqno: " << flush_job_info.largest_seqno
                  << " ,flush_reason: "
                  << GetFlushReason(flush_job_info.flush_reason);
    }
}

void RocksDBEventListener::OnFlushCompleted(
    rocksdb::DB *db, const rocksdb::FlushJobInfo &flush_job_info)
{
    if (flush_job_info.triggered_writes_slowdown ||
        flush_job_info.triggered_writes_stop)
    {
        LOG(INFO) << "Flush end, file: " << flush_job_info.file_path
                  << " ,job_id: " << flush_job_info.job_id
                  << " ,thread: " << flush_job_info.thread_id
                  << " ,file_number: " << flush_job_info.file_number
                  << " ,triggered_writes_slowdown: "
                  << flush_job_info.triggered_writes_slowdown
                  << " ,triggered_writes_stop: "
                  << flush_job_info.triggered_writes_stop
                  << " ,smallest_seqno: " << flush_job_info.smallest_seqno
                  << " ,largest_seqno: " << flush_job_info.largest_seqno
                  << " ,flush_reason: "
                  << GetFlushReason(flush_job_info.flush_reason);
    }
}

std::string RocksDBEventListener::GetFlushReason(
    rocksdb::FlushReason flush_reason)
{
    switch (flush_reason)
    {
    case rocksdb::FlushReason::kOthers:
        return "kOthers";
    case rocksdb::FlushReason::kGetLiveFiles:
        return "kGetLiveFiles";
    case rocksdb::FlushReason::kShutDown:
        return "kShutDown";
    case rocksdb::FlushReason::kExternalFileIngestion:
        return "kExternalFileIngestion";
    case rocksdb::FlushReason::kManualCompaction:
        return "kManualCompaction";
    case rocksdb::FlushReason::kWriteBufferManager:
        return "kWriteBufferManager";
    case rocksdb::FlushReason::kWriteBufferFull:
        return "kWriteBufferFull";
    case rocksdb::FlushReason::kTest:
        return "kTest";
    case rocksdb::FlushReason::kDeleteFiles:
        return "kDeleteFiles";
    case rocksdb::FlushReason::kAutoCompaction:
        return "kAutoCompaction";
    case rocksdb::FlushReason::kManualFlush:
        return "kManualFlush";
    case rocksdb::FlushReason::kErrorRecovery:
        return "kErrorRecovery";
    case rocksdb::FlushReason::kErrorRecoveryRetryFlush:
        return "kErrorRecoveryRetryFlush";
    case rocksdb::FlushReason::kWalFull:
        return "kWalFull";
    default:
        return "unknown";
    }
}

bool RocksDBDataStoreCommon::Initialize()
{
    // before opening rocksdb, rocksdb_storage_path_ must exist, create it
    // if not exist
    std::error_code error_code;
    bool rocksdb_storage_path_exists =
        std::filesystem::exists(db_path_, error_code);
    if (error_code.value() != 0)
    {
        LOG(ERROR) << "unable to check rocksdb directory: " << db_path_
                   << ", error code: " << error_code.value()
                   << ", error message: " << error_code.message();
        return false;
    }
    if (!rocksdb_storage_path_exists)
    {
        std::filesystem::create_directories(db_path_, error_code);
        if (error_code.value() != 0)
        {
            LOG(ERROR) << "unable to create rocksdb directory: " << db_path_
                       << ", error code: " << error_code.value()
                       << ", error message: " << error_code.message();
            return false;
        }
    }

    // Cleanup previous snapshots
    rocksdb_storage_path_exists =
        std::filesystem::exists(ckpt_path_, error_code);
    if (error_code.value() != 0)
    {
        LOG(ERROR) << "unable to check rocksdb directory: " << ckpt_path_
                   << ", error code: " << error_code.value()
                   << ", error message: " << error_code.message();
        return false;
    }
    if (!rocksdb_storage_path_exists)
    {
        std::filesystem::create_directories(ckpt_path_, error_code);
        if (error_code.value() != 0)
        {
            LOG(ERROR) << "unable to create rocksdb directory: " << ckpt_path_
                       << ", error code: " << error_code.value()
                       << ", error message: " << error_code.message();
            return false;
        }
    }
    else
    {
        // clean up previous snapshots
        for (const auto &entry :
             std::filesystem::directory_iterator(ckpt_path_))
        {
            std::filesystem::remove_all(entry.path());
        }
    }

    rocksdb_storage_path_exists =
        std::filesystem::exists(received_snapshot_path_, error_code);
    if (error_code.value() != 0)
    {
        LOG(ERROR) << "unable to check rocksdb directory: "
                   << received_snapshot_path_
                   << ", error code: " << error_code.value()
                   << ", error message: " << error_code.message();
        return false;
    }
    if (!rocksdb_storage_path_exists)
    {
        std::filesystem::create_directories(received_snapshot_path_,
                                            error_code);
        if (error_code.value() != 0)
        {
            LOG(ERROR) << "unable to create rocksdb directory: "
                       << received_snapshot_path_
                       << ", error code: " << error_code.value()
                       << ", error message: " << error_code.message();
            return false;
        }
    }
    else
    {
        // clean up previous snapshots
        for (const auto &entry :
             std::filesystem::directory_iterator(received_snapshot_path_))
        {
            std::filesystem::remove_all(entry.path());
        }
    }

    rocksdb_storage_path_exists =
        std::filesystem::exists(backup_path_, error_code);
    if (error_code.value() != 0)
    {
        LOG(ERROR) << "unable to check rocksdb directory: " << backup_path_
                   << ", error code: " << error_code.value()
                   << ", error message: " << error_code.message();
        return false;
    }
    if (!rocksdb_storage_path_exists)
    {
        std::filesystem::create_directories(backup_path_, error_code);
        if (error_code.value() != 0)
        {
            LOG(ERROR) << "unable to create rocksdb directory: " << backup_path_
                       << ", error code: " << error_code.value()
                       << ", error message: " << error_code.message();
            return false;
        }
    }

    if (query_worker_pool_ == nullptr)
    {
       query_worker_pool_ =
            std::make_unique<ThreadWorkerPool>(query_worker_number_);
    }
    else
    {
        query_worker_pool_->Initialize();
    }

    return true;
}

void RocksDBDataStoreCommon::Shutdown()
{
    // shutdown query worker pool
    if (query_worker_pool_ != nullptr)
    {
        query_worker_pool_->Shutdown();
        // If the data store be reused, query_worker_pool_ will be re-created in
        // Initialize().
    }

    if (data_store_service_ != nullptr)
    {
        data_store_service_->ForceEraseScanIters(shard_id_);
    }
}

void RocksDBDataStoreCommon::FlushData(FlushDataRequest *flush_data_req)
{
    bool res = query_worker_pool_->SubmitWork(
        [this, flush_data_req]()
        {
            // Create a guard to ensure the poolable object is released to pool
            std::unique_ptr<PoolableGuard> poolable_guard =
                std::make_unique<PoolableGuard>(flush_data_req);

            ::EloqDS::remote::CommonResult result;
            std::shared_lock<std::shared_mutex> db_lk(db_mux_);
            auto db = GetDBPtr();
            if (!db)
            {
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
                result.set_error_msg("DB is not opened");
                flush_data_req->SetFinish(result);
                return;
            }

            rocksdb::FlushOptions flush_options;
            flush_options.allow_write_stall = true;
            flush_options.wait = true;

            auto status = db->Flush(flush_options);
            if (!status.ok())
            {
                LOG(ERROR) << "Unable to flush db with error: "
                           << status.ToString();
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::FLUSH_FAILED);
                result.set_error_msg(status.ToString());
                flush_data_req->SetFinish(result);
                return;
            }

            result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
            flush_data_req->SetFinish(result);
            DLOG(INFO) << "FlushData successfully.";
        });

    if (!res)
    {
        LOG(ERROR) << "Failed to submit flush data work to query worker pool";
        ::EloqDS::remote::CommonResult result;
        result.set_error_code(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        result.set_error_msg("DB is not opened");
        flush_data_req->SetFinish(result);

        flush_data_req->Clear();
        flush_data_req->Free();
        return;
    }
}

void RocksDBDataStoreCommon::DeleteRange(DeleteRangeRequest *delete_range_req)
{
    bool res = query_worker_pool_->SubmitWork(
        [this, delete_range_req]()
        {
            // Create a guard to ensure the poolable object is released to pool
            std::unique_ptr<PoolableGuard> poolable_guard =
                std::make_unique<PoolableGuard>(delete_range_req);

            ::EloqDS::remote::CommonResult result;
            std::shared_lock<std::shared_mutex> db_lk(db_mux_);
            auto db = GetDBPtr();
            if (!db)
            {
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
                result.set_error_msg("DB is not opened");
                delete_range_req->SetFinish(result);
                return;
            }

            const std::string_view table_name =
                delete_range_req->GetTableName();
            const uint32_t partition_id = delete_range_req->GetPartitionId();

            // build start key
            std::string start_key_str = BuildKey(
                table_name, partition_id, delete_range_req->GetStartKey());
            rocksdb::Slice start_key(start_key_str);

            // build end key
            std::string end_key_str;
            auto end_key_strv = delete_range_req->GetEndKey();
            if (end_key_strv.empty())
            {
                // If end_key is empty, delete all keys starting from start_key
                end_key_str = BuildKey(table_name, partition_id, "");
                end_key_str.back()++;
            }
            else
            {
                end_key_str = BuildKey(table_name, partition_id, end_key_strv);
            }
            rocksdb::Slice end_key(end_key_str);

            rocksdb::WriteOptions write_opts;
            write_opts.disableWAL = delete_range_req->SkipWal();
            auto status = db->DeleteRange(write_opts, start_key, end_key);
            if (!status.ok())
            {
                LOG(ERROR) << "Unable to delete range with error: "
                           << status.ToString();
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::WRITE_FAILED);
                result.set_error_msg(status.ToString());
                delete_range_req->SetFinish(result);
                return;
            }

            result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
            delete_range_req->SetFinish(result);
            DLOG(INFO) << "DeleteRange successfully.";
        });

    if (!res)
    {
        LOG(ERROR) << "Failed to submit delete range work to query worker pool";
        ::EloqDS::remote::CommonResult result;
        result.set_error_code(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        result.set_error_msg("DB is not opened");
        delete_range_req->SetFinish(result);
        delete_range_req->Clear();
        delete_range_req->Free();
        return;
    }
}

void RocksDBDataStoreCommon::Read(ReadRequest *req)
{
    bool res = query_worker_pool_->SubmitWork(
        [this, req]()
        {
            // Create a guard to ensure the poolable object is released to pool
            std::unique_ptr<PoolableGuard> poolable_guard =
                std::make_unique<PoolableGuard>(req);

            auto table_name = req->GetTableName();
            uint32_t partition_id = req->GetPartitionId();
            auto key = req->GetKey();

            std::shared_lock<std::shared_mutex> db_lk(db_mux_);

            auto *db = GetDBPtr();
            if (db == nullptr)
            {
                req->SetFinish(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
                return;
            }

            std::string key_str = this->BuildKey(table_name, partition_id, key);
            std::string value;
            rocksdb::ReadOptions read_options;
            rocksdb::Status status = db->Get(read_options, key_str, &value);

            if (status.ok())
            {
                std::string rec;
                uint64_t rec_ts;
                uint64_t rec_ttl;
                DeserializeValueToRecord(
                    value.data(), value.size(), rec, rec_ts, rec_ttl);
                req->SetRecord(std::move(rec));
                req->SetRecordTs(rec_ts);
                req->SetRecordTtl(rec_ttl);
                req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
            }
            else if (status.IsNotFound())
            {
                req->SetRecord("");
                req->SetRecordTs(0);
                req->SetRecordTtl(0);
                req->SetFinish(::EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
            }
            else
            {
                req->SetFinish(::EloqDS::remote::DataStoreError::READ_FAILED);
                LOG(ERROR) << "RocksdbCloud read key:" << key_str
                           << " failed, status:" << status.ToString();
            }
        });
    if (!res)
    {
        LOG(ERROR) << "Failed to submit read work to query worker pool";
        req->SetFinish(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        req->Clear();
        req->Free();
        return;
    }
}

void RocksDBDataStoreCommon::BatchWriteRecords(
    WriteRecordsRequest *batch_write_req)
{
    assert(batch_write_req != nullptr);
    if (batch_write_req->RecordsCount() == 0)
    {
        ::EloqDS::remote::CommonResult result;
        result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
        batch_write_req->SetFinish(result);
        batch_write_req->Clear();
        batch_write_req->Free();
        return;
    }

    bool res = query_worker_pool_->SubmitWork(
        [this, batch_write_req]() mutable
        {
            // Create a guard to ensure the poolable object is released to pool
            std::unique_ptr<PoolableGuard> poolable_guard =
                std::make_unique<PoolableGuard>(batch_write_req);

            auto shard_status = FetchDSShardStatus();
            ::EloqDS::remote::CommonResult result;
            if (shard_status != DSShardStatus::ReadWrite)
            {
                if (shard_status == DSShardStatus::Closed)
                {
                    result.set_error_code(::EloqDS::remote::DataStoreError::
                                              REQUESTED_NODE_NOT_OWNER);
                    result.set_error_msg("Requested data not on local node.");
                }
                else
                {
                    assert(shard_status == DSShardStatus::ReadOnly);
                    result.set_error_code(::EloqDS::remote::DataStoreError::
                                              WRITE_TO_READ_ONLY_DB);
                    result.set_error_msg("Write to read-only DB.");
                }
                batch_write_req->SetFinish(result);

                return;
            }

            std::shared_lock<std::shared_mutex> db_lk(db_mux_);
            auto db = GetDBPtr();
            if (!db)
            {
                DLOG(ERROR) << "DB is not opened";
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
                result.set_error_msg("DB is not opened");
                batch_write_req->SetFinish(result);
                return;
            }

            const uint16_t parts_cnt_per_key =
                batch_write_req->PartsCountPerKey();
            const uint16_t parts_cnt_per_record =
                batch_write_req->PartsCountPerRecord();
            const uint16_t key_parts_cnt =
                4 + parts_cnt_per_key;  // 4 for table name, partition id and
                                        // two separators
            const uint16_t record_parts_cnt =
                2 + parts_cnt_per_record;  // 2 for timestamp and ttl
            rocksdb::WriteOptions write_options;
            write_options.disableWAL = true;
            rocksdb::WriteBatch write_batch;

            std::unique_ptr<rocksdb::Slice[]> key_slices =
                std::make_unique<rocksdb::Slice[]>(key_parts_cnt);
            rocksdb::SliceParts key_parts(key_slices.get(), key_parts_cnt);
            std::string partition_id_str =
                std::to_string(batch_write_req->GetPartitionId());
            BuildKeyPrefixSlices(
                batch_write_req->GetTableName(), partition_id_str, key_slices);

            std::unique_ptr<rocksdb::Slice[]> value_slices =
                std::make_unique<rocksdb::Slice[]>(record_parts_cnt);
            for (size_t i = 0; i < batch_write_req->RecordsCount(); i++)
            {
                BuildKeySlices(
                    key_slices, batch_write_req, i, parts_cnt_per_key);

                // TODO(lzx):enable rocksdb user-defined-timestamp?
                if (batch_write_req->KeyOpType(i) == WriteOpType::DELETE)
                {
                    write_batch.Delete(key_parts);
                }
                else
                {
                    assert(batch_write_req->KeyOpType(i) == WriteOpType::PUT);
                    uint64_t rec_ts = batch_write_req->GetRecordTs(i);
                    uint64_t rec_ttl = batch_write_req->GetRecordTtl(i);
                    const uint16_t value_parts_size =
                        TransformRecordToValueSlices(batch_write_req,
                                                     i,
                                                     parts_cnt_per_record,
                                                     rec_ts,
                                                     rec_ttl,
                                                     value_slices);
                    rocksdb::SliceParts value_parts(value_slices.get(),
                                                    value_parts_size);
                    write_batch.Put(key_parts, value_parts);
                }

                // keep key prefix, and clear key part
                for (uint16_t i = 0; i < parts_cnt_per_key; i++)
                {
                    key_slices[4 + i].clear();
                }
                // clear value part
                for (uint16_t i = 0; i < record_parts_cnt; i++)
                {
                    value_slices[i].clear();
                }
            }

            auto write_status = db->Write(write_options, &write_batch);

            if (!write_status.ok())
            {
                LOG(ERROR) << "BatchWriteRecords failed, table:"
                           << batch_write_req->GetTableName()
                           << ", result:" << static_cast<int>(write_status.ok())
                           << ", error: " << write_status.ToString()
                           << ", error code: " << write_status.code();
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::WRITE_FAILED);
                result.set_error_msg(write_status.ToString());
            }
            else if (!batch_write_req->SkipWal())
            {
                rocksdb::FlushOptions flush_options;
                flush_options.wait = true;
                auto flush_status = db->Flush(flush_options);
                if (!flush_status.ok())
                {
                    LOG(ERROR)
                        << "Flush failed after BatchWriteRecords, error: "
                        << flush_status.ToString();
                    result.set_error_code(
                        ::EloqDS::remote::DataStoreError::FLUSH_FAILED);
                    result.set_error_msg(flush_status.ToString());
                }
            }

            result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
            batch_write_req->SetFinish(result);
        });

    if (!res)
    {
        LOG(ERROR) << "Failed to submit batch write work to query worker pool";
        ::EloqDS::remote::CommonResult result;
        result.set_error_code(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        result.set_error_msg("DB is not opened");
        batch_write_req->SetFinish(result);

        batch_write_req->Clear();
        batch_write_req->Free();
        return;
    }
}

void RocksDBDataStoreCommon::CreateTable(CreateTableRequest *create_table_req)
{
    std::unique_ptr<PoolableGuard> poolable_guard =
        std::make_unique<PoolableGuard>(create_table_req);

    ::EloqDS::remote::CommonResult result;
    result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
    create_table_req->SetFinish(result);
    return;
}

void RocksDBDataStoreCommon::DropTable(DropTableRequest *drop_table_req)
{
    bool res = query_worker_pool_->SubmitWork(
        [this, drop_table_req]()
        {
            // Create a guard to ensure the poolable object is released to pool
            std::unique_ptr<PoolableGuard> poolable_guard =
                std::make_unique<PoolableGuard>(drop_table_req);

            ::EloqDS::remote::CommonResult result;
            std::shared_lock<std::shared_mutex> db_lk(db_mux_);
            auto db = GetDBPtr();
            if (!db)
            {
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
                result.set_error_msg("DB is not opened");
                drop_table_req->SetFinish(result);
                return;
            }

            const std::string_view table_name = drop_table_req->GetTableName();

            // build start key
            std::string start_key_str = BuildKey(table_name, 0, "");
            rocksdb::Slice start_key(start_key_str);

            // build end key
            std::string end_key_str = BuildKey(table_name, UINT32_MAX, "");
            end_key_str.back()++;
            rocksdb::Slice end_key(end_key_str);

            rocksdb::WriteOptions write_opts;
            write_opts.disableWAL = true;
            auto status = db->DeleteRange(write_opts, start_key, end_key);
            if (!status.ok())
            {
                LOG(ERROR) << "Unable to drop table with error: "
                           << status.ToString();
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::WRITE_FAILED);
                result.set_error_msg(status.ToString());
                drop_table_req->SetFinish(result);
                return;
            }

            rocksdb::FlushOptions flush_options;
            flush_options.allow_write_stall = true;
            flush_options.wait = true;

            status = db->Flush(flush_options);
            if (!status.ok())
            {
                LOG(ERROR) << "Unable to drop table with error: "
                           << status.ToString();
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::FLUSH_FAILED);
                result.set_error_msg(status.ToString());
                drop_table_req->SetFinish(result);
                return;
            }

            result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
            drop_table_req->SetFinish(result);
            DLOG(INFO) << "DropTable successfully.";
        });

    if (!res)
    {
        LOG(ERROR) << "Failed to submit drop table work to query worker pool";
        ::EloqDS::remote::CommonResult result;
        result.set_error_code(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        result.set_error_msg("DB is not opened");
        drop_table_req->SetFinish(result);

        drop_table_req->Clear();
        drop_table_req->Free();
        return;
    }
}

void RocksDBDataStoreCommon::ScanNext(ScanRequest *scan_req)
{
    bool res = query_worker_pool_->SubmitWork(
        [this, scan_req]()
        {
            // DLOG(INFO) << "RocksDBDataStoreCommon::ScanNext "
            //            << scan_req->GetPartitionId();
            PoolableGuard poolable_guard(scan_req);

            uint32_t partition_id = scan_req->GetPartitionId();
            bool scan_forward = scan_req->ScanForward();
            const std::string &session_id = scan_req->GetSessionId();
            scan_req->ClearSessionId();
            const bool inclusive_start = scan_req->InclusiveStart();
            const bool inclusive_end = scan_req->InclusiveEnd();
            const std::string_view start_key = scan_req->GetStartKey();
            std::string_view end_key = scan_req->GetEndKey();
            const size_t batch_size = scan_req->BatchSize();
            int search_cond_size = scan_req->GetSearchConditionsSize();

            std::shared_lock<std::shared_mutex> db_lk(db_mux_);

            TTLWrapper *iter_wrapper =
                data_store_service_->BorrowScanIter(shard_id_, session_id);

            auto shard_status = FetchDSShardStatus();
            if (shard_status != DSShardStatus::ReadOnly &&
                shard_status != DSShardStatus::ReadWrite)
            {
                if (iter_wrapper != nullptr)
                {
                    data_store_service_->EraseScanIter(shard_id_, session_id);
                }
                scan_req->SetFinish(
                    ::EloqDS::remote::DataStoreError::REQUESTED_NODE_NOT_OWNER);
                return;
            }

            std::string kv_start_key, kv_end_key;
            std::string key_prefix =
                BuildKeyPrefix(scan_req->GetTableName(), partition_id);

            // Prepare kv_start_key
            if (!start_key.empty())
            {
                // start key is not empty
                BuildKey(key_prefix, start_key, kv_start_key);
            }
            else if (scan_forward)
            {
                // scan forward and start key is empty
                // treat empty start key as the neg inf
                kv_start_key = key_prefix;
            }
            else
            {
                // scan backward and start key is empty
                // treat empty start key as the pos inf
                kv_start_key = key_prefix;
                kv_start_key.back()++;
            }

            // Prepare kv_end_key
            if (!end_key.empty())
            {
                // end key is not empty
                BuildKey(key_prefix, end_key, kv_end_key);
            }
            else if (scan_forward)
            {
                // scan forward and end key is empty
                // treat empty end key as the pos inf
                kv_end_key = key_prefix;
                kv_end_key.back()++;
            }
            else
            {
                // scan backward and end key is empty
                // treat empty end key as the neg inf
                kv_end_key = key_prefix;
            }

            rocksdb::Iterator *iter = nullptr;
            if (iter_wrapper != nullptr)
            {
                auto *rocksdb_iter_wrapper =
                    static_cast<RocksDBIteratorTTLWrapper *>(iter_wrapper);
                iter = rocksdb_iter_wrapper->GetIter();
            }
            else
            {
                rocksdb::ReadOptions read_options;
                // NOTICE: do not enable async_io if compiling rocksdbcloud
                // without iouring.
                read_options.async_io = false;
                iter = GetDBPtr()->NewIterator(read_options);

                rocksdb::Slice key(kv_start_key);
                if (scan_forward)
                {
                    iter->Seek(key);
                    if (!inclusive_start && iter->Valid())
                    {
                        rocksdb::Slice curr_key = iter->key();
                        if (curr_key.ToStringView() == key.ToStringView())
                        {
                            iter->Next();
                        }
                    }
                }
                else
                {
                    iter->SeekForPrev(key);
                    if (!inclusive_start && iter->Valid())
                    {
                        rocksdb::Slice curr_key = iter->key();
                        if (curr_key.ToStringView() == key.ToStringView())
                        {
                            iter->Prev();
                        }
                    }
                }
            }

            // Fetch the batch of records into the response
            uint32_t record_count = 0;
            bool scan_completed = false;
            while (iter->Valid() && record_count < batch_size)
            {
                if (scan_forward)
                {
                    if (!end_key.empty())
                    {
                        if (inclusive_end &&
                            iter->key().ToStringView() > kv_end_key)
                        {
                            scan_completed = true;
                            break;
                        }
                        else if (!inclusive_end &&
                                 iter->key().ToStringView() >= kv_end_key)
                        {
                            scan_completed = true;
                            break;
                        }
                    }
                    else
                    {
                        // end key is empty, then kv_end_key should not be
                        // available in the db
                        assert(iter->key().ToStringView() != kv_end_key);
                        if (iter->key().ToStringView() >= kv_end_key)
                        {
                            scan_completed = true;
                            break;
                        }
                    }
                }
                else
                {
                    if (!end_key.empty())
                    {
                        if (inclusive_end &&
                            iter->key().ToStringView() < kv_end_key)
                        {
                            scan_completed = true;
                            break;
                        }
                        else if (!inclusive_end &&
                                 iter->key().ToStringView() <= kv_end_key)
                        {
                            scan_completed = true;
                            break;
                        }
                    }
                    else
                    {
                        // end key is empty, then kv_end_key should not be
                        // available in the db
                        assert(iter->key().ToStringView() != kv_end_key);
                        if (iter->key().ToStringView() <= kv_end_key)
                        {
                            scan_completed = true;
                            break;
                        }
                    }
                }

                // NOTICE: must remove prefix from store-key
                rocksdb::Slice key_slice = iter->key();
                assert(key_slice.size() > 0);
                // Deserialize key_slice to key by removing prefix
                std::string_view key;
                [[maybe_unused]] bool ret =
                    DeserializeKey(key_slice.data(),
                                   key_slice.size(),
                                   scan_req->GetTableName(),
                                   scan_req->GetPartitionId(),
                                   key);
                assert(ret);
                rocksdb::Slice value = iter->value();

                // Deserialize value to record and record_ts
                std::string rec;
                uint64_t rec_ts;
                uint64_t rec_ttl;
                DeserializeValueToRecord(
                    value.data(), value.size(), rec, rec_ts, rec_ttl);

                const remote::SearchCondition *cond = nullptr;
                bool matched = true;
                if (!rec.empty())
                {
                    for (int cond_idx = 0; cond_idx < search_cond_size;
                         ++cond_idx)
                    {
                        cond = scan_req->GetSearchConditions(cond_idx);
                        assert(cond);
                        if (cond->field_name() == "type")
                        {
                            int8_t obj_type =
                                static_cast<int8_t>(cond->value()[0]);
                            int8_t store_obj_type = static_cast<int8_t>(rec[0]);
                            if (obj_type != store_obj_type)
                            {
                                // type mismatch
                                matched = false;
                                break;
                            }
                        }
                    }

                    if (!matched)
                    {
                        if (scan_forward)
                        {
                            iter->Next();
                        }
                        else
                        {
                            iter->Prev();
                        }
                        continue;
                    }
                }

                scan_req->AddItem(
                    std::string(key), std::move(rec), rec_ts, rec_ttl);
                if (scan_forward)
                {
                    iter->Next();
                }
                else
                {
                    iter->Prev();
                }
                record_count++;
            }

            if (!iter->Valid() || scan_completed)
            {
                // run out of records, remove iter
                if (iter_wrapper != nullptr)
                {
                    data_store_service_->EraseScanIter(shard_id_, session_id);
                }
                else
                {
                    delete iter;
                }
            }
            else
            {
                if (iter_wrapper != nullptr)
                {
                    data_store_service_->ReturnScanIter(shard_id_,
                                                        iter_wrapper);
                    // Set session id carry over to the response
                    scan_req->SetSessionId(session_id);
                }
                else if (scan_req->GenerateSessionId())
                {
                    // Otherwise, save the iterator in the session map
                    auto iter_wrapper =
                        std::make_unique<RocksDBIteratorTTLWrapper>(iter);
                    std::string session_id =
                        data_store_service_->GenerateSessionId();
                    // Set session id in the response
                    scan_req->SetSessionId(session_id);
                    // Save the iterator in the session map
                    data_store_service_->EmplaceScanIter(
                        shard_id_, session_id, std::move(iter_wrapper));
                }
                else
                {
                    delete iter;
                }
            }

            scan_req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
        });

    if (!res)
    {
        LOG(ERROR) << "Failed to submit scan work to query worker pool";
        scan_req->SetFinish(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        scan_req->Clear();
        scan_req->Free();
        return;
    }
}

void RocksDBDataStoreCommon::ScanClose(ScanRequest *scan_req)
{
    bool res = query_worker_pool_->SubmitWork(
        [this, scan_req]()
        {
            // Create a guard to ensure the poolable object is released to pool
            PoolableGuard self_guard(scan_req);

            const std::string &session_id = scan_req->GetSessionId();
            if (!session_id.empty())
            {
                // Erase the iterator from the session map
                data_store_service_->EraseScanIter(shard_id_, session_id);
            }

            scan_req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
        });
    if (!res)
    {
        LOG(ERROR) << "Failed to submit scan close work to query worker pool";
        scan_req->SetFinish(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        scan_req->Clear();
        scan_req->Free();
    }
}

DSShardStatus RocksDBDataStoreCommon::FetchDSShardStatus() const
{
    assert(data_store_service_ != nullptr);
    return data_store_service_->FetchDSShardStatus(shard_id_);
}

void RocksDBDataStoreCommon::SwitchToReadOnly()
{
    bthread::Mutex mutex;
    bthread::ConditionVariable cond_var;
    bool done = false;

    // pause all background jobs to stop compaction and obselete file
    // deletion
    bool res = query_worker_pool_->SubmitWork(
        [this, &mutex, &cond_var, &done]()
        {
            // Run pause background work in a separate thread to avoid blocking
            // bthread worker threads
            std::shared_lock<std::shared_mutex> db_lk(db_mux_);
            auto db = GetDBPtr();
            if (db != nullptr)
            {
                db->PauseBackgroundWork();
            }

            std::unique_lock<bthread::Mutex> lk(mutex);
            done = true;
            cond_var.notify_one();
        });
    if (!res)
    {
        LOG(ERROR)
            << "Failed to submit switch to read only work to query worker pool";
        return;
    }

    std::unique_lock<bthread::Mutex> lk(mutex);

    while (!done)
    {
        cond_var.wait(lk);
    }
}

void RocksDBDataStoreCommon::SwitchToReadWrite()
{
    // Since ContinueBackgroundWork() is non-blocking, we don't need the
    // thread synchronization machinery here
    std::shared_lock<std::shared_mutex> db_lk(db_mux_);
    auto db = GetDBPtr();
    if (db != nullptr)
    {
        db->ContinueBackgroundWork();
    }
}
// Build key in RocksDB
std::string RocksDBDataStoreCommon::BuildKey(const std::string_view table_name,
                                             uint32_t partition_id,
                                             const std::string_view key)
{
    std::string tmp_key;
    tmp_key.reserve(table_name.size() + 2 + key.size());
    tmp_key.append(table_name);
    tmp_key.append(KEY_SEPARATOR);
    tmp_key.append(std::to_string(partition_id));
    tmp_key.append(KEY_SEPARATOR);
    tmp_key.append(key);
    return tmp_key;
}

const std::string RocksDBDataStoreCommon::BuildKeyForDebug(
    const std::unique_ptr<rocksdb::Slice[]> &key_slices, size_t slice_size)
{
    std::string tmp_key;
    for (size_t i = 0; i < slice_size; ++i)
    {
        tmp_key.append(key_slices[i].data(), key_slices[i].size());
    }
    return tmp_key;
}

const std::string RocksDBDataStoreCommon::BuildKeyPrefix(
    const std::string_view table_name, uint32_t partition_id)
{
    std::string tmp_key;
    tmp_key.reserve(table_name.size() + 1);
    tmp_key.append(table_name);
    tmp_key.append(KEY_SEPARATOR);
    tmp_key.append(std::to_string(partition_id));
    tmp_key.append(KEY_SEPARATOR);
    return tmp_key;
}

void RocksDBDataStoreCommon::BuildKey(const std::string_view prefix,
                                      const std::string_view key,
                                      std::string &key_out)
{
    key_out.append(prefix);
    // if prefix ends with KEY_SEPARATOR, skiping append another KEY_SEPARATOR
    if (prefix.back() != KEY_SEPARATOR[0])
    {
        key_out.append(KEY_SEPARATOR);
    }
    key_out.append(key);
}

void RocksDBDataStoreCommon::BuildKeyPrefixSlices(
    const std::string_view table_name,
    const std::string_view partition_id,
    std::unique_ptr<rocksdb::Slice[]> &key_slices_out)
{
    key_slices_out[0] = rocksdb::Slice(table_name.data(), table_name.size());
    key_slices_out[1] = rocksdb::Slice(KEY_SEPARATOR, 1);
    key_slices_out[2] =
        rocksdb::Slice(partition_id.data(), partition_id.size());
    key_slices_out[3] = rocksdb::Slice(KEY_SEPARATOR, 1);
}

void RocksDBDataStoreCommon::BuildKeySlices(
    std::unique_ptr<rocksdb::Slice[]> &key_slices_out,
    const WriteRecordsRequest *batch_write_req,
    const size_t &idx,
    const uint16_t parts_cnt_per_key)
{
    for (uint16_t i = 0; i < parts_cnt_per_key; ++i)
    {
        uint16_t key_slice_idx = 4 + i;
        size_t key_part_idx = idx * parts_cnt_per_key + i;
        key_slices_out[key_slice_idx] =
            rocksdb::Slice(batch_write_req->GetKeyPart(key_part_idx).data(),
                           batch_write_req->GetKeyPart(key_part_idx).size());
    }
}

bool RocksDBDataStoreCommon::DeserializeKey(const char *data,
                                            const size_t size,
                                            const std::string_view table_name,
                                            const uint32_t partition_id,
                                            std::string_view &key_out)
{
    size_t prefix_size =
        table_name.size() + 1 + std::to_string(partition_id).size() + 1;
    if (prefix_size >= size)
    {
        return false;
    }

    key_out = std::string_view(data + prefix_size, size - prefix_size);
    return true;
}

void RocksDBDataStoreCommon::EncodeHasTTLIntoTs(uint64_t &ts, bool has_ttl)
{
    // Set the MSB to indicate that the timestamp is encoded
    if (has_ttl)
    {
        ts |= MSB;
    }
    else
    {
        // ts remains unchanged, since its MSB is always be 0
        // ts &= MSB_MASK;  // Clear the MSB
    }
}

uint16_t RocksDBDataStoreCommon::TransformRecordToValueSlices(
    const WriteRecordsRequest *batch_write_req,
    const size_t &idx,
    const uint16_t parts_cnt_per_record,
    uint64_t &ts,
    const uint64_t &ttl,
    std::unique_ptr<rocksdb::Slice[]> &value_slices)
{
    bool has_ttl = (ttl > 0);
    EncodeHasTTLIntoTs(ts, has_ttl);
    value_slices[0] =
        rocksdb::Slice(reinterpret_cast<const char *>(&ts), sizeof(uint64_t));
    size_t value_slice_idx = 1;
    if (has_ttl)
    {
        value_slices[1] = rocksdb::Slice(reinterpret_cast<const char *>(&ttl),
                                         sizeof(uint64_t));
        value_slice_idx++;
    }

    for (uint16_t i = 0; i < parts_cnt_per_record; ++i)
    {
        size_t record_slice_idx = value_slice_idx + i;
        size_t record_part_idx = idx * parts_cnt_per_record + i;
        value_slices[record_slice_idx] = rocksdb::Slice(
            batch_write_req->GetRecordPart(record_part_idx).data(),
            batch_write_req->GetRecordPart(record_part_idx).size());
    }
    return value_slice_idx + parts_cnt_per_record;
}

void RocksDBDataStoreCommon::DecodeHasTTLFromTs(uint64_t &ts, bool &has_ttl)
{
    // Check if the MSB is set
    if (ts & MSB)
    {
        has_ttl = true;
        ts &= MSB_MASK;  // Clear the MSB
    }
    else
    {
        has_ttl = false;
    }
}

void RocksDBDataStoreCommon::DeserializeValueToRecord(const char *data,
                                                      const size_t size,
                                                      std::string &record,
                                                      uint64_t &ts,
                                                      uint64_t &ttl)
{
    assert(size >= sizeof(uint64_t));
    size_t offset = 0;
    ts = *reinterpret_cast<const uint64_t *>(data);
    offset += sizeof(uint64_t);
    bool has_ttl = false;
    DecodeHasTTLFromTs(ts, has_ttl);
    if (has_ttl)
    {
        assert(size >= sizeof(uint64_t) * 2);
        ttl = *(reinterpret_cast<const uint64_t *>(data + offset));
        offset += sizeof(uint64_t);
    }
    else
    {
        assert(size >= sizeof(uint64_t));
        ttl = 0;
    }
    record.assign(data + offset, size - offset);
}

rocksdb::InfoLogLevel RocksDBDataStoreCommon::StringToInfoLogLevel(
    const std::string &log_level_str)
{
    if (log_level_str == "DEBUG")
    {
        return rocksdb::InfoLogLevel::DEBUG_LEVEL;
    }
    else if (log_level_str == "INFO")
    {
        return rocksdb::InfoLogLevel::INFO_LEVEL;
    }
    else if (log_level_str == "WARN")
    {
        return rocksdb::InfoLogLevel::WARN_LEVEL;
    }
    else if (log_level_str == "ERROR")
    {
        return rocksdb::InfoLogLevel::ERROR_LEVEL;
    }
    else
    {
        // If the log level string is not recognized, default to a specific log
        // level, e.g., INFO_LEVEL Alternatively, you could throw an exception
        // or handle the case as you see fit
        return rocksdb::InfoLogLevel::INFO_LEVEL;
    }
}
}  // namespace EloqDS