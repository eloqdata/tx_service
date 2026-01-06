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
#include "eloq_store_data_store.h"

#include <algorithm>

#include "eloq_store_data_store_factory.h"
#include "internal_request.h"

namespace EloqDS
{
thread_local ObjectPool<EloqStoreOperationData<::eloqstore::ReadRequest>>
    eloq_store_read_op_pool_;
thread_local ObjectPool<EloqStoreOperationData<::eloqstore::BatchWriteRequest>>
    eloq_store_batch_write_op_pool_;
thread_local ObjectPool<EloqStoreOperationData<::eloqstore::ScanRequest>>
    eloq_store_scan_req_op_pool_;
thread_local ObjectPool<EloqStoreOperationData<::eloqstore::TruncateRequest>>
    eloq_store_truncate_op_pool_;
thread_local ObjectPool<EloqStoreOperationData<::eloqstore::FloorRequest>>
    eloq_store_floor_op_pool_;
thread_local ObjectPool<EloqStoreOperationData<::eloqstore::DropTableRequest>>
    eloq_store_drop_table_op_pool_;
thread_local ObjectPool<ScanDeleteOperationData> eloq_store_scan_del_op_pool_;

inline void BuildKey(const WriteRecordsRequest &write_req,
                     const std::size_t key_first_idx,
                     uint16_t key_parts,
                     std::string &key_out)
{
    size_t part_idx = key_first_idx;
    for (uint16_t i = 0; i < key_parts; ++i, ++part_idx)
    {
        const std::string_view part = write_req.GetKeyPart(part_idx);
        key_out.append(part.data(), part.size());
    }
}

inline void BuildValue(const WriteRecordsRequest &write_req,
                       const std::size_t rec_first_idx,
                       uint16_t rec_parts,
                       std::string &rec_out)
{
    size_t part_idx = rec_first_idx;
    for (uint16_t i = 0; i < rec_parts; ++i, ++part_idx)
    {
        const std::string_view part = write_req.GetRecordPart(part_idx);
        rec_out.append(part.data(), part.size());
    }
}

EloqStoreDataStore::EloqStoreDataStore(uint32_t shard_id,
                                       DataStoreService *data_store_service)
    : DataStore(shard_id, data_store_service)
{
    const EloqStoreDataStoreFactory *factory =
        dynamic_cast<const EloqStoreDataStoreFactory *>(
            data_store_service->GetDataStoreFactory());
    assert(factory != nullptr);
    ::eloqstore::KvOptions opts =
        factory->eloq_store_configs_.eloqstore_configs_;
    DLOG(INFO) << "Create EloqStore storage with workers: " << opts.num_threads
               << ", store path: " << opts.store_path.front()
               << ", open files limit: " << opts.fd_limit
               << ", cloud store path: " << opts.cloud_store_path
               << ", buffer pool size per shard: " << opts.buffer_pool_size;
    eloq_store_service_ = std::make_unique<::eloqstore::EloqStore>(opts);
}

void EloqStoreDataStore::Read(ReadRequest *read_req)
{
    ::eloqstore::TableIdent eloq_store_table_id;
    eloq_store_table_id.tbl_name_ = read_req->GetTableName();
    eloq_store_table_id.partition_id_ = read_req->GetPartitionId();

    std::string_view key = read_req->GetKey();

    // Read from eloqstore async
    EloqStoreOperationData<::eloqstore::ReadRequest> *read_op =
        eloq_store_read_op_pool_.NextObject();
    read_op->Reset(read_req);

    PoolableGuard op_guard(read_op);

    ::eloqstore::ReadRequest &kv_read_req = read_op->EloqStoreRequest();
    kv_read_req.SetArgs(eloq_store_table_id, key);

    uint64_t user_data = reinterpret_cast<uint64_t>(read_op);
    if (!eloq_store_service_->ExecAsyn(&kv_read_req, user_data, OnRead))
    {
        LOG(ERROR) << "Send read request to EloqStore failed for table: "
                   << kv_read_req.TableId();
        read_req->SetFinish(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        return;
    }

    op_guard.Release();
}

void EloqStoreDataStore::OnRead(::eloqstore::KvRequest *req)
{
    EloqStoreOperationData<::eloqstore::ReadRequest> *read_op =
        static_cast<EloqStoreOperationData<::eloqstore::ReadRequest> *>(
            reinterpret_cast<void *>(req->UserData()));

    assert(req == &read_op->EloqStoreRequest());
    ::eloqstore::ReadRequest *read_req =
        static_cast<::eloqstore::ReadRequest *>(req);

    PoolableGuard op_guard(read_op);

    ReadRequest *ds_read_req =
        static_cast<ReadRequest *>(read_op->DataStoreRequest());

    if (read_req->Error() != ::eloqstore::KvError::NoError)
    {
        LOG_IF(ERROR, read_req->Error() != ::eloqstore::KvError::NotFound)
            << "Read from EloqStore failed with error code: "
            << static_cast<uint32_t>(read_req->Error())
            << ", error message: " << read_req->ErrMessage()
            << ". Table: " << req->TableId();

        remote::DataStoreError error_code =
            read_req->Error() == ::eloqstore::KvError::NotFound
                ? remote::DataStoreError::KEY_NOT_FOUND
                : remote::DataStoreError::READ_FAILED;
        ds_read_req->SetFinish(error_code);
        return;
    }

    ds_read_req->SetRecord(std::move(read_req->value_));
    ds_read_req->SetRecordTs(read_req->ts_);
    ds_read_req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
}

void EloqStoreDataStore::BatchWriteRecords(WriteRecordsRequest *write_req)
{
    ::eloqstore::TableIdent eloq_store_table_id;
    eloq_store_table_id.tbl_name_ = write_req->GetTableName();
    eloq_store_table_id.partition_id_ = write_req->GetPartitionId();

    // Write to eloqstore async
    EloqStoreOperationData<::eloqstore::BatchWriteRequest> *write_op =
        eloq_store_batch_write_op_pool_.NextObject();
    write_op->Reset(write_req);

    PoolableGuard op_guard(write_op);

    ::eloqstore::BatchWriteRequest &kv_write_req = write_op->EloqStoreRequest();

    std::vector<::eloqstore::WriteDataEntry> entries;
    size_t rec_cnt = write_req->RecordsCount();
    entries.reserve(rec_cnt);
    const uint16_t parts_per_key = write_req->PartsCountPerKey();
    const uint16_t parts_per_record = write_req->PartsCountPerRecord();
    size_t first_idx = 0;
    for (size_t i = 0; i < rec_cnt; ++i)
    {
        ::eloqstore::WriteDataEntry entry;
        first_idx = i * parts_per_key;
        BuildKey(*write_req, first_idx, parts_per_key, entry.key_);
        first_idx = i * parts_per_record;
        BuildValue(*write_req, first_idx, parts_per_record, entry.val_);
        entry.timestamp_ = write_req->GetRecordTs(i);
        entry.op_ = (write_req->KeyOpType(i) == WriteOpType::PUT
                         ? ::eloqstore::WriteOp::Upsert
                         : ::eloqstore::WriteOp::Delete);
        uint64_t ttl = write_req->GetRecordTtl(i);
        entry.expire_ts_ = ttl == UINT64_MAX ? 0 : ttl;
        entries.emplace_back(std::move(entry));
    }

    if (!std::ranges::is_sorted(
            entries, std::ranges::less{}, &::eloqstore::WriteDataEntry::key_))
    {
        DLOG(INFO) << "Sort this batch records in non-descending order before "
                      "send to EloqStore for table: "
                   << eloq_store_table_id;
        // Sort the batch keys
        std::ranges::sort(
            entries, std::ranges::less{}, &::eloqstore::WriteDataEntry::key_);
    }

    kv_write_req.SetArgs(eloq_store_table_id, std::move(entries));

    uint64_t user_data = reinterpret_cast<uint64_t>(write_op);
    if (!eloq_store_service_->ExecAsyn(&kv_write_req, user_data, OnBatchWrite))
    {
        LOG(ERROR) << "Send write request to EloqStore failed for table: "
                   << kv_write_req.TableId();
        remote::CommonResult result;
        result.set_error_code(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        result.set_error_msg("EloqStore not open.");
        write_req->SetFinish(result);
        return;
    }

    op_guard.Release();
}

void EloqStoreDataStore::OnBatchWrite(::eloqstore::KvRequest *req)
{
    EloqStoreOperationData<::eloqstore::BatchWriteRequest> *write_op =
        static_cast<EloqStoreOperationData<::eloqstore::BatchWriteRequest> *>(
            reinterpret_cast<void *>(req->UserData()));

    assert(req == &write_op->EloqStoreRequest());
    ::eloqstore::BatchWriteRequest *write_req =
        static_cast<::eloqstore::BatchWriteRequest *>(req);

    PoolableGuard op_guard(write_op);

    WriteRecordsRequest *ds_write_req =
        static_cast<WriteRecordsRequest *>(write_op->DataStoreRequest());

    remote::CommonResult result;
    if (write_req->Error() != ::eloqstore::KvError::NoError)
    {
        LOG(ERROR) << "Write to EloqStore failed with error code: "
                   << static_cast<uint32_t>(write_req->Error())
                   << ", error message: " << write_req->ErrMessage()
                   << ". Table: " << req->TableId();

        result.set_error_code(::EloqDS::remote::DataStoreError::WRITE_FAILED);
        result.set_error_msg(write_req->ErrMessage());
        write_req->Clear();
        ds_write_req->SetFinish(result);
        return;
    }

    result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
    write_req->Clear();
    ds_write_req->SetFinish(result);
}

void EloqStoreDataStore::FlushData(FlushDataRequest *flush_data_req)
{
    PoolableGuard req_guard(flush_data_req);
    remote::CommonResult result;
    result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
    flush_data_req->SetFinish(result);
}

void EloqStoreDataStore::DeleteRange(DeleteRangeRequest *delete_range_req)
{
    const std::string_view end_key = delete_range_req->GetEndKey();
    if (end_key.size() > 0)
    {
        // Delete batch keys
        ScanDelete(delete_range_req);
        return;
    }

    // Truncate from start key.

    ::eloqstore::TableIdent eloq_store_table_id;
    eloq_store_table_id.tbl_name_ = delete_range_req->GetTableName(),
    eloq_store_table_id.partition_id_ = delete_range_req->GetPartitionId();

    const std::string_view start_key = delete_range_req->GetStartKey();

    // Delete records from eloqstore async
    EloqStoreOperationData<::eloqstore::TruncateRequest> *truncate_op =
        eloq_store_truncate_op_pool_.NextObject();
    truncate_op->Reset(delete_range_req);

    PoolableGuard op_guard(delete_range_req);

    ::eloqstore::TruncateRequest &kv_truncate_req =
        truncate_op->EloqStoreRequest();
    kv_truncate_req.SetArgs(eloq_store_table_id, start_key);

    uint64_t user_data = reinterpret_cast<uint64_t>(truncate_op);
    if (!eloq_store_service_->ExecAsyn(
            &kv_truncate_req, user_data, OnDeleteRange))
    {
        LOG(ERROR) << "Send truncate request to EloqStore failed for table: "
                   << kv_truncate_req.TableId();

        remote::CommonResult result;
        result.set_error_code(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        result.set_error_msg("EloqStore not open");
        delete_range_req->SetFinish(result);
        return;
    }

    op_guard.Release();
}

void EloqStoreDataStore::OnDeleteRange(::eloqstore::KvRequest *req)
{
    EloqStoreOperationData<::eloqstore::TruncateRequest> *truncate_op =
        static_cast<EloqStoreOperationData<::eloqstore::TruncateRequest> *>(
            reinterpret_cast<void *>(req->UserData()));

    assert(req == &truncate_op->EloqStoreRequest());

    PoolableGuard op_guard(truncate_op);

    DeleteRangeRequest *ds_delete_range_req =
        static_cast<DeleteRangeRequest *>(truncate_op->DataStoreRequest());

    remote::CommonResult result;
    if (req->Error() != ::eloqstore::KvError::NoError &&
        req->Error() != ::eloqstore::KvError::NotFound)
    {
        LOG(ERROR) << "Delete keys from EloqStore failed with error code: "
                   << static_cast<uint32_t>(req->Error())
                   << ", error message: " << req->ErrMessage()
                   << ". Table: " << req->TableId();

        result.set_error_code(remote::DataStoreError::WRITE_FAILED);
        result.set_error_msg(req->ErrMessage());
        ds_delete_range_req->SetFinish(result);
        return;
    }

    result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
    ds_delete_range_req->SetFinish(result);
}

void EloqStoreDataStore::CreateTable(CreateTableRequest *create_table_req)
{
    PoolableGuard req_guard(create_table_req);

    remote::CommonResult result;
    result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
    create_table_req->SetFinish(result);
}

void EloqStoreDataStore::DropTable(DropTableRequest *drop_table_req)
{
    EloqStoreOperationData<::eloqstore::DropTableRequest> *drop_table_op =
        eloq_store_drop_table_op_pool_.NextObject();
    drop_table_op->Reset(drop_table_req);

    PoolableGuard op_guard(drop_table_op);

    ::eloqstore::DropTableRequest &kv_drop_table_req =
        drop_table_op->EloqStoreRequest();

    kv_drop_table_req.SetArgs(std::string(drop_table_req->GetTableName()));

    uint64_t user_data = reinterpret_cast<uint64_t>(drop_table_op);
    if (!eloq_store_service_->ExecAsyn(
            &kv_drop_table_req, user_data, OnDropTable))
    {
        LOG(ERROR) << "Send drop table request to EloqStore failed for table: "
                   << drop_table_req->GetTableName();
        remote::CommonResult result;
        result.set_error_code(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        result.set_error_msg("EloqStore not open.");
        drop_table_req->SetFinish(result);
        return;
    }

    op_guard.Release();
}

void EloqStoreDataStore::OnDropTable(eloqstore::KvRequest *req)
{
    EloqStoreOperationData<::eloqstore::DropTableRequest> *drop_table_op =
        static_cast<EloqStoreOperationData<::eloqstore::DropTableRequest> *>(
            reinterpret_cast<void *>(req->UserData()));

    assert(req == &drop_table_op->EloqStoreRequest());
    ::eloqstore::DropTableRequest *drop_table_request =
        static_cast<::eloqstore::DropTableRequest *>(req);

    PoolableGuard op_guard(drop_table_op);

    DropTableRequest *ds_drop_table_req =
        static_cast<DropTableRequest *>(drop_table_op->DataStoreRequest());

    remote::CommonResult result;
    if (drop_table_request->Error() != ::eloqstore::KvError::NoError)
    {
        LOG(ERROR) << "Drop table from EloqStore failed with error code: "
                   << static_cast<uint32_t>(drop_table_request->Error())
                   << ", error message: " << drop_table_request->ErrMessage()
                   << ". Table: " << req->TableId();

        result.set_error_code(::EloqDS::remote::DataStoreError::WRITE_FAILED);
        result.set_error_msg(drop_table_request->ErrMessage());
        ds_drop_table_req->SetFinish(result);
        return;
    }

    result.set_error_code(::EloqDS::remote::DataStoreError::NO_ERROR);
    ds_drop_table_req->SetFinish(result);
}

void EloqStoreDataStore::ScanNext(ScanRequest *scan_req)
{
    const size_t batch_size = scan_req->BatchSize();
    if (!scan_req->ScanForward() && batch_size == 1)
    {
        Floor(scan_req);
        return;
    }

    ::eloqstore::TableIdent eloq_store_table_id;
    eloq_store_table_id.tbl_name_ = scan_req->GetTableName();
    eloq_store_table_id.partition_id_ = scan_req->GetPartitionId();

    const std::string_view start_key = scan_req->GetStartKey();
    const bool inclusive_start = scan_req->InclusiveStart();
    std::string_view end_key = scan_req->GetEndKey();
    // const bool inclusive_end = scan_req->InclusiveEnd();

    // Scan from eloqstore async
    EloqStoreOperationData<::eloqstore::ScanRequest> *scan_op =
        eloq_store_scan_req_op_pool_.NextObject();
    scan_op->Reset(scan_req);

    PoolableGuard op_guard(scan_op);

    ::eloqstore::ScanRequest &kv_scan_req = scan_op->EloqStoreRequest();
    kv_scan_req.SetArgs(
        eloq_store_table_id, start_key, end_key, inclusive_start);
    kv_scan_req.SetPagination(batch_size, 0);

    uint64_t user_data = reinterpret_cast<uint64_t>(scan_op);
    if (!eloq_store_service_->ExecAsyn(&kv_scan_req, user_data, OnScanNext))
    {
        LOG(ERROR) << "Send scan request to EloqStore failed for table: "
                   << kv_scan_req.TableId();
        scan_req->SetFinish(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        return;
    }

    op_guard.Release();
}

void EloqStoreDataStore::OnScanNext(::eloqstore::KvRequest *req)
{
    EloqStoreOperationData<::eloqstore::ScanRequest> *scan_op =
        static_cast<EloqStoreOperationData<::eloqstore::ScanRequest> *>(
            reinterpret_cast<void *>(req->UserData()));

    assert(req == &scan_op->EloqStoreRequest());
    ::eloqstore::ScanRequest *scan_req =
        static_cast<::eloqstore::ScanRequest *>(req);

    PoolableGuard op_guard(scan_op);

    ScanRequest *ds_scan_req =
        static_cast<ScanRequest *>(scan_op->DataStoreRequest());

    if (scan_req->Error() != ::eloqstore::KvError::NoError)
    {
        LOG_IF(ERROR, scan_req->Error() != ::eloqstore::KvError::NotFound)
            << "Scan from EloqStore failed with error code: "
            << static_cast<uint32_t>(scan_req->Error())
            << ", error message: " << scan_req->ErrMessage()
            << ". Table: " << req->TableId();

        remote::DataStoreError error_code =
            scan_req->Error() == ::eloqstore::KvError::NotFound
                ? remote::DataStoreError::NO_ERROR
                : remote::DataStoreError::READ_FAILED;
        ds_scan_req->SetFinish(error_code);
        return;
    }

    int search_cond_size = ds_scan_req->GetSearchConditionsSize();
    const remote::SearchCondition *cond = nullptr;

    for (auto &entry : scan_req->Entries())
    {
        bool matched = true;
        for (int cond_idx = 0; cond_idx < search_cond_size; ++cond_idx)
        {
            cond = ds_scan_req->GetSearchConditions(cond_idx);
            assert(cond);
            if (cond->field_name() == "type" &&
                cond->value().compare(0, 1, entry.value_, 0, 1))
            {
                // type mismatch
                matched = false;
                break;
            }
        }
        if (!matched)
        {
            continue;
        }

        ds_scan_req->AddItem(std::move(entry.key_),
                             std::move(entry.value_),
                             entry.timestamp_,
                             entry.expire_ts_);
    }

    ds_scan_req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
}

void EloqStoreDataStore::ScanClose(ScanRequest *scan_req)
{
    PoolableGuard self_guard(scan_req);
    scan_req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
}

/**
 * @brief Switch the data store to read only mode.
 */
void EloqStoreDataStore::SwitchToReadOnly()
{
    return;
}

/**
 * @brief Switch the data store to read write mode.
 */
void EloqStoreDataStore::SwitchToReadWrite()
{
    return;
}

void EloqStoreDataStore::CreateSnapshotForBackup(
    CreateSnapshotForBackupRequest *req)
{
    return;
}

void EloqStoreDataStore::ScanDelete(DeleteRangeRequest *delete_range_req)
{
    ::eloqstore::TableIdent eloq_store_table_id;
    eloq_store_table_id.tbl_name_ = delete_range_req->GetTableName();
    eloq_store_table_id.partition_id_ = delete_range_req->GetPartitionId();

    const std::string_view start_key = delete_range_req->GetStartKey();
    const std::string_view end_key = delete_range_req->GetEndKey();

    // Delete records from eloqstore async
    ScanDeleteOperationData *scan_del_op =
        eloq_store_scan_del_op_pool_.NextObject();
    scan_del_op->Reset(delete_range_req, eloq_store_service_.get());

    PoolableGuard op_guard(scan_del_op);

    ::eloqstore::ScanRequest &kv_scan_req = scan_del_op->EloqStoreScanRequest();
    kv_scan_req.SetArgs(eloq_store_table_id, start_key, end_key, true);
    kv_scan_req.SetPagination(1024, 0);

    uint64_t user_data = reinterpret_cast<uint64_t>(scan_del_op);
    if (!eloq_store_service_->ExecAsyn(&kv_scan_req, user_data, OnScanDelete))
    {
        LOG(ERROR) << "Send scan request to EloqStore failed for table: "
                   << kv_scan_req.TableId();
        remote::CommonResult result;
        result.set_error_code(remote::DataStoreError::DB_NOT_OPEN);
        result.set_error_msg("EloqStore not open");
        delete_range_req->SetFinish(result);
        return;
    }

    op_guard.Release();
}

void EloqStoreDataStore::OnScanDelete(::eloqstore::KvRequest *req)
{
    ScanDeleteOperationData *scan_del_op =
        static_cast<ScanDeleteOperationData *>(
            reinterpret_cast<void *>(req->UserData()));

    assert(req == &scan_del_op->EloqStoreScanRequest() ||
           req == &scan_del_op->EloqStoreWriteRequest());
    PoolableGuard op_guard(scan_del_op);

    DeleteRangeRequest *ds_req =
        static_cast<DeleteRangeRequest *>(scan_del_op->DataStoreRequest());
    remote::CommonResult result;

    if (scan_del_op->OperationStage() == ScanDeleteOperationData::Stage::SCAN)
    {
        // scan stage
        ::eloqstore::ScanRequest *scan_req =
            static_cast<::eloqstore::ScanRequest *>(req);

        if (scan_req->Error() != ::eloqstore::KvError::NoError)
        {
            LOG_IF(ERROR, scan_req->Error() != ::eloqstore::KvError::NotFound)
                << "Scan from EloqStore failed with error code: "
                << static_cast<uint32_t>(scan_req->Error())
                << ", error message: " << scan_req->ErrMessage()
                << ". Table: " << req->TableId();

            if (scan_req->Error() == ::eloqstore::KvError::NotFound)
            {
                assert(!scan_req->HasRemaining() &&
                       scan_del_op->entries_.size() == 0);
                result.set_error_code(remote::DataStoreError::NO_ERROR);
            }
            else
            {
                result.set_error_code(remote::DataStoreError::WRITE_FAILED);
                result.set_error_msg(req->ErrMessage());
            }
            ds_req->SetFinish(result);
            return;
        }

        const size_t scan_key_cnt = scan_req->Entries().size();
        if (scan_key_cnt > 0)
        {
            const std::string &last_key = scan_req->Entries().back().key_;
            scan_del_op->UpdateLastScanEndKey(last_key,
                                              !scan_req->HasRemaining());

            // delete this batch keys
            scan_del_op->UpdateOperationStage(
                ScanDeleteOperationData::Stage::DELETE);
            ::eloqstore::BatchWriteRequest &kv_write_req =
                scan_del_op->EloqStoreWriteRequest();

            uint64_t delete_ts = scan_del_op->OpTs();

            std::vector<::eloqstore::WriteDataEntry> delete_entries;
            delete_entries.reserve(scan_key_cnt);
            for (auto &entry : scan_req->Entries())
            {
                delete_entries.emplace_back(std::move(entry.key_),
                                            std::move(entry.value_),
                                            delete_ts,
                                            ::eloqstore::WriteOp::Delete);
            }

            kv_write_req.SetArgs(scan_req->TableId(),
                                 std::move(delete_entries));

            uint64_t user_data = reinterpret_cast<uint64_t>(scan_del_op);
            if (!scan_del_op->EloqStoreService()->ExecAsyn(
                    &kv_write_req, user_data, OnScanDelete))
            {
                LOG(ERROR)
                    << "Send write request to EloqStore failed for table: "
                    << kv_write_req.TableId();
                result.set_error_code(
                    ::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
                result.set_error_msg("EloqStore not open.");
                ds_req->SetFinish(result);
                return;
            }
        }
        else
        {
            // no more keys need to be deleted.
            DLOG(INFO) << "No more keys need to be deleted for table: "
                       << req->TableId();
            result.set_error_code(remote::DataStoreError::NO_ERROR);
            ds_req->SetFinish(result);
            return;
        }
    }
    else
    {
        // delete stage
        assert(scan_del_op->OperationStage() ==
               ScanDeleteOperationData::Stage::DELETE);

        ::eloqstore::BatchWriteRequest *write_req =
            static_cast<::eloqstore::BatchWriteRequest *>(req);

        if (write_req->Error() != ::eloqstore::KvError::NoError)
        {
            LOG(ERROR)
                << "Delete batch keys from EloqStore failed with error code: "
                << static_cast<uint32_t>(write_req->Error())
                << ", error message: " << write_req->ErrMessage()
                << ". Table: " << req->TableId();

            result.set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_FAILED);
            result.set_error_msg(write_req->ErrMessage());
            ds_req->SetFinish(result);
            return;
        }

        const std::string &last_scan_end_key = scan_del_op->LastScanEndKey();
        if (last_scan_end_key.size() > 0)
        {
            // to scan next batch keys
            scan_del_op->UpdateOperationStage(
                ScanDeleteOperationData::Stage::SCAN);

            ::eloqstore::ScanRequest &kv_scan_req =
                scan_del_op->EloqStoreScanRequest();
            kv_scan_req.SetArgs(write_req->TableId(),
                                last_scan_end_key,
                                ds_req->GetEndKey(),
                                false);

            uint64_t user_data = reinterpret_cast<uint64_t>(scan_del_op);
            if (!scan_del_op->EloqStoreService()->ExecAsyn(
                    &kv_scan_req, user_data, OnScanDelete))
            {
                LOG(ERROR)
                    << "Send scan request to EloqStore failed for table: "
                    << kv_scan_req.TableId();
                result.set_error_code(remote::DataStoreError::DB_NOT_OPEN);
                result.set_error_msg("EloqStore not open");
                ds_req->SetFinish(result);
                return;
            }
        }
        else
        {
            // no more keys to be deleted.
            DLOG(INFO) << "Finished delete batch keys for table: "
                       << req->TableId();
            result.set_error_code(remote::DataStoreError::NO_ERROR);
            ds_req->SetFinish(result);
            return;
        }
    }  //  end of delete stage

    op_guard.Release();
}

void EloqStoreDataStore::Floor(ScanRequest *scan_req)
{
    assert(scan_req->BatchSize() == 1);

    ::eloqstore::TableIdent eloq_store_table_id;
    eloq_store_table_id.tbl_name_ = scan_req->GetTableName();
    eloq_store_table_id.partition_id_ = scan_req->GetPartitionId();

    const std::string_view start_key = scan_req->GetStartKey();

    // Floor from eloqstore async
    EloqStoreOperationData<::eloqstore::FloorRequest> *floor_op =
        eloq_store_floor_op_pool_.NextObject();
    floor_op->Reset(scan_req);

    PoolableGuard op_guard(floor_op);

    ::eloqstore::FloorRequest &kv_floor_req = floor_op->EloqStoreRequest();
    kv_floor_req.SetArgs(eloq_store_table_id, start_key);

    uint64_t user_data = reinterpret_cast<uint64_t>(floor_op);
    if (!eloq_store_service_->ExecAsyn(&kv_floor_req, user_data, OnFloor))
    {
        LOG(ERROR) << "Floor request to EloqStore failed for table: "
                   << kv_floor_req.TableId();
        scan_req->SetFinish(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        return;
    }

    op_guard.Release();
}

void EloqStoreDataStore::OnFloor(::eloqstore::KvRequest *req)
{
    EloqStoreOperationData<::eloqstore::FloorRequest> *floor_op =
        static_cast<EloqStoreOperationData<::eloqstore::FloorRequest> *>(
            reinterpret_cast<void *>(req->UserData()));

    assert(req == &floor_op->EloqStoreRequest());
    ::eloqstore::FloorRequest *floor_req =
        static_cast<::eloqstore::FloorRequest *>(req);

    PoolableGuard op_guard(floor_op);

    ScanRequest *ds_scan_req =
        static_cast<ScanRequest *>(floor_op->DataStoreRequest());

    if (floor_req->Error() != ::eloqstore::KvError::NoError)
    {
        LOG(ERROR) << "Floor from EloqStore failed with error code: "
                   << static_cast<uint32_t>(floor_req->Error())
                   << ", error message: " << floor_req->ErrMessage()
                   << ". Table: " << req->TableId();

        ds_scan_req->SetFinish(remote::DataStoreError::READ_FAILED);
        return;
    }

    assert(ds_scan_req->GetSearchConditionsSize() == 0);
    ds_scan_req->AddItem(std::move(floor_req->floor_key_),
                         std::move(floor_req->value_),
                         floor_req->ts_,
                         0);

    ds_scan_req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR);
}

}  // namespace EloqDS
