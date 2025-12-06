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

#include "data_store_service_client_closure.h"

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "cc_req_misc.h"
#include "error_messages.h"
#include "store_util.h"  // host_to_big_endian
#include "tx_service/include/cc/cc_request.h"
#include "tx_service/include/cc/local_cc_shards.h"

namespace EloqDS
{
static const std::string_view kv_table_catalogs_name("table_catalogs");
static const std::string_view kv_range_table_name("table_ranges");
static const std::string_view kv_range_slices_table_name("table_range_slices");
static const std::string_view kv_table_statistics_name("table_statistics");
static const std::string_view kv_table_statistics_version_name(
    "table_statistics_version");
static const std::string_view kv_database_catalogs_name("db_catalogs");
static const std::string_view kv_mvcc_archive_name("mvcc_archives");

void SyncCallback(void *data,
                  ::google::protobuf::Closure *closure,
                  DataStoreServiceClient &client,
                  const remote::CommonResult &result)
{
    auto *callback_data = static_cast<SyncCallbackData *>(data);
    callback_data->Result().set_error_code(result.error_code());
    callback_data->Result().set_error_msg(result.error_msg());

    callback_data->Notify();
}

void UpsertDatabaseCallback(void *data,
                            ::google::protobuf::Closure *closure,
                            DataStoreServiceClient &client,
                            const remote::CommonResult &result)
{
    auto *callback_data = static_cast<UpsertDatabaseCallbackData *>(data);
    callback_data->Result().set_error_code(result.error_code());
    callback_data->Result().set_error_msg(result.error_msg());
    callback_data->Notify();
}

void DropDatabaseCallback(void *data,
                          ::google::protobuf::Closure *closure,
                          DataStoreServiceClient &client,
                          const remote::CommonResult &result)
{
    auto *callback_data = static_cast<DropDatabaseCallbackData *>(data);
    callback_data->Result().set_error_code(result.error_code());
    callback_data->Result().set_error_msg(result.error_msg());
    callback_data->Notify();
}

void SyncBatchReadForArchiveCallback(void *data,
                                     ::google::protobuf::Closure *closure,
                                     DataStoreServiceClient &client,
                                     const remote::CommonResult &result)
{
    ReadBaseForArchiveCallbackData *callback_data =
        static_cast<ReadBaseForArchiveCallbackData *>(data);
    ReadClosure *read_closure = static_cast<ReadClosure *>(closure);

    auto err_code = result.error_code();
    if (err_code == remote::DataStoreError::KEY_NOT_FOUND)
    {
        LOG(INFO) << "BatchReadForArchiveCallback, key not found: "
                  << read_closure->Key() << " , set as deleted";
        std::string_view key_str = read_closure->Key();
        uint64_t ts = 1U;
        uint64_t ttl = 0U;
        std::string value_str = client.SerializeTxRecord(true, nullptr);
        callback_data->AddResult(read_closure->PartitionId(),
                                 key_str,
                                 std::move(value_str),
                                 ts,
                                 ttl);
        callback_data->DecreaseFlyingReadCount();
        return;
    }
    else if (err_code != remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "BatchReadForArchiveCallback, error_code: " << err_code
                   << ", error_msg: " << result.error_msg();
        callback_data->SetErrorCode(
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
        callback_data->DecreaseFlyingReadCount();
        return;
    }
    else
    {
        std::string_view key_str = read_closure->Key();
        std::string &value_str = read_closure->ValueStringRef();
        uint64_t ts = read_closure->Ts();
        uint64_t ttl = read_closure->Ttl();

        callback_data->AddResult(read_closure->PartitionId(),
                                 key_str,
                                 std::move(value_str),
                                 ts,
                                 ttl);
        callback_data->DecreaseFlyingReadCount();
    }
}

void FetchRecordCallback(void *data,
                         ::google::protobuf::Closure *closure,
                         DataStoreServiceClient &client,
                         const remote::CommonResult &result)
{
    auto *read_closure = static_cast<ReadClosure *>(closure);
    txservice::FetchRecordCc *fetch_cc =
        static_cast<txservice::FetchRecordCc *>(data);
    auto err_code = result.error_code();

    {
        using txservice::FaultEntry;
        using txservice::FaultInject;
        CODE_FAULT_INJECTOR("FetchRecordFail", {
            LOG(INFO) << "FaultInject FetchRecordFail";

            txservice::FaultInject::Instance().InjectFault("FetchRecordFail",
                                                           "remove");
            err_code = remote::DataStoreError::NETWORK_ERROR;
        });
    }

    if (metrics::enable_kv_metrics)
    {
        metrics::kv_meter->CollectDuration(metrics::NAME_KV_READ_DURATION,
                                           fetch_cc->start_);
        metrics::kv_meter->Collect(metrics::NAME_KV_READ_TOTAL, 1);
    }

    if (err_code == remote::DataStoreError::KEY_NOT_FOUND)
    {
        fetch_cc->rec_status_ = txservice::RecordStatus::Deleted;
        fetch_cc->rec_ts_ = 1U;

        fetch_cc->SetFinish(0);
    }
    else if (err_code == remote::DataStoreError::NO_ERROR)
    {
        uint64_t now = txservice::LocalCcShards::ClockTsInMillseconds();
        uint64_t rec_ttl = read_closure->Ttl();
        std::string_view val = read_closure->Value();

        if (fetch_cc->table_name_.Engine() == txservice::TableEngine::EloqKv)
        {
            // Hash partition
            if (rec_ttl > 0 && rec_ttl < now)
            {
                // expired record
                fetch_cc->rec_status_ = txservice::RecordStatus::Deleted;
                fetch_cc->rec_ts_ = 1U;
                fetch_cc->SetFinish(0);
                return;
            }

            fetch_cc->rec_status_ = txservice::RecordStatus::Normal;
            fetch_cc->rec_ts_ = read_closure->Ts();
            fetch_cc->rec_str_.assign(val.data(), val.size());
            fetch_cc->SetFinish(0);
        }
        else
        {
            // Range partition
            bool is_deleted = false;
            size_t offset = 0;
            if (!DataStoreServiceClient::DeserializeTxRecordStr(
                    val, is_deleted, offset))
            {
                LOG(ERROR) << "====fetch record===decode error==" << " key: "
                           << read_closure->Key()
                           << " status: " << (int) fetch_cc->rec_status_;
                std::abort();
            }

            if (is_deleted)
            {
                fetch_cc->rec_status_ = txservice::RecordStatus::Deleted;
                fetch_cc->rec_ts_ = read_closure->Ts();
            }
            else
            {
                fetch_cc->rec_status_ = txservice::RecordStatus::Normal;
                fetch_cc->rec_ts_ = read_closure->Ts();
                fetch_cc->rec_str_.assign(val.data() + offset,
                                          val.size() - offset);
            }

            if (fetch_cc->snapshot_read_ts_ > 0 &&
                fetch_cc->snapshot_read_ts_ < fetch_cc->rec_ts_)
            {
                auto op_st = client.FetchArchives(fetch_cc);

                if (op_st != txservice::store::DataStoreHandler::
                                 DataStoreOpStatus::Success)
                {
                    LOG(ERROR) << "FetchArchives failed, key: "
                               << fetch_cc->tx_key_.ToString();
                    // Through fetch archive failed, we can also backfill the
                    // base version.
                    fetch_cc->SetFinish(0);
                }
            }
            else
            {
                fetch_cc->SetFinish(0);
            }
        }
    }
    else
    {
        fetch_cc->SetFinish(
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
    }
}

void FetchBucketDataCallback(void *data,
                             ::google::protobuf::Closure *closure,
                             DataStoreServiceClient &client,
                             const remote::CommonResult &result)
{
    assert(data != nullptr);
    txservice::FetchBucketDataCc *fetch_bucket_data_cc =
        static_cast<txservice::FetchBucketDataCc *>(data);
    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);
    assert(!scan_next_closure->GenerateSessionId());

    if (result.error_code() != EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DataStoreHandler: Failed to do FetchBucketData. "
                   << result.error_msg();
        fetch_bucket_data_cc->SetFinish(
            static_cast<int32_t>(txservice::CcErrorCode::DATA_STORE_ERR));
        return;
    }

    assert(fetch_bucket_data_cc->table_name_.IsHashPartitioned());

    uint32_t items_size = scan_next_closure->ItemsSize();
    std::string key_str;
    std::string value_str;
    uint64_t ts = UINT64_MAX;
    uint64_t ttl = UINT64_MAX;
    uint64_t now = txservice::LocalCcShards::ClockTsInMillseconds();
    for (uint32_t item_idx = 0; item_idx < items_size; ++item_idx)
    {
        scan_next_closure->GetItem(item_idx, key_str, value_str, ts, ttl);
        if (ttl > 0 && ttl < now)
        {
            // fetch_bucket_data_cc->AddDataItem(std::move(tx_key), "", 1,
            // true);
            continue;
        }
        else
        {
            fetch_bucket_data_cc->AddDataItem(
                std::move(key_str), std::move(value_str), ts, false);
        }
    }

    if (items_size < fetch_bucket_data_cc->batch_size_)
    {
        fetch_bucket_data_cc->is_drained_ = true;
    }
    else
    {
        fetch_bucket_data_cc->is_drained_ = false;
        if (fetch_bucket_data_cc->bucket_data_items_.empty())
        {
            int32_t partition_id =
                txservice::Sharder::MapBucketIdToHashPartitionId(
                    fetch_bucket_data_cc->bucket_id_);
            int32_t kv_partition_id =
                client.KvPartitionIdOf(partition_id, false);
            uint32_t data_shard_id =
                client.GetShardIdByPartitionId(kv_partition_id, false);
            client.ScanNext(fetch_bucket_data_cc->kv_table_name_,
                            kv_partition_id,
                            data_shard_id,
                            fetch_bucket_data_cc->kv_start_key_,
                            fetch_bucket_data_cc->kv_end_key_,
                            "",
                            false,
                            fetch_bucket_data_cc->start_key_inclusive_,
                            fetch_bucket_data_cc->end_key_inclusive_,
                            true,
                            fetch_bucket_data_cc->batch_size_,
                            fetch_bucket_data_cc->pushdown_cond_,
                            fetch_bucket_data_cc,
                            &FetchBucketDataCallback);
            return;
        }
    }

    // callback_data->session_id_ = scan_next_closure->GetSessionId();
    fetch_bucket_data_cc->SetFinish(
        static_cast<int32_t>(txservice::CcErrorCode::NO_ERROR));
}

void FetchSnapshotCallback(void *data,
                           ::google::protobuf::Closure *closure,
                           DataStoreServiceClient &client,
                           const remote::CommonResult &result)
{
    auto *read_closure = static_cast<ReadClosure *>(closure);

    auto *fetch_cc = static_cast<txservice::FetchSnapshotCc *>(data);
    auto err_code = result.error_code();

    if (metrics::enable_kv_metrics)
    {
        metrics::kv_meter->CollectDuration(metrics::NAME_KV_READ_DURATION,
                                           fetch_cc->start_);
        metrics::kv_meter->Collect(metrics::NAME_KV_READ_TOTAL, 1);
    }

    if (err_code == remote::DataStoreError::KEY_NOT_FOUND)
    {
        fetch_cc->rec_status_ = txservice::RecordStatus::Deleted;
        fetch_cc->rec_ts_ = 1U;

        fetch_cc->SetFinish(0);
    }
    else if (err_code == remote::DataStoreError::NO_ERROR)
    {
        std::string_view val = read_closure->Value();

        if (fetch_cc->table_name_.IsHashPartitioned())
        {
            LOG(WARNING) << "FetchSnapshot on hash partition not supported";
            assert(false);
            fetch_cc->SetFinish(
                static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
        }
        else
        {
            // Range partition
            if (fetch_cc->snapshot_read_ts_ >= read_closure->Ts())
            {
                bool is_deleted = false;
                size_t offset = 0;
                if (!DataStoreServiceClient::DeserializeTxRecordStr(
                        val, is_deleted, offset))
                {
                    LOG(ERROR) << "====fetch snapshot===decode error=="
                               << " key: " << read_closure->Key()
                               << " status: " << (int) fetch_cc->rec_status_;
                    std::abort();
                }

                if (is_deleted)
                {
                    fetch_cc->rec_status_ = txservice::RecordStatus::Deleted;
                    fetch_cc->rec_ts_ = read_closure->Ts();
                }
                else
                {
                    fetch_cc->rec_status_ = txservice::RecordStatus::Normal;
                    fetch_cc->rec_ts_ = read_closure->Ts();
                    fetch_cc->rec_str_.assign(val.data() + offset,
                                              val.size() - offset);
                }
                fetch_cc->SetFinish(0);
            }
            else
            {
                auto op_st = client.FetchVisibleArchive(fetch_cc);
                assert(op_st == txservice::store::DataStoreHandler::
                                    DataStoreOpStatus::Success);
                if (op_st != txservice::store::DataStoreHandler::
                                 DataStoreOpStatus::Success)
                {
                    LOG(ERROR) << "FetchSnapshot failed on FetchArchive, key: "
                               << fetch_cc->tx_key_.ToString();
                    fetch_cc->SetFinish(static_cast<int>(
                        txservice::CcErrorCode::DATA_STORE_ERR));
                }
            }
        }
    }
    else
    {
        fetch_cc->SetFinish(
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
    }
}

void AsyncDropTableCallback(void *data,
                            ::google::protobuf::Closure *closure,
                            DataStoreServiceClient &client,
                            const remote::CommonResult &result)
{
    auto *drop_table_data = static_cast<AsyncDropTableCallbackData *>(data);

    if (result.error_code() != remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "Drop table failed: " << result.error_msg();
        // TODO(lzx): register (drop) failed table and retry by an timer thread.
    }

    delete drop_table_data;
}

void FetchTableCatalogCallback(void *data,
                               ::google::protobuf::Closure *closure,
                               DataStoreServiceClient &client,
                               const remote::CommonResult &result)
{
    auto *read_closure = static_cast<ReadClosure *>(closure);

    auto *fetch_cc = static_cast<txservice::FetchCatalogCc *>(data);
    auto err_code = result.error_code();

    if (err_code == remote::DataStoreError::KEY_NOT_FOUND)
    {
        fetch_cc->CatalogImage().clear();
        fetch_cc->SetCommitTs(1);
        fetch_cc->SetFinish(txservice::RecordStatus::Deleted, 0);
    }
    else if (err_code == remote::DataStoreError::NO_ERROR)
    {
        fetch_cc->SetCommitTs(read_closure->Ts());
        std::string &catalog_image = fetch_cc->CatalogImage();

        // TODO(lzx): Unify table schema (de)serialization method for EloqSql
        // and EloqKV. And all data_store_handler share one KvCatalogInfo.
        catalog_image.append(read_closure->ValueString());

        fetch_cc->SetFinish(txservice::RecordStatus::Normal, 0);
    }
    else
    {
        fetch_cc->SetFinish(
            txservice::RecordStatus::Unknown,
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
    }
}

void FetchTableCallback(void *data,
                        ::google::protobuf::Closure *closure,
                        DataStoreServiceClient &client,
                        const remote::CommonResult &result)
{
    auto *fetch_table_data = reinterpret_cast<FetchTableCallbackData *>(data);
    ReadClosure *read_closure = static_cast<ReadClosure *>(closure);
    auto err_code = result.error_code();

    if (err_code == remote::DataStoreError::KEY_NOT_FOUND)
    {
        *(fetch_table_data->found_) = false;
        (*fetch_table_data->version_ts_) = 1;
        fetch_table_data->schema_image_->clear();
    }
    else if (result.error_code() == remote::DataStoreError::NO_ERROR)
    {
        *(fetch_table_data->found_) = true;
        (*fetch_table_data->version_ts_) = read_closure->Ts();

        // TODO(lzx): Unify table schema (de)serialization method for EloqSql
        // and EloqKV. And all data_store_handler share one KvCatalogInfo.
        fetch_table_data->schema_image_->append(read_closure->ValueString());
    }
    else
    {
        *(fetch_table_data->found_) = false;
        (*fetch_table_data->version_ts_) = 1;
        fetch_table_data->schema_image_->clear();
    }

    fetch_table_data->Result().set_error_code(result.error_code());
    fetch_table_data->Result().set_error_msg(result.error_msg());

    fetch_table_data->Notify();
}

void SyncConcurrentRequestCallback(void *data,
                                   ::google::protobuf::Closure *closure,
                                   DataStoreServiceClient &client,
                                   const remote::CommonResult &result)
{
    auto *callback_data = reinterpret_cast<SyncConcurrentRequest *>(data);
    callback_data->Finish(result);
}

void PartitionBatchCallback(void *data,
                            ::google::protobuf::Closure *closure,
                            DataStoreServiceClient &client,
                            const remote::CommonResult &result)
{
    auto *callback_data = reinterpret_cast<PartitionCallbackData *>(data);
    auto *partition_state = callback_data->partition_state;
    auto *global_coordinator = callback_data->global_coordinator;

    // Check if the batch failed
    if (result.error_code() != remote::DataStoreError::NO_ERROR)
    {
        partition_state->MarkFailed(result);
        // Notify the global coordinator that this partition failed
        global_coordinator->OnPartitionCompleted();
        return;
    }

    // Try to get the next batch for this partition
    PartitionBatchRequest &next_batch = callback_data->inflight_batch;
    if (partition_state->GetNextBatch(next_batch))
    {
        // Send the next batch
        BatchWriteRecordsClosure *batch_closure =
            static_cast<BatchWriteRecordsClosure *>(closure);
        uint32_t data_shard_id = batch_closure->ShardId();
        client.BatchWriteRecords(callback_data->table_name,
                                 partition_state->partition_id,
                                 data_shard_id,
                                 std::move(next_batch.key_parts),
                                 std::move(next_batch.record_parts),
                                 std::move(next_batch.records_ts),
                                 std::move(next_batch.records_ttl),
                                 std::move(next_batch.op_types),
                                 true,  // skip_wal
                                 callback_data,
                                 PartitionBatchCallback,
                                 next_batch.parts_cnt_per_key,
                                 next_batch.parts_cnt_per_record);
    }
    else
    {
        // Notify the global coordinator that this partition completed
        global_coordinator->OnPartitionCompleted();
    }
}

void FetchDatabaseCallback(void *data,
                           ::google::protobuf::Closure *closure,
                           DataStoreServiceClient &client,
                           const remote::CommonResult &result)
{
    FetchDatabaseCallbackData *fetch_data =
        static_cast<FetchDatabaseCallbackData *>(data);
    ReadClosure *read_closure = static_cast<ReadClosure *>(closure);
    auto err_code = result.error_code();

    if (err_code == remote::DataStoreError::KEY_NOT_FOUND)
    {
        *(fetch_data->found_) = false;
    }
    else if (result.error_code() == remote::DataStoreError::NO_ERROR)
    {
        *(fetch_data->found_) = true;
        fetch_data->db_definition_->append(read_closure->ValueString());
    }
    else
    {
        *(fetch_data->found_) = false;
    }

    fetch_data->Result().set_error_code(result.error_code());
    fetch_data->Result().set_error_msg(result.error_msg());

    fetch_data->Notify();
}

void FetchAllDatabaseCallback(void *data,
                              ::google::protobuf::Closure *closure,
                              DataStoreServiceClient &client,
                              const remote::CommonResult &result)
{
    FetchAllDatabaseCallbackData *fetch_data =
        static_cast<FetchAllDatabaseCallbackData *>(data);

    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);
    auto err_code = result.error_code();
    if (err_code != remote::DataStoreError::NO_ERROR)
    {
        DLOG(INFO) << "FetchAllDatabaseCallback, error_code:" << err_code
                   << ", error_msg: " << result.error_msg();
        fetch_data->Result().set_error_code(result.error_code());
        fetch_data->Result().set_error_msg(result.error_msg());

        fetch_data->Notify();
        return;
    }
    else
    {
        uint32_t items_size = scan_next_closure->ItemsSize();
        std::string key;
        std::string value;
        uint64_t ts;
        uint64_t ttl;
        for (uint32_t i = 0; i < items_size; i++)
        {
            scan_next_closure->GetItem(i, key, value, ts, ttl);
            fetch_data->dbnames_->emplace_back(std::move(key));
        }

        if (items_size < scan_next_closure->BatchSize())
        {
            // has no more data, notify.
            fetch_data->Result().set_error_code(result.error_code());
            fetch_data->Result().set_error_msg(result.error_msg());

            fetch_data->Notify();
            return;
        }
        else
        {
            // has more data, continue to scan.
            fetch_data->session_id_ = scan_next_closure->SessionId();
            client.ScanNext(kv_database_catalogs_name,
                            0,
                            scan_next_closure->ShardId(),
                            fetch_data->dbnames_->back(),
                            fetch_data->end_key_,
                            fetch_data->session_id_,
                            true,
                            false,
                            false,
                            true,
                            scan_next_closure->BatchSize(),
                            nullptr,
                            fetch_data,
                            &FetchAllDatabaseCallback);
        }
    }
}

void DiscoverAllTableNamesCallback(void *data,
                                   ::google::protobuf::Closure *closure,
                                   DataStoreServiceClient &client,
                                   const remote::CommonResult &result)
{
    DiscoverAllTableNamesCallbackData *fetch_data =
        static_cast<DiscoverAllTableNamesCallbackData *>(data);

    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);
    auto err_code = result.error_code();
    if (err_code != remote::DataStoreError::NO_ERROR)
    {
        DLOG(INFO) << "DiscoverAllTableNamesCallback, error_code:" << err_code
                   << ", error_msg: " << result.error_msg();
        fetch_data->Result().set_error_code(result.error_code());
        fetch_data->Result().set_error_msg(result.error_msg());

        fetch_data->Notify();
        return;
    }
    else
    {
        uint32_t items_size = scan_next_closure->ItemsSize();
        std::string key;
        std::string value;
        uint64_t ts;
        uint64_t ttl;
        for (uint32_t i = 0; i < items_size; i++)
        {
            scan_next_closure->GetItem(i, key, value, ts, ttl);
            if (key == txservice::Sequences::table_name_sv_)
            {
                continue;
            }
            fetch_data->table_names_->emplace_back(std::move(key));
        }

        if (items_size < scan_next_closure->BatchSize())
        {
            // has no more data, notify.
            fetch_data->Result().set_error_code(result.error_code());
            fetch_data->Result().set_error_msg(result.error_msg());

            fetch_data->Notify();
            return;
        }
        else
        {
            // has more data, continue to scan.
            fetch_data->session_id_ = scan_next_closure->SessionId();
            client.ScanNext(kv_table_catalogs_name,
                            0,
                            scan_next_closure->ShardId(),
                            fetch_data->table_names_->back(),
                            "",
                            fetch_data->session_id_,
                            true,
                            false,
                            false,
                            true,
                            scan_next_closure->BatchSize(),
                            nullptr,
                            fetch_data,
                            &DiscoverAllTableNamesCallback);
        }
    }
}

void FetchTableRangesCallback(void *data,
                              ::google::protobuf::Closure *closure,
                              DataStoreServiceClient &client,
                              const remote::CommonResult &result)
{
    txservice::FetchTableRangesCc *fetch_range_cc =
        static_cast<txservice::FetchTableRangesCc *>(data);

    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);
    auto err_code = result.error_code();
    if (err_code != remote::DataStoreError::NO_ERROR)
    {
        DLOG(INFO) << "FetchTableRangesCallback, error_code:" << err_code
                   << ", error_msg: " << result.error_msg();

        fetch_range_cc->SetFinish(
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
    }
    else
    {
        txservice::LocalCcShards *shards =
            txservice::Sharder::Instance().GetLocalCcShards();
        std::unique_lock<std::mutex> heap_lk(shards->table_ranges_heap_mux_);
        bool is_override_thd = mi_is_override_thread();
        mi_threadid_t prev_thd =
            mi_override_thread(shards->GetTableRangesHeapThreadId());
        mi_heap_t *prev_heap =
            mi_heap_set_default(shards->GetTableRangesHeap());

        auto catalog_factory =
            client.GetCatalogFactory(fetch_range_cc->table_name_.Engine());
        assert(catalog_factory != nullptr);

        uint32_t items_size = scan_next_closure->ItemsSize();
        std::string key;
        std::string value;
        uint64_t ts;
        uint64_t ttl;

        std::string_view table_name_sv =
            fetch_range_cc->table_name_.StringView();

        std::vector<txservice::InitRangeEntry> range_vec;

        for (uint32_t i = 0; i < items_size; i++)
        {
            scan_next_closure->GetItem(i, key, value, ts, ttl);
            assert(value.size() == (sizeof(int32_t) + sizeof(uint64_t) +
                                    sizeof(uint64_t) + sizeof(uint32_t)));
            const char *buf = value.data();
            int32_t partition_id = *(reinterpret_cast<const int32_t *>(buf));
            buf += sizeof(partition_id);
            uint64_t range_version = *(reinterpret_cast<const uint64_t *>(buf));
            buf += sizeof(range_version);
            uint64_t slice_version = *(reinterpret_cast<const uint64_t *>(buf));
            buf += sizeof(slice_version);

            std::string_view start_key_sv(key.data() + table_name_sv.size(),
                                          key.size() - table_name_sv.size());

            // If the key is 0x00, it means that the key is NegativeInfinity.
            // (see EloqKey::PackedNegativeInfinity)
            if (start_key_sv.size() > 1 || start_key_sv[0] != 0x00)
            {
                txservice::TxKey start_key = catalog_factory->CreateTxKey(
                    start_key_sv.data(), start_key_sv.size());
                range_vec.emplace_back(
                    std::move(start_key), partition_id, range_version);
            }
            else
            {
                range_vec.emplace_back(catalog_factory->NegativeInfKey(),
                                       partition_id,
                                       range_version);
            }
        }

        mi_heap_set_default(prev_heap);
        if (is_override_thd)
        {
            mi_override_thread(prev_thd);
        }
        else
        {
            mi_restore_default_thread_id();
        }
        heap_lk.unlock();

        if (items_size < scan_next_closure->BatchSize())
        {
            // Has no more data, notify.
            if (static_cast<uint32_t>(fetch_range_cc->kv_partition_id_) + 1 <
                client.TotalRangeSlicesKvPartitions())
            {
                fetch_range_cc->AppendTableRanges(std::move(range_vec));
                fetch_range_cc->kv_partition_id_++;
                uint32_t data_shard_id = client.GetShardIdByPartitionId(
                    fetch_range_cc->kv_partition_id_, false);
                fetch_range_cc->kv_start_key_ =
                    fetch_range_cc->table_name_.String();
                fetch_range_cc->kv_end_key_ =
                    fetch_range_cc->table_name_.String();
                fetch_range_cc->kv_end_key_.back()++;
                fetch_range_cc->kv_session_id_.clear();
                client.ScanNext(kv_range_table_name,
                                fetch_range_cc->kv_partition_id_,
                                data_shard_id,
                                fetch_range_cc->kv_start_key_,
                                fetch_range_cc->kv_end_key_,
                                fetch_range_cc->kv_session_id_,
                                true,
                                true,
                                false,
                                true,
                                100,
                                nullptr,
                                fetch_range_cc,
                                &FetchTableRangesCallback);
            }
            else
            {
                assert(fetch_range_cc->kv_partition_id_ + 1 ==
                       client.TotalRangeSlicesKvPartitions());
                // When ddl_skip_kv_ is enabled and the range entry is not
                // physically ready, initializes the original range from
                // negative infinity to positive infinity.
                if (range_vec.empty() && fetch_range_cc->EmptyRanges())
                {
                    range_vec.emplace_back(
                        catalog_factory->NegativeInfKey(),
                        txservice::Sequences::InitialRangePartitionIdOf(
                            fetch_range_cc->table_name_),
                        1);
                }

                fetch_range_cc->AppendTableRanges(std::move(range_vec));
                std::sort(fetch_range_cc->ranges_vec_.begin(),
                          fetch_range_cc->ranges_vec_.end(),
                          [](const txservice::InitRangeEntry &lhs,
                             const txservice::InitRangeEntry &rhs)
                          { return lhs.key_ < rhs.key_; });

                LOG(INFO) << "== fetch table ranges start: table name = "
                          << fetch_range_cc->table_name_.StringView();
                for (auto &range_entry : fetch_range_cc->ranges_vec_)
                {
                    LOG(INFO) << "== range entry key = "
                              << range_entry.key_.ToString();
                }
                LOG(INFO) << "== fetch table ranges stop: table name = "
                          << fetch_range_cc->table_name_.StringView();

                fetch_range_cc->SetFinish(0);
            }
        }
        else
        {
            // has more data, continue to scan.
            fetch_range_cc->kv_session_id_ = scan_next_closure->SessionId();
            fetch_range_cc->kv_start_key_.clear();
            fetch_range_cc->kv_start_key_.append(key);
            fetch_range_cc->AppendTableRanges(std::move(range_vec));

            client.ScanNext(kv_range_table_name,
                            fetch_range_cc->kv_partition_id_,
                            scan_next_closure->ShardId(),
                            fetch_range_cc->kv_start_key_,
                            fetch_range_cc->kv_end_key_,
                            fetch_range_cc->kv_session_id_,
                            true,
                            false,
                            false,
                            true,
                            100,
                            nullptr,
                            fetch_range_cc,
                            &FetchTableRangesCallback);
        }
    }
}

void FetchRangeSlicesCallback(void *data,
                              ::google::protobuf::Closure *closure,
                              DataStoreServiceClient &client,
                              const remote::CommonResult &result)
{
    txservice::FetchRangeSlicesReq *fetch_req =
        static_cast<txservice::FetchRangeSlicesReq *>(data);

    ReadClosure *read_closure = static_cast<ReadClosure *>(closure);

    std::string_view read_val = read_closure->Value();
    txservice::NodeGroupId ng_id = fetch_req->cc_ng_id_;

    if (fetch_req->SegmentCnt() == 0U)
    {
        // step-1: fetched range info.
        if (result.error_code() == remote::DataStoreError::KEY_NOT_FOUND)
        {
            // Failed to read range info.
            assert(false);
            // This should only happen if ddl_skip_kv_ is true.
            fetch_req->slice_info_.emplace_back(
                txservice::TxKey(), 0, txservice::SliceStatus::PartiallyCached);
            fetch_req->SetFinish(txservice::CcErrorCode::NO_ERROR);
            txservice::Sharder::Instance().UnpinNodeGroupData(ng_id);
            return;
        }
        else if (result.error_code() != remote::DataStoreError::NO_ERROR)
        {
            assert(false);
            // Only read partial range slices info. Caller should retry.
            LOG(ERROR) << "Fetch range slices failed: Partial result, keep "
                          "retring";
            fetch_req->SetFinish(txservice::CcErrorCode::DATA_STORE_ERR);
            txservice::Sharder::Instance().UnpinNodeGroupData(ng_id);
            return;
        }
        else
        {
            assert(read_closure->TableName() == kv_range_table_name);
            assert(read_val.size() == (sizeof(int32_t) + sizeof(uint64_t) +
                                       sizeof(uint64_t) + sizeof(uint32_t)));
            const char *buf = read_val.data();
            int32_t range_partition_id =
                *(reinterpret_cast<const int32_t *>(buf));
            buf += sizeof(range_partition_id);
            uint64_t range_version = *(reinterpret_cast<const uint64_t *>(buf));
            buf += sizeof(range_version);
            uint64_t slice_version = *(reinterpret_cast<const uint64_t *>(buf));
            buf += sizeof(slice_version);
            uint32_t segment_cnt = *(reinterpret_cast<const uint32_t *>(buf));

            assert(range_version == fetch_req->range_entry_->Version());
            assert(range_partition_id ==
                   fetch_req->range_entry_->GetRangeInfo()->PartitionId());

            if (segment_cnt == 0)
            {
                assert(fetch_req->slice_info_.empty());
                // New Table, only has range info , no slice info.
                fetch_req->slice_info_.emplace_back(
                    txservice::TxKey(),
                    0,
                    txservice::SliceStatus::PartiallyCached);
                fetch_req->SetFinish(txservice::CcErrorCode::NO_ERROR);
                txservice::Sharder::Instance().UnpinNodeGroupData(ng_id);
                return;
            }
            else
            {
                assert(slice_version > 0);
                fetch_req->SetSliceVersion(slice_version);
                fetch_req->SetSegmentCnt(segment_cnt);
                fetch_req->SetCurrentSegmentId(0);

                fetch_req->kv_start_key_ =
                    client.EncodeRangeSliceKey(fetch_req->table_name_,
                                               range_partition_id,
                                               fetch_req->CurrentSegmentId());

                client.Read(kv_range_slices_table_name,
                            fetch_req->kv_partition_id_,
                            read_closure->ShardId(),
                            fetch_req->kv_start_key_,
                            fetch_req,
                            &FetchRangeSlicesCallback);
            }
        }
    }
    else
    {
        if (result.error_code() == remote::DataStoreError::KEY_NOT_FOUND)
        {
            assert(false);
            // Only read partial range slices info. Caller should retry.
            LOG(ERROR) << "Fetch range slices failed: Partial result, keep "
                          "retring";
            fetch_req->SetFinish(txservice::CcErrorCode::DATA_STORE_ERR);
            txservice::Sharder::Instance().UnpinNodeGroupData(ng_id);
            return;
        }
        else if (result.error_code() != remote::DataStoreError::NO_ERROR)
        {
            // Only read partial range slices info. Caller should retry.
            LOG(ERROR) << "Fetch range slices failed: Partial result, keep "
                          "retring";
            fetch_req->SetFinish(txservice::CcErrorCode::DATA_STORE_ERR);
            txservice::Sharder::Instance().UnpinNodeGroupData(ng_id);
            return;
        }
        else
        {
            uint32_t segment_id = fetch_req->CurrentSegmentId();
            const char *buf = read_val.data();
            size_t offset = 0;
            uint64_t slice_version = *(reinterpret_cast<const uint64_t *>(buf));
            buf += sizeof(slice_version);
            offset += sizeof(slice_version);

            txservice::LocalCcShards *shards =
                txservice::Sharder::Instance().GetLocalCcShards();
            std::unique_lock<std::mutex> heap_lk(
                shards->table_ranges_heap_mux_);
            bool is_override_thd = mi_is_override_thread();
            mi_threadid_t prev_thd =
                mi_override_thread(shards->GetTableRangesHeapThreadId());
            mi_heap_t *prev_heap =
                mi_heap_set_default(shards->GetTableRangesHeap());

            auto catalog_factory =
                client.GetCatalogFactory(fetch_req->table_name_.Engine());
            assert(catalog_factory != nullptr);

            while (offset < read_val.size())
            {
                uint32_t key_len = *(reinterpret_cast<const uint32_t *>(buf));
                buf += sizeof(uint32_t);

                txservice::TxKey tx_key =
                    catalog_factory->CreateTxKey(buf, key_len);
                buf += key_len;

                uint32_t slice_size =
                    *(reinterpret_cast<const uint32_t *>(buf));
                buf += sizeof(uint32_t);

                fetch_req->slice_info_.emplace_back(
                    std::move(tx_key),
                    slice_size,
                    txservice::SliceStatus::PartiallyCached);

                offset += (sizeof(uint32_t) + key_len + sizeof(uint32_t));
            }

            mi_heap_set_default(prev_heap);
            if (is_override_thd)
            {
                mi_override_thread(prev_thd);
            }
            else
            {
                mi_restore_default_thread_id();
            }
            heap_lk.unlock();

            segment_id++;
            if (segment_id == fetch_req->SegmentCnt())
            {
                // All segments are fetched.
                fetch_req->SetFinish(txservice::CcErrorCode::NO_ERROR);
                txservice::Sharder::Instance().UnpinNodeGroupData(ng_id);
                return;
            }

            fetch_req->SetCurrentSegmentId(segment_id);
            client.UpdateEncodedRangeSliceKey(fetch_req->kv_start_key_,
                                              fetch_req->CurrentSegmentId());

            client.Read(kv_range_slices_table_name,
                        fetch_req->kv_partition_id_,
                        read_closure->ShardId(),
                        fetch_req->kv_start_key_,
                        fetch_req,

                        &FetchRangeSlicesCallback);
        }
    }
}

void FetchCurrentTableStatsCallback(void *data,
                                    ::google::protobuf::Closure *closure,
                                    DataStoreServiceClient &client,
                                    const remote::CommonResult &result)
{
    auto *read_closure = static_cast<ReadClosure *>(closure);
    auto *fetch_cc = static_cast<txservice::FetchTableStatisticsCc *>(data);
    auto err_code = result.error_code();
    if (err_code == remote::DataStoreError::KEY_NOT_FOUND)
    {
        // empty statistics
        fetch_cc->SetFinish(0);
    }
    else if (err_code != remote::DataStoreError::NO_ERROR)
    {
        fetch_cc->SetFinish(
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
    }
    else
    {
        const std::string &val = read_closure->ValueString();
        uint64_t ckpt_version = std::stoull(val);
        fetch_cc->SetCurrentVersion(ckpt_version);
        fetch_cc->StoreHandler()->FetchTableStatistics(fetch_cc->CatalogName(),
                                                       fetch_cc);
    }
}

void FetchTableStatsCallback(void *data,
                             ::google::protobuf::Closure *closure,
                             DataStoreServiceClient &client,
                             const remote::CommonResult &result)
{
    txservice::FetchTableStatisticsCc *fetch_cc =
        static_cast<txservice::FetchTableStatisticsCc *>(data);
    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);
    auto err_code = result.error_code();

    if (err_code != remote::DataStoreError::NO_ERROR)
    {
        DLOG(INFO) << "FetchTableStatsCallback, error_code:" << err_code
                   << ", error_msg: " << result.error_msg();

        fetch_cc->SetFinish(
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
        return;
    }
    else
    {
        std::string key;
        std::string value;
        uint64_t ts;
        uint64_t ttl;
        uint32_t items_size = scan_next_closure->ItemsSize();
        fetch_cc->kv_session_id_ = scan_next_closure->SessionId();
        assert(items_size <= 1);
        if (items_size == 1)
        {
            scan_next_closure->GetItem(0, key, value, ts, ttl);

            auto &base_table_name = fetch_cc->CatalogName();
            std::string_view base_table_sv = base_table_name.StringView();
            size_t offset =
                base_table_sv.size() + sizeof(uint64_t) + sizeof(uint32_t);

            std::string indexname_str = key.substr(offset);

            const char *value_buf = value.data();
            size_t value_size = value.size();
            offset = 0;
            uint8_t indextype_val =
                *(reinterpret_cast<const uint8_t *>(value_buf));
            offset += sizeof(uint8_t);
            auto indextype = static_cast<txservice::TableType>(indextype_val);
            txservice::TableName indexname(std::move(indexname_str),
                                           indextype,
                                           fetch_cc->CatalogName().Engine());

            uint64_t records_cnt =
                *(reinterpret_cast<const uint64_t *>(value_buf + offset));
            offset += sizeof(uint64_t);
            if (records_cnt > 0)
            {
                fetch_cc->SetRecords(indexname, records_cnt);
            }
            std::vector<txservice::TxKey> samplekeys;

            auto catalog_factory =
                client.GetCatalogFactory(fetch_cc->CatalogName().Engine());
            assert(catalog_factory != nullptr);

            while (offset < value_size)
            {
                uint32_t samplekey_len =
                    *(reinterpret_cast<const uint32_t *>(value_buf + offset));
                offset += sizeof(uint32_t);

                txservice::TxKey samplekey = catalog_factory->CreateTxKey(
                    value_buf + offset, samplekey_len);
                offset += samplekey_len;
                samplekeys.emplace_back(std::move(samplekey));
            }
            fetch_cc->SamplePoolMergeFrom(indexname, std::move(samplekeys));

            // continue to scan
            fetch_cc->kv_start_key_ = std::move(key);
            client.ScanNext(kv_table_statistics_name,
                            fetch_cc->kv_partition_id_,
                            scan_next_closure->ShardId(),
                            fetch_cc->kv_start_key_,
                            fetch_cc->kv_end_key_,
                            fetch_cc->kv_session_id_,
                            true,
                            false,
                            false,
                            true,
                            1,
                            nullptr,
                            fetch_cc,
                            &FetchTableStatsCallback);
        }
        else
        {
            // has no more data, notify.

            fetch_cc->SetFinish(0);
            return;
        }
    }
}

void LoadRangeSliceCallback(void *data,
                            ::google::protobuf::Closure *closure,
                            DataStoreServiceClient &client,
                            const remote::CommonResult &result)
{
    assert(data != nullptr);
    auto *fill_store_slice_req =
        static_cast<txservice::FillStoreSliceCc *>(data);
    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);

    if (result.error_code() != EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DataStoreHandler: Failed to do LoadRangeSlice. "
                   << result.error_msg();
        fill_store_slice_req->SetKvFinish(false);
        txservice::Sharder::Instance().UnpinNodeGroupData(
            fill_store_slice_req->NodeGroup());
        return;
    }

    // Process records from this batch
    const txservice::TableName &table_name = fill_store_slice_req->TblName();
    uint32_t items_size = scan_next_closure->ItemsSize();

    auto catalog_factory = client.GetCatalogFactory(table_name.Engine());
    assert(catalog_factory != nullptr);

    std::string key_str, value_str;
    uint64_t ts, ttl;
    for (uint32_t i = 0; i < items_size; i++)
    {
        scan_next_closure->GetItem(i, key_str, value_str, ts, ttl);
        txservice::TxKey key =
            catalog_factory->CreateTxKey(key_str.data(), key_str.size());
        std::unique_ptr<txservice::TxRecord> record =
            catalog_factory->CreateTxRecord();
        bool is_deleted = false;
        if (table_name.Engine() == txservice::TableEngine::EloqKv)
        {
            // Hash partition
            txservice::TxObject *tx_object =
                reinterpret_cast<txservice::TxObject *>(record.get());
            size_t offset = 0;
            record = tx_object->DeserializeObject(value_str.data(), offset);
        }
        else
        {
            // Range partition
            size_t offset = 0;
            is_deleted = false;

            if (!DataStoreServiceClient::DeserializeTxRecordStr(
                    value_str, is_deleted, offset))
            {
                LOG(ERROR) << "DataStoreServiceClient::LoadRangeSliceCallback: "
                              "Failed to decode is_deleted. txkey: "
                           << key_str;
                assert(false);
                std::abort();
            }

            if (!is_deleted)
            {
                record->Deserialize(value_str.data(), offset);
            }
        }

        if (i == items_size - 1)
        {
            fill_store_slice_req->kv_start_key_ =
                std::string_view(key.Data(), key.Size());
        }

        fill_store_slice_req->AddDataItem(
            std::move(key), std::move(record), ts, is_deleted);
    }

    fill_store_slice_req->kv_session_id_ = scan_next_closure->GetSessionId();
    if (scan_next_closure->ItemsSize() == 1000)
    {
        // has more data, continue to scan.
        client.ScanNext(*fill_store_slice_req->kv_table_name_,
                        fill_store_slice_req->kv_partition_id_,
                        scan_next_closure->ShardId(),
                        fill_store_slice_req->kv_start_key_,
                        fill_store_slice_req->kv_end_key_,
                        fill_store_slice_req->kv_session_id_,
                        true,
                        false,  // include start_key
                        false,  // include end_key
                        true,   // scan forward
                        1000,
                        nullptr,
                        fill_store_slice_req,
                        &LoadRangeSliceCallback);
    }
    else
    {
        fill_store_slice_req->SetKvFinish(true);
        txservice::Sharder::Instance().UnpinNodeGroupData(
            fill_store_slice_req->NodeGroup());
    }
}

void FetchArchivesCallback(void *data,
                           ::google::protobuf::Closure *closure,
                           DataStoreServiceClient &client,
                           const remote::CommonResult &result)
{
    FetchArchivesCallbackData *fetch_data =
        static_cast<FetchArchivesCallbackData *>(data);
    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);
    auto err_code = result.error_code();

    if (err_code != remote::DataStoreError::NO_ERROR)
    {
        DLOG(INFO) << "FetchArchivesCallback, error_code:" << err_code
                   << ", error_msg: " << result.error_msg();
        fetch_data->Result().set_error_code(result.error_code());
        fetch_data->Result().set_error_msg(result.error_msg());
        fetch_data->Notify();
        return;
    }

    uint32_t items_size = scan_next_closure->ItemsSize();
    std::string archive_key;
    std::string archive_value;
    uint64_t commit_ts;
    uint64_t ttl;

    for (uint32_t i = 0; i < items_size; i++)
    {
        scan_next_closure->GetItem(
            i, archive_key, archive_value, commit_ts, ttl);
        fetch_data->archive_values_.emplace_back(std::move(archive_value));
        fetch_data->archive_commit_ts_.emplace_back(commit_ts);
    }
    // set the start key of next scan batch
    fetch_data->start_key_ = std::move(archive_key);

    if (items_size < scan_next_closure->BatchSize() ||
        items_size >= fetch_data->limit_)
    {
        // has no more data, notify
        fetch_data->Result().set_error_code(remote::DataStoreError::NO_ERROR);
        fetch_data->Notify();
    }
    else
    {
        client.ScanNext(fetch_data->kv_table_name_,
                        fetch_data->partition_id_,
                        scan_next_closure->ShardId(),
                        fetch_data->start_key_,
                        fetch_data->end_key_,
                        scan_next_closure->SessionId(),
                        true,
                        false,
                        false,
                        fetch_data->scan_forward_,
                        fetch_data->batch_size_,
                        nullptr,
                        fetch_data,
                        &FetchArchivesCallback);
    }
}

void FetchRecordArchivesCallback(void *data,
                                 ::google::protobuf::Closure *closure,
                                 DataStoreServiceClient &client,
                                 const remote::CommonResult &result)
{
    txservice::FetchRecordCc *fetch_cc =
        static_cast<txservice::FetchRecordCc *>(data);

    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);
    auto err_code = result.error_code();

    if (err_code != remote::DataStoreError::NO_ERROR)
    {
        assert(err_code != remote::DataStoreError::KEY_NOT_FOUND);
        DLOG(INFO) << "FetchRecordArchivesCallback, error_code:" << err_code
                   << ", error_msg: " << result.error_msg();
        fetch_cc->SetFinish(
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
        return;
    }

    uint32_t items_size = scan_next_closure->ItemsSize();
    DLOG(INFO) << "FetchRecordArchivesCallback, items_size:" << items_size;
    std::string archive_key;
    std::string archive_value;
    uint64_t commit_ts;
    uint64_t ttl;

    if (fetch_cc->archive_records_ == nullptr)
    {
        fetch_cc->archive_records_ = std::make_unique<std::vector<
            std::tuple<uint64_t, txservice::RecordStatus, std::string>>>();
    }
    auto &archive_records = *fetch_cc->archive_records_;

    archive_records.reserve(archive_records.size() + items_size);
    for (uint32_t i = 0; i < items_size; i++)
    {
        scan_next_closure->GetItem(
            i, archive_key, archive_value, commit_ts, ttl);
        // parse archive_value
        bool is_deleted = false;
        size_t value_offset = 0;
        client.DecodeArchiveValue(archive_value, is_deleted, value_offset);
        if (is_deleted)
        {
            archive_records.emplace_back(
                commit_ts, txservice::RecordStatus::Deleted, "");
        }
        else
        {
            std::string record_str(archive_value.data() + value_offset,
                                   archive_value.size() - value_offset);
            assert(record_str.size() > 0);
            archive_records.emplace_back(commit_ts,
                                         txservice::RecordStatus::Normal,
                                         std::move(record_str));
        }
    }

    if (scan_next_closure->BatchSize() == 1 &&
        !scan_next_closure->ScanForward())
    {
        if (items_size == 0)
        {
            // Not found the visible archive version in the archives table.
            assert(archive_records.size() == 0);
            archive_records.emplace_back(
                1U, txservice::RecordStatus::Deleted, "");

            fetch_cc->kv_start_key_ = client.EncodeArchiveKey(
                fetch_cc->kv_table_name_,
                std::string_view(fetch_cc->tx_key_.Data(),
                                 fetch_cc->tx_key_.Size()),
                EloqShare::host_to_big_endian(1U));
        }
        else
        {
            fetch_cc->kv_start_key_ = std::move(archive_key);
        }

        // Fetched the visible version, next scan is fetching all the
        // archives whose commit_ts is bigger than the visible version.
        fetch_cc->kv_end_key_ =
            client.EncodeArchiveKey(fetch_cc->kv_table_name_,
                                    std::string_view(fetch_cc->tx_key_.Data(),
                                                     fetch_cc->tx_key_.Size()),
                                    EloqShare::host_to_big_endian(UINT64_MAX));

        fetch_cc->kv_session_id_.clear();

        client.ScanNext(kv_mvcc_archive_name,
                        fetch_cc->partition_id_,
                        scan_next_closure->ShardId(),
                        fetch_cc->kv_start_key_,
                        fetch_cc->kv_end_key_,
                        fetch_cc->kv_session_id_,
                        true,
                        false,
                        false,
                        true,
                        100,
                        nullptr,
                        fetch_cc,
                        &FetchRecordArchivesCallback);
    }
    else if (items_size < scan_next_closure->BatchSize())
    {
        assert(archive_records.size() > 0);
        fetch_cc->SetFinish(0);
    }
    else
    {
        // set the start key of next scan batch
        fetch_cc->kv_start_key_ = std::move(archive_key);
        fetch_cc->kv_session_id_ = scan_next_closure->SessionId();

        client.ScanNext(kv_mvcc_archive_name,
                        fetch_cc->partition_id_,
                        scan_next_closure->ShardId(),
                        fetch_cc->kv_start_key_,
                        fetch_cc->kv_end_key_,
                        fetch_cc->kv_session_id_,
                        true,
                        false,
                        false,
                        true,
                        100,
                        nullptr,
                        fetch_cc,
                        &FetchRecordArchivesCallback);
    }
}

void FetchSnapshotArchiveCallback(void *data,
                                  ::google::protobuf::Closure *closure,
                                  DataStoreServiceClient &client,
                                  const remote::CommonResult &result)
{
    txservice::FetchSnapshotCc *fetch_cc =
        static_cast<txservice::FetchSnapshotCc *>(data);
    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);
    auto err_code = result.error_code();

    if (err_code != remote::DataStoreError::NO_ERROR)
    {
        assert(err_code != remote::DataStoreError::KEY_NOT_FOUND);
        DLOG(INFO) << "FetchSnapshotArchiveCallback, error_code:" << err_code
                   << ", error_msg: " << result.error_msg();
        fetch_cc->SetFinish(
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
        return;
    }

    uint32_t items_size = scan_next_closure->ItemsSize();
    DLOG(INFO) << "FetchSnapshotArchiveCallback, items_size:" << items_size;

    if (items_size == 1)
    {
        std::string archive_key;
        std::string archive_value;
        uint64_t commit_ts;
        uint64_t ttl;
        assert(items_size <= 1);

        scan_next_closure->GetItem(
            0, archive_key, archive_value, commit_ts, ttl);

        // parse archive_value
        bool is_deleted = false;
        size_t value_offset = 0;
        client.DecodeArchiveValue(archive_value, is_deleted, value_offset);
        if (is_deleted)
        {
            fetch_cc->rec_ts_ = commit_ts;
            fetch_cc->rec_status_ = txservice::RecordStatus::Deleted;
        }
        else
        {
            fetch_cc->rec_str_.assign(archive_value.data() + value_offset,
                                      archive_value.size() - value_offset);
            fetch_cc->rec_ts_ = commit_ts;
            fetch_cc->rec_status_ = txservice::RecordStatus::Normal;
        }
    }
    else
    {
        // Not found the visible archive version in the archives table.
        fetch_cc->rec_ts_ = 1U;
        fetch_cc->rec_status_ = txservice::RecordStatus::Deleted;
    }

    fetch_cc->SetFinish(0);
}

void CreateSnapshotForBackupCallback(void *data,
                                     ::google::protobuf::Closure *closure,
                                     DataStoreServiceClient &client,
                                     const remote::CommonResult &result)
{
    auto *backup_callback_data =
        static_cast<CreateSnapshotForBackupCallbackData *>(data);
    auto *backup_closure =
        static_cast<CreateSnapshotForBackupClosure *>(closure);

    backup_callback_data->Result().set_error_code(result.error_code());
    backup_callback_data->Result().set_error_msg(result.error_msg());

    bool is_local_request = backup_closure->IsLocalRequest();

    if (result.error_code() != remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DataStoreHandler: Failed to do CreateSnapshotForBackup. "
                   << result.error_msg();
    }
    else
    {
        if (!is_local_request)
        {
            backup_callback_data->backup_files_->swap(
                *backup_closure->BackupFiles());
        }
    }

    backup_callback_data->Notify();
}

bool PartitionFlushState::GetNextBatch(PartitionBatchRequest &batch)
{
    std::unique_lock<bthread::Mutex> lk(mux);
    if (pending_batches.empty())
    {
        return false;
    }
    batch = std::move(pending_batches.front());
    pending_batches.pop();
    return true;
}
}  // namespace EloqDS
