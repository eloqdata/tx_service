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

#include "data_store_service_scanner.h"

#include <glog/logging.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "data_store_service_client_closure.h"
#include "eloq_data_store_service/object_pool.h"
#include "tx_service/include/tx_key.h"

namespace EloqDS
{

thread_local ObjectPool<SinglePartitionScanner> single_partition_scanner_pool_;

bool SinglePartitionScanner::FetchNextBatch()
{
    if (IsRunOutOfData())
    {
        return false;
    }

    if (!scanner_)
    {
        LOG(ERROR) << "Invalid scanner parameters: scanner is null";
        return false;
    }

    DataStoreServiceClient *client = scanner_->GetClient();
    if (!client)
    {
        LOG(ERROR) << "Invalid scanner parameters: client is null";
        return false;
    }

    ResetCache();

    scanner_->IncrementInFlightFetchCount();
    client->ScanNext(
        scanner_->GetTableName(),
        partition_id_,
        last_key_,
        scanner_->GetEndKey(),
        session_id_,
        true,
        // only set inclusive_start for the first batch
        first_batch_fetched_ ? false : scanner_->IsInclusiveStart(),
        scanner_->IsInclusiveEnd(),
        scanner_->IsScanForward(),
        scanner_->GetBatchSize(),
        // scanner_->SearchConditions(),
        nullptr,
        this,
        ProcessScanNextResult);

    first_batch_fetched_ = true;

    return true;
}

bool SinglePartitionScanner::ScanClose()
{
    if (!scanner_)
    {
        LOG(ERROR) << "Invalid scanner parameters: scanner is null";
        return false;
    }

    DataStoreServiceClient *client = scanner_->GetClient();
    if (!client)
    {
        LOG(ERROR) << "Invalid scanner parameters: client is null";
        return false;
    }

    scanner_->IncrementInFlightFetchCount();

    assert(last_batch_size_ <= scanner_->GetBatchSize());
    // If parition scanner is not run out of data,
    // we need to close the scanner
    if (last_batch_size_ == scanner_->GetBatchSize())
    {
        client->ScanClose(scanner_->GetTableName(),
                          partition_id_,
                          session_id_,
                          this,
                          ProcessScanCloseResult);
    }

    return true;
}

void SinglePartitionScanner::ProcessScanNextResult(
    void *data,
    ::google::protobuf::Closure *closure,
    DataStoreServiceClient &client,
    const remote::CommonResult &result)
{
    SinglePartitionScanner *sp_scanner =
        static_cast<SinglePartitionScanner *>(data);

    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);

    if (result.error_code() != remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "Failed to fetch next batch: " << result.error_msg();
        sp_scanner->scanner_->SetError(
            static_cast<remote::DataStoreError>(result.error_code()),
            result.error_msg());
        sp_scanner->scanner_->DecrementInFlightFetchCount();
        return;
    }

    uint32_t items_size = scan_next_closure->ItemsSize();
    sp_scanner->last_batch_size_ = items_size;
    sp_scanner->session_id_ = scan_next_closure->GetSessionId();

    uint64_t now = txservice::LocalCcShards::ClockTsInMillseconds();
    for (uint32_t i = 0; i < items_size; i++)
    {
        std::string key, value;
        uint64_t ts, ttl;
        scan_next_closure->GetItem(i, key, value, ts, ttl);
        if (i == sp_scanner->last_batch_size_ - 1)
        {
            sp_scanner->last_key_ = key;
        }

        if (ttl > 0 && ttl < now)
        {
            // TTL expired record
            DLOG(INFO) << "TTL expired record, key: " << key << ", ttl: " << ttl
                       << ", now: " << now;
            continue;
        }

        sp_scanner->scan_cache_.emplace_back(
            std::move(key), std::move(value), ts, ttl);
    }

    sp_scanner->scanner_->DecrementInFlightFetchCount();
}

void SinglePartitionScanner::ProcessScanCloseResult(
    void *data,
    ::google::protobuf::Closure *closure,
    DataStoreServiceClient &client,
    const remote::CommonResult &result)
{
    SinglePartitionScanner *sp_scanner =
        static_cast<SinglePartitionScanner *>(data);

    if (result.error_code() != remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "Failed to fetch next batch: " << result.error_msg();
        sp_scanner->scanner_->SetError(
            static_cast<remote::DataStoreError>(result.error_code()),
            result.error_msg());
    }

    sp_scanner->scanner_->DecrementInFlightFetchCount();
}

bool SinglePartitionScanner::IsRunOutOfData()
{
    return last_batch_size_ < scanner_->GetBatchSize();
}

ScanTuple &SinglePartitionScanner::NextScanTuple()
{
    assert(head_ < scan_cache_.size());
    return scan_cache_[head_++];
}

void SinglePartitionScanner::ResetCache()
{
    head_ = 0;
    scan_cache_.clear();
}

template <bool ScanForward>
DataStoreServiceHashPartitionScanner<ScanForward>::
    DataStoreServiceHashPartitionScanner(
        DataStoreServiceClient *client,
        const txservice::CatalogFactory *catalog_factory,
        const txservice::KeySchema *key_sch,
        const txservice::RecordSchema *rec_sch,
        const txservice::TableName &table_name,
        const txservice::KVCatalogInfo *kv_info,
        const txservice::TxKey &start_key,
        bool inclusive,
        const std::vector<txservice::DataStoreSearchCond> &pushdown_cond,
        size_t batch_size)
    : DataStoreServiceScanner(client,
                              kv_info->GetKvTableName(table_name),
                              inclusive,  // inclusive_start
                              false,      // inclusive_end
                              "",         // end_key
                              ScanForward,
                              batch_size),
      catalog_factory_(catalog_factory),
      key_sch_(key_sch),
      rec_sch_(rec_sch),
      kv_info_(kv_info),
      pushdown_condition_(pushdown_cond),
      is_object_key_schema_(table_name.Engine() ==
                            txservice::TableEngine::EloqKv),
      initialized_(false)
{
    assert(client_ != nullptr);
    if (start_key.Type() == txservice::KeyType::NegativeInf ||
        start_key.Type() == txservice::KeyType::PositiveInf)
    {
        start_key_ = "";
    }
    else
    {
        start_key_ = start_key.ToString();
    }
    end_key_ = "";

    // convert pushdown conditions to search conditions
    for (const auto &cond : pushdown_cond)
    {
        remote::SearchCondition search_cond;
        search_cond.set_field_name(cond.field_name_);
        search_cond.set_op(cond.op_);
        search_cond.set_value(cond.val_str_);
        search_conditions_.emplace_back(std::move(search_cond));
    }
}

template <bool ScanForward>
DataStoreServiceHashPartitionScanner<
    ScanForward>::~DataStoreServiceHashPartitionScanner()
{
    // wait for all in-flight fetches to complete
    WaitUntilInFlightFetchesComplete();
    CloseScan();
}

template <bool ScanForward>
void DataStoreServiceHashPartitionScanner<ScanForward>::Current(
    txservice::TxKey &key,
    const txservice::TxRecord *&rec,
    uint64_t &version_ts,
    bool &deleted)
{
    if (!initialized_)
    {
        LOG(ERROR) << "Scanner is not initialized";
        return;
    }

    if (error_code_ != remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "Scanner error, error code: " << error_code_
                   << ", error msg: " << error_msg_;
        key = txservice::TxKey();
        rec = nullptr;
        version_ts = 0;
        deleted = false;
        return;
    }

    if (IsRunOutOfData())
    {
        key = txservice::TxKey();
        rec = nullptr;
        version_ts = 0;
        deleted = false;
        return;
    }

    const ScanHeapTuple<txservice::TxKey, txservice::TxRecord> &top =
        result_cache_.top();
    key = top.key_->GetShallowCopy();
    rec = top.rec_.get();
    version_ts = top.version_ts_;
    deleted = top.deleted_;
}

template <bool ScanForward>
bool DataStoreServiceHashPartitionScanner<ScanForward>::IsRunOutOfData()
{
    if (!initialized_)
    {
        return true;
    }

    // when the scanner is initialized, the result cache is empty and
    // there is no in-flight fetch
    return result_cache_.empty() &&
           in_flying_fetch_cnt_.load(std::memory_order_acquire) == 0;
}

template <bool ScanForward>
bool DataStoreServiceHashPartitionScanner<ScanForward>::MoveNext()
{
    // Initialize if not already done
    if (!initialized_)
    {
        return Init();
    }

    // Quick check if scanner is completely empty
    if (IsRunOutOfData())
    {
        return false;
    }

    if (error_code_ != remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "Scanner error, error code: " << error_code_
                   << ", error msg: " << error_msg_;
        return false;
    }

    // Extract the next item from the result cache
    const ScanHeapTuple<txservice::TxKey, txservice::TxRecord> &top =
        result_cache_.top();
    uint32_t part_id = top.sid_;
    result_cache_.pop();

    // Get the corresponding partition scanner
    SinglePartitionScanner *part_scanner = partition_scanners_[part_id];

    // Handle scanner lifecycle
    assert(part_scanner != nullptr);
    if (!part_scanner->IsCacheEmpty())
    {
        AddScanTuple(part_scanner, part_id);
    }
    else
    {
        if (part_scanner->IsRunOutOfData())
        {
            // Free the partition scanner if it's run out of data
            PoolableGuard guard(part_scanner);
            partition_scanners_[part_id] = nullptr;
        }
        else
        {
            // Fetch next batch asynchronously
            DLOG(INFO) << "Fetching next batch for partition scanner "
                       << part_id;
            part_scanner->FetchNextBatch();

            // Wait the result of this partition.
            WaitUntilInFlightFetchesComplete();

            if (part_scanner->IsCacheEmpty())
            {
                // No more data in this partition.
                assert(part_scanner->IsRunOutOfData());
                // Free the partition scanner if it's run out of data
                PoolableGuard guard(part_scanner);
                partition_scanners_[part_id] = nullptr;
                return false;
            }
            // Get the key
            AddScanTuple(part_scanner, part_id);
        }
    }

    return true;
}

template <bool ScanForward>
bool DataStoreServiceHashPartitionScanner<ScanForward>::Init()
{
    if (initialized_)
    {
        return true;
    }

    initialized_ = true;

    if (!client_)
    {
        LOG(ERROR) << "Invalid scanner parameters: client is null";
        return false;
    }

    partition_scanners_.reserve(HASH_PARTITION_COUNT);
    for (uint32_t part_cnt = 0; part_cnt < HASH_PARTITION_COUNT; part_cnt++)
    {
        auto *part_scanner = single_partition_scanner_pool_.NextObject();
        part_scanner->Reset(this, part_cnt, start_key_);
        partition_scanners_.push_back(part_scanner);
        // ignore the return value, since the scanner is not used
        part_scanner->FetchNextBatch();
    }

    // Wait the initial HASH_PARTITION_COUNT fetches to finish,
    // otherwise the result_cache_ is not overall ordered
    WaitUntilInFlightFetchesComplete();

    // Get the keys
    for (uint32_t part_id = 0; part_id < HASH_PARTITION_COUNT; ++part_id)
    {
        auto &part_scanner = partition_scanners_[part_id];
        if (!part_scanner->IsCacheEmpty())
        {
            AddScanTuple(part_scanner, part_id);
        }
        else
        {
            // There must be no data in this partition. Recycly this scanner.
            PoolableGuard guard(part_scanner);
            part_scanner = nullptr;
        }
    }

    return true;
}

template <bool ScanForward>
void DataStoreServiceHashPartitionScanner<ScanForward>::AddScanTuple(
    SinglePartitionScanner *part_scanner, uint32_t part_id)
{
    assert(part_scanner);
    auto &part_scan_tuple = part_scanner->NextScanTuple();
    ScanHeapTuple<txservice::TxKey, txservice::TxRecord> scan_tuple(
        part_id,
        std::make_unique<txservice::TxKey>(catalog_factory_->CreateTxKey()),
        catalog_factory_->CreateTxRecord());

    scan_tuple.key_->SetPackedKey(part_scan_tuple.key_.data(),
                                  part_scan_tuple.key_.size());

    size_t offset = 0;
    if (is_object_key_schema_)
    {
        txservice::TxObject *tx_obj =
            static_cast<txservice::TxObject *>(scan_tuple.rec_.get());
        txservice::TxRecord::Uptr obj_uptr =
            tx_obj->DeserializeObject(part_scan_tuple.value_.data(), offset);
        scan_tuple.rec_.reset(obj_uptr.release());
    }
    else
    {
        scan_tuple.rec_->Deserialize(part_scan_tuple.value_.data(), offset);
    }

    scan_tuple.version_ts_ = part_scan_tuple.ts_;
    scan_tuple.deleted_ = false;

    result_cache_.push(std::move(scan_tuple));
}

template <bool ScanForward>
void DataStoreServiceHashPartitionScanner<ScanForward>::End()
{
    // do nothing
}

template <bool ScanForward>
bool DataStoreServiceHashPartitionScanner<ScanForward>::CloseScan()
{
    DLOG(INFO) << "Closing scanner";
    if (!initialized_)
    {
        return true;
    }

    // close all partition scanners if they are not run out of data
    for (auto *part_scanner : partition_scanners_)
    {
        if (part_scanner != nullptr && !part_scanner->IsRunOutOfData())
        {
            part_scanner->ScanClose();
        }
    }

    // wait until all in-flight scan close finish
    WaitUntilInFlightFetchesComplete();

    // free all partition scanners
    for (auto *part_scanner : partition_scanners_)
    {
        if (part_scanner)
        {
            DLOG(INFO) << "Free partition scanner "
                       << part_scanner->GetPartitionId();
            PoolableGuard guard(part_scanner);
        }
    }

    DLOG(INFO) << "Freeing partition scanners";

    return true;
}

template class DataStoreServiceHashPartitionScanner<true>;
template class DataStoreServiceHashPartitionScanner<false>;

}  // namespace EloqDS
