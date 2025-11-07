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

#pragma once

#include <deque>
#include <memory>
#include <queue>
#include <string>
#include <vector>

#include "data_store_service_client.h"
#include "eloq_data_store_service/data_store_service_util.h"
#include "eloq_data_store_service/object_pool.h"
#include "kv_store.h"
#include "schema.h"
#include "store/data_store_scanner.h"
#include "tx_key.h"
#include "tx_record.h"

namespace EloqDS
{

template <bool ScanForward>
class DataStoreServiceHashPartitionScanner;
class SinglePartitionScanner;
class DataStoreServiceScanner;

class DataStoreServiceScanner
{
public:
    DataStoreServiceScanner(DataStoreServiceClient *client,
                            const std::string &table_name,
                            bool inclusive_start,
                            bool inclusive_end,
                            const std::string &end_key,
                            bool scan_forward,
                            uint32_t batch_size)
        : client_(client),
          table_name_(table_name),
          end_key_(end_key),
          inclusive_start_(inclusive_start),
          inclusive_end_(inclusive_end),
          scan_forward_(scan_forward),
          batch_size_(batch_size),
          search_conditions_(),
          error_code_(::EloqDS::remote::DataStoreError::NO_ERROR),
          error_msg_("")
    {
    }

    virtual ~DataStoreServiceScanner() = default;

    /**
     * @brief Initialize the scanner
     *        Initialize the sub partition scanners
     */
    virtual bool Init() = 0;

    DataStoreServiceClient *GetClient()
    {
        return client_;
    }
    const std::string &GetTableName()
    {
        return table_name_;
    }
    const std::string_view GetStartKey()
    {
        return start_key_;
    }
    const std::string_view GetEndKey()
    {
        return end_key_;
    }
    uint32_t GetBatchSize()
    {
        return batch_size_;
    }
    bool IsInclusiveStart()
    {
        return inclusive_start_;
    }
    bool IsInclusiveEnd()
    {
        return inclusive_end_;
    }
    bool IsScanForward()
    {
        return scan_forward_;
    }

    const std::vector<remote::SearchCondition> *SearchConditions()
    {
        return &search_conditions_;
    }

    void SetError(::EloqDS::remote::DataStoreError error_code,
                  const std::string &error_msg)
    {
        auto expected = ::EloqDS::remote::DataStoreError::NO_ERROR;
        if (error_code_.compare_exchange_strong(
                expected, error_code, std::memory_order_acq_rel))
        {
            error_msg_ = error_msg;
        }
    }

    ::EloqDS::remote::DataStoreError GetErrorCode()
    {
        return error_code_;
    }

    const std::string &GetErrorMsg()
    {
        return error_msg_;
    }

protected:
    void IncrementInFlightFetchCount()
    {
        in_flying_fetch_cnt_.fetch_add(1);
    }

    void DecrementInFlightFetchCount()
    {
        uint32_t prev = in_flying_fetch_cnt_.fetch_sub(1);
        if (prev == 1)
        {
            std::unique_lock<bthread::Mutex> lk(in_flying_fetch_mutex_);
            if (in_flying_fetch_cnt_ == 0)
            {
                in_flying_fetch_cv_.notify_all();
            }
        }
    }

    void WaitUntilInFlightFetchesComplete()
    {
        if (in_flying_fetch_cnt_.load(std::memory_order_acquire) == 0)
        {
            return;
        }
        else
        {
            std::unique_lock<bthread::Mutex> lk(in_flying_fetch_mutex_);
            while (in_flying_fetch_cnt_ > 0)
            {
                in_flying_fetch_cv_.wait(lk);
            }
        }
    }

protected:
    // initialized parameters
    DataStoreServiceClient *client_;
    const std::string &table_name_;
    std::string start_key_;
    std::string end_key_;
    bool inclusive_start_;
    bool inclusive_end_;
    bool scan_forward_;
    const uint32_t batch_size_;
    std::vector<remote::SearchCondition> search_conditions_;

    // count of the on flying fetch
    std::atomic<uint32_t> in_flying_fetch_cnt_{0};
    // mutex for wait on_flying_fetch_cnt_ to be zero
    bthread::Mutex in_flying_fetch_mutex_;
    bthread::ConditionVariable in_flying_fetch_cv_;

    // internal state
    std::atomic<::EloqDS::remote::DataStoreError> error_code_{
        ::EloqDS::remote::DataStoreError::NO_ERROR};
    std::string error_msg_{""};

    friend class SinglePartitionScanner;
};

class SinglePartitionScanner : public Poolable
{
public:
    SinglePartitionScanner()
        : scanner_(nullptr), partition_id_(0), last_key_(""), session_id_("")
    {
    }

    void Reset(DataStoreServiceScanner *scanner,
               uint32_t partition_id,
               const std::string_view start_key)
    {
        scanner_ = scanner;
        partition_id_ = partition_id;
        last_key_ = start_key;
        session_id_ = "";
        last_batch_size_ = scanner_->GetBatchSize();
        first_batch_fetched_ = false;
        head_ = 0;
        scan_cache_.reserve(scanner_->GetBatchSize());
    }

    bool FetchNextBatch();
    bool ScanClose();
    static void ProcessScanNextResult(void *data,
                                      ::google::protobuf::Closure *closure,
                                      DataStoreServiceClient &client,
                                      const remote::CommonResult &result);
    static void ProcessScanCloseResult(void *data,
                                       ::google::protobuf::Closure *closure,
                                       DataStoreServiceClient &client,
                                       const remote::CommonResult &result);
    bool IsRunOutOfData();

    uint32_t GetPartitionId()
    {
        return partition_id_;
    }

    ScanTuple &NextScanTuple();

    bool IsCacheEmpty() const
    {
        return head_ == scan_cache_.size();
    }

protected:
    void Clear() override
    {
        scanner_ = nullptr;
        partition_id_ = 0;
        last_key_ = "";
        session_id_ = "";
        last_batch_size_ = 0;
        first_batch_fetched_ = false;
        head_ = 0;
        scan_cache_.clear();
    }

private:
    void ResetCache();

    DataStoreServiceScanner *scanner_;
    uint32_t partition_id_;
    std::string last_key_;
    uint32_t last_batch_size_;
    std::string session_id_;
    // indicate if the first batch is fetched
    bool first_batch_fetched_{false};
    size_t head_{0};
    std::vector<ScanTuple> scan_cache_;

    friend DataStoreServiceHashPartitionScanner<true>;
    friend DataStoreServiceHashPartitionScanner<false>;
};

template <bool ScanForward>
class DataStoreServiceHashPartitionScanner
    : public txservice::store::DataStoreScanner,
      public DataStoreServiceScanner
{
#ifdef USE_ONE_ELOQDSS_PARTITION
    static constexpr uint32_t HASH_PARTITION_COUNT = 1;
#else
    static constexpr uint32_t HASH_PARTITION_COUNT = 0x400;  // 1024
#endif

public:
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
        size_t batch_size);

    ~DataStoreServiceHashPartitionScanner();

    // DataStoreScanner interface
    void Current(txservice::TxKey &key,
                 const txservice::TxRecord *&rec,
                 uint64_t &version_ts,
                 bool &deleted) override;
    bool MoveNext() override;
    void End() override;

    // DataStoreServiceScanner interface
    bool Init() override;

protected:
    bool CloseScan();
    bool IsRunOutOfData();

private:
    void AddScanTuple(SinglePartitionScanner *part_scanner, uint32_t part_id);
    // scanner parameters
    const txservice::CatalogFactory *catalog_factory_;
    const txservice::KeySchema *key_sch_;
    const txservice::RecordSchema *rec_sch_;
    const txservice::KVCatalogInfo *kv_info_;
    const std::vector<txservice::DataStoreSearchCond> pushdown_condition_;
    const bool is_object_key_schema_;

    // scanner state
    bool initialized_;

    // partition scanners
    std::vector<SinglePartitionScanner *> partition_scanners_;

    // result cache
    using CompareFunc = std::conditional_t<
        ScanForward,
        CacheCompare<txservice::TxKey, txservice::TxRecord>,
        CacheReverseCompare<txservice::TxKey, txservice::TxRecord>>;
    std::priority_queue<
        ScanHeapTuple<txservice::TxKey, txservice::TxRecord>,
        std::vector<ScanHeapTuple<txservice::TxKey, txservice::TxRecord>>,
        CompareFunc>
        result_cache_;
};

}  // namespace EloqDS
