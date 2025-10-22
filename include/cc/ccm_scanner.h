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

#include <cstdint>
#include <memory>  // std::shared_ptr
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "scan.h"
#include "schema.h"
#include "sharder.h"
#include "tx_key.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
struct ScanOpenCc;
struct ScanBatchCc;
struct ScanCloseCc;

class CcMapScanner;

enum class ScannerStatus
{
    Open = 0,
    Closed,
    Blocked,
};

class CcScanner;

struct ScanCache
{
public:
    static constexpr size_t ScanBatchSize = 128;

    // Approximate meta data size in storage.
    static constexpr size_t MetaDataSize = 8;

    ScanCache(CcScanner *scanner)
        : idx_(0),
          size_(0),
          capacity_(ScanBatchSize),
          scanner_(scanner),
          mem_size_(0),
          mem_max_bytes_(0)
    {
    }

    ScanCache(size_t idx, size_t size, size_t capacity, CcScanner *scanner)
        : idx_(idx),
          size_(size),
          capacity_(capacity),
          trailing_cnt_(0),
          scanner_(scanner),
          mem_size_(0),
          mem_max_bytes_(0)
    {
        assert(size_ <= capacity);
    }

    ScanCache(ScanCache &&rhs) noexcept
        : idx_(rhs.idx_),
          size_(rhs.size_),
          capacity_(rhs.capacity_),
          trailing_cnt_(rhs.trailing_cnt_),
          scanner_(rhs.scanner_),
          mem_size_(rhs.mem_size_),
          mem_max_bytes_(rhs.mem_max_bytes_)
    {
    }

    virtual ~ScanCache() = default;

    ScannerStatus Status() const
    {
        if (idx_ < size_)
        {
            return ScannerStatus::Open;
        }
        else if (size_ == capacity_)
        {
            return ScannerStatus::Blocked;
        }
        else
        {
            return ScannerStatus::Closed;
        }
    }

    void MoveNext()
    {
        ++idx_;
    }

    void Reset()
    {
        idx_ = 0;
        size_ = 0;
        capacity_ = ScanBatchSize;
        mem_size_ = 0;
        mem_max_bytes_ = 0;
        trailing_cnt_ = 0;
    }

    void Rewind()
    {
        idx_ = 0;
    }

    size_t Size() const
    {
        return size_;
    }

    bool Full() const
    {
        return size_ == capacity_;
    }

    CcScanner *Scanner() const
    {
        return scanner_;
    }

    virtual ScanTuple *AddScanTuple(const std::string &key_str,
                                    size_t &key_offset,
                                    uint64_t key_ts,
                                    const std::string &record_str,
                                    size_t &rec_offset,
                                    RecordStatus rec_status,
                                    uint64_t gap_ts,
                                    uint64_t cce_lock_ptr,
                                    int64_t term,
                                    uint32_t core_id,
                                    uint32_t ng_id) = 0;

    virtual const ScanTuple *LastTuple() const = 0;

    void RemoveLast()
    {
        assert(size_ > 0);
        --size_;
        trailing_cnt_++;
    }

    void RemoveLast(size_t new_size)
    {
        assert(new_size <= size_);
        trailing_cnt_ = size_ - new_size;
        size_ = new_size;
    }

    virtual void TrailingTuples(
        std::vector<const ScanTuple *> &tuple_buf) const = 0;

protected:
    size_t idx_;
    size_t size_;
    size_t capacity_;
    size_t trailing_cnt_{0};
    CcScanner *const scanner_;
    uint32_t mem_size_{0};
    uint32_t mem_max_bytes_{0};
};

template <typename KeyT, typename ValueT>
struct TemplateScanCache : public ScanCache
{
public:
    using ScanCache::RemoveLast;

    TemplateScanCache() = delete;

    TemplateScanCache(CcScanner *scanner, const KeySchema *key_schema)
        : ScanCache(scanner),
          cache_(ScanCache::ScanBatchSize),
          key_schema_(key_schema)
    {
        assert(cache_.size() == ScanCache::ScanBatchSize);
    }

    TemplateScanCache(CcScanner *scanner,
                      size_t cache_size,
                      const KeySchema *key_schema)
        : ScanCache(scanner), cache_(cache_size), key_schema_(key_schema)
    {
        assert(cache_.size() == cache_size);
    }

    TemplateScanCache(TemplateScanCache &&rhs)
        : ScanCache(rhs.idx_, rhs.size_, rhs.capacity_, rhs.scanner_),
          cache_(std::move(rhs.cache_)),
          key_schema_(rhs.key_schema_)
    {
    }

    TemplateScanCache(const TemplateScanCache &rhs) = delete;

    ~TemplateScanCache() = default;

    bool IsFull() const
    {
        return mem_size_ >= mem_max_bytes_;
    }

    void SetCacheMaxBytes(size_t max_bytes)
    {
        mem_max_bytes_ = max_bytes;
    }

    void SetCacheCapacity(size_t capacity)
    {
        capacity_ = capacity;
    }

    const KeySchema *GetKeySchema()
    {
        return key_schema_;
    }

    TemplateScanTuple<KeyT, ValueT> *AddScanTuple()
    {
        TemplateScanTuple<KeyT, ValueT> *scan_t = nullptr;

        if (size_ < cache_.size())
        {
            scan_t = &cache_[size_];
        }
        else
        {
            TemplateScanTuple<KeyT, ValueT> &new_tuple = cache_.emplace_back();
            scan_t = &new_tuple;
        }
        ++size_;

        return scan_t;
    }

    void AddScanTupleSize(uint32_t tuple_size)
    {
        mem_size_ += tuple_size;
    }

    ScanTuple *AddScanTuple(const std::string &key_str,
                            size_t &key_offset,
                            uint64_t key_ts,
                            const std::string &record_str,
                            size_t &rec_offset,
                            RecordStatus rec_status,
                            uint64_t gap_ts,
                            uint64_t cce_lock_ptr,
                            int64_t term,
                            uint32_t core_id,
                            uint32_t ng_id) override
    {
        assert(size_ <= cache_.size());

        TemplateScanTuple<KeyT, ValueT> *scan_tuple = nullptr;
        if (size_ < cache_.size())
        {
            scan_tuple = &cache_[size_];
        }
        else
        {
            TemplateScanTuple<KeyT, ValueT> &new_tuple = cache_.emplace_back();
            scan_tuple = &new_tuple;
        }

        if (key_ts == 0)
        {
            // ScanGap
            scan_tuple->key_ts_ = 0;
            scan_tuple->gap_ts_ = 0;
            scan_tuple->cce_addr_.SetCceLock(
                cce_lock_ptr, term, ng_id, core_id);
        }
        else
        {
            // When the key's timestamp is 0, the tuple's key is not included in
            // this scan. Only deserializes the key when the key is included.
            assert(key_ts > 0);
            scan_tuple->key_ts_ = key_ts;

            scan_tuple->KeyObj().SetPackedKey(key_str.data(), key_str.size());
            scan_tuple->rec_status_ = rec_status;
            scan_tuple->SetRecord(record_str.data(), rec_offset);

            scan_tuple->gap_ts_ = gap_ts;
            scan_tuple->cce_addr_.SetCceLock(
                cce_lock_ptr, term, ng_id, core_id);
        }
        ++size_;
        return scan_tuple;
    }

    const TemplateScanTuple<KeyT, ValueT> *Current() const
    {
        return idx_ < size_ ? &cache_.at(idx_) : nullptr;
    }

    const TemplateScanTuple<KeyT, ValueT> *Last() const
    {
        return size_ > 0 ? &cache_[size_ - 1] : nullptr;
    }

    const ScanTuple *LastTuple() const override
    {
        return Last();
    }

    const TemplateScanTuple<KeyT, ValueT> *At(uint32_t idx) const
    {
        assert(idx < cache_.size());
        return &cache_[idx];
    }

    TemplateScanTuple<KeyT, ValueT> *At(uint32_t idx)
    {
        assert(idx < cache_.size());
        return &cache_[idx];
    }

    void TrailingTuples(
        std::vector<const ScanTuple *> &tuple_buf) const override
    {
        for (size_t idx = size_; idx < size_ + trailing_cnt_; idx++)
        {
            tuple_buf.push_back(At(idx));
        }
    }

    void RemoveLast(const KeyT &start_key)
    {
        auto cmp =
            [](const KeyT &key, const TemplateScanTuple<KeyT, ValueT> &tuple)
        { return key < tuple.KeyObj(); };

        auto last =
            size_ == cache_.size() ? cache_.end() : cache_.begin() + size_;
        auto it = std::upper_bound(cache_.begin(), last, start_key, cmp);
        size_t new_size = it - cache_.begin();
        RemoveLast(new_size);
    }

private:
    std::vector<TemplateScanTuple<KeyT, ValueT>> cache_;
    const KeySchema *const key_schema_;
};

enum struct CcmScannerType
{
    HashPartition = 0,
    RangePartition
};

class CcScanner
{
public:
    using Uptr = std::unique_ptr<CcScanner>;

    CcScanner(ScanDirection direction, ScanIndexType index_type)
        : direct_(direction),
          index_type_(index_type),
          status_(ScannerStatus::Open),
          is_ckpt_delta_(false)
    {
    }

    virtual ~CcScanner() = default;

    virtual ScanCache *Cache(uint32_t shard_code) = 0;
    virtual ScanCache *KvCache(uint32_t shard_code,
                               uint16_t bucket_id,
                               size_t batch_size)
    {
        return nullptr;
    }

    virtual TxKey Merge(uint32_t shard_code,
                        bool &memory_is_drained,
                        absl::flat_hash_map<uint16_t, bool> &kv_is_drained)
    {
        assert(false);
        return TxKey();
    }

    virtual void ResetShards(size_t shard_cnt) = 0;
    virtual void ResetCaches() = 0;
    virtual void Reset(const KeySchema *key_schema) = 0;
    virtual void Close() = 0;
    virtual void ShardCacheSizes(std::vector<std::pair<uint32_t, size_t>>
                                     *shard_code_and_sizes) const = 0;
    virtual void MemoryShardCacheLastTuples(
        std::vector<const ScanTuple *> *last_tuples) const = 0;
    virtual void MemoryShardCacheTrailingTuples(
        std::vector<const ScanTuple *> *trailing_tuples) const = 0;

    virtual void Init() = 0;
    virtual const ScanTuple *Current() = 0;
    virtual CcmScannerType Type() const = 0;

    ScannerStatus Status() const
    {
        return status_;
    }

    void SetStatus(ScannerStatus status)
    {
        status_ = status;
    }

    virtual void MoveNext() = 0;

    virtual TxKey DecodeKey(const std::string &blob) const
    {
        return TxKey();
    }

    virtual int64_t PartitionNgTerm() const
    {
        return -1;
    }
    virtual void SetPartitionNgTerm(int64_t partition_ng_term)
    {
    }

    virtual uint32_t ShardCount() const = 0;

    virtual void CommitAtCore(uint16_t core_id)
    {
        assert(false);
    }

    virtual void FinalizeCommit()
    {
        assert(false);
    }

    ScanDirection Direction() const
    {
        return direct_;
    }

    ScanIndexType IndexType() const
    {
        return index_type_;
    }

    static CcOperation DeduceCcOperation(ScanIndexType index_type,
                                         bool is_for_write)
    {
        CcOperation cc_op;
        if (is_for_write)
        {
            cc_op = CcOperation::ReadForWrite;
        }
        else
        {
            if (index_type == ScanIndexType::Secondary)
            {
                cc_op = CcOperation::ReadSkIndex;
            }
            else
            {
                cc_op = CcOperation::Read;
            }
        }
        return cc_op;
    }

    LockType DeduceScanTupleLockType(RecordStatus rec_status)
    {
        if (rec_status == RecordStatus::Deleted && !is_for_write_)
        {
            return LockType::NoLock;
        }
        else
        {
            return lock_type_;
        }
    }

    bool IsCoveringKey() const
    {
        return is_covering_keys_;
    }

    bool IsRequireKeys() const
    {
        return is_require_keys_;
    }

    bool IsRequireRecords() const
    {
        return is_require_recs_;
    }

    bool IsRequireSort() const
    {
        return is_require_sort_;
    }

    void SetRequireKeys()
    {
        is_require_keys_ = true;
    }

    void SetRequireRecords()
    {
        is_require_recs_ = true;
    }

    void SetRequireSort()
    {
        is_require_sort_ = true;
    }

    IsolationLevel Isolation() const
    {
        return iso_level_;
    }

    bool IsReadForWrite() const
    {
        return is_for_write_;
    }

protected:
    ScanDirection direct_;
    ScanIndexType index_type_;
    ScannerStatus status_;

public:
    bool read_local_{false};
    bool is_ckpt_delta_{false};
    bool is_for_write_{false};
    bool is_covering_keys_{false};
    bool is_require_keys_{true};
    bool is_require_recs_{true};
    bool is_require_sort_{true};
    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
    CcProtocol protocol_{CcProtocol::OCC};

    // Store cc_op_ and lock_type_ to speedup DeduceScanTupleLockType()
    CcOperation cc_op_{CcOperation::Read};
    LockType lock_type_{LockType::NoLock};
};

template <typename KeyT, typename ValueT>
class HashParitionCcScanner : public CcScanner
{
public:
    struct ShardCache
    {
        ShardCache(CcScanner *scanner, const KeySchema *key_schema)
        {
            memory_cache_ = std::make_unique<TemplateScanCache<KeyT, ValueT>>(
                scanner, key_schema);
        }

        void Recycle()
        {
            if (memory_cache_)
            {
                memory_cache_->Reset();
            }

            for (auto &[bucket_id, cache] : kv_caches_)
            {
                cache->Reset();
                free_cache_pool_.push_back(std::move(cache));
            }

            kv_caches_.clear();
        }

        size_t ToTalSize()
        {
            size_t size = 0;
            if (memory_cache_)
            {
                size += memory_cache_->Size();
            }

            for (const auto &[bucket_id, kv_cache] : kv_caches_)
            {
                size += kv_cache->Size();
            }

            return size;
        }

        TemplateScanCache<KeyT, ValueT> *GetOrCreateKvCache(
            uint16_t bucket_id,
            CcScanner *scanner,
            const KeySchema *key_schema,
            size_t batch_size)
        {
            auto iter = kv_caches_.find(bucket_id);
            if (iter != kv_caches_.end())
            {
                return iter->second.get();
            }
            else
            {
                if (free_cache_pool_.empty())
                {
                    auto cache =
                        std::make_unique<TemplateScanCache<KeyT, ValueT>>(
                            scanner, batch_size, key_schema);
                    // we don't care kv cache capacity
                    cache->SetCacheCapacity(SIZE_MAX);
                    auto em_it =
                        kv_caches_.try_emplace(bucket_id, std::move(cache));
                    return em_it.first->second.get();
                }
                else
                {
                    auto cache = std::move(free_cache_pool_.back());
                    free_cache_pool_.pop_back();
                    cache->Reset();
                    cache->SetCacheCapacity(SIZE_MAX);
                    auto em_it =
                        kv_caches_.try_emplace(bucket_id, std::move(cache));
                    return em_it.first->second.get();
                }
            }
        }

        TemplateScanCache<KeyT, ValueT> *GetKvCache(uint16_t bucket_id)
        {
            auto iter = kv_caches_.find(bucket_id);
            if (iter != kv_caches_.end())
            {
                return iter->second.get();
            }
            else
            {
                // TableType::RangePartition
                // assert(Sharder::Instance().GetDataStoreHandler() == nullptr);
                return nullptr;
            }
        }

        class Iterator
        {
        public:
            Iterator()
                : mgr_(nullptr),
                  scan_cache_type_(ScanCacheType::MemoryCache),
                  inner_(0),
                  cur_(nullptr)
            {
            }

            Iterator(const ShardCache *m)
                : mgr_(m),
                  scan_cache_type_(ScanCacheType::MemoryCache),
                  inner_(0),
                  cur_(nullptr)
            {
            }

            Iterator(const Iterator &other)
            {
                mgr_ = other.mgr_;
                scan_cache_type_ = other.scan_cache_type_;
                inner_ = other.inner_;
                kv_cache_iter_ = other.kv_cache_iter_;
                cur_ = other.cur_;
            }

            Iterator &operator=(const Iterator &other)
            {
                if (this != &other)
                {
                    mgr_ = other.mgr_;
                    scan_cache_type_ = other.scan_cache_type_;
                    inner_ = other.inner_;
                    kv_cache_iter_ = other.kv_cache_iter_;
                    cur_ = other.cur_;
                }
                return *this;
            }

            enum class ScanCacheType
            {
                MemoryCache = 0,
                KvCache = 1
            };

            void Init()
            {
                scan_cache_type_ = ScanCacheType::MemoryCache;
                inner_ = 0;
                kv_cache_iter_ = mgr_->kv_caches_.cbegin();
                cur_ = nullptr;
                SkipEmpty();
            }

            const TemplateScanTuple<KeyT, ValueT> *Current() const
            {
                assert(Valid());
                return cur_;
            }

            bool MoveNext()
            {
                ++inner_;
                if (scan_cache_type_ == ScanCacheType::MemoryCache)
                {
                    if (inner_ < mgr_->memory_cache_->Size())
                    {
                        cur_ = mgr_->memory_cache_->At(inner_);
                        return true;
                    }

                    scan_cache_type_ = ScanCacheType::KvCache;
                    kv_cache_iter_ = mgr_->kv_caches_.cbegin();
                    inner_ = 0;  // reset containter index
                }
                else
                {
                    assert(kv_cache_iter_ != mgr_->kv_caches_.cend());
                    if (inner_ < kv_cache_iter_->second->Size())
                    {
                        cur_ = kv_cache_iter_->second->At(inner_);
                        return true;
                    }
                    // move to next kv cache
                    kv_cache_iter_++;
                    inner_ = 0;
                }

                SkipEmpty();
                return Valid();
            }

            bool Valid() const
            {
                return cur_ != nullptr;
            }

        private:
            void SkipEmpty()
            {
                if (mgr_ == nullptr)
                {
                    return;
                }

                cur_ = nullptr;
                inner_ = 0;

                if (scan_cache_type_ == ScanCacheType::MemoryCache)
                {
                    if (mgr_->memory_cache_ && mgr_->memory_cache_->Size() > 0)
                    {
                        cur_ = mgr_->memory_cache_->At(inner_);
                        return;
                    }

                    scan_cache_type_ = ScanCacheType::KvCache;
                    kv_cache_iter_ = mgr_->kv_caches_.cbegin();
                }

                while (kv_cache_iter_ != mgr_->kv_caches_.cend())
                {
                    if (kv_cache_iter_->second->Size() > 0)
                    {
                        cur_ = kv_cache_iter_->second->At(inner_);
                        return;
                    }

                    ++kv_cache_iter_;
                }

                assert(kv_cache_iter_ == mgr_->kv_caches_.cend());
            }

            const ShardCache *mgr_;
            ScanCacheType scan_cache_type_{ScanCacheType::MemoryCache};
            size_t inner_;  // containter index
            typename absl::flat_hash_map<
                uint16_t,
                std::unique_ptr<TemplateScanCache<KeyT, ValueT>>>::
                const_iterator kv_cache_iter_{};
            const TemplateScanTuple<KeyT, ValueT> *cur_;
        };

        Iterator NewIterator()
        {
            return Iterator(this);
        }

        std::vector<std::unique_ptr<TemplateScanCache<KeyT, ValueT>>>
            free_cache_pool_;
        std::unique_ptr<TemplateScanCache<KeyT, ValueT>> memory_cache_{nullptr};
        absl::flat_hash_map<uint16_t,
                            std::unique_ptr<TemplateScanCache<KeyT, ValueT>>>
            kv_caches_;
    };

    HashParitionCcScanner(ScanDirection direct,
                          ScanIndexType index_type,
                          const KeySchema *schema)
        : CcScanner(direct, index_type), key_schema_(schema)
    {
    }

    void ResetShards(size_t shard_cnt) override
    {
        assert(false &&
               "ResetShards is designed for RangePartitionedCcmScanner.");
    }

    void ResetCaches() override
    {
        for (auto &[shard_code, cache] : shard_caches_)
        {
            cache->memory_cache_->Reset();
            for (auto &[bucket_id, kv_cache] : cache->kv_caches_)
            {
                kv_cache->Reset();
            }
        }

        current_iter_ = {};
        shard_cache_iter_ = typename ShardCache::Iterator();
        cache_offset_ = 0;
        init_ = false;
    }

    ScanCache *Cache(uint32_t shard_code) override
    {
        ShardCache *shard_cache = GetShardCache(shard_code);
        return shard_cache->memory_cache_.get();
        // For TemplateCcScanner, shard_code is (ng_id << 10) + core_id.
    }

    ScanCache *KvCache(uint32_t shard_code,
                       uint16_t bucket_id,
                       size_t batch_size) override
    {
        ShardCache *shard_cache = GetShardCache(shard_code);
        return shard_cache->GetOrCreateKvCache(
            bucket_id, this, key_schema_, batch_size);
    }

    void ShardCacheSizes(std::vector<std::pair<uint32_t, size_t>>
                             *shard_code_and_sizes) const override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        for (const auto &[shard_code, cache] : shard_caches_)
        {
            shard_code_and_sizes->emplace_back(shard_code, cache->ToTalSize());
        }
    }

    void MemoryShardCacheLastTuples(
        std::vector<const ScanTuple *> *last_tuples) const override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        last_tuples->reserve(shard_caches_.size());
        for (const auto &[shard_code, shard_cache] : shard_caches_)
        {
            last_tuples->emplace_back(shard_cache->memory_cache_->LastTuple());
        }
    }

    void MemoryShardCacheTrailingTuples(
        std::vector<const ScanTuple *> *trailing_tuples) const override
    {
        std::unique_lock<std::mutex> lock(mutex_);

        for (auto &[shard_code, shard_cache] : shard_caches_)
        {
            shard_cache->memory_cache_->TrailingTuples(*trailing_tuples);
        }
    }

    TxKey DecodeKey(const std::string &blob) const override
    {
        std::unique_ptr<KeyT> key = std::make_unique<KeyT>();
        key->SetPackedKey(blob.data(), blob.size());
        return TxKey(std::move(key));
    }

    void Init() override
    {
        if (!init_)
        {
            current_iter_ = shard_caches_.begin();
            while (current_iter_ != shard_caches_.end())
            {
                shard_cache_iter_ = current_iter_->second->NewIterator();
                shard_cache_iter_.Init();
                if (shard_cache_iter_.Valid())
                {
                    break;
                }

                current_iter_++;
            }

            if (current_iter_ == shard_caches_.end())
            {
                status_ = ScannerStatus::Blocked;
            }
            else
            {
                status_ = ScannerStatus::Open;
            }

            init_ = true;
        }
    }

    const ScanTuple *Current() override
    {
        if (status_ != ScannerStatus::Open)
        {
            return nullptr;
        }

        if (current_iter_ == shard_caches_.end())
        {
            status_ = ScannerStatus::Blocked;
            return nullptr;
        }

        return shard_cache_iter_.Current();
    }

    void MoveNext() override
    {
        if (!init_)
        {
            return;
        }

        if (current_iter_ == shard_caches_.end())
        {
            status_ = ScannerStatus::Blocked;
            return;
        }

        if (status_ != ScannerStatus::Open)
        {
            return;
        }

        bool has_data = shard_cache_iter_.MoveNext();
        while (!has_data)
        {
            current_iter_++;
            if (current_iter_ == shard_caches_.end())
            {
                status_ = ScannerStatus::Blocked;
                return;
            }

            shard_cache_iter_ = current_iter_->second->NewIterator();
            shard_cache_iter_.Init();
            has_data = shard_cache_iter_.Valid();
        }
    }

    CcmScannerType Type() const override
    {
        return CcmScannerType::HashPartition;
    }

    uint32_t ShardCount() const override
    {
        return shard_caches_.size();
    }

    void Reset(const KeySchema *key_schema) override
    {
        status_ = ScannerStatus::Blocked;
        key_schema_ = key_schema;
        shard_caches_.clear();
        current_iter_ = {};
        shard_cache_iter_ = typename ShardCache::Iterator();
        init_ = false;
    }

    void Close() override
    {
        status_ = ScannerStatus::Closed;

        // shard_caches_.clear();
        for (auto &[shard_code, shard_cache] : shard_caches_)
        {
            shard_cache->Recycle();
        }

        current_iter_ = {};
        shard_cache_iter_ = typename ShardCache::Iterator();
        init_ = false;
    }

    ShardCache *GetShardCache(uint32_t shard_code)
    {
        std::lock_guard<std::mutex> lk(mutex_);
        auto iter = shard_caches_.find(shard_code);
        if (iter != shard_caches_.end())
        {
            return iter->second.get();
        }
        else
        {
            auto shard_cache = std::make_unique<ShardCache>(this, key_schema_);
            auto em_it =
                shard_caches_.try_emplace(shard_code, std::move(shard_cache));
            return em_it.first->second.get();
        }
    }

    TxKey Merge(uint32_t shard_code,
                bool &memory_is_drained,
                absl::flat_hash_map<uint16_t, bool> &kv_is_drained) override
    {
        assert(Direction() == ScanDirection::Forward);
        ShardCache *shard_cache = GetShardCache(shard_code);

        const KeyT *min_key = nullptr;

        assert(memory_is_drained || shard_cache->memory_cache_->Size() > 0);
        if (!memory_is_drained && shard_cache->memory_cache_->Size() > 0)
        {
            const TemplateScanTuple<KeyT, ValueT> *tuple =
                shard_cache->memory_cache_->Last();
            min_key = &tuple->KeyObj();
        }

        for (auto &[bucket_id, kv_cache] : shard_cache->kv_caches_)
        {
            assert(kv_is_drained.at(bucket_id) || kv_cache->Size() > 0);
            if (!kv_is_drained.at(bucket_id) && kv_cache->Size() > 0)
            {
                const TemplateScanTuple<KeyT, ValueT> *tuple = kv_cache->Last();
                if (min_key == nullptr || tuple->KeyObj() < *min_key)
                {
                    min_key = &tuple->KeyObj();
                }
            }
        }

        if (min_key != nullptr)
        {
            size_t memory_cache_size = shard_cache->memory_cache_->Size();
            if (memory_cache_size > 0)
            {
                shard_cache->memory_cache_->RemoveLast(*min_key);
                if (memory_cache_size != shard_cache->memory_cache_->Size())
                {
                    memory_is_drained = false;
                }
            }

            for (auto &[bucket_id, kv_cache] : shard_cache->kv_caches_)
            {
                size_t kv_cache_size = kv_cache->Size();
                if (kv_cache_size > 0)
                {
                    kv_cache->RemoveLast(*min_key);
                    if (kv_cache_size != kv_cache->Size())
                    {
                        kv_is_drained[bucket_id] = false;
                    }
                }
            }
        }

        // Init cache offset
        absl::flat_hash_map<uint16_t, size_t> cache_offset;
        for (auto &[bucket_id, kv_cache] : shard_cache->kv_caches_)
        {
            cache_offset[bucket_id] = 0;
        }

        size_t memory_cache_offset = 0;
        size_t memory_cache_size = shard_cache->memory_cache_->Size();

        // deduplicate
        while (memory_cache_offset < memory_cache_size)
        {
            const TemplateScanTuple<KeyT, ValueT> *memory_tuple =
                shard_cache->memory_cache_->At(memory_cache_offset);
            uint16_t target_bucket =
                Sharder::MapKeyHashToBucketId(memory_tuple->KeyObj().Hash());
            TemplateScanCache<KeyT, ValueT> *kv_cache =
                shard_cache->GetKvCache(target_bucket);
            if (kv_cache)
            {
                while (cache_offset[target_bucket] < kv_cache->Size())
                {
                    TemplateScanTuple<KeyT, ValueT> *kv_tuple =
                        kv_cache->At(cache_offset[target_bucket]);
                    if (kv_tuple->KeyObj() < memory_tuple->KeyObj())
                    {
                        // index_chain->offsets_.emplace_back(
                        //    kv_cache, cache_offset[target_bucket]);
                        cache_offset[target_bucket]++;
                    }
                    else if (kv_tuple->KeyObj() == memory_tuple->KeyObj())
                    {
                        kv_tuple->rec_status_ = RecordStatus::Deleted;
                        cache_offset[target_bucket]++;
                        break;
                    }
                    else
                    {
                        break;
                    }
                }
            }

            // index_chain->offsets_.emplace_back(shard_cache->memory_cache_.get(),
            //                                   memory_cache_offset);
            memory_cache_offset++;
        }

        // memory cache is drained. we don't need to merge data
        for (auto &[bucket_id, offset] : cache_offset)
        {
            TemplateScanCache<KeyT, ValueT> *kv_cache =
                shard_cache->GetKvCache(bucket_id);
            if (kv_cache)
            {
                while (offset < kv_cache->Size())
                {
                    // index_chain->offsets_.emplace_back(kv_cache, offset);
                    offset++;
                }
            }
        }

        return TxKey(min_key);
    }

private:
    /// <summary>
    /// A collection of local and remote scan caches, one per core.
    /// </summary>

    std::unordered_map<uint32_t, std::unique_ptr<ShardCache>> shard_caches_;
    typename std::unordered_map<uint32_t, std::unique_ptr<ShardCache>>::iterator
        current_iter_;
    typename ShardCache::Iterator shard_cache_iter_;

    size_t cache_offset_{0};

    bool init_{false};

    const KeySchema *key_schema_;
    mutable std::mutex mutex_;
};

template <typename KeyT, typename ValueT, bool IsForward>
class RangePartitionedCcmScanner : public CcScanner
{
public:
    RangePartitionedCcmScanner(ScanDirection direct,
                               ScanIndexType index_type,
                               const KeySchema *schema)
        : CcScanner(direct, index_type), scans_(), key_schema_(schema)
    {
    }

    void Init() override
    {
    }

    void ResetShards(size_t shard_cnt) override
    {
        size_t old_size = scans_.size();
        if (shard_cnt > old_size)
        {
            scans_.reserve(shard_cnt);
            index_chain_.reserve(shard_cnt);
            for (size_t idx = old_size; idx < shard_cnt; ++idx)
            {
                scans_.emplace_back(this, key_schema_);
                index_chain_.emplace_back();
            }
        }
        else if (shard_cnt < old_size)
        {
            for (size_t idx = shard_cnt; idx < old_size; ++idx)
            {
                scans_.pop_back();
            }
            index_chain_.resize(shard_cnt);
        }

        assert(scans_.size() == shard_cnt);

        for (size_t idx = 0; idx < old_size && idx < shard_cnt; ++idx)
        {
            scans_[idx].Reset();
            index_chain_[idx].clear();
        }

        std::unique_lock<std::mutex> lk(mux_);
        head_index_ = Inf();
        head_occupied_ = false;
    }

    void ResetCaches() override
    {
        for (size_t core_id = 0; core_id < scans_.size(); ++core_id)
        {
            scans_[core_id].Reset();
            index_chain_[core_id].clear();
        }

        head_index_ = Inf();
        head_occupied_ = false;
    }

    ScanCache *Cache(uint32_t shard_code) override
    {
        // For RangePartitionedCcmScanner, shard_code is core_id.
        return &scans_[shard_code];
    }

    void ShardCacheSizes(std::vector<std::pair<uint32_t, size_t>>
                             *shard_code_and_sizes) const override
    {
        for (size_t core_id = 0; core_id < scans_.size(); ++core_id)
        {
            shard_code_and_sizes->emplace_back(core_id, scans_[core_id].Size());
        }
    }

    void MemoryShardCacheLastTuples(
        std::vector<const ScanTuple *> *last_tuples) const override
    {
        last_tuples->reserve(scans_.size());
        for (size_t core_id = 0; core_id < scans_.size(); ++core_id)
        {
            last_tuples->emplace_back(scans_[core_id].LastTuple());
        }
    }

    void MemoryShardCacheTrailingTuples(
        std::vector<const ScanTuple *> *trailing_tuples) const override
    {
        for (size_t core_id = 0; core_id < scans_.size(); ++core_id)
        {
            scans_[core_id].TrailingTuples(*trailing_tuples);
        }
    }

    const ScanTuple *Current() override
    {
        if (head_index_ == Inf())
        {
            status_ = ScannerStatus::Blocked;
            return nullptr;
        }
        else
        {
            assert(status_ == ScannerStatus::Open);
            return At(head_index_);
        }
    }

    void MoveNext() override
    {
        if (head_index_ == Inf())
        {
            return;
        }

        head_index_ = AdvanceMergeIndex(head_index_);
        if (head_index_ == Inf())
        {
            status_ = ScannerStatus::Blocked;
        }
    }

    CcmScannerType Type() const override
    {
        return CcmScannerType::RangePartition;
    }

    int64_t PartitionNgTerm() const override
    {
        return partition_ng_term_;
    }

    void SetPartitionNgTerm(int64_t partition_ng_term) override
    {
        partition_ng_term_ = partition_ng_term;
    }

    TxKey DecodeKey(const std::string &blob) const override
    {
        std::unique_ptr<KeyT> key = std::make_unique<KeyT>();
        size_t offset = 0;
        key->Deserialize(blob.data(), offset, key_schema_);

        return TxKey(std::move(key));
    }

    uint32_t ShardCount() const override
    {
        return scans_.size();
    }

    void Reset(const KeySchema *key_schema) override
    {
        key_schema_ = key_schema;
        partition_ng_term_ = -1;
    }

    void Close() override
    {
        status_ = ScannerStatus::Closed;
        scans_.clear();
        index_chain_.clear();
        head_index_ = Inf();
        head_occupied_ = false;
    }

    /**
     * @brief Commits the scan at the specified core.
     *
     * @param core_id
     */
    void CommitAtCore(uint16_t core_id) override
    {
        size_t sz = scans_[core_id].Size();
        if (sz > 0)
        {
            std::vector<CompoundIndex> &next_chain = index_chain_[core_id];
            assert(next_chain.empty());
            next_chain.reserve(sz);

            for (uint32_t idx = 0; idx < sz - 1; ++idx)
            {
                next_chain.emplace_back(core_id, idx + 1);
            }
            // The next index of the last tuple is infinity.
            next_chain.emplace_back(Inf());
            assert(next_chain.size() == sz);

            if (is_require_sort_)
            {
                CompoundIndex head_index(core_id, 0);
                MergeCompoundIndex(head_index);
            }
            else
            {
                // Concat. Delay concat to FinalizeCommit() to avoid lock.
            }
        }
    }

    void FinalizeCommit() override
    {
        if (is_require_sort_)
        {
            // Already sorted by CommitAtCore().
        }
        else
        {
            ConcatAll();
        }
    }

private:
    struct CompoundIndex
    {
    public:
        CompoundIndex() : index_(UINT32_MAX)
        {
        }

        CompoundIndex(uint16_t core_id, uint32_t offset)
        {
            index_ = (offset << 10) | core_id;
        }

        friend bool operator==(const CompoundIndex &lhs,
                               const CompoundIndex &rhs)
        {
            return lhs.index_ == rhs.index_;
        }

        friend bool operator!=(const CompoundIndex &lhs,
                               const CompoundIndex &rhs)
        {
            return !(lhs == rhs);
        }

        uint16_t CoreId() const
        {
            return index_ & 0x3FF;
        }

        uint32_t Offset() const
        {
            return index_ >> 10;
        }

    private:
        /**
         * @brief The lower 10 bits represent the core ID. The remaining higher
         * bits represent the offset in the scan result vector.
         *
         */
        uint32_t index_;
    };

    const CompoundIndex &Inf() const
    {
        static CompoundIndex inf;
        return inf;
    }

    void MergeCompoundIndex(CompoundIndex head)
    {
        std::unique_lock<std::mutex> lk(mux_);
        if (!head_occupied_)
        {
            // The head is empty. There is nothing to merge. Sets the head to
            // the input scan list's head.
            head_index_ = head;
            head_occupied_ = true;
        }
        else if (head != Inf())
        {
            // Merges the input scan list with the list pointed by the head.
            if (head_index_ == Inf())
            {
                head_index_ = head;
                return;
            }
            CompoundIndex curr_head = head_index_;
            head_occupied_ = false;

            lk.unlock();
            MergeCompoundIndex(head, curr_head);
        }
    }

    void MergeCompoundIndex(CompoundIndex left, CompoundIndex right)
    {
        CompoundIndex merge_head;
        CompoundIndex prev_index;

        if (left == Inf())
        {
            // The left is empty.
            return MergeCompoundIndex(right);
        }
        else if (right == Inf())
        {
            // The right is empty.
            return MergeCompoundIndex(left);
        }

        const TemplateScanTuple<KeyT, ValueT> *left_tuple = At(left);
        const TemplateScanTuple<KeyT, ValueT> *right_tuple = At(right);

        if (IsForward)
        {
            if (left_tuple->KeyObj() < right_tuple->KeyObj())
            {
                merge_head = left;
                prev_index = left;
                left = AdvanceMergeIndex(left);
            }
            else
            {
                merge_head = right;
                prev_index = right;
                right = AdvanceMergeIndex(right);
            }

            while (left != Inf() && right != Inf())
            {
                left_tuple = At(left);
                right_tuple = At(right);

                if (left_tuple->KeyObj() < right_tuple->KeyObj())
                {
                    UpdateNextIndex(prev_index, left);
                    prev_index = left;
                    left = AdvanceMergeIndex(left);
                }
                else
                {
                    UpdateNextIndex(prev_index, right);
                    prev_index = right;
                    right = AdvanceMergeIndex(right);
                }
            }
        }
        else
        {
            if (left_tuple->KeyObj() < right_tuple->KeyObj())
            {
                merge_head = right;
                prev_index = right;
                right = AdvanceMergeIndex(right);
            }
            else
            {
                merge_head = left;
                prev_index = left;
                left = AdvanceMergeIndex(left);
            }

            while (left != Inf() && right != Inf())
            {
                left_tuple = At(left);
                right_tuple = At(right);

                if (left_tuple->KeyObj() < right_tuple->KeyObj())
                {
                    UpdateNextIndex(prev_index, right);
                    prev_index = right;
                    right = AdvanceMergeIndex(right);
                }
                else
                {
                    UpdateNextIndex(prev_index, left);
                    prev_index = left;
                    left = AdvanceMergeIndex(left);
                }
            }
        }

        if (left != Inf())
        {
            UpdateNextIndex(prev_index, left);
        }

        if (right != Inf())
        {
            UpdateNextIndex(prev_index, right);
        }

        MergeCompoundIndex(merge_head);
    }

    /**
     * @brief Concat all chains at last finished core to avoid lock.
     */
    void ConcatAll()
    {
        assert(head_index_ == Inf());
        for (uint16_t core_id = 0; core_id < index_chain_.size(); ++core_id)
        {
            std::vector<CompoundIndex> &chain = index_chain_[core_id];
            if (!chain.empty())
            {
                ConcatLockFree(core_id, chain);
            }
        }
    }

    void ConcatLockFree(uint16_t core_id, std::vector<CompoundIndex> &chain)
    {
        chain.back() = head_index_;
        head_index_ = {core_id, 0};
    }

    CompoundIndex AdvanceMergeIndex(CompoundIndex index)
    {
        assert(index.CoreId() < index_chain_.size());
        assert(index.Offset() < index_chain_[index.CoreId()].size());

        return index_chain_[index.CoreId()][index.Offset()];
    }

    const TemplateScanTuple<KeyT, ValueT> *At(CompoundIndex index) const
    {
        assert(index.CoreId() < scans_.size());
        assert(index.Offset() < scans_[index.CoreId()].Size());

        return scans_[index.CoreId()].At(index.Offset());
    }

    void UpdateNextIndex(CompoundIndex prev_index, CompoundIndex index)
    {
        assert(prev_index.CoreId() < index_chain_.size());
        assert(prev_index.Offset() < index_chain_[prev_index.CoreId()].size());

        index_chain_[prev_index.CoreId()][prev_index.Offset()] = index;
    }

    // Scan caches of the target node group. Its size is core count of the
    // target node.
    std::vector<TemplateScanCache<KeyT, ValueT>> scans_;
    std::vector<std::vector<CompoundIndex>> index_chain_;
    std::mutex mux_;
    bool head_occupied_{false};
    CompoundIndex head_index_{Inf()};

    const KeySchema *key_schema_;
    /**
     * @brief The term of the cc node group where the range partition resides.
     * When the first slice from the range is scanned, the term is set. The
     * following scans of the same range partition expects to see the same term.
     *
     */
    int64_t partition_ng_term_{-1};
};
}  // namespace txservice