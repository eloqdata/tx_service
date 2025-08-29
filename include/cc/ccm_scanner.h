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

#include "scan.h"
#include "schema.h"
#include "tx_key.h"
#include "tx_record.h"

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
    Blocked
};

class CcScanner;

struct ScanCache
{
public:
    static constexpr size_t ScanBatchSize = 128;

    // Approximate meta data size in storage.
    static constexpr size_t MetaDataSize = 8;

    ScanCache(CcScanner *scanner)
        : idx_(0), size_(0), scanner_(scanner), mem_size_(0), mem_max_bytes_(0)
    {
    }

    ScanCache(size_t idx, size_t size, CcScanner *scanner)
        : idx_(idx),
          size_(size),
          trailing_cnt_(0),
          scanner_(scanner),
          mem_size_(0),
          mem_max_bytes_(0)
    {
    }

    ScanCache(ScanCache &&rhs) noexcept
        : idx_(rhs.idx_),
          size_(rhs.size_),
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
        else if (size_ == ScanCache::ScanBatchSize)
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
        return size_ == ScanCache::ScanBatchSize;
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

    virtual void TrailingTuples(
        std::vector<const ScanTuple *> &tuple_buf) const = 0;

protected:
    size_t idx_;
    size_t size_;
    size_t trailing_cnt_{0};
    CcScanner *const scanner_;
    uint32_t mem_size_{0};
    uint32_t mem_max_bytes_{0};
};

template <typename KeyT, typename ValueT>
struct TemplateScanCache : public ScanCache
{
public:
    TemplateScanCache() = delete;

    TemplateScanCache(CcScanner *scanner, const KeySchema *key_schema)
        : ScanCache(scanner),
          cache_(ScanCache::ScanBatchSize),
          key_schema_(key_schema)
    {
        assert(cache_.size() == ScanCache::ScanBatchSize);
    }

    TemplateScanCache(TemplateScanCache &&rhs)
        : ScanCache(rhs.idx_, rhs.size_, rhs.scanner_),
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
            scan_tuple->KeyObj().Deserialize(
                key_str.data(), key_offset, key_schema_);

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

    void TrailingTuples(
        std::vector<const ScanTuple *> &tuple_buf) const override
    {
        for (size_t idx = size_; idx < size_ + trailing_cnt_; idx++)
        {
            tuple_buf.push_back(At(idx));
        }
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
          drain_cache_mode_(false),
          is_ckpt_delta_(false)
    {
    }

    virtual ~CcScanner() = default;

    // virtual ScannerStatus MoveNext(const ScanTuple *&tuple) = 0;
    virtual uint32_t BlockedShard() const = 0;
    virtual ScanCache *Cache(uint32_t shard_code) = 0;
    virtual ScanCache *AddShard(uint32_t shard_code) = 0;
    virtual void ResetShards(size_t shard_cnt) = 0;
    virtual void ResetCaches() = 0;
    virtual void Reset(const KeySchema *key_schema) = 0;
    virtual void Close() = 0;
    virtual void ShardCacheSizes(std::vector<std::pair<uint32_t, size_t>>
                                     *shard_code_and_sizes) const = 0;
    virtual void ShardCacheLastTuples(
        std::vector<const ScanTuple *> *last_tuples) const = 0;
    virtual void ShardCacheTrailingTuples(
        std::vector<const ScanTuple *> *trailing_tuples) const = 0;

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

    virtual void SetDrainCacheMode(bool drain_cache_mode) = 0;
    virtual bool GetDrainCacheMode() = 0;
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

    virtual uint32_t CacheCount() const = 0;

    virtual void CommitAtCore(uint16_t core_id) = 0;

    virtual void FinalizeCommit() = 0;

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

    // In drain cache mode, Movenext/Current will drain out the cached the
    // tuples in each buckets
    bool drain_cache_mode_{false};

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
class TemplateCcScanner : public CcScanner
{
public:
    TemplateCcScanner(ScanDirection direct,
                      ScanIndexType index_type,
                      const KeySchema *schema)
        : CcScanner(direct, index_type),
          scans_(),
          curr_shard_code_(0),
          curr_tuple_(nullptr),
          key_schema_(schema)
    {
    }

    ScanCache *AddShard(uint32_t shard_code) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto em_it = scans_.try_emplace(shard_code, this, key_schema_);
        em_it.first->second.Reset();
        return &em_it.first->second;
    }

    void ResetShards(size_t shard_cnt) override
    {
        assert(false &&
               "ResetShards is designed for RangePartitionedCcmScanner.");
    }

    void ResetCaches() override
    {
        for (auto &[shard_code, cache] : scans_)
        {
            cache.Reset();
        }

        curr_shard_code_ = 0;
        curr_tuple_ = 0;
    }

    uint32_t BlockedShard() const override
    {
        return curr_shard_code_;
    }

    ScanCache *Cache(uint32_t shard_code) override
    {
        // For TemplateCcScanner, shard_code is (ng_id << 10) + core_id.
        return &scans_.at(shard_code);
    }

    void ShardCacheSizes(std::vector<std::pair<uint32_t, size_t>>
                             *shard_code_and_sizes) const override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        for (const auto &[shard_code, cache] : scans_)
        {
            shard_code_and_sizes->emplace_back(shard_code, cache.Size());
        }
    }

    void ShardCacheLastTuples(
        std::vector<const ScanTuple *> *last_tuples) const override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        for (const auto &[shard_code, cache] : scans_)
        {
            last_tuples->emplace_back(cache.LastTuple());
        }
    }

    void ShardCacheTrailingTuples(
        std::vector<const ScanTuple *> *last_tuples) const override
    {
        // Hash partition does not have trailing tuples.
    }

    const ScanTuple *Current() override
    {
        if (curr_tuple_ != nullptr)
        {
            return curr_tuple_;
        }
        else if (status_ == ScannerStatus::Closed)
        {
            return nullptr;
        }

        const KeyT *min_key = nullptr;

        for (const auto &[shard_code, cache] : scans_)
        {
            const TemplateScanTuple<KeyT, ValueT> *tuple = cache.Current();

            if (tuple != nullptr)
            {
                if (min_key == nullptr || tuple->key_ts_ == 0 ||
                    (direct_ == ScanDirection::Forward &&
                     tuple->key_obj_ < *min_key) ||
                    (direct_ == ScanDirection::Backward &&
                     !(tuple->key_obj_ < *min_key)))
                {
                    min_key = &tuple->key_obj_;
                    curr_shard_code_ = shard_code;

                    if (tuple->key_ts_ == 0)
                    {
                        // When the key's timestamp is 0, the key is not
                        // included in the scan results. The scan tuple is
                        // only returned for later validation. Since the
                        // key is not included in the results, the key's
                        // relative order w.r.t. other keys is irrelevant.
                        // Hence, we terminate merging early and returns the
                        // tuple immediately. The upper-layer tx will bookkeep
                        // the gap in the scan set and skips the key.
                        min_key = &tuple->key_obj_;
                        curr_shard_code_ = shard_code;
                        break;
                    }
                }
            }
            else
            {
                if (cache.Status() == ScannerStatus::Blocked)
                {
                    if (!drain_cache_mode_)
                    {
                        status_ = ScannerStatus::Blocked;
                        curr_shard_code_ = shard_code;
                        curr_tuple_ = nullptr;

                        return nullptr;
                    }
                    {
                        // In drain_cache_mode_, we just it iterate all cache
                    }
                }
            }
        }

        if (min_key == nullptr)
        {
            curr_tuple_ = nullptr;
            status_ = ScannerStatus::Closed;
            return nullptr;
        }
        else
        {
            curr_tuple_ = scans_.at(curr_shard_code_).Current();
            status_ = ScannerStatus::Open;
            return curr_tuple_;
        }
    }

    void MoveNext() override
    {
        if (curr_tuple_ != nullptr)
        {
            scans_.at(curr_shard_code_).MoveNext();
            curr_tuple_ = nullptr;
        }
        else if (status_ != ScannerStatus::Closed)
        {
            curr_tuple_ =
                static_cast<const TemplateScanTuple<KeyT, ValueT> *>(Current());
            if (curr_tuple_ != nullptr)
            {
                // The scanner is not blocked. Advances the cache that produces
                // the min/max key.
                scans_.at(curr_shard_code_).MoveNext();
                curr_tuple_ = nullptr;
            }
        }
    }

    void SetDrainCacheMode(bool drain_cache_mode) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        drain_cache_mode_ = drain_cache_mode;
    }

    bool GetDrainCacheMode() override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return drain_cache_mode_;
    }

    CcmScannerType Type() const override
    {
        return CcmScannerType::HashPartition;
    }

    uint32_t CacheCount() const override
    {
        return scans_.size();
    }

    void CommitAtCore(uint16_t core_id) override
    {
    }

    void FinalizeCommit() override
    {
    }

    void Reset(const KeySchema *key_schema) override
    {
        key_schema_ = key_schema;
        curr_shard_code_ = 0;
        curr_tuple_ = nullptr;

        for (auto cache_it = scans_.begin(); cache_it != scans_.end();
             ++cache_it)
        {
            cache_it->second.Reset();
        }
    }

    void Close() override
    {
        status_ = ScannerStatus::Closed;
        scans_.clear();
        curr_shard_code_ = 0;
        curr_tuple_ = 0;
    }

private:
    /// <summary>
    /// A collection of local and remote scan caches, one per core.
    /// </summary>
    std::unordered_map<uint32_t, TemplateScanCache<KeyT, ValueT>> scans_;

    uint32_t curr_shard_code_;
    const TemplateScanTuple<KeyT, ValueT> *curr_tuple_;

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

    ScanCache *AddShard(uint32_t shard_code) override
    {
        size_t curr_size = scans_.size();
        if (shard_code >= curr_size)
        {
            scans_.reserve(shard_code + 1);
            index_chain_.reserve(shard_code + 1);
            for (size_t idx = curr_size; idx < shard_code + 1; ++idx)
            {
                scans_.emplace_back(this, key_schema_);
                index_chain_.emplace_back();
            }
        }

        for (size_t idx = 0; idx < curr_size; ++idx)
        {
            scans_[idx].Reset();
            index_chain_[idx].clear();
        }

        assert(shard_code < scans_.size());

        return &scans_[shard_code];
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

    uint32_t BlockedShard() const override
    {
        return UINT32_MAX;
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

    void ShardCacheLastTuples(
        std::vector<const ScanTuple *> *last_tuples) const override
    {
        for (size_t core_id = 0; core_id < scans_.size(); ++core_id)
        {
            last_tuples->emplace_back(scans_[core_id].LastTuple());
        }
    }

    void ShardCacheTrailingTuples(
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

    void SetDrainCacheMode(bool drain_cache_mode) override
    {
        drain_cache_mode_ = drain_cache_mode;
    }

    bool GetDrainCacheMode() override
    {
        return drain_cache_mode_;
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

    uint32_t CacheCount() const override
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
                Merge(head_index);
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

    void Merge(CompoundIndex head)
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
            Merge(head, curr_head);
        }
    }

    void Merge(CompoundIndex left, CompoundIndex right)
    {
        CompoundIndex merge_head;
        CompoundIndex prev_index;

        if (left == Inf())
        {
            // The left is empty.
            return Merge(right);
        }
        else if (right == Inf())
        {
            // The right is empty.
            return Merge(left);
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

        Merge(merge_head);
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
