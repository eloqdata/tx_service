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

#include <atomic>
#include <memory>  //unique_ptr
#include <shared_mutex>
#include <utility>
#include <vector>

#include "cc/cc_entry.h"
#include "proto/cc_request.pb.h"
#include "tx_command.h"
#include "type.h"

namespace txservice
{
class CcScanner;
class TransactionExecution;

enum class AckStatus : unsigned char
{
    Unknown = 0,  // Not set ack status;
    BlockQueue,   // The cc request is in block queue
    Finished,     // THe cc request has been executed.
    ErrorTerm     // cc node group term has changed
};

enum class ResultTemplateType
{
    AcquireKeyResult = 1,
    ReadKeyResult,
    AcquireAllResult
};

struct AcquireKeyResult
{
    void Reset()
    {
        last_vali_ts_ = 0;
        commit_ts_ = 0;
        cce_addr_ = {};
        remote_ack_cnt_ = nullptr;
        remote_hd_result_is_set_ = false;
    }

    uint64_t last_vali_ts_{0};
    uint64_t commit_ts_{0};
    CcEntryAddr cce_addr_;
    // Number of remote acquire requests to be acknowledged in the transaction's
    // upload phase. For OCC protocol (optimistic write), an acquire request is
    // non-blocking, and the request's response is same as acknowledgement. For
    // OccRead/Locking protocols (pessimistic write), the request may be
    // blocked. An acknowledgement is a special response notifying the sender
    // the address and the term of the cc entry on which the request is blocked.
    std::atomic<int32_t> *remote_ack_cnt_{nullptr};

    bool IsRemoteHdResultSet(std::memory_order order) const
    {
        return reinterpret_cast<const std::atomic<bool> &>(
                   remote_hd_result_is_set_)
            .load(order);
    }

    void SetRemoteHdResult(bool value, std::memory_order order)
    {
        reinterpret_cast<std::atomic<bool> &>(remote_hd_result_is_set_)
            .store(value, order);
    }

private:
    // Use this atomic flag to indicate whether the hd_result of remote request
    // has been called SetFinish()/SetError(). Local request always set this
    // flag to `true`.
    //
    // std::vector<std::atomic<T>>.resize() compiles error. C++17 lack of
    // std::atomic_ref<T>. Wrap load/store operation as member methods.
    bool remote_hd_result_is_set_{false};
};

struct AcquireAllResult
{
    void Reset()
    {
        last_vali_ts_ = 1;
        commit_ts_ = 1;
        node_term_ = -1;
        local_cce_addr_ = {};
        blocked_remote_cce_addr_.clear();
        remote_ack_cnt_ = nullptr;
    }

    uint64_t last_vali_ts_{1};
    uint64_t commit_ts_{1};
    int64_t node_term_{-1};
    /**
     * @brief The address of the cc entry that co-locates with the sending tx in
     * the same core. The address is used to dedup the read intent/lock acquired
     * from prior reads of the local cc entry.
     *
     */
    CcEntryAddr local_cce_addr_;

    std::vector<CcEntryAddr> blocked_remote_cce_addr_;
    std::atomic<int32_t> *remote_ack_cnt_{nullptr};
};

struct ReadKeyResult
{
    void Reset()
    {
        rec_ = nullptr;
        ts_ = 0U;
        cce_addr_.SetTerm(-1);
        rec_status_ = RecordStatus::Unknown;
        lock_type_ = LockType::NoLock;
        is_local_ = true;
    }

    TxRecord *rec_;
    uint64_t ts_;
    CcEntryAddr cce_addr_;
    RecordStatus rec_status_;

    // Acquired key lock type by this read operation.
    LockType lock_type_{LockType::NoLock};

    bool is_local_{true};
};

struct ScanOpenResult
{
    ScanOpenResult()
    {
    }

    ScanOpenResult(ScanOpenResult &&other)
    {
        scan_alias_ = other.scan_alias_;
        cc_node_terms_ = std::move(other.cc_node_terms_);
        cc_node_returned_ = std::move(other.cc_node_returned_);
        scanner_ = std::move(other.scanner_);
    }

    ScanOpenResult &operator=(const ScanOpenResult &rhs) = delete;

    ScanOpenResult &operator=(ScanOpenResult &&rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        scan_alias_ = rhs.scan_alias_;
        cc_node_terms_ = std::move(rhs.cc_node_terms_);
        cc_node_returned_ = std::move(rhs.cc_node_returned_);
        scanner_ = std::move(rhs.scanner_);

        return *this;
    }

    void Reset(const std::set<NodeGroupId> &cc_node_groups)
    {
        cc_node_terms_.clear();
        cc_node_returned_.clear();

        for (NodeGroupId ng_id : cc_node_groups)
        {
            cc_node_terms_.try_emplace(ng_id, -1);
            cc_node_returned_.try_emplace(ng_id, 0);
        }
    }

    std::unique_ptr<CcScanner> scanner_{nullptr};
    uint64_t scan_alias_{0};
    // The terms of all cc node groups. As cc node groups currently employ the
    // hash partition function, a scan is directed to all cc node groups. For
    // locking-based protocols, the scan request in a cc node group may be
    // blocked, which sends an acknowledgement to the request's issuer notifying
    // the cc node's term. This vector bookkeeps which cc node groups have sent
    // acknowledgement.
    std::unordered_map<NodeGroupId, int64_t> cc_node_terms_;
    // std::vector<bool> is discouraged. We use one byte to denote if the scan
    // response toward a cc node has returned or not. We do not designate a
    // separate vector to bookkeep error codes of individual requests. This is
    // because if a scan request toward a cc node finishes with an error, the
    // error code is recorded in the cc handler result.
    std::unordered_map<NodeGroupId, uint8_t> cc_node_returned_;
};

struct RemoteScanCache
{
    RemoteScanCache() : cache_msg_(nullptr), cache_mem_size_(0)
    {
    }

    RemoteScanCache(remote::ScanCache_msg *cache_msg, uint32_t mem_size)
        : cache_msg_(cache_msg), cache_mem_size_(mem_size)
    {
    }

    size_t Size() const
    {
        return cache_msg_->scan_tuple_size();
    }

    bool IsFull() const
    {
        return cache_mem_size_ >= 1024;
    }

    const std::string &LastScanKey() const
    {
        return cache_msg_->scan_tuple(cache_msg_->scan_tuple_size() - 1).key();
    }

    remote::ScanTuple_msg *LastTuple()
    {
        auto *tuples = cache_msg_->mutable_scan_tuple();
        assert(!tuples->empty());
        return &tuples->at(tuples->size() - 1);
    }

    remote::ScanCache_msg *cache_msg_;
    uint32_t cache_mem_size_;
};

struct RemoteScanSliceCache
{
    // Approximate meta data size in storage.
    static constexpr size_t MetaDataSize = 8;

    RemoteScanSliceCache(uint16_t shard_cnt)
        : cache_mem_size_(0),
          mem_max_bytes_(0),
          shard_cnt_(shard_cnt),
          trailing_cnt_(0)
    {
    }

    bool IsFull() const
    {
        return cache_mem_size_ >= mem_max_bytes_;
    }

    void SetCacheMaxBytes(size_t max_bytes)
    {
        mem_max_bytes_ = max_bytes;
    }

    void Reset(uint16_t shard_cnt)
    {
        key_ts_.clear();
        gap_ts_.clear();
        cce_ptr_.clear();
        cce_lock_ptr_.clear();
        term_.clear();
        rec_status_.clear();
        keys_.clear();
        records_.clear();
        cache_mem_size_ = 0;
        trailing_cnt_ = 0;
        mem_max_bytes_ = 0;
        shard_cnt_ = shard_cnt;
        archive_positions_.clear();
        archive_records_.clear();
    }

    void RemoveLast()
    {
        trailing_cnt_++;
    }

    uint64_t LastCce()
    {
        return cce_ptr_.at(cce_ptr_.size() - 1 - trailing_cnt_);
    }

    size_t Size() const
    {
        return cce_ptr_.size() - trailing_cnt_;
    }

    void SetLastCceLock(uint64_t lock_ptr)
    {
        assert(Size() > 0);
        cce_lock_ptr_[Size() - 1] = lock_ptr;
    }

    std::vector<uint64_t> key_ts_;
    std::vector<uint64_t> gap_ts_;
    std::vector<uint64_t> cce_ptr_;
    std::vector<uint64_t> cce_lock_ptr_;
    std::vector<int64_t> term_;
    std::vector<remote::RecordStatusType> rec_status_;
    std::string keys_;
    std::string records_;
    uint32_t cache_mem_size_;
    uint32_t mem_max_bytes_;
    uint16_t shard_cnt_;
    size_t trailing_cnt_;

    // The first element of archive_positions_ is the index of key_ts_ to
    // backfill and the second element is the position in records_ to be
    // backfilled after snapshot being fetched.
    std::vector<std::pair<size_t, size_t>> archive_positions_;
    std::vector<std::string> archive_records_;
};

struct RangeScanSliceResult
{
    RangeScanSliceResult()
        : last_key_(),
          slice_position_(SlicePosition::FirstSlice),
          cc_ng_id_(0),
          ccm_scanner_(nullptr),
          is_local_(true),
          last_key_status_(LastKeySetStatus::Unset)
    {
    }

    RangeScanSliceResult(TxKey last_key, SlicePosition status)
        : last_key_(std::move(last_key)),
          slice_position_(status),
          cc_ng_id_(0),
          ccm_scanner_(nullptr),
          is_local_(true),
          last_key_status_(LastKeySetStatus::Setup)
    {
    }

    RangeScanSliceResult(RangeScanSliceResult &&rhs)
        : last_key_(std::move(rhs.last_key_)),
          slice_position_(rhs.slice_position_),
          cc_ng_id_(rhs.cc_ng_id_),
          is_local_(rhs.is_local_),
          last_key_status_(rhs.last_key_status_.load(std::memory_order_acquire))
    {
        if (rhs.is_local_)
        {
            ccm_scanner_ = rhs.ccm_scanner_;
        }
        else
        {
            remote_scan_caches_ = rhs.remote_scan_caches_;
        }
    }

    ~RangeScanSliceResult() = default;

    RangeScanSliceResult &operator=(RangeScanSliceResult &&rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        last_key_ = std::move(rhs.last_key_);
        slice_position_ = rhs.slice_position_;
        is_local_ = rhs.is_local_;
        cc_ng_id_ = rhs.cc_ng_id_;
        last_key_status_.store(
            rhs.last_key_status_.load(std::memory_order_acquire),
            std::memory_order_release);

        if (rhs.is_local_)
        {
            ccm_scanner_ = rhs.ccm_scanner_;
        }
        else
        {
            remote_scan_caches_ = rhs.remote_scan_caches_;
        }

        return *this;
    }

    void Reset()
    {
        last_key_status_.store(LastKeySetStatus::Unset,
                               std::memory_order_release);
        last_key_ = TxKey();
    }

    const TxKey *SetLastKey(TxKey key)
    {
        assert(last_key_status_.load(std::memory_order_acquire) ==
               LastKeySetStatus::Unset);
        last_key_ = std::move(key);
        last_key_status_.store(LastKeySetStatus::Setup,
                               std::memory_order_release);

        return &last_key_;
    }

    template <typename KeyT>
    std::pair<const KeyT *, bool> UpdateLastKey(const KeyT *key,
                                                SlicePosition slice_pos)
    {
        bool success = false;

        LastKeySetStatus actual = LastKeySetStatus::Unset;
        if (last_key_status_.compare_exchange_strong(
                actual, LastKeySetStatus::Setting, std::memory_order_acq_rel))
        {
            slice_position_ = slice_pos;

            // If the slice position is the last or the first, this is the last
            // scan batch, which must end with positive/negative infinity or the
            // request's end key. In both cases, the input key is a valid
            // reference throughout the lifetime of RangeScanSliceResult. So,
            // the tx key does not own a new copy of the input key.
            if (slice_pos == SlicePosition::FirstSlice ||
                slice_pos == SlicePosition::LastSlice)
            {
                last_key_ = TxKey(key);
            }
            else
            {
                last_key_ = key->CloneTxKey();
            }

            last_key_status_.store(LastKeySetStatus::Setup,
                                   std::memory_order_release);
            success = true;
        }
        else
        {
            if (actual != LastKeySetStatus::Setup)
            {
                while (last_key_status_.load(std::memory_order_acquire) !=
                       LastKeySetStatus::Setup)
                {
                    // Busy poll.
                }
            }
        }

        return {last_key_.GetKey<KeyT>(), success};
    }

    std::pair<const TxKey *, bool> PeekLastKey() const
    {
        if (last_key_status_.load(std::memory_order_acquire) ==
            LastKeySetStatus::Setup)
        {
            return {&last_key_, true};
        }
        else
        {
            return {nullptr, false};
        }
    }

    TxKey MoveLastKey()
    {
        last_key_status_.store(LastKeySetStatus::Unset,
                               std::memory_order_release);
        return std::move(last_key_);
    }

    /**
     * @brief The last key of the current scan batch. For forward scans, the
     * last key is the exclusive end of the current slice, which is the
     * inclusive start key of the next scan batch. For backward scans, the
     * last key is the inclusive start of the current slice, which is the
     * exclusive start key of the next scan batch.
     */
    TxKey last_key_;

    SlicePosition slice_position_;
    NodeGroupId cc_ng_id_{0};

    union
    {
        CcScanner *ccm_scanner_;
        std::vector<RemoteScanSliceCache> *remote_scan_caches_;
    };
    bool is_local_{true};

    /**
     * For scene like: (1-write, n-read), atomic variable has obvious
     * performance advantage over mutex/shared_mutex. For readers, mutex needs
     * to modify a flag, and shared_mutex needs to modify a counter. However,
     * atomic variable merely load a variable.
     */
    enum struct LastKeySetStatus : uint8_t
    {
        Unset,
        Setting,
        Setup,
    };
    std::atomic<LastKeySetStatus> last_key_status_;
};

struct ScanNextResult
{
    bool is_local_;
    int64_t term_;
    uint32_t node_group_id_;
};

struct InitTxResult
{
    TxId txid_;
    uint64_t start_ts_;
    // The term of the cc node group to which the tx is bound.
    int64_t term_;
};

struct PostProcessResult
{
    PostProcessResult() = default;

    PostProcessResult(PostProcessResult &&rhs) noexcept
        : conflicting_tx_cnt_(
              rhs.conflicting_tx_cnt_.load(std::memory_order_relaxed))
    {
    }

    PostProcessResult &operator=(const PostProcessResult &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        conflicting_tx_cnt_.store(
            rhs.conflicting_tx_cnt_.load(std::memory_order_relaxed),
            std::memory_order_relaxed);
        return *this;
    }

    void IncrConflictingTx(int32_t cnt = 1)
    {
        if (cnt > 0)
        {
            conflicting_tx_cnt_.fetch_add(cnt, std::memory_order_relaxed);
        }
    }

    size_t Size() const
    {
        return conflicting_tx_cnt_.load(std::memory_order_relaxed);
    }

    void Clear()
    {
        conflicting_tx_cnt_.store(0, std::memory_order_relaxed);
        is_local_ = true;
    }

    std::atomic<int32_t> conflicting_tx_cnt_{0};
    bool is_local_{true};
};

struct ObjectCommandResult
{
    void Reset()
    {
        commit_ts_ = 0;
        last_vali_ts_ = 0;
        cce_addr_ = CcEntryAddr{};
        rec_status_ = RecordStatus::Unknown;
        lock_acquired_ = LockType::NoLock;
        object_modified_ = false;
        is_local_ = true;
        cmd_result_ = nullptr;
        ttl_expired_ = false;
        ttl_ = UINT64_MAX;
        ttl_reset_ = false;
    }

    // cce commit_ts, for validation?
    uint64_t commit_ts_{};
    // currently not used, used to set commit_ts_, only write cmd needs it?
    uint64_t last_vali_ts_{};
    // add read write set
    CcEntryAddr cce_addr_{};

    RecordStatus rec_status_{RecordStatus::Unknown};
    LockType lock_acquired_{LockType::NoLock};
    // True: The command has updated the object and will be added into write set
    // for writing log.
    // False: The command failed to exec or is readonly, does not need to write
    // log.
    bool object_modified_{};

    // Whether the command operation executting on local node.
    bool is_local_{true};

    // Only used for remote request deserializes the received command result.
    TxCommandResult *cmd_result_{nullptr};

    // TTL expired
    bool ttl_expired_{false};
    // TTL of this key after command is executed. This written to log to decide
    // if commands from this tx still need to be replayed during recovery.
    uint64_t ttl_{UINT64_MAX};
    // TTL reset
    bool ttl_reset_{false};
};

struct UploadBatchResult
{
    UploadBatchResult() = default;

    UploadBatchResult(const UploadBatchResult &rhs) = delete;
    UploadBatchResult(UploadBatchResult &&rhs) noexcept
        : term_(rhs.term_.load(std::memory_order_relaxed)),
          node_group_id_(rhs.node_group_id_)
    {
    }

    UploadBatchResult &operator=(const UploadBatchResult &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        term_ = rhs.term_.load(std::memory_order_relaxed);
        node_group_id_ = rhs.node_group_id_;
        return *this;
    }

    void Reset()
    {
        term_ = -1;
        node_group_id_ = 0;
    }

    std::atomic<int64_t> term_{-1};
    NodeGroupId node_group_id_{0};
};

struct GenerateSkParallelResult
{
    GenerateSkParallelResult() = default;
    GenerateSkParallelResult(const GenerateSkParallelResult &rhs) = delete;
    GenerateSkParallelResult(GenerateSkParallelResult &&rhs) noexcept = default;

    GenerateSkParallelResult &operator=(GenerateSkParallelResult &&rhs) =
        default;

    void Reset()
    {
        indexes_multikey_attr_.clear();
        pack_sk_error_.Reset();
    }

    void IndexesMergeMultiKeyAttr(
        const std::vector<TableName> &indexes_name,
        const std::vector<bool> &indexes_multikey,
        const std::vector<const MultiKeyPaths *> &indexes_multikey_paths)
    {
        if (indexes_multikey_attr_.empty())
        {
            indexes_multikey_attr_.reserve(indexes_name.size());
            for (uint16_t idx = 0; idx < indexes_name.size(); ++idx)
            {
                const TableName *index_name = &indexes_name[idx];
                bool multikey = indexes_multikey[idx];
                if (multikey)
                {
                    const MultiKeyPaths *multikey_paths =
                        indexes_multikey_paths[idx];
                    assert(multikey_paths);
                    indexes_multikey_attr_.emplace_back(
                        index_name, true, multikey_paths->Clone());
                }
                else
                {
                    indexes_multikey_attr_.emplace_back(
                        index_name, false, nullptr);
                }
            }
        }
        else
        {
            for (uint16_t idx = 0; idx < indexes_name.size(); ++idx)
            {
                assert(*indexes_multikey_attr_[idx].index_name_ ==
                       indexes_name[idx]);

                bool multikey = indexes_multikey[idx];
                if (multikey)
                {
                    indexes_multikey_attr_[idx].multikey_ = true;

                    const MultiKeyPaths *multikey_paths =
                        indexes_multikey_paths[idx];
                    assert(multikey_paths);
                    if (indexes_multikey_attr_[idx].multikey_paths_)
                    {
                        indexes_multikey_attr_[idx].multikey_paths_->MergeWith(
                            *multikey_paths);
                    }
                    else
                    {
                        indexes_multikey_attr_[idx].multikey_paths_ =
                            multikey_paths->Clone();
                    }
                }
            }
        }
    }

    void IndexesMergeMultiKeyAttr(
        const std::vector<TableName> &indexes_name,
        const std::vector<bool> &indexes_multikey,
        std::vector<MultiKeyPaths::Uptr> &indexes_multikey_paths)
    {
        if (indexes_multikey_attr_.empty())
        {
            indexes_multikey_attr_.reserve(indexes_name.size());
            for (uint16_t idx = 0; idx < indexes_name.size(); ++idx)
            {
                const TableName *index_name = &indexes_name[idx];
                bool multikey = indexes_multikey[idx];
                if (multikey)
                {
                    MultiKeyPaths::Uptr &multikey_paths =
                        indexes_multikey_paths[idx];
                    assert(multikey_paths);
                    indexes_multikey_attr_.emplace_back(
                        index_name, true, std::move(multikey_paths));
                }
                else
                {
                    indexes_multikey_attr_.emplace_back(
                        index_name, false, nullptr);
                }
            }
        }
        else
        {
            for (uint16_t idx = 0; idx < indexes_name.size(); ++idx)
            {
                assert(*indexes_multikey_attr_[idx].index_name_ ==
                       indexes_name[idx]);

                bool multikey = indexes_multikey[idx];
                if (multikey)
                {
                    indexes_multikey_attr_[idx].multikey_ = true;

                    MultiKeyPaths::Uptr &multikey_paths =
                        indexes_multikey_paths[idx];
                    assert(multikey_paths);
                    if (indexes_multikey_attr_[idx].multikey_paths_)
                    {
                        indexes_multikey_attr_[idx].multikey_paths_->MergeWith(
                            *multikey_paths);
                    }
                    else
                    {
                        indexes_multikey_attr_[idx].multikey_paths_ =
                            std::move(multikey_paths);
                    }
                }
            }
        }
    }

    std::vector<MultiKeyAttr> indexes_multikey_attr_;

    // When creating index violates some constraint, reports the concrete reason
    // to calculation engine.
    PackSkError pack_sk_error_;
};
}  // namespace txservice
