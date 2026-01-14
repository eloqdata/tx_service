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

#include <algorithm>  // std::max
#include <atomic>
#include <cassert>
#include <cstdint>
#include <deque>
#include <list>
#include <memory>  // std::make_unique, make_shared, shared_ptr
#include <string>
#include <type_traits>
#include <utility>  // std::move
#include <vector>

#include "cc_req_base.h"
#include "non_blocking_lock.h"
#include "slice_data_item.h"
#include "tx_command.h"
#include "tx_id.h"
#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
class CcMap;
class CcShardHeap;
class CcShard;
struct StandbyForwardEntry;

template <typename KeyT,
          typename ValueT,
          bool VersionedRecord,
          bool RangePartitioned>
class TemplateCcMap;

struct LruEntry;

template <typename KeyT,
          typename ValueT,
          bool VersionedRecord,
          bool RangePartitioned>
struct CcEntry;

template <typename KeyT,
          typename ValueT,
          bool VersionedRecord,
          bool RangePartitioned>
struct CcPage;

struct FlushRecord
{
private:
    std::variant<std::shared_ptr<TxRecord>, BlobTxRecord> payload_;

    // Variant holds either TxKey or key index.
    std::variant<TxKey, size_t> flush_key_;

public:
    RecordStatus payload_status_{RecordStatus::Unknown};
    // Size of this version FlushRecord. 0 if the record is in Deleted status.
    // 1. Used to updated the cce::data_store_size_ after this version item has
    // been flushed to the data store.
    // 2. Used to calculate the key of the subslice when the slice need to be
    // split. At this time, the value is the size of the item in the base table.
    // In short, the version item has been persisted, and does not need to be
    // flushed, therefore, the payload of this FlushRecord is nullptr.
    uint32_t post_flush_size_{0};

    uint64_t commit_ts_{1U};
    // cce is nullptr means this cce is already persisted on kv and we
    // do not need to flush / update its ckpt ts.
    LruEntry *cce_{nullptr};

    int32_t partition_id_{-1};

    FlushRecord() = default;

    // Only used by "data_sync_vec->emplace_back()" in
    // LocalCcShards::DataSync().
    FlushRecord(TxKey key,
                std::shared_ptr<TxRecord> payload,
                RecordStatus payload_status,
                uint64_t commit_ts,
                LruEntry *cce,
                uint32_t post_flush_size,
                int32_t partition_id)
        : payload_(std::in_place_type<std::shared_ptr<TxRecord>>,
                   std::move(payload)),
          flush_key_(std::in_place_type<TxKey>, std::move(key)),
          payload_status_(payload_status),
          post_flush_size_(post_flush_size),
          commit_ts_(commit_ts),
          cce_(cce),
          partition_id_(partition_id)
    {
    }

    FlushRecord(TxKey key,
                const BlobTxRecord &payload,
                RecordStatus payload_status,
                uint64_t commit_ts,
                LruEntry *cce,
                int32_t post_flush_size,
                int32_t partition_id)

        :  // use copy constructor to avoid reallocating memory in
           // DataSyncScanCc
          payload_(std::in_place_type<BlobTxRecord>, payload),
          flush_key_(std::in_place_type<TxKey>, std::move(key)),
          payload_status_(payload_status),
          post_flush_size_(post_flush_size),
          commit_ts_(commit_ts),
          cce_(cce),
          partition_id_(partition_id)
    {
    }

    ~FlushRecord() = default;

    FlushRecord &operator=(FlushRecord &&rhs) noexcept
    {
        if (this == &rhs)
        {
            return *this;
        }

        flush_key_ = std::move(rhs.flush_key_);

        payload_ = std::move(rhs.payload_);
        payload_status_ = rhs.payload_status_;
        commit_ts_ = rhs.commit_ts_;
        cce_ = rhs.cce_;
        post_flush_size_ = rhs.post_flush_size_;
        partition_id_ = rhs.partition_id_;
        return *this;
    }

    FlushRecord(FlushRecord &&rhs) noexcept
    {
        flush_key_ = std::move(rhs.flush_key_);

        payload_ = std::move(rhs.payload_);
        payload_status_ = rhs.payload_status_;
        commit_ts_ = rhs.commit_ts_;
        post_flush_size_ = rhs.post_flush_size_;
        cce_ = rhs.cce_;
        partition_id_ = rhs.partition_id_;
    }

    FlushRecord(const FlushRecord &) = delete;

    void CloneOrCopyKey(const TxKey &key)
    {
        // Check if the variant already holds TxKey, if so copy it
        if (std::holds_alternative<TxKey>(flush_key_))
        {
            if (auto &stored_key = std::get<TxKey>(flush_key_);
                stored_key.IsOwner())
            {
                stored_key.Copy(key);
            }
            else
            {
                stored_key = key.Clone();
            }
        }
        else
        {
            // If the variant does not hold TxKey, assign it a cloned version of
            // key
            flush_key_ = key.Clone();  // This will store TxKey in the variant
        }
    }

    void SetKeyIndex(size_t offset)
    {
        flush_key_ = offset;
    }

    size_t GetKeyIndex() const
    {
        assert(std::holds_alternative<size_t>(flush_key_));
        return std::get<size_t>(flush_key_);
    }

    void SetKey(TxKey key)
    {
        flush_key_ = std::move(key);
    }

    void SetVersionedPayload(std::shared_ptr<TxRecord> sptr)
    {
        payload_ = sptr;
    }

    void SetNonVersionedPayload(const TxRecord *ptr)
    {
        if (payload_.index() == 0)
        {
            payload_.emplace<1>();
        }
        else
        {
            std::get<1>(payload_).value_.clear();
        }
        if (ptr != nullptr)
        {
            ptr->Serialize(std::get<1>(payload_).value_);
            if (ptr->HasTTL())
            {
                std::get<1>(payload_).SetTTL(ptr->GetTTL());
            }
        }
    }

    const TxRecord *Payload() const
    {
        if (payload_.index() == 0)
        {
            if (std::get<0>(payload_) == nullptr)
            {
                return nullptr;
            }
            return std::get<0>(payload_).get();
        }
        else
        {
            if (std::get<1>(payload_).value_.empty())
            {
                return nullptr;
            }
            return &std::get<1>(payload_);
        }
    }

    std::shared_ptr<TxRecord> ReleaseVersionedPayload()
    {
        assert(payload_.index() == 0);
        return std::move(std::get<0>(payload_));
    }

    size_t PayloadSize() const
    {
        return Payload() == nullptr ? 0 : Payload()->Size();
    }

    const BlobTxRecord &GetNonVersionedPayload()
    {
        assert(payload_.index() == 1);
        return std::get<1>(payload_);
    }

    bool HoldsVersionedPayload() const
    {
        return payload_.index() == 0;
    }

    TxKey Key() const;

    uint64_t FlushSize()
    {
        if (std::holds_alternative<TxKey>(flush_key_))
        {
            return Key().Size() + PayloadSize();
        }
        else
        {
            return sizeof(size_t) + PayloadSize();
        }
    }
};

struct LruEntry
{
    ~LruEntry() = default;
    /**
     * @brief Get key lock from lock array if it is null.
     *
     */
    NonBlockingLock &GetOrCreateKeyLock(CcShard *ccs,
                                        CcMap *ccm,
                                        LruPage *page);

    NonBlockingLock *GetKeyLock() const;

    NonBlockingLock *GetGapLock() const;

    KeyGapLockAndExtraData *GetLockAddr() const;

    /**
     * @brief When release a lock, ccentry should call TryResetKeyLock to try to
     * recycle the lock ptr to lock array if lock set is empty.
     *
     */
    bool RecycleKeyLock(CcShard &ccs);

    /**
     * @brief Forces to clear the locks on the cc entry. This is called when a
     * node fails over and its in-memory cc maps are cleared.
     *
     * @param ccs
     */
    void ClearLocks(CcShard &ccs,
                    NodeGroupId ng_id,
                    bool invalidate_owner_term = false);

    CcMap *GetCcMap() const
    {
        return cc_lock_and_extra_ != nullptr ? cc_lock_and_extra_->GetCcMap()
                                             : nullptr;
    }

    LruPage *GetCcPage() const
    {
        return cc_lock_and_extra_ != nullptr ? cc_lock_and_extra_->GetCcPage()
                                             : nullptr;
    }

    void UpdateCcPage(LruPage *page)
    {
        if (cc_lock_and_extra_ != nullptr)
        {
            cc_lock_and_extra_->UpdateCcPage(page);
        }
    }
    void UpdateBufferedCommandCnt(CcShard *shard, int64_t delta);
    BufferedTxnCmdList &BufferedCommandList()
    {
        assert(cc_lock_and_extra_ != nullptr);
        return cc_lock_and_extra_->BufferedCommandList();
    }

    bool HasBufferedCommandList()
    {
        return cc_lock_and_extra_ != nullptr &&
               cc_lock_and_extra_->HasBufferedCommandList();
    }

    void PushBlockCmdRequest(CcRequestBase *req)
    {
        if (cc_lock_and_extra_ != nullptr)
        {
            cc_lock_and_extra_->KeyLock()->PushBlockCmdRequest(req);
        }
    }
    bool AbortBlockCmdRequest(TxNumber txid, CcErrorCode err, CcShard *ccs)
    {
        if (cc_lock_and_extra_ != nullptr)
        {
            bool ret = cc_lock_and_extra_->KeyLock()->AbortBlockCmdRequest(
                txid, err, ccs);
            if (ret)
            {
                RecycleKeyLock(*ccs);
            }

            return ret;
        }
        return true;
    }

    StandbyForwardEntry *ForwardEntry()
    {
        assert(cc_lock_and_extra_ != nullptr);
        return cc_lock_and_extra_->ForwardEntry();
    }

    void SetForwardEntry(std::unique_ptr<StandbyForwardEntry> entry)
    {
        assert(cc_lock_and_extra_ != nullptr);
        cc_lock_and_extra_->SetForwardEntry(std::move(entry));
    }

    std::unique_ptr<StandbyForwardEntry> ReleaseForwardEntry()
    {
        assert(cc_lock_and_extra_ != nullptr);
        return cc_lock_and_extra_->ReleaseForwardEntry();
    }

    KeyGapLockAndExtraData *GetKeyGapLockAndExtraData()
    {
        return cc_lock_and_extra_;
    }
    // TODO(liunyl): Lock structure needs to be templated too to differentiate
    // between versioned and non-versioned.
    KeyGapLockAndExtraData *cc_lock_and_extra_{nullptr};
};
struct EntryInfo
{
    uint64_t CkptTs() const
    {
        assert(false);
        return 0;
    }

    void SetCkptTs(uint64_t ts)
    {
        uint64_t curr_val = commit_ts_and_status_;
        uint64_t curr_commit_ts = curr_val >> 8;
        if (curr_commit_ts <= ts)
        {
            commit_ts_and_status_ = curr_val | 0x10;
        }
    }

    void SetDataStoreSize(int32_t size)
    {
        assert(false);
    }

    int32_t DataStoreSize() const
    {
        assert(false);
        return 0;
    }
    /**
     * @brief The 8-byte integer encodes the record's commit timestamp and
     * status. The higher 7 bytes represent the timestamp. The 7-byte integer is
     * big enough to encode 100 years from now on (2024) in micro seconds. The
     * lowest 4 bits (0-3 bits) represent record status. The next 4 bits (4-7
     * bits) are reserved for other usage: when MVCC is not needed, the 5th bit
     * represents whether or not the latest version has been flushed. And, the
     * 6th bit represents wheter or not the cce is in progress of being ckpt to
     * kv store.
     */
    uint64_t commit_ts_and_status_{0};
};

struct RangePartitionedEntryInfo
{
    uint64_t CkptTs() const
    {
        return 0;
    }
    void SetCkptTs(uint64_t ts)
    {
        uint64_t curr_val = commit_ts_and_status_;
        uint64_t curr_commit_ts = curr_val >> 8;
        if (curr_commit_ts <= ts)
        {
            commit_ts_and_status_ = curr_val | 0x10;
        }
    }

    void SetDataStoreSize(int32_t size)
    {
        data_store_size_ = size;
    }

    int32_t DataStoreSize() const
    {
        return data_store_size_;
    }

    /**
     * @brief The 8-byte integer encodes the record's commit timestamp and
     * status. The higher 7 bytes represent the timestamp. The 7-byte integer is
     * big enough to encode 100 years from now on (2024) in micro seconds. The
     * lowest 4 bits (0-3 bits) represent record status. The next 4 bits (4-7
     * bits) are reserved for other usage: when MVCC is not needed, the 5th bit
     * represents whether or not the latest version has been flushed. And, the
     * 6th bit represents wheter or not the cce is in progress of being ckpt to
     * kv store.
     */
    uint64_t commit_ts_and_status_{0};
    /**
     * @brief Size of this record in data store.
     * INT32_MAX is a special value that means unknown size.
     * Unkown size is used during log replay where the latest version
     * is directly written into ccmap and we don't know the record size
     * in KV storage.
     */
    int32_t data_store_size_{INT32_MAX};
};

struct VersionedEntryInfo
{
    uint64_t CkptTs() const
    {
        return ckpt_ts_;
    }

    void SetCkptTs(uint64_t ts)
    {
        if (ckpt_ts_ <= ts)
        {
            ckpt_ts_ = ts;
        }
    }
    void SetDataStoreSize(int32_t size)
    {
        assert(false);
    }

    int32_t DataStoreSize() const
    {
        assert(false);
        return 0;
    }
    /**
     * @brief The 8-byte integer encodes the record's commit timestamp and
     * lowest 4 bits (0-3 bits) represent record status. The next 4 bits (4-7
     * bits) are reserved for other usage: when MVCC is not needed, the 5th bit
     * represents whether or not the latest version has been flushed. And, the
     * 6th bit represents wheter or not the cce is in progress of being ckpt to
     * kv store.
     */
    uint64_t commit_ts_and_status_{0};
    // The commit timestamp of the latest checkpoint version record.
    uint64_t ckpt_ts_{0};
};

struct RangePartitionedVersionedEntryInfo
{
    uint64_t CkptTs() const
    {
        return ckpt_ts_;
    }
    void SetCkptTs(uint64_t ts)
    {
        if (ckpt_ts_ <= ts)
        {
            ckpt_ts_ = ts;
        }
    }

    void SetDataStoreSize(int32_t size)
    {
        data_store_size_ = size;
    }

    int32_t DataStoreSize() const
    {
        return data_store_size_;
    }

    /**
     * @brief The 8-byte integer encodes the record's commit timestamp and
     * lowest 4 bits (0-3 bits) represent record status. The next 4 bits (4-7
     * bits) are reserved for other usage: when MVCC is not needed, the 5th bit
     * represents whether or not the latest version has been flushed. And, the
     * 6th bit represents wheter or not the cce is in progress of being ckpt to
     * kv store.
     */
    uint64_t commit_ts_and_status_{0};
    /**
     * @brief Size of this record in data store.
     * INT32_MAX is a special value that means unknown size.
     * Unkown size is used during log replay where the latest version
     * is directly written into ccmap and we don't know the record size
     * in KV storage.
     */
    int32_t data_store_size_{INT32_MAX};
    // The commit timestamp of the latest checkpoint version record.
    uint64_t ckpt_ts_{0};
};

template <bool Versioned, bool RangePartitioned>
struct VersionedLruEntry : public LruEntry
{
public:
    VersionedLruEntry()
    {
        uint8_t unknown_status = (uint8_t) RecordStatus::Unknown;
        uint64_t init_ts = 0;
        entry_info_.commit_ts_and_status_ = (init_ts << 8) | unknown_status;
    }

    ~VersionedLruEntry() = default;

    /**
     * @brief check whether the entry can be kicked out from ccmap, iff no key
     * lock, no gap lock and not 'dirty' entry (entry which has been
     * checkpointed since the last change).
     *
     * @return true: entry can be kicked out.
     */
    bool IsFree() const;

    uint64_t CommitTs() const
    {
        return entry_info_.commit_ts_and_status_ >> 8;
    }

    void SetBeingCkpt();

    void ClearBeingCkpt();

    bool GetBeingCkpt() const;

    uint64_t CkptTs() const
    {
        return entry_info_.CkptTs();
    }

    /**
     * @brief Updates the checkpoint timestamp such that it is no smaller than
     * the input timestamp. The method is called by the tx processor thread,
     * after flushing the key-value pair to the data store, or the tx processor
     * thread which back fills the cache-miss record into memory.
     *
     * @param ts
     */
    void SetCkptTs(uint64_t ts)
    {
        entry_info_.SetCkptTs(ts);
    }

    bool IsPersistent() const;

    RecordStatus PayloadStatus() const;

    void SetCommitTsPayloadStatus(uint64_t ts, RecordStatus status);

    bool NeedCkpt() const
    {
        RecordStatus rec_status = PayloadStatus();
        return !this->IsPersistent() && (rec_status == RecordStatus::Normal ||
                                         rec_status == RecordStatus::Deleted);
    }

    // Contains meta data for this lru entry.
    std::conditional_t<
        RangePartitioned,
        std::conditional_t<Versioned,
                           RangePartitionedVersionedEntryInfo,
                           RangePartitionedEntryInfo>,
        std::conditional_t<Versioned, VersionedEntryInfo, EntryInfo>>
        entry_info_;
};

/**
 * @brief Used as the result value type of read(scan/get) operation.
 *
 * @param payload_ptr_ Point to an payload saved in CcEntry or archives_.
 * Notice: "payload_ptr_" should not be deleted explicitly.
 * @param payload_status_
 * @param commit_ts_
 */
template <typename ValueT>
struct VersionResultRecord
{
public:
    std::shared_ptr<ValueT> payload_ptr_;
    RecordStatus payload_status_;
    uint64_t commit_ts_;

    VersionResultRecord()
        : payload_ptr_(nullptr),
          payload_status_(RecordStatus::Unknown),
          commit_ts_(1)
    {
    }
};

template <typename ValueT>
struct VersionRecord
{
public:
    std::shared_ptr<ValueT> payload_;
    uint64_t commit_ts_;
    RecordStatus payload_status_;

    VersionRecord()
        : payload_(nullptr),
          commit_ts_(1),
          payload_status_(RecordStatus::Unknown)
    {
    }

    VersionRecord(std::shared_ptr<ValueT> payload,
                  uint64_t commit_ts,
                  RecordStatus status)
        : payload_(std::move(payload)),
          commit_ts_(commit_ts),
          payload_status_(status)
    {
    }

    VersionRecord(const VersionRecord<ValueT> &rhs)
    {
        payload_ = std::move(rhs.payload_);
        payload_status_ = rhs.payload_status_;
        commit_ts_ = rhs.commit_ts_;
    }
    VersionRecord &operator=(const VersionRecord<ValueT> &rhs)
    {
        payload_ = std::move(rhs.payload_);
        payload_status_ = rhs.payload_status_;
        commit_ts_ = rhs.commit_ts_;
        return *this;
    }

    VersionRecord &operator=(VersionRecord<ValueT> &&rhs)
    {
        if (this != &rhs)
        {
            payload_ = std::move(rhs.payload_);
            payload_status_ = rhs.payload_status_;
            commit_ts_ = rhs.commit_ts_;
        }
        return *this;
    }

    size_t MemUsage() const
    {
        size_t mem_usage_ = sizeof(*this);
        if (payload_ != nullptr)
        {
            mem_usage_ += payload_->MemUsage();
        }
        return mem_usage_;
    }

    bool NeedsDefrag(mi_heap_t *heap)
    {
        bool defrag = false;
        TxRecord *payload = static_cast<TxRecord *>(payload_.get());
        if (payload != nullptr)
        {
            defrag = payload->NeedsDefrag(heap);
        }
        return defrag;
    }

    void CloneForDefragment(VersionRecord<ValueT> *clone)
    {
        clone->payload_status_ = payload_status_;
        clone->commit_ts_ = commit_ts_;
        if (payload_ != nullptr)
        {
            clone->payload_ = std::make_shared<ValueT>(*payload_);
        }
        else
        {
            clone->payload_ = nullptr;
        }
    }
};

/**
 * VersionedPayload is the payload type for versioned cc map. It will store the
 * current payload as a shared pointer to avoid serialize and copy on scanning.
 * When updating the payload, it will create a new payload and assign it to
 * cur_payload_.
 * It also maintains a list of archived payloads for multi versioning.
 */
template <typename ValueT>
struct VersionedPayload
{
    void SetArchives(std::unique_ptr<std::list<VersionRecord<ValueT>>> archives)
    {
        archives_ = std::move(archives);
    }

    std::list<VersionRecord<ValueT>> *GetArchives() const
    {
        return archives_.get();
    }

    std::unique_ptr<std::list<VersionRecord<ValueT>>> ReleaseArchives()
    {
        return std::move(archives_);
    }

    void SetCurrentPayload(const ValueT *payload)
    {
        if (payload == nullptr)
        {
            cur_payload_ = nullptr;
            return;
        }
        if (cur_payload_ != nullptr && cur_payload_.use_count() == 1)
        {
            *(cur_payload_) = *payload;
        }
        else
        {
            cur_payload_ = std::make_shared<ValueT>(*payload);
        }
    }

    void PassInCurrentPayload(std::unique_ptr<TxRecord> payload)
    {
        if (payload == nullptr)
        {
            cur_payload_ = nullptr;
        }
        else
        {
            cur_payload_.reset(static_cast<ValueT *>(payload.release()));
        }
    }

    void DeserializeCurrentPayload(const char *data, size_t &offset)
    {
        if (cur_payload_.use_count() != 1)
        {
            cur_payload_ = std::make_shared<ValueT>();
        }
        cur_payload_->Deserialize(data, offset);
    }

    std::shared_ptr<ValueT> VersionedCurrentPayload()
    {
        return cur_payload_;
    }

    std::shared_ptr<ValueT> cur_payload_{nullptr};
    std::unique_ptr<std::list<VersionRecord<ValueT>>> archives_{nullptr};
};

/**
 * NonVersionedPayload is the payload type for non-versioned cc map. It will
 * store the current payload as a unique pointer since the update is always
 * applied on the current payload so that we do not need to create a new payload
 * every time the payload is updated. This is more efficient if we're usually
 * just updating a small portion of the payload or if the payload is large.
 */
template <typename ValueT>
struct NonVersionedPayload
{
    void SetArchives(std::unique_ptr<std::list<VersionRecord<ValueT>>> archives)
    {
        assert(false);
    }

    std::list<VersionRecord<ValueT>> *GetArchives() const
    {
        assert(false);
        return nullptr;
    }

    std::unique_ptr<std::list<VersionRecord<ValueT>>> ReleaseArchives()
    {
        assert(false);
        return nullptr;
    }

    void SetCurrentPayload(const ValueT *payload)
    {
        if (payload == nullptr)
        {
            cur_payload_ = nullptr;
        }
        else
        {
            cur_payload_ = std::make_unique<ValueT>(*payload);
        }
    }

    void PassInCurrentPayload(std::unique_ptr<TxRecord> payload)
    {
        if (payload == nullptr)
        {
            cur_payload_ = nullptr;
        }
        else
        {
            cur_payload_.reset(static_cast<ValueT *>(payload.release()));
        }
    }

    void DeserializeCurrentPayload(const char *data, size_t &offset)
    {
        if (cur_payload_ == nullptr)
        {
            cur_payload_ = std::make_unique<ValueT>();
        }
        ValueT tx_obj;
        cur_payload_.reset(static_cast<ValueT *>(
            tx_obj.DeserializeObject(data, offset).release()));
        ;
    }

    std::shared_ptr<ValueT> VersionedCurrentPayload()
    {
        assert(false);
        return nullptr;
    }

    std::unique_ptr<ValueT> cur_payload_{nullptr};
};

/**
 * @brief A map entry in the concurrency control map. An entry governs
 * concurrency control of a data item and caches the data item's newest
 * committed value, as well as historical versions if needed.
 *
 * @tparam KeyT The key type of the data item
 * @tparam ValueT The value type of the data item
 * @tparam VersionedRecord Whether the record is versioned
 * @tparam RangePartitioned Whether the record is range partitioned
 */
template <typename KeyT,
          typename ValueT,
          bool VersionedRecord,
          bool RangePartitioned>
struct CcEntry : public VersionedLruEntry<VersionedRecord, RangePartitioned>
{
public:
    /**
     * @brief A cc entry is initialized in the cc map with ts = 1, the beginning
     * of history. The record's status is initially unknown, until a tx reads
     * the key in the data store and brings the record into the cc entry, or a
     * tx updates the key with a new value. Ts = 0 is reserved to indicate
     * whether or not a key/gap is included in a scan's result.
     *
     * @param parent Pointer of the cc map to which the cc entry belongs
     */
    CcEntry() = default;

    ~CcEntry() = default;

    CcEntry(CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> &other) =
        delete;

    size_t GetCcEntryMemUsage() const
    {
        size_t mem_usage = basic_mem_overhead_;
        mem_usage += PayloadMemUsage();
        if (VersionedRecord)
        {
            mem_usage += GetArchiveMemUsage();
        }
        return mem_usage;
    }

    // For debug log use only.
    std::string KeyString() const
    {
        if (this->GetCcPage() == nullptr)
        {
            return "no lock, no key";
        }
        return reinterpret_cast<
                   CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *>(
                   this->GetCcPage())
            ->KeyOfEntry(this)
            ->ToString();
    }

    size_t PayloadMemUsage() const
    {
        if (payload_.cur_payload_ == nullptr)
        {
            return 0;
        }
        else
        {
            return payload_.cur_payload_->MemUsage();
        }
    }

    size_t PayloadSize() const
    {
        return payload_.cur_payload_ == nullptr ? 0
                                                : payload_.cur_payload_->Size();
    }

    size_t PayloadSerializedLength() const
    {
        return payload_.cur_payload_ == nullptr
                   ? 0
                   : payload_.cur_payload_->SerializedLength();
    }

    std::variant<TxCommand *, std::unique_ptr<TxCommand>> PendingCmd()
    {
        assert(this->cc_lock_and_extra_ != nullptr);
        return this->cc_lock_and_extra_->PendingCmd();
    }

    TxCommand *GetPendingCommand() const
    {
        if (this->cc_lock_and_extra_ == nullptr)
        {
            return nullptr;
        }
        return this->cc_lock_and_extra_->GetPendingCmd();
    }

    void SetPendingCmd(
        std::variant<TxCommand *, std::unique_ptr<TxCommand>> cmd)
    {
        assert(this->cc_lock_and_extra_ != nullptr);
        this->cc_lock_and_extra_->SetPendingCmd(std::move(cmd));
    }

    bool IsNullPendingCmd()
    {
        return this->cc_lock_and_extra_ == nullptr ||
               this->cc_lock_and_extra_->IsNullPendingCmd();
    }

    std::unique_ptr<ValueT> DirtyPayload()
    {
        assert(this->cc_lock_and_extra_ != nullptr);
        return std::unique_ptr<ValueT>(static_cast<ValueT *>(
            this->cc_lock_and_extra_->DirtyPayload().release()));
    }

    void SetDirtyPayload(std::unique_ptr<ValueT> dirty_payload)
    {
        auto tx_obj_uptr = std::unique_ptr<TxObject>(
            static_cast<TxObject *>(dirty_payload.release()));
        assert(this->cc_lock_and_extra_ != nullptr);
        this->cc_lock_and_extra_->SetDirtyPayload(std::move(tx_obj_uptr));
    }

    RecordStatus DirtyPayloadStatus() const
    {
        assert(this->cc_lock_and_extra_ != nullptr);
        return this->cc_lock_and_extra_->DirtyPayloadStatus();
    }

    void SetDirtyPayloadStatus(RecordStatus status)
    {
        assert(this->cc_lock_and_extra_ != nullptr);
        this->cc_lock_and_extra_->SetDirtyPayloadStatus(status);
    }

    using PayloadType = std::conditional_t<VersionedRecord,
                                           VersionedPayload<ValueT>,
                                           NonVersionedPayload<ValueT>>;
    PayloadType payload_{};

    void CloneForDefragment(
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *clone)
    {
        clone->entry_info_.commit_ts_and_status_ =
            this->entry_info_.commit_ts_and_status_;
        clone->cc_lock_and_extra_ = this->cc_lock_and_extra_;
        if (RangePartitioned)
        {
            clone->entry_info_.SetDataStoreSize(
                this->entry_info_.DataStoreSize());
        }
        if (VersionedRecord)
        {
            if (payload_.cur_payload_ != nullptr)
            {
                clone->payload_.SetCurrentPayload(payload_.cur_payload_.get());
            }
            else
            {
                assert(this->PayloadStatus() == RecordStatus::Deleted);
                clone->payload_.cur_payload_ = nullptr;
            }

            clone->SetCkptTs(this->CkptTs());
            clone->payload_.SetArchives(payload_.ReleaseArchives());
        }
        else
        {
            TxRecord *rec =
                static_cast<TxRecord *>(payload_.cur_payload_.get());
            TxRecord::Uptr rec_clone = rec->Clone();
            clone->payload_.PassInCurrentPayload(std::move(rec_clone));
        }
    }

    inline static size_t basic_mem_overhead_ =
        sizeof(CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>);

    /**
     * @brief Move(not copy) the current version (payload, payload_status,
     * commit_ts) to the archives_.
     */
    void ArchiveBeforeUpdate()
    {
        assert(VersionedRecord);
        const RecordStatus rec_status = this->PayloadStatus();
        if (rec_status == RecordStatus::Unknown)
        {
            return;
        }

        if (payload_.GetArchives() == nullptr)
        {
            payload_.SetArchives(
                std::make_unique<std::list<VersionRecord<ValueT>>>());
        }

        const uint64_t commit_ts = this->CommitTs();
        if (payload_.GetArchives()->size() > 0)
        {
            assert(commit_ts > payload_.GetArchives()->front().commit_ts_);
        }

        //  Handle payloads of pk and sk the same way.
        payload_.GetArchives()->emplace_front(
            std::move(payload_.cur_payload_), commit_ts, rec_status);
    }

    /**
     * @brief Add a batch of historical versions.
     *@param v_recs versions descending ordered by commit_ts
     */
    void AddArchiveRecords(std::vector<VersionTxRecord> &v_recs)
    {
        assert(VersionedRecord);
        if (v_recs.size() == 0)
        {
            return;
        }

        if (payload_.GetArchives() == nullptr)
        {
            payload_.SetArchives(
                std::make_unique<std::list<VersionRecord<ValueT>>>());
        }

        auto it = payload_.GetArchives()->begin();
        for (; it != payload_.GetArchives()->end(); it++)
        {
            if (it->commit_ts_ <= v_recs[0].commit_ts_)
            {
                break;
            }
        }
        for (auto &vrec : v_recs)
        {
            if (it == payload_.GetArchives()->end() ||
                it->commit_ts_ != vrec.commit_ts_)
            {
                it = payload_.GetArchives()->emplace(it);
                it->commit_ts_ = vrec.commit_ts_;
                it->payload_status_ = vrec.record_status_;
                it->payload_.reset(
                    static_cast<ValueT *>(vrec.record_.release()));
            }
            it++;
        }
    }

    void AddArchiveRecord(std::shared_ptr<ValueT> payload_ptr,
                          RecordStatus payload_status,
                          uint64_t commit_ts)
    {
        assert(VersionedRecord);
        // if (commit_ts == 1U && payload_status == RecordStatus::Deleted)
        // {
        //     return;
        // }
        if (payload_.GetArchives() == nullptr)
        {
            payload_.SetArchives(
                std::make_unique<std::list<VersionRecord<ValueT>>>());
        }

        auto it = payload_.GetArchives()->begin();
        for (; it != payload_.GetArchives()->end(); it++)
        {
            if (it->commit_ts_ <= commit_ts)
            {
                break;
            }
        }
        if (it == payload_.GetArchives()->end() || it->commit_ts_ < commit_ts)
        {
            it = payload_.GetArchives()->emplace(it);
            it->commit_ts_ = commit_ts;
            it->payload_status_ = payload_status;
            it->payload_ = payload_ptr;
        }
    }

    /**
     *
     * @brief Kick out historical versions that won't be used according to
     * 'oldest_active_tx_ts'.
     * eg:  achives[10,8,4,3,1], oldest_active_tx_ts= 5;
     * after kicking out, archives will be [10,8,4].
     */
    void KickOutArchiveRecords(uint64_t oldest_active_tx_ts)
    {
        assert(VersionedRecord);
        if (payload_.GetArchives() == nullptr)
        {
            return;
        }

        if (this->CommitTs() <= oldest_active_tx_ts)
        {
            payload_.SetArchives(nullptr);
            return;
        }

        if (payload_.GetArchives()->size() <= 1)
        {
            return;
        }

        auto it = payload_.GetArchives()->begin();
        for (; it != payload_.GetArchives()->end(); it++)
        {
            if (it->commit_ts_ <= oldest_active_tx_ts)
            {
                break;
            }
        }
        if (it == payload_.GetArchives()->end())
        {
            return;
        }
        it++;
        payload_.GetArchives()->erase(it, payload_.GetArchives()->end());
    }

    size_t GetArchiveMemUsage() const
    {
        assert(VersionedRecord);
        if (payload_.GetArchives() == nullptr)
        {
            return 0;
        }
        size_t mem_usage = sizeof(*payload_.GetArchives());
        for (auto it = payload_.GetArchives()->begin();
             it != payload_.GetArchives()->end();
             ++it)
        {
            mem_usage += it->MemUsage();
        }
        return mem_usage;
    }

    /**
     * @brief Gets the visible version according to read timestamp.
     *
     * @param ts - snapshot read timestamp
     * @param rec - variable to store result
     */
    void MvccGet(uint64_t ts, VersionResultRecord<ValueT> &rec)
    {
        assert(VersionedRecord);
        const uint64_t commit_ts = this->CommitTs();
        const RecordStatus rec_status = this->PayloadStatus();

        if (rec_status == RecordStatus::Unknown)
        {
            rec.payload_status_ = RecordStatus::Unknown;
            rec.commit_ts_ = commit_ts;
            return;
        }
        if (commit_ts <= ts)
        {
            if (rec_status == RecordStatus::Normal)
            {
                rec.payload_ptr_ = payload_.VersionedCurrentPayload();
            }
            rec.commit_ts_ = commit_ts;
            rec.payload_status_ = rec_status;
            return;
        }

        if (payload_.GetArchives() != nullptr)
        {
            // if commit_ts_ > ts, find from archives_
            for (auto it = payload_.GetArchives()->cbegin();
                 it != payload_.GetArchives()->cend();
                 it++)
            {
                if (it->commit_ts_ <= ts)
                {
                    if (it->payload_status_ == RecordStatus::Normal)
                    {
                        rec.payload_ptr_ = it->payload_;
                    }
                    rec.commit_ts_ = it->commit_ts_;
                    rec.payload_status_ = it->payload_status_;
                    return;
                }
            }
        }

        rec.commit_ts_ = 1U;
        const uint64_t ckpt_ts = this->CkptTs();
        if (ckpt_ts == 0U)
        {
            // need fetch base table and archive table.
            rec.payload_status_ = RecordStatus::VersionUnknown;
        }
        else if (ckpt_ts <= ts)
        {
            // only need fetch base table.
            rec.payload_status_ = RecordStatus::BaseVersionMiss;
        }
        else
        {
            // only need fetch archive table.
            rec.payload_status_ = RecordStatus::ArchiveVersionMiss;
        }
    }

    /**
     * @brief For a read timestamp, is there a visible version in memory?
     */
    bool HasVisibleVersion(uint64_t read_ts) const
    {
        assert(VersionedRecord);
        if (this->CommitTs() <= read_ts)
        {
            return true;
        }
        if (payload_.GetArchives() != nullptr &&
            !payload_.GetArchives()->empty() &&
            payload_.GetArchives()->back().commit_ts_ <= read_ts)
        {
            return true;
        }
        return false;
    }

    size_t ArchiveRecordsCount() const
    {
        assert(VersionedRecord);
        if (payload_.GetArchives() == nullptr)
        {
            return 0;
        }
        return payload_.GetArchives()->size();
    }

    void ClearArchives()
    {
        if (payload_.GetArchives() != nullptr)
        {
            payload_.SetArchives(nullptr);
        }
    }

    /**
     * @brief Export version records to flush into KvStore when mvcc is enabled.
     * eg. CcEntry's versions is [10,8,7,5,4], param ckpt_ts is "9",
     * then the version "8" is exported to ckpt_vec, versions [7,5,4] are
     * exported to akv_vec.
     * @param key - the key of this entry
     * @param ckpt_vec - store the version records to flush into "base table".
     * @param akv_vec - store the version records to flush into "archives
     * table".
     * @param to_ts - Current round checkpoint timestamp.
     * @param export_persisted_item_to_ckpt_vec - True means If no larger
     * version exists, we need to export the data which commit_ts same as
     * ckpt_ts to ckpt_vec. Note: This flag only used for RangePartition.
     * @param export_base_table_item_only - True means only need to export the
     * base data. This is used for scan during add index txm.
     * @param export_persisted_key_only - True means if no larger version
     * exists, need to export the key which commit_ts same as ckpt_ts to
     * ckpt_vec. This is happen when the slice need to split, and we need the
     * key to calculate the subslice key. Note: This flag only used for
     * RangePartition.
     * @return the number of exported version records.
     */

    size_t ExportForCkpt(const KeyT &key,
                         std::vector<FlushRecord> &ckpt_vec,
                         std::vector<FlushRecord> &akv_vec,
                         std::vector<size_t> &mv_base_vec,
                         uint64_t to_ts,
                         uint64_t oldest_active_tx_ts,
                         bool mvcc_enabled,
                         size_t &ckpt_vec_size,
                         bool export_persisted_item_to_ckpt_vec,
                         bool export_base_table_item_only,
                         bool export_persisted_key_only,
                         uint64_t &flush_size)
    {
        // `export_store_record_if_need` - True means If no larger version needs
        // to be flushed(commit_ts > ckpt_ts && commit_ts <= to_ts), we need to
        // export the data which commit_ts same as ckpt_ts. Because we
        // want to flush this key to new range on base table. Only for range
        // partition
        size_t exported_count = 0;

        const uint64_t commit_ts = this->CommitTs();
        const RecordStatus rec_status = this->PayloadStatus();

        if (VersionedRecord && this->IsPersistent())
        {
            // 1.This version data has already flushed to base
            // table(commit_ts == ckpt_ts).
            // 2. No new version data to be
            // flushed(newest commit ts == ckpt_ts).
            // But we need to migrate data to new range on
            // base table. So we need to export this record
            // for range migration
            assert(RangePartitioned);
            if ((export_persisted_item_to_ckpt_vec) && commit_ts != 1 &&
                commit_ts <= to_ts &&
                (rec_status == RecordStatus::Normal ||
                 rec_status == RecordStatus::Deleted))
            {
                assert(commit_ts != 1);
                assert(exported_count == 0);
                FlushRecord &ref = ckpt_vec[ckpt_vec_size++];
                ref.CloneOrCopyKey(TxKey(&key));

                // This record was load from storage. We can't safely
                // point to CcEntry of CcMap. Because the entry will be
                // kickout after UnpinSlice. the pointer will become
                // invalid.
                ref.cce_ = nullptr;

                ref.payload_status_ = rec_status;
                ref.commit_ts_ = commit_ts;

                if (rec_status == RecordStatus::Normal)
                {
                    ref.SetVersionedPayload(payload_.VersionedCurrentPayload());
                }
                else
                {
                    ref.SetVersionedPayload(nullptr);
                }

                ref.post_flush_size_ = (rec_status == RecordStatus::Normal)
                                           ? (key.Size() + ref.PayloadSize())
                                           : 0;
                flush_size += ref.FlushSize();

                exported_count++;
            }
            else if (export_persisted_key_only && commit_ts != 1 &&
                     commit_ts <= to_ts &&
                     (rec_status == RecordStatus::Normal ||
                      rec_status == RecordStatus::Deleted))
            {
                // Just need the key of this version item which is used to
                // calculate the subslice keys
                assert(!export_persisted_item_to_ckpt_vec);

                FlushRecord &ref = ckpt_vec[ckpt_vec_size++];
                ref.CloneOrCopyKey(TxKey(&key));

                // Use nullptr to indicate that this item does not need be
                // flush.
                ref.cce_ = nullptr;
                ref.payload_status_ = rec_status;
                ref.commit_ts_ = commit_ts;

                ref.post_flush_size_ =
                    (rec_status == RecordStatus::Normal)
                        ? (key.Size() + payload_.cur_payload_->Size())
                        : 0;
                flush_size += ref.FlushSize();

                ++exported_count;
            }
            return exported_count;
        }

        size_t ckpt_idx = ckpt_vec_size;
        assert(!VersionedRecord || commit_ts > this->CkptTs());

        if (!VersionedRecord || commit_ts <= to_ts)
        {
            FlushRecord &ref = ckpt_vec[ckpt_vec_size++];
            ref.CloneOrCopyKey(TxKey(&key));
            ref.cce_ =
                const_cast<LruEntry *>(static_cast<const LruEntry *>(this));
            this->SetBeingCkpt();

            if (rec_status == RecordStatus::Normal)
            {
                if (VersionedRecord)
                {
                    ref.SetVersionedPayload(payload_.VersionedCurrentPayload());
                }
                else
                {
                    ref.SetNonVersionedPayload(payload_.cur_payload_.get());
                }
            }
            else
            {
                if (VersionedRecord)
                {
                    ref.SetVersionedPayload(nullptr);
                }
                else
                {
                    ref.SetNonVersionedPayload(nullptr);
                }
            }

            ref.payload_status_ = rec_status;
            ref.commit_ts_ = commit_ts;

            if (RangePartitioned)
            {
                assert(this->entry_info_.DataStoreSize() != INT32_MAX);
                ref.post_flush_size_ =
                    (ref.payload_status_ != RecordStatus::Deleted)
                        ? key.Size() + ref.PayloadSize()
                        : 0;
            }
            flush_size += ref.FlushSize();
            exported_count++;
        }

        if (!mvcc_enabled || export_base_table_item_only)
        {
            return exported_count;
        }

        if (VersionedRecord)
        {
            const uint64_t ckpt_ts = this->CkptTs();

            if (payload_.GetArchives() != nullptr &&
                payload_.GetArchives()->size() > 0)
            {
                // Scan data from largest version to smallest version
                for (auto it = payload_.GetArchives()->begin();
                     it != payload_.GetArchives()->end();
                     it++)
                {
                    if (it->commit_ts_ <= to_ts)
                    {
                        if (it->commit_ts_ < ckpt_ts || it->commit_ts_ == 1U)
                        {
                            // This version has already flushed to archive
                            // table.(it->commit_ts_ < ckpt_ts_)
                            break;
                        }
                        else if (it->commit_ts_ == ckpt_ts)
                        {
                            // `exported_count != 0` - means the larger version
                            // record will be flushed to base table.
                            if (exported_count != 0)
                            {
                                // This record on base table will be overrided.
                                // We need to flush this record to archive
                                // table.
                                auto &ref = akv_vec.emplace_back();
                                ref.SetKeyIndex(ckpt_idx);
                                ref.cce_ = const_cast<LruEntry *>(
                                    static_cast<const LruEntry *>(this));
                                this->SetBeingCkpt();

                                if (it->payload_status_ == RecordStatus::Normal)
                                {
                                    ref.SetVersionedPayload(it->payload_);
                                }
                                else
                                {
                                    ref.SetVersionedPayload(nullptr);
                                }
                                ref.payload_status_ = it->payload_status_;
                                ref.commit_ts_ = it->commit_ts_;
                                flush_size += ref.FlushSize();
                                exported_count++;
                            }
                            else
                            {
                                assert(exported_count == 0);

                                // 1.This version has already flushed to base
                                // table(commit_ts == ckpt_ts).
                                // 2. No larger version data to be
                                // flushed(exported_count == 0).
                                // We need to export this record in order to
                                // flush it to new range.
                                if (export_persisted_item_to_ckpt_vec)
                                {
                                    FlushRecord &ref =
                                        ckpt_vec[ckpt_vec_size++];
                                    ref.CloneOrCopyKey(TxKey(&key));

                                    // This record was load from storage. We
                                    // can't safely point to CcEntry of CcMap.
                                    // Because the entry will be kickout after
                                    // UnpinSlice. the pointer will become
                                    // invalidation.
                                    ref.cce_ = nullptr;

                                    if (it->payload_status_ ==
                                        RecordStatus::Normal)
                                    {
                                        ref.SetVersionedPayload(it->payload_);
                                    }
                                    else
                                    {
                                        ref.SetVersionedPayload(nullptr);
                                    }
                                    ref.payload_status_ = it->payload_status_;
                                    ref.commit_ts_ = it->commit_ts_;

                                    ref.post_flush_size_ =
                                        (it->payload_status_ ==
                                         RecordStatus::Normal)
                                            ? (key.Size() + ref.PayloadSize())
                                            : 0;

                                    flush_size += ref.FlushSize();
                                    exported_count++;
                                }
                                else if (RangePartitioned &&
                                         export_persisted_key_only)
                                {
                                    // Do not need to flush this version item to
                                    // base table(because it has already in the
                                    // base table), just need the key to
                                    // calculate the subsice keys.
                                    assert(!export_persisted_item_to_ckpt_vec);
                                    FlushRecord &ref =
                                        ckpt_vec[ckpt_vec_size++];
                                    ref.CloneOrCopyKey(TxKey(&key));

                                    // Use nullptr to indicate that this item
                                    // does not need be flush.
                                    ref.cce_ = nullptr;

                                    ref.payload_status_ = it->payload_status_;
                                    ref.commit_ts_ = it->commit_ts_;

                                    size_t payload_size = 0;
                                    if (it->payload_status_ ==
                                        RecordStatus::Normal)
                                    {
                                        payload_size = it->payload_->Size();
                                    }
                                    ref.post_flush_size_ =
                                        (it->payload_status_ ==
                                         RecordStatus::Normal)
                                            ? (key.Size() + payload_size)
                                            : 0;

                                    flush_size += ref.FlushSize();
                                    exported_count++;
                                }
                            }

                            break;
                        }
                        else
                        {
                            assert(it->commit_ts_ > ckpt_ts);

                            if (exported_count == 0)
                            {
                                FlushRecord &ref = ckpt_vec[ckpt_vec_size++];
                                ref.CloneOrCopyKey(TxKey(&key));
                                ref.cce_ = const_cast<LruEntry *>(
                                    static_cast<const LruEntry *>(this));
                                this->SetBeingCkpt();

                                if (it->payload_status_ == RecordStatus::Normal)
                                {
                                    ref.SetVersionedPayload(it->payload_);
                                }
                                else
                                {
                                    ref.SetVersionedPayload(nullptr);
                                }
                                ref.payload_status_ = it->payload_status_;
                                ref.commit_ts_ = it->commit_ts_;

                                if (RangePartitioned)
                                {
                                    assert(this->entry_info_.DataStoreSize() !=
                                           INT32_MAX);
                                    ref.post_flush_size_ =
                                        (ref.payload_status_ ==
                                         RecordStatus::Normal)
                                            ? (key.Size() + ref.PayloadSize())
                                            : 0;
                                }
                                flush_size += ref.FlushSize();
                            }
                            else
                            {
                                auto &ref = akv_vec.emplace_back();
                                ref.SetKeyIndex(ckpt_idx);
                                ref.cce_ = const_cast<LruEntry *>(
                                    static_cast<const LruEntry *>(this));
                                this->SetBeingCkpt();

                                if (it->payload_status_ == RecordStatus::Normal)
                                {
                                    ref.SetVersionedPayload(it->payload_);
                                }
                                else
                                {
                                    ref.SetVersionedPayload(nullptr);
                                }
                                ref.payload_status_ = it->payload_status_;
                                ref.commit_ts_ = it->commit_ts_;
                                flush_size += ref.FlushSize();
                            }

                            exported_count++;
                        }
                    }
                    // else: it->commit_ts_ > to_ts
                }
            }

            if (exported_count > 0 && ckpt_ts == 0 &&
                !HasVisibleVersion(oldest_active_tx_ts))
            {
                // last ckpt version is needed but not in memory, and we're not
                // sure if an older version exists, need to copy record from
                // "base table" into "mvcc_archives table".
                mv_base_vec.push_back(ckpt_idx);
            }
        }

        return exported_count;
    }

    /**
     * Commit replay txn commands in version order and update cur_ver.
     *
     * @param now_ts The current timestamp. Used to check if the commands are
     *               expired.
     * @param cur_ver
     */
    void TryCommitBufferedCommands(uint64_t &cur_ver, uint64_t now_ts)
    {
        BufferedTxnCmdList &buffered_cmd_list = this->BufferedCommandList();
        std::deque<TxnCmd> &txn_cmd_list = buffered_cmd_list.txn_cmd_list_;
        // iterate the list and apply the commands in version order
        auto it = txn_cmd_list.begin();
        auto erase_it = it;
        while (it != txn_cmd_list.end())
        {
            if (it->ignore_previous_version_ || it->valid_scope_ < now_ts)
            {
                if (it->ignore_previous_version_)
                {
                    // If a TxnCmd ignores previous version, the TxnCmds before
                    // it must have been discarded in EmplaceTxnCmd.
                    assert(it == txn_cmd_list.begin());

                    assert(it->obj_version_ >= cur_ver ||
                           it->obj_version_ == 1);
                }
                else
                {
                    // the commands in this txn are already expired, discard all
                    // commands before this txn.
                    it = txn_cmd_list.erase(txn_cmd_list.begin(), it);
                }

                cur_ver = it->obj_version_;
            }

            // check version match
            if (it->obj_version_ != cur_ver)
            {
                // commands must be applied in order, so if the version doesn't
                // match, we need to skip this txn for now.
                // Continue to the rest of the txns to see if we can find a out
                // of scope txn so that we can discard all commands before it.
                ++it;
                continue;
            }

            if (it->valid_scope_ < now_ts)
            {
                // If this key has gone out of its valid scope, we
                // can treat this txn command as a delete command.
                payload_.SetCurrentPayload(nullptr);
            }
            else if (!it->cmd_list_.empty())
            {
                auto &first_cmd = it->cmd_list_.front();

                // If a command was applied on deleted record, we set
                // `has_overwrite` flag to true in the log. If the first command
                // doesn't have an overwrite property, we need to create an
                // empty object.
                if (it->ignore_previous_version_ && !first_cmd->IsOverwrite())
                {
                    payload_.SetCurrentPayload(nullptr);
                }
                // apply the commands of this txn
                for (auto &cmd : it->cmd_list_)
                {
                    if (payload_.cur_payload_ == nullptr)
                    {
                        std::unique_ptr<TxRecord> obj_ptr =
                            cmd->CreateObject(nullptr);
                        payload_.PassInCurrentPayload(std::move(obj_ptr));
                    }
                    TxObject *obj_ptr = reinterpret_cast<TxObject *>(
                        payload_.cur_payload_.get());
                    TxObject *new_obj_ptr =
                        reinterpret_cast<TxObject *>(cmd->CommitOn(obj_ptr));
                    if (new_obj_ptr != obj_ptr)
                    {
                        // FIXME(lzx): should we use "new_obj_ptr->Clone()" ?
                        payload_.PassInCurrentPayload(
                            std::unique_ptr<TxRecord>(new_obj_ptr));
                    }
                }
            }

            cur_ver = it->new_version_;
            ++it;
            erase_it = it;
        }
        txn_cmd_list.erase(txn_cmd_list.begin(), erase_it);

        if (txn_cmd_list.empty())
        {
            DLOG(INFO) << "destruct buffered_cmd_list_ on object";
            buffered_cmd_list.Clear();
        }
        else
        {
            DLOG(INFO) << "replay not finished, current ver: " << cur_ver
                       << ", msg expect ver: "
                       << txn_cmd_list.front().obj_version_
                       << ", msg commit ts: "
                       << txn_cmd_list.front().new_version_;
        }
    }
    /**
     * The replayed commands must apply in order. Replayed commands are first
     * stored in buffered_cmd_list_ and committed in order.
     */
    void EmplaceAndCommitBufferedTxnCommand(TxnCmd &txn_cmd, uint64_t now_ts)
    {
        uint64_t cur_ver = this->CommitTs();
        RecordStatus status = this->PayloadStatus();
        auto &buffered_cmd_list = this->BufferedCommandList();
        bool waiting_for_fetch = status == RecordStatus::Unknown;
        bool try_commit = txn_cmd.ignore_previous_version_ ||
                          !waiting_for_fetch || txn_cmd.valid_scope_ < now_ts;

        buffered_cmd_list.EmplaceTxnCmd(txn_cmd, now_ts);

        if (try_commit)
        {
            TryCommitBufferedCommands(cur_ver, now_ts);
            status = payload_.cur_payload_ == nullptr ? RecordStatus::Deleted
                                                      : RecordStatus::Normal;
        }
        this->SetCommitTsPayloadStatus(cur_ver, status);
    }

    void UpdateCcEntry(
        SliceDataItem &data_item,
        bool enable_mvcc,
        int32_t &normal_rec_change,
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *ccp,
        CcShard *shard,
        uint64_t now_ts)
    {
        if (RangePartitioned && this->entry_info_.DataStoreSize() == INT32_MAX)
        {
            this->entry_info_.SetDataStoreSize(
                data_item.is_deleted_
                    ? 0
                    : data_item.key_.Size() + data_item.record_->Size());
        }

        if (VersionedRecord)
        {
            const ValueT *record =
                static_cast<const ValueT *>(data_item.record_.get());
            // If the in-memory version is from a upload request (i.e.
            // generated sk record from pk), the data store version
            // might be newer. Only overwrite if in memory version is
            // newer.
            const uint64_t cce_version = this->CommitTs();
            if (cce_version < data_item.version_ts_)
            {
                if (data_item.is_deleted_)
                {
                    payload_.SetCurrentPayload(nullptr);
                }
                else
                {
                    payload_.SetCurrentPayload(record);
                }
                RecordStatus status = data_item.is_deleted_
                                          ? RecordStatus::Deleted
                                          : RecordStatus::Normal;
                this->SetCommitTsPayloadStatus(data_item.version_ts_, status);
            }
            if (cce_version > 1 && data_item.version_ts_ < cce_version &&
                enable_mvcc)
            {
                AddArchiveRecord(std::make_shared<ValueT>(*record),
                                 data_item.is_deleted_ ? RecordStatus::Deleted
                                                       : RecordStatus::Normal,
                                 data_item.version_ts_);

                // The cc entry's commit ts is 1 when it is initialized.
                // Commit ts greater than 1 means that the key is
                // already cached in memory.
                return;
            }
        }
        else
        {
            // If the in-memory version is from a upload request (i.e.
            // generated sk record from pk), the data store version
            // might be newer. Only overwrite if in memory version is
            // newer.
            const uint64_t cce_version = this->CommitTs();
            if (cce_version < data_item.version_ts_)
            {
                bool obj_already_exist =
                    this->PayloadStatus() == RecordStatus::Normal;
                if (data_item.is_deleted_)
                {
                    payload_.SetCurrentPayload(nullptr);
                    if (obj_already_exist)
                    {
                        normal_rec_change--;
                    }
                    ccp->smallest_ttl_ = 0;
                }
                else
                {
                    payload_.PassInCurrentPayload(std::move(data_item.record_));
                    if (!obj_already_exist)
                    {
                        normal_rec_change++;
                    }
                    if (ccp->smallest_ttl_ != 0 &&
                        payload_.cur_payload_->HasTTL())
                    {
                        uint64_t ttl = payload_.cur_payload_->GetTTL();
                        ccp->smallest_ttl_ = std::min(ccp->smallest_ttl_, ttl);
                    }
                }
                RecordStatus status = data_item.is_deleted_
                                          ? RecordStatus::Deleted
                                          : RecordStatus::Normal;
                this->SetCommitTsPayloadStatus(data_item.version_ts_, status);

                if (this->HasBufferedCommandList())
                {
                    BufferedTxnCmdList &buffered_cmd_list =
                        this->BufferedCommandList();
                    auto &cmd_list = buffered_cmd_list.txn_cmd_list_;
                    int64_t buffered_cmd_cnt_old = buffered_cmd_list.Size();

                    uint64_t new_commit_ts = this->CommitTs();
                    assert(new_commit_ts == data_item.version_ts_);

                    // Clear cmds with smaller commit_ts than uploaded version.
                    auto it = cmd_list.begin();
                    while (it != cmd_list.end() &&
                           it->new_version_ <= new_commit_ts)
                    {
                        ++it;
                    }
                    cmd_list.erase(cmd_list.begin(), it);

                    DLOG(INFO) << "Try commit buffered command on "
                                  "UpdateCcEntry(...)";
                    TryCommitBufferedCommands(new_commit_ts, now_ts);
                    int64_t buffered_cmd_cnt_new = buffered_cmd_list.Size();
                    this->UpdateBufferedCommandCnt(
                        shard, buffered_cmd_cnt_new - buffered_cmd_cnt_old);

                    if (payload_.cur_payload_)
                    {
                        this->SetCommitTsPayloadStatus(new_commit_ts,
                                                       RecordStatus::Normal);
                    }
                    else
                    {
                        this->SetCommitTsPayloadStatus(new_commit_ts,
                                                       RecordStatus::Deleted);
                    }

                    if (buffered_cmd_list.Empty())
                    {
                        // Recycles the lock if all the replay commands have
                        // been applied.
                        this->RecycleKeyLock(*shard);
                    }
                }
            }
        }
        this->SetCkptTs(data_item.version_ts_);
    }
};

struct LruPage
{
    explicit LruPage(CcMap *parent) : parent_map_(parent)
    {
    }

    // Lru link which records the age of page, when ccshard is full, kickout
    // the entries by the order of lru.
    LruPage *lru_prev_{nullptr};
    LruPage *lru_next_{nullptr};

    CcMap *parent_map_{nullptr};

    uint64_t last_access_ts_{0};

    // The largest commit ts of dirty cc entries on this page. This value might
    // be larger than the actual max commit ts of cc entries. Currently used to
    // decide if this page has dirty data after a given ts.
    uint64_t last_dirty_commit_ts_{0};

    // The smallest ttl on this page. TTL for deleted key is 0, for normal keys
    // without TTL set is UINT64_MAX. This value is used to decide if this page
    // needs to be scanned when purging deleted entries in memory.
    uint64_t smallest_ttl_{UINT64_MAX};
};

template <typename KeyT,
          typename ValueT,
          bool VersionedRecord,
          bool RangePartitioned>
struct CcPage : public LruPage
{
    /**
     * Construct page negative infinity and positive infinity.
     * @param parent
     */
    explicit CcPage(CcMap *parent) : LruPage(parent)
    {
    }

    /**
     * Construct a normal page.
     * @param parent
     * @param prev_page
     * @param next_page
     */
    CcPage(CcMap *parent,
           CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *prev_page,
           CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *next_page)
        : LruPage(parent), prev_page_(prev_page), next_page_(next_page)
    {
        keys_.reserve(split_threshold_);
        entries_.reserve(split_threshold_);
        if (prev_page_ != nullptr)
        {
            prev_page_->next_page_ = this;
        }
        if (next_page_ != nullptr)
        {
            next_page_->prev_page_ = this;
        }
    }

    /**
     * Construct a page when splitting an existing page.
     * @param parent
     * @param keys
     * @param entries
     * @param prev_page
     * @param next_page
     */
    CcPage(CcMap *parent,
           std::vector<KeyT> &&keys,
           std::vector<std::unique_ptr<
               CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>>
               &&entries,
           CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *prev_page,
           CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *next_page)
        : LruPage(parent),
          keys_(std::move(keys)),
          entries_(std::move(entries)),
          prev_page_(prev_page),
          next_page_(next_page)
    {
        if (prev_page_ != nullptr)
        {
            prev_page_->next_page_ = this;
        }
        if (next_page_ != nullptr)
        {
            next_page_->prev_page_ = this;
        }
    }

    ~CcPage()
    {
        if (prev_page_ != nullptr)
        {
            prev_page_->next_page_ = next_page_;
        }
        if (next_page_ != nullptr)
        {
            next_page_->prev_page_ = prev_page_;
        }
    }

    CcPage(const CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>
               &page) = delete;
    CcPage operator=(
        const CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> &page) =
        delete;
    CcPage(CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> &&page) =
        delete;
    CcPage operator=(CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>
                         &&page) = delete;

    /**
     * Memory usage of this CcPage, not including the keys and entries
     * @return
     */
    size_t MemUsage() const
    {
        return basic_mem_overhead_;
    }

    /**
     * Memory usage of this CcPage, including the keys and entries
     * @return
     */
    size_t TotalMemUsage() const
    {
        size_t mem_usage = MemUsage();
        auto key_it = keys_.begin();
        auto entry_ptr_it = entries_.begin();
        for (; key_it != keys_.end(); key_it++, entry_ptr_it++)
        {
            mem_usage += key_it->MemUsage() - sizeof(KeyT) +
                         (*entry_ptr_it)->GetCcEntryMemUsage();
        }
        return mem_usage;
    }

    /**
     *
     * @param key
     * @return
     */
    size_t Find(const KeyT &key) const
    {
        if (keys_.empty() || key < keys_.front() || keys_.back() < key)
        {
            // not found
            return keys_.size();
        }

        auto lb_it = std::lower_bound(keys_.begin(), keys_.end(), key);
        size_t idx_in_page = lb_it - keys_.begin();

        // check it equals key
        if (lb_it != keys_.end() && *lb_it == key)
        {
            return idx_in_page;
        }
        else
        {
            // not found
            return keys_.size();
        }
    }

    // Use two-way merge algrothim to bulk emplace keys to reduce data moving
    // overhead. Note: new_keys are not exist in keys_
    void EmplaceKeys(std::vector<KeyT> &new_keys,
                     std::vector<size_t> &idxs_in_page)
    {
        if (new_keys.empty())
        {
            idxs_in_page.clear();
            return;
        }

        idxs_in_page.clear();
        idxs_in_page.resize(new_keys.size(), 0);
        assert(!idxs_in_page.empty());

        if (new_keys.size() == 1)
        {
            size_t idx_in_page = Emplace(std::move(new_keys.front()));
            idxs_in_page[0] = idx_in_page;
            return;
        }

        if (keys_.empty() || keys_.back() < new_keys.front())
        {
            for (size_t i = 0; i < new_keys.size(); ++i)
            {
                idxs_in_page[i] = keys_.size();

                keys_.push_back(std::move(new_keys[i]));
                entries_.push_back(
                    std::make_unique<CcEntry<KeyT,
                                             ValueT,
                                             VersionedRecord,
                                             RangePartitioned>>());
            }

            return;
        }

        size_t total_size = new_keys.size() + keys_.size();
        size_t res_index = total_size;
        size_t old_index = keys_.size();
        size_t new_index = new_keys.size();

        keys_.resize(total_size);
        entries_.resize(total_size);

        while (old_index > 0 && new_index > 0)
        {
            if (keys_[old_index - 1] < new_keys[new_index - 1])
            {
                keys_[res_index - 1] = std::move(new_keys[new_index - 1]);
                entries_[res_index - 1] = std::make_unique<
                    CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>();
                idxs_in_page[new_index - 1] = res_index - 1;
                new_index--;
            }
            else
            {
                assert(new_keys[new_index - 1] < keys_[old_index - 1]);
                keys_[res_index - 1] = std::move(keys_[old_index - 1]);
                entries_[res_index - 1] = std::move(entries_[old_index - 1]);
                old_index--;
            }

            res_index--;
        }

        for (; new_index > 0; --new_index, --res_index)
        {
            keys_[res_index - 1] = std::move(new_keys[new_index - 1]);
            entries_[res_index - 1] = std::make_unique<
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>();

            idxs_in_page[new_index - 1] = res_index - 1;
        }
    }

    size_t Emplace(const KeyT &key)
    {
        // append check
        auto insert_it =
            keys_.size() > 0 && keys_.back() < key
                ? keys_.end()
                : std::lower_bound(keys_.begin(), keys_.end(), key);
        assert(insert_it == keys_.end() || *insert_it != key);

        size_t insert_pos = insert_it - keys_.begin();
        keys_.emplace(insert_it, key);
        entries_.emplace(
            entries_.begin() + insert_pos,
            std::make_unique<
                CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>());
        return insert_pos;
    }

    /**
     * Find lower bound of the key. Return lower bound the the key, or return
     * keys_.size() if all keys is less than key.
     * @param key
     * @return index for the lower bound of the key.
     */
    size_t LowerBound(const KeyT &key) const
    {
        size_t target_idx =
            std::lower_bound(keys_.begin(), keys_.end(), key) - keys_.begin();
        return target_idx;
    }

    void Split(std::vector<KeyT> &new_page_keys,
               std::vector<std::unique_ptr<
                   CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>>
                   &new_page_entries,
               uint64_t &new_last_commit_ts,
               uint64_t &new_smallest_ttl)
    {
        new_page_keys.reserve(split_threshold_);
        new_page_entries.reserve(split_threshold_);
        new_last_commit_ts = 0;
        new_smallest_ttl = UINT64_MAX;
        size_t split_pos = keys_.size() / 2;
        new_page_keys.insert(new_page_keys.end(),
                             std::make_move_iterator(keys_.begin() + split_pos),
                             std::make_move_iterator(keys_.end()));
        for (size_t idx = split_pos; idx < entries_.size(); idx++)
        {
            new_last_commit_ts =
                std::max(new_last_commit_ts, entries_[idx]->CommitTs());
            if (new_smallest_ttl != 0)
            {
                if (entries_[idx]->PayloadStatus() == RecordStatus::Deleted)
                {
                    new_smallest_ttl = 0;
                }
                else if (entries_[idx]->PayloadStatus() ==
                             RecordStatus::Normal &&
                         entries_[idx]->payload_.cur_payload_ &&
                         entries_[idx]->payload_.cur_payload_->HasTTL())
                {
                    uint64_t ttl =
                        entries_[idx]->payload_.cur_payload_->GetTTL();
                    new_smallest_ttl = std::min(new_smallest_ttl, ttl);
                }
            }
            new_page_entries.push_back(std::move(entries_[idx]));
        }
        keys_.erase(keys_.begin() + split_pos, keys_.end());
        entries_.erase(entries_.begin() + split_pos, entries_.end());
    }

    // Only used by batch fill slice
    // This method only palces some keys in [start_index, end_index).
    // Note: If there are keys in [start_index, end_index) before, then these
    // keys will be overwritten.
    void PlaceKeys(
        std::vector<KeyT> &src_keys,
        std::vector<std::unique_ptr<
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>>
            &src_entries,
        std::deque<SliceDataItem> &slice_items,
        const std::vector<std::pair<size_t, bool>> &location_infos,
        size_t start_index,
        size_t end_index,
        size_t offset,
        bool enable_mvcc,
        int32_t &normal_rec_change,
        CcShard *shard,
        uint64_t now_ts)
    {
        assert(end_index <= Size());
        assert(offset + (end_index - start_index) <= location_infos.size());

        offset += (end_index - start_index - 1);
        for (size_t idx = end_index; idx > start_index; --idx, --offset)
        {
            auto &location_info = location_infos[offset];
            if (location_info.second)
            {
                // Emplace new key to target page
                auto new_cc_entry = std::make_unique<
                    CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>();
                new_cc_entry->UpdateCcEntry(slice_items[location_info.first],
                                            enable_mvcc,
                                            normal_rec_change,
                                            this,
                                            shard,
                                            now_ts);

                // emplace new key into page
                const KeyT *item_key =
                    slice_items[location_info.first].key_.GetKey<KeyT>();

                keys_[idx - 1] = KeyT(*item_key);
                entries_[idx - 1] = std::move(new_cc_entry);
            }
            else
            {
                keys_[idx - 1] = std::move(src_keys[location_info.first]);
                entries_[idx - 1] = std::move(src_entries[location_info.first]);
                entries_[idx - 1]->UpdateCcPage(this);
            }
        }
    }

    /**
     * Find upper bound of key, requiring key is in the range of this page's
     * [front, back).
     * @param key
     * @return
     */
    size_t UpperBound(const KeyT &key) const
    {
        assert(!keys_.empty() && keys_.front() <= key && key < keys_.back());
        size_t target_idx =
            std::upper_bound(keys_.begin(), keys_.end(), key) - keys_.begin();
        return target_idx;
    }

    /**
     * Find cce in entries_, return its index.
     * @param cce
     * @return
     */
    size_t FindEntry(
        const CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce)
        const
    {
        auto it = entries_.begin();
        while (it->get() != cce && it != entries_.end())
        {
            it++;
        }
        return it - entries_.begin();
    }

    bool Full() const
    {
        return keys_.size() == split_threshold_;
    }

    bool Empty() const
    {
        return keys_.size() == 0;
    }

    bool IsNegInf() const
    {
        return prev_page_ == nullptr;
    }

    bool IsPosInf() const
    {
        return next_page_ == nullptr;
    }

    const KeyT &FirstKey() const
    {
        if (IsNegInf())
        {
            return *KeyT::NegativeInfinity();
        }
        if (IsPosInf())
        {
            return *KeyT::PositiveInfinity();
        }
        assert(!keys_.empty());
        return keys_.front();
    }

    const KeyT &LastKey() const
    {
        if (IsNegInf())
        {
            return *KeyT::NegativeInfinity();
        }
        if (IsPosInf())
        {
            return *KeyT::PositiveInfinity();
        }
        assert(!keys_.empty());
        return keys_.back();
    }

    const KeyT *KeyOfEntry(
        const CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce)
        const
    {
        if (IsNegInf())
        {
            return KeyT::NegativeInfinity();
        }
        if (IsPosInf())
        {
            return KeyT::PositiveInfinity();
        }
        size_t idx_in_page = FindEntry(cce);
        assert(idx_in_page < keys_.size());
        return &keys_.at(idx_in_page);
    }

    const KeyT *Key(size_t idx_in_page) const
    {
        if (IsNegInf())
        {
            return KeyT::NegativeInfinity();
        }
        if (IsPosInf())
        {
            return KeyT::PositiveInfinity();
        }

        assert(idx_in_page < keys_.size());
        return &keys_.at(idx_in_page);
    }

    CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *Entry(
        size_t idx_in_page)
    {
        assert(idx_in_page < entries_.size());
        return entries_[idx_in_page].get();
    }

    void Remove(size_t idx)
    {
        assert(idx < keys_.size());

        auto key_it = keys_.begin() + idx;
        auto entry_ptr_it = entries_.begin() + idx;
        keys_.erase(key_it);
        entries_.erase(entry_ptr_it);
    }

    void Remove(const KeyT &key)
    {
        size_t idx = Find(key);
        return Remove(idx);
    }

    void Remove(
        const CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *entry)
    {
        size_t idx = FindEntry(entry);
        return Remove(idx);
    }

    size_t Size() const
    {
        return keys_.size();
    }

    void DebugPrint() const
    {
        LOG(INFO) << "page addr: " << this << ", keys_ size: " << keys_.size()
                  << ", entries_ size: " << entries_.size()
                  << ", prev_page_: " << prev_page_
                  << ", next_page_: " << next_page_
                  << ", IsNegInf: " << IsNegInf()
                  << ", IsPosInf: " << IsPosInf();
        //        for (const auto &up : entries_)
        //        {
        //            LOG(INFO) << "entry ptr: " << up.get();
        //        }
    }

    /**
     * @brief Software prefetch to speed up memory release.
     *
     * @note To make prefetch takes effect, NUMA machine should run with
     * `numactl` binding or disable `kernel.numa_balancing`.
     */
    enum PREFETCH_FLAG
    {
        PREFETCH_FLAG_CCENTRY = 1u,
        PREFETCH_FLAG_PAYLOAD = (1u << 1),
        PREFETCH_FLAG_BLOB = (1u << 2),
    };

    static void SoftwarePrefetch(
        PREFETCH_FLAG flag,
        typename std::vector<std::unique_ptr<
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>>::iterator
            begin,
        typename std::vector<std::unique_ptr<
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>>::iterator
            end)
    {
        if (flag & PREFETCH_FLAG_CCENTRY)
        {
            std::for_each(
                begin,
                end,
                [](const std::unique_ptr<
                    CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>
                       &cce)
                {
                    if (cce)
                    {
                        __builtin_prefetch(cce.get(), 1, 3);
                    }
                });
        }
        if (flag & PREFETCH_FLAG_PAYLOAD)
        {
            std::for_each(
                begin,
                end,
                [](const std::unique_ptr<
                    CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>
                       &cce)
                {
                    if (cce && cce->payload_.cur_payload_)
                    {
                        __builtin_prefetch(
                            cce->payload_.cur_payload_.get(), 1, 2);
                    }
                });
        }
        if (flag & PREFETCH_FLAG_BLOB)
        {
            std::for_each(
                begin,
                end,
                [](const std::unique_ptr<
                    CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>
                       &cce)
                {
                    if (cce && cce->payload_.cur_payload_)
                    {
                        cce->payload_.cur_payload_->Prefetch();
                    }
                });
        }
    }

    // threshold to trigger page split when inserting
    inline static constexpr size_t split_threshold_ = 64;
    // threshold to trigger page merge
    // needs thorough consideration to configure this as eager merging could
    // lead to thrashing, where a lot of successive delete and insert operations
    // lead to constant splits and merges
    inline static constexpr size_t merge_threshold_ = split_threshold_ / 2;

    std::vector<KeyT> keys_;
    std::vector<std::unique_ptr<
        CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>>
        entries_;

    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *prev_page_{
        nullptr};
    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *next_page_{
        nullptr};

    // CcPage is contained in std::_Rb_tree_node with node key and RBT node
    // pointers (32 bytes)
    inline static size_t basic_mem_overhead_ =
        sizeof(KeyT) +
        sizeof(CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>) + 32 +
        sizeof(KeyT) * split_threshold_ +
        sizeof(std::unique_ptr<
               CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>) *
            split_threshold_ +
        sizeof(uint64_t);
};

struct CcEntryAddr
{
public:
    CcEntryAddr() : cce_lock_ptr_(0), node_group_id_(0), core_id_(0), term_(-1)
    {
    }

    CcEntryAddr(const CcEntryAddr &rhs)
        : cce_lock_ptr_(rhs.cce_lock_ptr_),
          node_group_id_(rhs.node_group_id_),
          core_id_(rhs.core_id_),
          term_(rhs.term_.load(std::memory_order_acquire))
    {
    }

    bool operator==(const CcEntryAddr &rhs) const
    {
        return node_group_id_ == rhs.node_group_id_ && term_ == rhs.term_ &&
               cce_lock_ptr_ != 0 && rhs.cce_lock_ptr_ != 0 &&
               cce_lock_ptr_ == rhs.cce_lock_ptr_;
    }

    CcEntryAddr &operator=(const CcEntryAddr &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        cce_lock_ptr_ = rhs.cce_lock_ptr_;
        node_group_id_ = rhs.node_group_id_;
        term_.store(rhs.term_.load(std::memory_order_acquire),
                    std::memory_order_release);
        core_id_ = rhs.core_id_;

        return *this;
    }

    bool Empty() const
    {
        return cce_lock_ptr_ == 0;
    }

    uint64_t CceLockPtr() const
    {
        return cce_lock_ptr_;
    }

    /**
     * This func should be used with caution. Can only be called by the lock's
     * owner CcShard when executing CcRequest since the lock's memory is
     * directly accessed. You cannot call ExtractCce() from another thread or
     * another node which has a total different memory space, use CceLockPtr()
     * instead. The lock owner CcEntry might change in the future (when page
     * merge or rebalance happens).
     * @return
     */
    LruEntry *ExtractCce() const
    {
        if (cce_lock_ptr_ == 0)
        {
            return nullptr;
        }
        KeyGapLockAndExtraData *lock =
            reinterpret_cast<KeyGapLockAndExtraData *>(cce_lock_ptr_);
        return lock->GetCcEntry();
    }

    uint64_t InsertPtr() const
    {
        return 0;
    }

    uint32_t NodeGroupId() const
    {
        return node_group_id_;
    }

    int64_t Term() const
    {
        return term_.load(std::memory_order_acquire);
    }

    uint32_t CoreId() const
    {
        return core_id_;
    }

    void SetCceLock(uint64_t cce_lock_addr)
    {
        cce_lock_ptr_ = cce_lock_addr;
    }

    void SetCceLock(uint64_t cce_lock_addr, int64_t term, uint32_t core_id)
    {
        cce_lock_ptr_ = cce_lock_addr;
        term_.store(term, std::memory_order_release);
        core_id_ = core_id;
    }

    void SetCceLock(uint64_t cce_lock_addr,
                    int64_t term,
                    uint32_t ng,
                    uint32_t core_id)
    {
        cce_lock_ptr_ = cce_lock_addr;
        node_group_id_ = ng;
        term_.store(term, std::memory_order_release);
        core_id_ = core_id;
    }

    void SetNodeGroupId(uint32_t ng_id)
    {
        node_group_id_ = ng_id;
    }

    void SetTerm(int64_t term)
    {
        term_.store(term, std::memory_order_release);
    }

private:
    /**
     * The lock structure's memory address. It's safe to access the lock object
     * since the space will stay in memory long enough.
     */
    uint64_t cce_lock_ptr_{};
    uint32_t node_group_id_;
    uint32_t core_id_;
    // The term of the cc node group to which the cc entry belongs. The variable
    // needs to be std::atomic, because for locking-based protocols the remote
    // node will send an acknowledge message to notify the tx when the
    // read/write request is blocked. The acknowledgement message, when arrives,
    // will set the term and the cc entry's address. We use std::atomic to sync
    // between the remote cc handler thread and the tx thread, which
    // periodically checks the term to determine if there is a timeout. Note
    // that for non-blocking protocols, we rely on the cc handler result to sync
    // between the remote handler thread and the tx thread, and the term does
    // not need to be std::atomic.
    std::atomic<int64_t> term_;
};

struct DrainTuple
{
    DrainTuple(const CcEntryAddr &cce_addr,
               uint64_t version_ts,
               PostReadType post_read_type)
        : cce_addr_(cce_addr),
          version_ts_(version_ts),
          post_read_type_(post_read_type)
    {
    }

    CcEntryAddr cce_addr_;
    uint64_t version_ts_;
    PostReadType post_read_type_;
};

}  // namespace txservice
namespace std
{
template <>
struct hash<txservice::CcEntryAddr>
{
    std::size_t operator()(const txservice::CcEntryAddr &key) const
    {
        uint64_t ptr_hash = key.CceLockPtr();
        return (size_t) key.NodeGroupId() * 23 + ptr_hash;
    }
};
}  // namespace std
