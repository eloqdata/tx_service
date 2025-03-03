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

#include <butil/logging.h>

#include <algorithm>  // std::max
#include <atomic>
#include <cassert>
#include <cstdint>
#include <deque>
#include <list>
#include <memory>  // std::make_unique, make_shared, shared_ptr
#include <string>
#include <utility>  // std::move
#include <vector>

#include "cc_req_base.h"
#include "non_blocking_lock.h"
#include "slice_data_item.h"
#include "tx_id.h"
#include "tx_key.h"
#include "tx_record.h"

#ifdef ON_KEY_OBJECT
#include "tx_command.h"
#endif

namespace txservice
{
class CcMap;
class CcShardHeap;
class CcShard;
struct StandbyForwardEntry;

template <typename KeyT, typename ValueT>
class TemplateCcMap;

struct LruEntry;

template <typename KeyT, typename ValueT>
struct CcEntry;

template <typename KeyT, typename ValueT>
struct CcPage;

struct FlushRecord
{
private:
#ifndef ON_KEY_OBJECT
    std::shared_ptr<TxRecord> payload_{nullptr};
#else
    BlobTxRecord payload_{};
#endif

    enum class FlushKeyType : uint8_t
    {
        TxKey = 0,
        KeyIndex
    };

    union
    {
        TxKey tx_key_;
        size_t key_idx_;
    };
    FlushKeyType key_type_;

public:
    RecordStatus payload_status_{RecordStatus::Unknown};
#ifndef ON_KEY_OBJECT
    // Size of this version FlushRecord. 0 if the record is in Deleted status.
    // 1. Used to updated the cce::data_store_size_ after this version item has
    // been flushed to the data store.
    // 2. Used to calculate the key of the subslice when the slice need to be
    // split. At this time, the value is the size of the item in the base table.
    // In short, the version item has been persisted, and does not need to be
    // flushed, therefore, the payload of this FlushRecord is nullptr.
    uint32_t post_flush_size_{0};
#endif

    uint64_t commit_ts_{1U};
    // cce is nullptr means this cce is already persisted on kv and we
    // do not need to flush / update its ckpt ts.
    LruEntry *cce_{nullptr};

    int32_t partition_id_{-1};

    FlushRecord() : tx_key_(), key_type_(FlushKeyType::TxKey)
    {
    }

#ifndef ON_KEY_OBJECT
    // Only used by "data_sync_vec->emplace_back()" in
    // LocalCcShards::DataSync().
    FlushRecord(TxKey key,
                std::shared_ptr<TxRecord> payload,
                RecordStatus payload_status,
                uint64_t commit_ts,
                LruEntry *cce,
                int32_t post_flush_size,
                int32_t partition_id)
    {
        tx_key_.Release();
        tx_key_ = std::move(key);
        key_type_ = FlushKeyType::TxKey;
        payload_ = std::move(payload);
        payload_status_ = payload_status;
        commit_ts_ = commit_ts;
        cce_ = cce;
        post_flush_size_ = post_flush_size;
        partition_id_ = partition_id;
    }
#else
    FlushRecord(TxKey key,
                const BlobTxRecord &payload,
                RecordStatus payload_status,
                uint64_t commit_ts,
                LruEntry *cce,
                int32_t partition_id)
    {
        tx_key_.Release();
        tx_key_ = std::move(key);
        key_type_ = FlushKeyType::TxKey;
        // use copy constructor to avoid re-allocatting memory in DataSyncScanCc
        payload_ = payload;
        payload_status_ = payload_status;
        commit_ts_ = commit_ts;
        cce_ = cce;
        partition_id_ = partition_id;
    }
#endif

    ~FlushRecord()
    {
        if (key_type_ == FlushKeyType::TxKey)
        {
            tx_key_.~TxKey();
        }
    }

    FlushRecord &operator=(FlushRecord &&rhs) noexcept
    {
        if (this == &rhs)
        {
            return *this;
        }

        switch (rhs.key_type_)
        {
        case FlushKeyType::TxKey:
            SetKey(std::move(rhs.tx_key_));
            break;
        case FlushKeyType::KeyIndex:
            if (key_type_ == FlushKeyType::TxKey)
            {
                tx_key_.~TxKey();
            }
            key_idx_ = rhs.key_idx_;
            key_type_ = FlushKeyType::KeyIndex;
            break;
        default:
            break;
        }

        payload_ = std::move(rhs.payload_);
        payload_status_ = rhs.payload_status_;
        commit_ts_ = rhs.commit_ts_;
        cce_ = rhs.cce_;
#ifndef ON_KEY_OBJECT
        post_flush_size_ = rhs.post_flush_size_;
#endif
        partition_id_ = rhs.partition_id_;
        return *this;
    }

    FlushRecord(FlushRecord &&rhs) noexcept
    {
        switch (rhs.key_type_)
        {
        case FlushKeyType::TxKey:
            tx_key_.Release();
            tx_key_ = std::move(rhs.tx_key_);
            key_type_ = FlushKeyType::TxKey;
            break;
        case FlushKeyType::KeyIndex:
            key_idx_ = rhs.key_idx_;
            key_type_ = FlushKeyType::KeyIndex;
            break;
        default:
            break;
        }

        payload_ = std::move(rhs.payload_);
        payload_status_ = rhs.payload_status_;
        commit_ts_ = rhs.commit_ts_;
#ifndef ON_KEY_OBJECT
        post_flush_size_ = rhs.post_flush_size_;
#endif
        cce_ = rhs.cce_;
        partition_id_ = rhs.partition_id_;
    }

    FlushRecord(const FlushRecord &) = delete;

    void CloneOrCopyKey(const TxKey &key)
    {
        if (key_type_ == FlushKeyType::TxKey && tx_key_.IsOwner())
        {
            tx_key_.Copy(key);
        }
        else
        {
            tx_key_.Release();
            tx_key_ = key.Clone();
            key_type_ = FlushKeyType::TxKey;
        }
    }

    void SetKeyIndex(size_t offset)
    {
        if (key_type_ == FlushKeyType::TxKey)
        {
            tx_key_.~TxKey();
        }

        key_idx_ = offset;
        key_type_ = FlushKeyType::KeyIndex;
    }

    size_t GetKeyIndex() const
    {
        assert(key_type_ == FlushKeyType::KeyIndex);
        return key_idx_;
    }

    void SetKey(TxKey key)
    {
        if (key_type_ != FlushKeyType::TxKey)
        {
            // Ensures that if "this" union is not of type TxKey, the ownership
            // bit of tx_key_ is 0 and the following move assignment does not
            // trigger de-allocation accidentally.
            tx_key_.Release();
        }
        tx_key_ = std::move(key);
        key_type_ = FlushKeyType::TxKey;
    }

#ifndef ON_KEY_OBJECT
    void SetPayload(std::shared_ptr<TxRecord> sptr)
    {
        payload_ = sptr;
    }

    const TxRecord *Payload() const
    {
        if (payload_ == nullptr)
        {
            return nullptr;
        }
        return payload_.get();
    }

    std::shared_ptr<TxRecord> ReleasePayload()
    {
        return std::move(payload_);
    }

    size_t PayloadSize() const
    {
        return Payload() == nullptr ? 0 : Payload()->Size();
    }
#else
    void SetPayload(const TxRecord *ptr)
    {
        payload_.value_.clear();
        ptr->Serialize(payload_.value_);
        if (ptr->HasTTL())
        {
            payload_.SetTTL(ptr->GetTTL());
        }
    }

    const TxRecord *Payload() const
    {
        if (payload_.value_.empty())
        {
            return nullptr;
        }
        return &payload_;
    }

    const BlobTxRecord &GetPayload()
    {
        return payload_;
    }

    size_t PayloadSize() const
    {
        return Payload() == nullptr ? 0 : Payload()->Size();
    }
#endif

    TxKey Key() const;

    uint64_t MemUsage()
    {
        uint64_t mem_usage = 0;
        if (key_type_ == FlushKeyType::TxKey)
        {
            mem_usage += tx_key_.MemUsage();
        }
        else
        {
            mem_usage += sizeof(key_idx_);
        }

#ifdef ON_KEY_OBJECT
        mem_usage += payload_.MemUsage();
#else
        mem_usage += sizeof(std::shared_ptr<TxRecord>);
#endif

        return mem_usage;
    }
};

struct LruEntry
{
public:
    LruEntry();

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

    CcMap *GetCcMap() const;

    LruPage *GetCcPage() const;

    void UpdateCcPage(LruPage *page);

    void UpdateBufferedCommandCnt(CcShard *shard, int64_t delta);

    /**
     * @brief check whether the entry can be kicked out from ccmap, iff no key
     * lock, no gap lock and not 'dirty' entry (entry which has been
     * checkpointed since the last change).
     *
     * @return true: entry can be kicked out.
     */
    bool IsFree() const;

    uint64_t CommitTs() const;

    void SetBeingCkpt();

    void ClearBeingCkpt();

    bool GetBeingCkpt() const;

#ifndef ON_KEY_OBJECT
    uint64_t CkptTs() const
    {
        return ckpt_ts_;
    }
#endif

#ifdef ON_KEY_OBJECT
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

    void PopBlockRequest(CcShard *ccs, txservice::TxObject *object)
    {
        if (cc_lock_and_extra_ != nullptr)
        {
            cc_lock_and_extra_->PopBlockRequest(ccs, object);
        }
    }
    void PushBlockRequest(CcRequestBase *req)
    {
        if (cc_lock_and_extra_ != nullptr)
        {
            cc_lock_and_extra_->PushBlockRequest(req);
        }
    }
    void AbortBlockRequest(TxNumber txid, CcErrorCode err)
    {
        if (cc_lock_and_extra_ != nullptr)
        {
            cc_lock_and_extra_->AbortBlockRequest(txid, err);
        }
    }
#endif

    KeyGapLockAndExtraData *GetKeyGapLockAndExtraData()
    {
        return cc_lock_and_extra_;
    }

    /**
     * @brief Updates the checkpoint timestamp such that it is no smaller than
     * the input timestamp. The method is called by the tx processor thread,
     * after flushing the key-value pair to the data store, or the tx processor
     * thread which back fills the cache-miss record into memory.
     *
     * @param ts
     */
    void SetCkptTs(uint64_t ts);

    bool IsPersistent() const;

    RecordStatus PayloadStatus() const;

    void SetCommitTsPayloadStatus(uint64_t ts, RecordStatus status);

protected:
    KeyGapLockAndExtraData *cc_lock_and_extra_{nullptr};
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

private:
#ifndef ON_KEY_OBJECT
    // The commit timestamp of the latest checkpoint version record.
    uint64_t ckpt_ts_{0};

public:
    /**
     * @brief Size of this record in data store.
     * INT32_MAX is a special value that means unknown size.
     * Unkown size is used during log replay where the latest version
     * is directly written into ccmap and we don't know the record size
     * in KV storage.
     */
    int32_t data_store_size_{INT32_MAX};
#endif
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
#ifdef ON_KEY_OBJECT
    const ValueT *payload_ptr_;
#else
    std::shared_ptr<ValueT> payload_ptr_;
#endif
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
#ifdef ON_KEY_OBJECT
    std::unique_ptr<ValueT> payload_;
#else
    std::shared_ptr<ValueT> payload_;
#endif
    uint64_t commit_ts_;
    RecordStatus payload_status_;

    VersionRecord()
        : payload_(nullptr),
          commit_ts_(1),
          payload_status_(RecordStatus::Unknown)
    {
    }

    VersionRecord(

#ifdef ON_KEY_OBJECT
        std::unique_ptr<ValueT> payload,
#else
        std::shared_ptr<ValueT> payload,
#endif
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
 * @brief A map entry in the concurrency control map. An entry governs
 * concurrency control of a data item and caches the data item's newest
 * committed value, as well as historical versions if needed.
 *
 * @tparam KeyT The key type of the data item
 * @tparam ValueT The value type of the data item
 */
template <typename KeyT, typename ValueT>
struct CcEntry : public LruEntry
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

    CcEntry(CcEntry<KeyT, ValueT> &other) = delete;

    size_t GetCcEntryMemUsage() const
    {
        size_t mem_usage = basic_mem_overhead_;
        mem_usage += PayloadMemUsage();
#ifndef ON_KEY_OBJECT
        mem_usage += GetArchiveMemUsage();
#endif
        return mem_usage;
    }

    // For debug log use only.
    std::string KeyString() const
    {
        if (GetCcPage() == nullptr)
        {
            return "no lock, no key";
        }
        return reinterpret_cast<CcPage<KeyT, ValueT> *>(GetCcPage())
            ->KeyOfEntry(this)
            ->ToString();
    }

    size_t PayloadMemUsage() const
    {
        if (payload_ == nullptr)
        {
            return 0;
        }
        else
        {
            return payload_->MemUsage();
        }
    }

    size_t PayloadSize() const
    {
        return payload_ == nullptr ? 0 : payload_->Size();
    }

    size_t PayloadSerializedLength() const
    {
        return payload_ == nullptr ? 0 : payload_->SerializedLength();
    }

#ifdef ON_KEY_OBJECT
    std::variant<TxCommand *, std::unique_ptr<TxCommand>> PendingCmd()
    {
        assert(cc_lock_and_extra_ != nullptr);
        return cc_lock_and_extra_->PendingCmd();
    }

    TxCommand *GetPendingCommand() const
    {
        if (cc_lock_and_extra_ == nullptr)
        {
            return nullptr;
        }
        return cc_lock_and_extra_->GetPendingCmd();
    }

    void SetPendingCmd(
        std::variant<TxCommand *, std::unique_ptr<TxCommand>> cmd)
    {
        assert(cc_lock_and_extra_ != nullptr);
        cc_lock_and_extra_->SetPendingCmd(std::move(cmd));
    }

    bool IsNullPendingCmd()
    {
        return cc_lock_and_extra_ == nullptr ||
               cc_lock_and_extra_->IsNullPendingCmd();
    }

    std::unique_ptr<ValueT> DirtyPayload()
    {
        assert(cc_lock_and_extra_ != nullptr);
        return std::unique_ptr<ValueT>(static_cast<ValueT *>(
            cc_lock_and_extra_->DirtyPayload().release()));
    }

    void SetDirtyPayload(std::unique_ptr<ValueT> dirty_payload)
    {
        auto tx_obj_uptr = std::unique_ptr<TxObject>(
            static_cast<TxObject *>(dirty_payload.release()));
        assert(cc_lock_and_extra_ != nullptr);
        cc_lock_and_extra_->SetDirtyPayload(std::move(tx_obj_uptr));
    }

    RecordStatus DirtyPayloadStatus() const
    {
        assert(cc_lock_and_extra_ != nullptr);
        return cc_lock_and_extra_->DirtyPayloadStatus();
    }

    void SetDirtyPayloadStatus(RecordStatus status)
    {
        assert(cc_lock_and_extra_ != nullptr);
        cc_lock_and_extra_->SetDirtyPayloadStatus(status);
    }

    StandbyForwardEntry *ForwardEntry()
    {
        assert(cc_lock_and_extra_ != nullptr);
        return cc_lock_and_extra_->ForwardEntry();
    }

    void SetForwardEntry(StandbyForwardEntry *entry)
    {
        assert(cc_lock_and_extra_ != nullptr);
        cc_lock_and_extra_->SetForwardEntry(entry);
    }

    std::unique_ptr<ValueT> payload_{nullptr};
#else
    std::shared_ptr<ValueT> payload_{nullptr};
    // save versions exclude the current version.(descending order,eg.[4,3,2,1])
    std::unique_ptr<std::list<VersionRecord<ValueT>>> archives_{nullptr};
#endif

    void CloneForDefragment(CcEntry<KeyT, ValueT> *clone)
    {
        clone->commit_ts_and_status_ = commit_ts_and_status_;
        clone->cc_lock_and_extra_ = cc_lock_and_extra_;
#ifndef ON_KEY_OBJECT
        if (payload_ != nullptr)
        {
            clone->payload_ = std::make_shared<ValueT>(*payload_);
        }
        else
        {
            assert(PayloadStatus() == RecordStatus::Deleted);
            clone->payload_ = nullptr;
        }
        clone->SetCkptTs(CkptTs());
        clone->data_store_size_ = data_store_size_;
        clone->archives_ = std::move(archives_);
#else
        TxRecord *rec = static_cast<TxRecord *>(payload_.get());
        TxRecord::Uptr rec_clone = rec->Clone();
        clone->payload_.reset(static_cast<ValueT *>(rec_clone.release()));
#endif
    }

    inline static size_t basic_mem_overhead_ = sizeof(CcEntry<KeyT, ValueT>);

#ifndef ON_KEY_OBJECT
    /**
     * @brief Move(not copy) the current version (payload, payload_status,
     * commit_ts) to the archives_.
     */
    void ArchiveBeforeUpdate()
    {
        const RecordStatus rec_status = PayloadStatus();
        if (rec_status == RecordStatus::Unknown)
        {
            return;
        }

        if (archives_ == nullptr)
        {
            archives_ = std::make_unique<std::list<VersionRecord<ValueT>>>();
        }

        const uint64_t commit_ts = CommitTs();
        if (archives_->size() > 0)
        {
            assert(commit_ts > archives_->front().commit_ts_);
        }

        //  Handle payloads of pk and sk the same way.
        archives_->emplace_front(std::move(payload_), commit_ts, rec_status);
    }

    /**
     * @brief Add a batch of historical versions.
     *@param v_recs versions descending ordered by commit_ts
     */
    void AddArchiveRecords(std::vector<VersionTxRecord> &v_recs)
    {
        if (v_recs.size() == 0)
        {
            return;
        }

        if (archives_ == nullptr)
        {
            archives_ = std::make_unique<std::list<VersionRecord<ValueT>>>();
        }

        auto it = archives_->begin();
        for (; it != archives_->end(); it++)
        {
            if (it->commit_ts_ <= v_recs[0].commit_ts_)
            {
                break;
            }
        }
        for (auto &vrec : v_recs)
        {
            if (it == archives_->end() || it->commit_ts_ != vrec.commit_ts_)
            {
                it = archives_->emplace(it);
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
        if (commit_ts == 1U && payload_status == RecordStatus::Deleted)
        {
            return;
        }
        if (archives_ == nullptr)
        {
            archives_ = std::make_unique<std::list<VersionRecord<ValueT>>>();
        }

        auto it = archives_->begin();
        for (; it != archives_->end(); it++)
        {
            if (it->commit_ts_ <= commit_ts)
            {
                break;
            }
        }
        if (it == archives_->end() || it->commit_ts_ < commit_ts)
        {
            it = archives_->emplace(it);
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
        if (archives_ == nullptr)
        {
            return;
        }

        if (CommitTs() <= oldest_active_tx_ts)
        {
            archives_.reset(nullptr);
            return;
        }

        if (archives_->size() <= 1)
        {
            return;
        }

        auto it = archives_->begin();
        for (; it != archives_->end(); it++)
        {
            if (it->commit_ts_ <= oldest_active_tx_ts)
            {
                break;
            }
        }
        if (it == archives_->end())
        {
            return;
        }
        it++;
        archives_->erase(it, archives_->end());
    }

    size_t GetArchiveMemUsage() const
    {
        if (archives_ == nullptr)
        {
            return 0;
        }
        size_t mem_usage = sizeof(*archives_);
        for (auto it = archives_->begin(); it != archives_->end(); ++it)
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
    void MvccGet(uint64_t ts,
                 uint64_t &last_read_ts,
                 VersionResultRecord<ValueT> &rec)
    {
        const uint64_t commit_ts = CommitTs();
        const RecordStatus rec_status = PayloadStatus();

        if (rec_status == RecordStatus::Unknown)
        {
            rec.payload_status_ = RecordStatus::Unknown;
            rec.commit_ts_ = commit_ts;
            return;
        }
        if (commit_ts <= ts)
        {
            // MVCC update last_read_ts_ of lastest ccentry to tell later
            // writer's commit_ts must be higher than MVCC reader's ts. Or it
            // will break the REPEATABLE READ since the next MVCC read in the
            // same transaction will read the new updated ccentry.
            last_read_ts = std::max(ts, last_read_ts);
            if (rec_status == RecordStatus::Normal)
            {
                rec.payload_ptr_ = payload_;
            }
            rec.commit_ts_ = commit_ts;
            rec.payload_status_ = rec_status;
            return;
        }

        if (archives_ != nullptr)
        {
            // if commit_ts_ > ts, find from archives_
            for (auto it = archives_->cbegin(); it != archives_->cend(); it++)
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
        const uint64_t ckpt_ts = CkptTs();
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
        if (CommitTs() <= read_ts)
        {
            return true;
        }
        if (archives_ != nullptr && !archives_->empty() &&
            archives_->back().commit_ts_ <= read_ts)
        {
            return true;
        }
        return false;
    }
#endif

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
#ifdef RANGE_PARTITION_ENABLED
                         bool export_persisted_key_only,
#endif
                         uint64_t &mem_usage) const
    {
        // `export_store_record_if_need` - True means If no larger version needs
        // to be flushed(commit_ts > ckpt_ts && commit_ts <= to_ts), we need to
        // export the data which commit_ts same as ckpt_ts. Because we
        // want to flush this key to new range on base table. Only for range
        // partition
        size_t exported_count = 0;

        const uint64_t commit_ts = CommitTs();
        const RecordStatus rec_status = PayloadStatus();

#ifndef ON_KEY_OBJECT
        if (IsPersistent())
        {
            // 1.This version data has alredy flushed to base
            // table(commit_ts == ckpt_ts).
            // 2. No new version data to be
            // flushed(newest commit ts == ckpt_ts).
            // But we need to migrate data to new range on
            // base table. So we need to export this record
            // for range migration
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
                    ref.SetPayload(payload_);
                }

                mem_usage += ref.MemUsage();

                ref.post_flush_size_ = (rec_status == RecordStatus::Normal)
                                           ? (key.Size() + ref.PayloadSize())
                                           : 0;

                exported_count++;
            }
#ifdef RANGE_PARTITION_ENABLED
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

                mem_usage += ref.MemUsage();

                ref.post_flush_size_ = (rec_status == RecordStatus::Normal)
                                           ? (key.Size() + payload_->Size())
                                           : 0;

                ++exported_count;
            }
#endif

            return exported_count;
        }

        size_t ckpt_idx = ckpt_vec_size;
        assert(commit_ts > CkptTs());
#endif

#ifndef ON_KEY_OBJECT
        if (commit_ts <= to_ts)
#endif
        {
            FlushRecord &ref = ckpt_vec[ckpt_vec_size++];
            ref.CloneOrCopyKey(TxKey(&key));
            ref.cce_ =
                const_cast<LruEntry *>(static_cast<const LruEntry *>(this));
            ref.cce_->SetBeingCkpt();

            if (rec_status == RecordStatus::Normal)
            {
#ifndef ON_KEY_OBJECT
                ref.SetPayload(payload_);
#else
                ref.SetPayload(payload_.get());
#endif
            }

            ref.payload_status_ = rec_status;
            ref.commit_ts_ = commit_ts;
            mem_usage += ref.MemUsage();

#ifndef ON_KEY_OBJECT
            assert(data_store_size_ != INT32_MAX);
            ref.post_flush_size_ =
                (ref.payload_status_ != RecordStatus::Deleted)
                    ? key.Size() + ref.PayloadSize()
                    : 0;
#endif
            exported_count++;
        }

        if (!mvcc_enabled || export_base_table_item_only)
        {
            return exported_count;
        }

#ifndef ON_KEY_OBJECT
        const uint64_t ckpt_ts = CkptTs();

        if (archives_ != nullptr && archives_->size() > 0)
        {
            // Scan data from largest version to smallest version
            for (auto it = archives_->begin(); it != archives_->end(); it++)
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
                            // This record on base table will be overrided. We
                            // need to flush this record to archive table.
                            auto &ref = akv_vec.emplace_back();
                            ref.SetKeyIndex(ckpt_idx);
                            ref.cce_ = const_cast<LruEntry *>(
                                static_cast<const LruEntry *>(this));
                            ref.cce_->SetBeingCkpt();

                            if (it->payload_status_ == RecordStatus::Normal)
                            {
                                ref.SetPayload(it->payload_);
                            }
                            ref.payload_status_ = it->payload_status_;
                            ref.commit_ts_ = it->commit_ts_;
                            mem_usage += ref.MemUsage();
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
                                FlushRecord &ref = ckpt_vec[ckpt_vec_size++];
                                ref.CloneOrCopyKey(TxKey(&key));

                                // This record was load from storage. We
                                // can't safely point to CcEntry of CcMap.
                                // Because the entry will be kickout after
                                // UnpinSlice. the pointer will become
                                // invalidation.
                                ref.cce_ = nullptr;

                                if (it->payload_status_ == RecordStatus::Normal)
                                {
                                    ref.SetPayload(it->payload_);
                                }
                                ref.payload_status_ = it->payload_status_;
                                ref.commit_ts_ = it->commit_ts_;

                                ref.post_flush_size_ =
                                    (it->payload_status_ ==
                                     RecordStatus::Normal)
                                        ? (key.Size() + ref.PayloadSize())
                                        : 0;

                                mem_usage += ref.MemUsage();
                                exported_count++;
                            }
                            else if (export_persisted_key_only)
                            {
                                // Do not need to flush this version item to
                                // base table(because it has already in the base
                                // table), just need the key to calculate the
                                // subsice keys.
                                assert(!export_persisted_item_to_ckpt_vec);
                                FlushRecord &ref = ckpt_vec[ckpt_vec_size++];
                                ref.CloneOrCopyKey(TxKey(&key));

                                // Use nullptr to indicate that this item does
                                // not need be flush.
                                ref.cce_ = nullptr;

                                ref.payload_status_ = it->payload_status_;
                                ref.commit_ts_ = it->commit_ts_;

                                size_t payload_size = 0;
                                if (it->payload_status_ == RecordStatus::Normal)
                                {
                                    payload_size = it->payload_->Size();
                                }
                                ref.post_flush_size_ =
                                    (it->payload_status_ ==
                                     RecordStatus::Normal)
                                        ? (key.Size() + payload_size)
                                        : 0;

                                mem_usage += ref.MemUsage();
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
                            ref.cce_->SetBeingCkpt();

                            if (it->payload_status_ == RecordStatus::Normal)
                            {
                                ref.SetPayload(it->payload_);
                            }
                            ref.payload_status_ = it->payload_status_;
                            ref.commit_ts_ = it->commit_ts_;
                            mem_usage += ref.MemUsage();

                            assert(data_store_size_ != INT32_MAX);
                            ref.post_flush_size_ =
                                (ref.payload_status_ == RecordStatus::Normal)
                                    ? (key.Size() + ref.PayloadSize())
                                    : 0;
                        }
                        else
                        {
                            auto &ref = akv_vec.emplace_back();
                            ref.SetKeyIndex(ckpt_idx);
                            ref.cce_ = const_cast<LruEntry *>(
                                static_cast<const LruEntry *>(this));
                            ref.cce_->SetBeingCkpt();

                            if (it->payload_status_ == RecordStatus::Normal)
                            {
                                ref.SetPayload(it->payload_);
                            }
                            mem_usage += ref.MemUsage();
                            ref.payload_status_ = it->payload_status_;
                            ref.commit_ts_ = it->commit_ts_;
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
            // last ckpt version is needed but not in memory, and we're not sure
            // if an older version exists, need to copy record from "base table"
            // into "mvcc_archives table".
            mv_base_vec.push_back(ckpt_idx);
            mem_usage += sizeof(ckpt_idx);
        }
#endif

        return exported_count;
    }

    void UpdateCcEntry(SliceDataItem &data_item,
                       bool enable_mvcc,
                       int32_t &normal_rec_change,
                       CcPage<KeyT, ValueT> *ccp,
                       CcShard *shard)
    {
#ifdef RANGE_PARTITION_ENABLED
        // Initialize the data store size if it is unspecified
        // before
        if (data_store_size_ == INT32_MAX)
        {
            data_store_size_ =
                data_item.is_deleted_
                    ? 0
                    : data_item.key_.Size() + data_item.record_->Size();
        }
#endif

#ifndef ON_KEY_OBJECT
        const ValueT *record =
            static_cast<const ValueT *>(data_item.record_.get());
        // If the in-memory version is from a upload request (i.e.
        // generated sk record from pk), the data store version
        // might be newer. Only overwrite if in memory version is
        // newer.
        const uint64_t cce_version = CommitTs();
        if (cce_version < data_item.version_ts_)
        {
            if (data_item.is_deleted_)
            {
                payload_ = nullptr;
            }
            else
            {
                if (payload_.use_count() == 1)
                {
                    *(payload_) = *record;
                }
                else
                {
                    payload_ = std::make_shared<ValueT>(*record);
                }
            }
            RecordStatus status = data_item.is_deleted_ ? RecordStatus::Deleted
                                                        : RecordStatus::Normal;
            SetCommitTsPayloadStatus(data_item.version_ts_, status);
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
#else
        // If the in-memory version is from a upload request (i.e.
        // generated sk record from pk), the data store version
        // might be newer. Only overwrite if in memory version is
        // newer.
        const uint64_t cce_version = CommitTs();
        if (cce_version < data_item.version_ts_)
        {
            bool obj_already_exist = PayloadStatus() == RecordStatus::Normal;
            if (data_item.is_deleted_)
            {
                payload_ = nullptr;
                if (obj_already_exist)
                {
                    normal_rec_change--;
                }
                ccp->smallest_ttl_ = 0;
            }
            else
            {
                payload_.reset(
                    static_cast<ValueT *>(data_item.record_.release()));
                if (!obj_already_exist)
                {
                    normal_rec_change++;
                }
                if (ccp->smallest_ttl_ != 0 && payload_->HasTTL())
                {
                    uint64_t ttl = payload_->GetTTL();
                    ccp->smallest_ttl_ = std::min(ccp->smallest_ttl_, ttl);
                }
            }
            RecordStatus status = data_item.is_deleted_ ? RecordStatus::Deleted
                                                        : RecordStatus::Normal;
            SetCommitTsPayloadStatus(data_item.version_ts_, status);

            if (HasBufferedCommandList())
            {
                BufferedTxnCmdList &buffered_cmd_list = BufferedCommandList();
                auto &cmd_list = buffered_cmd_list.txn_cmd_list_;
                int64_t buffered_cmd_cnt_old = buffered_cmd_list.Size();

                uint64_t new_commit_ts = CommitTs();
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
                TryCommitBufferedCommands(
                    payload_, buffered_cmd_list, new_commit_ts);
                int64_t buffered_cmd_cnt_new = buffered_cmd_list.Size();
                LruEntry::UpdateBufferedCommandCnt(
                    shard, buffered_cmd_cnt_new - buffered_cmd_cnt_old);

                if (payload_)
                {
                    SetCommitTsPayloadStatus(new_commit_ts,
                                             RecordStatus::Normal);
                }
                else
                {
                    SetCommitTsPayloadStatus(new_commit_ts,
                                             RecordStatus::Deleted);
                }

                if (buffered_cmd_list.Empty())
                {
                    // Recycles the lock if all the replay commands have been
                    // applied.
                    RecycleKeyLock(*shard);
                }
            }
        }
#endif
        SetCkptTs(data_item.version_ts_);
    }

#ifndef ON_KEY_OBJECT
    size_t ArchiveRecordsCount() const
    {
        if (archives_ == nullptr)
        {
            return 0;
        }
        return archives_->size();
    }

    void ClearArchives()
    {
        if (archives_ != nullptr)
        {
            archives_.reset(nullptr);
        }
    }
#endif

    bool NeedCkpt() const
    {
        RecordStatus rec_status = PayloadStatus();
        return !IsPersistent() && (rec_status == RecordStatus::Normal ||
                                   rec_status == RecordStatus::Deleted);
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

template <typename KeyT, typename ValueT>
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
           CcPage<KeyT, ValueT> *prev_page,
           CcPage<KeyT, ValueT> *next_page)
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
           std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>> &&entries,
           CcPage<KeyT, ValueT> *prev_page,
           CcPage<KeyT, ValueT> *next_page)
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

    CcPage(const CcPage<KeyT, ValueT> &page) = delete;
    CcPage<KeyT, ValueT> operator=(const CcPage<KeyT, ValueT> &page) = delete;
    CcPage(CcPage<KeyT, ValueT> &&page) = delete;
    CcPage<KeyT, ValueT> operator=(CcPage<KeyT, ValueT> &&page) = delete;

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
                entries_.push_back(std::make_unique<CcEntry<KeyT, ValueT>>());
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
                entries_[res_index - 1] =
                    std::make_unique<CcEntry<KeyT, ValueT>>();
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
            entries_[res_index - 1] = std::make_unique<CcEntry<KeyT, ValueT>>();

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
        entries_.emplace(entries_.begin() + insert_pos,
                         std::make_unique<CcEntry<KeyT, ValueT>>());
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

    void Split(
        std::vector<KeyT> &new_page_keys,
        std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>> &new_page_entries,
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
                         entries_[idx]->payload_ &&
                         entries_[idx]->payload_->HasTTL())
                {
                    uint64_t ttl = entries_[idx]->payload_->GetTTL();
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
        std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>> &src_entries,
        std::deque<SliceDataItem> &slice_items,
        const std::vector<std::pair<size_t, bool>> &location_infos,
        size_t start_index,
        size_t end_index,
        size_t offset,
        bool enable_mvcc,
        int32_t &normal_rec_change,
        CcShard *shard)
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
                auto new_cc_entry = std::make_unique<CcEntry<KeyT, ValueT>>();
                new_cc_entry->UpdateCcEntry(slice_items[location_info.first],
                                            enable_mvcc,
                                            normal_rec_change,
                                            this,
                                            shard);

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
    size_t FindEntry(const CcEntry<KeyT, ValueT> *cce) const
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

    const KeyT *KeyOfEntry(const CcEntry<KeyT, ValueT> *cce) const
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

    CcEntry<KeyT, ValueT> *Entry(size_t idx_in_page)
    {
        assert(idx_in_page < entries_.size());
        return entries_[idx_in_page].get();
    }

    size_t Remove(size_t idx)
    {
        assert(idx < keys_.size());

        auto key_it = keys_.begin() + idx;
        auto entry_ptr_it = entries_.begin() + idx;
        size_t mem_decreased = key_it->MemUsage() - sizeof(KeyT) +
                               (*entry_ptr_it)->GetCcEntryMemUsage();

        keys_.erase(key_it);
        entries_.erase(entry_ptr_it);

        return mem_decreased;
    }

    size_t Remove(const KeyT &key)
    {
        size_t idx = Find(key);
        return Remove(idx);
    }

    size_t Remove(const CcEntry<KeyT, ValueT> *entry)
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
        typename std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>>::iterator
            begin,
        typename std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>>::iterator
            end)
    {
        if (flag & PREFETCH_FLAG_CCENTRY)
        {
            std::for_each(begin,
                          end,
                          [](const std::unique_ptr<CcEntry<KeyT, ValueT>> &cce)
                          {
                              if (cce)
                              {
                                  __builtin_prefetch(cce.get(), 1, 3);
                              }
                          });
        }
        if (flag & PREFETCH_FLAG_PAYLOAD)
        {
            std::for_each(begin,
                          end,
                          [](const std::unique_ptr<CcEntry<KeyT, ValueT>> &cce)
                          {
                              if (cce && cce->payload_.get())
                              {
                                  __builtin_prefetch(cce->payload_.get(), 1, 2);
                              }
                          });
        }
        if (flag & PREFETCH_FLAG_BLOB)
        {
            std::for_each(begin,
                          end,
                          [](const std::unique_ptr<CcEntry<KeyT, ValueT>> &cce)
                          {
                              if (cce && cce->payload_.get())
                              {
                                  cce->payload_->Prefetch();
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
    std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>> entries_;

    CcPage<KeyT, ValueT> *prev_page_{nullptr};
    CcPage<KeyT, ValueT> *next_page_{nullptr};

    // CcPage is contained in std::_Rb_tree_node with node key and RBT node
    // pointers (32 bytes)
    inline static size_t basic_mem_overhead_ =
        sizeof(KeyT) + sizeof(CcPage<KeyT, ValueT>) + 32 +
        sizeof(KeyT) * split_threshold_ +
        sizeof(std::unique_ptr<CcEntry<KeyT, ValueT>>) * split_threshold_ +
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
