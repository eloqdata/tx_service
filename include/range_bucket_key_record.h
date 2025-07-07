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
#include <cstdint>

#include "tx_key.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
struct BucketInfo
{
public:
    BucketInfo() = default;
    BucketInfo(const NodeGroupId bucket_owner, uint64_t version)
        : bucket_owner_(bucket_owner), version_(version)
    {
    }

    BucketInfo(const BucketInfo &other) : BucketInfo()
    {
        *this = other;
    }

    BucketInfo &operator=(const BucketInfo &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }
        bucket_owner_ = rhs.bucket_owner_;
        version_ = rhs.version_;
        dirty_bucket_owner_ = rhs.dirty_bucket_owner_;
        dirty_version_ = rhs.dirty_version_;
        accepts_upload_batch_.store(
            rhs.accepts_upload_batch_.load(std::memory_order_relaxed),
            std::memory_order_relaxed);
        return *this;
    }

    void Reset()
    {
        bucket_owner_ = UINT32_MAX;
        version_ = 0;
        dirty_bucket_owner_ = UINT32_MAX;
        dirty_version_ = 0;
    }

    void ClearDirty()
    {
        dirty_bucket_owner_ = UINT32_MAX;
        dirty_version_ = 0;
    }

    NodeGroupId BucketOwner() const
    {
        return bucket_owner_;
    }

    uint64_t Version() const
    {
        return version_;
    }

    NodeGroupId DirtyBucketOwner() const
    {
        return dirty_bucket_owner_;
    }

    uint64_t DirtyVersion() const
    {
        return dirty_version_;
    }

    std::unique_ptr<BucketInfo> Clone() const
    {
        std::unique_ptr<BucketInfo> clone =
            std::make_unique<BucketInfo>(bucket_owner_, version_);
        clone->SetDirty(dirty_bucket_owner_, dirty_version_);
        return clone;
    }

    void Set(NodeGroupId owner_ng, uint64_t version)
    {
        if (version > version_)
        {
            bucket_owner_ = owner_ng;
            version_ = version;
            assert(dirty_version_ == 0);
        }
    }

    void SetDirty(NodeGroupId dirty_bucket_owner, uint64_t dirty_version)
    {
        if (dirty_version > version_ && dirty_version > dirty_version_)
        {
            dirty_bucket_owner_ = dirty_bucket_owner;
            dirty_version_ = dirty_version;
        }
    }

    void SetAcceptsUploadBatch(bool accepts_upload_batch)
    {
        accepts_upload_batch_.store(accepts_upload_batch,
                                    std::memory_order_release);
    }

    bool AcceptsUploadBatch() const
    {
        return accepts_upload_batch_.load(std::memory_order_acquire);
    }

    void CommitDirty()
    {
        if (dirty_version_ > version_)
        {
            version_ = dirty_version_;
            bucket_owner_ = dirty_bucket_owner_;
        }
        accepts_upload_batch_.store(false, std::memory_order_release);
        dirty_bucket_owner_ = UINT32_MAX;
        dirty_version_ = 0;
    }

private:
    NodeGroupId bucket_owner_{UINT32_MAX};
    uint64_t version_{0};

    NodeGroupId dirty_bucket_owner_{UINT32_MAX};
    uint64_t dirty_version_{0};
    // If this bucket accepts records sent from other node groups with
    // UploadBatchCc. This is set to true during bucket migration if this
    // node group is the new owner of the bucket. It will be set back to false
    // when 1) the migration is done or 2) the data in this buckets has been
    // kicked out from cc map, in which case we're running short of memory so we
    // don't want to accept more records. Also it is not safe to accept upload
    // batch since we might have newer version in kv store.
    // We use atomic here since it might be updated by any tx processor without
    // bucket write lock.
    std::atomic_bool accepts_upload_batch_{false};
    friend struct RangeBucketRecord;
    friend class RangeBucketCcMap;
};

struct BucketMigrateInfo
{
    BucketMigrateInfo() = default;

    std::vector<uint16_t> bucket_ids_;
    std::vector<NodeGroupId> new_owner_ngs_;
    bool has_migration_tx_{false};
};

struct RangeBucketKey
{
public:
    RangeBucketKey() = default;
    RangeBucketKey(const uint16_t bucket_id) : bucket_id_(bucket_id)
    {
    }
    RangeBucketKey(RangeBucketKey &&rhs) = default;
    RangeBucketKey(const RangeBucketKey &rhs) = default;
    RangeBucketKey &operator=(RangeBucketKey &&) = default;
    ~RangeBucketKey() = default;

    bool operator==(const TxKey &rhs) const;
    bool operator<(const TxKey &rhs) const;
    size_t Hash() const;
    void Serialize(std::vector<char> &buf, size_t &offset) const;
    void Serialize(std::string &str) const;
    size_t SerializedLength() const;
    void Deserialize(const char *buf, size_t &offset, const KeySchema *);
    std::string_view KVSerialize() const
    {
        assert(false);
        return std::string_view();
    }
    void KVDeserialize(const char *buf, size_t len)
    {
        assert(false);
    }
    TxKey CloneTxKey() const;
    std::string ToString() const;
    void Copy(const RangeBucketKey &rhs);
    KeyType Type() const
    {
        return KeyType::Normal;
    }

    void SetPackedKey(const char *data, size_t size)
    {
        assert(false);
    }

    const char *Data() const
    {
        assert(false);
        return nullptr;
    }

    size_t Size() const
    {
        return sizeof(bucket_id_);
    }

    size_t MemUsage() const
    {
        return Size();
    }

    bool NeedsDefrag(mi_heap_t *heap)
    {
        return false;
    }

    static const RangeBucketKey *NegativeInfinity()
    {
        static const RangeBucketKey neg_inf;
        return &neg_inf;
    }

    static const RangeBucketKey *PositiveInfinity()
    {
        static const RangeBucketKey pos_inf;
        return &pos_inf;
    }

    uint16_t BucketId() const
    {
        return bucket_id_;
    }

    void Reset(const uint16_t bucket_id)
    {
        bucket_id_ = bucket_id;
    }

    friend bool operator==(const RangeBucketKey &lhs,
                           const RangeBucketKey &rhs);
    friend bool operator!=(const RangeBucketKey &lhs,
                           const RangeBucketKey &rhs);
    friend bool operator<(const RangeBucketKey &lhs, const RangeBucketKey &rhs);
    friend bool operator<=(const RangeBucketKey &lhs,
                           const RangeBucketKey &rhs);

    static const TxKeyInterface *TxKeyImpl()
    {
        static const TxKeyInterface tx_key_impl{RangeBucketKey()};
        return &tx_key_impl;
    }

    uint16_t bucket_id_{UINT16_MAX};
};

struct RangeBucketRecord : public TxRecord
{
public:
    RangeBucketRecord() : bucket_info_(nullptr), is_owner_(false)
    {
    }
    RangeBucketRecord(BucketInfo *bucket_info)
        : bucket_info_(bucket_info), is_owner_(false)
    {
    }
    RangeBucketRecord(RangeBucketRecord &&rhs) : is_owner_(rhs.is_owner_)
    {
        if (is_owner_)
        {
            bucket_info_uptr_ = std::move(rhs.bucket_info_uptr_);
        }
        else
        {
            bucket_info_ = rhs.bucket_info_;
        }
    }
    RangeBucketRecord(const RangeBucketRecord &rhs) : is_owner_(rhs.is_owner_)
    {
        if (is_owner_)
        {
            bucket_info_uptr_ = rhs.bucket_info_uptr_->Clone();
        }
        else
        {
            bucket_info_ = rhs.bucket_info_;
        }
    }
    RangeBucketRecord &operator=(const RangeBucketRecord &rhs)
    {
        if (&rhs == this)
        {
            return *this;
        }
        if (!is_owner_)
        {
            // clear pointer so that we don't accidently frees the bucket info
            // when moving in unique ptr.
            bucket_info_ = nullptr;
        }
        else if (bucket_info_uptr_)
        {
            // Free the bucket info since we might not call destructor of bucket
            // info if rhs is not owner.
            bucket_info_uptr_.reset();
        }

        is_owner_ = rhs.is_owner_;
        if (is_owner_)
        {
            bucket_info_uptr_ = rhs.bucket_info_uptr_->Clone();
        }
        else
        {
            bucket_info_ = rhs.bucket_info_;
        }
        return *this;
    }
    ~RangeBucketRecord()
    {
        if (is_owner_ && bucket_info_uptr_)
        {
            bucket_info_uptr_.reset();
        }
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const override;
    void Serialize(std::string &str) const override;
    size_t SerializedLength() const override;
    void Deserialize(const char *buf, size_t &offset) override;
    TxRecord::Uptr Clone() const override;
    void Copy(const TxRecord &rhs) override;
    std::string ToString() const override;

    const BucketInfo *GetBucketInfo() const
    {
        return is_owner_ ? bucket_info_uptr_.get() : bucket_info_;
    }

    size_t Size() const override
    {
        return sizeof(bucket_info_);
    }

    size_t MemUsage() const override
    {
        return sizeof(*this);
    }

    void SetBucketInfo(const BucketInfo *bucket_info)
    {
        if (is_owner_ && bucket_info_uptr_)
        {
            bucket_info_uptr_.reset();
        }
        is_owner_ = false;
        bucket_info_ = bucket_info;
    }

    void SetBucketInfo(std::unique_ptr<BucketInfo> bucket_info)
    {
        if (!is_owner_)
        {
            bucket_info_ = nullptr;
        }
        is_owner_ = true;
        bucket_info_uptr_ = std::move(bucket_info);
    }

private:
    union
    {
        const BucketInfo *bucket_info_;
        std::unique_ptr<BucketInfo> bucket_info_uptr_;
    };

    bool is_owner_{false};
};
}  // namespace txservice
