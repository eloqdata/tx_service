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

#include <assert.h>
#include <mimalloc-2.1/mimalloc.h>

#include <memory>
#include <string>
#include <utility>  //std::move
#include <vector>   //std::vector

#include "tx_serialize.h"

namespace txservice
{
enum struct RecordStatus : uint8_t
{
    /// <summary>
    /// The record returned from the cc map is the newest committed value.
    /// </summary>
    Normal = 0,
    /// <summary>
    /// A tx starts concurrency control for the key, but the key's record is
    /// unknown and needs to be retrieved from the data store.
    /// </summary>
    Unknown,
    /// <summary>
    /// The record is deleted.
    /// </summary>
    Deleted,
    /// <summary>
    /// A tx sends a read request to bring the record into a remote cc map for
    /// caching. The read request does not wait for the response, so the remote
    /// record's newest status is unknown (which may have changed since the
    /// initial read that starts concurrency control).
    /// </summary>
    RemoteUnknown,
    /// <summary>
    /// Under SnapshotIsolation, a transaction needs to read a historical
    /// version of ccentry but the version is not in memory.
    /// Case the checkpoint timestamp of ccentry was not set (eg. created
    /// through log replay), we don't know whether the version to be read is in
    /// "base table" or "mvcc_archives tables" in the data store.
    /// </summary>
    VersionUnknown,
    /// <summary>
    /// Case the checkpoint timestamp of ccentry has been set and the checkpoint
    /// version (the version in "base table") is just the expected version.
    /// </summary>
    BaseVersionMiss,
    /// <summary>
    /// Case the checkpoint timestamp of ccentry has been set but the checkpoint
    /// timestamp is bigger than read timestamp. Then, the expected version may
    /// be in "mvcc_archives tables".
    /// </summary>
    ArchiveVersionMiss,

    Invalid,

    /// <summary>
    /// Used only to indicate the status of temporary object. The temporary
    /// object does not exist.(no dirty_payload, no pending_cmd)
    /// </summary>
    NonExistent,
    /// <summary>
    /// Used only to indicate the status of temporary object. The temporary
    /// hasn't been created yet.(no dirty_payload, has pending_cmd)
    /// </summary>
    Uncreated,
};

struct TxRecord
{
    using Uptr = std::unique_ptr<TxRecord>;

    virtual ~TxRecord() = default;
    virtual void Serialize(std::vector<char> &buf, size_t &offset) const = 0;
    virtual void Serialize(std::string &str) const = 0;
    virtual void Deserialize(const char *buf, size_t &offset) = 0;
    virtual TxRecord::Uptr Clone() const = 0;
    virtual void Copy(const TxRecord &rhs) = 0;
    virtual std::string ToString() const = 0;

    /**
     * To estimate log length.
     * @return
     */
    virtual size_t SerializedLength() const
    {
        return 0;
    };

    virtual size_t MemUsage() const
    {
        return 0;
    }

    virtual size_t Size() const
    {
        return 0;
    }

    virtual bool NeedsDefrag(mi_heap_t *heap)
    {
        return false;
    }

    virtual void SetTTL(uint64_t ttl)
    {
        assert(false);
        return;
    }

    virtual uint64_t GetTTL() const
    {
        assert(false);
        return 0;
    }

    virtual bool HasTTL() const
    {
        return false;
    }

    // convert to ttl txrecord
    virtual TxRecord::Uptr AddTTL(uint64_t ttl)
    {
        assert(false);
        return nullptr;
    }

    // convert to plain txrecord without ttl
    virtual TxRecord::Uptr RemoveTTL()
    {
        assert(false);
        return nullptr;
    }

    virtual void SetUnpackInfo(const unsigned char *unpack_ptr,
                               size_t unpack_size)
    {
        assert(false);
    }

    virtual void SetEncodedBlob(const unsigned char *blob_ptr, size_t blob_size)
    {
        assert(false);
    }

    virtual const char *EncodedBlobData() const
    {
        assert(false);
        return nullptr;
    }

    virtual size_t EncodedBlobSize() const
    {
        assert(false);
        return 0;
    }

    virtual const char *UnpackInfoData() const
    {
        assert(false);
        return nullptr;
    }

    virtual size_t UnpackInfoSize() const
    {
        assert(false);
        return 0;
    }

    virtual size_t Length() const
    {
        assert(false);
        return 0;
    }

    virtual void Prefetch() const
    {
        return;
    }
};

template <typename... Types>
class CompositeRecord : public TxRecord
{
public:
    CompositeRecord() : field_cnt_(0)
    {
    }

    CompositeRecord(Types &&...val) : fields_(val...)
    {
        field_cnt_ = std::tuple_size<decltype(fields_)>::value;
    }

    CompositeRecord(std::tuple<Types...> &&t) : fields_(t)
    {
        field_cnt_ = std::tuple_size<decltype(fields_)>::value;
    }

    CompositeRecord(const CompositeRecord &other)
        : fields_(other.fields_), field_cnt_(other.field_cnt_)
    {
    }

    CompositeRecord(const TxRecord &other)
    {
        if (const CompositeRecord<Types...> *other_ptr =
                static_cast<const CompositeRecord<Types...> *>(&other))
        {
            fields_ = other_ptr->fields_;
            field_cnt_ = other_ptr->field_cnt_;
        }
    }

    CompositeRecord(CompositeRecord &&other)
        : fields_(other.fields_), field_cnt_(other.field_cnt_)
    {
    }

    ~CompositeRecord() = default;

    void Reset(Types &&...vals)
    {
        // fields_(val...);
        TupleResetHelper(fields_, std::index_sequence_for<Types...>{}, vals...);
    }

    void Reset(const Types &...vals)
    {
        TupleResetHelper(fields_, std::index_sequence_for<Types...>{}, vals...);
    }

    std::string ToString() const override
    {
        return std::apply([](Types... v)
                          { return ((Stringify<Types>::Get(v) + ",") + ...); },
                          fields_);
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const override
    {
        size_t mem_size = std::apply(
            [](Types... field) { return (MemSize<Types>::Size(field) + ...); },
            fields_);

        if (buf.capacity() - offset < mem_size)
        {
            buf.resize(offset + mem_size);
        }

        std::apply(
            [&buf, &offset](Types... field)
            { (void(Serializer<Types>::Serialize(field, buf, offset)), ...); },
            fields_);
    }

    void Serialize(std::string &buf) const override
    {
        std::apply([&buf](Types... field)
                   { (void(Serializer<Types>::Serialize(field, buf)), ...); },
                   fields_);
    }

    size_t SerializedLength() const override
    {
        return 0;
    }

    void Deserialize(const char *buf, size_t &offset) override
    {
        TupleDeserializeHelper(
            fields_, std::index_sequence_for<Types...>{}, buf, offset);
    }

    TxRecord::Uptr Clone() const override
    {
        return std::make_unique<CompositeRecord<Types...>>(*this);
    }

    void Copy(const TxRecord &rhs) override
    {
        const CompositeRecord<Types...> &typed_rhs =
            static_cast<const CompositeRecord<Types...> &>(rhs);

        fields_ = typed_rhs.fields_;
        field_cnt_ = typed_rhs.field_cnt_;
    }

    size_t MemUsage() const override
    {
        return sizeof(field_cnt_) + sizeof(fields_);
    }

    CompositeRecord &operator=(const CompositeRecord &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        fields_ = rhs.fields_;
        field_cnt_ = rhs.field_cnt_;

        return *this;
    }

    const std::tuple<Types...> &Tuple() const
    {
        return fields_;
    }

    std::tuple<Types...> &Tuple()
    {
        return fields_;
    }

private:
    std::tuple<Types...> fields_;
    size_t field_cnt_;
};

struct VoidRecord : public TxRecord
{
public:
    VoidRecord()
    {
    }

    VoidRecord(const VoidRecord &rhs)
    {
    }

    VoidRecord(VoidRecord &&rhs)
    {
    }

    ~VoidRecord() = default;

    void Serialize(std::vector<char> &buf, size_t &offset) const override
    {
    }

    void Serialize(std::string &str) const override
    {
    }

    size_t SerializedLength() const override
    {
        return 0;
    }

    void Deserialize(const char *buf, size_t &offset) override
    {
    }

    TxRecord::Uptr Clone() const override
    {
        return std::make_unique<VoidRecord>();
    }

    void Copy(const TxRecord &rhs) override
    {
        return;
    }

    VoidRecord &operator=(const VoidRecord &rhs)
    {
        return *this;
    }

    VoidRecord &operator=(VoidRecord &&other) noexcept
    {
        return *this;
    }

    std::string ToString() const override
    {
        return std::string("");
    }
};

/**
 * @brief A wrap type of TxRecord with version and status.
 * @param record_status_ : txservice::RecordStatus
 * @param commit_ts_ : uint64_t
 */
struct VersionTxRecord
{
public:
    VersionTxRecord()
        : record_(nullptr),
          record_status_(RecordStatus::Unknown),
          commit_ts_(1UL)
    {
    }

    std::unique_ptr<TxRecord> record_;
    RecordStatus record_status_;
    uint64_t commit_ts_;
};

struct BlobTxRecord : public TxRecord
{
    BlobTxRecord() = default;

    BlobTxRecord(const BlobTxRecord &rhs) : value_(rhs.value_), ttl_(rhs.ttl_)
    {
    }

    BlobTxRecord(BlobTxRecord &&rhs)
        : value_(std::move(rhs.value_)), ttl_(rhs.ttl_)
    {
    }

    BlobTxRecord &operator=(const BlobTxRecord &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }
        value_ = rhs.value_;
        ttl_ = rhs.ttl_;
        return *this;
    }

    BlobTxRecord &operator=(BlobTxRecord &&rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        value_ = std::move(rhs.value_);
        ttl_ = rhs.ttl_;

        return *this;
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const override
    {
        assert(false);
    }

    void Serialize(std::string &str) const override
    {
        str.append(value_);
    }
    void Deserialize(const char *buf, size_t &offset) override
    {
        assert(false);
    }
    TxRecord::Uptr Clone() const override
    {
        assert(false);

        return std::make_unique<BlobTxRecord>(*this);
    }
    void Copy(const TxRecord &rhs) override
    {
        assert(false);
        const BlobTxRecord &typed_rhs = static_cast<const BlobTxRecord &>(rhs);
        value_ = typed_rhs.value_;
    }
    std::string ToString() const override
    {
        assert(false);
        return value_;
    }

    /**
     * To estimate log length.
     * @return
     */
    size_t SerializedLength() const override
    {
        assert(false);
        return value_.size();
    };

    size_t MemUsage() const override
    {
        return sizeof(BlobTxRecord) + value_.size();
    }

    size_t Size() const override
    {
        return value_.size();
    }

    bool HasTTL() const override
    {
        return ttl_ != UINT64_MAX;
    }

    void SetTTL(uint64_t ttl) override
    {
        ttl_ = ttl;
    }

    uint64_t GetTTL() const override
    {
        return ttl_;
    }

    const char *EncodedBlobData() const override
    {
        return value_.data();
    }

    size_t EncodedBlobSize() const override
    {
        return value_.size();
    }

    size_t UnpackInfoSize() const override
    {
        return 0;
    }

    size_t Length() const override
    {
        return value_.size();
    }

    std::string value_;
    // For leveraging the TTL feature support by Cassandra and Dynamo
    uint64_t ttl_{UINT64_MAX};
};

}  // namespace txservice
