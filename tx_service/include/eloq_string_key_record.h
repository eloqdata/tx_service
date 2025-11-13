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

#include <functional>
#include <vector>

#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{

class EloqStringKey
{
public:
    EloqStringKey() = default;

    EloqStringKey(const char *key_buf, size_t key_len) : key_(key_buf, key_len)
    {
    }

    EloqStringKey(std::string_view str_view) : key_(str_view)
    {
    }

    // Deep copy the key_
    EloqStringKey(const EloqStringKey &rhs) : key_(rhs.key_)
    {
    }

    EloqStringKey(EloqStringKey &&rhs) : key_(std::move(rhs.key_))
    {
    }

    static txservice::TxKey Create(const char *data, size_t size)
    {
        return txservice::TxKey(std::make_unique<EloqStringKey>(data, size));
    }

    static txservice::TxKey CreateDefault()
    {
        return txservice::TxKey(std::make_unique<EloqStringKey>());
    }

    EloqStringKey &operator=(EloqStringKey &&rhs) noexcept
    {
        if (this != &rhs)
        {
            key_ = std::move(rhs.key_);
        }
        return *this;
    }

    EloqStringKey &operator=(const EloqStringKey &rhs)
    {
        if (this != &rhs)
        {
            // Deep copy the key
            key_ = rhs.key_;
        }

        return *this;
    }

    friend bool operator==(const EloqStringKey &lhs, const EloqStringKey &rhs)
    {
        const EloqStringKey *neg_ptr = EloqStringKey::NegativeInfinity();
        const EloqStringKey *pos_ptr = EloqStringKey::PositiveInfinity();

        if (&lhs == neg_ptr || &lhs == pos_ptr || &rhs == neg_ptr ||
            &rhs == pos_ptr)
        {
            return &lhs == &rhs;
        }

        return lhs.key_ == rhs.key_;
    }

    friend bool operator!=(const EloqStringKey &lhs, const EloqStringKey &rhs)
    {
        return !(lhs == rhs);
    }

    friend bool operator<(const EloqStringKey &lhs, const EloqStringKey &rhs)
    {
        const EloqStringKey *neg_ptr = EloqStringKey::NegativeInfinity();
        const EloqStringKey *pos_ptr = EloqStringKey::PositiveInfinity();

        if (&lhs == neg_ptr)
        {
            // Negative infinity is less than any key, except itself.
            return &rhs != neg_ptr;
        }
        else if (&lhs == pos_ptr || &rhs == neg_ptr)
        {
            return false;
        }
        else if (&rhs == pos_ptr)
        {
            // Positive infinity is greater than any key, except itself.
            return &lhs != pos_ptr;
        }

        return std::string_view(lhs.key_) < std::string_view(rhs.key_);
    }

    friend bool operator<=(const EloqStringKey &lhs, const EloqStringKey &rhs)
    {
        return lhs < rhs || lhs == rhs;
    }

    // Use std::hash to calculate the hash for the key string value
    size_t Hash() const
    {
        std::hash<std::string_view> hasher;
        return hasher(std::string_view(key_));
    }

    static size_t Hash(const char *ptr, int32_t keylen)
    {
        std::hash<std::string_view> hasher;
        return hasher(std::string_view(ptr, keylen));
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const
    {
        std::string_view key_view = std::string_view(key_);

        uint16_t len_val = (uint16_t) key_view.size();
        buf.resize(offset + sizeof(uint16_t) + len_val);
        const char *val_ptr =
            static_cast<const char *>(static_cast<const void *>(&len_val));
        std::copy(val_ptr, val_ptr + sizeof(uint16_t), buf.begin() + offset);
        offset += sizeof(uint16_t);
        std::copy(key_view.begin(), key_view.end(), buf.begin() + offset);
        offset += len_val;
    }

    void Serialize(std::string &str) const
    {
        std::string_view key_view = std::string_view(key_);

        size_t len_sizeof = sizeof(uint16_t);
        // A 2-byte integer represents key lengths up to 65535
        uint16_t len_val = (uint16_t) key_view.size();
        const char *len_ptr = reinterpret_cast<const char *>(&len_val);

        str.append(len_ptr, len_sizeof);
        str.append(key_view.data(), len_val);
    }

    void Deserialize(const char *buf,
                     size_t &offset,
                     const txservice::KeySchema *key_schema)
    {
        const uint16_t *len_ptr =
            reinterpret_cast<const uint16_t *>(buf + offset);
        uint16_t len_val = *len_ptr;
        offset += sizeof(uint16_t);

        key_ = std::string(buf + offset, len_val);

        offset += len_val;
    }

    static size_t HashFromSerializedKey(const char *buf, size_t offset)
    {
        const uint16_t *len_ptr =
            reinterpret_cast<const uint16_t *>(buf + offset);
        uint16_t len_val = *len_ptr;

        const char *key_str_ptr = buf + offset + sizeof(uint16_t);

        return Hash(key_str_ptr, len_val);
    }

    std::string_view KVSerialize() const
    {
        return std::string_view(key_);
    }

    void KVDeserialize(const char *buf, size_t len)
    {
        key_ = std::string(buf, len);
    }

    txservice::TxKey CloneTxKey() const
    {
        if (this == NegativeInfinity())
        {
            return txservice::TxKey(NegativeInfinity());
        }
        else if (this == PositiveInfinity())
        {
            return txservice::TxKey(PositiveInfinity());
        }
        else
        {
            return txservice::TxKey(std::make_unique<EloqStringKey>(*this));
        }
    }

    std::string ToString() const
    {
        return key_;
    }

    void SetPackedKey(const char *data, size_t len)
    {
        key_ = std::string(data, len);
    }

    void Copy(const EloqStringKey &rhs)
    {
        key_ = rhs.key_;
    }

    size_t SerializedLength() const
    {
        return key_.size() + sizeof(uint16_t);
    }

    size_t MemUsage() const
    {
        return key_.size();
    }

    const char *Buf() const
    {
        return key_.data();
    }

    size_t Length() const
    {
        return key_.size();
    }

    const char *Data() const
    {
        return Buf();
    }

    size_t Size() const
    {
        return Length();
    }

    std::string_view StringView() const
    {
        return std::string_view(key_);
    }

    static const EloqStringKey *NegativeInfinity()
    {
        static const EloqStringKey neg_inf;
        return &neg_inf;
    }

    static const EloqStringKey *PositiveInfinity()
    {
        static const EloqStringKey pos_inf;
        return &pos_inf;
    }

    static const txservice::TxKey *NegInfTxKey()
    {
        static const txservice::TxKey neg_inf_tx_key{NegativeInfinity()};
        return &neg_inf_tx_key;
    }

    static const txservice::TxKey *PosInfTxKey()
    {
        static const txservice::TxKey pos_inf_tx_key{PositiveInfinity()};
        return &pos_inf_tx_key;
    }

    static const EloqStringKey *PackedNegativeInfinity()
    {
        static char neg_inf_packed_key = 0x00;
        static const EloqStringKey neg_inf_key(&neg_inf_packed_key, 1);
        return &neg_inf_key;
    }

    static const txservice::TxKey *PackedNegativeInfinityTxKey()
    {
        static const txservice::TxKey packed_negative_infinity_tx_key{
            PackedNegativeInfinity()};
        return &packed_negative_infinity_tx_key;
    }

    txservice::KeyType Type() const
    {
        if (this == EloqStringKey::NegativeInfinity())
        {
            return txservice::KeyType::NegativeInf;
        }
        else if (this == EloqStringKey::PositiveInfinity())
        {
            return txservice::KeyType::PositiveInf;
        }
        else
        {
            return txservice::KeyType::Normal;
        }
    }

    bool NeedsDefrag(mi_heap_t *heap)
    {
        if (key_.data() != nullptr)
        {
            float page_utilization =
                mi_heap_page_utilization(heap, key_.data());
            if (page_utilization < 0.8)
            {
                return true;
            }
        }
        return false;
    }

    static const ::txservice::TxKeyInterface *TxKeyImpl()
    {
        static const txservice::TxKeyInterface tx_key_impl{
            *EloqStringKey::NegativeInfinity()};
        return &tx_key_impl;
    }

private:
    // TODO(liunyl): use EloqString instead of std::string
    std::string key_;
};

class EloqStringRecord : public TxRecord
{
public:
    EloqStringRecord() = default;

    void Serialize(std::vector<char> &buf, size_t &offset) const override;

    void Serialize(std::string &str) const override;

    void Deserialize(const char *buf, size_t &offset) override;

    TxRecord::Uptr Clone() const override;

    void Copy(const TxRecord &rhs) override;

    std::string ToString() const override;

    size_t SerializedLength() const override;

    size_t MemUsage() const override;

    size_t Size() const override;

    bool NeedsDefrag(mi_heap_t *heap) override;

    void SetTTL(uint64_t ttl) override;

    uint64_t GetTTL() const override;

    bool HasTTL() const override;

    TxRecord::Uptr AddTTL(uint64_t ttl) override;

    TxRecord::Uptr RemoveTTL() override;

    void SetUnpackInfo(const unsigned char *unpack_ptr,
                       size_t unpack_size) override;

    void SetEncodedBlob(const unsigned char *blob_ptr,
                        size_t blob_size) override;

    const char *EncodedBlobData() const override;

    size_t EncodedBlobSize() const override;

    size_t UnpackInfoSize() const override;

    const char *UnpackInfoData() const override;

    size_t Length() const override;

    void Prefetch() const override;

    static TxRecord::Uptr Create()
    {
        return std::make_unique<EloqStringRecord>();
    }

private:
    std::vector<char> encoded_blob_;
    std::vector<char> unpack_info_;
};

}  // namespace txservice
