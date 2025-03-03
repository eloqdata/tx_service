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
#include "range_bucket_key_record.h"

#include <cstdint>

namespace txservice
{
bool operator==(const RangeBucketKey &lhs, const RangeBucketKey &rhs)
{
    return lhs.bucket_id_ == rhs.bucket_id_;
}

bool operator!=(const RangeBucketKey &lhs, const RangeBucketKey &rhs)
{
    return lhs.bucket_id_ != rhs.bucket_id_;
}

bool operator<(const RangeBucketKey &lhs, const RangeBucketKey &rhs)
{
    return lhs.bucket_id_ < rhs.bucket_id_;
}

bool operator<=(const RangeBucketKey &lhs, const RangeBucketKey &rhs)
{
    return lhs.bucket_id_ <= rhs.bucket_id_;
}

bool RangeBucketKey::operator==(const TxKey &rhs) const
{
    return false;
}

bool RangeBucketKey::operator<(const TxKey &rhs) const
{
    return false;
}

size_t RangeBucketKey::Hash() const
{
    return std::hash<uint16_t>()(bucket_id_);
}

void RangeBucketKey::Serialize(std::vector<char> &buf, size_t &offset) const
{
    // Serialize bucket_id_ to buffer at offset
    const char *val_ptr =
        static_cast<const char *>(static_cast<const void *>(&bucket_id_));
    std::copy(val_ptr, val_ptr + sizeof(uint16_t), buf.begin() + offset);
    offset += sizeof(uint16_t);
}

void RangeBucketKey::Serialize(std::string &str) const
{
    // Serialize bucket_id_ to string
    const char *ptr = reinterpret_cast<const char *>(&bucket_id_);
    str.append(ptr, sizeof(uint16_t));
}

size_t RangeBucketKey::SerializedLength() const
{
    return sizeof(uint16_t);
}

void RangeBucketKey::Deserialize(const char *buf,
                                 size_t &offset,
                                 const KeySchema *schema)
{
    bucket_id_ = *((uint16_t *) (buf + offset));
    offset += sizeof(uint16_t);
}

TxKey RangeBucketKey::CloneTxKey() const
{
    return TxKey(std::make_unique<RangeBucketKey>(*this));
}

void RangeBucketKey::Copy(const RangeBucketKey &rhs)
{
    bucket_id_ = rhs.bucket_id_;
}

std::string RangeBucketKey::ToString() const
{
    return std::to_string(bucket_id_);
}

void RangeBucketRecord::Serialize(std::vector<char> &buf, size_t &offset) const
{
    assert(false);
}

void RangeBucketRecord::Serialize(std::string &str) const
{
    const BucketInfo *bucket_info =
        is_owner_ ? bucket_info_uptr_.get() : bucket_info_;
    assert(bucket_info != nullptr);
    SerializeToStr(&bucket_info->bucket_owner_, str);
    SerializeToStr(&bucket_info->version_, str);
    SerializeToStr(&bucket_info_->dirty_bucket_owner_, str);
    SerializeToStr(&bucket_info_->dirty_version_, str);
}

size_t RangeBucketRecord::SerializedLength() const
{
    return sizeof(NodeGroupId) * 2 + sizeof(uint64_t) * 2;
}

void RangeBucketRecord::Deserialize(const char *buf, size_t &offset)
{
    if (!is_owner_)
    {
        bucket_info_ = nullptr;
    }

    is_owner_ = true;
    NodeGroupId bucket_owner, dirty_owner;
    uint64_t version, dirty_version;
    DesrializeFrom(buf, offset, &bucket_owner);
    DesrializeFrom(buf, offset, &version);
    DesrializeFrom(buf, offset, &dirty_owner);
    DesrializeFrom(buf, offset, &dirty_version);
    bucket_info_uptr_ = std::make_unique<BucketInfo>(bucket_owner, version);
    bucket_info_uptr_->SetDirty(dirty_owner, dirty_version);
}

TxRecord::Uptr RangeBucketRecord::Clone() const
{
    return std::make_unique<RangeBucketRecord>(*this);
}

void RangeBucketRecord::Copy(const TxRecord &rhs)
{
    const RangeBucketRecord &typed_rhs =
        static_cast<const RangeBucketRecord &>(rhs);
    *this = typed_rhs;
}

std::string RangeBucketRecord::ToString() const
{
    return "";
}

}  // namespace txservice
