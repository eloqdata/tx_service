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

#include "eloq_string_key_record.h"

#include <cstring>
#include <stdexcept>

namespace txservice
{

// EloqStringRecord implementation
void EloqStringRecord::Serialize(std::vector<char> &buf, size_t &offset) const
{
    size_t len = sizeof(size_t);
    buf.resize(offset + 2 * sizeof(size_t) + encoded_blob_.size() +
               unpack_info_.size());

    size_t blob_len = (size_t) unpack_info_.size();
    const unsigned char *val_ptr = static_cast<const unsigned char *>(
        static_cast<const void *>(&blob_len));
    std::copy(val_ptr, val_ptr + len, buf.begin() + offset);
    offset += len;
    std::copy(unpack_info_.begin(), unpack_info_.end(), buf.begin() + offset);
    offset += blob_len;

    blob_len = encoded_blob_.size();
    val_ptr = static_cast<const unsigned char *>(
        static_cast<const void *>(&blob_len));
    std::copy(val_ptr, val_ptr + len, buf.begin() + offset);
    offset += len;

    std::copy(encoded_blob_.begin(), encoded_blob_.end(), buf.begin() + offset);
    offset += encoded_blob_.size();
}

void EloqStringRecord::Serialize(std::string &str) const
{
    size_t len_sizeof = sizeof(size_t);
    size_t unpack_len = (size_t) unpack_info_.size();
    const char *unpack_len_ptr =
        static_cast<const char *>(static_cast<const void *>(&unpack_len));
    str.append(unpack_len_ptr, len_sizeof);
    str.append(unpack_info_.data(), unpack_len);

    size_t blob_len = encoded_blob_.size();
    const char *blob_len_ptr =
        static_cast<const char *>(static_cast<const void *>(&blob_len));

    str.append(blob_len_ptr, len_sizeof);
    str.append(encoded_blob_.data(), blob_len);
}

void EloqStringRecord::Deserialize(const char *buf, size_t &offset)
{
    size_t *len_ptr = (size_t *) (buf + offset);
    size_t len = *len_ptr;
    offset += sizeof(size_t);
    unpack_info_.clear();
    unpack_info_.reserve(len);
    std::copy(
        buf + offset, buf + offset + len, std::back_inserter(unpack_info_));
    offset += len;

    len_ptr = (size_t *) (buf + offset);
    len = *len_ptr;
    offset += sizeof(size_t);
    encoded_blob_.clear();
    encoded_blob_.reserve(len);
    std::copy(
        buf + offset, buf + offset + len, std::back_inserter(encoded_blob_));
    offset += len;
}

TxRecord::Uptr EloqStringRecord::Clone() const
{
    return std::make_unique<EloqStringRecord>(*this);
}

void EloqStringRecord::Copy(const TxRecord &rhs)
{
    const EloqStringRecord &typed_rhs =
        static_cast<const EloqStringRecord &>(rhs);
    encoded_blob_ = typed_rhs.encoded_blob_;
    unpack_info_ = typed_rhs.unpack_info_;
}

std::string EloqStringRecord::ToString() const
{
    return std::string(encoded_blob_.begin(), encoded_blob_.end());
}

size_t EloqStringRecord::SerializedLength() const
{
    // unpack_info_ and encoded_blob_ and their length
    return sizeof(size_t) * 2 + unpack_info_.size() + encoded_blob_.size();
}

size_t EloqStringRecord::MemUsage() const
{
    return sizeof(EloqStringRecord) + encoded_blob_.capacity() +
           unpack_info_.capacity();
}

size_t EloqStringRecord::Size() const
{
    return encoded_blob_.size();
}

bool EloqStringRecord::NeedsDefrag(mi_heap_t *heap)
{
    if (encoded_blob_.data() != nullptr)
    {
        float page_utilization =
            mi_heap_page_utilization(heap, encoded_blob_.data());
        if (page_utilization < 0.8)
        {
            return true;
        }
    }
    return false;
}

void EloqStringRecord::SetTTL(uint64_t ttl)
{
    // EloqStringRecord doesn't support TTL
    throw std::runtime_error("EloqStringRecord does not support TTL");
}

uint64_t EloqStringRecord::GetTTL() const
{
    // EloqStringRecord doesn't support TTL
    throw std::runtime_error("EloqStringRecord does not support TTL");
}

bool EloqStringRecord::HasTTL() const
{
    return false;
}

TxRecord::Uptr EloqStringRecord::AddTTL(uint64_t ttl)
{
    // EloqStringRecord doesn't support TTL conversion
    throw std::runtime_error(
        "EloqStringRecord does not support TTL conversion");
}

TxRecord::Uptr EloqStringRecord::RemoveTTL()
{
    // EloqStringRecord doesn't support TTL conversion
    throw std::runtime_error(
        "EloqStringRecord does not support TTL conversion");
}

void EloqStringRecord::SetUnpackInfo(const unsigned char *unpack_ptr,
                                     size_t unpack_size)
{
    unpack_info_.assign(
        reinterpret_cast<const char *>(unpack_ptr),
        reinterpret_cast<const char *>(unpack_ptr) + unpack_size);
}

void EloqStringRecord::SetEncodedBlob(const unsigned char *blob_ptr,
                                      size_t blob_size)
{
    encoded_blob_.assign(reinterpret_cast<const char *>(blob_ptr),
                         reinterpret_cast<const char *>(blob_ptr) + blob_size);
}

const char *EloqStringRecord::EncodedBlobData() const
{
    return encoded_blob_.data();
}

size_t EloqStringRecord::EncodedBlobSize() const
{
    return encoded_blob_.size();
}

const char *EloqStringRecord::UnpackInfoData() const
{
    return unpack_info_.data();
}

size_t EloqStringRecord::UnpackInfoSize() const
{
    return unpack_info_.size();
}

size_t EloqStringRecord::Length() const
{
    return encoded_blob_.size();
}

void EloqStringRecord::Prefetch() const
{
    // No prefetching needed for simple string record
}

}  // namespace txservice
