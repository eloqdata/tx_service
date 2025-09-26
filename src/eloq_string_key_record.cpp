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
    size_t unpack_info_size = unpack_info_.size();
    size_t encoded_blob_size = encoded_blob_.size();
    size_t total_size =
        sizeof(size_t) * 2 + unpack_info_size + encoded_blob_size;

    if (buf.capacity() - offset < total_size)
    {
        buf.resize(offset + total_size);
    }

    // Serialize unpack info size and unpack info
    const char *size_ptr = reinterpret_cast<const char *>(&unpack_info_size);
    std::copy(size_ptr, size_ptr + sizeof(size_t), buf.begin() + offset);
    offset += sizeof(size_t);

    if (unpack_info_size > 0)
    {
        std::copy(
            unpack_info_.begin(), unpack_info_.end(), buf.begin() + offset);
        offset += unpack_info_size;
    }

    // Serialize encoded blob size and encoded blob
    size_ptr = reinterpret_cast<const char *>(&encoded_blob_size);
    std::copy(size_ptr, size_ptr + sizeof(size_t), buf.begin() + offset);
    offset += sizeof(size_t);

    if (encoded_blob_size > 0)
    {
        std::copy(
            encoded_blob_.begin(), encoded_blob_.end(), buf.begin() + offset);
        offset += encoded_blob_size;
    }
}

void EloqStringRecord::Serialize(std::string &str) const
{
    size_t blob_size = unpack_info_.size();
    // Serialize unpack info size
    const char *size_ptr = reinterpret_cast<const char *>(&blob_size);
    str.append(size_ptr, sizeof(size_t));

    // Serialize unpack info
    if (blob_size > 0)
    {
        str.append(unpack_info_.data(), blob_size);
    }

    blob_size = encoded_blob_.size();
    // Serialize encoded blob size
    size_ptr = reinterpret_cast<const char *>(&blob_size);
    str.append(size_ptr, sizeof(size_t));

    // Serialize encoded blob
    if (blob_size > 0)
    {
        str.append(encoded_blob_.data(), blob_size);
    }
}

void EloqStringRecord::Deserialize(const char *buf, size_t &offset)
{
    // Deserialize unpack info size and unpack info
    const size_t *size_ptr = reinterpret_cast<const size_t *>(buf + offset);
    size_t len = *size_ptr;
    offset += sizeof(size_t);

    if (len > 0)
    {
        unpack_info_.assign(buf + offset, buf + offset + len);
        offset += len;
    }
    else
    {
        unpack_info_.clear();
    }

    // Deserialize encoded blob size
    size_ptr = reinterpret_cast<const size_t *>(buf + offset);
    len = *size_ptr;
    offset += sizeof(size_t);

    // Deserialize encoded blob
    if (len > 0)
    {
        encoded_blob_.assign(buf + offset, buf + offset + len);
        offset += len;
    }
    else
    {
        encoded_blob_.clear();
    }
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
}

std::string EloqStringRecord::ToString() const
{
    return std::string(encoded_blob_.begin(), encoded_blob_.end());
}

size_t EloqStringRecord::SerializedLength() const
{
    return sizeof(uint32_t) + encoded_blob_.size();
}

size_t EloqStringRecord::MemUsage() const
{
    return sizeof(EloqStringRecord) + encoded_blob_.size();
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
