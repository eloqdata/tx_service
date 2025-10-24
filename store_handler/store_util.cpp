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
#include "store_util.h"

#include <memory>
#include <string>
#include <vector>

namespace EloqShare
{

std::string uint32_str_converter(uint32_t val)
{
    return std::to_string(val);
}

std::string uint16_str_converter(uint16_t val)
{
    return std::to_string(val);
}

std::string string_str_converter(const std::string &val)
{
    return val;
}

std::string bool_str_converter(bool val)
{
    return val ? "1" : "0";
}

uint32_t str_uint32_converter(const std::string &s)
{
    return std::stoul(s);
}

uint16_t str_uint16_converter(const std::string &s)
{
    return std::stoul(s);
}

std::string str_string_converter(const std::string &s)
{
    return s;
}

bool str_bool_converter(const std::string &s)
{
    return s == "1";
}

template <>
void SerializeVector<std::string>(const std::vector<std::string> &vec,
                                  std::string &str)
{
    // The vector size
    size_t cnt = vec.size();
    size_t val_len = sizeof(cnt);
    const char *val_ptr = reinterpret_cast<const char *>(&cnt);
    str.append(val_ptr, val_len);

    // The vector content
    val_len = sizeof(size_t);
    for (size_t i = 0; i < vec.size(); ++i)
    {
        // string size
        size_t str_len = vec[i].size();
        val_ptr = reinterpret_cast<const char *>(&str_len);
        str.append(val_ptr, val_len);
        // string content
        str.append(vec[i].data(), str_len);
    }
}

template <>
void DeserializeToVector<std::string>(const char *str,
                                      size_t length,
                                      std::vector<std::string> &vec)
{
    if (!length)
    {
        return;
    }

    size_t offset = 0;
    // The vector size.
    size_t vec_size = *(reinterpret_cast<const size_t *>(str + offset));
    offset += sizeof(size_t);

    // The vector content
    for (size_t i = 0; i < vec_size; ++i)
    {
        // string size
        size_t str_len = *(reinterpret_cast<const size_t *>(str + offset));
        offset += sizeof(str_len);
        // string content
        vec.emplace_back((str + offset), str_len);
        offset += str_len;
    }

    assert(offset == length);
}

// void SerializeFlushRecord(const txservice::FlushRecord &flush_rec,
//                           std::vector<char> &buf)
// {
//     if (flush_rec.payload_status_ != txservice::RecordStatus::Deleted)
//     {
//         int64_t commit_ts = flush_rec.commit_ts_;
//         int8_t deleted = 0;

//         const txservice::BlobTxRecord *rec =
//             dynamic_cast<const txservice::BlobTxRecord
//             *>(flush_rec.Payload());
//         assert(rec != nullptr);
//         buf.resize(sizeof(int8_t) + sizeof(int64_t) + rec->value_.size());
//         char *p = buf.data();

//         // encode deleted
//         std::memcpy(p, &deleted, sizeof(int8_t));
//         p += sizeof(int8_t);
//         // encode version
//         std::memcpy(p, &commit_ts, sizeof(int64_t));
//         p += sizeof(int64_t);
//         std::memcpy(p, rec->value_.c_str(), rec->value_.size());
//     }
//     else
//     {
//         buf.resize(sizeof(int8_t) + sizeof(int64_t));
//         char *p = buf.data();
//         int64_t commit_ts = flush_rec.commit_ts_;
//         int8_t deleted = 1;

//         // encode deleted
//         std::memcpy(p, &deleted, sizeof(int8_t));
//         p += sizeof(int8_t);
//         // encode version
//         std::memcpy(p, &commit_ts, sizeof(int64_t));
//         p += sizeof(int64_t);
//     }
// }

// void SerializeFlushRecord(const txservice::FlushRecord &flush_rec,
//                           std::string &rec_str)
// {
//     if (flush_rec.payload_status_ != txservice::RecordStatus::Deleted)
//     {
//         const txservice::BlobTxRecord *rec =
//             dynamic_cast<const txservice::BlobTxRecord
//             *>(flush_rec.Payload());
//         assert(rec != nullptr);

//         rec_str.resize(sizeof(int8_t) + sizeof(int64_t) +
//         rec->value_.size()); char *p = rec_str.data();

//         int8_t deleted = 0;
//         int64_t commit_ts = flush_rec.commit_ts_;

//         std::memcpy(p, &deleted, sizeof(int8_t));
//         p += sizeof(int8_t);
//         std::memcpy(p, &commit_ts, sizeof(int64_t));
//         p += sizeof(int64_t);
//         std::memcpy(p, rec->value_.c_str(), rec->value_.size());
//     }
//     else
//     {
//         rec_str.resize(sizeof(int8_t) + sizeof(int64_t));
//         char *p = rec_str.data();

//         int8_t deleted = 1;
//         int64_t commit_ts = flush_rec.commit_ts_;

//         std::memcpy(p, &deleted, sizeof(int8_t));
//         p += sizeof(int8_t);
//         std::memcpy(p, &commit_ts, sizeof(int64_t));
//     }
// }

void DeserializeRecord(const char *payload,
                       const size_t payload_size,
                       std::string &rec_str,
                       bool &is_deleted,
                       int64_t &version_ts)
{
    assert(payload_size >= (sizeof(int8_t) + sizeof(int64_t)));
    const char *p = payload;
    int8_t deleted = 0;
    std::memcpy(&deleted, p, sizeof(int8_t));
    p += sizeof(int8_t);
    int64_t version = 0;
    std::memcpy(&version, p, sizeof(int64_t));
    p += sizeof(int64_t);
    version_ts = version;
    if (deleted == 0)
    {
        is_deleted = false;
        rec_str =
            std::string(p, payload_size - sizeof(int8_t) - sizeof(int64_t));
    }
    else
    {
        is_deleted = true;
    }
}

}  // namespace EloqShare
