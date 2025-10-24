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

#include <cassert>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include "tx_key.h"
#include "tx_record.h"

#define ROCKSDB_CLOUD_FS_TYPE_S3 1
#define ROCKSDB_CLOUD_FS_TYPE_GCS 2

namespace EloqDS
{
template <typename KeyT, typename ValueT>
struct ScanHeapTuple
{
    ScanHeapTuple() = delete;

    ScanHeapTuple(uint32_t shard_id)
        : key_(std::make_unique<KeyT>()),
          rec_(std::make_unique<ValueT>()),
          sid_(shard_id)
    {
    }

    std::unique_ptr<KeyT> key_;
    std::unique_ptr<ValueT> rec_;
    uint64_t version_ts_;
    bool deleted_;
    // sid_ is the offset in shard_scan_XX vectors.
    uint32_t sid_;
};

template <>
struct ScanHeapTuple<txservice::TxKey, txservice::TxRecord>
{
    ScanHeapTuple() = delete;

    explicit ScanHeapTuple(uint32_t shard_id,
                           std::unique_ptr<txservice::TxKey> key,
                           std::unique_ptr<txservice::TxRecord> rec)
        : key_(std::move(key)),
          rec_(std::move(rec)),
          version_ts_(0),
          deleted_(false),
          sid_(shard_id)
    {
    }

    std::unique_ptr<txservice::TxKey> key_;
    std::unique_ptr<txservice::TxRecord> rec_;
    uint64_t version_ts_;
    bool deleted_;
    // sid_ is the offset in shard_scan_XX vectors.
    uint32_t sid_;
};

template <typename KeyT, typename ValueT>
struct CacheCompare
{
    bool operator()(const ScanHeapTuple<KeyT, ValueT> &lhs,
                    const ScanHeapTuple<KeyT, ValueT> &rhs)
    {
        return !(*lhs.key_ < *rhs.key_);
    }
};

template <typename KeyT, typename ValueT>
struct CacheReverseCompare
{
    bool operator()(const ScanHeapTuple<KeyT, ValueT> &lhs,
                    const ScanHeapTuple<KeyT, ValueT> &rhs)
    {
        return *lhs.key_ < *rhs.key_;
    }
};

static inline std::string SerializeSchemaImage(
    const std::string &frm,
    const std::string &kv_info,
    const std::string &key_schemas_ts)
{
    size_t len = frm.length();
    std::string res;

    res.append(reinterpret_cast<const char *>(&len), sizeof(len));
    res.append(frm.data(), frm.length());
    len = kv_info.length();
    res.append(reinterpret_cast<const char *>(&len), sizeof(len));
    res.append(kv_info.data(), kv_info.length());
    len = key_schemas_ts.length();
    res.append(reinterpret_cast<const char *>(&len), sizeof(len));
    res.append(key_schemas_ts.data(), key_schemas_ts.length());

    return res;
}

static inline void DeserializeSchemaImage(const std::string &image,
                                          std::string &frm,
                                          std::string &kv_info,
                                          std::string &key_schemas_ts)
{
    size_t offset = 0;
    const char *buf = image.data();
    size_t len_val;
    len_val = *(size_t *) (buf + offset);
    offset += sizeof(len_val);
    frm.append(buf + offset, len_val);
    offset += len_val;

    len_val = *(size_t *) (buf + offset);
    offset += sizeof(len_val);
    kv_info.append(buf + offset, len_val);
    offset += len_val;

    len_val = *(size_t *) (buf + offset);
    offset += sizeof(len_val);
    key_schemas_ts.append(buf + offset, len_val);
    offset += len_val;

    assert(offset == image.length());
}

static inline std::string stringToHex(const std::string &input)
{
    std::ostringstream oss;
    for (unsigned char c : input)
    {
        oss << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(c);
    }
    return oss.str();
}
static inline std::string stringToHex(const std::string_view &input)
{
    std::ostringstream oss;
    for (unsigned char c : input)
    {
        oss << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(c);
    }
    return oss.str();
}
}  // namespace EloqDS
