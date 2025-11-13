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

#include <memory>
#include <string>

#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
struct RawSliceDataItem
{
    RawSliceDataItem() = delete;

    RawSliceDataItem(std::string &&key_str,
                     std::string &&rec_str,
                     uint64_t version_ts,
                     bool is_deleted)
        : key_str_(std::move(key_str)),
          rec_str_(std::move(rec_str)),
          version_ts_(version_ts),
          is_deleted_(is_deleted)
    {
    }

    std::string key_str_;
    std::string rec_str_;
    uint64_t version_ts_;
    bool is_deleted_;
};
struct SliceDataItem
{
    SliceDataItem() = delete;

    SliceDataItem(txservice::TxKey key,
                  std::unique_ptr<txservice::TxRecord> &&rec,
                  uint64_t version_ts,
                  bool is_deleted)
        : key_(std::move(key)),
          record_(std::move(rec)),
          version_ts_(version_ts),
          is_deleted_(is_deleted)
    {
    }

    txservice::TxKey key_;
    std::unique_ptr<txservice::TxRecord> record_;
    uint64_t version_ts_;
    bool is_deleted_;
};

}  // namespace txservice
