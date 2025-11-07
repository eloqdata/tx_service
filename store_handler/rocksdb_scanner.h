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

#include "kv_store.h"

#if (defined(ROCKSDB_CLOUD_FS_TYPE) &&                                         \
     (ROCKSDB_CLOUD_FS_TYPE == ROCKSDB_CLOUD_FS_TYPE_S3 ||                     \
      ROCKSDB_CLOUD_FS_TYPE == ROCKSDB_CLOUD_FS_TYPE_GCS))
#include "rocksdb/cloud/db_cloud.h"
#else
#include "rocksdb/db.h"
#endif

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "eloqkv_key.h"
#include "redis_object.h"
#include "rocksdb_config.h"
#include "tx_service/include/store/data_store_handler.h"

namespace EloqKV
{

class RocksDBScanner : public txservice::store::DataStoreScanner
{
public:
    RocksDBScanner(
        rocksdb::DB *db,
        rocksdb::ColumnFamilyHandle *cfh,
        const txservice::KeySchema *key_sch,
        const txservice::RecordSchema *rec_sch,
        const txservice::TableName &table_name,
        const txservice::KVCatalogInfo *kv_info,
        const EloqKV::EloqKey *start_key,
        bool inclusive,
        const std::vector<txservice::store::DataStoreSearchCond> &pushdown_cond,
        bool scan_forward,
        std::shared_lock<std::shared_mutex> db_lock)
        : db_(db),
          cfh_(cfh),
          key_sch_(key_sch),
          rec_sch_(rec_sch),
          table_name_(table_name),
          kv_info_(kv_info),
          start_key_(*start_key),
          inclusive_(inclusive),
          scan_forward_(scan_forward),
          pushdown_condition_(pushdown_cond),
          initialized_(false),
          db_lock_(std::move(db_lock)),
          iter_(nullptr),
          current_key_(nullptr),
          current_rec_(nullptr)
    {
        assert(table_name_.Type() == txservice::TableType::Primary ||
               table_name_.Type() == txservice::TableType::Secondary ||
               table_name_.Type() == txservice::TableType::UniqueSecondary);
    };

    void Current(txservice::TxKey &key,
                 const txservice::TxRecord *&rec,
                 uint64_t &version_ts,
                 bool &deleted_) override;

    bool MoveNext() override;

    void End() override;

    bool Init();

private:
    rocksdb::DB *db_;
    rocksdb::ColumnFamilyHandle *cfh_{nullptr};
    const std::string_view keyspace_name_v_;
    // primary key or secondary key schema
    const txservice::KeySchema *key_sch_;
    const txservice::RecordSchema *rec_sch_;
    const txservice::TableName
        table_name_;  // not string owner, sv -> TableSchema
    const txservice::KVCatalogInfo *kv_info_;
    const EloqKV::EloqKey start_key_;  // pk or (sk,pk)
    const bool inclusive_;
    bool scan_forward_;
    const std::vector<txservice::DataStoreSearchCond> pushdown_condition_;
    bool initialized_{false};

    std::shared_lock<std::shared_mutex> db_lock_;
    std::unique_ptr<rocksdb::Iterator> iter_{nullptr};
    std::unique_ptr<EloqKV::EloqKey> current_key_{nullptr};
    txservice::TxRecord::Uptr current_rec_{nullptr};
};
}  // namespace EloqKV
