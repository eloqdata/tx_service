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
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "catalog_factory.h"
#include "reader_writer_cntl.h"
#include "tx_key.h"
#include "tx_record.h"

#include "sharder.h"

namespace txservice
{

struct DataSyncTask;

/**
 * @brief A special type of tx keys for concurrency control (cc) maps of table
 * catalogs.
 *
 */
struct CatalogKey
{
public:
    CatalogKey();
    CatalogKey(const TableName &name);
    CatalogKey(CatalogKey &&rhs) noexcept;
    CatalogKey(const CatalogKey &rhs);
    CatalogKey &operator=(CatalogKey &&) = default;
    ~CatalogKey() = default;
    CatalogKey(const CatalogKey &rhs, const KeySchema *);

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
    void Copy(const CatalogKey &rhs);

    KeyType Type() const
    {
        return KeyType::Normal;
    }

    // CatalogCcMap is not need to be defragmented, so this method just need
    // for satisting compling
    bool NeedsDefrag(mi_heap_t *heap)
    {
        return false;
    }

    friend bool operator==(const CatalogKey &lhs, const CatalogKey &rhs);
    friend bool operator!=(const CatalogKey &lhs, const CatalogKey &rhs);
    friend bool operator<(const CatalogKey &lhs, const CatalogKey &rhs);
    friend bool operator<=(const CatalogKey &lhs, const CatalogKey &rhs);
    const TableName &Name() const;
    TableName &Name();

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
        return table_name_.StringView().size();
    }

    size_t MemUsage() const
    {
        return Size();
    }

    static const CatalogKey *NegativeInfinity()
    {
        static const CatalogKey neg_inf;
        return &neg_inf;
    }

    static const CatalogKey *PositiveInfinity()
    {
        static const CatalogKey pos_inf;
        return &pos_inf;
    }

    static const TxKeyInterface *TxKeyImpl()
    {
        static const TxKeyInterface tx_key_impl{CatalogKey()};
        return &tx_key_impl;
    }

private:
    // table_name_ is string owner if the CatalogKey is stored in CcMap. When
    // the CatalogKey is constructed to perform a lookup(most of the time), it
    // should use string_view.
    TableName table_name_;
};

/**
 * @brief Owner of current and dirty schema on one node. Mutilple ccshards will
 * have their own copies of CatalogRecord in catolog_cc_map. Their current and
 * dirty schema point to the corresponding CatalogEntry.
 *
 * Note that it's possible that different shard's have different current schema
 * or dirty schema, since CreateDirty() and CommitDirty() steps happen in
 * parallel. But there are at most two schema on a single node, which is
 * described by CatalogEntry.
 *
 */
struct CatalogEntry
{
    CatalogEntry() = default;

    ~CatalogEntry() = default;

    void InitSchema(std::unique_ptr<TableSchema> schema, uint64_t version_ts)
    {
        std::unique_lock<std::shared_mutex> lk(s_mux_);
        assert(version_ts > 0);

        if (schema_version_ < version_ts)
        {
            schema_ = std::move(schema);
            schema_version_ = version_ts;
        }
        if (dirty_schema_version_ <= version_ts)
        {
            dirty_schema_ = nullptr;
            dirty_schema_version_ = 0;
        }
    }

    void SetDirtySchema(std::unique_ptr<TableSchema> dirty_schema,
                        uint64_t dirty_version_ts)
    {
        std::unique_lock<std::shared_mutex> lk(s_mux_);
        if (dirty_version_ts >= dirty_schema_version_ &&
            dirty_version_ts > schema_version_)
        {
            dirty_schema_ = std::move(dirty_schema);
            dirty_schema_version_ = dirty_version_ts;
        }
    }

    void CommitDirtySchema()
    {
        std::unique_lock<std::shared_mutex> lk(s_mux_);
        if (dirty_schema_version_ > schema_version_)
        {
            schema_ = std::move(dirty_schema_);
            schema_version_ = dirty_schema_version_;
        }
        else
        {
            dirty_schema_ = nullptr;
        }
        dirty_schema_version_ = 0;
    }

    void RejectDirtySchema()
    {
        std::unique_lock<std::shared_mutex> lk(s_mux_);
        dirty_schema_.reset();
        dirty_schema_version_ = 0;
    }

    // FIXME(lokax):
    std::vector<uint64_t> last_sync_ts_;

    uint64_t GetMinLastSyncTs()
    {
        std::shared_lock<std::shared_mutex> lk(s_mux_);
        if (last_sync_ts_.empty())
        {
            return 0;
        }

        uint64_t ts = UINT64_MAX;
        for (const auto &last_ts : last_sync_ts_)
        {
            ts = std::min(ts, last_ts);
        }

        return ts;
    }

    uint64_t GetLastSyncTs(size_t worker_idx)
    {
        std::shared_lock<std::shared_mutex> lk(s_mux_);
        if (last_sync_ts_.empty())
        {
            return 0;
        }

        return last_sync_ts_[worker_idx];
    }

    void UpdateLastDataSyncTS(uint64_t last_sync_ts, size_t worker_idx)
    {
        std::unique_lock<std::shared_mutex> lk(s_mux_);

        if (last_sync_ts_.empty())
        {
            size_t core_cnt = Sharder::Instance().GetLocalCcShardsCount();
            last_sync_ts_.resize(core_cnt, 0);
        }

        if (last_sync_ts > last_sync_ts_[worker_idx])
        {
            // data sync succeeded, update last sync ts
            last_sync_ts_[worker_idx] = last_sync_ts;
        }
    }


    std::shared_ptr<TableSchema> schema_{nullptr};
    std::shared_ptr<TableSchema> dirty_schema_{nullptr};
    uint64_t schema_version_{0};
    uint64_t dirty_schema_version_{0};

    std::shared_mutex s_mux_;
};

/**
 * @brief A special type of records for cc maps of table catalogs. A catalog
 * record contains either (1) a serialized representation of the catalog, or (2)
 * a pointer to the catalog instance at this node (in LocalCcShards). The former
 * is used when instantiating a table catalog instance at this node and creating
 * the corresponding cc map(s). The latter provides fast access to a view of the
 * current and dirty schemas for online tx's. A view of the dirty schema is
 * necessary for certain schema operations, e.g., creating secondary indexes
 * while not blocking online tx's.
 *
 */
struct CatalogRecord : public TxRecord
{
public:
    CatalogRecord() = default;
    CatalogRecord(CatalogRecord &&rhs) noexcept;
    CatalogRecord(const CatalogRecord &rhs);
    ~CatalogRecord() = default;

    void Serialize(std::vector<char> &buf, size_t &offset) const override;
    void Serialize(std::string &str) const override;
    size_t SerializedLength() const override;
    void Deserialize(const char *buf, size_t &offset) override;
    TxRecord::Uptr Clone() const override;
    void Copy(const TxRecord &rhs) override;
    std::string ToString() const override;

    void Set(const std::shared_ptr<TableSchema> &schema,
             const std::shared_ptr<TableSchema> &dirty_schema,
             uint64_t schema_ts);
    const std::string &SchemaImage() const;
    void SetSchemaImage(std::string &&schema_image);
    void SetSchemaImage(const std::string &schema_image);
    const std::string &DirtySchemaImage() const;
    void SetDirtySchemaImage(std::string &&schema_image);
    void SetDirtySchemaImage(const std::string &schema_image);
    const TableSchema *Schema() const;
    std::shared_ptr<const TableSchema> CopySchema() const;
    const TableSchema *DirtySchema() const;
    std::shared_ptr<const TableSchema> CopyDirtySchema() const;
    void ClearDirtySchema();
    void Reset();
    uint64_t SchemaTs() const;

    CatalogRecord &operator=(const CatalogRecord &rhs);
    CatalogRecord &operator=(CatalogRecord &&rhs) noexcept;

    size_t Size() const override
    {
        return 8 * 3;
    }

    size_t MemUsage() const override
    {
        // Most C++ implementations provide SSO, Small String Optimization.
        // Based on libstdc++, basic_string<char> has a local capacity of 15,
        // plus one more byte to store '\0', i.e. strings shorter than 16 are
        // stored directly in the String object, only strings longer than 16
        // have an allocated buffer to store the contents.
        size_t mem_usage = sizeof(CatalogRecord);
        if (schema_image_.capacity() >= 16)
        {
            mem_usage += schema_image_.capacity();
        }
        if (dirty_schema_image_.capacity() >= 16)
        {
            mem_usage += dirty_schema_image_.capacity();
        }
        return mem_usage;
    }

    void SetSchemaCntl(std::shared_ptr<ReaderWriterObject<TableSchema>> cntl)
    {
        cntl_ = cntl;
    }

    std::shared_ptr<ReaderWriterObject<TableSchema>> GetSchemaCntl() const
    {
        return cntl_;
    }

private:
    /**
     * @brief The CatalogRecord serves three purposes:
     * (1) a tx looks up a table's schema. schema_ points to the current schema
     * and dirty_schema_ points to the dirty schema of the table. Allowing
     * ongoing tx's to see the dirty schema is crucial to make schema operations
     * non-blocking. (2) Initialization of a table's catalog at a node reads the
     * table's schema from the data store and instantiates the schema instance
     * in memory. The schema_image_ in this case contains serialized images of
     * the schema. (3) A tx modifies a table's schema and uses the catalog
     * record to install a dirty version of the schema in the tx service. The
     * schema_image_ is the binary image of the current schema and
     * dirty_schema_image_ is the binary image of the new schema.
     *
     */
    std::shared_ptr<const TableSchema> schema_{nullptr};
    std::shared_ptr<const TableSchema> dirty_schema_{nullptr};
    uint64_t schema_ts_{0};
    std::string schema_image_{""};
    std::string dirty_schema_image_{""};
    std::shared_ptr<ReaderWriterObject<TableSchema>> cntl_{nullptr};
};
}  // namespace txservice
