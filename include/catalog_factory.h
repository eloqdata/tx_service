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
#include <unordered_map>
#include <vector>

#include "cc/cc_map.h"
#include "cc/ccm_scanner.h"  // CcScanner
#include "schema.h"
#include "tx_command.h"

namespace txservice
{
struct TableRangeEntry;
class StoreRange;

struct KVCatalogInfo
{
    using uptr = std::unique_ptr<KVCatalogInfo>;

    KVCatalogInfo() = default;
    virtual ~KVCatalogInfo() = default;
    virtual std::string Serialize() const
    {
        std::string str;
        size_t len_sizeof = sizeof(uint32_t);
        uint32_t len_val = static_cast<uint32_t>(kv_table_name_.size());
        char *len_ptr = reinterpret_cast<char *>(&len_val);
        str.append(len_ptr, len_sizeof);
        str.append(kv_table_name_.data(), len_val);

        std::string index_names;
        if (kv_index_names_.size() != 0)
        {
            for (auto it = kv_index_names_.cbegin();
                 it != kv_index_names_.cend();
                 ++it)
            {
                index_names.append(it->first.StringView())
                    .append(" ")
                    .append(it->second)
                    .append(" ")
                    .append(1, static_cast<char>(it->first.Engine()))
                    .append(" ");
            }
            // index_names.substr(0, index_names.size() - 1);
            index_names.erase(index_names.size() - 1);
        }
        else
        {
            index_names.clear();
        }
        len_val = static_cast<uint32_t>(index_names.size());
        str.append(len_ptr, len_sizeof);
        str.append(index_names.data(), len_val);

        return str;
    }

    virtual void Deserialize(const char *buf, size_t &offset)
    {
        if (buf[0] == '\0')
        {
            return;
        }
        const uint32_t *len_ptr =
            reinterpret_cast<const uint32_t *>(buf + offset);
        uint32_t len_val = *len_ptr;
        offset += sizeof(uint32_t);

        kv_table_name_ = std::string(buf + offset, len_val);
        offset += len_val;

        len_ptr = reinterpret_cast<const uint32_t *>(buf + offset);
        len_val = *len_ptr;
        offset += sizeof(uint32_t);
        if (len_val != 0)
        {
            std::string index_names(buf + offset, len_val);
            offset += len_val;
            std::stringstream ss(index_names);
            std::istream_iterator<std::string> begin(ss);
            std::istream_iterator<std::string> end;
            std::vector<std::string> tokens(begin, end);
            for (auto it = tokens.begin(); it != tokens.end(); ++it)
            {
                TableType table_type = TableName::Type(*it);
                assert(table_type == txservice::TableType::Secondary ||
                       table_type == txservice::TableType::UniqueSecondary);

                const std::string &index_name_str = *it;
                const std::string &kv_index_name_str = *(++it);
                const std::string &table_engine_str = *(++it);
                assert(table_engine_str.size() == 1);

                txservice::TableEngine table_engine =
                    static_cast<txservice::TableEngine>(table_engine_str[0]);
                txservice::TableName index_table_name(
                    index_name_str, table_type, table_engine);
                kv_index_names_.emplace(index_table_name, kv_index_name_str);
            }
        }
        else
        {
            kv_index_names_.clear();
        }
    }

    virtual const std::string &GetKvTableName(const TableName &table_name) const
    {
        const TableType table_type = table_name.Type();
        assert(table_type == TableType::Primary ||
               table_type == TableType::Secondary ||
               table_type == TableType::UniqueSecondary);
        if (table_type == TableType::Primary)
        {
            return kv_table_name_;
        }
        else
        {
            return kv_index_names_.at(table_name);
        }
    }

    std::string kv_table_name_;
    // map of <mysql_index_table_name, kv_index_table_name>
    std::unordered_map<TableName, std::string> kv_index_names_;
};

class Statistics;

struct WriteEntry
{
    WriteEntry() = delete;
    WriteEntry(TxKey key, TxRecord::Uptr rec, uint64_t commit_ts)
        : key_(std::move(key)), rec_(std::move(rec)), commit_ts_(commit_ts)
    {
    }

    WriteEntry(const WriteEntry &rhs) = delete;
    WriteEntry(WriteEntry &&rhs)
    {
        key_ = std::move(rhs.key_);
        rec_ = std::move(rhs.rec_);
        commit_ts_ = rhs.commit_ts_;
    }

    WriteEntry &operator=(WriteEntry &&rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        key_ = std::move(rhs.key_);
        rec_ = std::move(rhs.rec_);
        commit_ts_ = rhs.commit_ts_;

        return *this;
    }

    TxKey key_;
    TxRecord::Uptr rec_;
    uint64_t commit_ts_;
};

struct SkEncoder
{
    using uptr = std::unique_ptr<SkEncoder>;

    virtual ~SkEncoder() = default;

    /**
     * @brief Generate packed secondary key(s) using the primary key and record.
     * @note This is a non-const method. Subclasses should call SetError() to
     * store any errors encountered during generation and ClearError() to reset
     * the error state.
     * @param pk The primary key.
     * @param record The record data.
     * @param version_ts The version timestamp for the new secondary key
     * entries.
     * @param dest_vec The vector to which generated secondary key WriteEntry
     * objects will be appended.
     * @return An `int32_t`:
     *         - A non-negative value representing the number of secondary key
     *           `WriteEntry` objects appended to `dest_vec` if the key
     *           generation was successful. This count can be zero if no
     *           secondary keys were generated for the given record (e.g.,
     *           MongoDB partial or sparse indexes). It can also be greater
     *           than one if the record generates multiple secondary keys
     *           (e.g., MongoDB multikey indexes on array fields).
     *         - `-1` if the key generation failed (check `GetError()` for
     *           details on the failure).
     */
    virtual int32_t AppendPackedSk(const TxKey *pk,
                                   const TxRecord *record,
                                   uint64_t version_ts,
                                   std::vector<WriteEntry> &dest_vec) = 0;

    virtual void Reset()
    {
        ClearError();
    }

    virtual bool IsMultiKey() const
    {
        return false;
    }

    virtual const txservice::MultiKeyPaths *MultiKeyPaths() const
    {
        return nullptr;
    }

    virtual std::string SerializeMultiKeyPaths() const
    {
        return "";
    }

    const PackSkError &GetError() const
    {
        return err_;
    }

    PackSkError &GetError()
    {
        return err_;
    }

protected:
    void SetError(int32_t code, std::string message)
    {
        err_.code_ = code;
        err_.message_ = std::move(message);
    }

    void ClearError()
    {
        err_.Reset();
    }

private:
    PackSkError err_;
};

struct TableSchema
{
    using uptr = std::unique_ptr<TableSchema>;

    virtual ~TableSchema() = default;
    virtual TableSchema::uptr Clone() const = 0;
    virtual const TableName &GetBaseTableName() const = 0;
    virtual const txservice::KeySchema *KeySchema() const = 0;
    virtual const txservice::RecordSchema *RecordSchema() const = 0;
    virtual const std::string &SchemaImage() const = 0;
    virtual const std::unordered_map<
        uint16_t,
        std::pair<txservice::TableName, txservice::SecondaryKeySchema>>
        *GetIndexes() const = 0;
    virtual KVCatalogInfo *GetKVCatalogInfo() const = 0;
    virtual void SetKVCatalogInfo(const std::string &kv_info_str) = 0;
    virtual uint64_t Version() const = 0;
    virtual std::string_view VersionStringView() const = 0;
    virtual std::vector<TableName> IndexNames() const = 0;
    virtual size_t IndexesSize() const = 0;
    virtual const SecondaryKeySchema *IndexKeySchema(
        const TableName &index_name) const = 0;
    virtual uint16_t IndexOffset(const TableName &index_name) const = 0;
    virtual void BindStatistics(std::shared_ptr<Statistics> statistics) = 0;
    virtual std::shared_ptr<Statistics> StatisticsObject() const = 0;

    /**
     * Create TxCommand from serialized command image. Used when processing
     * remote command request and replaying command log. This function is
     * necessary as the command type and object type is unknown in tx_service
     * layer.
     * @param cmd_image
     * @return
     */
    virtual std::unique_ptr<TxCommand> CreateTxCommand(
        std::string_view cmd_image) const
    {
        return nullptr;
    }

    virtual SkEncoder::uptr CreateSkEncoder(const TableName &index_name) const
    {
        return nullptr;
    }

    virtual bool HasAutoIncrement() const = 0;
    virtual const TableName *GetSequenceTableName() const = 0;
    virtual std::pair<TxKey, TxRecord::Uptr> GetSequenceKeyAndInitRecord(
        const TableName &table_name) const = 0;

    // Rebuild table schema image by setting multikey.
    virtual void IndexesSetMultiKeyAttr(
        const std::vector<MultiKeyAttr> &indexes){};
};

class CatalogFactory
{
public:
    CatalogFactory() = default;
    virtual ~CatalogFactory() = default;

    virtual TableSchema::uptr CreateTableSchema(
        const TableName &table_name,
        const std::string &catalog_image,
        uint64_t version) = 0;

    virtual CcMap::uptr CreatePkCcMap(const TableName &table_name,
                                      const TableSchema *table_schema,
                                      bool ccm_has_full_entries,
                                      CcShard *shard,
                                      NodeGroupId cc_ng_id) = 0;

    virtual CcMap::uptr CreateSkCcMap(const TableName &table_name,
                                      const TableSchema *table_schema,
                                      CcShard *shard,
                                      NodeGroupId cc_ng_id) = 0;

    virtual CcMap::uptr CreateRangeMap(const TableName &range_table_name,
                                       const TableSchema *table_schema,
                                       uint64_t schema_ts,
                                       CcShard *shard,
                                       NodeGroupId ng_id) = 0;

    virtual std::unique_ptr<TableRangeEntry> CreateTableRange(
        TxKey start_key,
        uint64_t version_ts,
        int64_t partition_id,
        std::unique_ptr<StoreRange> slices = nullptr) = 0;

    virtual std::unique_ptr<CcScanner> CreatePkCcmScanner(
        ScanDirection direction, const KeySchema *key_schema) = 0;

    virtual std::unique_ptr<CcScanner> CreateSkCcmScanner(
        ScanDirection direction, const KeySchema *compound_key_schema) = 0;

    virtual std::unique_ptr<CcScanner> CreateRangeCcmScanner(
        ScanDirection direction,
        const KeySchema *key_schema,
        const TableName &range_table_name) = 0;

    virtual std::unique_ptr<Statistics> CreateTableStatistics(
        const TableSchema *table_schema, NodeGroupId cc_ng_id) = 0;

    /**
     * @param sample_pool_map is declared as value-type. Copy won't happen if it
     * is a right-value since C++11.
     */
    virtual std::unique_ptr<Statistics> CreateTableStatistics(
        const TableSchema *table_schema,
        std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
            sample_pool_map,
        CcShard *ccs,
        NodeGroupId cc_ng_id) = 0;

    virtual TxKey NegativeInfKey() = 0;
    virtual TxKey PositiveInfKey() = 0;
    virtual size_t KeyHash(const char *buf,
                           size_t offset,
                           const txservice::KeySchema *key_schema) const
    {
        return 0;
    }
};
}  // namespace txservice
