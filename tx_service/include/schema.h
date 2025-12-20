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

#include <stdint.h>

#include <memory>
#include <string>
#include <utility>

#include "glog/logging.h"
#include "type.h"

namespace txservice
{
class TxKey;
struct TxRecord;

struct KeySchema;
struct RecordSchema
{
public:
    using Uptr = std::unique_ptr<RecordSchema>;
    virtual ~RecordSchema() = default;

    virtual int AutoIncrementIndex() const
    {
        // The index for auto increment field. -1 if not exist,
        return -1;
    }
};

struct MultiKeyPaths
{
    using Uptr = std::unique_ptr<MultiKeyPaths>;
    virtual ~MultiKeyPaths() = default;

    // Prototype Pattern. KeySchema or SkEncoder returns a pointer to the
    // abstract class, which can be used to create a new instance.
    virtual MultiKeyPaths::Uptr Clone() const = 0;

    virtual std::string Serialize(const KeySchema *key_schema) const = 0;
    virtual bool Deserialize(const KeySchema *key_schema,
                             const std::string &str) = 0;
    virtual bool Contain(const MultiKeyPaths &rhs) const = 0;
    virtual bool MergeWith(const MultiKeyPaths &rhs) = 0;
};

struct KeySchema
{
    using Uptr = std::unique_ptr<KeySchema>;
    virtual ~KeySchema() = default;
    virtual bool CompareKeys(const TxKey &key1,
                             const TxKey &key2,
                             size_t *const column_index) const = 0;

    virtual uint16_t ExtendKeyParts() const = 0;
    virtual uint64_t SchemaTs() const = 0;

    virtual bool IsMultiKey() const
    {
        return false;
    }

    /**
     * @return Return an valid object if the index type support multikey index.
     * @return Return nullptr if the index type doesn't support multikey index.
     */
    virtual const txservice::MultiKeyPaths *MultiKeyPaths() const
    {
        return nullptr;
    };
};

struct SecondaryKeySchema : public KeySchema
{
public:
    SecondaryKeySchema() = delete;

    SecondaryKeySchema(std::unique_ptr<const KeySchema> sk_sch,
                       const KeySchema *pk_sch)
        : sk_schema_(static_cast<const KeySchema *>(sk_sch.release())),
          pk_schema_(static_cast<const KeySchema *>(pk_sch))
    {
    }

    bool CompareKeys(const TxKey &key1,
                     const TxKey &key2,
                     size_t *const column_index) const override
    {
        return sk_schema_->CompareKeys(key1, key2, column_index);
    }

    // Number of key parts in the index (including "index extension")
    uint16_t ExtendKeyParts() const override
    {
        return sk_schema_->ExtendKeyParts();
    }

    uint64_t SchemaTs() const override
    {
        return sk_schema_->SchemaTs();
    }

    std::unique_ptr<const KeySchema> sk_schema_;
    const KeySchema *pk_schema_{nullptr};
};

struct TableKeySchemaTs
{
    explicit TableKeySchemaTs(TableEngine table_engine)
        : table_engine_(table_engine)
    {
    }

    TableKeySchemaTs(const std::string &key_schemas_ts_str,
                     TableEngine table_engine)
    {
        table_engine_ = table_engine;
        // This constructor is used with the old space-delimited format for
        // backward compatibility Parse the serialized format:
        // [pk_ts_len][pk_ts_string][index_data_len][index_data]
        size_t offset = 0;
        const char *buf = key_schemas_ts_str.data();

        // Read pk schema ts
        size_t pk_ts_len = *(size_t *) (buf + offset);
        offset += sizeof(pk_ts_len);
        std::string pk_ts_str(buf + offset, pk_ts_len);
        offset += pk_ts_len;
        pk_schema_ts_ = std::stoull(pk_ts_str);

        // Read index data length
        size_t index_data_len = *(size_t *) (buf + offset);
        offset += sizeof(index_data_len);

        if (index_data_len != 0)
        {
            // Deserialize length-prefixed format: for each index:
            // [table_name_len][table_name][ts_len][ts_string]
            size_t current_offset = offset;
            while (current_offset < offset + index_data_len)
            {
                // Read table name length and value
                size_t table_name_len = *(size_t *) (buf + current_offset);
                current_offset += sizeof(table_name_len);
                std::string table_name(buf + current_offset, table_name_len);
                current_offset += table_name_len;

                // Read ts string length and value
                size_t ts_len = *(size_t *) (buf + current_offset);
                current_offset += sizeof(ts_len);
                std::string ts_str(buf + current_offset, ts_len);
                current_offset += ts_len;

                txservice::TableType table_type =
                    txservice::TableType::Secondary;
                if (table_name.find(txservice::UNIQUE_INDEX_NAME_PREFIX) !=
                    std::string::npos)
                {
                    table_type = txservice::TableType::UniqueSecondary;
                }
                else if (table_name.find(txservice::INDEX_NAME_PREFIX) !=
                         std::string::npos)
                {
                    table_type = txservice::TableType::Secondary;
                }
                else
                {
                    LOG(FATAL) << "Unknown secondary key type: " << table_name;
                    assert(false && "Unknown secondary key type.");
                }
                txservice::TableName table_name_obj(
                    table_name, table_type, table_engine_);
                sk_schemas_ts_.try_emplace(std::move(table_name_obj),
                                           std::stoull(ts_str));
            }
        }
    }

    std::string Serialize() const
    {
        std::string output_str;
        size_t len_sizeof = sizeof(size_t);

        std::string table_ts(std::to_string(pk_schema_ts_));
        size_t len_val = table_ts.size();
        char *len_ptr = reinterpret_cast<char *>(&len_val);
        output_str.append(len_ptr, len_sizeof);
        output_str.append(table_ts.data(), len_val);

        // Use length-prefixed format: for each index:
        // [table_name_len][table_name][ts_len][ts_string]
        std::string index_tables_ts;
        if (sk_schemas_ts_.size() != 0)
        {
            for (auto it = sk_schemas_ts_.cbegin(); it != sk_schemas_ts_.cend();
                 ++it)
            {
                std::string table_name = it->first.String();
                size_t table_name_len = table_name.length();
                std::string ts_str = std::to_string(it->second);
                size_t ts_len = ts_str.length();
                index_tables_ts.append(
                    reinterpret_cast<const char *>(&table_name_len),
                    sizeof(table_name_len));
                index_tables_ts.append(table_name);
                index_tables_ts.append(reinterpret_cast<const char *>(&ts_len),
                                       sizeof(ts_len));
                index_tables_ts.append(ts_str);
            }
        }

        len_val = index_tables_ts.size();
        output_str.append(len_ptr, len_sizeof);
        output_str.append(index_tables_ts.data(), len_val);

        return output_str;
    }

    void Deserialize(const char *buf, size_t &offset)
    {
        if (buf == nullptr || buf[0] == '\0')
        {
            return;
        }
        size_t len_sizeof = sizeof(size_t);
        size_t *len_ptr = (size_t *) (buf + offset);
        size_t len_val = *len_ptr;
        offset += len_sizeof;

        pk_schema_ts_ = std::stoull(std::string(buf + offset, len_val));
        offset += len_val;

        len_ptr = (size_t *) (buf + offset);
        len_val = *len_ptr;
        offset += len_sizeof;
        if (len_val != 0)
        {
            // Deserialize length-prefixed format: for each index:
            // [table_name_len][table_name][ts_len][ts_string]
            size_t current_offset = offset;
            while (current_offset < offset + len_val)
            {
                // Read table name length and value
                size_t table_name_len = *(size_t *) (buf + current_offset);
                current_offset += sizeof(table_name_len);
                std::string table_name(buf + current_offset, table_name_len);
                current_offset += table_name_len;

                // Read ts string length and value
                size_t ts_len = *(size_t *) (buf + current_offset);
                current_offset += sizeof(ts_len);
                std::string ts_str(buf + current_offset, ts_len);
                current_offset += ts_len;

                txservice::TableType table_type =
                    txservice::TableType::Secondary;
                if (table_name.find(txservice::UNIQUE_INDEX_NAME_PREFIX) !=
                    std::string::npos)
                {
                    table_type = txservice::TableType::UniqueSecondary;
                }
                else if (table_name.find(txservice::INDEX_NAME_PREFIX) !=
                         std::string::npos)
                {
                    table_type = txservice::TableType::Secondary;
                }
                else
                {
                    LOG(FATAL) << "Unknown secondary key type: " << table_name;
                    assert(false && "Unknown secondary key type.");
                }
                txservice::TableName table_name_obj(
                    table_name, table_type, table_engine_);
                sk_schemas_ts_.try_emplace(std::move(table_name_obj),
                                           std::stoull(ts_str));
            }
        }
        else
        {
            sk_schemas_ts_.clear();
        }
        offset += len_val;
    }

    /**
     * @brief Get the key schema ts of the specified [primary/secondary] key.
     *
     * @param table_name key schema name.
     *
     * @return If the key is newly added, return 1. Otherwise, return normal ts.
     */
    uint64_t GetKeySchemaTs(const txservice::TableName &table_name) const
    {
        if (table_name.Type() == txservice::TableType::Primary)
        {
            return pk_schema_ts_;
        }
        else
        {
            auto v_it = sk_schemas_ts_.find(table_name);
            return v_it == sk_schemas_ts_.end() ? 1 : v_it->second;
        }
    }

    TableEngine table_engine_{TableEngine::None};
    uint64_t pk_schema_ts_{1};
    std::unordered_map<txservice::TableName, uint64_t> sk_schemas_ts_;
};

struct MultiKeyAttr
{
    MultiKeyAttr(const TableName *index_name,
                 bool multikey,
                 MultiKeyPaths::Uptr multikey_paths)
        : index_name_(index_name),
          multikey_(multikey),
          multikey_paths_(std::move(multikey_paths))
    {
    }

    const TableName *index_name_{nullptr};
    bool multikey_{false};

    // Points to nullptr if:
    // - The calculation engine has no multikey index concept.
    // - The multikey attribute is false.
    MultiKeyPaths::Uptr multikey_paths_{nullptr};
};
}  // namespace txservice
