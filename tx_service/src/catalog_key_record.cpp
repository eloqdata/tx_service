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
#include "catalog_key_record.h"

#include <butil/logging.h>

#include <memory>

namespace txservice
{
CatalogKey::CatalogKey()
    : table_name_(empty_sv, TableType::Primary, TableEngine::None)
{
}

CatalogKey::CatalogKey(const TableName &name)
    : table_name_(
          name.IsStringOwner()
              ? TableName{name.StringView().data(),
                          name.StringView().size(),
                          name.Type(),
                          name.Engine()}
              : TableName{name.StringView(), name.Type(), name.Engine()})
{
    assert(table_name_.Type() == TableType::Primary);
}

CatalogKey::CatalogKey(CatalogKey &&rhs) noexcept
    : table_name_(std::move(rhs.table_name_))
{
}

CatalogKey::CatalogKey(const CatalogKey &rhs) : table_name_(rhs.table_name_)
{
}

CatalogKey::CatalogKey(const CatalogKey &rhs, const KeySchema *)
    : table_name_(rhs.table_name_.StringView().data(),
                  rhs.table_name_.StringView().size(),
                  rhs.table_name_.Type(),
                  rhs.table_name_.Engine())
{
    assert(table_name_.Type() == TableType::Primary);
    assert(table_name_.IsStringOwner());
}

bool CatalogKey::operator==(const TxKey &rhs) const
{
    return false;
}

bool operator==(const CatalogKey &lhs, const CatalogKey &rhs)
{
    return lhs.table_name_ == rhs.table_name_;
}

bool operator!=(const CatalogKey &lhs, const CatalogKey &rhs)
{
    return !(lhs == rhs);
}

bool CatalogKey::operator<(const TxKey &rhs) const
{
    return false;
}

bool operator<(const CatalogKey &lhs, const CatalogKey &rhs)
{
    return lhs.table_name_ < rhs.table_name_;
}

bool operator<=(const CatalogKey &lhs, const CatalogKey &rhs)
{
    return !(rhs < lhs);
}

size_t CatalogKey::Hash() const
{
    return std::hash<TableName>{}(table_name_);
}

void CatalogKey::Serialize(std::vector<char> &buf, size_t &offset) const
{
    // A 2-byte integer represents lengths up to 65535, which is far more enough
    // for table names.
    uint16_t len_val = (uint16_t) table_name_.StringView().size();
    buf.resize(offset + sizeof(uint16_t) + len_val);
    const char *val_ptr =
        static_cast<const char *>(static_cast<const void *>(&len_val));
    std::copy(val_ptr, val_ptr + sizeof(uint16_t), buf.begin() + offset);
    offset += sizeof(uint16_t);

    std::copy(table_name_.StringView().begin(),
              table_name_.StringView().end(),
              buf.begin() + offset);
    offset += len_val;
    // 1 byte integer for table engine
    const char *engine_ptr = static_cast<const char *>(
        static_cast<const void *>(&table_name_.Engine()));
    std::copy(engine_ptr, engine_ptr + sizeof(uint8_t), buf.begin() + offset);
    offset += sizeof(uint8_t);
    // 1 byte integer for table type
    const char *type_ptr = static_cast<const char *>(
        static_cast<const void *>(&table_name_.Type()));
    std::copy(type_ptr, type_ptr + sizeof(uint8_t), buf.begin() + offset);
    offset += sizeof(uint8_t);
}

void CatalogKey::Serialize(std::string &str) const
{
    size_t len_sizeof = sizeof(uint16_t);
    // A 2-byte integer represents lengths up to 65535, which is far more enough
    // for table names.
    uint16_t len_val = (uint16_t) table_name_.StringView().size();
    const char *ptr = reinterpret_cast<const char *>(&len_val);

    str.append(ptr, len_sizeof);
    str.append(table_name_.StringView().data(), len_val);
    // 1 byte integer for table engine
    ptr = reinterpret_cast<const char *>(&table_name_.Engine());
    str.append(ptr, sizeof(uint8_t));
    // 1 byte integer for table type
    ptr = reinterpret_cast<const char *>(&table_name_.Type());
    str.append(ptr, sizeof(uint8_t));
}

size_t CatalogKey::SerializedLength() const
{
    // table name's length + table name + table type
    return sizeof(uint16_t) + table_name_.StringView().size() + sizeof(uint8_t);
}

void CatalogKey::Deserialize(const char *buf, size_t &offset, const KeySchema *)
{
    // construct table name string_view
    uint16_t *len_ptr = (uint16_t *) (buf + offset);
    uint16_t len_val = *len_ptr;
    offset += sizeof(uint16_t);

    std::string_view str_view{buf + offset, len_val};
    offset += len_val;

    // construct table engine
    TableEngine table_engine = TableEngine::None;
    uint8_t *engine_ptr = (uint8_t *) (buf + offset);
    uint8_t engine_val = *engine_ptr;
    table_engine = static_cast<TableEngine>(engine_val);

    offset += sizeof(uint8_t);
    // construct table type
    TableType table_type = TableType::Primary;
    uint8_t *type_ptr = (uint8_t *) (buf + offset);
    uint8_t type_val = *type_ptr;
    table_type = static_cast<TableType>(type_val);

    offset += sizeof(uint8_t);

    table_name_ = TableName{str_view, table_type, table_engine};
}

TxKey CatalogKey::CloneTxKey() const
{
    return TxKey(std::make_unique<CatalogKey>(table_name_));
}

void CatalogKey::Copy(const CatalogKey &rhs)
{
    table_name_.CopyFrom(rhs.table_name_);
}

std::string CatalogKey::ToString() const
{
    return table_name_.String();
}

const TableName &CatalogKey::Name() const
{
    return table_name_;
}

TableName &CatalogKey::Name()
{
    return table_name_;
}

CatalogRecord::CatalogRecord(CatalogRecord &&rhs) noexcept
    : schema_(rhs.schema_),
      dirty_schema_(rhs.dirty_schema_),
      schema_ts_(rhs.schema_ts_),
      schema_image_(std::move(rhs.schema_image_)),
      dirty_schema_image_(std::move(rhs.dirty_schema_image_)),
      cntl_(rhs.cntl_)
{
}

CatalogRecord::CatalogRecord(const CatalogRecord &rhs)
    : schema_(rhs.schema_),
      dirty_schema_(rhs.dirty_schema_),
      schema_ts_(rhs.schema_ts_),
      schema_image_(rhs.schema_image_),
      dirty_schema_image_(rhs.dirty_schema_image_),
      cntl_(rhs.cntl_)
{
}

void CatalogRecord::Serialize(std::vector<char> &buf, size_t &offset) const
{
    buf.reserve(offset + 2 * sizeof(uint32_t) + dirty_schema_image_.size() +
                schema_image_.size());
    uint32_t len_val = (uint32_t) dirty_schema_image_.size();
    const char *val_ptr =
        static_cast<const char *>(static_cast<const void *>(&len_val));
    std::copy(val_ptr, val_ptr + sizeof(uint32_t), buf.begin() + offset);
    offset += sizeof(uint32_t);
    std::copy(dirty_schema_image_.begin(),
              dirty_schema_image_.end(),
              buf.begin() + offset);
    offset += len_val;

    len_val = (uint32_t) schema_image_.size();
    val_ptr = static_cast<const char *>(static_cast<const void *>(&len_val));
    std::copy(val_ptr, val_ptr + sizeof(uint32_t), buf.begin() + offset);
    offset += sizeof(uint32_t);
    std::copy(schema_image_.begin(), schema_image_.end(), buf.begin() + offset);
    offset += len_val;
}

void CatalogRecord::Serialize(std::string &str) const
{
    size_t len_sizeof = sizeof(uint32_t);
    uint32_t len_val = (uint32_t) dirty_schema_image_.size();
    const char *len_ptr = reinterpret_cast<const char *>(&len_val);
    str.append(len_ptr, len_sizeof);
    str.append(dirty_schema_image_.data(), len_val);

    len_val = (uint32_t) schema_image_.size();
    len_ptr = reinterpret_cast<const char *>(&len_val);
    str.append(len_ptr, len_sizeof);
    str.append(schema_image_.data(), len_val);
}

size_t CatalogRecord::SerializedLength() const
{
    // dirty_schema_image_ and schema_image_ and their length
    return sizeof(uint32_t) * 2 + dirty_schema_image_.size() +
           schema_image_.size();
}

void CatalogRecord::Deserialize(const char *buf, size_t &offset)
{
    uint32_t *len_ptr = (uint32_t *) (buf + offset);
    uint32_t len_val = *len_ptr;
    offset += sizeof(uint32_t);
    dirty_schema_image_.clear();
    dirty_schema_image_.reserve(len_val);
    dirty_schema_image_.append(buf + offset, len_val);
    offset += len_val;

    len_ptr = (uint32_t *) (buf + offset);
    len_val = *len_ptr;
    offset += sizeof(uint32_t);
    schema_image_.clear();
    schema_image_.reserve(len_val);
    schema_image_.append(buf + offset, len_val);
    offset += len_val;
}

TxRecord::Uptr CatalogRecord::Clone() const
{
    std::unique_ptr<CatalogRecord> rec = std::make_unique<CatalogRecord>();

    rec->schema_ = schema_;
    rec->dirty_schema_ = dirty_schema_;
    rec->schema_ts_ = schema_ts_;
    rec->schema_image_ = schema_image_;
    rec->dirty_schema_image_ = dirty_schema_image_;

    return rec;
}

void CatalogRecord::Copy(const TxRecord &rhs)
{
    const CatalogRecord &typed_rhs = static_cast<const CatalogRecord &>(rhs);
    *this = typed_rhs;
}

std::string CatalogRecord::ToString() const
{
    return std::string();
}

void CatalogRecord::Set(const std::shared_ptr<TableSchema> &schema,
                        const std::shared_ptr<TableSchema> &dirty_schema,
                        uint64_t schema_ts)
{
    schema_ = schema;
    dirty_schema_ = dirty_schema;
    schema_ts_ = schema_ts;
}

const std::string &CatalogRecord::SchemaImage() const
{
    return schema_image_;
}

void CatalogRecord::SetSchemaImage(std::string &&schema_image)
{
    schema_image_ = std::move(schema_image);
}

void CatalogRecord::SetSchemaImage(const std::string &schema_image)
{
    schema_image_ = schema_image;
}

const std::string &CatalogRecord::DirtySchemaImage() const
{
    return dirty_schema_image_;
}

void CatalogRecord::SetDirtySchemaImage(std::string &&schema_image)
{
    dirty_schema_image_ = std::move(schema_image);
}

void CatalogRecord::SetDirtySchemaImage(const std::string &schema_image)
{
    dirty_schema_image_ = schema_image;
}

const TableSchema *CatalogRecord::Schema() const
{
    return schema_.get();
}

std::shared_ptr<const TableSchema> CatalogRecord::CopySchema() const
{
    return schema_;
}

const TableSchema *CatalogRecord::DirtySchema() const
{
    return dirty_schema_.get();
}

std::shared_ptr<const TableSchema> CatalogRecord::CopyDirtySchema() const
{
    return dirty_schema_;
}

void CatalogRecord::ClearDirtySchema()
{
    dirty_schema_ = nullptr;
}

void CatalogRecord::Reset()
{
    schema_ = nullptr;
    dirty_schema_ = nullptr;
    schema_ts_ = 0;
    schema_image_.clear();
    dirty_schema_image_.clear();
}

uint64_t CatalogRecord::SchemaTs() const
{
    return schema_ts_;
}

CatalogRecord &CatalogRecord::operator=(const CatalogRecord &rhs)
{
    if (this == &rhs)
    {
        return *this;
    }

    schema_ = rhs.schema_;
    dirty_schema_ = rhs.dirty_schema_;
    schema_ts_ = rhs.schema_ts_;
    // schema_image_ = rhs.schema_image_;
    // dirty_schema_image_ = rhs.dirty_schema_image_;

    return *this;
}

CatalogRecord &CatalogRecord::operator=(CatalogRecord &&rhs) noexcept
{
    if (this == &rhs)
    {
        return *this;
    }

    schema_ = rhs.schema_;
    dirty_schema_ = rhs.dirty_schema_;
    schema_ts_ = rhs.schema_ts_;
    schema_image_ = std::move(rhs.schema_image_);
    dirty_schema_image_ = std::move(rhs.dirty_schema_image_);

    return *this;
}

}  // namespace txservice
