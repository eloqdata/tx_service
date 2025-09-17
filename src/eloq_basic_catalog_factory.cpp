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
#include "eloq_basic_catalog_factory.h"

#include <memory>
#include <vector>

#include "cc/ccm_scanner.h"
#include "cc/template_cc_map.h"
#include "eloq_string_key_record.h"
#include "range_cc_map.h"
#include "schema.h"
#include "sequences/sequences.h"
#include "sharder.h"
#include "tx_key.h"
#include "type.h"

namespace txservice
{

EloqBasicTableSchema::EloqBasicTableSchema(const TableName &table_name,
                                           const std::string &catalog_image,
                                           uint64_t version)
    : table_name_(table_name), schema_image_(catalog_image), version_(version)
{
    // Create a simple KV catalog info for internal use
    kv_info_ = std::make_unique<KVCatalogInfo>();

    // Use table version as key schema version
    uint64_t key_schema_ts = version;

    // Set a simple table name for internal use
    kv_info_->kv_table_name_ = table_name.String();

    // Create key and record schemas
    key_schema_ = std::make_unique<EloqBasicKeySchema>(key_schema_ts);
    record_schema_ = std::make_unique<EloqBasicRecordSchema>();
}

TableSchema::uptr EloqBasicTableSchema::Clone() const
{
    return std::make_unique<EloqBasicTableSchema>(
        table_name_, schema_image_, version_);
}

std::unique_ptr<TxCommand> EloqBasicTableSchema::CreateTxCommand(
    std::string_view cmd_image) const
{
    // For internal use, we don't need complex command creation
    // Return a simple command or nullptr
    return nullptr;
}

const TableName *EloqBasicTableSchema::GetSequenceTableName() const
{
    return &Sequences::table_name_;
}

std::pair<TxKey, TxRecord::Uptr>
EloqBasicTableSchema::GetSequenceKeyAndInitRecord(
    const TableName &table_name) const
{
    // Should not support auto increment column for basic table schema
    return {TxKey(), nullptr};
}

TableSchema::uptr EloqHashCatalogFactory::CreateTableSchema(
    const TableName &table_name,
    const std::string &catalog_image,
    uint64_t version)
{
    if (table_name == txservice::Sequences::table_name_)
    {
        DLOG(INFO) << "===create sequence table schema";
        return std::make_unique<txservice::SequenceTableSchema>(
            table_name, catalog_image, version);
    }
    return std::make_unique<EloqBasicTableSchema>(
        table_name, catalog_image, version);
}

CcMap::uptr EloqHashCatalogFactory::CreatePkCcMap(
    const TableName &table_name,
    const TableSchema *table_schema,
    bool ccm_has_full_entries,
    CcShard *shard,
    NodeGroupId cc_ng_id)
{
    uint64_t table_version = table_schema->Version();
    assert(table_version == table_schema->KeySchema()->SchemaTs());

    // Create a hash-partitioned CCM with versioned records
    // Using TemplateCcMap<EloqStringKey, EloqStringRecord, true, false>
    // where true = VersionedRecord, false = RangePartitioned
    return std::make_unique<
        TemplateCcMap<EloqStringKey, EloqStringRecord, true, false>>(
        shard,
        cc_ng_id,
        table_name,
        table_version,
        table_schema,
        ccm_has_full_entries || txservice_skip_kv);
}

CcMap::uptr EloqHashCatalogFactory::CreateSkCcMap(
    const TableName &table_name,
    const TableSchema *table_schema,
    CcShard *shard,
    NodeGroupId cc_ng_id)
{
    // No secondary indexes supported for hash catalog factory
    return nullptr;
}

CcMap::uptr EloqHashCatalogFactory::CreateRangeMap(
    const TableName &range_table_name,
    const TableSchema *table_schema,
    uint64_t schema_ts,
    CcShard *shard,
    NodeGroupId ng_id)
{
    // No range partitioning supported for hash catalog factory
    return nullptr;
}

std::unique_ptr<CcScanner> EloqHashCatalogFactory::CreatePkCcmScanner(
    ScanDirection direction, const KeySchema *key_schema)
{
    return std::make_unique<
        HashParitionCcScanner<EloqStringKey, EloqStringRecord>>(
        direction, ScanIndexType::Primary, key_schema);
}

std::unique_ptr<CcScanner> EloqHashCatalogFactory::CreateSkCcmScanner(
    ScanDirection direction, const KeySchema *compound_key_schema)
{
    // No secondary indexes supported
    return nullptr;
}

std::unique_ptr<CcScanner> EloqHashCatalogFactory::CreateRangeCcmScanner(
    ScanDirection direction,
    const KeySchema *key_schema,
    const TableName &range_table_name)
{
    // No range partitioning supported
    return nullptr;
}

std::unique_ptr<Statistics> EloqHashCatalogFactory::CreateTableStatistics(
    const TableSchema *table_schema, NodeGroupId cc_ng_id)
{
    // No statistics needed for internal use
    return nullptr;
}

std::unique_ptr<Statistics> EloqHashCatalogFactory::CreateTableStatistics(
    const TableSchema *table_schema,
    std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
        sample_pool_map,
    CcShard *ccs,
    NodeGroupId cc_ng_id)
{
    // No statistics needed for internal use
    return nullptr;
}

TxKey EloqHashCatalogFactory::NegativeInfKey()
{
    return TxKey(EloqStringKey::NegativeInfinity());
}

TxKey EloqHashCatalogFactory::PositiveInfKey()
{
    return TxKey(EloqStringKey::PositiveInfinity());
}

size_t EloqHashCatalogFactory::KeyHash(const char *buf,
                                       size_t offset,
                                       const KeySchema *key_schema) const
{
    return EloqStringKey::HashFromSerializedKey(buf, offset);
}

TableSchema::uptr EloqRangeCatalogFactory::CreateTableSchema(
    const TableName &table_name,
    const std::string &catalog_image,
    uint64_t version)
{
    if (table_name == txservice::Sequences::table_name_)
    {
        DLOG(INFO) << "===create sequence table schema";
        return std::make_unique<txservice::SequenceTableSchema>(
            table_name, catalog_image, version);
    }
    return std::make_unique<EloqBasicTableSchema>(
        table_name, catalog_image, version);
}

CcMap::uptr EloqRangeCatalogFactory::CreatePkCcMap(
    const TableName &table_name,
    const TableSchema *table_schema,
    bool ccm_has_full_entries,
    CcShard *shard,
    NodeGroupId cc_ng_id)
{
    uint64_t table_version = table_schema->Version();
    assert(table_version == table_schema->KeySchema()->SchemaTs());

    // Create a range-partitioned CCM with versioned records
    // Using TemplateCcMap<EloqStringKey, EloqStringRecord, true, true>
    // where true = VersionedRecord, true = RangePartitioned
    return std::make_unique<
        TemplateCcMap<EloqStringKey, EloqStringRecord, true, true>>(
        shard,
        cc_ng_id,
        table_name,
        table_version,
        table_schema,
        ccm_has_full_entries || txservice_skip_kv);
}

CcMap::uptr EloqRangeCatalogFactory::CreateSkCcMap(
    const TableName &table_name,
    const TableSchema *table_schema,
    CcShard *shard,
    NodeGroupId cc_ng_id)
{
    // No secondary indexes supported for range catalog factory
    return nullptr;
}

CcMap::uptr EloqRangeCatalogFactory::CreateRangeMap(
    const TableName &range_table_name,
    const TableSchema *table_schema,
    uint64_t schema_ts,
    CcShard *shard,
    NodeGroupId ng_id)
{
    // For range-partitioned tables, we create a range CCM
    // This maps table ranges to partition IDs
    return std::make_unique<RangeCcMap<EloqStringKey>>(
        range_table_name, table_schema, schema_ts, shard, ng_id);
}

std::unique_ptr<CcScanner> EloqRangeCatalogFactory::CreatePkCcmScanner(
    ScanDirection direction, const KeySchema *key_schema)
{
    // For range partitioning, we need to use RangePartitionedCcmScanner
    // with forward direction for range scans
    if (direction == ScanDirection::Forward)
    {
        return std::make_unique<
            RangePartitionedCcmScanner<EloqStringKey, EloqStringRecord, true>>(
            direction, ScanIndexType::Primary, key_schema);
    }
    else
    {
        return std::make_unique<
            RangePartitionedCcmScanner<EloqStringKey, EloqStringRecord, false>>(
            direction, ScanIndexType::Primary, key_schema);
    }
}

std::unique_ptr<CcScanner> EloqRangeCatalogFactory::CreateSkCcmScanner(
    ScanDirection direction, const KeySchema *compound_key_schema)
{
    // No secondary indexes supported
    return nullptr;
}

std::unique_ptr<CcScanner> EloqRangeCatalogFactory::CreateRangeCcmScanner(
    ScanDirection direction,
    const KeySchema *key_schema,
    const TableName &range_table_name)
{
    // For range table scans, we use RangePartitionedCcmScanner
    if (direction == ScanDirection::Forward)
    {
        return std::make_unique<
            RangePartitionedCcmScanner<EloqStringKey, EloqStringRecord, true>>(
            direction, ScanIndexType::Primary, key_schema);
    }
    else
    {
        return std::make_unique<
            RangePartitionedCcmScanner<EloqStringKey, EloqStringRecord, false>>(
            direction, ScanIndexType::Primary, key_schema);
    }
}

std::unique_ptr<Statistics> EloqRangeCatalogFactory::CreateTableStatistics(
    const TableSchema *table_schema, NodeGroupId cc_ng_id)
{
    // No statistics needed for internal use
    return nullptr;
}

std::unique_ptr<Statistics> EloqRangeCatalogFactory::CreateTableStatistics(
    const TableSchema *table_schema,
    std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
        sample_pool_map,
    CcShard *ccs,
    NodeGroupId cc_ng_id)
{
    // No statistics needed for internal use
    return nullptr;
}

std::unique_ptr<TableRangeEntry> EloqRangeCatalogFactory::CreateTableRange(
    TxKey start_key,
    uint64_t version_ts,
    int64_t partition_id,
    std::unique_ptr<StoreRange> slices)
{
    assert(start_key.Type() == KeyType::NegativeInf || start_key.IsOwner());
    // The range's start key must not be null. If the start points to negative
    // infinity, it points to MongoKey::NegativeInfinity().
    const EloqStringKey *start = start_key.GetKey<EloqStringKey>();
    assert(start != nullptr);

    txservice::TemplateStoreRange<EloqStringKey> *range_ptr =
        static_cast<txservice::TemplateStoreRange<EloqStringKey> *>(
            slices.release());

    std::unique_ptr<txservice::TemplateStoreRange<EloqStringKey>> typed_range{
        range_ptr};

    return std::make_unique<txservice::TemplateTableRangeEntry<EloqStringKey>>(
        start, version_ts, partition_id, std::move(typed_range));
}

TxKey EloqRangeCatalogFactory::NegativeInfKey()
{
    return TxKey(EloqStringKey::NegativeInfinity());
}

TxKey EloqRangeCatalogFactory::PositiveInfKey()
{
    return TxKey(EloqStringKey::PositiveInfinity());
}

size_t EloqRangeCatalogFactory::KeyHash(const char *buf,
                                        size_t offset,
                                        const KeySchema *key_schema) const
{
    return EloqStringKey::HashFromSerializedKey(buf, offset);
}

}  // namespace txservice
