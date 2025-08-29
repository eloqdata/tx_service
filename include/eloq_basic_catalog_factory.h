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

#include <cstdint>

#include "catalog_factory.h"
#include "tx_command.h"
#include "tx_key.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{

class EloqBasicKeySchema : public KeySchema
{
public:
    explicit EloqBasicKeySchema(uint64_t key_schema_ts) : schema_ts_(key_schema_ts)
    {
    }

    using Uptr = std::unique_ptr<EloqBasicKeySchema>;

    bool CompareKeys(const TxKey &key1,
                     const TxKey &key2,
                     size_t *const start_column_diff) const override
    {
        *start_column_diff = -1;
        return true;
    }

    uint16_t ExtendKeyParts() const override
    {
        return 0;  // No extended key parts for simple string keys
    }

    uint64_t SchemaTs() const override
    {
        return schema_ts_;
    }

    uint64_t schema_ts_{1};
};

struct EloqBasicRecordSchema : public RecordSchema
{
public:
    EloqBasicRecordSchema() = default;
};

struct EloqBasicTableSchema : public TableSchema
{
public:
    EloqBasicTableSchema(const TableName &table_name,
                        const std::string &catalog_image,
                        uint64_t version);

    TableSchema::uptr Clone() const override;

    const TableName &GetBaseTableName() const override
    {
        return table_name_;
    }

    const txservice::KeySchema *KeySchema() const override
    {
        return key_schema_.get();
    }

    const txservice::RecordSchema *RecordSchema() const override
    {
        return record_schema_.get();
    }

    const std::string &SchemaImage() const override
    {
        return schema_image_;
    }

    const std::unordered_map<
        uint16_t,
        std::pair<TableName, SecondaryKeySchema>> *
    GetIndexes() const override
    {
        // No secondary indexes supported
        return nullptr;
    }

    KVCatalogInfo *GetKVCatalogInfo() const override
    {
        return kv_info_.get();
    }

    uint16_t IndexOffset(const TableName &index_name) const override
    {
        // No secondary indexes supported
        return UINT16_MAX;
    }

    void BindStatistics(
        std::shared_ptr<Statistics> statistics) override
    {
        // No statistics binding needed for internal use
    }

    void SetKVCatalogInfo(const std::string &kv_info_str) override
    {
        // No KV catalog info setting needed for internal use
    }

    std::shared_ptr<Statistics> StatisticsObject() const override
    {
        return nullptr;
    }

    uint64_t Version() const override
    {
        return version_;
    }

    std::string_view VersionStringView() const override
    {
        static std::string version_str = std::to_string(version_);
        return {version_str.data(), version_str.length()};
    }

    std::vector<TableName> IndexNames() const override
    {
        return std::vector<TableName>();
    }

    size_t IndexesSize() const override
    {
        return 0;
    }

    const SecondaryKeySchema *IndexKeySchema(
        const TableName &index_name) const override
    {
        // No secondary indexes supported
        return nullptr;
    }

    std::unique_ptr<TxCommand> CreateTxCommand(
        std::string_view cmd_image) const override;

    bool HasAutoIncrement() const override
    {
        return false;
    }

    const TableName *GetSequenceTableName() const override;
    
    std::pair<TxKey, TxRecord::Uptr>
    GetSequenceKeyAndInitRecord(
        const TableName &table_name) const override;
    
    void SetVersion(uint64_t version)
    {
        version_ = version;
    }

private:
    TableName table_name_;
    KVCatalogInfo::uptr kv_info_{nullptr};
    EloqBasicKeySchema::Uptr key_schema_{nullptr};
    EloqBasicRecordSchema::Uptr record_schema_{nullptr};
    std::string schema_image_;
    uint64_t version_{1};
};

class EloqHashCatalogFactory : public CatalogFactory
{
public:
    EloqHashCatalogFactory() = default;
    ~EloqHashCatalogFactory() override = default;

    TableSchema::uptr CreateTableSchema(
        const TableName &table_name,
        const std::string &catalog_image,
        uint64_t version) override;

    CcMap::uptr CreatePkCcMap(
        const TableName &table_name,
        const TableSchema *table_schema,
        bool ccm_has_full_entries,
        CcShard *shard,
        NodeGroupId cc_ng_id) override;

    CcMap::uptr CreateSkCcMap(
        const TableName &table_name,
        const TableSchema *table_schema,
        CcShard *shard,
        NodeGroupId cc_ng_id) override;

    CcMap::uptr CreateRangeMap(
        const TableName &range_table_name,
        const TableSchema *table_schema,
        uint64_t schema_ts,
        CcShard *shard,
        NodeGroupId ng_id) override;

    std::unique_ptr<CcScanner> CreatePkCcmScanner(
        ScanDirection direction, const KeySchema *key_schema) override;

    std::unique_ptr<CcScanner> CreateSkCcmScanner(
        ScanDirection direction,
        const KeySchema *compound_key_schema) override;

    std::unique_ptr<CcScanner> CreateRangeCcmScanner(
        ScanDirection direction,
        const KeySchema *key_schema,
        const TableName &range_table_name) override;

    std::unique_ptr<Statistics> CreateTableStatistics(
        const TableSchema *table_schema, NodeGroupId cc_ng_id) override;

    std::unique_ptr<Statistics> CreateTableStatistics(
        const TableSchema *table_schema,
        std::unordered_map<TableName,
                           std::pair<uint64_t, std::vector<TxKey>>>
            sample_pool_map,
        CcShard *ccs,
        NodeGroupId cc_ng_id) override;

    std::unique_ptr<TableRangeEntry> CreateTableRange(
        TxKey start_key,
        uint64_t version_ts,
        int64_t partition_id,
        std::unique_ptr<StoreRange> slices = nullptr) override
    {
        // No range partitioning supported for hash catalog factory
        return nullptr;
    }

    TxKey NegativeInfKey() override;

    TxKey PositiveInfKey() override;

    size_t KeyHash(const char *buf,
                   size_t offset,
                   const KeySchema *key_schema) const override;
};



class EloqRangeCatalogFactory : public CatalogFactory
{
public:
    EloqRangeCatalogFactory() = default;
    ~EloqRangeCatalogFactory() override = default;

    TableSchema::uptr CreateTableSchema(
        const TableName &table_name,
        const std::string &catalog_image,
        uint64_t version) override;

    CcMap::uptr CreatePkCcMap(
        const TableName &table_name,
        const TableSchema *table_schema,
        bool ccm_has_full_entries,
        CcShard *shard,
        NodeGroupId cc_ng_id) override;

    CcMap::uptr CreateSkCcMap(
        const TableName &table_name,
        const TableSchema *table_schema,
        CcShard *shard,
        NodeGroupId cc_ng_id) override;

    CcMap::uptr CreateRangeMap(
        const TableName &range_table_name,
        const TableSchema *table_schema,
        uint64_t schema_ts,
        CcShard *shard,
        NodeGroupId ng_id) override;

    std::unique_ptr<CcScanner> CreatePkCcmScanner(
        ScanDirection direction, const KeySchema *key_schema) override;

    std::unique_ptr<CcScanner> CreateSkCcmScanner(
        ScanDirection direction,
        const KeySchema *compound_key_schema) override;

    std::unique_ptr<CcScanner> CreateRangeCcmScanner(
        ScanDirection direction,
        const KeySchema *key_schema,
        const TableName &range_table_name) override;

    std::unique_ptr<Statistics> CreateTableStatistics(
        const TableSchema *table_schema, NodeGroupId cc_ng_id) override;

    std::unique_ptr<Statistics> CreateTableStatistics(
        const TableSchema *table_schema,
        std::unordered_map<TableName,
                           std::pair<uint64_t, std::vector<TxKey>>>
            sample_pool_map,
        CcShard *ccs,
        NodeGroupId cc_ng_id) override;

    std::unique_ptr<TableRangeEntry> CreateTableRange(
        TxKey start_key,
        uint64_t version_ts,
        int64_t partition_id,
        std::unique_ptr<StoreRange> slices = nullptr) override;

    TxKey NegativeInfKey() override;

    TxKey PositiveInfKey() override;

    size_t KeyHash(const char *buf,
                   size_t offset,
                   const KeySchema *key_schema) const override;
};
}  // namespace txservice
