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

#include <map>
#include <memory>  //unique_ptr
#include <string>
#include <utility>  //pair
#include <vector>

#include "store/data_store_handler.h"
#include "tx_key.h"     //CompositeKey
#include "tx_record.h"  //CompositeRecord,VersionTxRecord

namespace txservice::store
{
class IntMemoryStore : public DataStoreHandler
{
public:
    IntMemoryStore()
    {
    }

    bool Connect() override
    {
        return true;
    }

    /**
     * @brief flush entries in \@param flush_task to base table or skindex table in
     * data store, stop and return false if the flush failed after max retry times.
     * @param flush_task - the flush task to be flushed. Map key is the kv table name,
     * value is the vector of flush task entries. PutAll only flushes the data_sync_vec_
     * in each flush task entry.
     * @return whether all entries are written to data store successfully
     */
    bool PutAll(std::unordered_map<
                std::string_view,
                std::vector<std::unique_ptr<txservice::FlushTaskEntry>>>
                    &flush_task) override
    {
        assert(false);
        // for (const auto &ref : batch)
        // {
        //     const CompositeKey<int> *key =
        //         ref.Key().GetKey<CompositeKey<int>>();

        //     const CompositeRecord<int> &rec = *ref.Payload();

        //     int key_val = std::get<0>(key->Tuple());
        //     if (ref.payload_status_ == RecordStatus::Deleted)
        //     {
        //         int_store_.erase(key_val);
        //     }
        //     else
        //     {
        //         int rec_val = std::get<0>(rec.Tuple());
        //         int_store_.insert_or_assign(key_val, rec_val);
        //     }

        //     if (int_store_.size() > 1000)
        //     {
        //         int_store_.erase(int_store_.begin());
        //     }
        // }

        return true;
    }

    void UpsertTable(
        const txservice::TableSchema *old_table_schema,
        const txservice::TableSchema *table_schema,
        OperationType op_type,
        uint64_t commit_ts,
        NodeGroupId ng_id,
        int64_t tx_term,
        txservice::CcHandlerResult<txservice::Void> *hd_res,
        const txservice::AlterTableInfo *alter_table_info = nullptr,
        CcRequestBase *cc_req = nullptr,
        CcShard *ccs = nullptr,
        CcErrorCode *err_code = nullptr) override
    {
    }

    void FetchTableCatalog(const TableName &ccm_table_name,
                           FetchCatalogCc *fetch_cc) override
    {
    }

    void FetchCurrentTableStatistics(const txservice::TableName &ccm_table_name,
                                     FetchTableStatisticsCc *fetch_cc) override
    {
    }

    void FetchTableStatistics(const txservice::TableName &ccm_table_name,
                              FetchTableStatisticsCc *fetch_cc) override
    {
    }

    void FetchTableRanges(FetchTableRangesCc *fetch_cc) override
    {
    }

    void FetchRangeSlices(FetchRangeSlicesReq *fetch_cc) override
    {
    }

    bool Read(const txservice::TableName &table_name,
              const txservice::TxKey &key,
              txservice::TxRecord &rec,
              bool &found,
              uint64_t &version_ts,
              const txservice::TableSchema *table_schema) override
    {
        assert(false);
        return false;
    }

    bool FetchTable(const txservice::TableName &table_name,
                    std::string &schema_image,
                    bool &found,
                    uint64_t &version_ts) override
    {
        assert(false);
        return false;
    }

    bool DiscoverAllTableNames(
        std::vector<std::string> &norm_name_vec,
        const std::function<void()> *yield_fptr = nullptr,
        const std::function<void()> *resume_fptr = nullptr) override
    {
        assert(false);
        return false;
    }

    //-- database
    bool UpsertDatabase(std::string_view db,
                        std::string_view definition) override
    {
        assert(false);
        return false;
    }
    bool DropDatabase(std::string_view db) override
    {
        assert(false);
        return false;
    }
    bool FetchDatabase(
        std::string_view db,
        std::string &definition,
        bool &found,
        const std::function<void()> *yield_fptr = nullptr,
        const std::function<void()> *resume_fptr = nullptr) override
    {
        assert(false);
        return false;
    }
    bool FetchAllDatabase(
        std::vector<std::string> &dbnames,
        const std::function<void()> *yield_fptr = nullptr,
        const std::function<void()> *resume_fptr = nullptr) override
    {
        assert(false);
        return false;
    }

    bool DropKvTable(const std::string &kv_table_name) override
    {
        return true;
    }

    void DropKvTableAsync(const std::string &kv_table_name) override
    {
    }

    std::unique_ptr<DataStoreScanner> ScanForward(
        const txservice::TableName &table_name,
        uint32_t ng_id,
        const txservice::TxKey &start_key,
        bool inclusive,
        uint8_t key_parts,
        const std::vector<DataStoreSearchCond> &search_cond,
        const txservice::KeySchema *key_schema,
        const txservice::RecordSchema *rec_schema,
        const txservice::KVCatalogInfo *kv_info,
        bool scan_foward) override
    {
        assert(false);
        return nullptr;
    }

    bool UpsertTableStatistics(
        const txservice::TableName &ccm_table_name,
        const std::unordered_map<
            txservice::TableName,
            std::pair<uint64_t, std::vector<txservice::TxKey>>>
            &sample_pool_map,
        uint64_t version) override
    {
        assert(false);
        return false;
    }

    bool UpsertRanges(const TableName &table_name,
                      std::vector<SplitRangeInfo> range_info,
                      uint64_t version) override
    {
        return true;
    }

    bool DeleteOutOfRangeData(
        const txservice::TableName &table_name,
        int32_t partition_id,
        const TxKey *start_key,
        const txservice::TableSchema *table_schema) override
    {
        return true;
    }

    /**
     * @brief Write batch historical versions into DataStore.
     *
     */
    bool PutArchivesAll(std::unordered_map<
                        std::string_view,
                        std::vector<std::unique_ptr<txservice::FlushTaskEntry>>>
                            &flush_task) override
    {
        assert(false);
        return true;
    }
    /**
     * @brief Copy record from base/sk table to mvcc_archives.
     */
    bool CopyBaseToArchive(
        std::unordered_map<
            std::string_view,
            std::vector<std::unique_ptr<txservice::FlushTaskEntry>>>
                &flush_task) override
    {
        assert(false);
        return true;
    }

    /**
     * @brief  Get the latest visible(commit_ts <= upper_bound_ts) historical
     * version.
     */
    bool FetchVisibleArchive(const txservice::TableName &table_name,
                             const txservice::KVCatalogInfo *kv_info,
                             const txservice::TxKey &key,
                             const uint64_t upper_bound_ts,
                             txservice::TxRecord &rec,
                             txservice::RecordStatus &rec_status,
                             uint64_t &commit_ts) override
    {
        auto &typed_key = reinterpret_cast<const CompositeKey<int> &>(key);
        int int_key = std::get<0>(typed_key.Tuple());
        auto &ref =
            int_archives_[std::pair<TableName, int>(table_name, int_key)];
        for (size_t i = 0; i < ref.size(); i++)
        {
            if (ref[i].commit_ts_ <= upper_bound_ts)
            {
                rec = *ref[i].record_;
                rec_status = ref[i].record_status_;
                commit_ts = ref[i].commit_ts_;
                return true;
            }
        }

        return false;
    }

    /**
     * @brief  Fetch all archives whose commit_ts >= from_ts.
     */
    bool FetchArchives(const txservice::TableName &table_name,
                       const txservice::KVCatalogInfo *kv_info,
                       const txservice::TxKey &key,
                       std::vector<txservice::VersionTxRecord> &archives,
                       uint64_t from_ts) override
    {
        auto &typed_key = reinterpret_cast<const CompositeKey<int> &>(key);
        int int_key = std::get<0>(typed_key.Tuple());
        auto &ref =
            int_archives_[std::pair<TableName, int>(table_name, int_key)];
        for (size_t i = 0; i < ref.size(); i++)
        {
            if (ref[i].commit_ts_ >= from_ts)
            {
                auto &tmp = archives.emplace_back();
                tmp.commit_ts_ = ref[i].commit_ts_;
                tmp.record_status_ = ref[i].record_status_;
                tmp.record_ = ref[i].record_->Clone();
            }
        }

        return true;
    }

    std::string CreateKVCatalogInfo(
        const txservice::TableSchema *table_schema) const override
    {
        assert(false);
        return std::string("");
    }

    KVCatalogInfo::uptr DeserializeKVCatalogInfo(const std::string &kv_info_str,
                                                 size_t &offset) const override
    {
        assert(false);
        return KVCatalogInfo::uptr(nullptr);
    }

    size_t Size() const
    {
        return int_store_.size();
    }

    int MinKey() const
    {
        return int_store_.begin()->first;
    }

    int MaxKey() const
    {
        return int_store_.rbegin()->first;
    }

    std::string CreateNewKVCatalogInfo(
        const txservice::TableName &table_name,
        const txservice::TableSchema *current_table_schema,
        txservice::AlterTableInfo &alter_table_info) override
    {
        assert(false);
        return std::string("");
    }

    bool NeedCopyRange() const override
    {
        return false;
    }

private:
    std::map<int, int> int_store_;
    std::map<std::pair<int, int>, Void> int_index_;
    std::map<std::pair<TableName, int>, std::vector<VersionTxRecord>>
        int_archives_;
};
}  // namespace txservice::store
