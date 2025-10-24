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

#include <butil/logging.h>

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "cc_entry.h"
#include "read_write_entry.h"

namespace txservice
{
extern bool txservice_skip_wal;

using TableWriteSet = std::map<TxKey, WriteSetEntry>;

enum class ReadEntryResult : uint8_t
{
    NO_INSERT = 0,
    INSERT_ONE,
    INSERT_REPEAT
};

class ReadWriteSet
{
    static const uint32_t MaxWriteSetBytesCnt = 62 * 1024 * 1024;

public:
    ReadWriteSet()
        : data_rset_(),
          meta_data_rset_(),
          wset_(),
          wset_cnt_(0),
          wset_bytes_cnt_(0),
          forward_write_cnt_(0)
    {
    }

    void Reset()
    {
        wset_cnt_ = 0;
        wset_.clear();
        data_rset_.clear();
        meta_data_rset_.clear();
        read_lock_ng_terms_.clear();
        wset_bytes_cnt_ = 0;
        forward_write_cnt_ = 0;
        catalog_wset_.clear();
    }

    /**
     * @brief Returns the number of read data items, excluding catalog entries.
     *
     * @return size_t
     */
    size_t DataReadSetSize() const
    {
        return data_rset_.size();
    }

    size_t MetaDataReadSetSize() const
    {
        return meta_data_rset_.size();
    }

    size_t WriteSetSize() const
    {
        return wset_cnt_;
    }

    size_t CatalogWriteSetSize() const
    {
        return catalog_wset_.size();
    }

    size_t ForwardWriteCnt() const
    {
        return forward_write_cnt_;
    }

    void IncreaseFowardWriteCnt(size_t cnt)
    {
        forward_write_cnt_ += cnt;
    }

    const absl::flat_hash_map<CcEntryAddr,
                              std::pair<ReadSetEntry, const std::string_view>> &
    DataReadSet() const
    {
        return data_rset_;
    }

    const absl::flat_hash_map<CcEntryAddr,
                              std::pair<ReadSetEntry, const std::string_view>> &
    MetaDataReadSet() const
    {
        return meta_data_rset_;
    }

    const absl::flat_hash_map<uint32_t, int64_t> &ReadLockNgTerms() const
    {
        return read_lock_ng_terms_;
    }

    /**
     * @brief When adding a read key, checks if there is already one in the
     * readset and matches the timestamp. Does not add the read key if
     * there is a timestamp mismatch.
     *
     * @return true - add sucess; false - the version is different with previous
     * read, that is, break RepeatableRead isolation level.
     */
    bool AddRead(const CcEntryAddr &cce_addr,
                 uint64_t read_ts,
                 const TableName *table_name)
    {
        bool inserted = false;
        absl::flat_hash_map<
            CcEntryAddr,
            std::pair<ReadSetEntry, const std::string_view>>::iterator it;
        if (table_name->IsMeta())
        {
            std::tie(it, inserted) = meta_data_rset_.try_emplace(
                cce_addr,
                std::piecewise_construct,
                std::forward_as_tuple(read_ts),
                std::forward_as_tuple(table_name->StringView()));
        }
        else
        {
            assert(table_name->Type() == TableType::Primary ||
                   table_name->Type() == TableType::Secondary ||
                   table_name->Type() == TableType::UniqueSecondary);
            std::tie(it, inserted) = data_rset_.try_emplace(
                cce_addr,
                std::piecewise_construct,
                std::forward_as_tuple(read_ts),
                std::forward_as_tuple(table_name->StringView()));
        }

        if (!inserted)
        {
            // (read_ts == 0) means it is a boundary key added gap lock or its
            // payload status is Unkonwn.
            if (it->second.first.version_ts_ < read_ts &&
                it->second.first.version_ts_ != 0)
            {
                // breaks repeatable read isolation level under
                // Occ/OccRead protocol, return error.
                return false;
            }
            else if (read_ts > 0)
            {
                it->second.first.version_ts_ = read_ts;
            }

            it->second.first.read_cnt_++;
        }
        return true;
    }

    /**
     * @brief Updates a data item's timestamp in the read set. The method is
     * called after a read-outside request. A read-outside request immediately
     * follows a read-inside request and is only issued if the initial
     * read-inside returns a record whose value is unknown. The read-outside
     * request retrieves the value and its commit timestamp from the data store
     * and updates the timestamp in the read set.
     *
     * @param cce_addr Cc entry address
     * @param version_ts Commit timestamp of the value retrieved from the data
     * store.
     */
    void UpdateRead(const CcEntryAddr &cce_addr, uint64_t version_ts)
    {
        auto read_it = data_rset_.find(cce_addr);
        if (read_it != data_rset_.end())
        {
            ReadSetEntry &rs_entry = read_it->second.first;
            rs_entry.version_ts_ = version_ts;
        }
    }

    /**
     * @brief Removes the read-set key given the cc entry's address.
     *
     * @param cce_addr The cc entry's address.
     * @return uint64_t Commit timestamp of the cc entry, if the specified
     * cc entry exists and is removed. 0, if the specified cc entry does not
     * exist.
     */
    uint64_t DedupRead(const CcEntryAddr &cce_addr)
    {
        uint64_t read_ts = 0;
        // Check the data read set
        auto read_it = data_rset_.find(cce_addr);
        if (read_it != data_rset_.end())
        {
            read_ts = read_it->second.first.version_ts_;
            data_rset_.erase(read_it);
            return read_ts;
        }

        // Check the meta data read set
        read_it = meta_data_rset_.find(cce_addr);
        if (read_it != meta_data_rset_.end())
        {
            read_ts = read_it->second.first.version_ts_;
            meta_data_rset_.erase(read_it);
        }

        return read_ts;
    }

    uint64_t DedupRead(const TableName &tbl_name, const CcEntryAddr &cce_addr)
    {
        uint64_t read_ts = 0;
        if (tbl_name.Type() == TableType::Primary ||
            tbl_name.Type() == TableType::Secondary ||
            tbl_name.Type() == TableType::UniqueSecondary)
        {
            auto cce_it = data_rset_.find(cce_addr);
            if (cce_it != data_rset_.end())
            {
                read_ts = cce_it->second.first.version_ts_;
                data_rset_.erase(cce_it);
            }
        }
        else
        {
            assert(tbl_name.IsMeta());
            auto cce_it = meta_data_rset_.find(cce_addr);
            if (cce_it != meta_data_rset_.end())
            {
                read_ts = cce_it->second.first.version_ts_;
                meta_data_rset_.erase(cce_it);
            }
        }

        return read_ts;
    }

    uint16_t GetReadCnt(const TableName &tbl_name, const CcEntryAddr &cce_addr)
    {
        uint16_t read_cnt = 0;
        if (tbl_name.Type() == TableType::Primary ||
            tbl_name.Type() == TableType::Secondary ||
            tbl_name.Type() == TableType::UniqueSecondary)
        {
            auto cce_it = data_rset_.find(cce_addr);
            if (cce_it != data_rset_.end())
            {
                read_cnt = cce_it->second.first.read_cnt_;
            }
        }
        else
        {
            assert(tbl_name.IsMeta());
            auto cce_it = meta_data_rset_.find(cce_addr);
            if (cce_it != meta_data_rset_.end())
            {
                read_cnt = cce_it->second.first.read_cnt_;
            }
        }
        return read_cnt;
    }

    TxErrorCode AddWrite(const TableName &table_name,
                         uint64_t schema_version,
                         TxKey tx_key,
                         TxRecord::Uptr rec,
                         OperationType op_type,
                         bool check_unique = false)
    {
        // Check write set bytes count.
        wset_bytes_cnt_ += ((tx_key.KeyPtr() ? tx_key.SerializedLength() : 0) +
                            (rec.get() ? rec.get()->SerializedLength() : 0));
        if (wset_bytes_cnt_ > ReadWriteSet::MaxWriteSetBytesCnt)
        {
            return TxErrorCode::WRITE_SET_BYTES_COUNT_EXCEED_ERR;
        }

        auto iter = wset_.find(table_name);
        if (iter == wset_.end())
        {
            auto insert_it = wset_.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(table_name.StringView(),
                                      table_name.Type(),
                                      table_name.Engine()),
                std::forward_as_tuple(schema_version, TableWriteSet()));
            iter = insert_it.first;
        }

        assert(!iter->first.IsStringOwner());

        TableWriteSet &tws = iter->second.second;
        auto [it, inserted] = tws.try_emplace(std::move(tx_key));
        if (inserted)
        {
            WriteSetEntry &wset_entry = it->second;
            wset_entry.op_ = op_type;
            wset_entry.rec_ = std::move(rec);
            ++wset_cnt_;
            return TxErrorCode::NO_ERROR;
        }
        else
        {
            WriteSetEntry &wset_entry = it->second;
            bool succeed = ApplyToWSetEntry(
                wset_entry, op_type, std::move(rec), check_unique);
            return succeed ? TxErrorCode::NO_ERROR : TxErrorCode::DUPLICATE_KEY;
        }
    }

    void AddCatalogWrite(TxKey tx_key, TxRecord::Uptr rec)
    {
        assert(rec->Size() > 0);
        auto [iter, inserted] = catalog_wset_.try_emplace(std::move(tx_key));
        ReplicaWriteSetEntry &wset_entry = iter->second;
        assert(inserted || wset_entry.op_ == OperationType::Update);
        wset_entry.rec_ = std::move(rec);
        wset_entry.op_ = OperationType::Update;
    }

    const WriteSetEntry *FindWrite(const TableName &table_name,
                                   const TxKey &key) const
    {
        auto tab_it = wset_.find(table_name);
        if (tab_it != wset_.end())
        {
            auto &tws = tab_it->second.second;
            auto key_it = tws.find(key);
            if (key_it != tws.end())
            {
                return &key_it->second;
            }
        }

        return nullptr;
    }

    void DeleteWrite(const TableName &table_name, const TxKey &key)
    {
        auto tab_it = wset_.find(table_name);
        assert(tab_it != wset_.end());

        auto &tws = tab_it->second.second;

        auto key_it = tws.find(key);
        assert(key_it != tws.end());

        wset_bytes_cnt_ -= (key_it->first.SerializedLength() +
                            key_it->second.rec_->SerializedLength());

        tws.erase(key_it);
        if (tws.size() == 0)
        {
            wset_.erase(tab_it);
        }
        --wset_cnt_;
    }

    std::pair<TableWriteSet::const_iterator, TableWriteSet::const_iterator>
    InitIter(const TableWriteSet &table_wset,
             const TxKey &start_key,
             bool inclusive)
    {
        auto it = table_wset.lower_bound(start_key);
        if (it != table_wset.end())
        {
            if (it->first == start_key && !inclusive)
            {
                ++it;
            }
        }
        return std::make_pair(it, table_wset.end());
    }

    std::pair<TableWriteSet::const_reverse_iterator,
              TableWriteSet::const_reverse_iterator>
    InitReverseIter(const TableWriteSet &table_wset,
                    const TxKey &start_key,
                    bool inclusive)
    {
        auto rit = std::make_reverse_iterator(table_wset.upper_bound(
            start_key));  // return key not large than start_key
        if (rit != table_wset.rend())
        {
            if (rit->first == start_key && !inclusive)
            {
                ++rit;
            }
        }
        return std::make_pair(rit, table_wset.rend());
    }

    /**
     * @brief Removes all read-set entries of data items, keeping catalog and
     * range read entries.
     *
     */
    bool ClearDataReadSet()
    {
        bool read_lock_term_valid = true;
        for (auto cce_it = data_rset_.begin(); cce_it != data_rset_.end();
             ++cce_it)
        {
            // Keep the read lock terms to check them when writing log.
            const CcEntryAddr cce_addr = cce_it->first;
            uint32_t ng_id = cce_addr.NodeGroupId();
            int64_t ng_term = cce_addr.Term();
            auto [it, success] =
                read_lock_ng_terms_.try_emplace(ng_id, ng_term);
            if (!success && it->second != ng_term)
            {
                LOG(ERROR) << "two reads on the same node group have "
                              "different terms, ng: "
                           << ng_id << ", terms: " << it->second << ", "
                           << ng_term;
                read_lock_term_valid = false;
            }
        }
        data_rset_.clear();

        assert(data_rset_.empty());
        return read_lock_term_valid;
    }

    void ClearWriteSet()
    {
        wset_.clear();
        wset_cnt_ = 0;
        wset_bytes_cnt_ = 0;
        forward_write_cnt_ = 0;
    }

    void ClearWriteSet(const TableName &table_name)
    {
        auto tab_it = wset_.find(table_name);
        if (tab_it != wset_.end())
        {
            const TableWriteSet &tab_wset = tab_it->second.second;
            assert(wset_cnt_ >= tab_wset.size());
            wset_cnt_ -= tab_wset.size();
            for (auto &key_it : tab_wset)
            {
                wset_bytes_cnt_ -= key_it.first.SerializedLength();
                wset_bytes_cnt_ -= key_it.second.rec_ != nullptr
                                       ? key_it.second.rec_->SerializedLength()
                                       : 0;
                forward_write_cnt_ -= key_it.second.forward_addr_.size();
            }
            wset_.erase(tab_it);
        }
    }

    std::unordered_map<TableName, std::pair<uint64_t, TableWriteSet>> &
    WriteSet()
    {
        return wset_;
    }

    const std::map<TxKey, ReplicaWriteSetEntry> &CatalogWriteSet() const
    {
        return catalog_wset_;
    }

    void ClearCatalogWriteSet()
    {
        catalog_wset_.clear();
    }

    void ClearReadSet(const TableName &table_name)
    {
        assert(table_name.Type() == TableType::Primary ||
               table_name.Type() == TableType::UniqueSecondary);
        for (auto cce_it = data_rset_.begin(); cce_it != data_rset_.end();)
        {
            if (cce_it->second.second == table_name.StringView())
            {
                data_rset_.erase(cce_it++);
            }
            else
            {
                ++cce_it;
            }
        }

        for (auto cce_it = meta_data_rset_.begin();
             cce_it != meta_data_rset_.end();)
        {
            if (cce_it->second.second == table_name.StringView())
            {
                meta_data_rset_.erase(cce_it++);
            }
            else
            {
                ++cce_it;
            }
        }
    }

    uint16_t RemoveDataReadEntry(const TableName &table_name,
                                 const CcEntryAddr &addr)
    {
        assert(!table_name.IsMeta());

        uint16_t read_cnt = 0;
        auto cce_it = data_rset_.find(addr);
        if (cce_it != data_rset_.end())
        {
            ReadSetEntry &read_entry = cce_it->second.first;
            assert(read_entry.read_cnt_ > 0);
            read_cnt = --read_entry.read_cnt_;
            if (read_cnt == 0)
            {
                data_rset_.erase(cce_it);
            }
        }

        return read_cnt;
    }

    void ResetForwardWriteCount()
    {
        forward_write_cnt_ = 0;
    }

private:
    /**
     * @brief Apply an operation to an existing wset_entry.
     * @return [false] means duplicate key conflict. [true] means apply new
     * operator successfully.
     */
    static bool ApplyToWSetEntry(WriteSetEntry &wset_entry,
                                 OperationType op,
                                 TxRecord::Uptr rec,
                                 bool check_unique)
    {
        if (wset_entry.op_ == OperationType::Insert)
        {
            if (op == OperationType::Insert)
            {
                // duplicate key
                if (check_unique)
                {
                    return false;
                }
                else
                {
                    wset_entry.op_ = OperationType::Insert;
                    wset_entry.rec_ = std::move(rec);
                    return true;
                }
            }
            else if (op == OperationType::Update)
            {
                wset_entry.op_ = OperationType::Insert;
                wset_entry.rec_ = std::move(rec);
                return true;
            }
            else if (op == OperationType::Delete)
            {
                wset_entry.op_ = OperationType::Delete;
                wset_entry.rec_ = nullptr;
                return true;
            }
            else
            {
                assert(false);
                return false;
            }
        }
        else if (wset_entry.op_ == OperationType::Update)
        {
            if (op == OperationType::Insert)
            {
                assert(false);
                return false;
            }
            else if (op == OperationType::Update)
            {
                wset_entry.op_ = OperationType::Update;
                wset_entry.rec_ = std::move(rec);
                return true;
            }
            else if (op == OperationType::Delete)
            {
                wset_entry.op_ = OperationType::Delete;
                wset_entry.rec_ = nullptr;
                return true;
            }
            else
            {
                assert(false);
                return false;
            }
        }
        else if (wset_entry.op_ == OperationType::Delete)
        {
            if (op == OperationType::Insert)
            {
                wset_entry.op_ = OperationType::Update;
                wset_entry.rec_ = std::move(rec);
                return true;
            }
            else if (op == OperationType::Update)
            {
                assert(false);
                return false;
            }
            else if (op == OperationType::Delete)
            {
                wset_entry.op_ = OperationType::Delete;
                wset_entry.rec_ = nullptr;
                return true;
            }
            else
            {
                assert(false);
                return false;
            }
        }
        else
        {
            assert(false);
            return false;
        }
    }

private:
    // rset_, wset_cnt_, read_cache_ are not string owner.
    // data read entry map
    absl::flat_hash_map<CcEntryAddr,
                        std::pair<ReadSetEntry, const std::string_view>>
        data_rset_;
    absl::flat_hash_map<CcEntryAddr,
                        std::pair<ReadSetEntry, const std::string_view>>
        meta_data_rset_;
    // the terms of read locks, for write log term check
    absl::flat_hash_map<uint32_t, int64_t> read_lock_ng_terms_;
    std::unordered_map<TableName, std::pair<uint64_t, TableWriteSet>> wset_;
    size_t wset_cnt_;
    size_t wset_bytes_cnt_;
    size_t forward_write_cnt_;

    // Logically alter a table inside a DML transaction.
    std::map<TxKey, ReplicaWriteSetEntry> catalog_wset_;
};
}  // namespace txservice
