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
#include "tx_command.h"

namespace txservice
{
extern bool txservice_skip_wal;

class CommandSet
{
    static const uint32_t MaxWriteSetBytesCnt = 62 * 1024 * 1024;

public:
    CommandSet()
        : cmd_set_internal_(),
          cce_with_writelock_size_(0),
          need_forward_cmd_cnt_(0)
    {
    }

    void Reset()
    {
        cmd_set_internal_.clear();
        cce_with_writelock_size_ = 0;
        need_forward_cmd_cnt_ = 0;
    }

    void AddObjectCommand(const TableName &table_name,
                          const CcEntryAddr &cce_addr,
                          RecordStatus payload_status,
                          uint64_t cce_version,
                          uint64_t lock_ts,
                          uint64_t last_vali_ts,
                          const TxKey *key,
                          const TxCommand *cmd,
                          uint64_t ttl,
                          uint32_t forward_key_shard = UINT32_MAX)
    {
        auto [table_it, success] = cmd_set_internal_.try_emplace(table_name);
        std::unordered_map<CcEntryAddr, CmdSetEntry> &table_cmd_set =
            table_it->second;

        auto cce_it = table_cmd_set.find(cce_addr);
        if (cce_it == table_cmd_set.end())
        {
            std::string key_str;
            key->Serialize(key_str);
            bool inserted = false;
            bool cmd_apply_on_deleted =
                (payload_status == RecordStatus::Deleted);

            std::tie(cce_it, inserted) =
                table_cmd_set.try_emplace(cce_addr,
                                          cce_version,
                                          lock_ts,
                                          last_vali_ts,
                                          std::move(key_str),
                                          cmd_apply_on_deleted);
            assert(inserted);
            cce_with_writelock_size_++;
        }

        CmdSetEntry &entry = cce_it->second;
        assert(cce_version >= entry.object_version_);
        entry.object_version_ = cce_version;
        if (cmd != nullptr)
        {
            entry.object_modified_ = true;
            // The command modifies the object and wal is enabled. Put it
            // into the command set for writing log and post-processing. If
            // the command fails, only to release the write lock.
            if (!txservice_skip_wal)
            {
                entry.AddCommand(cmd, ttl);
            }

            if (forward_key_shard != UINT32_MAX &&
                entry.forward_entry_ == nullptr)
            {
                assert(cmd != nullptr);
                entry.forward_entry_ = std::make_unique<CmdForwardEntry>(
                    key->Clone(), forward_key_shard);
                need_forward_cmd_cnt_++;
            }
        }
    }

    const CmdSetEntry *FindObjectCommand(const TableName &table_name,
                                         const CcEntryAddr &cce_addr) const
    {
        const CmdSetEntry *obj_cmd_entry = nullptr;
        const auto iter = cmd_set_internal_.find(table_name);
        if (iter != cmd_set_internal_.end())
        {
            const auto &[table_name, obj_cmd_set] = *iter;
            const auto it = obj_cmd_set.find(cce_addr);
            if (it != obj_cmd_set.end())
            {
                obj_cmd_entry = &it->second;
            }
        }
        return obj_cmd_entry;
    }

    const std::unordered_map<TableName,
                             std::unordered_map<CcEntryAddr, CmdSetEntry>> *
    ObjectCommandSet() const
    {
        return &cmd_set_internal_;
    }

    // The number of objects(cce) that already acquired write lock.
    uint32_t ObjectCntWithWriteLock() const
    {
        return cce_with_writelock_size_;
    }

    void IncreaseObjectCntWithWriteLock()
    {
        cce_with_writelock_size_++;
    }

    bool ObjectModified() const
    {
        for (const auto &[table_name, obj_cmd_set] : cmd_set_internal_)
        {
            for (const auto &[cce_addr, obj_cmd_entry] : obj_cmd_set)
            {
                if (obj_cmd_entry.HasSuccessfulCommand())
                {
                    return true;
                }
            }
        }
        return false;
    }

    size_t ObjectCountToForwardWrite()
    {
        return need_forward_cmd_cnt_;
    }

private:
    /**
     * Collection of object keys and commands on each object.
     */
    std::unordered_map<TableName, std::unordered_map<CcEntryAddr, CmdSetEntry>>
        cmd_set_internal_;

    // the count of different cc entries the command set contains that acquires
    // writelock
    uint32_t cce_with_writelock_size_{};
    // the count of cmd keys to acquire write lock on forward node group
    uint32_t need_forward_cmd_cnt_{0};
};
}  // namespace txservice
