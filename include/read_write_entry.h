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
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cc_entry.h"
#include "tx_command.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
struct WriteSetEntry
{
    using Uptr = std::unique_ptr<WriteSetEntry>;

    WriteSetEntry() : rec_(nullptr), op_(OperationType::Upsert), cce_addr_()
    {
    }

    WriteSetEntry(const WriteSetEntry &other) = delete;

    WriteSetEntry(WriteSetEntry &&other) noexcept
        : rec_(std::move(other.rec_)),
          op_(other.op_),
          cce_addr_(other.cce_addr_),
          key_shard_code_(other.key_shard_code_),
          forward_addr_(std::move(other.forward_addr_))
    {
    }

    WriteSetEntry &operator=(WriteSetEntry &&other) noexcept
    {
        rec_ = std::move(other.rec_);
        op_ = other.op_;
        cce_addr_ = other.cce_addr_;
        key_shard_code_ = other.key_shard_code_;
        forward_addr_ = std::move(other.forward_addr_);

        return *this;
    }

    TxRecord::Uptr rec_;
    OperationType op_;
    CcEntryAddr cce_addr_;
    uint32_t key_shard_code_{};
    // Used in double write scenarios during online DDL.
    std::unordered_map<uint32_t, CcEntryAddr> forward_addr_;
};

/**
 * @brief Meta table is replication table. Most meta table update is done by
 * 2-PC, even DML trigger DDL.
 */
struct ReplicaWriteSetEntry
{
    using Uptr = std::unique_ptr<ReplicaWriteSetEntry>;

    TxRecord::Uptr rec_;
    OperationType op_;
};

struct ReadSetEntry
{
    ReadSetEntry() = delete;
    explicit ReadSetEntry(uint64_t ts) : version_ts_(ts)
    {
    }

    uint64_t version_ts_;
    uint16_t read_cnt_{1};
};

struct ScanSetEntry
{
    using Uptr = std::unique_ptr<ScanSetEntry>;

    ScanSetEntry() : key_ts_(0), gap_ts_(0), cce_addr_()
    {
    }

    ScanSetEntry(const ScanSetEntry &rhs) = delete;

    ScanSetEntry(ScanSetEntry &&other) noexcept
        : key_ts_(other.key_ts_),
          gap_ts_(other.gap_ts_),
          cce_addr_(other.cce_addr_)
    {
    }

    void Reset(uint64_t begin_ts, const CcEntryAddr &addr)
    {
        key_ts_ = begin_ts;
        gap_ts_ = begin_ts;
        cce_addr_ = addr;
    }

    uint64_t key_ts_;
    uint64_t gap_ts_;
    CcEntryAddr cce_addr_;
};

struct CmdForwardEntry
{
    TxKey key_;
    uint32_t key_shard_code_;
    CcEntryAddr cce_addr_;

    CmdForwardEntry(TxKey &&key, uint32_t key_shard)
        : key_(std::move(key)), key_shard_code_(key_shard), cce_addr_()
    {
    }
};

/**
 * Txn commands on the same object.
 */
struct CmdSetEntry
{
    CmdSetEntry(uint64_t object_version,
                uint64_t last_vali_ts,
                std::string &&key,
                bool apply_on_deleted)
        : object_version_(object_version),
          last_vali_ts_(last_vali_ts),
          obj_key_str_(std::move(key)),
          ignore_previous_version_(apply_on_deleted)
    {
    }

    void AddCommand(const TxCommand *cmd, uint64_t ttl)
    {
        assert(cmd != nullptr);
        if (cmd->IsOverwrite())
        {
            // clear all the commands since we don't need to write them into log
            cmd_str_list_.clear();
            ignore_previous_version_ = true;
        }

        ttl_ = ttl;

        std::string cmd_str;
        cmd->Serialize(cmd_str);
        cmd_str_list_.emplace_back(std::move(cmd_str));
    }

    // No need to write to the log if there is no successful command.
    bool HasSuccessfulCommand() const
    {
        return object_modified_;
    }

    // commit_ts of the object cce when the commands apply to it, commands on
    // the same object must apply in commit_ts order
    uint64_t object_version_{};
    // The cce's last_validation ts, for setting commit ts of this txn.
    uint64_t last_vali_ts_{};
    // TTL of the object after the commands apply to it.
    uint64_t ttl_{UINT64_MAX};
    // Whether the cc entry's object is modified by the cmd.
    bool object_modified_{};
    // serialized key, for writing log
    std::string obj_key_str_{};
    // serialized commands, for writing log
    std::vector<std::string> cmd_str_list_{};
    // Whether a overwrite command exists. If true, commands before this cmd are
    // discarded since there is no point writing them into the log.
    bool ignore_previous_version_{};
    // Store the forward write key_shard_code info If this key's bucket is in
    // migration
    std::unique_ptr<CmdForwardEntry> forward_entry_{nullptr};
};

}  // namespace txservice
