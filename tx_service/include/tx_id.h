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

#include <functional>

namespace txservice
{
class CcShard;
class LocalCcHandler;

using TxNumber = uint64_t;

struct TxId
{
public:
    TxId(uint32_t global_core_id = UINT32_MAX)
        : global_core_id_(global_core_id), ident_(0), vec_idx_(0)
    {
    }

    TxId(uint32_t global_core_id, uint32_t ident_, uint32_t vec_idx)
        : global_core_id_(global_core_id), ident_(ident_), vec_idx_(vec_idx)
    {
    }

    TxId(const TxId &that)
    {
        global_core_id_ = that.global_core_id_;
        ident_ = that.ident_;
        vec_idx_ = that.vec_idx_;
    }

    TxId &operator=(const TxId &rhs)
    {
        global_core_id_ = rhs.global_core_id_;
        ident_ = rhs.ident_;
        vec_idx_ = rhs.vec_idx_;

        return *this;
    }

    bool operator==(const TxId &other) const
    {
        return (global_core_id_ == other.global_core_id_) &&
               (ident_ == other.ident_);
    }

    bool operator==(const uint64_t &tx_number) const
    {
        return TxNumber() == tx_number;
    }

    void Reset(uint32_t core_id = UINT32_MAX,
               uint32_t ident = 0,
               uint32_t idx = 0)
    {
        global_core_id_ = core_id;
        ident_ = ident;
        vec_idx_ = idx;
    }

    bool Empty() const
    {
        return global_core_id_ == UINT32_MAX;
    }

    uint64_t TxNumber() const
    {
        uint64_t tid = global_core_id_;
        return (tid << 32L) | ident_;
    }

    uint32_t GlobalCoreId() const
    {
        return global_core_id_;
    }

    uint32_t Identity() const
    {
        return ident_;
    }

    uint32_t VecIdx() const
    {
        return vec_idx_;
    }

    uint32_t GetNodeId()
    {
        // For a global core ID, the lower 10 bits represents the local core
        // ID and the remaining higher bits represent the ccnode ID.
        return GlobalCoreId() >> 10;
    }

private:
    // transaction identifier: node_id + core_id
    uint32_t global_core_id_;
    // transaction identifier: tx_no inside a core.
    uint32_t ident_;
    // index in transaction array, it is used to point search the txentry in tx
    // array. Keep in mind that tx array in ccshard is the ground truth of the
    // status of a transaction.
    uint32_t vec_idx_;

    friend struct std::hash<txservice::TxId>;
    friend class CcShard;
    friend class LocalCcHandler;
};
}  // namespace txservice

namespace std
{
template <>
struct hash<txservice::TxId>
{
    size_t operator()(const txservice::TxId &txid) const
    {
        return txid.TxNumber();
    }
};
}  // namespace std
