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

#include "tx_id.h"
#include "type.h"

namespace txservice
{
/*
 * TEntry describes a transaction.
 */
struct TEntry
{
public:
    using Uptr = std::unique_ptr<TEntry>;

    TEntry() = delete;
    TEntry(const TEntry &rhs) = delete;

    TEntry(uint32_t vec_idx)
        : commit_ts_(0),
          lower_bound_(0),
          status_(TxnStatus::Finished),
          ident_(UINT32_MAX),
          vec_idx_(vec_idx)
    {
    }

    TEntry(TEntry &&rhs) noexcept
        : commit_ts_(rhs.commit_ts_),
          lower_bound_(rhs.lower_bound_),
          status_(rhs.status_),
          ident_(rhs.ident_),
          vec_idx_(rhs.vec_idx_)
    {
    }

    void Reset(uint64_t start_ts, uint32_t tx_ident, int64_t term)
    {
        ident_ = tx_ident;
        commit_ts_ = 0;
        lower_bound_ = start_ts;
        status_ = TxnStatus::Ongoing;
        term_ = term;
    }

    TxId GetTxId(uint32_t core_id) const
    {
        return TxId(core_id, ident_, vec_idx_);
    }

    uint64_t commit_ts_;
    uint64_t lower_bound_;  // start_ts
    TxnStatus status_;
    uint32_t ident_;
    const uint32_t vec_idx_;
    int64_t term_;
};

}  // namespace txservice
