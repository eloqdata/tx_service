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

#include "tx_key.h"

namespace txservice
{
enum struct SliceStatus
{
    /**
     * @brief The slice's data are not fully cached in memory.
     *
     */
    PartiallyCached = 0,
    /**
     * @brief The slice's data are fully cached in memory.
     *
     */
    FullyCached,
    /**
     * @brief The slice's data are being loaded into memory. The flag prevents
     * cache replacement from kicking out records from the slice while a cc
     * request is loading the slice's records into cc maps.
     *
     */
    BeingLoaded
};

struct SliceInitInfo
{
    SliceInitInfo() = delete;
    explicit SliceInitInfo(TxKey key, uint32_t size, SliceStatus status)
        : key_(std::move(key)), size_(size), status_(status)
    {
    }

    SliceInitInfo(SliceInitInfo &&rhs)
        : key_(std::move(rhs.key_)), size_(rhs.size_), status_(rhs.status_)
    {
    }

    SliceInitInfo(const SliceInitInfo &rhs) = delete;

    TxKey key_;
    uint32_t size_;
    SliceStatus status_;
};
}  // namespace txservice