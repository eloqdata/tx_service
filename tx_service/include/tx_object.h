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

#include "tx_record.h"

namespace txservice
{
class PagedTxObject;

struct TxObject : public TxRecord
{
public:
    TxObject() = default;

    TxObject(const TxObject &obj)
    {
    }

    TxObject &operator=(const TxObject &obj)
    {
        return *this;
    }

    virtual TxRecord::Uptr DeserializeObject(const char *buf,
                                             size_t &offset) const
    {
        return nullptr;
    }

    /**
     * @brief Length-bounded, FALLIBLE variant for store-sourced rows: `avail`
     * is the row's byte count and a malformed row yields nullptr instead of
     * undefined reads (eloqkv docs/08 §5 "nothing from the store is trusted").
     * Default delegates to the legacy unbounded form so existing types keep
     * their behavior; types with store-side validation override.
     */
    virtual TxRecord::Uptr DeserializeObject(const char *buf,
                                             size_t avail,
                                             size_t &offset) const
    {
        (void) avail;
        return DeserializeObject(buf, offset);
    }

    /**
     * @brief The representation query for paged large objects (eloqkv
     * docs/08-paged-objects.md §6): non-null iff this payload is paged. The
     * engine's page fetch/flush paths and the protocol layer's per-command
     * dispatch both branch on it; monolithic objects inherit the null
     * default and are untouched.
     */
    virtual PagedTxObject *AsPaged()
    {
        return nullptr;
    }

    virtual const PagedTxObject *AsPaged() const
    {
        return nullptr;
    }
};
}  // namespace txservice
