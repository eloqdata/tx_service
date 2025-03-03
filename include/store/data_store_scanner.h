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

namespace txservice
{
class TxKey;
struct TxRecord;
namespace store
{
class DataStoreScanner
{
public:
    virtual ~DataStoreScanner() = default;
    virtual void Current(TxKey &key,
                         const txservice::TxRecord *&rec,
                         uint64_t &version_ts_,
                         bool &deleted_) = 0;
    virtual bool MoveNext() = 0;
    virtual void End() = 0;
};
}  // namespace store
}  // namespace txservice
