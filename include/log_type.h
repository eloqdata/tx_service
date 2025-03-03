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

#include "log.pb.h"
#include "type.h"

namespace txlog
{
class ToRemoteType
{
public:
    static CcTableType ConvertTableType(txservice::TableType table_type)
    {
        switch (table_type)
        {
        case txservice::TableType::Primary:
            return CcTableType::Primary;
        case txservice::TableType::Secondary:
            return CcTableType::Secondary;
        case txservice::TableType::Catalog:
            return CcTableType::Catalog;
        case txservice::TableType::RangePartition:
            return CcTableType::RangePartition;
        default:
            assert(false);
            return CcTableType::Primary;
        }
    }
};

class ToLocalType
{
public:
    static txservice::TableType ConvertCcTableType(CcTableType table_type)
    {
        switch (table_type)
        {
        case CcTableType::Primary:
            return txservice::TableType::Primary;
        case CcTableType::Secondary:
            return txservice::TableType::Secondary;
        case CcTableType::Catalog:
            return txservice::TableType::Catalog;
        default:
            return txservice::TableType::RangePartition;
        }
    }
};

}  // namespace txlog
