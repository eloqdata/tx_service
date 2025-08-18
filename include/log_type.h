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
    static constexpr CcTableType ConvertTableType(
        txservice::TableType table_type)
    {
        switch (table_type)
        {
        case txservice::TableType::Primary:
            return CcTableType::Primary;
        case txservice::TableType::Secondary:
            return CcTableType::Secondary;
        case txservice::TableType::UniqueSecondary:
            return CcTableType::UniqueSecondary;
        case txservice::TableType::Catalog:
            return CcTableType::Catalog;
        case txservice::TableType::RangePartition:
            return CcTableType::RangePartition;
        case txservice::TableType::RangeBucket:
            return CcTableType::RangeBucket;
        case txservice::TableType::ClusterConfig:
            return CcTableType::ClusterConfig;
        default:
            assert(false);
            return CcTableType::Primary;
        }
    }

    static TableEngine ConvertTableEngine(txservice::TableEngine table_engine)
    {
        switch (table_engine)
        {
        case txservice::TableEngine::None:
            return TableEngine::None;
        case txservice::TableEngine::EloqSql:
            return TableEngine::EloqSql;
        case txservice::TableEngine::EloqDoc:
            return TableEngine::EloqDoc;
        case txservice::TableEngine::EloqKv:
            return TableEngine::EloqKv;
        case txservice::TableEngine::Sequence:
            return TableEngine::Sequence;
        default:
            assert(false);
            return TableEngine::None;
        }
    }
};

class ToLocalType
{
public:
    static constexpr txservice::TableType ConvertCcTableType(
        CcTableType table_type)
    {
        switch (table_type)
        {
        case CcTableType::Primary:
            return txservice::TableType::Primary;
        case CcTableType::Secondary:
            return txservice::TableType::Secondary;
        case CcTableType::UniqueSecondary:
            return txservice::TableType::UniqueSecondary;
        case CcTableType::Catalog:
            return txservice::TableType::Catalog;
        case CcTableType::RangeBucket:
            return txservice::TableType::RangeBucket;
        case CcTableType::ClusterConfig:
            return txservice::TableType::ClusterConfig;
        default:
            assert(false);
            return txservice::TableType::Primary;
        }
    }

    static txservice::TableEngine ConvertTableEngine(TableEngine table_engine)
    {
        switch (table_engine)
        {
        case TableEngine::None:
            return txservice::TableEngine::None;
        case TableEngine::EloqSql:
            return txservice::TableEngine::EloqSql;
        case TableEngine::EloqKv:
            return txservice::TableEngine::EloqKv;
        case TableEngine::EloqDoc:
            return txservice::TableEngine::EloqDoc;
        case TableEngine::Sequence:
            return txservice::TableEngine::Sequence;
        default:
            assert(false);
            return txservice::TableEngine::None;
        }
    }
};

}  // namespace txlog
