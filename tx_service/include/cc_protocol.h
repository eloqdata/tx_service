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

#include <cassert>  // assert
#include <cstdint>

namespace txservice
{

/**
 * @brief
 * - "Optimistic Read"  : Read intent or no intent. No conflict with anyone.
 *
 * - "Pessimistic Read" : ReadLock. Conflict: block and wait.
 *
 * - "Optimistic Write" : WriteLock and WriteIntent. Confilict: back off and
 * retry.
 *
 * - "Pessimistic Write" : WriteLock and WriteIntent. Confilict: block and wait.
 *
 */
enum class CcProtocol
{
    OCC = 0,  // Optimistic Read + Optimistic Write
    OccRead,  // Optimistic Read + Pessimistic Write
    Locking,  // Pessimistic Read + Pessimistic Write
};

enum class CcOperation : uint8_t
{
    Read = 0,
    ReadForWrite,
    Write,
    ReadSkIndex
};

/**
 * @brief
 * - "Snapshot isolation level" can be accomplished only using "OCC" or
 * "OccRead" CcProtocol.
 *
 * - "ReadCommitted"/"RepeatableRead"/"Serializable" can be accomplished using
 * all CcProtocol.
 *
 */
enum class IsolationLevel
{
    ReadCommitted = 0,
    Snapshot,
    RepeatableRead,
    Serializable
};

enum class LockType : uint8_t
{
    NoLock = 0,
    ReadIntent,
    ReadLock,
    WriteIntent,
    WriteLock,
};

enum class LockOpStatus
{
    Successful = 0,
    Failed,
    Blocked
};

class LockTypeUtil
{
public:
    static LockType DeduceLockType(CcOperation cc_op,
                                   IsolationLevel iso_level,
                                   CcProtocol cc_protocol,
                                   bool is_covering_keys)
    {
        if (cc_op == CcOperation::ReadSkIndex)
        {
            if (iso_level == IsolationLevel::Snapshot ||
                (iso_level == IsolationLevel::ReadCommitted &&
                 is_covering_keys))
            {
                return LockType::NoLock;
            }
            else
            {
                return LockType::ReadLock;
            }
        }
        else if (cc_op == CcOperation::ReadForWrite)
        {
            return LockType::WriteIntent;
        }
        else if (cc_op == CcOperation::Write)
        {
            return LockType::WriteLock;
        }
        else if (cc_op == CcOperation::Read)
        {
            switch (iso_level)
            {
            case IsolationLevel::ReadCommitted:
            case IsolationLevel::Snapshot:
                return LockType::NoLock;
            case IsolationLevel::RepeatableRead:
            case IsolationLevel::Serializable:
                if (cc_protocol == CcProtocol::Locking)
                {
                    return LockType::ReadLock;
                }
                else
                {
                    return LockType::ReadIntent;
                }
            default:
                assert(false);
                return LockType::NoLock;
            }
        }

        assert(false);
        return LockType::NoLock;
    }
};

}  // namespace txservice
