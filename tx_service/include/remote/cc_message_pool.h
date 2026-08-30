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
 */
#pragma once

#include <bthread/moodycamelqueue.h>

#include <memory>
#include <utility>

#include "proto/cc_request.pb.h"

namespace txservice::remote
{
/**
 * @brief Thread-safe reuse pool for CC protobuf messages.
 *
 * Recycle clears a message before making it available to another thread. This
 * keeps the idle pool from retaining payload allocations that ParseFrom would
 * discard before the next use anyway.
 */
class CcMessagePool
{
public:
    CcMessagePool() = default;
    CcMessagePool(const CcMessagePool &) = delete;
    CcMessagePool &operator=(const CcMessagePool &) = delete;
    CcMessagePool(CcMessagePool &&) = delete;
    CcMessagePool &operator=(CcMessagePool &&) = delete;

    /**
     * @brief Returns an empty pooled message or allocates a new one.
     *
     * The caller owns the returned message until passing it to Recycle().
     */
    std::unique_ptr<CcMessage> Acquire()
    {
        std::unique_ptr<CcMessage> msg;
        if (!pool_.try_dequeue(msg))
        {
            msg = std::make_unique<CcMessage>();
        }
        return msg;
    }

    /**
     * @brief Clears and transfers a message back to the shared pool.
     *
     * @param msg A non-null message exclusively owned by the caller.
     */
    void Recycle(std::unique_ptr<CcMessage> msg)
    {
        msg->Clear();
        pool_.enqueue(std::move(msg));
    }

private:
    moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> pool_;
};
}  // namespace txservice::remote
