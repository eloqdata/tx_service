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
// Let Catch provide main():
#include <catch2/catch_all.hpp>

#include "cc/cc_req_base.h"
#include "cc/non_blocking_lock.h"
#include "cc_protocol.h"

namespace
{
/**
 * @brief Minimal CcRequestBase with a settable tx number. NonBlockingLock only
 * reads Txn() off the queued requests, so Execute() is never reached.
 */
struct StubCcRequest : public txservice::CcRequestBase
{
    explicit StubCcRequest(txservice::TxNumber txn)
    {
        tx_number_ = txn;
        proto_ = txservice::CcProtocol::Locking;
        isolation_level_ = txservice::IsolationLevel::RepeatableRead;
    }

    bool Execute(txservice::CcShard &) override
    {
        return false;
    }
};

}  // namespace

using txservice::CcProtocol;
using txservice::NonBlockingLock;

TEST_CASE("write intent holder's upgrade is queued at the head",
          "[NonBlockingLock]")
{
    // The holder of a WriteIntent cannot be satisfied by waiting behind other
    // requests on the same entry: they all conflict with the intent it still
    // holds, so none of them can be granted until it finishes and releases.
    // Its WriteIntent -> WriteLock upgrade must therefore jump the queue.
    constexpr txservice::TxNumber kHolder = 1001;
    constexpr txservice::TxNumber kOtherWriter = 1002;
    constexpr txservice::TxNumber kReader = 2001;

    NonBlockingLock lock;
    StubCcRequest holder_intent{kHolder};
    StubCcRequest other_intent{kOtherWriter};
    StubCcRequest holder_upgrade{kHolder};

    REQUIRE(lock.AcquireWriteIntent(&holder_intent, CcProtocol::Locking));

    // A read lock coexists with a write intent, and is what will keep the
    // upgrade from being granted outright.
    REQUIRE(lock.AcquireReadLockFast(kReader));

    // A second writer conflicts with the held intent and queues.
    REQUIRE_FALSE(lock.AcquireWriteIntent(&other_intent, CcProtocol::Locking));

    // The holder now upgrades. It is blocked by the outstanding read lock, so
    // it queues -- but ahead of the other writer, not behind it.
    REQUIRE_FALSE(lock.AcquireWriteLock(&holder_upgrade, CcProtocol::Locking));

    std::vector<txservice::TxNumber> queued = lock.GetBlockTxIds(0);
    REQUIRE(queued.size() == 2);
    REQUIRE(queued[0] == kHolder);       // upgrade at the head
    REQUIRE(queued[1] == kOtherWriter);  // the earlier request behind it
}

TEST_CASE("upgrade still waits for outstanding read locks", "[NonBlockingLock]")
{
    // Head placement must not turn into an early grant: while a read lock is
    // outstanding the upgrade stays queued, and it is only grantable once the
    // reader is gone.
    constexpr txservice::TxNumber kHolder = 1001;
    constexpr txservice::TxNumber kReader = 2001;

    NonBlockingLock lock;
    StubCcRequest holder_intent{kHolder};
    StubCcRequest holder_upgrade{kHolder};

    REQUIRE(lock.AcquireWriteIntent(&holder_intent, CcProtocol::Locking));
    REQUIRE(lock.AcquireReadLockFast(kReader));

    // Blocked by the read lock, not granted.
    REQUIRE_FALSE(lock.AcquireWriteLock(&holder_upgrade, CcProtocol::Locking));
    REQUIRE(lock.FindQueueRequest(kHolder));

    // With no reader in the way the same upgrade succeeds outright, which is
    // the state TryPopBlockingQueue() reaches once the reader releases.
    NonBlockingLock unblocked;
    StubCcRequest intent2{kHolder};
    StubCcRequest upgrade2{kHolder};
    REQUIRE(unblocked.AcquireWriteIntent(&intent2, CcProtocol::Locking));
    REQUIRE(unblocked.AcquireWriteLock(&upgrade2, CcProtocol::Locking));
}

TEST_CASE("a write lock request from a non-holder still queues FIFO",
          "[NonBlockingLock]")
{
    // Regression guard: only the intent holder's own upgrade jumps the queue.
    constexpr txservice::TxNumber kHolder = 1001;
    constexpr txservice::TxNumber kQueuedFirst = 1002;
    constexpr txservice::TxNumber kQueuedSecond = 1003;

    NonBlockingLock lock;
    StubCcRequest holder_intent{kHolder};
    StubCcRequest first_intent{kQueuedFirst};
    StubCcRequest second_write{kQueuedSecond};

    REQUIRE(lock.AcquireWriteIntent(&holder_intent, CcProtocol::Locking));
    REQUIRE_FALSE(lock.AcquireWriteIntent(&first_intent, CcProtocol::Locking));
    REQUIRE_FALSE(lock.AcquireWriteLock(&second_write, CcProtocol::Locking));

    std::vector<txservice::TxNumber> queued = lock.GetBlockTxIds(0);
    REQUIRE(queued.size() == 2);
    REQUIRE(queued[0] == kQueuedFirst);
    REQUIRE(queued[1] == kQueuedSecond);
}
