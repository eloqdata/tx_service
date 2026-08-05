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
// The Phase 3 merge blocker (eloqkv docs/08-paged-objects-plan.md): pins
// NonBlockingLock::WouldAcquireLock to NonBlockingLock::AcquireLock across the
// whole lock lattice.
//
// Why this test is load-bearing rather than nice-to-have: the paged Yield
// protocol defers lock acquisition until ExecuteOn is known to complete
// (docs/08-paged-objects.md §6), and its correctness rests on test-then-acquire
// being atomic on the shard core — "if the test says grantable and ExecuteOn
// did not yield, the acquisition cannot fail." Any state in which the
// prediction and the real acquisition disagree breaks that argument, so this
// test enumerates lock states exhaustively and compares the two directly.
//
// It also asserts the *other* half of the contract, which is easy to lose in a
// refactor: a prediction must not mutate the lock. Every case snapshots the
// observable lock state before predicting and requires it unchanged after.

// Engine headers FIRST, then Catch2, and the order is load-bearing: both glog
// (pulled in transitively by the engine headers) and Catch2 define a CHECK
// macro, and whichever lands last wins. With Catch2 first, every CHECK here
// would silently compile as a *fatal glog assertion* — aborting the process on
// the first mismatch instead of reporting it, and reporting nothing at all
// about the cases after it.
//
// clang-format must not sort these into its usual system-then-project order,
// which would reintroduce exactly that bug.
// clang-format off
#include "cc/cc_req_base.h"
#include "cc/non_blocking_lock.h"

#include <catch2/catch_all.hpp>
// clang-format on

#include <memory>
#include <string>
#include <vector>

namespace txservice
{
namespace
{
/**
 * @brief A CcRequestBase that exists only to carry a tx number into the
 * acquisition paths. Execute() is never called: these tests drive the lock
 * directly rather than through a shard.
 */
struct StubCcRequest : public CcRequestBase
{
    explicit StubCcRequest(TxNumber txn)
    {
        tx_number_ = txn;
    }

    bool Execute(CcShard &ccs) override
    {
        (void) ccs;
        assert(false && "stub request is never executed");
        return false;
    }
};

// The transactions used to build lock states. "Self" is the txn under test,
// so cases where the holder is Self exercise the already-held fast paths.
constexpr TxNumber kSelf = 1001;
constexpr TxNumber kOther = 2002;
constexpr TxNumber kThird = 3003;

/**
 * @brief One reproducible lock state, built from scratch per case. Requests
 * outlive the lock so blocking-queue entries never dangle.
 */
struct LockFixture
{
    NonBlockingLock lock_;
    std::vector<std::unique_ptr<StubCcRequest>> reqs_;

    StubCcRequest *Req(TxNumber txn)
    {
        reqs_.push_back(std::make_unique<StubCcRequest>(txn));
        return reqs_.back().get();
    }

    void GiveReadLock(TxNumber txn)
    {
        lock_.AcquireLock(Req(txn), CcProtocol::Locking, LockType::ReadLock);
    }

    void GiveWriteIntent(TxNumber txn)
    {
        lock_.AcquireLock(Req(txn), CcProtocol::Locking, LockType::WriteIntent);
    }

    void GiveWriteLock(TxNumber txn)
    {
        lock_.AcquireLock(Req(txn), CcProtocol::Locking, LockType::WriteLock);
    }

    void GiveReadIntent(TxNumber txn)
    {
        lock_.AcquireLock(Req(txn), CcProtocol::Locking, LockType::ReadIntent);
    }

    /**
     * @brief Parks a request of `queued_type` behind an existing conflict, so
     * the blocking queue is non-empty with that entry at its head — the state
     * the fairness rules turn on.
     */
    void QueueBehindConflict(TxNumber txn, LockType queued_type)
    {
        lock_.AcquireLock(Req(txn), CcProtocol::Locking, queued_type);
    }
};

// The observable state a prediction must not disturb.
struct LockSnapshot
{
    size_t queue_size_{0};
    bool has_write_lock_{false};
    TxNumber write_tx_{0};

    static LockSnapshot Of(NonBlockingLock &lock)
    {
        LockSnapshot s;
        s.queue_size_ = lock.BlockingQueueSize();
        s.has_write_lock_ = lock.HasWriteLock();
        s.write_tx_ = lock.WriteTx().first;
        return s;
    }

    bool operator==(const LockSnapshot &rhs) const
    {
        return queue_size_ == rhs.queue_size_ &&
               has_write_lock_ == rhs.has_write_lock_ &&
               write_tx_ == rhs.write_tx_;
    }
};

using StateBuilder = void (*)(LockFixture &);

struct NamedState
{
    const char *name_;
    StateBuilder build_;
};

// Every distinct shape the lattice can be in, including the fast-path states
// (holder == Self) and both blocking-queue head types, which is where the
// read-lock fairness rule and the write-intent starvation rule diverge.
const std::vector<NamedState> &AllStates()
{
    static const std::vector<NamedState> states = {
        {"free", [](LockFixture &) {}},
        {"read_intent_other", [](LockFixture &f) { f.GiveReadIntent(kOther); }},
        {"read_intent_self", [](LockFixture &f) { f.GiveReadIntent(kSelf); }},
        {"read_lock_other", [](LockFixture &f) { f.GiveReadLock(kOther); }},
        {"read_lock_self", [](LockFixture &f) { f.GiveReadLock(kSelf); }},
        {"read_lock_two_others",
         [](LockFixture &f)
         {
             f.GiveReadLock(kOther);
             f.GiveReadLock(kThird);
         }},
        {"read_lock_self_and_other",
         [](LockFixture &f)
         {
             f.GiveReadLock(kSelf);
             f.GiveReadLock(kOther);
         }},
        {"write_intent_other",
         [](LockFixture &f) { f.GiveWriteIntent(kOther); }},
        {"write_intent_self", [](LockFixture &f) { f.GiveWriteIntent(kSelf); }},
        {"write_lock_other", [](LockFixture &f) { f.GiveWriteLock(kOther); }},
        {"write_lock_self", [](LockFixture &f) { f.GiveWriteLock(kSelf); }},
        {"write_intent_other_plus_read_other",
         [](LockFixture &f)
         {
             f.GiveReadLock(kOther);
             f.GiveWriteIntent(kOther);
         }},
        // Queue head == WriteLock: holds new readers off (fairness).
        {"queued_write_lock_behind_read",
         [](LockFixture &f)
         {
             f.GiveReadLock(kOther);
             f.QueueBehindConflict(kThird, LockType::WriteLock);
         }},
        // Queue head == WriteIntent: does NOT hold readers off, but does
        // block a new write intent.
        {"queued_write_intent_behind_write_lock",
         [](LockFixture &f)
         {
             f.GiveWriteLock(kOther);
             f.QueueBehindConflict(kThird, LockType::WriteIntent);
         }},
        {"queued_read_lock_behind_write_lock",
         [](LockFixture &f)
         {
             f.GiveWriteLock(kOther);
             f.QueueBehindConflict(kThird, LockType::ReadLock);
         }},
        {"self_write_lock_with_queued_writer",
         [](LockFixture &f)
         {
             f.GiveWriteLock(kSelf);
             f.QueueBehindConflict(kThird, LockType::WriteLock);
         }},
    };
    return states;
}

const std::vector<LockType> &AllLockTypes()
{
    static const std::vector<LockType> types = {LockType::NoLock,
                                                LockType::ReadIntent,
                                                LockType::ReadLock,
                                                LockType::WriteIntent,
                                                LockType::WriteLock};
    return types;
}

const std::vector<CcProtocol> &AllProtocols()
{
    static const std::vector<CcProtocol> protos = {
        CcProtocol::OCC, CcProtocol::OccRead, CcProtocol::Locking};
    return protos;
}

const char *Name(LockType t)
{
    switch (t)
    {
    case LockType::NoLock:
        return "NoLock";
    case LockType::ReadIntent:
        return "ReadIntent";
    case LockType::ReadLock:
        return "ReadLock";
    case LockType::WriteIntent:
        return "WriteIntent";
    case LockType::WriteLock:
        return "WriteLock";
    }
    return "?";
}

const char *Name(CcProtocol p)
{
    switch (p)
    {
    case CcProtocol::OCC:
        return "OCC";
    case CcProtocol::OccRead:
        return "OccRead";
    case CcProtocol::Locking:
        return "Locking";
    }
    return "?";
}

std::string Describe(const char *state, LockType lock_type, CcProtocol protocol)
{
    return std::string(state) + " / " + Name(lock_type) + " / " +
           Name(protocol);
}
}  // namespace

TEST_CASE("WouldAcquireLock matches AcquireLock across the lock lattice",
          "[paged][lock]")
{
    size_t cases = 0;
    for (const NamedState &state : AllStates())
    {
        for (LockType lock_type : AllLockTypes())
        {
            for (CcProtocol protocol : AllProtocols())
            {
                // A read lock is only ever requested under Locking; the other
                // protocols never route a Read to ReadLock
                // (LockTypeUtil::DeduceLockType), so pairing them here would
                // assert behaviour the engine never asks for.
                if (lock_type == LockType::ReadLock &&
                    protocol == CcProtocol::OCC)
                {
                    continue;
                }

                // Predict on one fixture...
                LockFixture predict_fix;
                state.build_(predict_fix);
                LockSnapshot before = LockSnapshot::Of(predict_fix.lock_);
                LockOpStatus predicted = predict_fix.lock_.WouldAcquireLock(
                    kSelf, protocol, lock_type);
                LockSnapshot after = LockSnapshot::Of(predict_fix.lock_);

                // ...and acquire for real on an identically built one, so the
                // prediction cannot have perturbed the state under test.
                LockFixture acquire_fix;
                state.build_(acquire_fix);
                StubCcRequest *req = acquire_fix.Req(kSelf);
                LockOpStatus actual =
                    acquire_fix.lock_.AcquireLock(req, protocol, lock_type);

                INFO(Describe(state.name_, lock_type, protocol)
                     << " | queue=" << before.queue_size_ << " wlock="
                     << before.has_write_lock_ << " wtx=" << before.write_tx_
                     << " | predicted=" << static_cast<int>(predicted)
                     << " actual=" << static_cast<int>(actual));
                CHECK(predicted == actual);
                // The prediction must be side-effect free: no lock taken, no
                // requester enqueued (§6 "acquire nothing; park on the
                // fetch").
                CHECK(before == after);
                ++cases;
            }
        }
    }
    // Guard against the enumeration silently collapsing in a refactor.
    CHECK(cases >= 200);
}

TEST_CASE("Grantable prediction implies the acquisition succeeds",
          "[paged][lock]")
{
    // The load-bearing direction for the Yield protocol: when the test says
    // grantable and nothing intervenes (single shard core, no suspension
    // point), the subsequent acquire cannot fail. A false "grantable" would
    // let a command finish and then fail to take its lock.
    for (const NamedState &state : AllStates())
    {
        for (LockType lock_type : AllLockTypes())
        {
            for (CcProtocol protocol : AllProtocols())
            {
                if (lock_type == LockType::ReadLock &&
                    protocol == CcProtocol::OCC)
                {
                    continue;
                }
                LockFixture fix;
                state.build_(fix);
                if (fix.lock_.WouldAcquireLock(kSelf, protocol, lock_type) !=
                    LockOpStatus::Successful)
                {
                    continue;
                }
                StubCcRequest *req = fix.Req(kSelf);
                INFO(Describe(state.name_, lock_type, protocol));
                CHECK(fix.lock_.AcquireLock(req, protocol, lock_type) ==
                      LockOpStatus::Successful);
            }
        }
    }
}

TEST_CASE("Prediction never enqueues the requester", "[paged][lock]")
{
    // Specifically the case the Yield protocol depends on: a command that
    // would block must be able to walk away leaving no queue entry, so that
    // yielding is a clean unwind (§6 "a lock that is never acquired is never
    // released").
    LockFixture fix;
    fix.GiveWriteLock(kOther);
    size_t queue_before = fix.lock_.BlockingQueueSize();

    for (int i = 0; i < 5; ++i)
    {
        CHECK(fix.lock_.WouldAcquireLock(
                  kSelf, CcProtocol::Locking, LockType::WriteLock) ==
              LockOpStatus::Blocked);
        CHECK(fix.lock_.WouldAcquireLock(
                  kSelf, CcProtocol::OCC, LockType::WriteLock) ==
              LockOpStatus::Failed);
    }
    CHECK(fix.lock_.BlockingQueueSize() == queue_before);
    CHECK(!fix.lock_.HasWriteLock(kSelf));
}
}  // namespace txservice
