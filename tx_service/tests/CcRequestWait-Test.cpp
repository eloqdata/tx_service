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

// Regression tests for the multi-core completion-counter that backs the
// "waitable" CC requests (PR #491 / tx_service issue: avoid bthread::Mutex
// shared between a CC request's Execute() on tx processors and its Wait() on a
// bthread). The fix replaced the bthread::Mutex + ConditionVariable handshake
// with a std::atomic completion counter polled by Wait(), and latches the
// first error reported by AbortCcRequest(). WaitableCc is the generic carrier
// of that pattern (ClearTxCc / ActiveTxMaxTsCc / EscalateStandbyCcmCc /
// DbSizeCc / ClearCcNodeGroup share the same atomic-counter + polling Wait()).
//
// These tests drive the completion counter directly (no engine needed):
// AbortCcRequest() is the public entry into FinishOne() (the same decrement
// Execute() performs on success), and Wait() polls the atomic. They guard
// against a future regression that (a) loses or races a decrement so Wait()
// never returns, or (b) lets a later success clobber a previously latched
// error.

#include <atomic>
#include <catch2/catch_all.hpp>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

#include "cc/cc_req_misc.h"  // WaitableCc
#include "error_messages.h"  // CcErrorCode

using namespace txservice;

namespace
{
// A regressed Wait() would hang forever (exactly the deadlock the fix
// prevents). Rather than wedge the whole test binary, a watchdog thread aborts
// the process with a clear message if the body does not finish in time. It only
// reads an atomic flag, so it never touches the test's stack objects.
class Watchdog
{
public:
    explicit Watchdog(std::chrono::milliseconds budget) : budget_(budget)
    {
        thread_ = std::thread(
            [this]
            {
                const auto deadline =
                    std::chrono::steady_clock::now() + budget_;
                while (!done_.load(std::memory_order_acquire))
                {
                    if (std::chrono::steady_clock::now() > deadline)
                    {
                        std::fprintf(stderr,
                                     "CcRequestWait-Test: WaitableCc::Wait() "
                                     "did not return within the timeout -- the "
                                     "completion counter likely regressed.\n");
                        std::abort();
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }
            });
    }

    ~Watchdog()
    {
        done_.store(true, std::memory_order_release);
        thread_.join();
    }

private:
    std::chrono::milliseconds budget_;
    std::atomic<bool> done_{false};
    std::thread thread_;
};
}  // namespace

TEST_CASE("WaitableCc Wait() returns once every core has finished",
          "[cc-request-wait]")
{
    Watchdog wd(std::chrono::seconds(20));

    for (uint16_t core_cnt : {1, 2, 4, 8})
    {
        std::atomic<int> finished{0};
        WaitableCc cc([](CcShard &) { return true; }, core_cnt);

        // Concurrent finishers: each is the equivalent of one core completing
        // its Execute() and calling FinishOne(). A tiny stagger makes Wait()
        // actually poll across the completions instead of seeing them all done.
        std::vector<std::thread> finishers;
        finishers.reserve(core_cnt);
        for (uint16_t i = 0; i < core_cnt; ++i)
        {
            finishers.emplace_back(
                [&cc, &finished, i]
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(i));
                    finished.fetch_add(1, std::memory_order_relaxed);
                    cc.AbortCcRequest(CcErrorCode::NO_ERROR);
                });
        }

        cc.Wait();  // must return only after all core_cnt finishers ran

        for (auto &t : finishers)
        {
            t.join();
        }

        CHECK(finished.load() == core_cnt);
        CHECK(cc.IsFinished());
        CHECK_FALSE(cc.IsError());
    }
}

TEST_CASE("WaitableCc latches the first error and ignores later successes",
          "[cc-request-wait]")
{
    Watchdog wd(std::chrono::seconds(20));

    // Three cores: first reports an error, then a success, then a different
    // error. The first error must win and a later success must not clear it.
    WaitableCc cc([](CcShard &) { return true; }, /*core_cnt=*/3);

    cc.AbortCcRequest(CcErrorCode::REQUESTED_NODE_NOT_LEADER);  // first error
    CHECK(cc.IsError());
    CHECK(cc.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER);

    cc.AbortCcRequest(CcErrorCode::NO_ERROR);  // success must not erase it
    cc.AbortCcRequest(
        CcErrorCode::NG_TERM_CHANGED);  // later error must not win

    cc.Wait();

    CHECK(cc.IsFinished());
    CHECK(cc.IsError());
    CHECK(cc.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER);
}

TEST_CASE("WaitableCc Reset re-arms the counter and clears the error",
          "[cc-request-wait]")
{
    Watchdog wd(std::chrono::seconds(20));

    WaitableCc cc([](CcShard &) { return true; }, /*core_cnt=*/2);
    cc.AbortCcRequest(CcErrorCode::NG_TERM_CHANGED);
    cc.AbortCcRequest(CcErrorCode::NO_ERROR);
    cc.Wait();
    REQUIRE(cc.IsError());

    // Re-arm for a clean 2-core round.
    cc.Reset([](CcShard &) { return true; }, /*core_cnt=*/2);
    CHECK_FALSE(cc.IsError());
    CHECK_FALSE(cc.IsFinished());

    cc.AbortCcRequest(CcErrorCode::NO_ERROR);
    cc.AbortCcRequest(CcErrorCode::NO_ERROR);
    cc.Wait();
    CHECK(cc.IsFinished());
    CHECK_FALSE(cc.IsError());
}

TEST_CASE("WaitableCc completion counter survives repeated concurrent rounds",
          "[cc-request-wait][stress]")
{
    Watchdog wd(std::chrono::seconds(60));

    constexpr uint16_t kCoreCnt = 4;
    constexpr int kRounds = 2000;

    for (int round = 0; round < kRounds; ++round)
    {
        WaitableCc cc([](CcShard &) { return true; }, kCoreCnt);
        std::vector<std::thread> finishers;
        finishers.reserve(kCoreCnt);
        for (uint16_t i = 0; i < kCoreCnt; ++i)
        {
            finishers.emplace_back(
                [&cc] { cc.AbortCcRequest(CcErrorCode::NO_ERROR); });
        }
        cc.Wait();
        for (auto &t : finishers)
        {
            t.join();
        }
        REQUIRE(cc.IsFinished());
    }
}
