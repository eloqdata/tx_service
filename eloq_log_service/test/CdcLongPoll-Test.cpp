/**
 * @file
 * Tests for the CdcScan long poll (cdc_long_poll.h): every parked request is
 * completed exactly once, by whichever source comes first, on a bthread of its
 * own, and the poll outlives every completion.
 */

#include <bthread/bthread.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "cdc_long_poll.h"

// butil's logging, pulled in through bthread, defines a one-argument CHECK
// that would replace Catch's. Catch comes last so that its definition is the
// one in force.
#undef CHECK
#include <catch2/catch_all.hpp>

using namespace txlog;
using Wake = CdcLongPoll::Wake;

namespace
{
/** @brief What the completions of one request saw. */
struct Outcome
{
    std::atomic<int> runs{0};             ///< How often it completed.
    std::atomic<int> why{-1};             ///< The last reason, as an int.
    std::atomic<bool> on_bthread{false};  ///< It ran on a bthread.
};

/**
 * @param outcome Where the completion records itself.
 * @return A completion that records its runs, reason and thread.
 */
CdcLongPoll::Completion Recorder(Outcome &outcome)
{
    return [&outcome](Wake why)
    {
        outcome.why.store(static_cast<int>(why));
        outcome.on_bthread.store(bthread_self() != 0);
        outcome.runs.fetch_add(1);
    };
}

/**
 * @brief Polls until a condition holds or five seconds pass.
 *
 * @param done The condition.
 * @return Whether it held.
 */
template <typename Pred>
bool WaitFor(Pred done)
{
    for (int i = 0; i < 5000 && !done(); ++i)
    {
        usleep(1000);
    }
    return done();
}
}  // namespace

TEST_CASE("A parked scan completes once, with the first wake",
          "[cdc_long_poll]")
{
    CdcLongPoll poll;
    Outcome outcome;
    auto request = poll.Prepare(Recorder(outcome));
    poll.Park(request, 60'000);
    CHECK(poll.Size() == 1);

    poll.CompleteAll(Wake::kAdvance);
    REQUIRE(WaitFor([&] { return outcome.runs.load() == 1; }));
    CHECK(outcome.why.load() == static_cast<int>(Wake::kAdvance));
    CHECK(outcome.on_bthread.load());
    CHECK(poll.Size() == 0);

    // Later sources find it completed.
    poll.CompleteAll(Wake::kStop);
    poll.Complete(request, Wake::kStop);
    CdcLongPoll::Cancel(request);
    usleep(20 * 1000);
    CHECK(outcome.runs.load() == 1);
}

TEST_CASE("A parked scan's deadline completes it", "[cdc_long_poll]")
{
    CdcLongPoll poll;
    Outcome outcome;
    const auto start = std::chrono::steady_clock::now();
    auto request = poll.Prepare(Recorder(outcome));
    poll.Park(request, 50);
    REQUIRE(WaitFor([&] { return outcome.runs.load() == 1; }));
    CHECK(std::chrono::steady_clock::now() - start >=
          std::chrono::milliseconds(45));
    CHECK(outcome.why.load() == static_cast<int>(Wake::kDeadline));
    CHECK(poll.Size() == 0);
}

TEST_CASE("A cancellation completes a scan, parked or not", "[cdc_long_poll]")
{
    CdcLongPoll poll;

    // Cancelled before the park, as when the call was cancelled already when
    // it registered its cancellation: the park leaves it alone.
    Outcome early;
    auto first = poll.Prepare(Recorder(early));
    CdcLongPoll::Cancel(first);
    poll.Park(first, 60'000);
    REQUIRE(WaitFor([&] { return early.runs.load() == 1; }));
    CHECK(early.why.load() == static_cast<int>(Wake::kCancel));
    CHECK(poll.Size() == 0);

    Outcome parked;
    auto second = poll.Prepare(Recorder(parked));
    poll.Park(second, 60'000);
    CdcLongPoll::Cancel(second);
    REQUIRE(WaitFor([&] { return parked.runs.load() == 1; }));
    CHECK(parked.why.load() == static_cast<int>(Wake::kCancel));
    CHECK(poll.Size() == 0);
}

TEST_CASE("Racing sources complete every scan exactly once", "[cdc_long_poll]")
{
    CdcLongPoll poll;
    constexpr int kRequests = 300;
    std::vector<Outcome> outcomes(kRequests);
    std::vector<std::shared_ptr<CdcLongPoll::Request>> requests;
    for (int i = 0; i < kRequests; ++i)
    {
        requests.push_back(poll.Prepare(Recorder(outcomes[i])));
        // Some deadlines fire while the other sources race.
        poll.Park(requests.back(), i % 3);
    }
    std::thread advance([&] { poll.CompleteAll(Wake::kAdvance); });
    std::thread cancel(
        [&]
        {
            for (const auto &request : requests)
            {
                CdcLongPoll::Cancel(request);
            }
        });
    std::thread stop(
        [&]
        {
            for (auto it = requests.rbegin(); it != requests.rend(); ++it)
            {
                poll.Complete(*it, Wake::kStop);
            }
        });
    advance.join();
    cancel.join();
    stop.join();

    REQUIRE(WaitFor(
        [&]
        {
            for (const Outcome &o : outcomes)
            {
                if (o.runs.load() == 0)
                {
                    return false;
                }
            }
            return true;
        }));
    usleep(20 * 1000);
    int more_than_once = 0;
    for (const Outcome &o : outcomes)
    {
        more_than_once += o.runs.load() > 1;
    }
    CHECK(more_than_once == 0);
    CHECK(poll.Size() == 0);
}

TEST_CASE("The size observer sees every change of the parked count",
          "[cdc_long_poll]")
{
    CdcLongPoll poll;
    std::vector<size_t> seen;  // Written under the poll's lock.
    std::mutex seen_mutex;
    auto snapshot = [&]
    {
        std::lock_guard<std::mutex> lk(seen_mutex);
        return seen;
    };
    poll.SetSizeObserver(
        [&](size_t parked)
        {
            std::lock_guard<std::mutex> lk(seen_mutex);
            seen.push_back(parked);
        });
    // Setting the observer reports the count at once.
    CHECK(snapshot() == std::vector<size_t>{0});

    Outcome first, second, cancelled;
    auto a = poll.Prepare(Recorder(first));
    auto b = poll.Prepare(Recorder(second));
    auto c = poll.Prepare(Recorder(cancelled));
    poll.Park(a, 60'000);
    poll.Park(b, 60'000);
    poll.Park(c, 60'000);
    CHECK(snapshot() == std::vector<size_t>{0, 1, 2, 3});

    CdcLongPoll::Cancel(c);
    REQUIRE(WaitFor([&] { return cancelled.runs.load() == 1; }));
    CHECK(snapshot().back() == 2);

    poll.CompleteAll(Wake::kAdvance);
    REQUIRE(WaitFor([&]
                    { return first.runs.load() == 1 && second.runs.load() == 1; }));
    CHECK(snapshot() == std::vector<size_t>{0, 1, 2, 3, 2, 1, 0});

    // A request completed before it is parked never counts.
    Outcome early;
    auto d = poll.Prepare(Recorder(early));
    CdcLongPoll::Cancel(d);
    poll.Park(d, 60'000);
    REQUIRE(WaitFor([&] { return early.runs.load() == 1; }));
    CHECK(snapshot().size() == 7);

    // Clearing the observer stops the reports.
    poll.SetSizeObserver(nullptr);
    Outcome late;
    auto e = poll.Prepare(Recorder(late));
    poll.Park(e, 60'000);
    CHECK(snapshot().size() == 7);
    // Completed here, while 'late' is alive; the poll outlives it.
    poll.CompleteAll(Wake::kStop);
    REQUIRE(WaitFor([&] { return late.runs.load() == 1; }));
}

TEST_CASE("The poll completes its scans with kStop and waits for them",
          "[cdc_long_poll]")
{
    constexpr int kRequests = 3;
    std::vector<Outcome> outcomes(kRequests);
    std::atomic<int> finished{0};
    {
        CdcLongPoll poll;
        for (int i = 0; i < kRequests; ++i)
        {
            Outcome &outcome = outcomes[i];
            poll.Park(poll.Prepare(
                          [&outcome, &finished](Wake why)
                          {
                              // A completion still running holds the poll up.
                              bthread_usleep(50 * 1000);
                              outcome.why.store(static_cast<int>(why));
                              outcome.runs.fetch_add(1);
                              finished.fetch_add(1);
                          }),
                      60'000);
        }
        CHECK(poll.Size() == kRequests);
    }
    CHECK(finished.load() == kRequests);
    for (const Outcome &o : outcomes)
    {
        CHECK(o.runs.load() == 1);
        CHECK(o.why.load() == static_cast<int>(Wake::kStop));
    }
}
