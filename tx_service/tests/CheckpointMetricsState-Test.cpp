// clang-format off
#include <chrono>

#include <catch2/catch_all.hpp>
// clang-format on

#include "checkpoint_metrics_state.h"

using txservice::CheckpointMetricsState;

TEST_CASE("checkpoint intervals are independent per NG and term",
          "[checkpoint-metrics]")
{
    CheckpointMetricsState state(3);
    const auto start = CheckpointMetricsState::TimePoint{};

    auto first_attempt = state.RecordAttempt(1, 10, start);
    REQUIRE_FALSE(first_attempt.interval_seconds_.has_value());
    auto second_attempt =
        state.RecordAttempt(1, 10, start + std::chrono::seconds{90});
    REQUIRE(second_attempt.interval_seconds_ == 90.0);

    auto first_advance =
        state.RecordAdvance(1, 10, start + std::chrono::seconds{10});
    REQUIRE_FALSE(first_advance.interval_seconds_.has_value());
    auto second_advance =
        state.RecordAdvance(1, 10, start + std::chrono::seconds{130});
    REQUIRE(second_advance.interval_seconds_ == 120.0);

    // A second NG has its own anchors, and a new term resets both anchors.
    REQUIRE_FALSE(state.RecordAttempt(2, 20, start + std::chrono::seconds{200})
                      .interval_seconds_.has_value());
    REQUIRE_FALSE(state.RecordAttempt(1, 11, start + std::chrono::seconds{200})
                      .interval_seconds_.has_value());
    REQUIRE_FALSE(state.RecordAdvance(1, 11, start + std::chrono::seconds{200})
                      .interval_seconds_.has_value());
}

TEST_CASE("continuous checkpoint failure state aggregates and erases by NG",
          "[checkpoint-metrics]")
{
    CheckpointMetricsState state(3);

    REQUIRE_FALSE(
        state.RecordFailure(1, 10).continuous_failure_gauge_.has_value());
    REQUIRE_FALSE(
        state.RecordFailure(1, 10).continuous_failure_gauge_.has_value());
    REQUIRE(state.RecordFailure(1, 10).continuous_failure_gauge_ == true);
    REQUIRE(state.ConsecutiveFailures(1) == 3);

    state.RecordFailure(2, 20);
    state.RecordFailure(2, 20);
    REQUIRE(state.RecordFailure(2, 20).continuous_failure_gauge_ == true);

    // Clearing one breached NG leaves the node alert set while another is
    // still breached. A success only mutates its own NG.
    REQUIRE(state.RecordSuccess(1, 10).continuous_failure_gauge_ == true);
    REQUIRE(state.ConsecutiveFailures(1) == 0);
    REQUIRE(state.Erase(2).continuous_failure_gauge_ == false);
    REQUIRE_FALSE(state.Contains(2));

    // A thresholded old term cannot leak into a new leadership tenure.
    state.RecordFailure(1, 10);
    state.RecordFailure(1, 10);
    REQUIRE(state.RecordFailure(1, 10).continuous_failure_gauge_ == true);
    auto new_term = state.RecordAttempt(
        1, 11, CheckpointMetricsState::TimePoint{} + std::chrono::seconds{1});
    REQUIRE(new_term.continuous_failure_gauge_ == false);
    REQUIRE(state.ConsecutiveFailures(1) == 0);
    REQUIRE_FALSE(new_term.interval_seconds_.has_value());
}
