/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 */
#pragma once

#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <unordered_map>

#include "type.h"

namespace txservice
{
/**
 * Pure state machine behind node-aggregated checkpoint metrics.
 *
 * Checkpointer serializes calls and validates leadership terms before entering
 * this class. Keeping metric I/O and Sharder access outside makes every per-NG
 * transition deterministic and independently testable.
 */
class CheckpointMetricsState
{
public:
    using TimePoint = std::chrono::steady_clock::time_point;

    struct Update
    {
        std::optional<double> interval_seconds_;
        std::optional<bool> continuous_failure_gauge_;
    };

    explicit CheckpointMetricsState(size_t failure_threshold)
        : failure_threshold_(failure_threshold)
    {
        assert(failure_threshold_ > 0);
    }

    CheckpointMetricsState(const CheckpointMetricsState &) = delete;
    CheckpointMetricsState &operator=(const CheckpointMetricsState &) = delete;
    CheckpointMetricsState(CheckpointMetricsState &&) = delete;
    CheckpointMetricsState &operator=(CheckpointMetricsState &&) = delete;

    /** Records an eligible attempt and returns an interval after its anchor. */
    Update RecordAttempt(NodeGroupId node_group_id, int64_t term, TimePoint now)
    {
        Update update;
        NodeGroupState &state = StateForTerm(node_group_id, term, update);
        if (state.last_attempt_.has_value())
        {
            update.interval_seconds_ =
                std::chrono::duration<double>(now - *state.last_attempt_)
                    .count();
        }
        state.last_attempt_ = now;
        return update;
    }

    /** Records a durable advance and returns an interval after its anchor. */
    Update RecordAdvance(NodeGroupId node_group_id, int64_t term, TimePoint now)
    {
        Update update;
        NodeGroupState &state = StateForTerm(node_group_id, term, update);
        if (state.last_advance_.has_value())
        {
            update.interval_seconds_ =
                std::chrono::duration<double>(now - *state.last_advance_)
                    .count();
        }
        state.last_advance_ = now;
        return update;
    }

    /** Clears this NG's failure streak without changing another NG. */
    Update RecordSuccess(NodeGroupId node_group_id, int64_t term)
    {
        Update update;
        NodeGroupState &state = StateForTerm(node_group_id, term, update);
        state.consecutive_failures_ = 0;
        if (state.continuous_failure_)
        {
            state.continuous_failure_ = false;
            assert(continuous_failure_ng_count_ > 0);
            --continuous_failure_ng_count_;
            update.continuous_failure_gauge_ = continuous_failure_ng_count_ > 0;
        }
        return update;
    }

    /** Increments this NG's streak and raises the node signal at threshold. */
    Update RecordFailure(NodeGroupId node_group_id, int64_t term)
    {
        Update update;
        NodeGroupState &state = StateForTerm(node_group_id, term, update);
        ++state.consecutive_failures_;
        if (!state.continuous_failure_ &&
            state.consecutive_failures_ >= failure_threshold_)
        {
            state.continuous_failure_ = true;
            ++continuous_failure_ng_count_;
            update.continuous_failure_gauge_ = true;
        }
        return update;
    }

    /** Erases all leadership-tenure state for an NG. */
    Update Erase(NodeGroupId node_group_id)
    {
        Update update;
        auto it = states_.find(node_group_id);
        if (it == states_.end())
        {
            return update;
        }
        if (it->second.continuous_failure_)
        {
            assert(continuous_failure_ng_count_ > 0);
            --continuous_failure_ng_count_;
            update.continuous_failure_gauge_ = continuous_failure_ng_count_ > 0;
        }
        states_.erase(it);
        return update;
    }

    size_t ConsecutiveFailures(NodeGroupId node_group_id) const
    {
        auto it = states_.find(node_group_id);
        return it == states_.end() ? 0 : it->second.consecutive_failures_;
    }

    bool Contains(NodeGroupId node_group_id) const
    {
        return states_.find(node_group_id) != states_.end();
    }

private:
    struct NodeGroupState
    {
        int64_t term_{-1};
        size_t consecutive_failures_{0};
        bool continuous_failure_{false};
        std::optional<TimePoint> last_attempt_;
        std::optional<TimePoint> last_advance_;
    };

    NodeGroupState &StateForTerm(NodeGroupId node_group_id,
                                 int64_t term,
                                 Update &update)
    {
        auto [it, inserted] = states_.try_emplace(node_group_id);
        NodeGroupState &state = it->second;
        if (!inserted && state.term_ != term)
        {
            if (state.continuous_failure_)
            {
                assert(continuous_failure_ng_count_ > 0);
                --continuous_failure_ng_count_;
                update.continuous_failure_gauge_ =
                    continuous_failure_ng_count_ > 0;
            }
            state = NodeGroupState{};
        }
        state.term_ = term;
        return state;
    }

    const size_t failure_threshold_;
    std::unordered_map<NodeGroupId, NodeGroupState> states_;
    size_t continuous_failure_ng_count_{0};
};
}  // namespace txservice
