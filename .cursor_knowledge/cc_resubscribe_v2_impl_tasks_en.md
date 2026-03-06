## Resubscribe v2 Implementation Task Breakdown

### 0. Purpose

This checklist translates `cc_resubscribe_epoch_design.md` (revised v2) into executable implementation tasks.
Each task includes:

- `Goal`
- `Status` (`TODO` / `IN_PROGRESS` / `DONE` / `BLOCKED`)
- `Files Involved`
- `State Changes`
- `Function Changes`
- `Definition of Done`

---

### 1. Recommended Execution Order

1. Complete protocol fields and end-to-end propagation first (T1/T2/T3).
2. Then complete primary-side round source-of-truth and strict validation (T4/T5/T6/T7).
3. Finally complete standby local preemption and strict message filtering (T8/T9).
4. Use the test matrix as the regression gate (T10).

---

### T1. Proto Field Extensions and Semantic Comments

- Status: `TODO`
- Goal: provide protocol carriers for round propagation and validation.
- Files Involved:
  - `data_substrate/tx_service/include/proto/cc_request.proto`
  - Generated pb outputs (build-generated)
- State Changes:
  - No runtime state additions.
- Function Changes:
  - No direct function logic changes in this task (protocol-only).
- Detailed Changes:
  - Add `uint32 subscribe_round_id` to `OnStartFollowingRequest` (optional, default 0).
  - Add `bool resubscribe_intent` to `OnStartFollowingRequest` (optional, default false).
  - Add `uint32 subscribe_round_id` to `StandbyStartFollowingRequest` (optional hint).
  - Add `bool resubscribe_intent` to `StandbyStartFollowingRequest` (optional, default false).
  - Add `uint32 subscribe_round_id` to `KeyObjectStandbyForwardRequest` (optional).
  - Keep `StandbyStartFollowingResponse.subscribe_id`, and clarify in comments that its semantic is `subscribe_round_id`.
- Definition of Done:
  - Build passes.
  - New fields are correctly propagated and used in validation.

---

### T2. End-to-End `subscribe_round_id + resubscribe_intent` Propagation in `OnStartFollowing`

- Status: `TODO`
- Goal: allow host manager triggers, standby-local triggers, and out-of-sync triggers to enter the unified path with round hints.
- Files Involved:
  - `data_substrate/tx_service/include/sharder.h`
  - `data_substrate/tx_service/src/sharder.cpp`
  - `data_substrate/tx_service/include/fault/cc_node.h`
  - `data_substrate/tx_service/src/fault/cc_node.cpp`
  - `data_substrate/tx_service/include/remote/cc_node_service.h`
  - `data_substrate/tx_service/src/remote/cc_node_service.cpp`
  - `data_substrate/tx_service/src/cc/cc_shard.cpp`
  - `data_substrate/tx_service/src/remote/cc_stream_receiver.cpp`
  - `data_substrate/tx_service/include/cc/catalog_cc_map.h` (fault-inject path)
- State Changes:
  - No new global state (parameter propagation only).
- Function Changes:
  - Add `subscribe_round_id` parameter to `Sharder::OnStartFollowing(...)` (default 0).
  - Add `resubscribe_intent` parameter to `Sharder::OnStartFollowing(...)` (default false).
  - Add `subscribe_round_id` parameter to `CcNode::OnStartFollowing(...)`.
  - Add `resubscribe_intent` parameter to `CcNode::OnStartFollowing(...)`.
  - Update `CcNodeService::OnStartFollowing(...)` to read and pass through new field.
  - Update trigger call sites: lag, stream idle timeout, out-of-sync, host manager path.
  - Semantic constraints:
    - lag / idle timeout: pass `current_round + resubscribe_intent=true`.
    - out-of-sync (round pre-assigned by primary): pass `subscribe_round_id=<msg_round> + resubscribe_intent=false`.
- Definition of Done:
  - All compile-time call sites updated.
  - `subscribe_round_id + resubscribe_intent` are propagated correctly end-to-end.

---

### T3. Primary Single Source of Truth: Round Allocation and Latest-Round Table

- Status: `TODO`
- Goal: unify `subscribe_round_id` source of truth and prevent cross-shard drift.
- Files Involved:
  - `data_substrate/tx_service/include/sharder.h`
  - `data_substrate/tx_service/src/sharder.cpp`
- State Changes:
  - Reuse: `subscribe_counter_`.
  - Add: `latest_subscribe_round_[node_id]` (recommended in `Sharder` for centralized ownership).
  - Add: `out_of_sync_ctrl_[node_id]` (stores `{inflight_round, inflight, last_send_ts, last_progress_ts}` or equivalent) for deduplicating repeated out-of-sync triggers.
  - Add: concurrency protection for this map (`mutex`/`shared_mutex`).
- Config parameters (default):
  - `same_round_resend_interval_ms=2000`
  - `no_progress_timeout_ms=20000`
- Function Changes (recommended new APIs):
  - `uint32_t ResolveSubscribeRound(uint32_t node_id, uint32_t hint_round)`
  - `uint32_t LatestSubscribeRound(uint32_t node_id) const`
  - `bool IsLatestSubscribeRound(uint32_t node_id, uint32_t round) const`
  - `bool ShouldReuseOutOfSyncRound(uint32_t node_id, uint32_t latest_round) const`
  - `void MarkOutOfSyncInflight(uint32_t node_id, uint32_t round)`
  - `void ClearOutOfSyncInflight(uint32_t node_id, uint32_t round)`
  - `bool ShouldEscalateOutOfSyncRound(uint32_t node_id, uint64_t now_ms) const`
- Definition of Done:
  - Round values are monotonic increasing only.
  - Stale hint rounds are detectable as stale.

---

### T4. Strict Round Resolution and Return in `StandbyStartFollowing`

- Status: `TODO`
- Goal: centralize round decision logic in primary RPC entry and make “requested round -> final round” traceable.
- Files Involved:
  - `data_substrate/tx_service/src/remote/cc_node_service.cpp`
  - `data_substrate/tx_service/include/cc/cc_shard.h`
  - `data_substrate/tx_service/src/cc/cc_shard.cpp`
- State Changes:
  - Candidate state must include round/standby_term (cannot remain `start_seq_id` only).
  - Recommended upgrade for `candidate_standby_nodes_`:
    `node_id -> {start_seq_id, standby_term}`.
- Function Changes:
  - `CcNodeService::StandbyStartFollowing(...)`
    - Resolve round from `subscribe_round_id + resubscribe_intent`.
    - If `resubscribe_intent=true` and `hint_round==latest`, allocate a newer round.
    - If `resubscribe_intent=false` and `hint_round==latest`, return idempotent success.
    - Build `standby_term=(term<<32)|round` and pass it into shard candidate registration.
  - Extend `CcShard::AddCandidateStandby(...)` signature to include `standby_term` (or round).
  - Add precise removal capability in `CcShard::RemoveCandidateStandby(...)` by `standby_term`.
- Definition of Done:
  - Stale-round requests are rejected.
  - Returned `subscribe_id` matches primary-side record.

---

### T5. Strict Validation + Idempotency in `ResetStandbySequenceId`

- Status: `TODO`
- Goal: prevent late reset requests from polluting newer rounds; enforce triple checks: term + round + candidate validity.
- Files Involved:
  - `data_substrate/tx_service/src/remote/cc_node_service.cpp`
  - `data_substrate/tx_service/include/cc/cc_shard.h`
  - `data_substrate/tx_service/src/cc/cc_shard.cpp`
  - `data_substrate/tx_service/src/cc/local_cc_shards.cpp` (heartbeat target ordering)
- State Changes:
  - Both subscribed/candidate states must support round-aware (`standby_term`) checks.
- Function Changes:
  - `CcNodeService::ResetStandbySequenceId(...)`
    - Parse `(term, round)` from `standby_node_term`.
    - Validate `term == current leader term`.
    - Validate `round == latest_subscribe_round_[node_id]`.
    - Validate candidate is still valid; otherwise reject as stale.
    - If already subscribed with same `(term, round)`, return idempotent success.
  - Add helper APIs in `CcShard`:
    - `HasCandidateStandby(node_id, standby_term)`
    - `HasSubscribedStandby(node_id, standby_term)`
    - `PromoteCandidateToSubscribed(...)` (optional)
- Definition of Done:
  - Old-round reset cannot override new-round state.
  - Repeated same-round reset returns idempotent success.

---

### T6. Explicit Round in Out-of-Sync Path

- Status: `TODO`
- Goal: make the key passive-resubscribe path round-directed, so standby does not retry under stale context.
- Files Involved:
  - `data_substrate/tx_service/src/cc/cc_shard.cpp`
  - `data_substrate/tx_service/include/proto/cc_request.proto`
  - `data_substrate/tx_service/include/cc/cc_request.h`
- State Changes:
  - Before sending out-of-sync notification, primary checks/updates `out_of_sync_ctrl_`; repeated triggers should reuse inflight round by default.
- Function Changes:
  - `CcShard::NotifyStandbyOutOfSync(uint32_t node_id)`
    - If this standby already has inflight out-of-sync round:
      - if `now-last_progress_ts < 20s` and `now-last_send_ts >= 2s`: resend same round;
      - if `now-last_progress_ts < 20s` and `now-last_send_ts < 2s`: ignore;
      - if `now-last_progress_ts >= 20s`: allocate newer round.
    - Otherwise allocate/update round and set `req->set_subscribe_round_id(...)`.
    - After sending, call `MarkOutOfSyncInflight(node_id, round)`.
  - Standby out-of-sync handling calls
    `OnStartFollowing(..., subscribe_round_id=<msg>, resubscribe_intent=false)`.
- Coordination with `T4`:
  - After `StandbyStartFollowing` validation succeeds, call `ClearOutOfSyncInflight(node_id, round)`.
- Definition of Done:
  - Round used by standby follow-up flow matches primary-side round after out-of-sync trigger.
  - Repeated out-of-sync triggers for the same standby during inflight do not allocate multiple newer rounds.
  - Threshold behavior is correct: same-round resend after >=2s; newer-round escalation after >=20s no progress.

---

### T7. Standby Local Preemption: `follow_req_id` Framework

- Status: `TODO`
- Goal: fix “old flow not finished, new same-term request dropped.”
- Files Involved:
  - `data_substrate/tx_service/include/fault/cc_node.h`
  - `data_substrate/tx_service/src/fault/cc_node.cpp`
- State Changes:
  - Add `requested_follow_req_id_`.
  - Add `active_follow_req_id_`.
  - Add `active_subscribe_ctx_` (packed term+round).
  - Add `pending_resubscribe_intent_` (or equivalent) to coalesce repeated same-term local “request-new-round” triggers.
  - Remove `requested_subscribe_primary_term_` after refactor completion.
- Function Changes:
  - `CcNode::OnStartFollowing(...)`
    - Generate a larger `follow_req_id` on every call.
    - Stop relying only on term-based suppression for same-term requests.
    - Coalesce repeated same-term local triggers (if a “request-new-round in progress” or “new round resolved but still running” flow already exists, do not issue another `resubscribe_intent=true` request).
  - `CcNode::SubscribePrimaryNode(...)`
    - Extend signature with `follow_req_id`, `subscribe_round_id_hint`, and `resubscribe_intent`.
    - Check “still latest follow_req_id” before/after every blocking point.
    - After `StandbyStartFollowing` returns round, update request context and force all follow-up retries to use that round with `resubscribe_intent=false`.
    - Non-latest request exits immediately without committing state.
- Definition of Done:
  - Later same-term request can preempt earlier in-flight request.

---

### T8. Context-Consistency Checks in `SubscribePrimaryNode`

- Status: `TODO`
- Goal: enforce `(term, round, follow_req_id)` consistency checks before applying any key RPC response.
- Files Involved:
  - `data_substrate/tx_service/src/fault/cc_node.cpp`
  - `data_substrate/tx_service/src/remote/cc_node_service.cpp`
  - `data_substrate/tx_service/include/store/data_store_handler.h` (interface remains reused)
- State Changes:
  - Update `active_subscribe_ctx_` only when request is confirmed effective.
- Function Changes:
  - In `SubscribePrimaryNode`:
    - Send `StandbyStartFollowingRequest.subscribe_round_id` and `resubscribe_intent`.
    - After response, verify request is still current before setting `standby_term` and before candidate/snapshot/reset state commits.
    - Ensure reset/snapshot follow-up requests always carry the resolved round (retry semantics should use `resubscribe_intent=false`).
  - Add follow-request checks to reset/snapshot retry-loop exit conditions.
- Definition of Done:
  - Late responses cannot overwrite newer request state.

---

### T9. Strict Forward Message Filtering

- Status: `TODO`
- Goal: enforce strict round filtering and reject stale/legacy forwarding messages directly.
- Files Involved:
  - `data_substrate/tx_service/include/cc/cc_request.h`
  - `data_substrate/tx_service/src/cc/cc_shard.cpp`
  - `data_substrate/tx_service/include/standby.h`
- State Changes:
  - No new core state (reuse `active_subscribe_ctx_` / `standby_term`).
- Function Changes:
  - `KeyObjectStandbyForwardCc::ValidTermCheck()`
    - If message carries round, compare with active round.
    - If message has no round, reject it as invalid.
  - `CcShard::ForwardStandbyMessage(...)`
    - Always include round for both normal forwarding messages and out-of-sync notifications.
- Definition of Done:
  - Stale-round or no-round forwarding messages are never applied.

---

### T10. Test Work Items (Matrix Implementation)

- Status: `TODO`
- Goal: cover same-term preemption, late responses, and concurrent triggers.
- Files Involved:
  - Relevant integration tests under `tests/`
  - Fault-injection scenarios (`CODE_FAULT_INJECTOR` points)
- State Changes:
  - None.
- Function Changes:
  - Mainly add/adjust tests; no production-logic changes required.
- Minimum Test Cases:
  1. Two same-term resubscribe requests; the second preempts the first in-flight request.
  2. Concurrent out-of-sync + lag trigger; only latest round becomes effective.
  3. Late `StandbyStartFollowingResponse` is dropped.
  4. Late `ResetStandbySequenceId` is rejected or handled idempotently.
  5. Snapshot coverage remains monotonic by round (no rollback).
  6. Host manager trigger using `current_round + resubscribe_intent=true` does not conflict with concurrent resubscribe.
  7. Forwarding messages that lack round fields are rejected.
  8. Repeated same-term standby-local triggers (lag/idle) produce only one `resubscribe_intent=true` request; all others are coalesced.
  9. After round=`R` is resolved, all follow-up retries carry `R` with `resubscribe_intent=false`.
  10. Repeated primary out-of-sync triggers for the same standby in a short interval reuse inflight round and do not continuously allocate newer rounds.
  11. Repeated out-of-sync triggers with `now-last_send_ts < 2s` are ignored and do not send duplicate notifications.
  12. After 2s and while `now-last_progress_ts < 20s`, same inflight round is resent.
  13. After 20s no progress, a newer round is allocated for escalation.
- Definition of Done:
  - All cases pass, with no new deadlocks or stalls.

---

### 2. Recommended Task Dependency Graph

1. `T1 -> T2 -> T7 -> T8`
2. `T1 -> T3 -> T4 -> T5`
3. `T1 -> T6 -> T9`
4. `T10` is final gate.

---

### 3. High-Risk Points to Confirm Before Coding

1. `candidate_standby_nodes_` currently has no round dimension; without upgrade, cross-round wrong remove/promote can happen.
2. `ResetStandbySequenceId` currently adds heartbeat target before validation; ordering should be adjusted to avoid stale pollution.
3. `SubscribePrimaryNode` has multiple blocking retry loops; missing follow_req checks in any one loop leaves a race-commit window.
4. In multi-standby forwarding, round must be stamped and validated per standby strictly to avoid cross-standby round pollution.
5. Wrong cleanup timing of out-of-sync dedup state can cause either “never allocate newer round again” or “allocate too many newer rounds”.

---

### 4. Current Execution Status

- Global Status: `READY_FOR_IMPLEMENTATION`
- Completed Documents:
  - Design (Chinese v2)
  - Design (English version)
  - This implementation task breakdown
