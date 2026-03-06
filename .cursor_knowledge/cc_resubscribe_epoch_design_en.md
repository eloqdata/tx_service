## Resubscribe While Previous Subscription Is Still In Progress (Revised v2)

### 1. Background and Existing Problems

- The current `CcNode::OnStartFollowing` only deduplicates by `requested_subscribe_primary_term_`; if a resubscribe is triggered again in the same term while the previous subscription is still running, the new request is dropped directly.
- Resubscribe is not triggered only by out-of-sync. It can also be triggered by lag threshold and stream idle timeout; all of these can happen repeatedly within the same term.
- The current system already has `subscribe_id` (encoded in the lower 32 bits of `standby_node_term`). Introducing an additional independent epoch can easily create dual sources of truth and state divergence.

### 2. Revision Goals

1. Fix the deadlock where a new resubscribe in the same term is dropped while an old subscription has not finished.
2. Define a single round identifier and avoid ambiguity from having both `subscribe_id` and `subscribe_epoch`.
3. Cover all trigger paths (out-of-sync / lag / stream timeout / host manager OnStartFollowing).
4. Establish strict stale-round filtering and idempotent handling for key RPCs.
5. Keep the single-version implementation simple, verifiable, and observable.

### 3. Core Design Decisions

#### 3.1 Single Source of Truth

- Do not introduce a new independent round system.
- Define the resubscribe round uniformly as `subscribe_round_id`, directly reusing existing `subscribe_id` semantics.
- Keep `standby_node_term` unchanged: upper 32 bits are `primary_term`, lower 32 bits are `subscribe_round_id`.

#### 3.2 Separation of Two Context Types

- **Protocol-level round (cross-node):** `(primary_term, subscribe_round_id)`, used for primary/standby consistency.
- **Local request sequence (standby-local only):** `follow_req_id`, used to preempt/cancel a local in-flight subscription workflow.

Note: `follow_req_id` is not a protocol field and does not participate in cross-node consistency checks; it only solves local concurrent preemption.

### 4. State to Maintain

| Role | State | Description |
| --- | --- | --- |
| Primary (`Sharder`) | `subscribe_counter_` (existing) | Round allocator that assigns `subscribe_round_id`. |
| Primary (`CcShard`) | `latest_subscribe_round_[node_id]` | Latest accepted round for this standby; used to reject stale requests. |
| Primary (`CcShard`) | `out_of_sync_ctrl_[node_id]` | Out-of-sync dedup control state (for example `{inflight_round, inflight, last_send_ts, last_progress_ts}`) to avoid repeated triggers continuously allocating new rounds. |
| Standby (`CcNode`) | `requested_follow_req_id_` | Latest local request sequence id. |
| Standby (`CcNode`) | `active_follow_req_id_` | Currently executing local request sequence id. |
| Standby (`CcNode`) | `active_subscribe_ctx_` | Current effective `(term, round)` for filtering stale messages/RPCs. |

### 5. Protocol and Message Changes

1. `OnStartFollowingRequest`
   - Add optional `uint32 subscribe_round_id` (default `0`).
   - Add optional `bool resubscribe_intent` (default `false`).
   - Semantics: `subscribe_round_id` is the caller-known current round; `resubscribe_intent=true` means “request primary to allocate a newer round.”

2. `KeyObjectStandbyForwardRequest`
   - Add optional `uint32 subscribe_round_id`.
   - Both out-of-sync notifications and regular forwarding messages must carry round.

3. `StandbyStartFollowingRequest / Response`
   - Request adds optional `subscribe_round_id` hint.
   - Request adds optional `resubscribe_intent` (default `false`).
   - Response keeps `subscribe_id`, with semantics explicitly defined as `subscribe_round_id`.

4. `ResetStandbySequenceIdRequest / Response`
   - No new mandatory field. Continue using `standby_node_term` to carry `(term, round)`.
   - Server must explicitly parse and validate round.

### 6. Key Flows

#### 6.1 Unified Trigger Entrypoints

- out-of-sync: primary first checks `out_of_sync_ctrl_[node_id]` for dedup, then decides whether to allocate/reuse `subscribe_round_id` before notifying standby.
- lag threshold (standby local trigger): `OnStartFollowing(..., subscribe_round_id=current_round, resubscribe_intent=true, resubscribe=true)`.
- stream idle timeout (standby local trigger): same as above.
- host manager `OnStartFollowing`: can also trigger a new round with `current_round + resubscribe_intent=true`.

#### 6.2 Standby Local Preemption

- Every `OnStartFollowing` generates a larger `follow_req_id` and overrides the target request.
- In-flight `SubscribePrimaryNode` checks `follow_req_id` before and after each blocking point:
  - If it is no longer the latest request, exit immediately without committing state.
- This allows a new request in the same term to preempt old flow instead of being dropped by term-level deduplication.

#### 6.3 Round Resolution in `StandbyStartFollowing`

- If `resubscribe_intent=true` (request a newer round):
  - If `subscribe_round_id < latest_subscribe_round_[node_id]`, reject as stale.
  - If `subscribe_round_id == latest_subscribe_round_[node_id]`, allocate a new round (reuse `GetNextSubscribeId()`) and update latest.
  - If `subscribe_round_id == 0` and there is no known current round context, allocate a new round and update latest (initial/join fallback).
  - If `subscribe_round_id > latest_subscribe_round_[node_id]`, reject as invalid/out-of-order.
- If `resubscribe_intent=false` (join/retry existing round):
  - If `subscribe_round_id < latest_subscribe_round_[node_id]`, reject as stale.
  - If `subscribe_round_id == latest_subscribe_round_[node_id]`, handle as idempotent success.
  - If `subscribe_round_id > latest_subscribe_round_[node_id]`, reject as invalid/out-of-order.
- Response returns the final round (the current `subscribe_id` field).

#### 6.3.1 Dedup and Round-Carry Rules for Standby Self-Initiated Resubscribe

- When standby is self-triggered by lag / stream idle timeout, it should send “current round + `resubscribe_intent=true`” to request a newer round.
- If standby has no valid current round context, it may use `subscribe_round_id=0 + resubscribe_intent=true`.
- Standby must deduplicate locally: if there is already an in-flight “request-new-round” workflow or a newly resolved round still in progress in the same term, new local triggers must be coalesced/ignored.
- Once `StandbyStartFollowing` returns round=`R_new`, standby must switch request context to `(term, R_new)`; all retries in that round must carry `R_new` with `resubscribe_intent=false`.
- Primary applies a unified rule for round-bearing requests: reject if `< latest`, process by intent if `== latest` (allocate new or idempotent), reject if `> latest`.

#### 6.3.2 Primary-side Dedup for Repeated Out-of-Sync Triggers (Simplified)

- Default parameters (recommended, configurable):
  - `same_round_resend_interval_ms = 2000`: minimum resend interval for the same round.
  - `no_progress_timeout_ms = 20000`: no-progress timeout; allow newer-round escalation after timeout.

- If the same standby already has an in-flight out-of-sync notification and `inflight_round == latest_subscribe_round_[node_id]`:
  - if `now - last_progress_ts < no_progress_timeout_ms`:
    - resend same-round notification only when `now - last_send_ts >= same_round_resend_interval_ms`;
    - otherwise ignore repeated trigger;
  - if `now - last_progress_ts >= no_progress_timeout_ms`: allocate and send a newer round (escalation).
- By default, repeated triggers reuse the same inflight round; escalation is timeout-based, not retry-count-based.
- After primary receives valid `StandbyStartFollowing` for that assigned round (`resubscribe_intent=false`), update `last_progress_ts` (and clear/reset inflight marker per implementation policy).
- Progress definition (recommended): a validated `StandbyStartFollowing(node_id, round=inflight_round, resubscribe_intent=false)`.

#### 6.4 Strict Validation in `ResetStandbySequenceId`

- Parse `(term, round)` from `standby_node_term`.
- Execute candidate -> subscribed only if all conditions hold:
  - `term == current leader term`
  - `round == latest_subscribe_round_[node_id]`
  - corresponding candidate is still valid
- If already the same `(term, round)`, return idempotent success; if older, reject.

#### 6.5 Stale Message Filtering

- When standby handles forwarded messages, if message carries round and it does not equal `active_subscribe_ctx_.round`, drop directly.
- If a forwarded message does not carry round, reject it directly and record a warning metric.

### 7. Concurrency and Consistency Constraints

1. Any step that commits effective state (setting `CandidateStandbyNodeTerm`, `StandbyNodeTerm`, Reset completion) must re-check that `follow_req_id` is still the latest.
2. Any RPC response (StartFollowing/Reset/Snapshot) must validate `(term, round)` against current request context before applying.
3. `latest_subscribe_round_[node_id]` must be monotonic increasing only.
4. `resubscribe_intent=true` is only for requesting a newer round; retries in the same round must use resolved round with `resubscribe_intent=false`.
5. Repeated out-of-sync triggers should reuse the same in-flight round by default and must not allocate unbounded newer rounds.

### 8. Parameters and Runtime Policy

1. Default parameters:
   - `same_round_resend_interval_ms = 2000`
   - `no_progress_timeout_ms = 20000`
2. Parameter requirements:
   - `same_round_resend_interval_ms < no_progress_timeout_ms`
   - both parameters should be configurable and observable by metrics.

### 9. Test Matrix (Must Be Covered)

1. Two consecutive resubscribe requests in the same term: the second must preempt successfully while the first is still in progress.
2. Concurrent out-of-sync notification + lag trigger: only the latest round becomes effective.
3. Late `StandbyStartFollowingResponse` (old round) is dropped.
4. Late `ResetStandbySequenceId` (old round) is rejected or handled idempotently as expected.
5. Snapshot request coverage and round monotonicity are consistent (no rollback).
6. Host manager `OnStartFollowing` (`current_round + resubscribe_intent=true`) does not conflict with concurrent resubscribe.
7. Forwarding messages missing round field are rejected, and warning metrics are correct.
8. Multiple standby-local triggers in the same term (lag/idle) must result in only one “request-new-round (`resubscribe_intent=true`)” request; all others are locally coalesced.
9. After standby gets round=`R`, all follow-up retries must carry `R` with `resubscribe_intent=false`.
10. Repeated primary out-of-sync triggers in a short interval must not allocate multiple new rounds for the same standby unless escalation policy allows it.
11. Out-of-sync dedup thresholds are enforced: allow same-round resend after 2s; allow newer-round escalation after 20s no progress.

### 10. Expected Benefits

- Remove the deadlock point where a same-term resubscribe is dropped while subscription is in progress.
- Unify round semantics and avoid dual-source-of-truth conflict between `subscribe_id` and a newly introduced epoch.
- Define explicit stale/new round rules across all trigger paths and key RPCs, reducing race-condition pollution risk.
