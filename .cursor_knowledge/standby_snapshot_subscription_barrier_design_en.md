# Standby Snapshot Subscription Barrier: Design

## 1. Background
Standby snapshot sync runs after standby subscribes to primary.
Existing snapshot eligibility checks are mostly term and subscribe-id coverage.

This can still produce a snapshot that does not include all writes from transactions that were already active when standby started subscribing.

## 2. Goal
Introduce a **subscription-time active transaction barrier**:
- At subscription start, record `global_max_active_tx_ts` on primary.
- A snapshot for that subscription epoch is valid only if:
  - `ckpt_ts > subscription_time_global_max_active_tx_ts`

This guarantees snapshot visibility is strictly after all transactions active at subscription time.

## 3. Non-goals
- No change to standby forward-stream message protocol.
- No change to transaction execution semantics.
- No redesign of checkpoint algorithm.

## 4. Capture point
Capture barrier at primary RPC **`StandbyStartFollowing`**.
Do not capture it in `RequestStorageSnapshotSync`.

Why:
- `StandbyStartFollowing` is the actual subscription start moment.
- `RequestStorageSnapshotSync` may be delayed/retried and does not represent subscription time.

## 5. Conceptual model
Each subscription epoch (identified by `standby_term`) has a fixed barrier:
- `standby_term = (primary_term << 32) | subscribe_id`
- `barrier_ts = max ActiveTxMaxTs across all local shards at subscription time`

Snapshot for this epoch is sendable only when checkpoint advances beyond this barrier.

## 6. Runtime flow
1. Standby calls `StandbyStartFollowing`.
2. Primary computes `barrier_ts` and allocates `subscribe_id`, forming `standby_term`.
3. Primary stores `(standby_node_id, standby_term) -> barrier_ts`.
4. Standby later calls `RequestStorageSnapshotSync` with `standby_term`.
5. Primary loads barrier and attaches it to pending snapshot-sync task.
6. In snapshot worker loop, primary checks existing conditions plus:
   - `ckpt_ts > barrier_ts`
7. If false, task remains pending; if true, snapshot send + notify proceed.

## 7. Safety and lifecycle principles
- Missing barrier for incoming snapshot-sync request is treated as invalid request.
- Barrier is scoped to subscription epoch and must be cleaned after completion or term supersession.
- Concurrency control follows existing standby-sync mutex boundary in `SnapshotManager`.

## 8. Expected effects
- Prevents early snapshots that miss subscription-time in-flight transaction writes.
- Keeps retry behavior and snapshot transport model unchanged.
- Adds controlled waiting under heavy long-running-transaction scenarios.

## 9. Observability (design level)
Need logs/metrics for barrier gating:
- blocked reason: `ckpt_ts <= barrier_ts`
- dimensions: `node_id`, `standby_term`, `barrier_ts`, `ckpt_ts`
- counters: blocked rounds, barrier map size
