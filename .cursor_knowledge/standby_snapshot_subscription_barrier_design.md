# Standby Snapshot Subscription Barrier: Design

## 1. Background
Snapshot sync for standby was previously gated mostly by leader term and
subscribe-id coverage. That was not enough to ensure snapshot content is after
all transactions that were active when standby subscription became effective.

## 2. Goal
Introduce a subscription barrier per standby epoch:
- `barrier_ts = max ActiveTxMaxTs across all local ccshards`
- A snapshot is valid for that epoch only when:
  - `current_ckpt_ts > barrier_ts`

This guarantees the snapshot is after all transactions active at the
subscription success point.

## 3. Key decisions
- Barrier sampling point is **`ResetStandbySequenceId` success path on primary**
  (not `StandbyStartFollowing` and not `RequestStorageSnapshotSync`).
- Barrier key is `(standby_node_id, standby_term)`.
- `standby_term = (primary_term << 32) | subscribe_id`.
- Snapshot worker uses a lightweight checkpoint-ts probe before running heavy
  checkpoint/flush.

## 4. Runtime flow
1. Standby calls `StandbyStartFollowing`, receives `subscribe_id` and start seq.
2. Standby calls `ResetStandbySequenceId`.
3. Primary marks standby as subscribed on all shards and samples
   `global_active_tx_max_ts` using `ActiveTxMaxTsCc`.
4. Primary registers barrier in `SnapshotManager`:
   - `subscription_barrier_[node_id][standby_term] = barrier_ts`
5. Standby calls `RequestStorageSnapshotSync` with `standby_term`.
6. Primary validates barrier existence and enqueues one pending task per node
   with attached `subscription_active_tx_max_ts`.
7. `StandbySyncWorker` loop:
   - Probe `current_ckpt_ts` via `GetCurrentCheckpointTs()`
   - Select pending tasks that satisfy:
     - same primary term
     - `subscribe_id < current_subscribe_id`
     - `current_ckpt_ts > barrier_ts`
   - If no task is eligible, skip heavy checkpoint for this round.
   - If at least one task is eligible, run `RunOneRoundCheckpoint`, build/send
     snapshot, then notify standby.

## 5. Pending and cleanup model
- Pending tasks blocked by subscribe-id or barrier remain queued for retry.
- Worker uses retry backoff wait when pending queue is non-empty but blocked, to
  avoid tight loop.
- Superseded standby terms are pruned:
  - registering newer barrier can drop older pending task for the same node
  - older barriers are removed
- On leader loss, all pending tasks and barriers are cleared.
- On node removal, `EraseSubscriptionBarriersByNode` clears both pending and
  barriers for that node.

## 6. Safety properties
- Missing barrier on snapshot-sync request is rejected (safe default).
- Barrier is epoch-scoped and never shared across standby terms.
- All barrier/pending updates are under `standby_sync_mux_`.

## 7. Expected effects
- Prevent early snapshots that miss writes from subscription-time active txns.
- Avoid unnecessary heavy checkpoint rounds when no task is currently eligible.
- Keep existing transport/retry semantics for snapshot send/notify.
