# Standby Snapshot Subscription Barrier: Implementation

## 1. Scope of code changes
- `tx_service/src/remote/cc_node_service.cpp`
- `tx_service/src/fault/cc_node.cpp`
- `tx_service/include/cc/cc_request.h` (`ActiveTxMaxTsCc`)
- `tx_service/include/store/snapshot_manager.h`
- `tx_service/src/store/snapshot_manager.cpp`

## 2. New/updated state

### 2.1 Barrier registry in `SnapshotManager`
Add a map keyed by standby node and standby term:
- `subscription_barrier_[standby_node_id][standby_term] = barrier_ts`

### 2.2 Pending snapshot task extension
Pending value is a task struct:
- `req` (`StorageSnapshotSyncRequest`)
- `subscription_active_tx_max_ts`

## 3. New APIs in `SnapshotManager`
- `RegisterSubscriptionBarrier(standby_node_id, standby_term, barrier_ts)`
- `GetSubscriptionBarrier(standby_node_id, standby_term, uint64_t* out)`
- `EraseSubscriptionBarrier(standby_node_id, standby_term)`
- `EraseSubscriptionBarriersByNode(standby_node_id)`
- `GetCurrentCheckpointTs(node_group) -> uint64_t`
- `RunOneRoundCheckpoint(node_group, leader_term) -> bool`

Barrier/pending updates are protected by `standby_sync_mux_`.

## 4. Barrier collection in `ResetStandbySequenceId`
In `CcNodeService::ResetStandbySequenceId` on primary:
1. Move standby from candidate to subscribed on all shards.
2. Validate leader term.
3. If barrier for `(node_id, standby_term)` does not exist:
   - run `ActiveTxMaxTsCc` across all shards
   - compute global max
   - call `SnapshotManager::RegisterSubscriptionBarrier(...)`

This makes the sampling point aligned with "subscription success".

## 5. `RequestStorageSnapshotSync` path changes
In `SnapshotManager::OnSnapshotSyncRequested`:
1. Parse `(standby_node_id, standby_term)` from request.
2. Query barrier by `(standby_node_id, standby_term)`.
3. If not found: reject request.
4. If found: enqueue task with barrier ts.

Dedup is still term-based per standby node.

## 6. Snapshot gating logic
`SnapshotManager::SyncWithStandby` now runs in two phases:
1. Lightweight phase:
   - `current_ckpt_ts = GetCurrentCheckpointTs(node_group)`
   - Select tasks that satisfy:
     - term alignment
     - `subscribe_id < current_subscribe_id`
     - `current_ckpt_ts > subscription_active_tx_max_ts`
   - If no task is eligible, return directly.
2. Heavy phase:
   - Run `RunOneRoundCheckpoint(...)` (flush)
   - Create/send snapshot and notify standby for selected tasks.

## 7. Worker retry pacing
`StandbySyncWorker` keeps existing wake condition on non-empty pending queue, and
adds short wait-for backoff (`200ms`) when requests remain pending after a
round, to avoid tight retry loops.

## 8. Cleanup rules
- On successful snapshot completion for `(node_id, standby_term)`: erase barrier entry.
- On registering newer term for same node: prune older barriers and drop older
  pending task.
- On node removal: `EraseSubscriptionBarriersByNode(node_id)` clears both
  pending and barrier entries.
- On leader loss in sync loop: clear all pending and barriers.

## 9. Failure behavior
- Missing barrier on sync request: reject request (safe default).
- Checkpoint failure: keep task queued for next rounds.
- Snapshot transfer failure: task stays pending and retries in later rounds.

## 10. Standby-side rejection handling
In `CcNode::SubscribePrimaryNode`, if `ResetStandbySequenceId` is rejected by
primary, local standby following state is rolled back:
- unsubscribe per-shard standby sequence groups
- reset standby/candidate standby term if still on the failed term
- guard against clobbering newer resubscribe attempts.

## 11. Tests

### Unit tests
- barrier register/get/erase and supersession behavior
- pending dedup with barrier replacement
- gating boundaries (`current_ckpt_ts == / < / > barrier_ts`)

### Integration tests
- long-running active tx at subscription success blocks snapshot until
  `current_ckpt_ts > barrier`
- multiple standbys with independent barriers
- repeated sync-request retries with same standby term
- leader term switch cleanup correctness
