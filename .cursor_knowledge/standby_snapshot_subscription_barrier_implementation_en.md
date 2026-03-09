# Standby Snapshot Subscription Barrier: Implementation

## 1. Scope of code changes
- `tx_service/src/remote/cc_node_service.cpp`
- `tx_service/include/store/snapshot_manager.h`
- `tx_service/src/store/snapshot_manager.cpp`
- `tx_service/include/cc/cc_shard.h` (if `ActiveTxMaxTs` is missing)
- Optional helper request class for cross-shard max-ts aggregation

## 2. New/updated state

### 2.1 Barrier registry in `SnapshotManager`
Add a map keyed by standby node and standby term:
- `subscription_barrier_[standby_node_id][standby_term] = barrier_ts`

### 2.2 Pending snapshot task extension
Replace pending value from raw request to task struct, e.g.:
- `req` (`StorageSnapshotSyncRequest`)
- `subscription_active_tx_max_ts`
- optional `created_at`

## 3. New APIs in `SnapshotManager`
- `RegisterSubscriptionBarrier(ng_id, standby_node_id, standby_term, barrier_ts)`
- `GetSubscriptionBarrier(standby_node_id, standby_term, uint64_t* out)`
- `EraseSubscriptionBarrier(standby_node_id, standby_term)`
- optional cleanup utility for old terms

All above plus pending-task updates should be protected by `standby_sync_mux_`.

## 4. Barrier collection in `StandbyStartFollowing`
In `CcNodeService::StandbyStartFollowing` on primary:
1. Validate leader term.
2. Compute `global_max_active_tx_ts = max(shard.ActiveTxMaxTs(ng_id))`.
3. Generate `subscribe_id`.
4. Form `standby_term`.
5. Call `SnapshotManager::RegisterSubscriptionBarrier(...)`.

Implementation note:
- Keep collection in shard-safe context (same pattern as existing cross-shard requests).

## 5. `RequestStorageSnapshotSync` path changes
In `SnapshotManager::OnSnapshotSyncRequested`:
1. Parse `standby_term` from request.
2. Query barrier by `(standby_node_id, standby_term)`.
3. If not found: reject / do not enqueue.
4. If found: enqueue task with barrier ts.

Dedup logic remains term-based; when task is replaced by newer term, barrier ts is replaced accordingly.

## 6. Snapshot gating logic
In `SnapshotManager::SyncWithStandby` keep existing checks and add barrier check:
- Obtain current-round `ckpt_ts`.
- For each candidate pending task, require:
  - `ckpt_ts > task.subscription_active_tx_max_ts`

If condition fails, leave task pending for next round.

## 7. Checkpoint API adjustment
Current `RunOneRoundCheckpoint` returns `bool` only.
Prefer changing signature to:
- `bool RunOneRoundCheckpoint(uint32_t node_group, int64_t term, uint64_t* out_ckpt_ts)`

Set `*out_ckpt_ts` from round checkpoint request result (`CkptTsCc::GetCkptTs()`).

## 8. `ActiveTxMaxTs` helper
If missing, add in `CcShard`:
- `uint64_t ActiveTxMaxTs(NodeGroupId cc_ng_id)`

Expected behavior:
- Traverse `lock_holding_txs_[cc_ng_id]`
- Use write-lock timestamp domain aligned with existing checkpoint min-ts logic
- Exclude meta-table tx entries (same scope policy as min-ts path)

## 9. Cleanup rules
- On successful snapshot completion for `(node_id, standby_term)`: erase barrier entry.
- On registering newer term for same node: prune older-term barriers.
- On standby reset/unsubscribe: remove all barriers for that node (if hook available).
- Optional TTL sweep as fallback.

## 10. Failure behavior
- Missing barrier on sync request: reject request (safe default).
- Checkpoint failure: keep task queued.
- Snapshot transfer or callback failure: preserve existing retry behavior.

## 11. Tests

### Unit tests
- barrier register/get/erase and supersession behavior
- pending dedup with barrier replacement
- gating predicate boundaries (`ckpt_ts == barrier_ts`, `ckpt_ts < barrier_ts`, `ckpt_ts > barrier_ts`)

### Integration tests
- long-running active tx at subscription time blocks snapshot until ckpt passes barrier
- multiple standbys with independent barriers
- repeated sync-request retries with same standby term
- leader term switch cleanup correctness
