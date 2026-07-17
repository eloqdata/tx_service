# Hash Scan Resume Pin Design

## Problem

`ScanNextBatchCc` pins the next unprocessed CCE, and a finite end CCE, while a
128-entry memory pass yields and re-enqueues itself. Those pins live only in
`blocking_info_`. If a terminal path calls `SetFinish` or `SetError` before the
normal resume block consumes that state, the transaction read set cannot see
or release the pins.

The reachable datastore-backed race is:

1. persisted progress is `memory finished, KV unfinished`;
2. a later memory pass yields with a continuation pin;
3. the final KV callback marks the shard drained before the continuation runs;
4. the continuation returns through `ShardIsDrained` before normal pin release.

## Design

- At the start of a new local or remote batch, clear the memory-finished flag
  only for cores whose complete memory+KV progress is not already finished.
  Finished cores remain finished.
- Make `SetFinish` the single completion/error owner-cleanup point. It consumes
  pending `blocking_info_` exactly once:
  - continuation/end `ReadIntent`s use `CcMap::DecrReadIntent`, preserving other
    references held by the same transaction;
  - a resumed blocked CCE releases the actual granted lock returned by
    `NonBlockingLock::SearchLock`.
  Implement this as a shared static helper on `ScanNextBatchCc`, with the
  narrow `CcMap` friendship needed to call its protected lock-release methods;
  the remote request reuses the same helper.
- Normal resume takes and clears `blocking_info_` before continuing, then uses
  the copied state. This prevents terminal cleanup from touching stale or
  already-consumed lock addresses.
- Explicitly reset both stored lock addresses instead of relying on aggregate
  value-initialization, and route local catalog-not-found errors through
  `SetError`, so completion/error paths cannot bypass the cleanup.
- Apply the same lifecycle to `ScanNextBatchCc` and
  `remote::RemoteScanNextBatch`.

Cancellation through the inherited `AbortCcRequest` is outside this race fix.
That base path bypasses both per-core cleanup and `unfinished_core_cnt_`
accounting for parallel requests, so correcting it is a larger separate
lifecycle change. This design does not claim to change that cancellation path.

## Verification

Extend the existing single-`TestNode` transaction consistency test. Seed more
than 128 keys on one shard, start with `memory=true/KV=false`, enqueue the scan
and a same-shard marker in one shard callback, and let the marker flip the final
bucket before the self-enqueued continuation. The regression check must fail on
the current code because the continuation CCE still owns the transaction's
`ReadIntent`, then pass after the fix.

To exercise both defenses independently, record that `Reset` clears the stale
memory flag, then deliberately restore the old `true` value before enqueueing
the request. The interleaving must therefore enter the early `SetFinish` path,
while the final assertions separately require the reset and cleanup behavior.

Before restoring the race state, reset once with memory and all KV buckets
finished and verify the completed flag remains true. In the marker, resolve the
lock address to and retain the stable `LruEntry *` while the pin is still held;
the lock wrapper itself can be recycled after cleanup. Require both a captured
CCE and an observed `ReadIntent` before accepting the final absence check.

Resetting stale memory progress also prevents `Merge` from treating a partial
memory pass as exhausted and advancing the pause key past unscanned entries.

Run the focused test before and after implementation, followed by the complete
`TxConsistency-Test` binary and the relevant tx-service test subset.
