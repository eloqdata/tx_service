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

This is a serialized queue-order interleaving on one `TxProcessor`, not a
concurrent C++ memory/data race.

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
- Increment a `KeyGapLockAndExtraData` generation on every `Reset`, and save
  that generation beside each continuation/end address. Terminal cleanup
  operates only on a wrapper that is still in use at the saved generation.
  This distinguishes a transient term rejection, where the CC map and pin are
  still live and must be released, from teardown/reuse at the same address,
  where cleanup must leave the new owner untouched.
- Normal resume validates both saved address-generation tokens before taking
  and clearing `blocking_info_`, then uses the copied state. If either token is
  stale, `SetError` consumes the still-attached state, skipping the stale token
  while releasing any matching sibling pin. Once detached, later terminal
  cleanup cannot consume an already-released reference.
- Give every stored blocking field an explicit default and replace consumed
  state with a value-initialized record, so both addresses and generations are
  reset together. Route local catalog-not-found errors through `SetError`, so
  completion/error paths cannot bypass cleanup.
- Before an error return from scan post-processing, drain scanner-only and
  trailing locks into the transaction read set so the normal abort path owns
  their release.
- Apply the same lifecycle to `ScanNextBatchCc` and
  `remote::RemoteScanNextBatch`.

Generation inspection runs only on the owning shard. A live pin keeps its lock
wrapper in use; after teardown, inspecting an already-queued token relies on
the lock pool's existing 30-second retirement grace. This is not a general
long-lived raw-pointer validation mechanism and does not change the separate
range-scan resume protocol.

Cancellation through the inherited `AbortCcRequest` is outside this race fix.
That base path bypasses both per-core cleanup and `unfinished_core_cnt_`
accounting for parallel requests, so correcting it is a larger separate
lifecycle change. This design does not claim to change that cancellation path.

## Verification

Extend the existing single-`TestNode` transaction consistency test. Seed more
than two 128-entry passes on one shard, start with `memory=true/KV=false`,
enqueue the scan and same-shard markers in one shard callback, and control
whether the next continuation resumes or finishes. Add a second transaction
reference to each continuation CCE so the checks distinguish exact-one
decrement (`2 -> 1`) from both no cleanup (`2`) and release-all (`0`).
Use a finite end key and apply the same counted-reference checks to its pin
across both normal resume and terminal completion.

To exercise both defenses independently, record that `Reset` clears the stale
memory flag, then deliberately restore the old `true` value before enqueueing
the request. The interleaving must therefore enter the early `SetFinish` path,
while the final assertions separately require the reset and cleanup behavior.

Before restoring the race state, reset once with memory and all KV buckets
finished and verify the completed flag remains true. In each marker, resolve
the lock address to and retain the stable `LruEntry *` while the pin is still
held. Require a captured CCE and an initial pin count of one, augment it to two,
and require a count of one after normal resume or terminal cleanup.

Model teardown/reuse by capturing a token, resetting the same lock wrapper to
advance its generation, and giving the reused wrapper an unrelated live
reference. Terminal cleanup must detach the stale token without changing that
reference. Pass another recycled token through normal resume; it must return
`NG_TERM_CHANGED` before dereferencing the new owner and must preserve the new
reference. Separately drive a real self-enqueued continuation through a
temporary invalid leader term without clearing its CC map; terminal error
cleanup must decrement the still-matching continuation exactly once. Together
these cases prove the error code alone cannot decide whether a saved address is
safe.

Also force a repeatable-read version mismatch after the CC layer has populated
a multi-key scan cache; abort must release the scanner-only key, proving that
error-path draining transfers its ownership.

Resetting stale memory progress also prevents `Merge` from treating a partial
memory pass as exhausted and advancing the pause key past unscanned entries.

Run the focused test before and after implementation, followed by the complete
`TxConsistency-Test` binary and the relevant tx-service test subset.
