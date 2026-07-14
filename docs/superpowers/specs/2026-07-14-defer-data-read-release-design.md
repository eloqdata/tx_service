# Defer Data Read Release Until Transaction Finalization

**Issue:** [tx_service #508](https://github.com/eloqdata/tx_service/issues/508)

**Status:** Approved direction (solution 2)

**Base:** `a1162d3c598d7afbcbf9efcce897b9227d191d36`

## Problem

`TransactionExecution` currently releases some data read locks/read intents before the transaction finishes:

- scan-close entries supplied through `unlock_batch_`;
- range-scan last-tuple read-intent pins and trailing tuples;
- tuples drained after scan-open/scan-next errors; and
- a unique-secondary read lock released before the primary-key lookup.

Those paths enqueue `PostReadCc` through `ReleaseScanExtraLockOp`. Remote releases are fire-and-forget: the operation may finish after its local references reach zero while a remote `PostReadCc` is still queued. The same transaction can then reacquire the CCE. A delayed old release calls transaction-scoped `ClearTx(txn)`, clears the newer acquisition, and makes the CCE evictable while a newer read-set address still refers to it. A later post-read can consequently dereference a recycled lock/CCE address.

## Decision

Retain every data read lock/read intent until the transaction enters its final commit or abort processing. Do not issue intermediate data `PostReadCc` requests while the transaction can still execute another user operation.

“Until commit” means the existing commit-time `ValidateOperation`, not after durable WAL commit. `ValidateOperation` already fans out one `PostReadCc` per data read-set CCE, waits for completion, validates versions, and releases the transaction's read ownership. After commit begins, the transaction cannot perform another user read, so a release cannot cross a later acquisition. Abort already releases the data read set through `PostProcessOp`.

This keeps the current validation, timestamp, WAL, and transaction-status ordering unchanged.

## Data-flow changes

### Normal reads and returned scan tuples

Entries already recorded in `ReadWriteSet::data_rset_` remain there. `ScanClose()` must not call `RemoveDataReadEntry()` for `unlock_batch_`, and the unique-secondary-to-primary read path must not remove the secondary entry or start an early release operation.

### Scanner-only ownership

Some scanner ownership is intentionally absent from the semantic read set today:

- a range scanner's last-tuple read-intent pin;
- trailing tuples read past the requested end; and
- cached tuples drained during scan failure cleanup.

These CCEs must be added to the existing data read set with version `0`. Version `0` is the existing convention for an entry retained only for release: it contributes no version validation but ensures the final `PostReadCc(Release)` clears all ownership for that transaction. Reusing the read-set map also deduplicates a scanner-only pin with a later semantic read of the same CCE.

`DrainScanner()` continues to exclude empty CCE addresses and `key_ts == 0`
under its existing no-gap-lock rule. Range-scan last-tuple pins and locked
trailing tuples are retained whenever their CCE address is non-empty: those
paths already proved ownership by explicitly acquiring the pin or deducing a
non-`NoLock` lock type.

### Final release

- Successful commit: `ValidateOperation` validates/releases all data read-set entries and waits for their results before WAL processing.
- Abort or failed commit before successful validation: `PostProcessOp` releases all remaining data read-set entries and waits for completion.
- Validation failure: the current validation request has already released the entries; the existing `ClearDataReadSet()` prevents abort from releasing them twice.

No changes are required in `PostReadCc`, `NonBlockingLock`, the remote protocol, WAL ordering, or transaction status recovery.

## Code scope

Modify:

- `tx_service/src/tx_execution.cpp`
  - retain unique-secondary reads;
  - retain scan-close read-set entries;
  - add scanner-only CCEs to the read set with version `0`;
  - make `DrainScanner()` retain rather than release CCEs;
  - remove `ReleaseScanExtraLockOp` processing.
- `tx_service/include/tx_execution.h`
  - remove `drain_batch_`, `abundant_lock_op_`, and their operation hooks.
- `tx_service/include/tx_operation.h` and `tx_service/src/tx_operation.cpp`
  - remove `ReleaseScanExtraLockOp`.
- `tx_service/include/cc/cc_entry.h`
  - remove `DrainTuple` once it has no callers.
- `docs/04-transaction-execution.md`
  - document that scan-close/error cleanup retains data-read ownership for final validation/abort.
- `tx_service/tests/TxConsistency-Test.cpp`
  - add deterministic scan-close retention coverage.

Do not change public scan request types. `ScanCloseTxRequest::unlock_batch_` remains accepted for API compatibility even though it no longer triggers early release.

## Retry behavior and non-goals

Final cleanup continues to use `PostReadType::Release`, not `DecrReadIntent`. `Release` clears all ownership for the transaction and therefore does not introduce count drift when the transaction acquired multiple read intents on the same CCE.

This change removes the unsafe intermediate logical release and guarantees there is no later acquisition by the same live transaction. It does not add receiver-side request-ID deduplication or a generation-tagged CCE handle. General transport-level stale-request hardening is a separate protocol change and is outside this patch.

## Performance trade-off

The chosen behavior deliberately retains a scan's data-read footprint until commit/abort:

- read intents keep CCEs non-evictable;
- read locks under locking protocols may block writers longer; and
- the read set can grow with a long scan.

Scanner-only pins also make every node group holding such a pin part of the
final post-read term check. A leadership change on one of those node groups can
therefore abort a transaction that previously could finish after its early
release was ignored.

There is no added network round trip at scan close. Final post-reads are already fanned out in parallel by validation/post-processing. This trade-off was explicitly chosen because scans are common in the shared SQL/Mongo/Redis transaction engine and waiting for scan-close ACKs would add a synchronization barrier to every close.

## Deterministic tests

Extend the existing in-process `TxConsistency-Test` fixture:

1. Populate a key.
2. Start a RepeatableRead/OccRead transaction and scan the key.
3. Fetch the returned tuple that will be supplied in the close request's
   `unlock_batch_`.
4. On the CCE's owner shard, capture the concrete `LruEntry` and verify that
   its `NonBlockingLock` contains the transaction in `ReadIntents()` or
   `ReadLocks()`.
5. Close the scan, then enqueue the ownership check on the same shard. This
   request is ordered after the local early-release `PostReadCc` on current
   `main`, so current `main` deterministically reports that the transaction no
   longer owns the read; the fixed code still reports ownership.
6. Commit and verify that the transaction no longer appears in either read
   owner collection, proving final cleanup still happens.
7. Repeat the retention/final-release check through abort.

Run the focused test repeatedly to guard against timing sensitivity, then run the existing transaction and CC request test binaries.

## Initial implementation plan

1. Add the failing scan-close retention and final commit/abort release assertions.
2. Replace every transaction-level data-read early-release producer with read-set retention.
3. Delete the now-unused early-release operation and buffer.
4. Update transaction-execution documentation.
5. Run focused repeated tests, relevant existing tests, formatting, diff review, and the final independent Claude gate.

## Rollback

Reverting the patch restores the prior early-release optimization. No persisted data, wire format, configuration, or public API migration is involved.
