# Hash Scan Resume Pin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent hash-partition scans from leaving continuation or end CCE locks behind when a resumed request terminates before its normal resume block.

**Architecture:** Keep resume ownership in the existing per-core `blocking_info_`. Terminal completion consumes it centrally and normal continuation takes it before resuming; local and remote requests use the same cleanup semantics. Reset only stale memory-finished progress at a new batch boundary.

**Tech Stack:** C++20, Catch2, existing `CcShard` request queue and `TestNode` harness.

## Global Constraints

- Follow red-green-refactor: no production edit before the focused regression test fails for the expected orphan `ReadIntent`.
- Reuse `CcMap::DecrReadIntent`; do not add a new lock abstraction or dependency.
- Preserve already-complete cores and existing scanner/read-set ownership.
- Keep local and remote scan request behavior symmetric.

---

### Task 1: Reproduce the stale-progress continuation leak

**Files:**
- Modify: `tx_service/tests/TxConsistency-Test.cpp`

**Interfaces:**
- Consumes: `ScanNextBatchCc`, `BucketScanPlan`, `HashParitionCcScanner`, `WaitableCc`, `CcShard::Enqueue`, and `NonBlockingLock::ReadIntents`.
- Produces: one deterministic regression scenario in the existing `transaction consistency on TestNode` test.

- [ ] **Step 1: Write the failing test**

Add a helper/scoped scenario that:

```cpp
// Seed > ScanNextBatchCc::ScanBatchSize keys on one core.
// Build progress with memory_scan_is_finished_=true and one KV bucket=false.
// Enqueue scan_req then mark_bucket_drained from one same-shard WaitableCc.
// Capture scan_req.BlockingCceLockAddr(core) in the marker.
// Wait for the scan result, inspect the captured CCE on its owner shard, and
// require that txn is absent from key_lock->ReadIntents().
```

Use non-fatal observation for the stale flag so the same run reaches the orphan
assertion. Always call `CcShard::ClearTx(txn)` after observing the result, before
the final `REQUIRE`, so RED does not pollute later scenarios.

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
cmake --build bld --target TxConsistency-Test -j2
LD_LIBRARY_PATH=/data/workspace/eloqkv/data_substrate/third_party/install/lib:/data/workspace/eloqkv/data_substrate/third_party/install/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH} \
  ctest --test-dir bld -R '^transaction consistency on TestNode$' --output-on-failure
```

Expected: FAIL because the captured continuation CCE still contains the scan
transaction in `ReadIntents`; the request itself completes normally.

### Task 2: Consume resume ownership on every terminal path

**Files:**
- Modify: `tx_service/include/cc/cc_request.h`
- Modify: `tx_service/include/remote/remote_cc_request.h`
- Modify: `tx_service/src/remote/remote_cc_request.cpp`
- Modify: `tx_service/include/cc/template_cc_map.h`

**Interfaces:**
- Consumes: `CcMap::DecrReadIntent`, `CcMap::ReleaseCceLock`,
  `NonBlockingLock::SearchLock`, existing `BlockingCceLockAddr` and
  `BlockingPair`.
- Produces: per-core `ClearBlockingInfo`/terminal cleanup used by both local and
  remote `SetFinish`.

- [ ] **Step 1: Implement the minimum cleanup**

Implement one shared inline cleanup function taking:

```cpp
uint64_t cce_lock_addr;
uint64_t end_cce_lock_addr;
ScanBlockingType blocking_type;
TxNumber txn;
NodeGroupId node_group_id;
```

For the end CCE and a `NoBlocking` continuation, call
`CcMap::DecrReadIntent`. For `BlockOnFuture`/`BlockOnLock`, query the CCE's
actual held type with `SearchLock(txn)` and release that exact type. Ignore zero
or detached addresses.

Have local and remote `SetFinish(core)` consume and clear the per-core state
before checking outstanding KV callbacks. Simplify `SetError` to latch the
error and delegate to `SetFinish`.

In both normal resume blocks, copy `blocking_info_`, clear it immediately, and
replace `ReleaseCceLock(..., ReadIntent)` with `DecrReadIntent`.

- [ ] **Step 2: Reset only stale memory progress**

In local and remote request reset, change:

```cpp
memory_finished = memory_finished && all_kv_buckets_finished;
```

Equivalently, preserve `true` only for an already `AllFinished()` core; set it
to `false` when any KV bucket still needs work.

- [ ] **Step 3: Run the focused test and verify GREEN**

Run the Task 1 commands.

Expected: PASS; the marker observes the continuation pin, the final request
finishes, and the captured CCE no longer owns the transaction RI.

### Task 3: Verify symmetry and final scope

**Files:**
- Modify if needed: `docs/03-concurrency-control.md`

**Interfaces:**
- Consumes: final implementation and regression output.
- Produces: current docs and reviewer-facing verification record.

- [ ] **Step 1: Format and rebuild**

```bash
clang-format-18 -i \
  tx_service/tests/TxConsistency-Test.cpp \
  tx_service/include/cc/cc_request.h \
  tx_service/include/remote/remote_cc_request.h \
  tx_service/src/remote/remote_cc_request.cpp \
  tx_service/include/cc/template_cc_map.h
cmake --build bld --target TxConsistency-Test -j2
```

- [ ] **Step 2: Run focused and related tests**

```bash
LD_LIBRARY_PATH=/data/workspace/eloqkv/data_substrate/third_party/install/lib:/data/workspace/eloqkv/data_substrate/third_party/install/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH} \
  ctest --test-dir bld -R 'TxConsistency-Test|CcEntry-Test|CcRequestWait-Test' --output-on-failure
```

Expected: all selected tests PASS with no crashes, hangs, or orphan-lock
assertions.

- [ ] **Step 3: Audit the final diff**

Confirm that local and remote request reset, terminal cleanup, and normal resume
are symmetric; the test fails without the production hunk; no unrelated files
or dependencies changed.
