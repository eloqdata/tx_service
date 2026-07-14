# Deferred Data Read Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove transaction-level data-read early release so scan and unique-secondary CCE ownership remains valid until existing commit-time validation or abort cleanup.

**Architecture:** Keep semantic reads in `ReadWriteSet::data_rset_` and add scanner-only ownership there with version `0`. Reuse the existing final `ValidateOperation` and abort `PostProcessOp` fan-out; delete `ReleaseScanExtraLockOp` and its drain buffer instead of adding a new wait or remote protocol.

**Tech Stack:** C++20, Catch2, CMake, tx_service asynchronous CC requests.

## Global Constraints

- Work only in `/data/workspace/eloqkv/.claude/worktrees/tx-service-issue-508` on `codex/issue-508-defer-read-release`.
- Preserve the existing commit ordering: data read validation/release remains before WAL.
- Do not change public scan request types, wire formats, `PostReadCc`, or `NonBlockingLock`.
- Use `PostReadType::Release` at finalization; do not introduce decrement accounting.
- Scanner-only CCEs use read version `0`, so they are retained for release without version validation.
- Use the main checkout dependency prefix `/data/workspace/eloqkv/data_substrate/third_party/install` and set the same directory in `LD_LIBRARY_PATH` when running binaries.
- Format changed C++ with `clang-format-18` and leave no unrelated edits.

---

### Task 1: Add a deterministic scan-close lifetime regression test

**Files:**
- Modify: `tx_service/tests/include/mock/mock_catalog_factory.h`
- Modify: `tx_service/tests/TxConsistency-Test.cpp`

**Interfaces:**
- Consumes: `HashParitionCcScanner`, `BucketScanSavePoint`, `BucketScanPlan`,
  `WaitableCc`, `CcEntryAddr::CoreId()`, `CcEntryAddr::ExtractCce()`,
  `LocalCcShards::EnqueueCcRequest()`, and the existing `TestNode`/`TxHandle`
  fixture.
- Produces: `CceOwner(const CcEntryAddr&)`, `TxOwnsScanRead(...)`,
  `ClosedScanRead`, and
  `ScanOneAndClose(TestNode&, TxHandle&, int)` test helpers plus commit/abort
  lifetime assertions.

- [ ] **Step 1: Make the mock factory create a matching primary hash scanner**

Add this include to `mock_catalog_factory.h`:

```cpp
#include "cc/ccm_scanner.h"
```

Replace the `CreatePkCcmScanner()` assertion stub with:

```cpp
    std::unique_ptr<CcScanner> CreatePkCcmScanner(
        ScanDirection direction, const KeySchema *key_schema) override
    {
        return std::make_unique<HashParitionCcScanner<CompositeKey<int>,
                                                         CompositeRecord<int>>>(
            direction, ScanIndexType::Primary, key_schema);
    }
```

This scanner's key/record and hash-partition shape exactly match the
`TemplateCcMap<CompositeKey<int>, CompositeRecord<int>, true, false>` already
created by the mock factory. Leave the secondary/range scanner stubs unchanged.

- [ ] **Step 2: Add owner-shard-safe test helpers**

Add the required headers to `TxConsistency-Test.cpp`:

```cpp
#include <atomic>
#include <vector>

#include "cc/cc_entry.h"
#include "cc/local_cc_shards.h"
#include "cc/cc_req_misc.h"
#include "sharder.h"
#include "tx_request.h"
```

Add these helpers in an anonymous namespace before the `TEST_CASE`.
`ExtractCce()` and every later `LruEntry`/`NonBlockingLock` access must stay
inside a `WaitableCc` lambda on the CCE's owner shard. The fixture performs no
eviction, so the captured seeded-key `LruEntry` remains alive while the test
checks whether its key lock still contains this transaction. The test observes
actual transaction membership, not the pooled lock's stale `entry_`
back-pointer.

```cpp
namespace
{
LruEntry *CceOwner(const CcEntryAddr &cce_addr)
{
    std::atomic<LruEntry *> owner{nullptr};
    WaitableCc check_owner(
        [&cce_addr, &owner](CcShard &)
        {
            owner.store(cce_addr.ExtractCce(), std::memory_order_release);
            return true;
        });

    Sharder::Instance().GetLocalCcShards()->EnqueueCcRequest(
        cce_addr.CoreId(), &check_owner);
    check_owner.Wait();
    REQUIRE_FALSE(check_owner.IsError());
    return owner.load(std::memory_order_acquire);
}

struct ClosedScanRead
{
    CcEntryAddr cce_addr_;
    LruEntry *cce_;
    TxNumber txn_;
};

bool TxOwnsScanRead(const ClosedScanRead &read)
{
    std::atomic<bool> owns_read{false};
    WaitableCc check_ownership(
        [&read, &owns_read](CcShard &)
        {
            NonBlockingLock *key_lock = read.cce_->GetKeyLock();
            if (key_lock != nullptr)
            {
                const auto &read_locks = key_lock->ReadLocks();
                const auto &read_intents = key_lock->ReadIntents();
                owns_read.store(
                    read_locks.find(read.txn_) != read_locks.end() ||
                        read_intents.find(read.txn_) != read_intents.end(),
                    std::memory_order_release);
            }
            return true;
        });

    Sharder::Instance().GetLocalCcShards()->EnqueueCcRequest(
        read.cce_addr_.CoreId(), &check_ownership);
    check_ownership.Wait();
    REQUIRE_FALSE(check_ownership.IsError());
    return owns_read.load(std::memory_order_acquire);
}

ClosedScanRead ScanOneAndClose(TestNode &node, TxHandle &tx, int key)
{
    TxKey start_key = Key(key);
    TxKey end_key = Key(key);

    BucketScanSavePoint save_point;
    auto &bucket_group = save_point.bucket_groups_.emplace_back();
    bucket_group[0].push_back(
        Sharder::MapKeyHashToBucketId(start_key.Hash()));

    ScanOpenTxRequest open_req(&node.Table(),
                               node.SchemaVersion(),
                               ScanIndexType::Primary,
                               &start_key,
                               true,
                               &end_key,
                               true,
                               ScanDirection::Forward);
    open_req.bucket_scan_save_point_ = &save_point;
    uint64_t alias = tx.Txm()->OpenTxScan(open_req);
    open_req.Wait();
    REQUIRE_FALSE(open_req.IsError());

    BucketScanPlan plan = save_point.PickPlan(0);
    std::vector<ScanBatchTuple> batch;
    ScanBatchTxRequest batch_req(alias, node.Table(), &batch);
    batch_req.bucket_scan_plan_ = &plan;
    tx.Txm()->Execute(&batch_req);
    batch_req.Wait();
    REQUIRE_FALSE(batch_req.IsError());
    REQUIRE(batch.size() == 1);
    REQUIRE_FALSE(batch.front().cce_addr_.Empty());

    CcEntryAddr cce_addr = batch.front().cce_addr_;
    ClosedScanRead read{cce_addr,
                        CceOwner(cce_addr),
                        tx.Txm()->TxNumber()};
    REQUIRE(read.cce_ != nullptr);
    REQUIRE(TxOwnsScanRead(read));

    ScanCloseTxRequest close_req(batch, 0, alias, node.Table());
    tx.Txm()->Execute(&close_req);
    close_req.Wait();
    REQUIRE_FALSE(close_req.IsError());
    return read;
}
}  // namespace
```

The explicit one-bucket save point is required because the fixture's EloqKV
table is hash-partitioned. It avoids the null `bucket_scan_save_point_` and
`bucket_scan_plan_` paths and scans only the bucket that owns the seeded key.

- [ ] **Step 3: Add commit and abort retention scenarios**

Append the following sequential blocks to the existing single `TestNode` test case. Use distinct keys to avoid state coupling with scenarios 1-3.

```cpp
    // Scenario 4: scan close retains the transaction's read ownership until
    // commit validation releases it. Lock state is observed only on the CCE's
    // owner shard.
    {
        auto seed = node.BeginTx();
        REQUIRE(seed.Upsert(20, 200));
        REQUIRE(seed.Commit());

        auto tx =
            node.BeginTx(IsolationLevel::RepeatableRead, CcProtocol::OccRead);
        ClosedScanRead scanned = ScanOneAndClose(node, tx, 20);

        // This owner-shard request is queued after current main's local
        // scan-close PostReadCc, so no sleep or timing window is involved.
        CHECK(TxOwnsScanRead(scanned));
        REQUIRE(tx.Commit());
        CHECK_FALSE(TxOwnsScanRead(scanned));
    }

    // Scenario 5: abort uses the same final read-set cleanup path.
    {
        auto seed = node.BeginTx();
        REQUIRE(seed.Upsert(21, 210));
        REQUIRE(seed.Commit());

        auto tx =
            node.BeginTx(IsolationLevel::RepeatableRead, CcProtocol::OccRead);
        ClosedScanRead scanned = ScanOneAndClose(node, tx, 21);

        CHECK(TxOwnsScanRead(scanned));
        REQUIRE(tx.Abort());
        CHECK_FALSE(TxOwnsScanRead(scanned));
    }
```

- [ ] **Step 4: Build and run the focused test to prove it fails on current behavior**

Run:

```bash
LD_LIBRARY_PATH=/data/workspace/eloqkv/data_substrate/third_party/install/lib \
cmake --build bld --target TxConsistency-Test --parallel 16
LD_LIBRARY_PATH=/data/workspace/eloqkv/data_substrate/third_party/install/lib \
./bld/tx_service/tests/TxConsistency-Test
```

Expected: build succeeds; scenarios 4 and 5 report failed retention checks
because current `ScanClose()` queues a local `PostReadCc(Release)` before each
owner-shard membership check. The final commit/abort cleanup checks still pass,
and the binary exits normally rather than hanging or crashing.

---

### Task 2: Retain all transaction-level data reads until finalization

**Files:**
- Modify: `tx_service/src/tx_execution.cpp`
- Modify: `tx_service/include/tx_execution.h`

**Interfaces:**
- Consumes: `ReadWriteSet::AddRead(const CcEntryAddr&, uint64_t, const TableName*)`, `GetReadCnt()`, existing `ValidateOperation`, and existing abort `PostProcessOp`.
- Produces: a two-argument `TransactionExecution::ScanClose(uint64_t, const TableName&)`; no transaction-level caller queues a data `PostReadCc` before commit/abort.

- [ ] **Step 1: Stop the unique-secondary-to-primary early release**

In `TransactionExecution::PostProcess(ReadOperation&)`, delete the whole branch beginning with the comment `Read lock early release logic` through the primary-key `abundant_lock_op_` processing. The code immediately after the deletion must flow directly from read-set insertion to the existing cache-miss handling:

```cpp
        }

        if (read_.read_type_ == ReadType::Inside &&
            (read_res.rec_status_ == RecordStatus::Unknown ||
             read_res.rec_status_ == RecordStatus::VersionUnknown))
```

The unique-secondary read entry therefore stays in `rw_set_` and is released by final validation/abort.

- [ ] **Step 2: Make scan close retain returned and scanner-only CCEs**

Change the private declaration in `tx_execution.h` to:

```cpp
    void ScanClose(uint64_t alias, const TableName &table_name);
```

Change `ProcessTxRequest(ScanCloseTxRequest&)` to call:

```cpp
    ScanClose(scan_close_req.alias_, scan_close_req.table_name_);
```

Change the definition header to:

```cpp
void TransactionExecution::ScanClose(uint64_t alias,
                                     const TableName &table_name)
```

Delete the complete `unlock_batch` block. A normally returned locked scan tuple
is already added to `rw_set_` by the scan-next path, so ignoring
`unlock_batch_` retains rather than removes it. Tuples skipped because
`CommandSet::FindObjectCommand()` already owns the CCE remain managed by the
command set's final cleanup; `NoLock` tuples were skipped by the old close loop
as well.

Replace the range-partition last-tuple enqueue with release-only read-set retention:

```cpp
                if (lk_type == LockType::NoLock &&
                    !last_tuple->cce_addr_.Empty() &&
                    rw_set_.GetReadCnt(table_name,
                                       last_tuple->cce_addr_) == 0)
                {
                    bool added = rw_set_.AddRead(
                        last_tuple->cce_addr_, 0, &table_name);
                    assert(added);
                    (void) added;
                }
```

Replace the trailing-tuple enqueue with:

```cpp
        if (lk_type != LockType::NoLock &&
            !tuple->cce_addr_.Empty() &&
            rw_set_.GetReadCnt(table_name, tuple->cce_addr_) == 0)
        {
            bool added = rw_set_.AddRead(tuple->cce_addr_, 0, &table_name);
            assert(added);
            (void) added;
        }
```

Do not add a `key_ts_ != 0` filter to these two paths. The range-middle producer
explicitly calls `AcquireReadIntent()` for its last CCE (after asserting a
non-unknown payload), and `lk_type != NoLock` proves ownership for a trailing
tuple. If either tuple carried an unexpected zero timestamp, version-0
retention is still required to avoid leaking that ownership. `DrainScanner()`
keeps its distinct existing `key_ts_ != 0` no-gap-lock rule.

After this existing scanner-recycling call, delete the reset/push/process calls
for `abundant_lock_op_`; retain the call and `scans_.erase(scan_it)`:

```cpp
    cc_handler_->ScanClose(
        table_name, scanner->Direction(), std::move(scan_it->second.scanner_));
```

- [ ] **Step 3: Make scan failure draining retain ownership**

In `DrainScanner()`, replace the condition containing
`rw_set_.RemoveDataReadEntry(table_name, cc_scan_tuple->cce_addr_) == 0`
and both following `drain_batch_.emplace_back` branches with:

```cpp
        if (scan_tuple_lock_type != LockType::NoLock &&
            !cc_scan_tuple->cce_addr_.Empty() &&
            cc_scan_tuple->key_ts_ != 0 &&
            rw_set_.GetReadCnt(table_name, cc_scan_tuple->cce_addr_) == 0)
        {
            bool added = rw_set_.AddRead(
                cc_scan_tuple->cce_addr_, 0, &table_name);
            assert(added);
            (void) added;
        }
```

This preserves an existing semantic read version when present and adds only absent cleanup ownership at version `0`.

In the scan-open error branch of `PostProcess(ScanOpenOperation&)`, keep the
`DrainScanner(open_result.scanner_.get(), table_name)` call but delete the
following reset/push/process block for `abundant_lock_op_`. The drained CCEs
now remain in the read set for the transaction's later commit or abort cleanup.

- [ ] **Step 4: Remove `drain_batch_` and `abundant_lock_op_` from transaction state**

In `TransactionExecution`'s constructor initializer list, delete:

```cpp
      abundant_lock_op_(this),
```

In `TransactionExecution::Reset()`, delete the complete capacity-management block for `drain_batch_`.

In `tx_execution.h`, delete:

```cpp
    void Process(ReleaseScanExtraLockOp &unlock_op);
    void PostProcess(ReleaseScanExtraLockOp &unlock_op);
```

Delete the `drain_batch_` member and its comments, delete the `abundant_lock_op_` member, and delete:

```cpp
    friend struct ReleaseScanExtraLockOp;
```

In `tx_execution.cpp`, delete the complete definitions of `Process(ReleaseScanExtraLockOp&)` and `PostProcess(ReleaseScanExtraLockOp&)`.

- [ ] **Step 5: Run the focused regression test**

Run the Task 1 build and binary commands again.

Expected: all scenarios pass; CCE ownership remains attached after scan close and is detached only after commit or abort.

- [ ] **Step 6: Commit the tested behavior change**

```bash
git add tx_service/tests/TxConsistency-Test.cpp \
        tx_service/tests/include/mock/mock_catalog_factory.h \
        tx_service/src/tx_execution.cpp \
        tx_service/include/tx_execution.h
git commit -m "fix: retain data reads until transaction finalization"
```

---

### Task 3: Delete obsolete early-release types and document the invariant

**Files:**
- Modify: `tx_service/include/tx_operation.h`
- Modify: `tx_service/src/tx_operation.cpp`
- Modify: `tx_service/include/cc/cc_entry.h`
- Modify: `docs/04-transaction-execution.md`

**Interfaces:**
- Consumes: Task 2's absence of all `ReleaseScanExtraLockOp` and `DrainTuple` callers.
- Produces: no dead early-release operation/type; transaction documentation describing final data-read ownership and the node-group term trade-off.

- [ ] **Step 1: Delete the unused operation and tuple types**

Delete this complete declaration from `tx_operation.h`:

```cpp
struct ReleaseScanExtraLockOp : TransactionOperation
{
    explicit ReleaseScanExtraLockOp(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<PostProcessResult> hd_result_;
};
```

Delete `ReleaseScanExtraLockOp`'s constructor, `Reset()`, and `Forward()` definitions from `tx_operation.cpp`.

Delete this complete type from `cc_entry.h`:

```cpp
struct DrainTuple
{
    DrainTuple(const CcEntryAddr &cce_addr,
               uint64_t version_ts,
               PostReadType post_read_type)
        : cce_addr_(cce_addr),
          version_ts_(version_ts),
          post_read_type_(post_read_type)
    {
    }

    CcEntryAddr cce_addr_;
    uint64_t version_ts_;
    PostReadType post_read_type_;
};
```

Run:

```bash
rg -n "ReleaseScanExtraLockOp|abundant_lock_op_|drain_batch_|DrainTuple" \
  tx_service/include tx_service/src tx_service/tests
```

Expected: no matches.

- [ ] **Step 2: Document scan read ownership**

Add this subsection after the `TxRequest vocabulary` table in `docs/04-transaction-execution.md`:

```markdown
### Scan read ownership

Closing or draining a scanner recycles scanner state but does not release data
read locks or read intents while the transaction can still execute requests.
Returned tuples remain in the data read set. Scanner-only last/trailing/error
cleanup CCEs are stored there with version 0, which skips version validation but
keeps the CCE non-evictable until final cleanup. Commit releases them through
`ValidateOperation`; abort releases them through `PostProcessOp`.

This retention can increase the read-set footprint and writer blocking for long
locking scans. It also makes node groups touched only by scanner pins part of
the final post-read term check, so a leadership change on one of those groups
can abort the transaction.
```

- [ ] **Step 3: Format and run focused tests**

```bash
clang-format-18 -i \
  tx_service/tests/TxConsistency-Test.cpp \
  tx_service/src/tx_execution.cpp \
  tx_service/include/tx_execution.h \
  tx_service/src/tx_operation.cpp \
  tx_service/include/tx_operation.h \
  tx_service/include/cc/cc_entry.h
git diff --check
LD_LIBRARY_PATH=/data/workspace/eloqkv/data_substrate/third_party/install/lib \
cmake --build bld --target TxConsistency-Test CcRequestWait-Test --parallel 16
LD_LIBRARY_PATH=/data/workspace/eloqkv/data_substrate/third_party/install/lib \
./bld/tx_service/tests/TxConsistency-Test
LD_LIBRARY_PATH=/data/workspace/eloqkv/data_substrate/third_party/install/lib \
./bld/tx_service/tests/CcRequestWait-Test
```

Expected: both binaries build and pass.

- [ ] **Step 4: Commit cleanup and documentation**

```bash
git add tx_service/include/tx_operation.h \
        tx_service/src/tx_operation.cpp \
        tx_service/include/cc/cc_entry.h \
        docs/04-transaction-execution.md
git commit -m "refactor: remove scan early-release operation"
```

---

### Task 4: Full verification and reviewer handoff

**Files:**
- Verify: all files changed from base `a1162d3c598d7afbcbf9efcce897b9227d191d36`

**Interfaces:**
- Consumes: Tasks 1-3 complete branch diff.
- Produces: current verification evidence and a review-ready branch.

- [ ] **Step 1: Stress the deterministic regression test**

```bash
for run in $(seq 1 20); do
  LD_LIBRARY_PATH=/data/workspace/eloqkv/data_substrate/third_party/install/lib \
    ./bld/tx_service/tests/TxConsistency-Test \
    >/tmp/tx-consistency-issue-508-${run}.log 2>&1 || {
      cat /tmp/tx-consistency-issue-508-${run}.log
      exit 1
    }
done
```

Expected: 20/20 successful runs with no assertion, timeout, crash, or hang.

- [ ] **Step 2: Run relevant transaction tests and compile the complete library**

```bash
LD_LIBRARY_PATH=/data/workspace/eloqkv/data_substrate/third_party/install/lib \
cmake --build bld --target txservice TxConsistency-Test CcRequestWait-Test \
  --parallel 16
LD_LIBRARY_PATH=/data/workspace/eloqkv/data_substrate/third_party/install/lib \
./bld/tx_service/tests/TxConsistency-Test
LD_LIBRARY_PATH=/data/workspace/eloqkv/data_substrate/third_party/install/lib \
./bld/tx_service/tests/CcRequestWait-Test
```

Expected: every target builds and both test binaries pass.

- [ ] **Step 3: Audit the final branch diff**

```bash
git status --short --untracked-files=all
git diff --check a1162d3c598d7afbcbf9efcce897b9227d191d36...HEAD
git diff --stat a1162d3c598d7afbcbf9efcce897b9227d191d36...HEAD
git diff a1162d3c598d7afbcbf9efcce897b9227d191d36...HEAD
```

Expected: only the approved spec/plan, data-read retention implementation, regression test, dead-code deletion, and transaction-execution documentation are present.

- [ ] **Step 4: Run required simplification, PR review, and final Claude code gate**

Use `code-simplifier:code-simplifier` on the complete task diff, then `pr-review-toolkit:review-pr`. Triage each finding with `superpowers:receiving-code-review`, rerun affected tests after any change, and finally run:

```bash
RUNNER="${CODEX_HOME:-$HOME/.codex}/skills/claude-gated-development/scripts/claude-review.sh"
"$RUNNER" code \
  --base a1162d3c598d7afbcbf9efcce897b9227d191d36 \
  --focus "Review the complete issue #508 implementation against docs/superpowers/specs/2026-07-14-defer-data-read-release-design.md and docs/superpowers/plans/2026-07-14-defer-data-read-release.md. Check scan-close/drain/unique-secondary ownership, final commit/abort release, CCE lifetime, retry behavior, tests, and documentation."
```

Expected: no valid unaddressed blocking finding. Any mutation after the clearing review requires rerunning affected validation and the final gate.
