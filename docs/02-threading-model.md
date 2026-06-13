# Threading Model

The tx service runs thread-per-core: each core owns one `CcShard` (the data) and one `TxProcessor` (the worker that drives it). Nothing else touches a shard directly — transactions, RPC handlers, and background threads communicate with a shard exclusively by enqueueing `CcRequest`s. On top of this, the tx service runs on a **custom brpc fork** in which brpc worker threads themselves can drive TxProcessors ("external processors"), so the same shard work can execute either on the native `tx_proc_N` thread or on brpc worker N's main stack. Getting this model wrong is the #1 source of subtle deadlocks; read the invariants section before adding any synchronization primitive.

Key files: `tx_service/include/tx_service.h` (TxProcessor, TxServiceModule, TxService), `tx_service/include/tx_service_common.h` (TxProcCoordinator, TxShardStatus), `tx_service/include/cc/cc_shard.h`, `tx_service/include/cc/local_cc_shards.h`.

## Threads in a tx node

| Thread(s) | Name | Role |
|---|---|---|
| N native processors | `tx_proc_0..N-1` | `TxProcessor::Run()` loop: forward tx state machines + process the shard's cc queues |
| N brpc workers | brpc | Serve RPC/bthreads **and** (with `ELOQ_MODULE_ENABLED`) drive TxProcessor N between polls via `TxServiceModule::Process(thd_id)` — 1:1 binding worker N ↔ shard N |
| Checkpointer | — | `Checkpointer::Run()` ([07](07-durability-and-recovery.md)) |
| Flush/data-sync workers | `flush_data_K` | Write dirty entries to the kv store (`LocalCcShards::StartBackgroudWorkers`) |
| Range split workers | — | `range_split_worker_num` ([08](08-range-and-bucket-management.md)) |
| DeadLockCheck, TxStartTsCollector, SnapshotManager, timer | — | Periodic services |
| Log service / DSS threads | — | If running in-process ([10](10-log-service.md), [09](09-store-handler.md)) |

## TxProcessor: one round of work

`TxProcessor::RunOneRound()` (called by both the native thread and external processors) does, under the shard latch:

1. Drain newly created txs (`new_txs_` MPMC queue) into the idle list.
2. `Forward()` every idle tx once; route by result (Finished → free pool, Idle → idle list, Busy/ForwardFailed → on-fly list).
3. Loop ×3: forward on-fly txs, then `LocalCcShards::ProcessRequests(thd_id)` — drain the shard's cc request queue, calling `req->Execute(ccs)` on each.
4. Process the low-priority queue (background jobs) and the lazy-free queue (deferred `TxObject` destruction).

Transactions are *bound* to the shard that created them (`TxService::NewTx()` load-balances creation across processors; `NewTx(shard_id)` pins explicitly). A tx's state machine is only ever forwarded under its shard's latch.

## The shard latch and external processors (`EXT_TX_PROC_ENABLED`)

`TxProcCoordinator::shard_status_` is an atomic latch with states `Uninitialized / Free / Occupied / Deconstructed`. Every entry point CASes `Free → Occupied` before touching the shard and stores `Free` on exit:

- `RunOneRound()` (native or external),
- `TxProcessor::ForwardTx(txm)` — lets a bthread (e.g. a Redis connection handler) forward a tx inline. It refuses if the current bthread's task group id ≠ shard id (a tx spanning multiple commands may land on a different worker), returning false so the caller enlists the tx into `resume_tx_queue_` instead.

With `ELOQ_MODULE_ENABLED`, `TxService::Start()` registers `TxServiceModule` (`eloq::register_module`); the brpc fork then calls `Process(thd_id)` from worker `thd_id`'s main stack between polling rounds (`brpc_worker_as_ext_processor=true`, `worker_polling_time_us=100000` are forced in `core/src/data_substrate.cpp`). `ExtThdStart/ExtThdEnd` maintain `ext_processor_cnt_`.

**Native-thread standby protocol:** while external processors are active, the native `tx_proc_N` thread parks in `Standby` (2s waits on `sleep_cv_`), but it tracks `one_round_cnt_`; if the external processor hasn't visited the shard for a full wait period (round counter unchanged), the native thread takes over again. This is the safety net that bounds how long a stalled brpc worker can starve its shard (~≤2s). When fully idle and no external processors exist, the processor `Sleep`s until `NotifyTxProcessor()`.

**Wake-up discipline:** `TxProcessor::Notify()` deliberately takes `sleep_mux_` before `notify_one()` even though the cv predicate uses lock-free state — the mutex creates the barrier that makes lock-free queue mutations visible to a thread deciding whether to sleep. Preserve this pattern when adding wakeups. Under `ELOQ_MODULE_ENABLED`, waking the shard means `EloqModule::NotifyWorker(core_id)` (wake the brpc worker), not the native thread.

## Per-shard heaps

Each shard has a dedicated mimalloc heap (`CcShardHeap`, ~90% of the per-shard memory budget) plus a small data-sync-scan heap. Whoever enters the shard (native or external) switches the thread-default heap to the shard heap (`SetAsDefaultHeap`, plus `OverrideHeapThread()` for external threads, and optionally a jemalloc arena switch) and restores it on exit. Consequences:

- All cc map memory is attributable and bounded per shard; `CcShardHeap::Full()/NeedCleanShard()` drive cache eviction and OOM aborts ([03](03-concurrency-control.md)).
- Code that allocates "for the shard" must run on the shard (that's most of why `DispatchTask`/cc requests exist).
- Long-lived metadata uses separate heaps (table ranges heap in `LocalCcShards`).

## Critical invariants (read before changing concurrency code)

1. **Never block inside `CcRequest::Execute()`** — it runs on the shard's processing context, possibly a brpc worker main stack. Blocking it stalls the shard (native takeover restores progress after ≤2s) or worse (see #2). Requests that can't finish re-enqueue themselves or park on a waiting queue.
2. **bthread::Mutex deadlock class.** In the brpc fork, bthreads are bound to their worker (`_bound_rq`, not stealable) and `butex_wake` prefers bound-bthread waiters over pthread waiters. If a `bthread::Mutex`/`bthread::ConditionVariable` is (a) locked inside a CC request's `Execute()` (worker main stack) and (b) waited on by bthreads, with (c) concurrent multi-acquisition (a request broadcast to multiple shards, or one mutex shared by several in-flight requests), then an unlock can wake a bthread that is queued on the very worker whose main stack is blocked → permanent deadlock; `wait_for` timeouts do **not** rescue (no timer remains pending). **Fix pattern:** completion state in `std::atomic`, `Wait()` polls with `bthread_usleep` backoff — see `CkptTsCc` and `WaitableCc` in `tx_service/include/cc/cc_req_misc.h`. Structurally safe variants: single-flight execution + a single bthread waiter, or a `std::thread` (pthread) waiter.
3. **`std::mutex` sync waits from bthreads** don't deadlock (pthread futex wake) but block the whole worker; the native takeover bounds the stall at ~2s. Avoid on hot paths.
4. **A tx state machine is forwarded only under its shard latch**, and only by one thread at a time (`forward_latch_` additionally arbitrates with the cc stream receiver writing results — see [04](04-transaction-execution.md)).
5. **Cross-shard work goes through `Enqueue`** (`CcShard::Enqueue`, `LocalCcShards::EnqueueCcRequest`, or `DispatchTask` for cpu-bound jobs), never direct calls.
