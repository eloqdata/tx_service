# Threading Model

The tx service partitions mutable transaction and concurrency-control state by
core. `LocalCcShards` owns one `CcShard` per core, and `TxService` owns one
`TxProcessor` for each shard. A processor advances the transactions assigned to
its shard and executes requests addressed to that shard. Callers in other
execution contexts cross this boundary by enqueueing work rather than by
mutating shard state directly.

This ownership model makes shard state predominantly single-writer while still
allowing a transaction to coordinate work across local shards and remote node
groups. [03-concurrency-control.md](03-concurrency-control.md) describes the
request and locking model built on this boundary.

## Execution contexts

Each shard has a native `tx_proc_N` thread. One processing round interleaves
transaction progress with CC work: it admits newly created transactions,
advances runnable transaction state machines, drains the shard's regular CC
queue, then services low-priority and deferred-destruction work. An operation
that is waiting for a CC or RPC result yields the processing context, so another
transaction or request can make progress.

When external processing is enabled, the brpc integration may also run the same
round from worker `N` for shard `N`. `TxServiceModule` exposes this integration;
the worker identifier is the shard identifier. The native thread and external
worker therefore share a `TxProcCoordinator` latch whose states cover
initialization, exclusive occupancy, availability, and teardown. Every entry
that advances a transaction or processes shard work must hold this latch.

The native processor enters standby while external processors are active, but
periodically resumes if they stop visiting the shard. This preserves progress
without permitting two contexts to execute shard work concurrently. Inline
transaction forwarding is allowed only when the current worker is bound to the
transaction's shard and can acquire the same latch; otherwise the transaction
is re-enqueued for its owner processor.

## Scheduling and wakeups

Transactions are owned by the processor on which they are created and remain
bound to that shard for their lifetime. Completed state machines return to that
processor's reuse pool. Cross-shard CPU work and CC work are dispatched through
the destination shard's queues.

Queue publication and wakeup are one synchronization protocol. The notifier
takes the processor sleep mutex before signalling so that a processor deciding
to sleep observes the published queue state. External-module builds wake the
corresponding brpc worker; other builds wake the native processor.

Background checkpoint, data-sync, range-management, statistics, and service
threads do not gain direct ownership of a shard. They submit shard work and
wait for completion outside the shard processing context.

## Heap ownership

Each shard owns dedicated allocation heaps for resident CC state and data-sync
scans. A native or external processor switches the thread-default allocator to
the shard heap while it holds the shard latch and restores the previous
allocator before releasing the latch. This keeps resident memory attributable
to a shard and makes memory admission and eviction decisions local to that
shard.

Consequently, objects whose lifetime belongs to a shard must be allocated and
destroyed on that shard. Metadata with a node-wide lifetime is owned separately
by `LocalCcShards`; it must not borrow storage whose lifetime ends with a shard
processing round.

## Concurrency invariants

- `CcRequestBase::Execute()` runs in the shard processing context and must not
  block that context. A request that cannot finish parks in an owning queue or
  asynchronous operation and is re-enqueued when it can continue.
- The shard latch is the exclusive-ownership boundary for transaction
  forwarding, CC queue execution, and shard-heap use. The latch must be
  released with the heap and thread overrides restored.
- A completion primitive shared between `Execute()` and a bthread waiter must
  not depend on waking a bound bthread from the worker main stack. Fan-out
  waitable CC requests use atomic completion state and yield with
  `bthread_usleep` while polling.
- A transaction state machine is advanced by only one context at a time.
  Remote result delivery has a separate forward latch so it cannot mutate a
  result while `Forward()` is timing out or recycling that result.
- Cross-shard work is message-passed to the owner shard. Direct access from a
  background thread, another processor, or an unrelated brpc worker violates
  the ownership model.

## Source map

| Claim | Repository source |
|---|---|
| `TxService` owns one processor per configured core and `LocalCcShards` owns the shards | `tx_service/include/tx_service.h`; `tx_service/include/cc/local_cc_shards.h` |
| A processing round interleaves transaction forwarding and shard queues | `tx_service/include/tx_service.h` (`TxProcessor::RunOneRound`) |
| Native and external contexts arbitrate through the shard-status latch | `tx_service/include/tx_service_common.h`; `tx_service/include/tx_service.h` (`TxProcessor`, `TxServiceModule`) |
| brpc workers are configured as external processors | `core/src/data_substrate.cpp`; `tx_service/include/tx_service.h` (`TxService::Start`) |
| Queue publication and processor notification form one wakeup protocol | `tx_service/src/cc/cc_shard.cpp`; `tx_service/include/tx_service.h` (`NotifyTxProcessor`) |
| Processing contexts install and restore the shard allocation heap | `tx_service/include/tx_service.h`; `tx_service/include/cc/cc_shard.h` (`CcShardHeap`) |
| Waitable fan-out requests use atomic completion with yielding polls | `tx_service/include/cc/cc_req_misc.h`; `tx_service/src/cc/cc_req_misc.cpp`; `tx_service/src/cc/cc_request.cpp` |
| Concurrent completion and reset behavior has test coverage | `tx_service/tests/CcRequestWait-Test.cpp` |
