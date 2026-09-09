# 10 — Log Service

The log service is Data Substrate's write-ahead durability and recovery
authority. It stores transaction effects in independently replicated log
groups, remembers the current term for each CC node group (NG), exposes the
checkpoint/truncation boundary, and streams committed records back to a
recovering tx node.

The tx service accesses this subsystem through the `TxLog` abstraction and
`LogAgent`; it does not depend on the log-state implementation. The tx-side
checkpoint and replay model is described in
[07-durability-and-recovery.md](07-durability-and-recovery.md).

## Component boundary

`LogServer` hosts the brpc service and braft services. It creates one
`LogInstance` state machine for each log group assigned to the process.
`LogInstance` proposes mutating requests through braft, applies committed
entries to `LogState`, manages Raft snapshots and membership, and starts
`LogShippingAgent`s for replay.

`LogState` separates the replicated state machine from physical retention and
scan mechanics. Build configuration selects an in-memory, RocksDB, or
RocksDB-Cloud implementation. The persistent variants keep data logs in their
storage engine while the braft state machine orders writes, term changes,
checkpoint updates, and recovery metadata.

`LogAgent` is the tx-side client. It maps transaction work to a log group,
caches each group's leader, refreshes stale leaders, and exposes write,
checkpoint, replay, transaction-recovery, and membership operations through the
`TxLog` contract.

## Deployment and sharding

The log service may be started in process by Data Substrate or reached through
configured external endpoints. Both modes use the same protobuf and `LogAgent`
boundary. A log group is an independent braft replication group and therefore
an independent availability and throughput unit.

Each transaction writes its durable record to one selected log group, even when
its affected data spans several CC NGs. Consequently, recovery of one CC NG
must inspect every log group: records relevant to that NG may have been written
by transactions coordinated elsewhere.

## Write and term fencing

The serving log-group leader accepts a write, serializes it as a braft task, and
responds after the committed task is applied to `LogState`. Followers reject
the request and return leader information so `LogAgent` can refresh its route.

Each write carries the terms of the CC NGs whose locks protect the transaction.
Replay registration advances the log service's term for a recovering NG. A
later write carrying an older term is rejected, which prevents a former CC
leader from making a transaction durable after its authority has ended.

Data records contain the serialized transaction effects needed by tx-side
replay. Multi-stage catalog, range-split, cluster-scale, and migration records
retain the durable operation state needed to resolve work that was interrupted
between metadata and data publication. The exact protobuf fields are a wire
contract and remain defined in `log.proto`; architecture depends on their
semantic categories, not a duplicated field inventory.

## Checkpoint and retention

After store checkpointing succeeds, the current CC leader reports a checkpoint
timestamp to every log group. `LogInstance` accepts the update only for the
recorded CC NG term, so an old leader cannot truncate records needed by its
successor.

`LogState` uses the checkpoint as the lower recovery boundary and eventually
removes data records that are no longer needed. Retention mechanics differ by
backend: memory and RocksDB variants can use different physical cleanup
strategies, but all must preserve records at or above the safe checkpoint and
the staged operation metadata still required for recovery.

Log-service pressure may request an earlier tx-side checkpoint; it does not
independently choose a higher safe truncation timestamp. The checkpointer
remains responsible for proving that store persistence completed.

## Replay and transaction recovery

`ReplayLog` records the candidate CC leader and term, selects the requested
records from `LogState`, and opens a shipping stream to the tx node's recovery
service. Separate shipping work per log group allows a node to receive replay
in parallel. The tx side orders metadata before ordinary data and publishes its
leader term only after all groups finish.

`RecoverTx` resolves locks left on participant NGs when a coordinator
disappears. The log service compares the recorded terms and transaction record
to distinguish an uncommitted transaction from a committed one. For a committed
transaction it can stream the relevant record back for lock-scoped replay;
without conclusive evidence, the tx side retains the lock and retries later.

Raft snapshots preserve the log state machine's control and recovery metadata
across restart. Persistent data logs remain governed by `LogState`; snapshot
installation and backend reopening must reconstruct a coherent term,
checkpoint, and staged-operation view before the replica serves as leader.

## Cross-cutting invariants

- Mutating log-service state is ordered by the log group's braft state machine.
- A successful WAL response represents a committed and applied log-group task.
- CC NG terms fence both new writes and checkpoint/truncation updates.
- Recovering one CC NG requires completion from every log group.
- Backend-specific retention must preserve the common replay and staged-operation
  contract.
- Log-group leader caches are advisory; clients must refresh and retry after a
  `NotLeader` response.

## Source map

| Claim | Repository source |
|---|---|
| `LogServer` hosts brpc/braft services and assigned log groups | `eloq_log_service/include/log_server.h`; `eloq_log_service/src/log_server.cpp`; `eloq_log_service/include/log_service.h` |
| `LogInstance` is the braft state machine for writes, terms, checkpoints, replay, snapshots, and membership | `eloq_log_service/include/log_instance.h`; `eloq_log_service/src/log_instance.cpp` |
| `LogState` abstracts in-memory, RocksDB, and cloud retention implementations | `eloq_log_service/include/log_state.h`; `eloq_log_service/include/log_state_memory_impl.h`; `eloq_log_service/include/log_state_rocksdb_impl.h`; `eloq_log_service/src/log_state_memory_impl.cpp`; `eloq_log_service/src/log_state_rocksdb_impl.cpp`; `eloq_log_service/src/log_state_rocksdb_cloud_impl.cpp` |
| `LogAgent` implements the tx-side `TxLog` client boundary and leader refresh | `tx_service/include/txlog.h`; `tx_service/tx-log-protos/log_agent.h`; `tx_service/tx-log-protos/log_agent.cpp` |
| WAL, replay, checkpoint, recovery, and staged-operation wire contracts are defined in protobuf | `tx_service/tx-log-protos/log.proto` |
| Replay records are selected and shipped back to tx recovery services | `eloq_log_service/include/log_shipping_agent.h`; `eloq_log_service/src/log_instance.cpp`; `tx_service/src/fault/log_replay_service.cpp` |
| Startup selects in-process or external log-service deployment | `core/src/log_init.cpp`; `build_eloq_log_service.cmake`; `CMakeLists.txt` |
| Replicated writes and replay behavior have focused service tests | `eloq_log_service/test/write_log_test.cpp`; `eloq_log_service/test/async_write_log_test.cpp`; `eloq_log_service/test/rocksdb_test.cpp`; `eloq_log_service/test/log_server_rocksdb_cloud_tests.cpp` |
