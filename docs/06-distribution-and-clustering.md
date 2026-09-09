# 06 — Distribution & Clustering

Data Substrate partitions ownership across node groups (NGs). At any moment,
one recovered leader serves an NG's in-memory data; the other members either
maintain a standby copy or participate only in election. `Sharder` is the
process-wide boundary between transaction execution and this distributed
topology: it resolves ownership, tracks leadership terms, and connects local
CC requests to remote nodes.

This document describes the stable distribution model. Range internals and
bucket movement are covered by
[08-range-and-bucket-management.md](08-range-and-bucket-management.md), while
leader recovery is covered by
[07-durability-and-recovery.md](07-durability-and-recovery.md).

## Topology and placement

`ClusterConfig` assigns nodes and their roles to NGs and carries a monotonically
ordered configuration version. Each process creates a `CcNode` lifecycle object
for every NG in which it participates. The host manager owns election and
membership decisions; the tx service interacts with it only through the
`HostMangerService` RPC contract and receives leader transition callbacks
through `CcRpcService`.

Data placement has two levels:

1. A key hash, or a range identifier for a range-partitioned table, maps to one
   of the fixed range buckets.
2. Bucket metadata names the owning NG, and the bucket id selects the local
   `CcShard` when the request reaches that NG's leader.

The placement lookup is intentionally independent of the physical storage
backend. Hash-partition identifiers and range-partition identifiers remain
distinct at the store boundary even when they ultimately map to the same
storage service.

Topology and bucket ownership are not merely process-local caches. The current
cluster configuration is represented by `ClusterConfigCcMap`, and bucket
ownership by `RangeBucketCcMap`. Treating both as transactional metadata lets
normal CC locking, WAL, replay, and version checks protect routing changes.
In-memory copies are the fast read path; the transactional records are the
coordination and recovery boundary.

## Request routing

The local and remote CC handlers expose the same transaction-facing behavior.
A request for locally owned data is enqueued directly to the responsible
`CcShard`; a request for another NG is encoded as a `CcMessage`, sent to the
cached leader, reconstructed as a CC request there, and executed by the same
shard code as a local request.

Persistent brpc streams carry the high-volume request/response data plane.
Unary `CcRpcService` calls carry lifecycle and control operations such as
leader transitions, topology changes, migration coordination, snapshots, and
backup. Separating these paths keeps request routing asynchronous without
making transaction execution depend on the control plane's RPC granularity.

Remote completion is fenced twice: the destination validates the NG leadership
term, and the coordinator validates the transaction identity and command
generation before accepting a response. Leader-location caches are advisory;
senders must handle a stale destination, refresh leadership, and retry through
the owning operation's retry policy.

## Leadership lifecycle and term fencing

Election does not immediately make a node serviceable. `OnLeaderStart` first
places the NG in candidate-leader state and starts recovery. The leader term is
published only after every log group has completed replay, so normal CC
requests cannot observe a partially recovered in-memory replica. A synchronized
standby may retain its warm state and replay only the missing tail; a cold
candidate rebuilds from storage and WAL.

On stepdown, the serving and candidate terms are invalidated before NG-owned
memory is released. Code that accesses such memory outside the shard lifecycle
must pair `TryPinNodeGroupData` with `UnpinNodeGroupData`; stepdown waits for
those pins so readers cannot race destruction.

Terms are correctness fences rather than discovery hints. CC messages, WAL
writes, replay work, and standby sessions carry the term under which they were
created. A newer leadership term invalidates old work even if a cached network
route still points at the former leader.

## Cluster scaling

Cluster scaling changes two related authorities: NG membership in
`ClusterConfigCcMap` and data ownership in `RangeBucketCcMap`. The cluster-scale
transaction serializes the topology update and, when ownership changes,
coordinates data-migration transactions on the old owners.

Migration installs a versioned pending owner, makes the affected bucket use the
locked metadata path, synchronizes durable data and any transferable cache
state, then publishes the new owner and removes obsolete state from the old
owner. WAL records preserve the durable stages needed to resume an interrupted
scale or migration; replay uses the stored version and stage instead of
restarting from an assumed process-local state.

Adding election peers without changing bucket ownership does not require data
migration. Changes that do move ownership must not expose the new owner before
the synchronized data is durable and routable.

## Standby replication boundary

Candidate data-bearing members subscribe to the leader's per-shard committed
write stream and combine it with a storage snapshot. This is a separate
protocol with ordering, buffering, re-bootstrap, and snapshot-overlap
invariants; see
[standby_replication_protocol.md](standby_replication_protocol.md). The cluster
layer owns role and term transitions, while the standby protocol owns how a
candidate becomes a complete in-memory replica.

## Cross-cutting invariants

- Only a fully recovered leader term authorizes normal service for an NG.
- Bucket metadata is the authority for NG ownership; cached leader identities
  only locate the current server for that owner.
- Topology versions and NG terms must advance monotonically. Retried or replayed
  updates at an older version are idempotent no-ops or are rejected.
- Remote responses are valid only for the transaction generation and leader
  term that issued the request.
- Every successful node-group data pin must be released; otherwise leader
  stepdown cannot safely reclaim the NG's state.
- The host manager implementation is outside this documentation boundary. Its
  protobuf service contract and the tx-side callbacks are the supported
  integration surface.

## Source map

| Claim | Repository source |
|---|---|
| Cluster topology, placement functions, leader caches, terms, and data pinning are owned by `Sharder` | `tx_service/include/sharder.h`; `tx_service/src/sharder.cpp` |
| Election and membership cross the host-manager RPC boundary | `tx_service/include/proto/cc_request.proto`; `tx_service/src/remote/cc_node_service.cpp`; `tx_service/src/sharder.cpp` |
| An NG becomes serviceable only after leader recovery completes | `tx_service/include/fault/cc_node.h`; `tx_service/src/fault/cc_node.cpp`; `tx_service/src/fault/log_replay_service.cpp` |
| Local and remote requests converge on the CC-request/shard execution model | `tx_service/include/remote/remote_cc_handler.h`; `tx_service/include/remote/remote_cc_request.h`; `tx_service/src/remote/cc_stream_sender.cpp`; `tx_service/src/remote/cc_stream_receiver.cpp` |
| Cluster configuration and bucket ownership are transactional metadata | `tx_service/include/cc/cluster_config_cc_map.h`; `tx_service/include/cluster_config_record.h`; `tx_service/include/cc/range_bucket_cc_map.h`; `tx_service/src/cc/local_cc_shards.cpp` |
| Scaling and data migration are resumable transaction operations | `tx_service/include/tx_operation.h`; `tx_service/src/tx_operation.cpp`; `tx_service/tx-log-protos/log.proto` |
| Cross-NG request routing is exercised by the cluster harness | `tx_service/tests/ClusterCrossNg-Test.cpp`; `tx_service/tests/cluster/` |
| Standby replication has a distinct sequencing and snapshot protocol | `tx_service/include/standby.h`; `tx_service/src/standby.cpp`; `tx_service/tests/StandbyForward-Test.cpp` |
