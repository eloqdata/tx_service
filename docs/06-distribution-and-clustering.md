# 06 — Distribution & Clustering

Data Substrate distributes data across **node groups (NGs)**: raft-style replication groups whose leader holds an in-memory partition of every cc map. The `Sharder` singleton (`tx_service/include/sharder.h`) is the per-process hub for all of it: it owns the cluster topology (`ClusterConfig`), caches NG leaders and terms, maps keys → 1024 buckets → owning NG, and hosts the network services — a brpc *stream* channel for high-frequency cc messages (`remote/cc_stream_sender.h` / `cc_stream_receiver.h`) and a unary *RPC* channel for control-plane operations (`remote/cc_node_service.h`). Raft membership/election itself is run by an external **host manager** process that the tx service talks to only through the `HostMangerService` RPC interface. Cluster topology is itself a transactional record stored in a special cc map (`cc/cluster_config_cc_map.h`), so scaling the cluster is a logged, recoverable distributed transaction (`ClusterScaleOp` + `DataMigrationOp` in `tx_operation.h`). Everything is fenced by terms: every cc request, log record and standby message carries the term it was issued under, and `Sharder::CheckLeaderTerm` rejects anything stale.

Related docs: [01-architecture-overview.md](01-architecture-overview.md), [02-threading-model.md](02-threading-model.md), [04-transaction-execution.md](04-transaction-execution.md) (how txms issue cc requests), [07-durability-and-recovery.md](07-durability-and-recovery.md) (log replay), [08-range-and-bucket-management.md](08-range-and-bucket-management.md) (bucket/range cc maps in depth), [10-log-service.md](10-log-service.md), [standby_replication_protocol.md](standby_replication_protocol.md).

## 0. Module map

| File | Role |
|---|---|
| `tx_service/include/sharder.h`, `src/sharder.cpp` | `Sharder` singleton: topology, leader/term caches, bucket mapping, service bootstrap, host-manager channel |
| `tx_service/include/fault/cc_node.h`, `src/fault/cc_node.cpp` | `CcNode`: per-NG membership object; leader start/stop/follow, log-replay completion, data pinning |
| `tx_service/include/remote/cc_stream_sender.h` / `cc_stream_receiver.h` | Data-plane brpc streams for `CcMessage` traffic |
| `tx_service/include/remote/cc_node_service.h` | Control-plane unary RPC service (`CcRpcService`) |
| `tx_service/include/remote/remote_cc_handler.h` / `remote_cc_request.h` / `remote_type.h` | Remote mirror of `LocalCcHandler`; pooled remote cc-request wrappers; enum conversions |
| `tx_service/include/proto/cc_request.proto` | `CcMessage`, `CcStreamService`, `CcRpcService`, `HostMangerService` definitions |
| `tx_service/include/cluster_config_record.h`, `cc/cluster_config_cc_map.h` | Cluster topology as a transactional cc-map record |
| `tx_service/include/tx_request.h`, `tx_operation.h` | `ClusterScaleTxRequest`, `ClusterScaleOp`, `DataMigrationOp`, `NotifyStartMigrateOp` |
| `tx_service/include/standby.h` | `StandbyForwardEntry`, `StandbySequenceGroup` (see standby doc) |
| `tx_service/include/util.h` | `ParseNgConfig`, `ReadClusterConfigFile`, `ExtractNodesConfigs` |

## 1. Cluster topology model

### Nodes and node groups

- `NodeConfig` (`sharder.h`): `node_id_`, `host_name_`, `port_`, `is_candidate_`. A node group's member list is `std::vector<NodeConfig>`; the whole topology is `std::unordered_map<NodeGroupId, std::vector<NodeConfig>>`.
- `ClusterConfig` (`sharder.h`) adds: `cc_nodes_` (one `fault::CcNode` per NG **this node is a member of**), a monotonically increasing `version_`, and cached shared_ptrs `ng_ids_` / `nodes_configs_` for hot read paths.
- Member roles (cf. `NodeRole` in `proto/cc_request.proto`: `LeaderNode` / `StandbyNode` / `VoterNode`):
  - **candidate** (`is_candidate_ == true`): holds (replicated) data and can be elected leader. The standby-replication protocol applies to these members.
  - **voter** (`is_candidate_ == false`): participates in elections only; holds no data.
- **Native NG** (`Sharder::NativeNodeGroup()`): the NG a node "prefers" to lead. With ip-list configs, the i-th address in `ip_port_list` becomes node i *and* the founding candidate of NG i (`ParseNgConfig` in `util.h` assigns `node_id = ng_id = ng_cnt++`). `IsPreferredGroupLeader()` literally checks `LeaderTerm(node_id_) > 0`, i.e. it relies on native ng id == node id for original members.
- **`node_group_replica_num`** (`rep_group_cnt_` in Sharder): if a NG has fewer members than the target replica count, `AjustNgConfigs` (`util.h`) pads it with the *primary nodes of the next NGs* as non-candidate voters — so a 3-node, 3-NG cluster with replica_num=3 gives every NG 1 candidate + 2 voters without dedicated standby machines.

### Where topology comes from

| Source | Code | Notes |
|---|---|---|
| `ip_port_list` / `standby_ip_port_list` / `voter_ip_port_list` flags | `ParseNgConfig` (`tx_service/include/util.h:217`) | `,` separates NGs, `\|` separates nodes within a NG. Standbys are added as candidates, voters as non-candidates. |
| `cluster_config_file` | `ReadClusterConfigFile` (`util.h:353`) | Plain-text: NG count, then per-NG `ng_id {node_id host port is_candidate}...`, last line = `config_version`. Used to persist/restore topology across restarts and scale events. |

The parsed map plus `config_version` is handed to `Sharder::Init` (`src/sharder.cpp:137`), which builds `cluster_config_`, creates a `fault::CcNode` for every NG that contains this node, and starts the servers.

### Topology versioning and the cluster-config cc map

`cluster_config_.version_` (read via `Sharder::ClusterConfigVersion()`) orders all topology changes. The authoritative, transactional copy lives in a dedicated cc map:

- `ClusterConfigCcMap` (`cc/cluster_config_cc_map.h`): a `TemplateCcMap<VoidKey, ClusterConfigRecord>` with **exactly one entry** (the `neg_inf_` sentinel), stored **only on core 0** and only meaningfully processed on the *preferred leader* of each NG. Its commit_ts **is** the config version.
- `ClusterConfigRecord` (`cluster_config_record.h`): serializes the full `ng_configs` map + version; uses a raw pointer when broadcast locally and owns a deserialized copy when it arrives from remote (`PostWriteAllCc` path).
- Updating — every config change funnels through one path:

```
cluster-scale tx
  └ AcquireAllCc write lock on the single cluster-config entry (all NGs)
  └ write ConfigUpdate log
  └ PostWriteAllCc (broadcast new ClusterConfigRecord to every NG)
       └ ClusterConfigCcMap::Execute(PostWriteAllCc)            [core 0 only]
            └ Sharder::UpdateClusterConfig(new_cfg, commit_ts)  [async, sharder_worker_]
                 1. HostMangerService.UpdateClusterConfig  → raft membership change
                 2. UpdateInMemoryClusterConfig            → ng_configs_/cc_nodes_/version_
                 3. CcStreamSender::UpdateRemoteNodes      → connect/drop streams
                 4. unsubscribe standbys of removed nodes
                 5. re-enqueue the waiting PostWriteAllCc   → releases the lock
```

  Version checks at every step (`version_ >= version → no-op`) make the path idempotent under retries and replay.

## 2. Leadership and terms

### Term caches in Sharder

All are fixed-size arrays of 1000 atomics (max cluster size), initialized to `-1` (`ng_leader_cache_[nid]` is initialized to `nid` — node i is assumed leader of NG i until told otherwise):

| Cache | Meaning | Set by |
|---|---|---|
| `leader_term_cache_[ng]` | This node **is leader** of `ng` at that term (fully recovered). `LeaderTerm()` | `CcNode::FinishLogGroupReplay` after *all* log groups replayed |
| `candidate_leader_term_cache_[ng]` | This node won election for `ng` but is still replaying logs. `CandidateLeaderTerm()` | `CcNode::OnLeaderStart` |
| `ng_leader_cache_[ng]` / `ng_leader_term_cache_[ng]` | Best-effort cache of *which node* currently leads `ng` (used to route remote requests). Explicitly "not reliable". | `UpdateLeader(...)` variants |
| `standby_node_term_cache_` / `candidate_standby_node_term_cache_` | This node is a (candidate) standby subscribed at that term; encodes `(primary_term << 32) \| subscribe_id` (see `PrimaryNodeTerm()`) | subscription flow in `fault/cc_node.cpp` |
| `standby_becoming_leader_term_cache_` | Set while a standby escalates to leader so in-flight forward messages are discarded | `CcNode::OnLeaderStart` |

### Term fencing

`Sharder::CheckLeaderTerm(ng, term)` (`src/sharder.cpp:618`) returns true only if this node is *currently* leader of `ng` at exactly `term`. It is used pervasively: every remote response is dropped if the issuing tx node's term changed (`cc_stream_receiver.cpp:332ff` — "pointer stability does not hold anymore"), cc maps abort requests whose `ng_term` is stale, and WAL records carry per-NG terms (`WriteLogRequest.node_terms` in `tx-log-protos/log.proto`) so the log service can reject writes from deposed leaders (see 10-log-service.md).

### Leadership transitions (driven by the host manager via `CcNodeService` RPCs)

`fault::CcNode` (`fault/cc_node.h`) is the per-NG state object; transitions are single-flight via the `is_processing_` atomic CAS (callers retry). Failover timeline of one NG:

```
host manager elects node B for NG g, term T
        │ CcRpcService.OnLeaderStart(g, T)                (RPC into node B)
        ▼
B: CcNode::OnLeaderStart
   - was full standby?  keep warm cache, replay_start_ts = consistent_ts + 1
   - was candidate standby / cold?  ClearCcNodeGroupData, replay_start_ts = 0
   - SetCandidateTerm(g, T)   ← node is *candidate leader*: not serving yet
        │ log_agent_->ReplayLog(g, T, ..., replay_start_ts)   (per log group)
        ▼
B: FinishLogGroupReplay(lg, T) for each log group         (replay stream done)
   when all log groups done:
   - SetLeaderTerm(g, T); SetCandidateTerm(g, -1)         ← now serving
   - NodeGroupFinishRecovery(g)
        │ NotifyNewLeaderStart(g, B) broadcast (best effort, 100 ms timeout)
        ▼
all nodes: UpdateLeader(g, B, ...)  → ng_leader_cache_[g] = B

meanwhile on old leader A:  CcRpcService.OnLeaderStop(g, oldT)
   - SetLeaderTerm/SetCandidateTerm(g, -1)
   - wait pinning_threads_ == 0; ClearCcNodeGroupData     ← old cache wiped
other candidates:  CcRpcService.OnStartFollowing(g, T, B) → subscribe as standby
```

Step details:

- **`OnLeaderStart(term)`** (`src/fault/cc_node.cpp:257`): rejects if already candidate/leader at ≥ term. If the node *was a candidate standby*, it cannot escalate — clears data and asks for leader transfer. If it was a *full standby*, it unsubscribes all sequence groups, drains in-flight standby requests (`bthread_usleep` polling), waits for data pins, and **may keep its warm cache**: replay restarts from `min(last consistent standby ts)+1` instead of 0 (`replay_start_ts`). Otherwise it clears NG data. Sets `candidate_leader_term`, clears `recovered_log_groups_`, initializes range buckets and prebuilt tables. The node is *not* serving yet.
- **`FinishLogGroupReplay`** (`cc_node.cpp:80`): as committed log records from each log group finish replaying, the group is recorded; when all `log_group_cnt_` groups are done, `candidate_leader_term` is promoted to `leader_term_cache_`, candidate term reset to -1, `NodeGroupFinishRecovery` signaled. Only now do `CheckLeaderTerm`/`TryPinNodeGroupData` succeed.
- **`OnLeaderStop(term)`** (`cc_node.cpp:478`): zeroes both terms, calls the store handler's `OnLeaderStop`, waits until `pinning_threads_ == 0`, then `ClearCcNodeGroupData()` — the old leader's cc maps/catalogs are wiped.
- **`OnStartFollowing(node, term, resubscribe)`** (`cc_node.cpp:698`): a candidate member that lost the election (or sees a new leader) subscribes to the new primary as a standby (`SubscribePrimaryNode`); see §6.
- **`Failover(host, port)`** (`cc_node.cpp:529`): manual leader transfer of the native NG to a target node (waits for shard sync, then requests transfer through the host manager).
- **`NotifyNewLeaderStart`** (`cc_node.cpp:205`): after recovery the new leader broadcasts its identity to all nodes (100 ms timeout, best effort — "remote nodes will also refresh their leader caches passively").

Data pinning: `Sharder::TryPinNodeGroupData` / `UnpinNodeGroupData` bracket any thread touching NG-owned data; `OnLeaderStop` blocks until all pins are released, so an unpaired pin deadlocks leader stepdown (warned in `sharder.h:524`).

### Leader-cache maintenance

`Sharder::UpdateLeader(ng)` (no-arg-leader variant) asks the host manager `GetLeader` asynchronously; `UpdateLeader(ng, node, term)` CASes `ng_leader_term_cache_` monotonically (out-of-date terms are skipped) before publishing `ng_leader_cache_`. Callers invoke it whenever a remote op fails with "not leader" (e.g. `tx_operation.cpp:188`, `cc_stream_receiver.cpp:1533`), so the cache heals lazily through errors.

### Host manager (interface only)

The raft election/membership engine runs in a **separate process**. By default the tx service forks it at startup (`Sharder::Init`, `src/sharder.cpp:324-369`, guarded by `FORK_HM_PROCESS` / the `fork_host_manager` argument, spawning `hm_bin_path` via `posix_spawn`); alternatively an externally managed HM is reached at `hm_ip:hm_port`. The contract is `service HostMangerService` in `proto/cc_request.proto:692`:

| RPC | Direction / purpose |
|---|---|
| `StartNode` | tx service → HM at boot: node id, NG membership, log-service addresses, config version, txservice pid |
| `UpdateClusterConfig` | tx service → HM during cluster scale: install new NG membership |
| `GetLeader` | tx service → HM: refresh `ng_leader_cache_` |
| `Transfer` | leader transfer ("should only be exposed to raft host manager implementation") |
| `CheckHealth`, `AttachTxService`, `DetachTxService` | lifecycle/ops |

The HM calls *back* into the tx service through `CcRpcService` (`OnLeaderStart` / `OnLeaderStop` / `OnStartFollowing`). The HM implementation (`tx_service/raft_host_manager/`) is proprietary and intentionally not documented here. With no HM configured (`hm_ip` empty — tests/single node), `Sharder::Init` self-appoints leader of the native NG at term 1 (`sharder.cpp:478-503`).

## 3. Data placement: buckets, hash partitions, ranges

Constants in `type.h:632`: `total_range_buckets = 0x400` (1024), `total_hash_partitions = 0x400`.

| Function (`sharder.h`) | Mapping |
|---|---|
| `MapKeyHashToBucketId(hash)` | `(hash & 0x3FFF) % 1024` — keyspace → 16384 slots → bucket (the 16384 slot count matches Redis Cluster's slot space; *inference*: chosen for Redis compatibility) |
| `MapRangeIdToBucketId(range_id)` | `MurmurHash3(range_id, seed 9001) % 1024` — a range partition's placement is decided by hashing its range id |
| `MapKeyHashToHashPartitionId(hash)` | `hash % 1024` — kv-store hash partition for a key |
| `MapBucketIdToHashPartitionId(bucket)` | `bucket % total_hash_partitions` (1:1 today) — which kv partition stores a bucket's data |
| `ShardBucketIdToCoreIdx(bucket)` | `(bucket & 0x3FF) % core_count` — which CcShard/core owns the bucket locally |
| `ShardToCcNodeGroup(sharding_code)` | `sharding_code >> 10` — extract the NG from a key shard code |

A **key shard code** is `ng_id << 10 | residual` (`tx_execution.cpp:1973,1999`): the tx state machine looks up the bucket's owning NG, packs it into the high bits, and the low 10 bits keep enough of the key hash to pick the core on the destination node. Bucket → NG ownership is held in `LocalCcShards::buckets_infos_` (`GetBucketInfo` / `GetBucketOwner` / `GetRangeOwner`, `cc/local_cc_shards.h:1726-1738`), initialized on leader start (`InitRangeBuckets`) and mirrored as a cc map (`range_bucket_ccm_name = "__range_bucket"`, `type.h:613`) so bucket ownership changes can be locked/logged like ordinary writes. Normal reads use the lock-free `buckets_infos_`; during migration nodes are switched to lock-protected reads of the `RangeBucketCcMap` (`PublishBucketsMigrating` RPC → `SetBucketMigrating`). Details: 08-range-and-bucket-management.md.

For range-partitioned tables, a key maps to a range id (catalog ranges, see 05-data-model-and-catalog.md), and the *range id* maps to a bucket via `MapRangeIdToBucketId`; the bucket's owner NG owns the range.

## 4. Remote cc plumbing

Port layout (comment at `sharder.h:293`): cc **stream** service on `local_port`; cc-node RPC + raft on `local_port+1` (`GET_CCNODE_RPC_PORT`); log group on `local_port+2`; log-replay stream service on `local_port+3` (`GET_LOG_REPLAY_RPC_PORT`).

### (a) brpc streams — the data plane

High-frequency cc traffic rides `CcMessage` (proto: `proto/cc_request.proto:1407`) over persistent brpc streams:

- **`CcStreamSender`** (`remote/cc_stream_sender.h`): per remote node keeps a *regular* stream and a *long-message* stream (for slow-to-deserialize payloads like `ScanSliceResponse`, so they don't head-of-line-block small messages). A dedicated `connect_thd_` (re)establishes streams (`Connect` RPC of `CcStreamService` then brpc stream accept); each stream carries a version number to serialize concurrent reconnects, plus the peer IP so an IP change forces reconnect (`UpdateStreamIP`). `SendMessageToNg` resolves the NG leader via `ng_leader_cache_` first.
- **Resend logic**: `SendMessageResult{sent, queued_for_retry, need_reconnect}`. Failed writes are queued per node (`resend_message_list_`, EAGAIN cases in `eagain_resend_*`) and re-driven by `resend_thd_`; a hard failure flags the stream for reconnect. Exception: `SendStandbyMessageToNode` is deliberately best-effort (no retry queue) because standby replication has its own sequence-based resend (see standby doc §4.4).
- **`CcStreamReceiver`** (`remote/cc_stream_receiver.h`): a `brpc::StreamInputHandler` that parses inbound messages from a pooled `msg_pool_` (zero-alloc steady state) and dispatches in `OnReceiveCcMsg`:
  - **Requests** (Acquire/Read/Apply/ScanNext/PostWriteAll/...) are wrapped in pooled `Remote*` cc-request objects (`remote/remote_cc_request.h`, e.g. `RemoteAcquire : AcquireCc`) and enqueued to the proper CcShard — from a shard's point of view a remote request is indistinguishable from a local one (see 03-concurrency-control.md).
  - **Responses** must find their transaction: the message carries `tx_number`, `txm_addr`, `handler_addr`, `command_id`, `tx_term`. The receiver first fences with `CheckLeaderTerm(tx_node_id, tx_term)` (tx node id is encoded in the tx number: `(tx_number >> 32) >> 10`), then dereferences `txm_addr`, takes the txm's **shared forward latch** (`AcquireSharedForwardLatch`), and re-validates `txm->TxNumber() == msg.tx_number && txm->CommandId() == msg.command_id` before touching the `CcHandlerResult` at `handler_addr` (`cc_stream_receiver.cpp:332-400`). The tx_number+command_id check makes stale/duplicate responses for a recycled txm harmless; the latch keeps the txm from being reused mid-update.
  - Locking-protocol requests that block remotely send an **acknowledgement** (`is_ack=true` responses carry the remote cce lock address) so the sender can later probe blocked requests (`BlockedCcReqCheckRequest`).

Round trip of one remote cc operation:

```
node A (tx coordinator)                          node B (data owner, leader of g)
TransactionExecution
  └ LocalCcHandler::Read(...)
      owner = LeaderNodeId(g) ≠ A
  └ RemoteCcHandler::Read → CcMessage{type=ReadRequest,
        tx_number, command_id, tx_term, txm_addr, handler_addr}
  └ CcStreamSender::SendMessageToNg(g)  ──stream──▶  CcStreamReceiver
                                                       └ RemoteRead (pooled) → CcShard queue
                                                       └ TemplateCcMap::Execute(ReadCc)
                                                          (fenced by B's LeaderTerm(g))
       CcStreamReceiver  ◀──stream──  ReadResponse{tx_number, command_id, ...}
  └ CheckLeaderTerm(A, tx_term)?  drop if A's term changed
  └ txm = (TransactionExecution*)txm_addr
      AcquireSharedForwardLatch
      txm.TxNumber()==tx_number && txm.CommandId()==command_id?
        └ yes: fill CcHandlerResult at handler_addr → txm resumes
        └ no:  stale response, recycle message
```

### (b) `CcNodeService` unary RPCs — the control plane

`remote::CcNodeService` (`remote/cc_node_service.h`, service `CcRpcService` in the proto) on port+1. Channels to peers are cached in `Sharder::cc_node_service_channels_` (`GetCcNodeServiceChannel` / `UpdateCcNodeServiceChannel` to refresh a broken channel exactly once). RPC groups:

| Group | RPCs |
|---|---|
| Leadership | `OnLeaderStart`, `OnLeaderStop`, `OnStartFollowing` (from HM); `NotifyNewLeaderStart` (leader → everyone) |
| Tx recovery | `CheckTxStatus` (orphan-lock recovery asks the coordinator's node), `GetMinTxStartTs` |
| Cluster scale | `ClusterAddNodeGroup`, `ClusterRemoveNode`, `NodeGroupAddPeers`, `CheckClusterScaleStatus`, `GetClusterNodes`, `UpdateClusterConfig` |
| Migration / index build | `InitDataMigration`, `PublishBucketsMigrating`, `UploadBatch`, `UploadBatchSlices`, `UploadRangeSlices`, `GenerateSkFromPk`, `FlushDataAll` |
| Standby / snapshot | `StandbyStartFollowing`, `ResetStandbySequenceId`, `UpdateStandbyConsistentTs`, `UpdateStandbyCkptTs`, `RequestStorageSnapshotSync`, `OnSnapshotSynced`, `RequestSyncSnapshot`, `FetchNodeInfo`, `FetchPayload`, `FetchCatalog` |
| Backup | `CreateBackup`, `FetchBackup`, `TerminateBackup`, `CreateClusterBackup`, `FetchClusterBackup` |
| Shutdown ckpt | `NotifyShutdownCkpt`, `CheckCkptStatus` |

### Location transparency

`remote::RemoteCcHandler` (`remote/remote_cc_handler.h`) exposes the same operation set as `LocalCcHandler` (AcquireWrite, Read, PostWrite/PostWriteAll, ScanOpen/ScanNext, ObjectCommand, UploadTxCommands, KickoutData, ...). `LocalCcHandler` picks per call: if `Sharder::LeaderNodeId(ng) == this node`, enqueue locally; else marshal into a `CcMessage` and `CcStreamSender::SendMessageToNg` (e.g. `local_cc_handler.cpp:1718-1766`). The tx state machine (04) never knows where data lives.

## 5. Cluster scaling

Entry points: control plane calls `ClusterAddNodeGroup` / `ClusterRemoveNode` / `NodeGroupAddPeers` on the **preferred leader** (`cc_node_service.cpp:506ff` rejects if `LeaderTerm(native_ng) <= 0`). The handler starts a tx (Locking + RepeatableRead, pinned to **log group 0** which enforces one scale event at a time) and executes a `ClusterScaleTxRequest` (`tx_request.h:1181`) → `ClusterScaleOp` (`tx_operation.h:1263`). New topology is computed by `Sharder::AddNodeGroupToCluster` / `RemoveNodeFromCluster` / `AddNodeGroupPeersToCluster`.

`ClusterScaleOp` stage order (verbatim from the header comment, `tx_operation.h:1288-1321`):

```
AddNodeGroup:  prepare_log → acquire cluster-config write lock → config-update log
               → install config (PostWriteAll, releases lock; new streams + raft nodes)
               → notify migration → wait migration finished → clean log
RemoveNode:    prepare_log → notify migration → wait migration finished
               → acquire lock → config-update log → install config → clean log
AddNodeGroupPeers: no data migration (bucket ownership unchanged)
```

i.e. add-node installs the new config *first* (new NGs can run txs but own no buckets yet), remove-node drains bucket ownership *first*. `pub_buckets_migrate_begin/end_op_` bracket the whole thing, flipping every node to locked bucket reads (`PublishBucketsMigrating` RPC). Log stages are `ClusterScaleStage{PrepareScale, ConfigUpdate, CleanScale}` inside `ClusterScaleOpMessage` (`tx_service/tx-log-protos/log.proto:186,210`), which also embeds the new NG configs and the per-NG `NodeGroupMigrateMessage` progress map.

### Bucket migration (`DataMigrationOp`, `tx_operation.h:1395`)

The coordinator's `NotifyStartMigrateOp` sends `InitDataMigration` RPCs to each *old owner* NG with its `{bucket_id → new_owner}` plan. The handler (`cc_node_service.cpp:843`) builds a shared `DataMigrationStatus` (`cc/local_cc_shards.h:104`: cluster_scale_txn, batched bucket id lists, new owners, worker tx numbers, `next_bucket_idx_`, `unfinished_worker_`) and spawns multiple worker txs (`DataMigrationTxRequest`); each worker grabs the next batch of buckets until the list is drained. Per batch the pipeline is (fields of `DataMigrationOp`):

1. `write_first_prepare_log_op_` — validated against the live ClusterScaleTx log (log service rejects if the scale tx is gone, keeping late notifications idempotent); also plants an empty log for RecoverTx.
2. `write_before_locking_log_op_` → `prepare_bucket_lock_op_` (AcquireAll write lock on the bucket entry on **all** NGs) → `prepare_log_op_`.
3. `install_dirty_bucket_op_` (PostWriteAll installs the dirty bucket record with the new owner, downgrades to write intent) → `data_sync_op_` (flush bucket data to the kv store so the new owner can read it) → `acquire_bucket_lock_op_` (re-upgrade) → `commit_log_op_`.
4. `kickout_data_op_` (drop the bucket's entries from the old owner's memory) → `post_all_bucket_lock_op_` (commit the new bucket record, release locks) → `clean_log_op_`; the final worker writes `write_last_clean_log_op_`.

Bucket-level stages persisted in the log: `BucketMigrateStage{NotStarted, BeforeLocking, PrepareMigrate, CommitMigrate, CleanMigrate}`; data arriving for in-flight buckets is uploaded with `UploadBatch(kind=DIRTY_BUCKET_DATA)`.

### Recovery of in-flight scale/migration

On failover, replaying the cluster-scale log hits `ClusterConfigCcMap::Execute(ReplayLogCc)` (`cluster_config_cc_map.h:287`): it inspects `ClusterScaleOpMessage.stage` + per-NG migrate progress, re-acquires the cluster-config write lock if the crash happened while it was held, re-installs the new config if `ConfigUpdate` was logged but not applied (`UpdateClusterConfig` against the HM again), and — on the coordinator NG only (identified by `(txn >> 32) >> 10 == ng_id`) — spins up a fresh txm with `RecoverClusterScale(scale_op_msg, dm_started, dm_finished)` to resume from the recorded stage. Migration worker txs are likewise resumed from their own log records (`DataMigrateTxLogMessage.Stage{Prepare, Commit, Clean}`). `CheckClusterScaleStatus` lets the control plane poll a scale id whose outcome was lost (`ClusterScaleWriteLogResult::UNKOWN`).

## 6. Standby replication (summary)

Candidate members that are not the leader follow the primary as **standbys**: `OnStartFollowing` triggers `SubscribePrimaryNode`, which registers via the `StandbyStartFollowing` / `ResetStandbySequenceId` RPCs, bootstraps from a storage snapshot (`RequestStorageSnapshotSync` / `OnSnapshotSynced`), and then consumes a continuous stream of `KeyObjectStandbyForwardRequest` cc messages. Ordering is per **sequence group** — one `StandbySequenceGroup` per CcShard/core (`standby.h:83`) tracking `next_expecting_*`, the missing-id gap set, and the contiguous-prefix watermark `last_standby_consistent_ts_`; `StandbyForwardEntry` (`standby.h:37`) is the primary-side buffered message. The whole session is fenced by the standby term `(primary_term << 32) | subscribe_id`. Periodic `UpdateStandbyCkptTs` broadcasts (`BrocastPrimaryCkptTs`, `standby.h:136`) advance standby truncation watermarks. On election, a *full* standby can escalate to leader keeping its warm cache and replaying the WAL only from its consistent ts (`CcNode::OnLeaderStart`, §2); a *candidate* standby cannot. Full protocol — capture point, buffering/out-of-sync semantics, fuzzy-snapshot barrier, invariants P1–P6 — is in [standby_replication_protocol.md](standby_replication_protocol.md).

## 7. Command redirects

For object (Redis-style) commands, the engine above may not know which NG owns a key. `txservice_auto_redirect_redis_cmd` (`tx_service_common.h:48`, default `true`, settable via `TxService` init, `tx_service.h:1195`) controls what `LocalCcHandler::ObjectCommand` does when the owner NG's leader is not this node (`local_cc_handler.cpp:1740`): if redirect is enabled (or `always_redirect`), the command is transparently forwarded via `RemoteCcHandler::ObjectCommand` over the cc stream; otherwise the cc request fails with `CcErrorCode::DATA_NOT_ON_LOCAL_NODE` and the API layer is expected to return a protocol-level redirect (e.g. Redis `MOVED`) computed from the bucket → NG → leader-node mapping. See 04-transaction-execution.md for the ObjectCommand path.

## 8. Gotchas & invariants

- **Terms fence everything.** Remote responses, log writes, replay, standby messages, and `UploadBatch`-style RPCs all carry a term and are dropped/rejected on mismatch. Never cache a cce address or txm pointer across a term change — pointer stability is only guaranteed within a term (`cc_stream_receiver.cpp:336` comment).
- **Leader caches are best-effort.** `ng_leader_cache_` starts as "node i leads NG i" and is healed lazily: by `NotifyNewLeaderStart` pushes (100 ms timeout, no retry) and by `UpdateLeader(ng)` pulls after errors. Code sending to an NG must tolerate "not leader" responses and call `UpdateLeader` + retry (the standard pattern in `tx_operation.cpp`).
- **`UpdateLeader(ng, node, term)` is monotonic** — it CASes on `ng_leader_term_cache_` and skips stale terms, so out-of-order notifications can't roll the cache backward (`sharder.cpp:681`).
- **Leader ≠ candidate.** Between `OnLeaderStart` and the last `FinishLogGroupReplay`, only `CandidateLeaderTerm()` is set; `CheckLeaderTerm` still fails and normal transactions can't touch the NG. Replay-time cc requests check the candidate term instead (e.g. `ClusterConfigCcMap::Execute(ReplayLogCc)`).
- **Pin/unpin discipline.** `TryPinNodeGroupData` must be paired with `UnpinNodeGroupData`, "otherwise OnLeaderStop will be blocked forever" (`sharder.h:524`). `OnLeaderStart`/`OnLeaderStop` themselves are single-flight (`is_processing_` CAS) and the HM retries them.
- **Stream reconnects are versioned and IP-aware.** Each outbound stream carries a version to stop two threads from reconnecting concurrently; a changed peer IP (node restarted elsewhere) triggers reconnect (`UpdateStreamIP`). After topology changes the sender "spams" connects until all expected streams are up (`spam_stream_connect_`).
- **Two stream classes per peer.** Long-deserialization messages (scan slices) use a separate stream so bulk scans cannot stall point operations.
- **Standby sends are fire-and-forget at the stream layer** (`SendStandbyMessageToNode`); recovery is the standby's sequence-gap machinery, not the generic resend queue. Don't route standby traffic through `SendMessageToNode`.
- **Cluster config writes only happen on the preferred leader** (`ClusterConfigCcMap::Execute(PostWriteAllCc)` errors with `REQUESTED_NODE_NOT_LEADER` if `!shard_->IsNative(ng)`), and only **one scale event** can run cluster-wide (serialized by log group 0 + the single cluster-config lock entry).
- **`Sharder::Instance()` is init-once.** The first call's parameters matter (`Init` does the real work); everything else reads global state. `cc_nodes_init_` is the acquire/release barrier that makes term caches safe to read from all cores.
- **Bucket reads during migration.** Outside migration, bucket lookups are lock-free against `buckets_infos_`; `PublishBucketsMigrating` flips nodes to lock-protected `RangeBucketCcMap` reads, and the flag is also restored during replay (`SetBucketMigrating` in the ReplayLogCc path). Skipping the flip would let txs read stale ownership without conflict detection.
