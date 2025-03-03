# Transaction Service
Transaction Service of EloqDB.

# Overview

With the trend of digital transformation, massive volumes of data and the velocity of data expansion became popular in data driven companies. Traditional single node database often becomes the bottleneck of the business growth. About 20 years ago, hadoop emerged and the following NoSQL movement tried to overcome this issue, but with the tradeoff that NoSQL systems often give up strong transaction gurantee. This is painful as a universal database and introduce a large amount of work at application layer. Then in 2012, Google introduce Spanner, a NewSQL distributed database, which is used to replace its NoSQL system BigTable. Since then, Spanner's followers like CockroachDB, YugabyteDB, TiDB etc. joined the race, and supplied elastic distributed databases with strong transaction support. The target is to make users feel they are still using single node database (replacement of Oracle/PostgreSQL/MySQL), but the underneath engine can scale out to hundreds and even thousands of nodes to overcome the performance bottleneck.

But there is no free lunch. To reduce the complexity of handling fault tolerance in a distributed database, NewSQL databases get rid of the concept of the buffer pool from tranditional monothlic database. This leads to high latency and low throughput in the NewSQL design. Comparing single node MySQL with single node NewSQL, NewSQL is much slower than MySQL. Of course NewSQL database can scale out, but at the cost of low cost performance. To enhance the performance of NewSQL database, we bring back the buffer pool in the Transaction Service of EloqDB. At the same time, we supplies a more sophicated fault tolerance algorithm to gurantee the high availability and strong transaction during node failure, which will be illustrated in the following sections.

Transaction service of EloqDB is a distributed, fault-tolerant, high-performance, in-memory storage & transaction service with fine-grained transaction support. It's not written for a specific database, but play a core role in EloqDB, which is a pluggable data platform. Different kinds of database runtime and storage can be integrated with EloqDB to achieve the high performance transaction and consistent view. Take MariaDB/MySQL as an example, InnoDB storage engine of MariaDB is replaced by the eloq distributed storage engine. All the transaction requests such as read and write will be redirected into the TxService of EloqDB. Data in TxService are partitioned across the cluster by sharding policy, which could be hash partition or range partition. Hence a MariaDB connection can connect to any node to run the SQL (support multiple write), but the SQL will be transformed as a TxRequest and possibly be redirected to other nodes to read/write the data. But from MariaDB's point of view, it's not aware of the size of TxService cluster. Sending the request and receiving the response, all things done.

To be noted, transaction service only handles the requests from storage engine layer, it's not responsible for query execution. MariaDB's executor will read data from underlying TxService and do its work the same as the data from innodb engine. Take two tables join as an example, the two tables are read from TxService cluster, but the join is finished at executor where the MariaDB connection resides. In future, we will embed MPP runtime into TxService in order to speedup execution of operators like join, aggregation etc. or the general OLAP workloads.

# Architecture of Transaction Service
Transaction service follows an asynchrous programming model: requests will be put into a queue and wait for the specific worker thread to executed them asynchrously. To be specific, all the requests including read, write, acquire lock etc. are sharded into different groups which are called CcShards in one EloqDB instance. Each CcShard is assigned with a worker thread called TxProcessor. Each TxProcessor is binded to a single CPU core. Hence Txprocessors execute requests asynchrously and lock free.

Note that for a single CcShard, there is only one thread which processes the request on it. But it doesn't means it is slower than multithreads model. Consider monolithic database like MySQL, each client connection is a thread, and it will do everything including parser, optimizer, executor and to access storage engine with tranaction gurantee as well. But race contention can not be avoided when we are talking about transaction and multithreads, thus locks and latches are introduced and multiple threads have to wait for each other with a high frequency. This leads to a lot of context switch and CPU registers and caches just switch among threads. It's a waste of CPU cycles. Instead, single thread model is lock free and enable CPU to always do the real work. This architectur can also scale with CPU core numbers. You are able to shard data and requests into multiple CcShards and use corresponding multiple worker threads to improve the overall throughput of the system.

The following picture shows the overall architecture and components of transaction service.

<p align="center">
<img src="blob/images/tx_service_fig1.png">
Figure 1 Transaction Service Architecture
</p>

Next let's go through the important components one by one. They are organized into:
1. Main driver: TxService.
2. Transaction worker thread: TxProcessor.
3. Service provider: Sharder.
4. Transaction state machine: TxExecution, TxProcessor, LocalHandler, RemoteHandler, TxLog.
5. Sharded data and transaction: LogicalCcShard, CcShard, CcRequest, TemplateCcMap, SkCcMap, TableLock, RangeFunc etc.
6. Auxillary threads: Checkpointer, Timer.

## Main Driver: TxService
The entrypoint of transaction service. TxService is responsible for the following jobs:
1. Initialize and manage the TxProcessor thread pool.
2. Receive the new transaction requests from storage engine handler and forward to a TxProcessor and generate the transaction state machine: TxExecution.
3. Initialize the Sharder, which manages the CcNodes and RPC services supplied by TxService.
4. Initialize the sharded data: LocalCcShard.
5. Initialize auxillary threads: Checkpointer, Timer.

## Transaction Worker Thread: TxProcessor
TxProcessors are the workhorses of TxService, which is used to execute concurrent transactions in an asynchrous fasion. Each TxProcessor is a physical thread and is binded to a single CPU core, in order to fully utilize the multicores capability, we can start multiple TxProcessors at one TxService node.

TxProcessors have five functionalities:
1. Start new transaction state machine, a.k.a TxExecution, and put them into active TxExecution list.
2. Clean finished TxExecution and put them to freelist.
3. Fetch TxRequest from storage engine handler and launch the state machine of TxRequest.
4. Forwarding the transaction state machine.
5. Process CcRequests which handles the data and lock on CcShard. For example read entry from ccmap, acquire table write lock on table_lock.

TxProcessors have two characteristics:
1. Busy loop: The TxProcessor will process TxRequests, CCRequests continuously when there are active transactions.
2. Lock free: the data like ccmap are also binded to TxProcessors, which results in lock free processing.

The following picutre describes the Topology of TxProcessors. 
<p align="center">
<img src="blob/images/tx_service_fig2.png">
Figure 2 TxProcessor Topology
</p>

## Service Provider
### Sharder
Sharder is the collection of services supplied by TxService, which includes:
1. cc_stream rpc server, which transfers CcRequests between different TxService nodes. It also defines the **cc_stream_sender_** and **cc_stream_receiver_** respectively.
2. cc_node rpc server and corresponding **cc_node_service_**, which is used by raft to communicate CcNodes among the raft group.
3. log_replay rpc server and corresponding **recovery_service_**, which receives redo log from log service by streaming.

Sharder also provides sharding functions.
1. The ShardCode function: given a hash code, convert it to shard code which contain the node_id and residual (used to calcualte core_id).
2. The cache of node group leader.
3. The count of node groups.

### CcNode Service
CcNode service(CcNode cc_node.cpp) is used to provide HA and auto-failover capability of EloqDB. It's actually a Raft service. Multiple CcNodes are gathered to form a node group, each of CcNode can be considered as a replica(default replica number is 3). One of the replica is chosen to be the leader by Raft service and others are named followers. Once leader is down or network partition happens, Raft service can select a new leader to continue the service. We explain the CcNode service in detail with the following steps:
1. Each EloqDB instance contains multiple CcNodes. By default one CcNode is primary and others are secondary. By default primary CcNode is active while secondary CcNodes are inactive.
2. All the CcNodes share one LogicalCcShard. Only active CcNodes stored entries in the ccmap of CcShard. Inactive CcNode will not consume memory resource.
3. When failover happens, a remote primary CcNode is down and the secondary CcNode on this db instance may become the leader (active CcNode) and begin to store entries in its own ccmap. Note that memory usage is not balanced among the cluster since current instance serves more data (two active CcNodes).
4. Once remote primary CcNode is back, it will send `transfer_leader` request to ask secondary CcNode (the current leader) to transfer the leadership back to the primary CcNode to make workload balanced again. 

The following picutre describes the CcNode topology. 
<p align="center">
<img src="blob/images/tx_service_fig4.png">
Figure 3 CcNode Topology
</p>

### CC Stream Service
CcStream service is responsible for shuffing CcRequests among different CcNode groups. Transaction can be executed at any CcNode, but the data are distributed among CcNodes. To access the data on a remote CcNode. A RemoteCcRequest is created and will be redirected using CcStream service. 

CcStreamSender is used to send CcRequest to remote leader CcNodes. Each eloq instance has only one Sharder which contains multiple CcNodes (active or inactive). CcNodes share one CcStreamSender. CcStreamSender will start a background thread `connect_thd_` to setup connection to remote CcStreamReceivers. When send message fails, `connect_thd_` will also reconnect the stream.

CcStreamReceiver is the rpc stream service which handles both CcRquest and CcResponse. It provides the `Connect` API to allow CcStreamSender to setup the connection. It handles stream messages in `OnReceiveCcMsg()`. For request type message, it fetches a RemoteCcRquest from the remote request pool (different requests have diffrent request pools), and put the request into the CcReqQueue. For response type message, it parses the message and fill the CcHandlerResult to mark the current tx operation is finished or failed. And then the following `forward()` of transaction state machine will detect the finish or failure flag of current operation and move forward the state machine accordingly.

### Log Replay Stream Service
Log replay service(ReplayService log_replay_service.cpp) is used to recovery uncheckpointed ccentries from redo log. The procedure is as follows:
1. A CcNode becomes the leader of node group when failover happens.
2. This CcNode will create CcNodeRecoveryAgent and correspond thread **notify_thd_** in function `on_leader_start()` (Raft API for new leader start).
3. CcNodeRecoveryAgent will send ReplayLog request to LogService (all the log groups, since 1PC needs full recovery)
4. LogService replicate and apply (Raft API) the ReplayLog operation by starting the LogShippingAgent.
5. LogShippingAgent will connect to Log Replay streaming service of TxService and send redo logs continously.
6. ReplayService's `on_received_messages()` parse the redo log records one by one and replay them on ccmap by enqueue **ReplayLogCc** request. (Emphasize again that ccmap should be manipulate by TxProcessor in most cases.)


## Transaction State Machine

### TxExecution
TxExecution is the transaction runtime which stores the transaction state and supplies the API to execute a transaction.
API includes:
1. Process(TxRequest): TxProcessor uses this API to launch the state machine of TxRequest.
2. Execute(TxRequest): Storage engine handler uses this API to send TxRequest to TxService asynchrously.
3. Restart(): TxProcessor reuse this TxExecution from free list and set Ongoing flag. 

Transaction state includes:
1. Transaction identifier: **txid_**, **tx_number_**.
2. Transaction status: **tx_status_**.
3. Term of leader: **tx_term_**. The raft leader's term when the transaction BEGIN.
4. Timestamp: **commit_ts_bound_**  is the ts lower bound which is set at BEGIN request. While **commit_ts** is set at COMMIT.
5. TxOperation: **current_op_** records the current blocking operation(e.g. Read operation that sends CcRquest by local/remote handler and waits for the handler result). **state_clock_** records the begin time of blocking operation (used by timeout feature of CcReuqest).
6. Local cache: **rw_set_** is the local read/write cache. Small write tuples are buffered in TxExecution's local memory and upload to local/remote ccmap in Upload stage after Commit stage. Read **read_cce_addr_** records the cc entry's address of read record. This is an optimization of cache miss, which supports to quickly backfill the local/remote ccentry with the value from Cassandra. 
7. Isolation level: **iso_level_** specifies the per-transaction isolation level.We currently support ReadCommitted and Repeatable Read. Default isolation level is ReadCommitted. Serializable will be implemented in future.
8. Concurrecy control protocol: **protocol_** specifies the CC protocol: OCC(default protocol) and Lock based(2PL for short). To be noted, we modify the conventional protocol and unify the two protocol using the same framework, details will be illustated in another article. MVCC is not supported yet.

### TxOperation
Transaction state machine breaks the execution of TxRquests into steps. Each step corresponds to a TxOperation. The current step of state machine is recored in **current_op_** of TxExecution. TxProcessor will call **current_op_**'s `forward()` API to check whether the state machine can:
1. move forward to next step.
2. abort since handler result returns error.
3. abort since timeout happens.

### Local CC Handler
TxExecution will use LocalCcHandler to generate CcRequests and enqueue the requests into CcShard's CcReqQueue. LocalCcHandler also calculates the target CcNode group where the request belongs to based on `ShardCode()`. If the target CcNode group is not on the local CcNode, it will redirect to RemoteCcHandler to handle the remote request. 

### Remote CC Handler
TxExecution will use RemoteCcHandler to generate CcMessage which contains the CcRequest, tx_term, tx_number and CcHandlerResult etc. and then use CcStreamSender to send the CcMessage to the remote CcNode.

### State Machine of TxRequests
Requests which include SELECT, INSERT, UPDATE, DELETE,COMMIT and ABORT statements. They are encapsulated into differents kinds of TxRequests by storage engine handler. This section will discuss the transaction state machine of each TxRequests.

#### Begin
BeginRequest is not `BEGIN` command in SQL, but the initialization request after NewTx generated. It acquire a TEntry slot in CcShard's tx array and generates: txid, tx_number, tx_term and commit_ts_bound_ for TxExecution.

StateMachine: Begin()->PostBegin();

#### Point Read
**ReadRequest** handles PkRead from runtime, the ReadType is **Inside** which means it will read from ccmap.

Process: 
1. FindEmplace ccentry in ccmap with key. If key not found and ccmap is FULL, put the ReadRequest back into the cc_queue(expecting checkpointer will release some space later). 
2. For isolation level >= RepeatableRead, AcquireReadIntention under OCC, it doesn't block further write, but will prevent ccentry being kicked out. AcquireReadLock under 2PL, the request will be put into the wait queue of the ccentry if conflict happens, and notify the TxExecution that request recieved but blocked through Acknowledge message. 
3. Fill the ccentry's payload, commit_ts and payload_status into the handler result and return to TxExecution.
4. Set cache_miss_read_cce_addr_ if cache miss(rec_status_ == RecordStatus::Unknown).
5. Add the ccentry's addr and timestamp into the local rwset if iso_level_ >= RepeatableRead.

StateMachine: Read()->PostRead()

**ReadOutsideRequest** handles cache miss of ReadRequest. It will read tuples from data store(Cassandra) and backfill the ccmap.

Process:
1. Read data store if the return value of ReadRequest is RecordStatus::Unknown.
2. Send ReadType **OutsideNormal** ReadOutsideRequest to TxService. It will backfill the payload to ccmap.
3. Send ReadType **OutsideDeleted** ReadOutsideRequest to TxService. It will set tomb ccentry and prevent access data store again when read the same key in future.

#### Scan Read
The scan operation in eloqDB is a Merge of entries in ccmap, local write set and data store. From TxService's point of view, it is responsible for returning the merged result of ccmap and local write set. It will output the result entries to runtime handler ordered by scan key. Then runtime handler will merge the output of ccm_scanner and cass_scanner given the fact that the two lists are both ordered by the scan key.

**ScanOpenRequest** Process:
1. Create **CcScanner** to store the scan cache.
2. Find the first key to scan in ccmap.
3. Using **map_next_** or **map_prev_** to scan entries and put them into scan cache, currently the cache size is controlled by ScanCache::ScanBatchSize. The scan output is ordered.
4. Future optimization: we implemented a new scan mode: checkpoint delta scan which is not enabled yet. The idea is to skip the entries whose commit_ts is smaller than ckpt_ts_. This is used to get the un-checkpointed data and combine it with data store (e.g. HTAP usage). TODO: it's still slow to traverse all the entries using map_next_ and filter with ckpt_ts_.
5. Create **wset_iters_** if write_set contain local changes.

**ScanNextRequest** Process:
1. ScanNext() to check whether scan cache is used up, if yes, send ScanNextBatchCc request to fill the scan cache with new entries.
2. PostScanNext() read entry in scan cache and check whether the entry's key_ts_ = 0. If yes, it means it's a backfill entry and data store already contains this value, we can skip this entry. If the scan cache is used up (all the remaining entries are backfill entreis), send ScanNextBatchCc request to get a new scan cache.
3. For a non backfill entry, merge it with local write_set using wset_iters_ and output the result entry ordered by scan key. Note that except entry with RecordStatus::Normal needs to be returned, entry with RecordStatus::Deleted also needs to be returned to runtime handler, this is used to filter the entries, which are already deleted in ccmap, but reside in data store.
4. If all of the ccmap entries and local write_set entries are already scanned and returned to runtime handler, return an empty ScanNextRequest and runtime handler will know that the scan is finished.

**ScanCloseRequest** Process:
1. free the CcScanner created in ScanOpen.

#### Insert/Update/Delete
**UpsertRequest** handles all the Insert, Update and Delete requests. We currently only support delete and upsert syntax. Both insert and update are converted into upsert in TxService.

UpsertRequest Process: add the entry into rwset using AddWrite() (Bulk insert is not implemented yet).

State machine: Upsert()

#### Commit
The process of Read Only DML is the subset of Read Write DML, hence skip the detailed explanation.

Read Write DML Commit Process:
1. Commit() sets tx_status_ to Committing and call AcquireWrite()
2. AcquireWrite() iterate the rwset and send AcquireCC request for each modified(upsert/delete) tuples. AcquireCC() request will firstly `FindEmplace` to get the ccentry pointer. Then call `AcquireWriteLock` to try to get the entry lock. Return OK if acquire lock succeeds. Return error if acquire fails under OCC protocol, while put into entry's lockqueue under 2PL protocol and send Acknowledge message to TxExecution. Note that if a transaciton holding lock lasts more than 5 seconds, we will check with LogService whether the transaction is aborted and failed to release the lock. Note that this happens when a ccnode crash before WriteLog. LogService knows that it is not committed, but the original transaction has no chance to release the lock on remote ccentries.
3. PostAcquireWrite() just moves forward the state machine to SetTs().
4. SetTs() firstly calcualte the commit ts: the maximum value of {TxExecution's commit_ts_bound, max{read entries's commit_ts}, max{write enties's last_vali_ts_ + 1}, ccs.ts_base_, tx.lower_bound_}. Then update ccs.ts_base_ with commit_ts.
5. PostSetTs() set the TxExecution's commit_ts_ and call Vali() to do read validation.
6. Vali() iterates readset and send PostRead request for each read entry. PostRead() firstly check whether entry's commit_ts changed compared with read entry's ts(commit_ts when read happens). Note that it's OK to modify entry with read intention under OCC protocol. If true, release read intention and return failure. Then, under OCC protocol, PostRead() will update the entry's last_vali_ts_ and put the transaction which held the write lock on this entry into conflicting_txs and release read intention and return OK. Note that even if return value is OK, when conflicting_txs is not empty, we also need to abort the transaction in most cases(exceptional cases e.g. the held write lock transaction aborted later). While, under 2PL protocol, PostRead() only needs to update last_vali_ts_ and release read intention, conflicting_txs doesn't exist.
7. PostVali() just calls WriteLog() to flush redo logs to LogService.
8. WriteLog() flushes redo log to LogService.
9. PostWriteLog() just moves forward the state machine to SetTxStatus().
10. SetTxStatus() finds the TEntry in ccshard tx array and mark it status as Commit or Abort.
11. PostSetTxStatus() just calls PostProcess()
12. PostProcess() sends PostWrite request and CommitSecondaryKey requests to upload the payload and index data, set ccentry status and release ccentry's write lock if transaction committed. While if transaction aborted, PostProcess() sends PostWrite request to release the write lock(commit_ts is 0 hence no upload operation), and send PostRead request to release the read intention/lock, if readset is not empty (Vali() will clear the readset).
13. ReleaseAllTableLocks() releases the table lock (e.g. the read intention acquired when discovery table/discovery check version).
14. PostPostProcess() notifies the runtime TxRequest finished and set the transaction status (both TxExecution and TEntry) to TxnStatus::Finished.

State Machine: 
1. Read Write DML: Commit()->AcquireWrite()->PostAcquireWrite()->SetTs()->PostSetTs()->Vali()->PostVali()->WriteLog()->PostWriteLog()->SetTxStatus()->PostSetTxStatus()->PostProcess()->ReleaseAllTableLocks()->PostPostProcess()

The following figure explains the above DML procdcure.
<p align="center">
<img src="blob/images/tx_service_fig6.png">
Figure 4 DML request execute procedure
</p>

## Shard Data & Transaction
### LocalCcShard
A collection of CcShards on a local Eloq instance. The number of CcShards in a LocalCcShard is equal to the number of TxProcessors. Note that different machines can configure different number of Ccshards based on the CPU core number. 

### CcShard
TxService also splits data along with locks into different CcShards. Ccshard is the shard unit which will be processed by a specific TxProcessor. Ccshard manages all the state information of a shard, which includes not only the data part but also the transaction information. Typically, a Ccshard in Tx_service contains and manages:
1. CcShard identifier which consists of **node_id_**, **core_id_** and **core_cnt_**.
2. Sharded data: the ccmap selected as the primary replica on this shard: **native_ccms_**. The ccmaps which selected as secondary replica on this shard: **failover_ccms_**. Currently, the secondary ccmap doesn't store any value and recover data from redo log and warm up data from Cassandra on the fly when failover happens. But in future we could get rid of this assumption and tune the replication algorithm based on user requirment, e.g. hot replica to serve read requests.
3. Replicated data: table lock, table catalog, range function etc.: **table_locks_**, **table_metadata_**, **range_func_** etc.
4. Transactions on this ccshard: transaction array **tx_vec_** (item is TEntry), and auxiliary **next_tx_idx_** (next slot in tx array), **next_tx_ident_** (tx identifier on CcShard, used to compute txid), **ts_base_** (base timestamp of CcShard, updated by system clock in Timer thread and commit timestamp generated when tx commit).
5. Transaction & Lock relation: **lock_holding_txs_** records the active txs that have acquired locks/intentions in this shard.
6. CcRequest queue: **cc_queue_** stores all the CcRequests directed to this shard using lock free data structure.
7. LRU CcEntry info: the number of CcEntry **size_**, the head and tail of CcEntry: **head_cce_**, **tail_cce_**. Note that the entry next to head_cce_ is the oldest one (least recently not accessed). 

CcShard contains API to:
1. NewTx(): Find an available TEntry in tranaction array and initialize it. Return the TEntry to the client.
2. Replicate data operations: **CheckCatalogVersion()**, **FindCatalog()**, **ReleaseTableXXLock**(), **AcquireTableXXLock()**. Will be refactored later.

### TEntry
CcShard contains **tx_vec_** to track all the running transactions. Each transaction is declared as TEntry. TEntry contains:
1. **commit_ts**: it will be set when transaction `Commit`.
2. **lower_bound_**: the lower bound of commit_ts, but not used yet. It will be used in future with NegotiateCC.
3. **status_**: transaction status: commit, abort, ongoing, committing, finished
4. **ident_**: an auto incremental uint32 integer, used as the tx identifier. Will wrap around to 0.
5. **vec_idx_**: offet in **tx_vec_**.

TEntry contains API `GetTxId()` to compute the TxId. TxId includes:
1. **global_core_id_**: 32 bits integer. It compose of node_id (higher 22 bits) and local_core_id (lower 10 bits)
2. **ident_**: 32 bits integer. An auto incremental uint32 integer, used as the tx identifier
3. **vec_idx_**: 32 bits integer. offet in **tx_vec_**.

TxNumber is generated from TxId. It is a 64 bits integer which includes:
1. **global_core_id_**: higher 32 bits (with node_id as higher 22 bits and local_core_id as lower 10 bits).
2. **ident_**: lower 32 bits.

The following figure explains the TxId and TxNumber representation.
<p align="center">
<img src="blob/images/tx_service_fig5.png">
Figure 5 TxId and TxNumber representation
</p>

### CcEntry
CcEntry inherits from LruEntry, which contains:
1. LRU linkpointer : **lru_prev_** and **lru_next_**. This link records the age of entry, when ccmap is full, kickout the entries by the order of LRU.
2. **key_lock_**: entry's key lock which contains read, write and corresponding wait queue.
3. **commit_ts_**: commit_ts_ of entry, computed in `SetTs()` operator at TxExecution.
4. **last_vali_ts_**: updated duing `PostRead()`. This is used to advanced the commit_ts for the rule that: tx acquire write intention on entries in write set, the tx commit_ts should be greater than the last_vali_ts_ of all the above entries. 
5. **gap_lock_**, **gap_commit_ts_** and **gap_last_vali_ts_**: not fully implemented yet. Gap in CcEntry is used to handle phantom read using range lock.
6. **ckpt_ts_**: atomic since also update by checkpointer thread. it's the timestamp when this entry was last flushed to the data store.

CcEntry adds addtional fields:
1. **key_**: entry's key.
2. **payload_**: entry's record.
3. **payload_status_**: status `Normal` means entry is in ccmap. Status `Unknown` means entry needs to be read from data store and backfill the ccmap. Status `Deleted` means the entry is deleted on ccmap, deleted entry also needs to be 'flushed' to data store(ccmap is always newer than data store, hence a deleted entry in ccmap will override the existing entry on data store for read request and entry will be deleted on data store by checkpointer asynchrously). Status `RemoteUnknown` is used to describe the optimization that transaction will not wait for the success of backfill, hence entry status is remote unknown.  
4. **insert_intention_set_**: not used yet, currently insert in converted into upsert.
5. **payload_ckpt_**: CkptScanCc will copy the payload_ to payload_ckpt_, since flush to data store is handled by a separate checkpointer thread and original payload can be updated by TxProcessor thread concurrently by copying the payload_ to payload_ckpt_.
6. CcMap link: **map_prev_**, **map_next_**. This link is ordered by key, can be used to do ordered index scan.

## Auxillary Threads
### Checkpoint
Checkpoint is used to flush dirty entries into data store to persist. Different from traditional NewSQL which puts/gets data from data store directly, EloqDB bring back the buffer pool (ccmap) and puts/gets data from ccmap instead. Similar to monothlic database, buffer pool results in dirty entry (page) issues which includes:
1. Write lost: dirty entry (page) is not persisted in data store when node crashs.
2. Partial write: when flush dirty page, disk page size is smaller than database page size. MySQL uses double write buffer and PostgreSQL uses full page write to overcome this problem.
3. Checkpoint jitter. The OLTP performance drops during checkpoint.

Problem 1 is handled in Eloq by redolog. Uncheckpointed log records need to be replayed when a new CcNode becomes the CcNode group leader. Hence all the entries which are committed will appear in the ccmap if they are not in the data store.

Problem 2 doesn't exist in Eloq since checkpoint in Eloq doesn't require data store with transaction feature. Recall that the reason of MySQL use double write buffer is that MySQL cannot recovery the corruptted page using redo log alone, since redo log only records the change of page, but not the page itself. As a result, MySQL flush dirty pages twice: one in DW buffer, the other in the real data page to ensure it can recovery the page at any circumstance. PostgreSQL follows another way to write the complete page (not only the page change) into the redo log after each checkpoint, which is also costy. Eloq is different: the redo log stores the k-v pair,it's sufficient to just use redo log to recover the data. Even though then underlying data store partially writes the k-v pair and is corruptted. It doesn't matter since data store can be recovered by redo log record by record.

Problem 3 will be skipped here, we just pointed out two future optimization direction:
1. incremental checkpoint to amortize the cost of checkpoint.
2. replay data store from LogService. This avoid the network and CPU overhead at TxService side during checkpoint. Instead, Logservice will notify TxService the timestamp which can be used to determine whether the ccentries can be kicked out from ccmap. Issue to be noted: log recored in redo log is not ordered by ts currently, we may need a wait bound algorithm to find a correct timestamp sent to TxService.

Checkpoint process is described in Figure 4 with the following steps:
1. Checkpointer thread will begin to do `ckpt()` in two cases: timeout and be notified when ccmap is full (or a future optimization: at higher watermark). 
2. Function `ckpt()` will firstly send CkptTsCc request to get the timestamp for this checkpoint round. All the entries whose ts is smaller than ckpt_ts will be flushed to data store. Secondly, it will send CkptScanCc request for each ccmap (including failover ccmap and for every table) to all the TxProcessor on this node.
3. TxProcessor will using entry's ckpt link a.k.a ckpt_next_ to go through ccentry list and copy the payload of qualified entry (ts <= ckpt_ts) to payload_ckpt field. Qualified entries will be returned to Checkpointer thread. Note that due to TxProcessor can not be blocked, we split the work into pieces, each time CkptScanCc only processes CkptScanBatch number of entries.
4. Checkpoint thread flushs all the target entries's payload_ckpt to data store using PutAll and PutSkAll API.
5. Checkpoint thread updates all the target entries's ckpt_ts_ and checkpointer thread's last_ckpt_ts_.

The following picutre describes the checkpoint procedure.
<p align="center">
<img src="blob/images/tx_service_fig3.png">
figure 6 checkpoint procedure
</p>

### Timer
Timer is a thread to advance CcShard's ts_base by system clock every 2 seconds. ts_base of shard will also be adjusted by the commit_ts of transaction running on this shard.

## Other Topic

### Term based FTS
Overview: Normally, a distributed transacion in EloqDB invovles multiple CcNodes and they should all be the leaders of each CcNode group. If during the execution of transaction, some remote CcNodes are down or network partition happens, how can we detect it on txCcNode and whether should we abort or commit the transaction? In general node failure or network partition can be handled by Raft protocol, a new leader can be selected automatically and provides service once recovery done (without lock information). Note that leader transfer(failover) causes the change of raft term, thus we could use a protocol based on CcNode's term as the guide of transaction fate: commit or abort. To be simple, During write log to LogService, we check whether any of the invovled CcNodes's term changed by comparing with the CcNode term recorded in LogService (A CcNode firstly update the CcNode's term on LogService and then doing the recovery. As a result, a new leader cannot handle transaction if its term is not reflected in LogService). If yes, we abort the transaction. Otherwise, once the write log succeeds, the transacion is committed.

We divided the term based FTS into the following steps in details:

Transaction start: we get the transaction's term tx_term_. If the transaction owner CcNode is not the leader(term=-1) then abort the transaction directly.

Transaction execution: Upsert and Delete operations only modify the local write set, so term can be ignored. Scan and Read operations fetch entries from ccmap. It appends the term into CcEntryAddr and store it in local read set.

Transaction commit PartI acquire write: acquire write lock for each written entry and store the term into write set.

Transaction commit PartII post read (validation): ensure the term of entries in read set are not changed on all the invovled CcNodes.

Transaction commit PartIII write log:
1. Ensure the tx_term_ is not changed.
2. Ensure every write entry in write set have the same term, record this term as CcNode group's term and sent them to LogService.
3. For serializable isolation, need to ensure the read entries in read set have the same term and sent them to LogService as well, 
4. During write_log request is on_apply at LogService raft side, it will check whther the CcNode group's term in LogService and group's term in request message are the same. If not, we need to abort the transaction since we cannot recover the write lock at the failovered CcNode, which violates the consistence if we continue commit this transaction which involves an previous leader CcNode.
