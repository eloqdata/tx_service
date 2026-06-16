## Overview

Log service is a highly available service built on top of the Raft protocol containing one or more log groups. Each log group is a Raft cluster.

Consensus algorithms allow a collection of machines to work as a coherent group that survives the failures of some of its members. Because of this, they play a key role in building reliable large-scale software systems.

Raft is a consensus algorithm used in replicated state machines. The key role of Raft is to manage a replicated log containing state machine commands from clients. The state machines process identical sequences of commands from the raft logs, so they produce the same outputs and exhibit the same states. For our log service, the state machine is LogState, composed of transaction logs and some other critical information.

## Important roles of log service

### Storing transaction logs

Transaction logs are written to a separate log service instead of each participant ccnode group leader's local disk, as the group leader might change. The log service can scale independently.

### Decider and a place of truth

Although ccnodes are in Raft replication groups too, we use Raft in TxService only for data resource mangement and automatic failover, i.e, if one machine is down, another machine can automatically takes over all the data of the failed node. To avoid memory waste, data is not replicated in every member of ccnode group, only group leader stores the data. Think of ccnodes as Raft clusters that have an empty state machine. So new leader begins empty and  before it can serve requests, it needs to recover data that has not been flushed to storage from the log.

If a ccnode leader fails, what should we do about the ongoing transactions who are holding locks on its data? We can write a log when acquiring locks so the new leader knows which entries are locked by replaying log. But writing log in one transaction’s each lock acquisition would be too costly. Rather, we only write log once, when committing. Without persisting locks in the log, the locks in the old leader are just lost in the new leader and they can be grabed by another transaction. The old locks are not valid any more, but the transactions holding invalid old locks don’t know it and might still try to write log to the log service. These transactions must be rejected as their locks are invalid, but how? To solve this, we need a way to decide whether the transaction’s locks are still valid before actually writing the transaction’s log. It seems the transaction coordinator can simply inquire each lock’s ccnode leader again before writing log. But obviously it doesn’t solve the problem. There is no difference between the second inquiry and the first lock acquisition and the locks might be invalid again. There needs to be a phase for an absolutely reliable decider to decide whether a transaction can be committed or not. That is a consensus problem and we use log service as the decider.

First, the log service stores each ccnode group’s Raft term information. Each ccnode group’s leader must register its term to log service before it can serve requests. Luckily the leader also needs to replay log from the log service, so the two operations are combined into one: UpdateTermAndReplayLog. Thus, there is a strict order about ccnode group’s term evolvement, managed by log service.

Second, we bind resources like each ccnode group’s data and locks with its Raft term. When accessing the data and locks, the term is also recorded. When trying to write log, the term are also be carried. And the log service decides whether transaction can be committed by checking the transaction’s lock terms with the belonging ccnode group’s lastest term. If the terms do not match, this WriteLog request will be rejected. This term check applies not just to WriteLog, we always carry the ccnode group term when dealing with log service.

In summary, besides storing transaction log, we also use log service as a metadata registry like etcd. The most important meta data is each ccnode group’s latest term which we use to check the validity of resources in case of failover. Whenever we need the truth, like whether the locks are still valid when committing, we inquire the log service.

## Components of LogState

### Transaction logs

When transactions are committed, their redo logs are written to the log service through WriteLog API. In log service, the write log operations are first replicated through Raft and then applied to LogState in each log group node.

As data are sharded across different ccnode groups, we are talking about distributed transactions. We take one phase commit, that is, contrary to writing transaction logs in every pariticipants (cc node groups) in 2PL, the transaction coordinator writes log in only one place, its belonging log group. So each log group contains transactions logs of data on all cc node groups. The trade off is, for a ccnode group, as all log groups contain logs of its data, it needs to ReplayLog from all log groups. And transaction logs are organized in group of ccnode groups in LogState so when processing a ccnode group’s ReplayLog request, only this ccnode group’s logs needs to be traversed.

LogState stores data logs of each ccnode group as a map of lists in memory. The total active log size depends on the transaction workloads and the frequency of ccnode checkpoint. Logs are just another raw form of transactions' data that has not been flushed to storage. So there are issues about store all txn logs in memory, for example, the logs might grow very big and takes lots of memory or even use up all available memory. It seems weird if too much memory resources are consumed to store logs only for recovery. And we might need to store logs for quite a long time like weeks or months for point-in-time recovery. Either case, we need a way to flush unused logs from memory to disk. So besides LogStateMemoryImpl, we have LogStateRocksDBImpl which uses RocksDB to store txn logs basically in memtable and flush them to SST files in the background automatically.

### Information about each ccnode group

As described above, LogState also stores each ccnode group’s latest information, including term, lastest committed transaction number, checkpoint timestamp. So when a ccnode group fails over, the lastest committed transaction number and checkpoint timestamp keeps growing.
