# Checkpoint

(Please see "README.md" to know the aim of checkpoint)

## Checkpointer thread
Checkpointer is used to flush dirty entries into data store to persist at regular intervals. Also, it can be triggered if there is no enough free memory to use.

## Tasks of Checkpointer
1. Flush the current version data of CcEntry to base tables in DataStore. 
2. Flush the unexpired historical versions (may be used by active transactions) into mvcc_archives table in DataStore if "mvcc" is enabled. (Flush Undo)
3. Notify the "log service" to truncate redo logs.

## The calculation of checkpoint timestamp
We use the minimum WriteLock timestamp on the `CcShard` as the base checkpoint timestamp of the CcShard. (If there is no transaction acquired writelock, we use the {ts_base - 1} as instead)

**Case "mvcc" is not enabled**:  
 use the base checkpoint timestamp as `ckpt_ts`.

**Case "mvcc" is enabled**:  
 use a smaller timestamp than the base checkpoint timestamp as `ckpt_ts`.  
Why ?
Case mvcc is enabled, checkpointer must flush undo. However, many historical versions will be expired as oldest active transaction be committed and write these data to DataStore is a little waste. So, if we use a less timestamp than the base checkpoint timestamp is more appropriate. Of course, use the `GlobalMinSiTxStartTs`as the ckpt_ts can decreasing the count of historical versions to flush  in maximum , but too small ckpt_ts can also cause to many dirty CcEntry (not checkpointed) in memory.

As a compromise, we derive a arg (delay time) and calculate the `ckpt_ts` as follows:
```
max{GlobalMinSiTxStartTs, (BaseCkptTs - CkptDelayTime)}
```

## Difference of checkpoint between mvcc is enabled and not enabled
- Case "mvcc" is not enabled: 

(1) current version's commit_ts is [12], "ckpt_ts" of this checkpoint round is "15", then this entry will do checkpoint. After done, the entry's `ckpt_ts_` will be updated to "12".

(2) current version's commit_ts is [12], "ckpt_ts" of this checkpoint round is "11", then this entry will not do checkpoint and don't update the the entry's `ckpt_ts_`.

- Case "mvcc" is enabled: 
(1) current version's commit_ts is [12], unexpired historical versions's commit_ts are [10,9,8,7], "ckpt_ts" of this checkpoint round is "15", then this entry will do checkpoint. [12] will be flushed into "base table", [10,9,8,7] will be flushed into "mvcc_archives table".
After done, the entry's `ckpt_ts_` will be updated to "12".

(2) current version's commit_ts is [12], unexpired historical versions's commit_ts are [10,9,8,7], "ckpt_ts" of this checkpoint round is "11", then this entry will also do checkpoint. [10] will be flushed into "base table", [9,8,7] will be flushed into "mvcc_archives table".
After done, the entry's `ckpt_ts_` will be updated to "10".


That is, the latest flushed version is always stored in the "base table" and older versions will be flushed into "mvcc_archives table".