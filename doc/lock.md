
## LockType

(1) The definition of LockType is: 
```
enum class LockType
{
    NoLock = 0,
    ReadIntent,
    ReadLock,
    WriteIntent,
    WriteLock
};
``` 

(2) Conflicts between different lock types is as follows:

| LockType    | Confilcts With                   |
| ----------- | -------------------------------- |
| NoLock      | -                                |
| ReadIntent  | -                                |
| ReadLock    | WriteLock                        |
| WriteIntent | WriteIntent, WriteLock           |
| WriteLock   | ReadLock, WriteIntent, WriteLock |



## CcProtocol (concurrency control protocol)

The aim of CcProtcol is identify whether block or abort the transcation when the lock want to acquire is conflicted with the lock added(by another tranction) on the CcEntry. 

There are two way: 
- Optimistic: fail and abort when conflict occurs;
- Pessimistic: block and wait for other lock released when conflict occurs.

(1) In EloqDB, we conbine it with read and write operations, the definition of CcProtocol is: 
```
enum class CcProtocol
{
    OCC = 0,  // Optimistic Read + Optimistic Write
    OccRead,  // Optimistic Read + Pessimistic Write
    Locking,  // Pessimistic Read + Pessimistic Write
};
```

## LockOpStatus
It defines the action result of acquiring the lock and how to handle the request if acquiring lock fails.
The definition is:
```
enum class LockOpStatus
{
    Successful = 0,
    Failed,
    Blocked
};
```


## IsolationLevel

(1) The definition of IsolationLevel is: 
```
enum class IsolationLevel
{
    ReadCommitted = 0,
    Snapshot,
    RepeatableRead,
    Serializable
};
```
(2) Notice:
  - "Snapshot isolation level" can be accomplished only using "OCC" or
  "OccRead" CcProtocol.
 
  - "ReadCommitted"/"RepeatableRead"/"Serializable" can be accomplished using
  all CcProtocol.

## CcOperation

CcOperation defines the operations of the request. 
The definition is:  

```
enum class CcOperation
{
    Read = 0,
    ReadForWrite,
    Write,
    ReadSkIndex,
};
```

## The relationship between Lock, CcProtocol, IsolationLevel and CcOperation.

(1) What type of lock to acquire and how to handle the request if conflict occurs under different `IsolationLevel` and `CcProtocol`  

| IsolationLevel   | CcProtocol   | CcOperation                                    |                                 |                                   |               |
| ---------------- | ------------ | ---------------------------------------------- | ------------------------------- | --------------------------------- | ------------- |
|                  |              | Read                                           | Write                           | ReadForWrite                      | ReadSkIndex   |
| ---------------- | ------------ | ---------------------------------------------- | ------------------------------- | --------------------------------- | ------------- |
| ReadCommitted    | OCC          | NoLock                                         | WriteLock( conflict: backoff)   | WriteIntent( conflict: backoff)   | ReadLock      |
|                  | OccRead      | NoLock                                         | WriteLock( conflict: block)     | WriteIntent( conflict: block)     | ReadLock      |
|                  | Locking      | NoLock                                         | WriteLock( conflict: block)     | WriteIntent( conflict: block)     | ReadLock      |
| Snapshot         | OCC          | NoLock                                         | WriteLock( conflict: backoff)   | WriteIntent( conflict: backoff)   | No ReadLock   |
|                  | OccRead      | NoLock                                         | WriteLock( conflict: block)     | WriteIntent( conflict: block)     | No ReadLock   |
| RepeatableRead   | OCC          | ReadIntent (Commit: validate)                  | WriteLock( conflict: backoff)   | WriteIntent( conflict: backoff)   | ReadLock      |
|                  | OccRead      | ReadIntent (Commit: validate)                  | WriteLock( conflict: block)     | WriteIntent( conflict: block)     | ReadLock      |
|                  | Locking      | ReadLock                                       | WriteLock( conflict: block)     | WriteIntent( conflict: block)     | ReadLock      |
| Serializable     | OCC          | ReadIntent on key and gap (Commit: validate)   | WriteLock( conflict: backoff)   | WriteIntent( conflict: backoff)   | ReadLock      |
|                  | OccRead      | ReadIntent on key and gap (Commit: validate)   | WriteLock( conflict: block)     | WriteIntent( conflict: block)     | ReadLock      |
|                  | Locking      | ReadLock on key and gap                        | WriteLock( conflict: block)     | WriteIntent( conflict: block)     | ReadLock      |

*Notice:*
- The request is always blocked if failed to acquire ReadLock.

- Under Snapshot Isolation level, when update a record, we should validate whether the fetched version by snapshot read based transaction's start timestamp is the latest version.
>**Snapshot Isolation** is a guarantee that all reads made in a transaction will see a consistent snapshot of the database (in practice it reads the last committed values that existed at the time it started), and the transaction itself will successfully commit only if no updates it has made conflict with any concurrent updates made since that snapshot. *(cited from [wiki](https://en.wikipedia.org/wiki/Snapshot_isolation))*  

- Under Repeatable Read isolation level, no matter the fetched version is through "Read" or "ReadForWrite", we should always promise the record can not be changed by other transactions before committing the transaction.
(*this is different from general read under Snapshot Isolation*)

(2) How to handle `Select...Lock In Share Mode`?  

This query in mysql means the read operation should acquire ReadLock under all IsolationLevel. (like `Select ... For Update`.) 

To achieve it, we upgrades the IsolationLevel to `RepeatableRead` in `txservice::TxExectution` when handle this read operation, instead of adding another CcOperation like `Select ... For Update`.

(3) Whether acquire lock if the CcEntry's status is `Deleted` or `Unkown` under `RepeatableRead` Isolation Level?

- case the CcEntry is `Deleted`, all read operations don't acquire lock, write operation acquire lock.
- case the CcEntry is `Unkown`, read and write operations acquire lock.

