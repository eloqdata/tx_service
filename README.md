# Transaction Service

Transaction Service of EloqDB — A distributed, fault-tolerant, high-performance, in-memory storage and transaction engine.

[![GitHub Stars](https://img.shields.io/github/stars/monographdb/tx_service?style=social)](https://github.com/monographdb/tx_service/stargazers)

---

## Overview

The Transaction Service (`TxService`) is a core component of [EloqDB](https://github.com/monographdb), a pluggable data platform designed for high-performance transactions and consistent data views. It is not tied to a specific database but serves as a versatile, distributed in-memory storage and transaction layer. By integrating with various database runtimes and storage engines, such as [EloqSQL](https://github.com/monographdb/eloqsql), it enables seamless scalability and fine-grained transaction support.

For example, in EloqSQL, the InnoDB storage engine of MariaDB is replaced with EloqDB's distributed storage engine. SQL read and write requests are transformed into transaction requests (`TxRequests`) and routed to the `TxService`. Data is sharded across the cluster, allowing a MariaDB client to connect to any node and execute SQL queries (with multiple-writer support). The `TxService` handles request redirection transparently, abstracting the cluster size from the client.

---

## Features

### 🗃️ Distributed Cache
Scales out across multiple nodes and leverages multi-core CPUs. Data and requests are sharded into `CcShards`, each processed by a dedicated `TxProcessor`—a lock-free worker thread pinned to a single CPU core for optimal performance.

### ⚡ Concurrency Control
Supports two protocols: **Optimistic Concurrency Control (OCC)** and **Two-Phase Locking (2PL)**. Offers **ReadCommitted** and **RepeatableRead** isolation levels. Includes a novel **one-phase commit** protocol for distributed transactions, significantly reducing overhead.

### 📄 Durable
Ensures durability by storing transaction redo logs in the [Log Service](https://github.com/monographdb/log_service).

### 🔄 Elastic
Dynamically scales in or out to adapt to changing workloads.

### 💪 Fault Tolerance
Stateless and resilient to node failures. Recovers data from the log service and underlying storage engines like **RocksDB**, **Cassandra**, **ScyllaDB**, **DynamoDB**, or object storage.

---

## Build from Source

The Transaction Service is a foundational engine of EloqDB and can be built alongside these projects:
- [EloqKV](https://github.com/monographdb/eloqkv)  
- [EloqSQL](https://github.com/monographdb/eloqsql)  
- [EloqDoc](https://github.com/monographdb/eloqdoc)  

It can also be built standalone for unit testing:

```bash
mkdir bld
cd bld
cmake ..
cmake --build . -j
ctest
```
---


**Star This Repo ⭐** to Support Our Journey — Every Star Helps Us Reach More Developers!  
