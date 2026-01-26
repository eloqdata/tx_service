# Data Substrate

Data Substrate (also known as TxService) is a common, modular database foundation layer developed by EloqData.

It is API-agnostic and product-independent, and is not designed for or derived from any specific API implementation, including but not limited to:

- EloqKV (Redis-compatible API)

- EloqDoc (MongoDB-compatible API)

- EloqSQL (MySQL-compatible API)

API layers such as EloqKV, EloqDoc, and EloqSQL are separate, higher-level components that integrate with Data Substrate via well-defined interfaces. The existence of these API layers does not impose their licenses or license obligations on Data Substrate.

[![GitHub Stars](https://img.shields.io/github/stars/eloqdata/tx_service?style=social)](https://github.com/eloqdata/tx_service/stargazers)

---

## Overview

The Data Substrate is a core component of [EloqDB](https://github.com/eloqdata), a modular database designed for high-performance transaction and scalability. It is not tied to a specific database but serves as a versatile, distributed in-memory storage and transaction layer. By integrating with various database runtimes and storage engines, such as [EloqSQL](https://github.com/eloqdata/eloqsql), it enables seamless scalability and fine-grained transaction support.

For example, in EloqSQL, the InnoDB storage engine of MariaDB is replaced with EloqDB's distributed storage engine. SQL read and write requests are transformed into transaction requests (`TxRequests`) and routed to the `TxService`. Data is sharded across the cluster, allowing a MariaDB client to connect to any node and execute SQL queries (with multiple-writer support). The `TxService` handles request redirection transparently, abstracting the cluster size from the client.

---

## Features

### 🗃️ Distributed Cache
Scales out across multiple nodes and leverages multi-core CPUs. Data and requests are sharded into `CcShards`, each processed by a dedicated `TxProcessor`—a lock-free worker thread pinned to a single CPU core for optimal performance.

### ⚡ Concurrency Control
Supports two protocols: **Optimistic Concurrency Control (OCC)** and **Two-Phase Locking (2PL)**. Offers **ReadCommitted** and **RepeatableRead** isolation levels. Includes a novel **one-phase commit** protocol for distributed transactions, significantly reducing overhead.

### 📄 Durable
Ensures durability by storing transaction redo logs in the [Log Service](https://github.com/eloqdata/log_service).

### 🔄 Elastic
Dynamically scales in or out to adapt to changing workloads.

### 💪 Fault Tolerance
Stateless and resilient to node failures. Recovers data from the log service and underlying storage engines like **RocksDB**, **Cassandra**, **ScyllaDB**, **DynamoDB**, or object storage.

---

## Build from Source

The Transaction Service is a foundational engine of EloqDB and can be built alongside these projects:
- [EloqKV](https://github.com/eloqdata/eloqkv)  
- [EloqSQL](https://github.com/eloqdata/eloqsql)  
- [EloqDoc](https://github.com/eloqdata/eloqdoc)  

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
