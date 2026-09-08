# Transaction service module

`tx_service/` is Data Substrate's in-memory transaction engine. It owns transaction state machines, concurrency-control shards, distributed request routing, checkpoint coordination and recovery. It is normally built through the parent Data Substrate CMake project rather than as an independent product.

Start with the repository architecture [overview](../docs/01-architecture-overview.md), then use the [documentation index](../docs/README.md) to select the focused threading, concurrency-control, transaction, data-model, clustering, durability or partition-management document.

The public integration surface is under `include/`; module tests and their standalone CMake entry point are under `tests/`.
