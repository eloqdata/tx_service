/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */
#pragma once

#include <cstdint>
#include <memory>

#include "cc_protocol.h"  // CcProtocol, IsolationLevel
#include "tx_key.h"       // TxKey, CompositeKey
#include "tx_record.h"    // CompositeRecord
#include "type.h"         // TableName

namespace txservice
{
class TransactionExecution;
class TxService;

namespace test
{
// Build a TxKey wrapping a CompositeKey<int>. Mirrors the key type the
// MockCatalogFactory's primary CcMap is templated on.
TxKey Key(int k);

// Build a CompositeRecord<int> holding a single int field.
std::unique_ptr<CompositeRecord<int>> Rec(int v);

// Knobs for a TestNode. Chainable setters return *this so options can be
// composed inline: TestNodeOptions{}.CoreNum(2).Protocol(CcProtocol::Locking).
struct TestNodeOptions
{
    uint32_t core_num{2};
    bool enable_mvcc{true};
    bool skip_wal{true};
    CcProtocol protocol{CcProtocol::OccRead};
    IsolationLevel isolation{IsolationLevel::Snapshot};

    TestNodeOptions &CoreNum(uint32_t n)
    {
        core_num = n;
        return *this;
    }
    TestNodeOptions &EnableMvcc(bool b)
    {
        enable_mvcc = b;
        return *this;
    }
    TestNodeOptions &Protocol(CcProtocol p)
    {
        protocol = p;
        return *this;
    }
};

// A thin wrapper around a single TransactionExecution. Read/Write helpers are
// added in a later task (they need a registered table); Commit/Abort need no
// table and are implemented here.
class TxHandle
{
public:
    TxHandle(TransactionExecution *txm,
             const TableName *table,
             uint64_t schema_version)
        : txm_(txm), table_(table), schema_version_(schema_version)
    {
    }

    // TODO(C3): implement against a registered table.
    bool Upsert(int key, int value);
    bool Delete(int key);
    bool Read(int key, int &value_out);

    bool Commit();
    bool Abort();

    TransactionExecution *Txm() const
    {
        return txm_;
    }

private:
    TransactionExecution *txm_{nullptr};
    const TableName *table_{nullptr};
    uint64_t schema_version_{0};
};

// An in-process, single-node TxService backed by an in-memory MemDataStore and
// the MockCatalogFactory. Brings the whole engine up in its constructor and
// tears it down cleanly in its destructor. Tier-1 fixture for
// transaction-consistency tests.
class TestNode
{
public:
    explicit TestNode(const TestNodeOptions &options = {});
    ~TestNode();

    TestNode(const TestNode &) = delete;
    TestNode &operator=(const TestNode &) = delete;

    // Begin a transaction with the node's default isolation/protocol.
    TxHandle BeginTx();
    // Begin a transaction with explicit isolation/protocol.
    TxHandle BeginTx(IsolationLevel isolation, CcProtocol protocol);

    const TableName &Table() const;
    uint64_t SchemaVersion() const;
    TxService *Service();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace test
}  // namespace txservice
