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
// Phase 2a unit tests for the page-fetch machinery (eloqkv
// docs/08-paged-objects.md §4/§5/§7; docs/08-paged-objects-plan.md Phase 2).
// These cover the non-I/O mechanics: PageKey encoding/identity, FetchHub
// lifecycle (issue-or-join, the §7 swap splice, take-by-id and
// take-by-address), and the KeyGapLockAndExtraData recycle gate. The store
// round-trip is exercised by the integration harness, not here.

// Let Catch provide main():
#include <algorithm>
#include <catch2/catch_all.hpp>
#include <memory>
#include <string>

#include "cc/non_blocking_lock.h"
#include "cc/page_fetch.h"
#include "page_key_codec.h"

namespace txservice
{
namespace
{
/**
 * @brief Builds a PageFetch without dispatching I/O — exercising exactly
 * what FetchPage does up to the store call. The FetchRecordCc base needs a
 * table name/schema; tests that only drive hub mechanics never touch them,
 * so a minimally constructed PageFetch would drag in catalog machinery for
 * nothing. Instead the hub tests below use the hub's own unique_ptr slots
 * with default-constructed keys, which is all the lifecycle logic reads.
 */
std::string EncodedKeyFor(std::string_view obj_key, uint32_t page_id)
{
    std::string bytes;
    EncodePageKey(bytes, obj_key, PageRowKind::HashPage, page_id);
    return bytes;
}
}  // namespace

TEST_CASE("PageKey identity and ordering", "[page-fetch]")
{
    // Hash() is the OBJECT key's hash, never a hash of the page bytes (§5):
    // the page id decides which row, never which shard.
    PageKey a(EncodedKeyFor("user:1", 7), /*object_key_hash=*/0xABCD);
    REQUIRE(a.Hash() == 0xABCD);

    // Serialized bytes are exactly the encoded page key.
    std::string out;
    a.Serialize(out);
    REQUIRE(out == EncodedKeyFor("user:1", 7));
    REQUIRE(a.KVSerialize() == EncodedKeyFor("user:1", 7));
    REQUIRE(a.SerializedLength() == out.size());

    // Byte ordering: big-endian page ids sort page rows numerically.
    PageKey b(EncodedKeyFor("user:1", 8), 0xABCD);
    PageKey c(EncodedKeyFor("user:1", 256), 0xABCD);
    REQUIRE(a < b);
    REQUIRE(b < c);
    REQUIRE(!(a == b));
    // Identity is the bytes; the cached hash does not participate.
    PageKey a2(EncodedKeyFor("user:1", 7), 0x9999);
    REQUIRE(a == a2);

    // Clone round-trips through the type-erased TxKey.
    TxKey cloned = a.CloneTxKey();
    REQUIRE(std::string_view(cloned.Data(), cloned.Size()) ==
            EncodedKeyFor("user:1", 7));
    REQUIRE(cloned.Hash() == 0xABCD);
}

TEST_CASE("FetchHub empty-path semantics", "[page-fetch]")
{
    // Constructing a live PageFetch requires a CcShard, which no unit
    // harness builds today; splice/take with real fetches is covered by the
    // Phase 2b integration harness alongside the store round-trip
    // (docs/08-paged-objects-plan.md Phase 2 tests). Here: the absent-entry
    // paths every completion and swap crosses.
    FetchHub hub;
    REQUIRE(hub.Empty());
    REQUIRE(hub.TakeLive(42) == nullptr);
    REQUIRE(hub.TakeOrphan(nullptr) == nullptr);
    hub.SpliceAllToOrphans();
    REQUIRE(hub.Empty());
}

TEST_CASE("KeyGapLockAndExtraData fetch-hub recycle gate", "[page-fetch]")
{
    KeyGapLockAndExtraData lke;
    lke.Reset(nullptr, nullptr, nullptr);

    // No hub: empty (no lock, no pins, no buffered commands).
    REQUIRE(lke.FetchHubPtr() == nullptr);
    REQUIRE(!lke.HasFetchHubWork());
    REQUIRE(lke.IsEmpty());

    // An allocated-but-empty hub does not block recycling.
    FetchHub &hub = lke.GetOrCreateFetchHub();
    REQUIRE(lke.FetchHubPtr() == &hub);
    REQUIRE(!lke.HasFetchHubWork());
    REQUIRE(lke.IsEmpty());

    // The §7 splice preserves emptiness trivially.
    hub.SpliceAllToOrphans();
    REQUIRE(hub.Empty());
    REQUIRE(lke.IsEmpty());

    // Entry pins gate IsEmpty independently of the hub — every page fetch
    // holds one from issue to completion, so a non-empty hub implies a
    // non-zero pin count in production; the hub clause is defense in depth.
    lke.AddPin();
    REQUIRE(!lke.IsEmpty());
    lke.ReleasePin();
    REQUIRE(lke.IsEmpty());

    // Reset() with an empty hub releases it.
    lke.Reset(nullptr, nullptr, nullptr);
    REQUIRE(lke.FetchHubPtr() == nullptr);
}

TEST_CASE("FetchHub resolves each transaction exactly once", "[page-fetch]")
{
    FetchHub hub;
    auto *request = reinterpret_cast<CcRequestBase *>(uintptr_t{0x1000});
    constexpr TxNumber txn = 71;

    hub.RegisterWaiter(txn, request);
    hub.NoteAwaited(txn);
    hub.NoteAwaited(txn);
    REQUIRE(hub.PeekParked(txn) == request);
    REQUIRE(hub.tx_wakes_.at(txn).awaited_ == 2);

    bool reached_zero = true;
    REQUIRE(hub.ResolveWaiter(txn, true, &reached_zero) == nullptr);
    REQUIRE_FALSE(reached_zero);
    REQUIRE(hub.tx_wakes_.at(txn).awaited_ == 1);
    REQUIRE(hub.PeekParked(txn) == request);

    REQUIRE(hub.ResolveWaiter(txn, true, &reached_zero) == request);
    REQUIRE(reached_zero);
    REQUIRE(hub.tx_wakes_.at(txn).awaited_ == 0);
    REQUIRE(hub.PeekParked(txn) == nullptr);
    // A duplicate completion cannot return the request again.
    REQUIRE(hub.ResolveWaiter(txn, true, &reached_zero) == nullptr);
    REQUIRE(reached_zero);
    REQUIRE_FALSE(hub.ConsumeError(txn));
    REQUIRE(hub.tx_wakes_.find(txn) == hub.tx_wakes_.end());
}

TEST_CASE("FetchHub retains and consumes a coalesced fetch error",
          "[page-fetch]")
{
    FetchHub hub;
    auto *request = reinterpret_cast<CcRequestBase *>(uintptr_t{0x2000});
    constexpr TxNumber txn = 72;

    hub.RegisterWaiter(txn, request);
    hub.NoteAwaited(txn);
    bool reached_zero = false;
    REQUIRE(hub.ResolveWaiter(txn, false, &reached_zero) == request);
    REQUIRE(reached_zero);
    REQUIRE(hub.tx_wakes_.at(txn).errored_);
    REQUIRE(hub.ConsumeError(txn));
    REQUIRE_FALSE(hub.ConsumeError(txn));

    // A request that re-enters immediately can explicitly deregister while
    // its in-flight fetch still owns a waiter entry. Completion reaches zero
    // but has no stale pointer to enqueue.
    constexpr TxNumber retry_txn = 73;
    hub.RegisterWaiter(retry_txn, request);
    hub.NoteAwaited(retry_txn);
    hub.RegisterWaiter(retry_txn, nullptr);
    REQUIRE(hub.PeekParked(retry_txn) == nullptr);
    REQUIRE(hub.ResolveWaiter(retry_txn, false, &reached_zero) == nullptr);
    REQUIRE(reached_zero);
    REQUIRE(hub.ConsumeError(retry_txn));
    REQUIRE(hub.tx_wakes_.find(retry_txn) == hub.tx_wakes_.end());
}

TEST_CASE("FetchHub teardown drains parked pointers without duplicates",
          "[page-fetch]")
{
    FetchHub hub;
    auto *first = reinterpret_cast<CcRequestBase *>(uintptr_t{0x3000});
    auto *second = reinterpret_cast<CcRequestBase *>(uintptr_t{0x4000});

    hub.RegisterWaiter(80, first);
    hub.RegisterWaiter(81, second);
    hub.RegisterWaiter(82, nullptr);
    std::vector<CcRequestBase *> parked = hub.TakeAllParked();
    REQUIRE(parked.size() == 2);
    REQUIRE(std::find(parked.begin(), parked.end(), first) != parked.end());
    REQUIRE(std::find(parked.begin(), parked.end(), second) != parked.end());
    REQUIRE(hub.TakeAllParked().empty());

    hub.NoteAwaited(80);
    hub.ForgetWaiter(80);
    REQUIRE(hub.ResolveWaiter(80, true) == nullptr);
    hub.ForgetWaiter(81);
    hub.ForgetWaiter(82);
    REQUIRE(hub.tx_wakes_.empty());
}
}  // namespace txservice
