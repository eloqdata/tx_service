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
// Let Catch provide main():
#include <catch2/catch_all.hpp>

#include "error_messages.h"
#include "tx_operation.h"

using txservice::AcquireAllOp;
using txservice::CcErrorCode;

namespace
{
/**
 * @brief An AcquireAllOp with `count` handler results and no txm.
 *
 * CcHandlerResult only dereferences txm_ when is_blocking_ is set, which no
 * plain SetError() path does, so a null txm is enough to drive the real
 * post_lambda_ that maintains fail_cnt_. The op is held in place: its handler
 * results' post_lambda_ capture `this`, and its atomics make it immovable.
 */
struct TestOp
{
    explicit TestOp(uint32_t count) : op(nullptr)
    {
        op.Resize(count);
        op.upload_cnt_ = count;
    }

    AcquireAllOp op;
};
}  // namespace

TEST_CASE(
    "RepresentativeError reports a version mismatch when only fail_cnt_ "
    "is raised",
    "[AcquireAllOp]")
{
    // AcquireAllOp::Forward raises fail_cnt_ on the dedup read-version
    // mismatch path *without* erroring the handler result. An accessor that
    // only scanned hd_results_ would return NO_ERROR there, and a NO_ERROR on
    // a failed operation reads as success at the API boundary -- the silent
    // write loss this change fixes.
    TestOp t{3};
    AcquireAllOp &op = t.op;

    REQUIRE(op.RepresentativeError() == CcErrorCode::NO_ERROR);

    op.fail_cnt_.fetch_add(1, std::memory_order_relaxed);

    REQUIRE(op.RepresentativeError() ==
            CcErrorCode::VALIDATION_FAILED_FOR_VERSION_MISMATCH);
}

TEST_CASE("RepresentativeError prefers an infrastructure error over a conflict",
          "[AcquireAllOp]")
{
    // Conflicts are converted to a retryable exception at the API boundary,
    // and some of those retry loops are unbounded (mongo's writeConflictRetry
    // is `while (true)`). Reporting a node-group that lost leadership as a
    // conflict would retry it forever. A plain first-error-in-index-order
    // accessor passes a "not NO_ERROR" assertion but fails this one.
    SECTION("conflict first in index order")
    {
        TestOp t{3};
        AcquireAllOp &op = t.op;
        op.hd_results_[0].SetError(
            CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_WW_CONFLICT);
        op.hd_results_[1].SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);

        REQUIRE(op.RepresentativeError() ==
                CcErrorCode::REQUESTED_NODE_NOT_LEADER);
    }

    SECTION("infrastructure error first in index order")
    {
        TestOp t{3};
        AcquireAllOp &op = t.op;
        op.hd_results_[0].SetError(CcErrorCode::NG_TERM_CHANGED);
        op.hd_results_[1].SetError(
            CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_WW_CONFLICT);

        REQUIRE(op.RepresentativeError() == CcErrorCode::NG_TERM_CHANGED);
    }
}

TEST_CASE("RepresentativeError keeps the first conflict when all are conflicts",
          "[AcquireAllOp]")
{
    // Within one class the result must be deterministic across runs, so the
    // first in index order wins.
    TestOp t{4};
    AcquireAllOp &op = t.op;
    op.hd_results_[1].SetError(CcErrorCode::DEAD_LOCK_ABORT);
    op.hd_results_[2].SetError(
        CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_WW_CONFLICT);

    REQUIRE(op.RepresentativeError() == CcErrorCode::DEAD_LOCK_ABORT);
}

TEST_CASE("RepresentativeError ignores stale results past upload_cnt_",
          "[AcquireAllOp]")
{
    // hd_results_ is reserved to 8 and grown, never shrunk, so an earlier
    // round's error can sit in the tail. MaxTs() and IsDeadlock() already stop
    // at upload_cnt_ for the same reason.
    TestOp t{8};
    AcquireAllOp &op = t.op;
    op.hd_results_[5].SetError(CcErrorCode::DATA_STORE_ERR);

    // While the entry is in range it is reported, so the assertion below is
    // not vacuous.
    REQUIRE(op.RepresentativeError() == CcErrorCode::DATA_STORE_ERR);

    // Now the state a smaller next round leaves behind. Reset(node_cnt)
    // clears fail_cnt_ and recomputes upload_cnt_ together, so the stale
    // handler result in the tail is the only thing still carrying the old
    // round's error.
    op.upload_cnt_ = 2;
    op.fail_cnt_.store(0, std::memory_order_relaxed);

    REQUIRE(op.RepresentativeError() == CcErrorCode::NO_ERROR);
}

TEST_CASE("IsConflictError classifies the retryable group", "[AcquireAllOp]")
{
    // This list must stay aligned with the codes an API layer converts into
    // its retryable exception; see ThrowIfWriteConflict on the eloqdoc side.
    using txservice::IsConflictError;

    REQUIRE(
        IsConflictError(CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_WW_CONFLICT));
    REQUIRE(
        IsConflictError(CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT));
    REQUIRE(IsConflictError(CcErrorCode::ACQUIRE_GAP_LOCK_FAILED));
    REQUIRE(
        IsConflictError(CcErrorCode::VALIDATION_FAILED_FOR_VERSION_MISMATCH));
    REQUIRE(
        IsConflictError(CcErrorCode::VALIDATION_FAILED_FOR_CONFILICTED_TXS));
    REQUIRE(IsConflictError(CcErrorCode::DEAD_LOCK_ABORT));

    REQUIRE_FALSE(IsConflictError(CcErrorCode::NO_ERROR));
    REQUIRE_FALSE(IsConflictError(CcErrorCode::REQUESTED_NODE_NOT_LEADER));
    REQUIRE_FALSE(IsConflictError(CcErrorCode::NG_TERM_CHANGED));
    REQUIRE_FALSE(IsConflictError(CcErrorCode::REQUEST_LOST));
    REQUIRE_FALSE(IsConflictError(CcErrorCode::DATA_STORE_ERR));
    REQUIRE_FALSE(IsConflictError(CcErrorCode::READ_CATALOG_FAIL));
}
