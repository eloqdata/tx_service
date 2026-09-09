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
#include <atomic>
#include <catch2/catch_all.hpp>
#include <cstdint>
#include <string>
#include <utility>

#include "cc_request.pb.h"
#include "read_write_entry.h"
#include "remote/apply_response_util.h"
#include "tx_execution.h"
#include "tx_operation.h"
#include "tx_operation_result.h"
#include "tx_req_result.h"
#include "tx_request.h"

namespace txservice
{
// Seed a completed CC operation, then let the real Forward/commit-tail code
// finish the transaction. This isolates result ownership from the networking,
// catalog, and storage harnesses; it does not emulate their execution.
class TransactionExecutionTestPeer
{
public:
    static TxmStatus Forward(TransactionExecution &txm)
    {
        return txm.Forward();
    }

    static ObjectCommandOp &ObjectOp(TransactionExecution &txm)
    {
        return txm.obj_cmd_;
    }

    static void FinishObjectReply(TransactionExecution &txm,
                                  TxResult<RecordStatus> &reply,
                                  bool post_process_tail = false)
    {
        txm.rec_resp_ = &reply;
        PrepareTail(txm, TxnStatus::Committed, post_process_tail);
    }

    static void FinishExplicitReply(TransactionExecution &txm,
                                    TxResult<bool> &reply,
                                    bool commit,
                                    bool post_process_tail)
    {
        txm.bool_resp_ = &reply;
        PrepareTail(txm,
                    commit ? TxnStatus::Committed : TxnStatus::Aborted,
                    post_process_tail);
    }

private:
    static void PrepareTail(TransactionExecution &txm,
                            TxnStatus status,
                            bool post_process_tail)
    {
        txm.tx_status_.store(status, std::memory_order_relaxed);
        if (post_process_tail)
        {
            txm.post_process_.Reset(0, 0, 0, 0, false);
            txm.post_process_.is_running_ = true;
            txm.state_stack_.push_back(&txm.post_process_);
        }
        else
        {
            txm.update_txn_.is_running_ = true;
            txm.update_txn_.hd_result_.SetFinished();
            txm.state_stack_.push_back(&txm.update_txn_);
        }
    }
};

namespace
{
constexpr size_t kLargeImageSize = 1024 * 1024;

ObjectCommandResult &SeedImage(TransactionExecution &txm)
{
    auto &result =
        TransactionExecutionTestPeer::ObjectOp(txm).hd_result_.Value();
    result.rec_status_ = RecordStatus::Normal;
    result.commit_ts_ = 123;
    result.ttl_reset_ = true;
    result.recover_cmd_image_.assign(kLargeImageSize, 'x');
    return result;
}

void RequireImageReleased(const ObjectCommandResult &result)
{
    REQUIRE(result.recover_cmd_image_.empty());
    REQUIRE(result.recover_cmd_image_.capacity() == std::string{}.capacity());
    // Final request replies still depend on handler-result status. This change
    // must not indiscriminately reset the rest of the operation result.
    REQUIRE(result.rec_status_ == RecordStatus::Normal);
    REQUIRE(result.commit_ts_ == 123);
    REQUIRE(result.ttl_reset_);
}
}  // namespace

TEST_CASE("Object commit tail releases the image after preserving the reply",
          "[transaction-image]")
{
    const bool post_process_tail = GENERATE(false, true);
    TransactionExecution txm(nullptr, nullptr, nullptr);
    auto &result = SeedImage(txm);
    auto &op = TransactionExecutionTestPeer::ObjectOp(txm);
    op.Reset();
    REQUIRE(result.recover_cmd_image_.size() == kLargeImageSize);

    TxResult<RecordStatus> reply(nullptr, nullptr);
    TransactionExecutionTestPeer::FinishObjectReply(
        txm, reply, post_process_tail);
    REQUIRE(TransactionExecutionTestPeer::Forward(txm) == TxmStatus::Finished);
    REQUIRE(reply.Status() == TxResultStatus::Finished);
    REQUIRE(reply.Value() == RecordStatus::Normal);
    RequireImageReleased(result);
}

TEST_CASE("Explicit commit and abort tails release pooled recovery images",
          "[transaction-image]")
{
    const bool commit = GENERATE(false, true);
    const bool post_process_tail = GENERATE(false, true);
    TransactionExecution txm(nullptr, nullptr, nullptr);
    auto &result = SeedImage(txm);
    TxResult<bool> reply(nullptr, nullptr);
    TransactionExecutionTestPeer::FinishExplicitReply(
        txm, reply, commit, post_process_tail);
    REQUIRE(TransactionExecutionTestPeer::Forward(txm) == TxmStatus::Finished);
    REQUIRE(reply.Status() == TxResultStatus::Finished);
    REQUIRE(reply.Value() == commit);
    RequireImageReleased(result);
}

TEST_CASE("Terminal abort releases a large image after a small command reuse",
          "[transaction-image]")
{
    TransactionExecution txm(nullptr, nullptr, nullptr);
    auto &result = SeedImage(txm);
    auto &op = TransactionExecutionTestPeer::ObjectOp(txm);
    op.Reset(nullptr, nullptr, nullptr);
    REQUIRE(result.recover_cmd_image_.empty());
    REQUIRE(result.recover_cmd_image_.capacity() >= kLargeImageSize);
    result.recover_cmd_image_ = "small";

    // An unstarted/fenced transaction takes the actual early-abort Reset path.
    AbortTxRequest abort;
    txm.ProcessTxRequest(abort);
    REQUIRE(abort.tx_result_.Status() == TxResultStatus::Finished);
    REQUIRE_FALSE(abort.Result());
    REQUIRE(txm.TxStatus() == TxnStatus::Finished);
    REQUIRE(result.recover_cmd_image_.empty());
    REQUIRE(result.recover_cmd_image_.capacity() == std::string{}.capacity());
}

TEST_CASE("Remote response and copied log image outlive transaction cleanup",
          "[transaction-image]")
{
    TransactionExecution txm(nullptr, nullptr, nullptr);
    auto &result = SeedImage(txm);
    remote::ApplyResponse owner_response;
    owner_response.set_rec_status(remote::RecordStatusType::NORMAL);
    owner_response.set_commit_ts(123);
    owner_response.set_ttl_reset(true);
    owner_response.set_ttl(UINT64_MAX);
    owner_response.set_recover_cmd_image(std::string(kLargeImageSize, 'r'));
    std::string wire;
    REQUIRE(owner_response.SerializeToString(&wire));
    remote::ApplyResponse received;
    REQUIRE(received.ParseFromString(wire));

    // Use the production backfill helper under the same latch held by the
    // receiver. The actual remote receiver's stale-message checks are covered
    // by source review, not a fabricated network-response path here.
    txm.AcquireSharedForwardLatch();
    remote::BackfillObjectCommandResult(result, received);
    txm.ReleaseSharedForwardLatch();
    CmdSetEntry log_entry(1, 2, 3, std::string("key"), false);
    log_entry.AddOverwriteCommandImage(result.recover_cmd_image_, UINT64_MAX);
    REQUIRE(log_entry.cmd_str_list_.size() == 1);

    TxResult<RecordStatus> reply(nullptr, nullptr);
    TransactionExecutionTestPeer::FinishObjectReply(txm, reply);
    REQUIRE(TransactionExecutionTestPeer::Forward(txm) == TxmStatus::Finished);
    RequireImageReleased(result);
    REQUIRE(received.recover_cmd_image() == std::string(kLargeImageSize, 'r'));
    REQUIRE(log_entry.cmd_str_list_.front() == received.recover_cmd_image());
}

TEST_CASE("An active remote-result reader prevents terminal image release",
          "[transaction-image]")
{
    TransactionExecution txm(nullptr, nullptr, nullptr);
    auto &result = SeedImage(txm);
    TxResult<RecordStatus> reply(nullptr, nullptr);
    TransactionExecutionTestPeer::FinishObjectReply(txm, reply);

    txm.AcquireSharedForwardLatch();
    REQUIRE(TransactionExecutionTestPeer::Forward(txm) ==
            TxmStatus::ForwardFailed);
    REQUIRE(result.recover_cmd_image_.size() == kLargeImageSize);
    REQUIRE(reply.Status() == TxResultStatus::Unknown);
    txm.ReleaseSharedForwardLatch();

    REQUIRE(TransactionExecutionTestPeer::Forward(txm) == TxmStatus::Finished);
    RequireImageReleased(result);
}
}  // namespace txservice
