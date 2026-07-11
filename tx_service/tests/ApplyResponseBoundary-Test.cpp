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
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "cc/cc_request.h"               // ObjectCommandResult + prerequisites
#include "cc_request.pb.h"               // remote::ApplyResponse
#include "read_write_entry.h"            // CmdSetEntry
#include "remote/apply_response_util.h"  // FillApplyResponse/BackfillObject...
#include "tx_command.h"                  // TxCommand
#include "tx_operation_result.h"         // ComputeReportedTtl

namespace txservice
{
namespace
{
// Minimal local command. Serialize() emits its stored payload so the test can
// assert on exact log/wire bytes. RecoverTTLObjectCommand() returns an owned
// child command whose payload is the full-object snapshot image the owner would
// have produced after resetting a live TTL. GetResult() is null: the success
// path then serializes no cmd_result, keeping the wire bytes deterministic.
struct FakeCommand : public TxCommand
{
    explicit FakeCommand(std::string payload) : payload_(std::move(payload))
    {
    }

    std::unique_ptr<TxCommand> Clone() override
    {
        return std::make_unique<FakeCommand>(payload_);
    }

    bool IsReadOnly() const override
    {
        return false;
    }

    bool IsOverwrite() const override
    {
        return is_overwrite_;
    }

    void Serialize(std::string &str) const override
    {
        str.append(payload_);
    }

    void Deserialize(std::string_view cmd_img) override
    {
        payload_.assign(cmd_img);
    }

    TxCommand *RecoverTTLObjectCommand() override
    {
        return recover_.get();
    }

    // Remaining pure virtuals are never reached by the helpers under test;
    // provide trivial definitions so the type is concrete.
    std::unique_ptr<TxRecord> CreateObject(
        const std::string *image) const override
    {
        return nullptr;
    }

    std::unique_ptr<TxCommandResult> CreateCommandResult() const override
    {
        return nullptr;
    }

    bool ProceedOnNonExistentObject() const override
    {
        return true;
    }

    bool ProceedOnExistentObject() const override
    {
        return true;
    }

    ExecResult ExecuteOn(const TxObject &object) override
    {
        return ExecResult::Write;
    }

    TxCommandResult *GetResult() override
    {
        return nullptr;
    }

    bool IsVolatile() override
    {
        return false;
    }

    void SetVolatile() override
    {
    }

    std::string payload_;
    bool is_overwrite_{false};
    // Snapshot command returned by RecoverTTLObjectCommand().
    std::unique_ptr<FakeCommand> recover_;
};

// Fills the non-ttl result fields the owner always reports, so that the wire
// message resembles a real ApplyResponse and the resend-equivalence check is
// exercised on a fully populated message.
void SeedOwnerResult(ObjectCommandResult &r)
{
    r.Reset();
    r.commit_ts_ = 100;
    r.last_vali_ts_ = 200;
    r.rec_status_ = RecordStatus::Normal;
    r.lock_acquired_ = LockType::WriteLock;
    r.object_modified_ = true;
}

// Owner fill -> serialize -> parse: models the wire round-trip between the
// owner node and the coordinator.
remote::ApplyResponse RoundTrip(const ObjectCommandResult &owner_result,
                                FakeCommand &cmd)
{
    remote::ApplyResponse resp;
    resp.set_is_ack(false);
    remote::FillApplyResponse(resp, owner_result, &cmd);

    std::string wire;
    REQUIRE(resp.SerializeToString(&wire));
    remote::ApplyResponse parsed;
    REQUIRE(parsed.ParseFromString(wire));
    return parsed;
}
}  // namespace

// Case 1. Owner reset a live TTL and serialized a full-object snapshot ("X").
// The coordinator must recover both ttl_reset_ and the snapshot image so it can
// log an overwrite record instead of relying on a base object it never had.
TEST_CASE("ApplyResponse propagates ttl_reset snapshot", "[apply-boundary]")
{
    ObjectCommandResult owner_result;
    SeedOwnerResult(owner_result);
    owner_result.ttl_reset_ = true;
    owner_result.ttl_ = UINT64_MAX;

    FakeCommand cmd("payload");
    cmd.recover_ = std::make_unique<FakeCommand>("X");

    // Resend equivalence: two independent fills of the same owner result must
    // serialize to identical bytes (a resent response is byte-for-byte equal).
    remote::ApplyResponse resp_a;
    resp_a.set_is_ack(false);
    remote::FillApplyResponse(resp_a, owner_result, &cmd);
    remote::ApplyResponse resp_b;
    resp_b.set_is_ack(false);
    remote::FillApplyResponse(resp_b, owner_result, &cmd);
    std::string wire_a;
    std::string wire_b;
    REQUIRE(resp_a.SerializeToString(&wire_a));
    REQUIRE(resp_b.SerializeToString(&wire_b));
    REQUIRE(wire_a == wire_b);

    remote::ApplyResponse parsed = RoundTrip(owner_result, cmd);
    ObjectCommandResult coord;
    coord.Reset();
    remote::BackfillObjectCommandResult(coord, parsed);

    REQUIRE(coord.ttl_reset_ == true);
    REQUIRE(coord.recover_cmd_image_ == "X");
}

// Case 2. A finite, still-live TTL must survive the round-trip; recovery uses
// it as the object's validity horizon. A dropped ttl (left at UINT64_MAX) would
// let recovery replay writes onto an object that has since expired.
TEST_CASE("ApplyResponse propagates finite ttl", "[apply-boundary]")
{
    constexpr uint64_t kFiniteTtl = 5'000'000;

    ObjectCommandResult owner_result;
    SeedOwnerResult(owner_result);
    owner_result.ttl_reset_ = false;
    owner_result.ttl_expired_ = false;
    owner_result.ttl_ = kFiniteTtl;

    FakeCommand cmd("payload");
    remote::ApplyResponse parsed = RoundTrip(owner_result, cmd);

    ObjectCommandResult coord;
    coord.Reset();
    remote::BackfillObjectCommandResult(coord, parsed);
    REQUIRE(coord.ttl_ == kFiniteTtl);

    // The back-filled ttl flows into the WAL entry unchanged for an incremental
    // (non-overwrite) command.
    CmdSetEntry entry(0, 0, 0, std::string("k"), false);
    FakeCommand incr_cmd("INCR");
    entry.AddCommand(&incr_cmd, coord.ttl_);
    REQUIRE(entry.ttl_ == kFiniteTtl);
}

// Case 3. The command consumed an expired object and recreated a fresh one. The
// reported ttl must be UINT64_MAX (fix #4): reporting the expired pre-command
// ttl would make recovery discard the acknowledged recreation. ttl_expired_
// must also reach the coordinator so it prepends a retire record.
TEST_CASE("ApplyResponse handles expired recreation", "[apply-boundary]")
{
    constexpr uint64_t kPastTtl = 1000;

    // Owner rule: expired recreation reports the post-command ttl (no horizon).
    REQUIRE(ComputeReportedTtl(/*ttl_reset*/ false,
                               /*ttl_expired*/ true,
                               /*is_overwrite*/ false,
                               kPastTtl) == UINT64_MAX);

    ObjectCommandResult owner_result;
    SeedOwnerResult(owner_result);
    owner_result.ttl_expired_ = true;
    owner_result.ttl_ = ComputeReportedTtl(false, true, false, kPastTtl);

    FakeCommand cmd("payload");
    remote::ApplyResponse parsed = RoundTrip(owner_result, cmd);

    ObjectCommandResult coord;
    coord.Reset();
    remote::BackfillObjectCommandResult(coord, parsed);
    REQUIRE(coord.ttl_expired_ == true);
    REQUIRE(coord.ttl_ == UINT64_MAX);
}

// Case 4. An old owner that predates the ttl field sends proto default 0. The
// coordinator must treat 0 as "no horizon" (UINT64_MAX), not as an already-past
// ttl that would discard every replayed command.
TEST_CASE("ApplyResponse old-owner ttl fallback", "[apply-boundary]")
{
    remote::ApplyResponse old_resp;
    old_resp.set_is_ack(false);
    // Deliberately leave ttl unset (proto default 0).

    ObjectCommandResult coord;
    coord.Reset();
    remote::BackfillObjectCommandResult(coord, old_resp);
    REQUIRE(coord.ttl_ == UINT64_MAX);
}

// Case 5. The overwrite-image WAL API discards prior incremental commands and
// records a single overwrite image, then keeps appending later commands after
// it (new API; green from phase 1).
TEST_CASE("CmdSetEntry overwrite image invariant", "[apply-boundary]")
{
    CmdSetEntry entry(0, 0, 0, std::string("k"), false);

    FakeCommand incr_cmd("INCR");
    entry.AddCommand(&incr_cmd, 12345);
    REQUIRE(entry.cmd_str_list_.size() == 1);

    entry.AddOverwriteCommandImage("X", UINT64_MAX);
    REQUIRE(entry.cmd_str_list_.size() == 1);
    REQUIRE(entry.cmd_str_list_[0] == "X");
    REQUIRE(entry.ignore_previous_version_ == true);
    REQUIRE(entry.ttl_ == UINT64_MAX);

    FakeCommand incr_cmd2("INCR2");
    entry.AddCommand(&incr_cmd2, UINT64_MAX);
    REQUIRE(entry.cmd_str_list_.size() == 2);
    REQUIRE(entry.cmd_str_list_[0] == "X");
    REQUIRE(entry.cmd_str_list_[1] == "INCR2");
}

}  // namespace txservice
