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
#include <memory>
#include <string>
#include <string_view>
#include <utility>

// Let Catch provide main():
#include <catch2/catch_all.hpp>

#include "cc/cc_request.h"  // ApplyCc
#include "standby.h"        // StandbyForwardEntry
#include "tx_command.h"     // TxCommand

namespace txservice
{
namespace
{
// Minimal fake command whose Serialize() emits its stored payload. This lets
// the test distinguish the coordinator's pre-ExecuteOn image (the string handed
// to a remote ApplyCc via cmd_str_) from the owner-side executed command (the
// TxCommand set on the ApplyCc after execution).
struct FakeObjectCommand : public TxCommand
{
    explicit FakeObjectCommand(std::string payload)
        : payload_(std::move(payload))
    {
    }

    std::unique_ptr<TxCommand> Clone() override
    {
        return std::make_unique<FakeObjectCommand>(payload_);
    }

    bool IsReadOnly() const override
    {
        return false;
    }

    bool IsOverwrite() const override
    {
        return false;
    }

    // Serialize the executed command: append the payload verbatim.
    void Serialize(std::string &str) const override
    {
        str.append(payload_);
    }

    void Deserialize(std::string_view cmd_img) override
    {
        payload_.assign(cmd_img);
    }

    // The remaining pure virtuals are never reached by AddTxCommand; provide
    // trivial definitions so the type is concrete.
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
};

struct FakeOverwriteCommand : public FakeObjectCommand
{
    using FakeObjectCommand::FakeObjectCommand;

    bool IsOverwrite() const override
    {
        return true;
    }
};
}  // namespace

// Remote ApplyCc: the coordinator forwards a pre-ExecuteOn command image
// ("PRE"), but the owner node executes the command in place so that the
// executed command serializes to "POST". The standby forward entry must carry
// the executed command ("POST"), not the stale pre-image ("PRE"), otherwise the
// standby (which applies commands commit-only, never re-running ExecuteOn)
// diverges from the primary (eloqdata/eloqkv#509).
TEST_CASE("StandbyForward remote forwards executed command",
          "[standby-forward]")
{
    const std::string pre_image = "PRE";

    ApplyCc cc_req(/*is_local=*/false);
    cc_req.remote_input_.cmd_str_ = &pre_image;
    // Simulate owner-side execution replacing the command with the executed
    // one. SetCommand takes ownership; the ApplyCc destructor frees it.
    cc_req.SetCommand(new FakeObjectCommand("POST"));

    StandbyForwardEntry entry;
    entry.AddTxCommand(cc_req);

    REQUIRE(entry.Request().cmd_list_size() == 1);
    // Divergence assertion: pre-fix this is "PRE" (the forwarded pre-image).
    REQUIRE(entry.Request().cmd_list(0) == "POST");
    // IsOverwrite() is false, so has_overwrite must stay false.
    REQUIRE(entry.Request().has_overwrite() == false);
}

// Local ApplyCc guard: the local path already serialized the executed command;
// it must keep doing so before and after the fix.
TEST_CASE("StandbyForward local serializes executed command",
          "[standby-forward]")
{
    FakeObjectCommand fake("POST");

    ApplyCc cc_req(/*is_local=*/true);
    cc_req.local_input_.key_ = nullptr;
    cc_req.local_input_.cmd_ = &fake;

    StandbyForwardEntry entry;
    entry.AddTxCommand(cc_req);

    REQUIRE(entry.Request().cmd_list_size() == 1);
    REQUIRE(entry.Request().cmd_list(0) == "POST");
    REQUIRE(entry.Request().has_overwrite() == false);
}

TEST_CASE("Standby overwrite releases discarded command storage",
          "[standby-forward][memory]")
{
    constexpr size_t image_size = 1024 * 1024;
    constexpr size_t command_count = 8;
    FakeObjectCommand large_command(std::string(image_size, 'x'));
    ApplyCc cc_req(/*is_local=*/true);
    cc_req.local_input_.key_ = nullptr;
    cc_req.local_input_.cmd_ = &large_command;

    StandbyForwardEntry entry;
    for (size_t i = 0; i < command_count; ++i)
    {
        entry.AddTxCommand(cc_req);
    }
    REQUIRE(entry.Message().SpaceUsedLong() >= image_size * command_count);

    FakeOverwriteCommand overwrite("replacement");
    entry.AddOverWriteCommand(&overwrite);

    REQUIRE(entry.Request().cmd_list_size() == 1);
    REQUIRE(entry.Request().cmd_list(0) == "replacement");
    REQUIRE(entry.Request().has_overwrite());

    StandbyForwardEntry fresh_entry;
    fresh_entry.AddOverWriteCommand(&overwrite);
    // Compare owned protobuf storage, not just serialized bytes: Clear() made
    // the old implementation small on the wire while keeping all 8 MiB alive.
    REQUIRE(entry.Message().SpaceUsedLong() <=
            fresh_entry.Message().SpaceUsedLong() + 4096);

    // The same entry may accumulate more commands before another overwrite.
    entry.AddTxCommand(cc_req);
    entry.AddOverWriteCommand(&overwrite);
    REQUIRE(entry.Request().cmd_list_size() == 1);
    REQUIRE(entry.Message().SpaceUsedLong() <=
            fresh_entry.Message().SpaceUsedLong() + 4096);
}

TEST_CASE("Standby overwrite preserves routing and following commands",
          "[standby-forward]")
{
    StandbyForwardEntry entry;
    entry.SetSequenceId(17);
    auto &req = entry.Request();
    req.set_key("key");
    req.set_table_name("table");
    req.set_key_shard_code(42);
    req.set_primary_leader_term(3);
    req.set_forward_seq_grp(2);
    req.set_forward_seq_id(17);
    req.set_object_version(19);
    req.set_commit_ts(23);
    req.set_schema_version(29);
    req.set_tx_number(31);

    FakeObjectCommand preceding("discarded");
    ApplyCc cc_req(/*is_local=*/true);
    cc_req.local_input_.key_ = nullptr;
    cc_req.local_input_.cmd_ = &preceding;
    entry.AddTxCommand(cc_req);

    FakeOverwriteCommand overwrite("replacement");
    entry.AddOverWriteCommand(&overwrite);
    FakeObjectCommand following("following");
    cc_req.local_input_.cmd_ = &following;
    entry.AddTxCommand(cc_req);

    remote::CcMessage received;
    REQUIRE(received.ParseFromString(entry.Message().SerializeAsString()));
    const auto &received_req = received.key_obj_standby_forward_req();
    REQUIRE(received_req.cmd_list_size() == 2);
    REQUIRE(received_req.cmd_list(0) == "replacement");
    REQUIRE(received_req.cmd_list(1) == "following");
    REQUIRE(received_req.has_overwrite());
    REQUIRE_FALSE(received_req.out_of_sync());
    REQUIRE(entry.SequenceId() == 17);
    REQUIRE(received_req.key() == "key");
    REQUIRE(received_req.table_name() == "table");
    REQUIRE(received_req.key_shard_code() == 42);
    REQUIRE(received_req.primary_leader_term() == 3);
    REQUIRE(received_req.forward_seq_grp() == 2);
    REQUIRE(received_req.forward_seq_id() == 17);
    REQUIRE(received_req.object_version() == 19);
    REQUIRE(received_req.commit_ts() == 23);
    REQUIRE(received_req.schema_version() == 29);
    REQUIRE(received_req.tx_number() == 31);
}

}  // namespace txservice
