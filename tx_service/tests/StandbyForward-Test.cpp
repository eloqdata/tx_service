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
#include <memory>
#include <string>
#include <string_view>
#include <utility>

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

}  // namespace txservice
