/**
 *    Copyright (C) 2026 EloqData Inc.
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
 */
#include <catch2/catch_all.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cc/cc_req_pool.h"
#include "remote/remote_cc_request.h"

namespace txservice::remote
{
namespace
{
std::unique_ptr<CcMessage> MakeUpload(uint64_t txn,
                                      const std::vector<std::string> &commands)
{
    auto message = std::make_unique<CcMessage>();
    message->set_type(CcMessage::UploadTxCommandsRequest);
    message->set_tx_number(txn);
    message->set_tx_term(3);
    auto *upload = message->mutable_upload_cmds_req();
    upload->set_node_group_id(7);
    upload->set_object_version(txn);
    upload->set_commit_ts(txn + 1);
    upload->set_has_overwrite(false);
    auto *addr = upload->mutable_cce_addr();
    addr->set_cce_lock_ptr(8);
    addr->set_term(5);
    addr->set_core_id(2);
    for (const auto &command : commands)
    {
        upload->add_cmd_list(command);
    }
    return message;
}
}  // namespace

TEST_CASE("Remote upload Reset replaces the previous command list",
          "[remote-upload]")
{
    RemoteUploadTxCommandsCc request;
    const std::vector<std::string> first{std::string(1024 * 1024, 'a'),
                                         std::string("first\0tail", 10)};
    const std::vector<std::string> second{"second-only"};

    request.Reset(MakeUpload(100, first));
    REQUIRE(*request.CommandList() == first);

    // Exercise the actual Reset without relying on Free to sanitize the old
    // request. These are the bytes ObjectCcMap consumes for the second key.
    request.Reset(MakeUpload(200, second));
    REQUIRE(request.CommandList()->size() == second.size());
    REQUIRE(*request.CommandList() == second);
    REQUIRE(request.Txn() == 200);
    REQUIRE(request.ObjectVersion() == 200);
    REQUIRE(request.CommitTs() == 201);
    REQUIRE(request.CceAddr()->NodeGroupId() == 7);
    REQUIRE(request.CceAddr()->CoreId() == 2);

    request.Reset(MakeUpload(300, {}));
    REQUIRE(request.CommandList()->empty());
}

TEST_CASE("Remote upload releases command images at completion",
          "[remote-upload]")
{
    const bool abort_request = GENERATE(false, true);
    CcRequestPool<RemoteUploadTxCommandsCc> pool(1);
    auto *request = pool.NextRequest();
    REQUIRE(request != nullptr);
    const std::vector<std::string> commands{std::string(1024 * 1024, 'x'),
                                            "last-command"};
    request->Reset(MakeUpload(100, commands));

    // Replace only network delivery. The real result completion and abort
    // paths must invoke the callback before Free publishes the idle slot.
    bool callback_called = false;
    request->Result()->post_lambda_ =
        [&](CcHandlerResult<PostProcessResult> *result)
    {
        callback_called = true;
        REQUIRE(request->InUse());
        REQUIRE(*request->CommandList() == commands);
        REQUIRE(pool.NextRequest() == nullptr);
        REQUIRE(result->ErrorCode() ==
                (abort_request ? CcErrorCode::REQUESTED_NODE_NOT_LEADER
                               : CcErrorCode::NO_ERROR));
    };

    REQUIRE(*request->CommandList() == commands);
    REQUIRE(pool.NextRequest() == nullptr);
    if (abort_request)
    {
        // The production abort path invokes the virtual Free itself.
        request->AbortCcRequest(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
    }
    else
    {
        // On success the shard processor frees a request after Execute has
        // completed the result and returned true.
        REQUIRE(request->Result()->SetFinished());
        request->Free();
    }

    REQUIRE(callback_called);
    REQUIRE_FALSE(request->InUse());
    REQUIRE(request->CommandList()->empty());
    auto *reused = pool.NextRequest();
    REQUIRE(reused == request);
    const std::vector<std::string> next_commands{"next-request-only"};
    reused->Reset(MakeUpload(200, next_commands));
    REQUIRE(*reused->CommandList() == next_commands);
    reused->Free();
    REQUIRE(reused->CommandList()->empty());
}
}  // namespace txservice::remote
