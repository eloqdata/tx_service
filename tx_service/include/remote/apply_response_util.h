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

namespace txservice
{
struct ObjectCommandResult;
struct TxCommand;

namespace remote
{
class ApplyResponse;

// Owner side: serialize an executed command's success result into the
// ApplyResponse the owner ships back to the coordinator. Fills the result
// fields plus the owner-only facts the coordinator needs to log the WAL
// (ttl_reset / ttl / ttl_expired / recover_cmd_image). executed_cmd is the
// owner's in-place executed command (remote_input_.cmd_), non-null on the
// success path.
void FillApplyResponse(ApplyResponse &resp,
                       const ObjectCommandResult &apply_result,
                       TxCommand *executed_cmd);

// Coordinator side: consume the owner's ApplyResponse into the local command
// result the transaction post-processing reads. Mirrors FillApplyResponse.
void BackfillObjectCommandResult(ObjectCommandResult &obj_cmd_result,
                                 const ApplyResponse &apply_res);
}  // namespace remote
}  // namespace txservice
