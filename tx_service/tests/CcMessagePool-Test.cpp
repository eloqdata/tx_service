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
 */
#include <cstddef>
#include <memory>
#include <string>
#include <utility>

/** Let Catch provide main(). */
#include <catch2/catch_all.hpp>

#include "remote/cc_message_pool.h"

namespace txservice::remote
{
TEST_CASE("CcMessagePool clears payloads before recycling", "[cc-message-pool]")
{
    constexpr size_t payload_size = 64 * 1024;

    CcMessagePool pool;
    std::unique_ptr<CcMessage> msg = pool.Acquire();
    CcMessage *original = msg.get();
    msg->set_type(CcMessage::KeyObjectStandbyForwardRequest);
    msg->set_tx_number(42);
    KeyObjectStandbyForwardRequest *request =
        msg->mutable_key_obj_standby_forward_req();
    request->set_key(std::string(payload_size, 'k'));
    request->add_cmd_list(std::string(payload_size, 'c'));
    const size_t populated_space = msg->SpaceUsedLong();

    pool.Recycle(std::move(msg));
    std::unique_ptr<CcMessage> recycled = pool.Acquire();

    REQUIRE(recycled.get() == original);
    REQUIRE(recycled->content_case() == CcMessage::CONTENT_NOT_SET);
    REQUIRE(recycled->type() == CcMessage::AcquireRequest);
    REQUIRE(recycled->tx_number() == 0);
    REQUIRE(recycled->SpaceUsedLong() < populated_space / 2);
}
}  // namespace txservice::remote
