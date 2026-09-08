/**
 *    Copyright (C) 2026 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License, version 2, as published by the Free
 *    Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *    GNU Affero General Public License and GNU General Public License for
 *    more details. If not, see <http://www.gnu.org/licenses/>.
 */
#include <catch2/catch_all.hpp>
#include <cstddef>
#include <unordered_set>
#include <vector>

#include "cc/cc_req_pool.h"

namespace txservice
{
namespace
{
class PoolTestRequest : public CcRequestBase
{
public:
    bool Execute(CcShard &) override
    {
        return true;
    }
};
}  // namespace

TEST_CASE("CcRequestPool respects zero, small and odd capacity limits",
          "[cc-request-pool]")
{
    const size_t limit = GENERATE(0, 1, 3, 8, 9, 13, 17);
    CAPTURE(limit);
    CcRequestPool<PoolTestRequest> pool(limit);
    std::unordered_set<PoolTestRequest *> requests;

    REQUIRE(pool.IsAllFree());
    for (size_t i = 0; i < limit; ++i)
    {
        PoolTestRequest *request = pool.NextRequest();
        REQUIRE(request != nullptr);
        REQUIRE(request->InUse());
        REQUIRE(requests.insert(request).second);
    }

    REQUIRE(pool.NextRequest() == nullptr);
    REQUIRE(pool.NextRequest() == nullptr);
    for (PoolTestRequest *request : requests)
    {
        request->Free();
    }
    REQUIRE(pool.IsAllFree());
}

TEST_CASE("CcRequestPool reuses freed requests after reaching its limit",
          "[cc-request-pool]")
{
    const size_t limit = GENERATE(1, 3, 9, 13);
    CAPTURE(limit);
    CcRequestPool<PoolTestRequest> pool(limit);
    std::vector<PoolTestRequest *> requests;
    for (size_t i = 0; i < limit; ++i)
    {
        PoolTestRequest *request = pool.NextRequest();
        REQUIRE(request != nullptr);
        requests.push_back(request);
    }
    REQUIRE(pool.NextRequest() == nullptr);

    std::unordered_set<PoolTestRequest *> freed;
    for (size_t i = 0; i < limit; i += 2)
    {
        requests[i]->Free();
        freed.insert(requests[i]);
    }
    while (!freed.empty())
    {
        PoolTestRequest *request = pool.NextRequest();
        REQUIRE(request != nullptr);
        REQUIRE(freed.erase(request) == 1);
        REQUIRE(request->InUse());
    }
    REQUIRE(pool.NextRequest() == nullptr);

    for (PoolTestRequest *request : requests)
    {
        request->Free();
    }
    REQUIRE(pool.IsAllFree());
}

TEST_CASE("CcRequestPool default grows and preserves request addresses",
          "[cc-request-pool]")
{
    CcRequestPool<PoolTestRequest> pool;
    std::unordered_set<PoolTestRequest *> requests;
    for (size_t i = 0; i < 64; ++i)
    {
        PoolTestRequest *request = pool.NextRequest();
        REQUIRE(request != nullptr);
        REQUIRE(requests.insert(request).second);
    }

    for (PoolTestRequest *request : requests)
    {
        REQUIRE(request->InUse());
        request->Free();
    }
    REQUIRE(pool.IsAllFree());
}
}  // namespace txservice
