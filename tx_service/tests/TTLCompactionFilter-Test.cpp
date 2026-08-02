/**
 *    Copyright (C) 2026 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero
 *    General Public License or GNU General Public License for more details.
 */

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <cstring>
#include <limits>
#include <string>

#include "eloq_data_store_service/rocksdb_data_store_common.h"

namespace
{

std::string MakeValue(uint64_t encoded_ts, uint64_t expiration_ts)
{
    std::string value(sizeof(uint64_t) * 2, '\0');
    std::memcpy(value.data(), &encoded_ts, sizeof(encoded_ts));
    std::memcpy(value.data() + sizeof(uint64_t),
                &expiration_ts,
                sizeof(expiration_ts));
    return value;
}

bool ShouldFilter(const std::string &value)
{
    EloqDS::TTLCompactionFilter filter;
    return filter.Filter(0,
                         rocksdb::Slice("key"),
                         rocksdb::Slice(value),
                         nullptr,
                         nullptr);
}

}  // namespace

TEST_CASE("TTL compaction filter reads the TTL marker from the encoded "
          "timestamp",
          "[rocksdb][ttl]")
{
    SECTION("expired TTL value is removed")
    {
        const std::string value = MakeValue(EloqDS::MSB | 42, 1);
        REQUIRE(ShouldFilter(value));
    }

    SECTION("unexpired TTL value is retained")
    {
        const std::string value = MakeValue(
            EloqDS::MSB | 42, std::numeric_limits<uint64_t>::max());
        REQUIRE_FALSE(ShouldFilter(value));
    }

    SECTION("value without TTL is retained")
    {
        const std::string value(sizeof(uint64_t), '\0');
        REQUIRE_FALSE(ShouldFilter(value));
    }
}
