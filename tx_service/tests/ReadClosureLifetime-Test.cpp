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
 *
 */
#include <catch2/catch_all.hpp>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "data_store_service_client_closure.h"
#include "eloq_data_store_service/data_store_service_config.h"
#include "eloq_data_store_service/internal_request.h"
#include "eloq_data_store_service/object_pool.h"

namespace
{
constexpr size_t kLargeValueSize = 1024 * 1024;

enum class Consumption
{
    Take,
    Copy,
    Discard
};

struct Completion
{
    Consumption consumption{Consumption::Discard};
    size_t calls{0};
    size_t observed_size{0};
    uint64_t observed_ttl{0};
    int error_code{0};
    bool in_use_during_callback{false};
    std::string value;
};

void OnRead(void *data,
            google::protobuf::Closure *closure,
            EloqDS::DataStoreServiceClient &,
            const EloqDS::remote::CommonResult &result)
{
    auto &completion = *static_cast<Completion *>(data);
    auto &read = *static_cast<EloqDS::ReadClosure *>(closure);
    ++completion.calls;
    completion.observed_size = read.Value().size();
    completion.observed_ttl = read.Ttl();
    completion.error_code = result.error_code();
    completion.in_use_during_callback = read.InUse();
    if (completion.consumption == Consumption::Take)
    {
        completion.value = read.TakeValue();
    }
    else if (completion.consumption == Consumption::Copy)
    {
        completion.value.assign(read.Value());
    }
}

class ReadFixture
{
public:
    // No DSS, RPC or database is started. The real client only supplies the
    // callback argument; its constructor's idle worker is joined on
    // destruction.
    ReadFixture() : client_(false, catalogs_, cluster_, false)
    {
    }

    void Reset(EloqDS::ReadClosure &read, Completion &completion)
    {
        read.Reset(&client_, "table", 0, 0, "key", false, &completion, OnRead);
        read.LocalTsRef() = 42;
        read.LocalTtlRef() = 0;
    }

    void CompleteLocal(EloqDS::ReadClosure &read,
                       EloqDS::remote::DataStoreError error,
                       uint64_t ttl = 0)
    {
        // This is the same wrapper/closure completion chain used by the local
        // storage backend, including Run's final PoolableGuard.
        EloqDS::ReadLocalRequest request;
        request.Reset(nullptr,
                      "table",
                      0,
                      0,
                      "key",
                      false,
                      &read.LocalValueRef(),
                      &read.LocalTsRef(),
                      &read.LocalTtlRef(),
                      &read.LocalResultRef(),
                      &read);
        request.SetRecordTtl(ttl);
        request.SetFinish(error);
    }

private:
    txservice::CatalogFactory *catalogs_[3]{nullptr, nullptr, nullptr};
    EloqDS::DataStoreServiceClusterManager cluster_;
    EloqDS::DataStoreServiceClient client_;
};

void RequireReleased(EloqDS::ReadClosure &read)
{
    REQUIRE_FALSE(read.InUse());
    REQUIRE(read.LocalValueRef().empty());
    REQUIRE(read.LocalValueRef().capacity() == std::string{}.capacity());
}
}  // namespace

TEST_CASE("ReadClosure Clear releases even a shortened local buffer",
          "[read-closure]")
{
    const size_t final_size = GENERATE(kLargeValueSize, size_t{1}, size_t{0});
    EloqDS::ReadClosure read;
    read.LocalValueRef().assign(kLargeValueSize, 'x');
    read.LocalValueRef().resize(final_size);
    REQUIRE(read.LocalValueRef().capacity() >= kLargeValueSize);
    read.Clear();
    RequireReleased(read);
}

TEST_CASE(
    "Local read completion preserves the consumer and releases the closure",
    "[read-closure]")
{
    const auto consumption =
        GENERATE(Consumption::Take, Consumption::Copy, Consumption::Discard);
    // An expired value may still be returned by storage before compaction. The
    // consumer decides whether to discard it, while the closure owns cleanup.
    const uint64_t ttl = GENERATE(uint64_t{0}, uint64_t{1});
    ReadFixture fixture;
    EloqDS::ReadClosure read;
    Completion completion;
    completion.consumption = consumption;
    read.Use();
    fixture.Reset(read, completion);
    read.LocalValueRef().assign(kLargeValueSize, 'x');
    fixture.CompleteLocal(read, EloqDS::remote::NO_ERROR, ttl);

    REQUIRE(completion.calls == 1);
    REQUIRE(completion.in_use_during_callback);
    REQUIRE(completion.observed_size == kLargeValueSize);
    REQUIRE(completion.observed_ttl == ttl);
    REQUIRE(completion.error_code == EloqDS::remote::NO_ERROR);
    RequireReleased(read);
    REQUIRE(completion.value == (consumption == Consumption::Discard
                                     ? std::string{}
                                     : std::string(kLargeValueSize, 'x')));
}

TEST_CASE("Local read errors release an unconsumed result at completion",
          "[read-closure]")
{
    const auto error = GENERATE(EloqDS::remote::KEY_NOT_FOUND,
                                EloqDS::remote::READ_FAILED,
                                EloqDS::remote::DB_NOT_OPEN);
    ReadFixture fixture;
    EloqDS::ReadClosure read;
    Completion completion;
    read.Use();
    fixture.Reset(read, completion);
    read.LocalValueRef().assign(kLargeValueSize, 'x');
    // Missing/error paths can clear the output's length before completing.
    read.LocalValueRef().clear();
    fixture.CompleteLocal(read, error);

    REQUIRE(completion.calls == 1);
    REQUIRE(completion.in_use_during_callback);
    REQUIRE(completion.error_code == error);
    RequireReleased(read);
}

TEST_CASE(
    "A released retry guard retains the local buffer until final completion",
    "[read-closure]")
{
    ReadFixture fixture;
    EloqDS::ReadClosure read;
    Completion completion;
    completion.consumption = Consumption::Take;
    read.Use();
    fixture.Reset(read, completion);
    read.LocalValueRef().assign(kLargeValueSize, 'x');
    {
        EloqDS::PoolableGuard attempt_guard(&read);
        REQUIRE(attempt_guard.Release() == &read);
    }
    REQUIRE(read.InUse());
    REQUIRE(read.LocalValueRef().size() == kLargeValueSize);
    REQUIRE(completion.calls == 0);

    // Exercise a local-to-remote continuation without sending an RPC. The
    // production retry dispatcher is outside this ownership-boundary test.
    read.PrepareRequest(false);
    read.ReadResponse()->set_value("next attempt");
    read.ReadResponse()->mutable_result()->set_error_code(
        EloqDS::remote::NO_ERROR);
    read.Run();
    REQUIRE(completion.calls == 1);
    REQUIRE(completion.in_use_during_callback);
    REQUIRE(completion.value == "next attempt");
    RequireReleased(read);
}

TEST_CASE(
    "Pooled local read closures release payloads before the next checkout",
    "[read-closure]")
{
    ReadFixture fixture;
    EloqDS::ObjectPool<EloqDS::ReadClosure> pool;
    std::vector<EloqDS::ReadClosure *> first_round;
    bool released = true;
    bool reused = true;
    size_t callbacks = 0;
    for (size_t round = 0; round < 3; ++round)
    {
        for (size_t slot = 0; slot < 8; ++slot)
        {
            auto *read = pool.NextObject();
            // Keep assertions after completion so a failing assertion cannot
            // leave an occupied slot blocking the pool's destructor.
            EloqDS::PoolableGuard cleanup_on_failure(read);
            if (round == 0)
            {
                first_round.push_back(read);
            }
            else
            {
                reused = reused && read == first_round[slot];
                released = released && read->LocalValueRef().empty() &&
                           read->LocalValueRef().capacity() ==
                               std::string{}.capacity();
            }
            Completion completion;
            fixture.Reset(*read, completion);
            read->LocalValueRef().assign(kLargeValueSize, 'x');
            fixture.CompleteLocal(*read, EloqDS::remote::NO_ERROR, 1);
            cleanup_on_failure.Release();
            callbacks += completion.calls;
            released =
                released && !read->InUse() && read->LocalValueRef().empty() &&
                read->LocalValueRef().capacity() == std::string{}.capacity();
        }
    }
    REQUIRE(callbacks == 24);
    REQUIRE(reused);
    REQUIRE(released);
    REQUIRE(pool.IsAllFree());
}
