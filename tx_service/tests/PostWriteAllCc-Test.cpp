/**
 *    Copyright (C) 2025 EloqData Inc.
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
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program. If not, see
 *    <http://www.gnu.org/licenses/>.
 */
#include <catch2/catch_all.hpp>
#include <cstddef>
#include <memory>
#include <string>
#include <tuple>

#include "cc/cc_req_pool.h"
#include "cc/cc_request.h"
#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
namespace
{
constexpr size_t kPayloadSize = 64 * 1024;

class CountedKey : public CompositeKey<std::string>
{
public:
    CountedKey() = default;

    explicit CountedKey(size_t *destroyed)
        : CompositeKey<std::string>(std::string(kPayloadSize, 'k')),
          destroyed_(destroyed)
    {
    }

    ~CountedKey()
    {
        if (destroyed_ != nullptr)
        {
            ++*destroyed_;
        }
    }

    static const TxKeyInterface *TxKeyImpl()
    {
        static const TxKeyInterface key_interface{CountedKey{}};
        return &key_interface;
    }

private:
    size_t *destroyed_{nullptr};
};

class CountedRecord : public CompositeRecord<std::string>
{
public:
    explicit CountedRecord(size_t *destroyed)
        : CompositeRecord<std::string>(std::string(kPayloadSize, 'v')),
          destroyed_(destroyed)
    {
    }

    ~CountedRecord() override
    {
        ++*destroyed_;
    }

private:
    size_t *destroyed_;
};

void SetBorrowed(PostWriteAllCc &request,
                 const TableName &table,
                 const TxKey &key,
                 TxRecord &record,
                 CcHandlerResult<PostProcessResult> *result = nullptr)
{
    request.Reset(&table,
                  &key,
                  0,
                  1,
                  1,
                  &record,
                  OperationType::Update,
                  result,
                  PostWriteType::Commit,
                  1,
                  false);
}
}  // namespace

TEST_CASE("PostWriteAllCc releases decoded owners before pool reuse",
          "[post-write-all-cc]")
{
    CcRequestPool<PostWriteAllCc> pool(1);
    size_t destroyed_keys = 0;
    size_t destroyed_records = 0;
    PostWriteAllCc *request = pool.NextRequest();
    REQUIRE(request != nullptr);
    request->SetDecodedKey(
        TxKey(std::make_unique<CountedKey>(&destroyed_keys)));
    request->SetDecodedPayload(
        std::make_unique<CountedRecord>(&destroyed_records));
    REQUIRE(request->Key() != nullptr);
    REQUIRE(request->DecodedPayload() != nullptr);

    CcRequestBase *base = request;
    base->Free();

    REQUIRE(destroyed_keys == 1);
    REQUIRE(destroyed_records == 1);
    REQUIRE_FALSE(request->InUse());
    REQUIRE(request->Key() == nullptr);
    REQUIRE(request->Payload() == nullptr);
    REQUIRE(request->DecodedPayload() == nullptr);

    PostWriteAllCc *reused = pool.NextRequest();
    REQUIRE(reused == request);
    reused->Free();
    REQUIRE(destroyed_keys == 1);
    REQUIRE(destroyed_records == 1);
}

TEST_CASE("PostWriteAllCc recycling preserves borrowed input owners",
          "[post-write-all-cc]")
{
    size_t destroyed_keys = 0;
    size_t destroyed_records = 0;
    {
        CountedKey key(&destroyed_keys);
        CountedRecord record(&destroyed_records);
        TxKey borrowed_key(&key);
        const TableName table(std::string("post_write_all"),
                              TableType::Primary,
                              TableEngine::EloqKv);
        PostWriteAllCc request;
        request.Use();
        SetBorrowed(request, table, borrowed_key, record);
        REQUIRE(request.Payload() == &record);
        request.Free();

        REQUIRE_FALSE(request.InUse());
        REQUIRE(destroyed_keys == 0);
        REQUIRE(destroyed_records == 0);
        REQUIRE(std::get<0>(key.Tuple()).size() == kPayloadSize);
        REQUIRE(std::get<0>(record.Tuple()).size() == kPayloadSize);
        REQUIRE(request.Key() == nullptr);
        REQUIRE(request.Payload() == nullptr);
    }
    REQUIRE(destroyed_keys == 1);
    REQUIRE(destroyed_records == 1);
}

TEST_CASE("PostWriteAllCc abort releases decoded owners", "[post-write-all-cc]")
{
    size_t destroyed_keys = 0;
    size_t destroyed_records = 0;
    CompositeKey<int> original_key(1);
    TxKey borrowed_key(&original_key);
    CompositeRecord<int> original_record(1);
    const TableName table(
        std::string("post_write_all"), TableType::Primary, TableEngine::EloqKv);
    CcHandlerResult<PostProcessResult> result(nullptr);
    PostWriteAllCc request;
    request.Use();
    SetBorrowed(request, table, borrowed_key, original_record, &result);
    request.SetDecodedKey(TxKey(std::make_unique<CountedKey>(&destroyed_keys)));
    request.SetDecodedPayload(
        std::make_unique<CountedRecord>(&destroyed_records));

    request.AbortCcRequest(CcErrorCode::NG_TERM_CHANGED);

    REQUIRE_FALSE(request.InUse());
    REQUIRE(destroyed_keys == 1);
    REQUIRE(destroyed_records == 1);
    REQUIRE(request.DecodedPayload() == nullptr);
    REQUIRE(request.Payload() == nullptr);
}
}  // namespace txservice
