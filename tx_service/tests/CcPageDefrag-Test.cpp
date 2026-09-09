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
#include <cstddef>
#include <memory>
#include <utility>

#include "cc/cc_entry.h"
#include "cc/template_cc_map.h"
#include "mimalloc.h"
#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
namespace
{
// Force the real iterator's key-relocation branch without depending on the
// allocator's current page occupancy, and count every temporary key lifetime.
class DefragTestKey : public CompositeKey<int>
{
public:
    DefragTestKey()
    {
        ++live_count_;
    }

    explicit DefragTestKey(int value) : CompositeKey<int>(std::move(value))
    {
        ++live_count_;
    }

    DefragTestKey(const DefragTestKey &other) : CompositeKey<int>(other)
    {
        ++live_count_;
    }

    DefragTestKey(DefragTestKey &&other) noexcept
        : CompositeKey<int>(std::move(other))
    {
        ++live_count_;
    }

    DefragTestKey &operator=(const DefragTestKey &) = default;
    DefragTestKey &operator=(DefragTestKey &&) noexcept = default;

    ~DefragTestKey()
    {
        --live_count_;
    }

    bool NeedsDefrag(mi_heap_t *)
    {
        return true;
    }

    static const TxKeyInterface *TxKeyImpl()
    {
        static const TxKeyInterface interface{DefragTestKey{}};
        return &interface;
    }

    static inline size_t live_count_{0};
};

using TestRecord = CompositeRecord<int>;
template <bool Versioned>
class TestMap
    : public TemplateCcMap<DefragTestKey, TestRecord, Versioned, false>
{
    using BaseMap = TemplateCcMap<DefragTestKey, TestRecord, Versioned, false>;

public:
    using BaseMap::DEFRAGED;
    using BaseMap::Iterator;
};
}  // namespace

TEMPLATE_TEST_CASE_SIG("Page key defragmentation releases temporary owners",
                       "[cc-page][defrag]",
                       ((bool Versioned), Versioned),
                       true,
                       false)
{
    using TestPage = CcPage<DefragTestKey, TestRecord, Versioned, false>;
    using TestEntry = CcEntry<DefragTestKey, TestRecord, Versioned, false>;
    REQUIRE(DefragTestKey::live_count_ == 0);
    {
        TestPage page(nullptr, nullptr, nullptr);
        for (int value = 1; value <= 3; ++value)
        {
            page.keys_.emplace_back(value);
            auto entry = std::make_unique<TestEntry>();
            entry->payload_.cur_payload_ = std::make_unique<TestRecord>(value);
            entry->SetCommitTsPayloadStatus(2, RecordStatus::Normal);
            entry->SetCkptTs(2);
            page.entries_.emplace_back(std::move(entry));
        }

        // Repeatedly relocate first, middle and last vector elements. Each
        // completed relocation must leave exactly the three page-owned keys.
        for (size_t round = 0; round < 8; ++round)
        {
            for (size_t index = 0; index < page.keys_.size(); ++index)
            {
                typename TestMap<Versioned>::Iterator it(&page, index, nullptr);
                REQUIRE(it.DefragCurrentIfNecessary(mi_heap_get_default()) ==
                        TestMap<Versioned>::DEFRAGED);
                REQUIRE(DefragTestKey::live_count_ == page.keys_.size());
                REQUIRE(page.keys_.size() == 3);
                REQUIRE(page.entries_.size() == 3);
                for (size_t pos = 0; pos < page.keys_.size(); ++pos)
                {
                    REQUIRE(std::get<0>(page.keys_[pos].Tuple()) ==
                            static_cast<int>(pos + 1));
                    REQUIRE(std::get<0>(page.entries_[pos]
                                            ->payload_.cur_payload_->Tuple()) ==
                            static_cast<int>(pos + 1));
                }
            }
        }
    }
    REQUIRE(DefragTestKey::live_count_ == 0);
}
}  // namespace txservice
