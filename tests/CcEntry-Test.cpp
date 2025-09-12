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

#include "cc/cc_entry.h"
#include "cc/cc_request.h"
#include "cc/template_cc_map.h"
#include "tx_key.h"     // CompositeKey
#include "tx_record.h"  // CompositeRecord

namespace txservice
{

TEST_CASE("CcEntry Init", "[cc-entry]")
{
    CcEntry<CompositeKey<int>, CompositeRecord<int>, true, true> entry;

    REQUIRE(entry.CommitTs() == 0);
    REQUIRE(entry.PayloadStatus() == RecordStatus::Unknown);
    REQUIRE(entry.ArchiveRecordsCount() == 0);
}

TEST_CASE("CcEntry ArchiveBeforeUpdate", "[cc-entry]")
{
    CcEntry<CompositeKey<int>, CompositeRecord<int>, true, true> entry;

    entry.payload_.cur_payload_ = std::make_unique<CompositeRecord<int>>(1);
    entry.SetCommitTsPayloadStatus(1U, RecordStatus::Unknown);

    entry.ArchiveBeforeUpdate();
    REQUIRE(entry.ArchiveRecordsCount() == 0);

    entry.payload_.cur_payload_ = std::make_unique<CompositeRecord<int>>(2);
    entry.SetCommitTsPayloadStatus(2U, RecordStatus::Normal);
    entry.ArchiveBeforeUpdate();

    entry.payload_.cur_payload_ = std::make_unique<CompositeRecord<int>>(3);
    entry.SetCommitTsPayloadStatus(3U, RecordStatus::Normal);
    entry.ArchiveBeforeUpdate();
    REQUIRE(entry.ArchiveRecordsCount() == 2);

    REQUIRE(entry.payload_.archives_->front().commit_ts_ == 3);
    REQUIRE(std::get<0>(entry.payload_.archives_->front().payload_->Tuple()) ==
            3);
    REQUIRE(entry.payload_.archives_->front().payload_status_ ==
            RecordStatus::Normal);

    REQUIRE(entry.payload_.archives_->front().commit_ts_ == 3);
    REQUIRE(std::get<0>(entry.payload_.archives_->front().payload_->Tuple()) ==
            3);
    REQUIRE(entry.payload_.archives_->front().payload_status_ ==
            RecordStatus::Normal);
}

TEST_CASE("CcEntry AddArchiveRecords", "[cc-entry]")
{
    CcEntry<CompositeKey<int>, CompositeRecord<int>, true, true> entry;

    // [6,5,3]->...=>[6,5,3]
    std::vector<VersionTxRecord> records;  // desc order

    std::vector<int> nums{6, 5, 3};
    for (auto n : nums)
    {
        auto &ref = records.emplace_back();
        ref.commit_ts_ = n;
        ref.record_ = std::make_unique<CompositeRecord<int>>(n);
        ref.record_status_ = RecordStatus::Normal;
    }

    entry.AddArchiveRecords(records);
    REQUIRE(entry.ArchiveRecordsCount() == nums.size());

    auto entry_it = entry.payload_.archives_->cbegin();
    for (size_t i = 0; i < nums.size(); i++)
    {
        assert(entry_it != entry.payload_.archives_->cend());
        REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
        REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
        REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
        entry_it++;
    }

    // [8]->...=>[8,6,5,3]
    {
        records.clear();
        auto &ref = records.emplace_back();
        ref.commit_ts_ = 8U;
        ref.record_ = std::make_unique<CompositeRecord<int>>(8);
        ref.record_status_ = RecordStatus::Normal;

        nums = {8, 6, 5, 3};
        entry.AddArchiveRecords(records);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        auto entry_it = entry.payload_.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.payload_.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
        }
    }

    // [2]->...=>[8,6,5,3,2]
    {
        records.clear();
        auto &ref = records.emplace_back();
        ref.commit_ts_ = 2U;
        ref.record_ = std::make_unique<CompositeRecord<int>>(2);
        ref.record_status_ = RecordStatus::Normal;

        nums = {8, 6, 5, 3, 2};
        entry.AddArchiveRecords(records);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        auto entry_it = entry.payload_.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.payload_.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
        }
    }

    // [3,2,1]->...=>[8,6,5,3,2,1]
    {
        records.clear();
        std::vector<int> add_nums{3, 2, 1};
        for (auto n : add_nums)
        {
            auto &ref = records.emplace_back();
            ref.commit_ts_ = n;
            ref.record_ = std::make_unique<CompositeRecord<int>>(n);
            ref.record_status_ = RecordStatus::Normal;
        }

        nums = {8, 6, 5, 3, 2, 1};
        entry.AddArchiveRecords(records);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        auto entry_it = entry.payload_.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.payload_.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
        }
    }

    // Abnormal test case: [5,3,1,0]->...=>[8,6,5,3,1,0,2,1]
    {
        records.clear();
        std::vector<int> add_nums{5, 3, 1, 0};
        for (auto n : add_nums)
        {
            auto &ref = records.emplace_back();
            ref.commit_ts_ = n;
            ref.record_ = std::make_unique<CompositeRecord<int>>(n);
            ref.record_status_ = RecordStatus::Normal;
        }

        nums = {8, 6, 5, 3, 1, 0, 2, 1};
        entry.AddArchiveRecords(records);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        auto entry_it = entry.payload_.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.payload_.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
        }
    }
}

TEST_CASE("CcEntry KickOutArchiveRecords", "[cc-entry]")
{
    CcEntry<CompositeKey<int>, CompositeRecord<int>, true, true> entry;
    entry.SetCommitTsPayloadStatus(12U, RecordStatus::Deleted);

    // [10,9,8,6,3,2]
    std::vector<VersionTxRecord> records;  // desc order
    std::vector<int> nums{10, 9, 8, 6, 3, 2};
    for (auto n : nums)
    {
        auto &ref = records.emplace_back();
        ref.commit_ts_ = n;
        ref.record_ = std::make_unique<CompositeRecord<int>>(n);
        ref.record_status_ = RecordStatus::Normal;
    }
    entry.AddArchiveRecords(records);
    REQUIRE(entry.ArchiveRecordsCount() == nums.size());

    // (oldest_active_tx_ts: 1)->... => [10,9,8,6,3,2]
    {
        uint64_t oldest_active_tx_ts = 1;
        nums = {10, 9, 8, 6, 3, 2};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        auto entry_it = entry.payload_.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.payload_.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
        }
    }

    // (oldest_active_tx_ts: 2)->... => [10,9,8,6,3,2]
    {
        uint64_t oldest_active_tx_ts = 2;
        nums = {10, 9, 8, 6, 3, 2};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        auto entry_it = entry.payload_.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.payload_.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
        }
    }

    // (oldest_active_tx_ts: 7)->... => [10,9,8,6]
    {
        uint64_t oldest_active_tx_ts = 7;
        nums = {10, 9, 8, 6};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        auto entry_it = entry.payload_.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.payload_.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
        }
    }

    // (oldest_active_tx_ts: 8)->... => [10,9,8]
    {
        uint64_t oldest_active_tx_ts = 8;
        nums = {10, 9, 8};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        auto entry_it = entry.payload_.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.payload_.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
        }
    }

    // (oldest_active_tx_ts: 5)->... => [10,9,8]
    {
        uint64_t oldest_active_tx_ts = 5;
        nums = {10, 9, 8};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        auto entry_it = entry.payload_.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.payload_.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
        }
    }

    // (oldest_active_tx_ts: 15)->... => []
    {
        uint64_t oldest_active_tx_ts = 15;
        nums = {};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());
    }
}

TEST_CASE("CcEntry MvccGet", "[cc-entry]")
{
    CcEntry<CompositeKey<int>, CompositeRecord<int>, true, true> entry;
    //== CcEntry has not been filled

    // (read_ts: 5)->... => Unknown
    {
        uint64_t ts = 5;
        VersionResultRecord<CompositeRecord<int>> rec;

        entry.MvccGet(ts, rec);
        REQUIRE(rec.payload_status_ == RecordStatus::Unknown);
    }

    entry.SetCommitTsPayloadStatus(12U, RecordStatus::Deleted);
    entry.payload_.cur_payload_ = std::make_unique<CompositeRecord<int>>(12);
    //== CcEntry has been filled, but has no historical version.

    // (read_ts: 5, ckpt_ts:0)->... => VersionUnknown
    {
        uint64_t ts = 5;
        VersionResultRecord<CompositeRecord<int>> rec;

        entry.MvccGet(ts, rec);
        REQUIRE(rec.payload_status_ == RecordStatus::VersionUnknown);
    }

    // (read_ts: 5, ckpt_ts=3, ckpt_ts<=read_ts)->... => BaseVersionMiss
    entry.SetCkptTs(3);
    {
        uint64_t ts = 5;
        VersionResultRecord<CompositeRecord<int>> rec;

        entry.MvccGet(ts, rec);
        REQUIRE(rec.payload_status_ == RecordStatus::BaseVersionMiss);
    }

    // (read_ts: 5, ckpt_ts=8, ckpt_ts>read_ts)->... =>ArchiveVesionMiss
    entry.SetCkptTs(8);
    {
        uint64_t ts = 5;
        VersionResultRecord<CompositeRecord<int>> rec;

        entry.MvccGet(ts, rec);
        REQUIRE(rec.payload_status_ == RecordStatus::ArchiveVersionMiss);
    }

    // (read_ts: 15)->... => 12 (latest version)
    {
        uint64_t ts = 15;
        uint64_t target = 12;
        VersionResultRecord<CompositeRecord<int>> rec;

        entry.MvccGet(ts, rec);
        REQUIRE(rec.commit_ts_ == static_cast<uint64_t>(target));
        REQUIRE(rec.payload_ptr_ == nullptr);
        REQUIRE(rec.payload_status_ == RecordStatus::Deleted);
    }

    //== Filled historical versions: [10,9,8,6,3,2]
    std::vector<VersionTxRecord> records;  // desc order
    std::vector<int> nums{10, 9, 8, 6, 3, 2};
    for (auto n : nums)
    {
        auto &ref = records.emplace_back();
        ref.commit_ts_ = n;
        ref.record_ = std::make_unique<CompositeRecord<int>>(n);
        ref.record_status_ = RecordStatus::Normal;
    }
    entry.AddArchiveRecords(records);
    REQUIRE(entry.ArchiveRecordsCount() == nums.size());

    // (read_ts: 1, ckpt_ts=8)->... => ArchiveVersionMiss
    {
        uint64_t ts = 1;
        uint64_t target = 1;
        VersionResultRecord<CompositeRecord<int>> rec;

        entry.MvccGet(ts, rec);
        REQUIRE(rec.commit_ts_ == static_cast<uint64_t>(target));
        REQUIRE(rec.payload_status_ == RecordStatus::ArchiveVersionMiss);
    }

    // (read_ts: 2)->... => 2
    {
        uint64_t ts = 2;
        uint64_t target = 2;
        VersionResultRecord<CompositeRecord<int>> rec;

        entry.MvccGet(ts, rec);
        REQUIRE(rec.commit_ts_ == static_cast<uint64_t>(target));
        REQUIRE(std::get<0>(rec.payload_ptr_->Tuple()) ==
                static_cast<int>(target));
        REQUIRE(rec.payload_status_ == RecordStatus::Normal);
    }

    // (read_ts: 7)->... => 6
    {
        uint64_t ts = 7;
        uint64_t target = 6;
        VersionResultRecord<CompositeRecord<int>> rec;

        entry.MvccGet(ts, rec);
        REQUIRE(rec.commit_ts_ == static_cast<uint64_t>(target));
        REQUIRE(std::get<0>(rec.payload_ptr_->Tuple()) ==
                static_cast<int>(target));
        REQUIRE(rec.payload_status_ == RecordStatus::Normal);
    }

    // (read_ts: 15)->... => 12 (latest version)
    {
        uint64_t ts = 15;
        uint64_t target = 12;
        VersionResultRecord<CompositeRecord<int>> rec;

        entry.MvccGet(ts, rec);
        REQUIRE(rec.commit_ts_ == static_cast<uint64_t>(target));
        REQUIRE(rec.payload_ptr_ == nullptr);
        REQUIRE(rec.payload_status_ == RecordStatus::Deleted);
    }
}

}  // namespace txservice

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    int ret = Catch::Session().run(argc, argv);
    return ret;
}
