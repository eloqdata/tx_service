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
#include "range_record.h"

#include <mutex>

#include "cc_entry.h"
#include "cc_req_misc.h"
#include "local_cc_shards.h"
#include "type.h"

namespace txservice
{

TableRangeEntry::~TableRangeEntry()
{
}

void TableRangeEntry::FetchRangeSlices(const TableName &range_tbl_name,
                                       CcRequestBase *requester,
                                       NodeGroupId ng_id,
                                       int64_t ng_term,
                                       CcShard *cc_shard)
{
    std::unique_lock<std::shared_mutex> lk(mux_);
    if (RangeSlices() != nullptr)
    {
        cc_shard->Enqueue(requester);
        return;
    }
    if (!fetch_range_slices_req_)
    {
        fetch_range_slices_req_ = std::make_unique<FetchRangeSlicesReq>(
            range_tbl_name, this, ng_id, ng_term);
    }
    fetch_range_slices_req_->AddRequester(requester, cc_shard);
    if (fetch_range_slices_req_->RequesterCount() == 1)
    {
        lk.unlock();
        Sharder::Instance().GetLocalCcShards()->store_hd_->FetchRangeSlices(
            fetch_range_slices_req_.get());
    }
}

void RangeRecord::CopyForReadResult(const RangeRecord &other)
{
    // Release RangeInfo ownership
    if (is_info_owner_)
    {
        range_info_uptr_.reset();
        is_info_owner_ = false;
    }

    assert(!other.is_info_owner_);
    range_info_ = other.range_info_;
    is_info_owner_ = other.is_info_owner_;

    // Free own unique ptr.
    if (is_read_result_ && new_range_owner_bucket_)
    {
        new_range_owner_bucket_.reset();
    }
    else if (!is_read_result_ && new_range_owner_rec_)
    {
        new_range_owner_rec_.reset();
    }
    assert(!other.is_read_result_);
    is_read_result_ = true;

    range_owner_bucket_ =
        static_cast<const CcEntry<RangeBucketKey, RangeBucketRecord> *>(
            other.range_owner_rec_)
            ->payload_->GetBucketInfo();

    if (other.new_range_owner_rec_)
    {
        new_range_owner_bucket_ =
            std::make_unique<std::vector<const BucketInfo *>>();
        for (auto &entry : *other.new_range_owner_rec_)
        {
            new_range_owner_bucket_->push_back(
                static_cast<const CcEntry<RangeBucketKey, RangeBucketRecord> *>(
                    entry)
                    ->payload_->GetBucketInfo());
        }
    }
    else
    {
        new_range_owner_bucket_ = nullptr;
    }
}
};  // namespace txservice