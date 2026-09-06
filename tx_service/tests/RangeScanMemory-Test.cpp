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

#include <gflags/gflags.h>
#include <mimalloc.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include "cc/cc_entry.h"
#include "cc/cc_req_misc.h"
#include "cc/cc_request.h"
#include "cc/cc_shard.h"
#include "cc/local_cc_shards.h"
#include "cc/template_cc_map.h"
#include "harness/test_node.h"
#include "sharder.h"
#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
namespace
{
void Check(bool value, const char *message)
{
    if (!value)
    {
        std::cerr << "FAIL: " << message << std::endl;
        std::exit(1);
    }
}

/**
 * ReleaseDataSyncScanHeapCc leaves size zero while retaining capacity. Both
 * a partial/full batch and a normal consumed batch must be reusable afterward:
 * ExportForCkpt writes existing FlushRecord objects with operator[].
 */
void ResetReleasedBatch(bool heap_full)
{
    const TableName table(std::string_view("scan_memory_test"),
                          TableType::Primary,
                          TableEngine::EloqDoc);
    TxKey start = CompositeKey<int>::NegativeInfinity()->CloneTxKey();
    TxKey end = CompositeKey<int>::PositiveInfinity()->CloneTxKey();
    constexpr size_t kBatchSize = 64;
    RangePartitionDataSyncScanCc scan(
        table, 20, 0, 1, kBatchSize, 1, &start, &end, 0, true, true);
    auto payload = std::make_shared<CompositeRecord<int>>(7);
    std::weak_ptr<TxRecord> weak = payload;
    scan.DataSyncVec()[0].SetVersionedPayload(std::move(payload));
    scan.accumulated_scan_cnt_ = 1;
    scan.scan_heap_is_full_ = heap_full;
    scan.PausePos().first = TxKey(std::make_unique<CompositeKey<int>>(9));
    // Isolate the Reset postcondition from scheduling: this is the state left
    // after the source-shard release request has destroyed all elements.
    scan.DataSyncVec().clear();
    Check(weak.expired(), "consumed payload reference was not released");
    scan.Reset();
    Check(scan.DataSyncVec().size() == kBatchSize,
          "Reset did not reconstruct released scan slots without Full");
    Check(scan.accumulated_scan_cnt_ == 0, "Reset kept the old count");
    Check(scan.scan_heap_is_full_ == 0, "Reset kept the Full flag");
    Check(!scan.IsDrained(), "Reset incorrectly marked the range drained");
    Check(*scan.PausePos().first.GetKey<CompositeKey<int>>() ==
              CompositeKey<int>(9),
          "Reset changed the resume key");

    CcEntry<CompositeKey<int>, CompositeRecord<int>, true, true> entry;
    entry.payload_.cur_payload_ = std::make_unique<CompositeRecord<int>>(11);
    entry.SetCommitTsPayloadStatus(10, RecordStatus::Normal);
    entry.SetCkptTs(10);
    uint64_t flush_size = 0;
    const size_t exported = entry.ExportForCkpt(CompositeKey<int>(9),
                                                scan.DataSyncVec(),
                                                scan.ArchiveVec(),
                                                scan.MoveBaseIdxVec(),
                                                20,
                                                1,
                                                true,
                                                scan.accumulated_scan_cnt_,
                                                true,
                                                true,
                                                false,
                                                flush_size);
    Check(exported == 1 && scan.accumulated_scan_cnt_ == 1,
          "the next batch did not export its resume record");
    Check(scan.DataSyncVec()[0].Payload() != nullptr,
          "the next batch lost its payload");
}

/**
 * Exercise real scan-heap accounting and incremental release on a running CC
 * shard. This is the export/release boundary, not a full index-build fixture.
 */
void ReleaseOnSourceShard()
{
    test::TestNode node;
    LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
    // Use a nonzero source so release must honor the supplied shard index.
    constexpr uint16_t kSourceShard = 1;
    constexpr size_t kBatchSize = LocalCcShards::DATA_SYNC_SCAN_BATCH_SIZE;
    using Key = CompositeKey<std::string>;
    using Record = CompositeRecord<int>;
    using Entry = CcEntry<Key, Record, true, true>;
    using Map = TemplateCcMap<Key, Record, true, true>;
    const TableName table(std::string_view("scan_memory_test"),
                          TableType::Primary,
                          TableEngine::EloqDoc);
    TxKey start = Key::NegativeInfinity()->CloneTxKey();
    TxKey end = Key::PositiveInfinity()->CloneTxKey();
    RangePartitionDataSyncScanCc scan(
        table, 20, 0, 1, kBatchSize, 1, &start, &end, 0, true, true);
    std::weak_ptr<TxRecord> pk_payload;
    int64_t allocated_before = 0;
    int64_t allocated_after = 0;

    auto run_on_source = [&](std::function<bool(CcShard &)> task)
    {
        WaitableCc request(std::move(task));
        shards->EnqueueToCcShard(kSourceShard, &request);
        request.Wait();
        Check(!request.IsError(), "source-shard request failed");
    };
    auto export_entry = [&](Map &map, Entry &entry, const Key &key)
    {
        return map.ExportForCkpt(&entry,
                                 key,
                                 scan.DataSyncVec(),
                                 scan.ArchiveVec(),
                                 scan.MoveBaseIdxVec(),
                                 20,
                                 1,
                                 true,
                                 scan.accumulated_scan_cnt_,
                                 true,
                                 true,
                                 false,
                                 scan.accumulated_flush_data_size_);
    };

    run_on_source(
        [&](CcShard &shard)
        {
            Map map(&shard, 0, table, 1);
            Entry entry;
            entry.payload_.cur_payload_ = std::make_unique<Record>(7);
            entry.SetCommitTsPayloadStatus(10, RecordStatus::Normal);
            entry.SetCkptTs(10);
            pk_payload = entry.payload_.VersionedCurrentPayload();
            // Large cloned keys reach the real heap limit with a small number
            // of exports; the PK payload itself is still shared from the source
            // entry.
            Key key(std::string(512 * 1024, 'k'));
            // The old libstdc++ ABI uses copy-on-write strings. Taking a
            // mutable element reference makes this source unshareable, so each
            // scan key clone really allocates its bytes on the scan heap in
            // either ABI.
            std::get<0>(key.Tuple())[0] = 'k';
            bool full = false;
            while (scan.accumulated_scan_cnt_ < kBatchSize)
            {
                const auto result = export_entry(map, entry, key);
                if (result.second)
                {
                    full = true;
                    break;
                }
                Check(result.first == 1, "pressure fixture failed to export");
            }
            std::cout << "Pressure fixture: rows=" << scan.accumulated_scan_cnt_
                      << " full=" << full << " limit="
                      << shard.GetShardDataSyncScanHeap()->MemoryLimit()
                      << std::endl;
            Check(full && scan.accumulated_scan_cnt_ > 0,
                  "pressure fixture did not fill the scan heap");
            scan.scan_heap_is_full_ = 1;

            // Exercise both vectors across more than one release request round.
            CcShardHeap *heap = shard.GetShardDataSyncScanHeap();
            mi_heap_t *previous = heap->SetAsDefaultHeap();
#if defined(WITH_JEMALLOC)
            auto previous_arena = heap->SetAsDefaultArena();
#endif
            const Key archive_key(std::string("archive"));
            for (size_t i = 0;
                 i < 2 * ReleaseDataSyncScanHeapCc::VEC_ERASE_BATCH_SIZE + 1;
                 ++i)
            {
                scan.ArchiveVec().emplace_back();
                // Small archive keys are enough to exercise incremental
                // deletion.
                scan.ArchiveVec().back().CloneOrCopyKey(TxKey(&archive_key));
            }
            Check(heap->Full(&allocated_before),
                  "scan heap unexpectedly shrank");
            mi_heap_set_default(previous);
#if defined(WITH_JEMALLOC)
            JemallocArenaSwitcher::SwitchToArena(previous_arena);
#endif
            return true;
        });

    // The previous Reset-only pattern retains the consumed batch. The next
    // export stops at the heap check before it can overwrite any old slot.
    scan.Reset();
    Check(!pk_payload.expired(), "Reset unexpectedly released the PK batch");
    run_on_source(
        [&](CcShard &shard)
        {
            Map map(&shard, 0, table, 1);
            Entry entry;
            const auto result =
                export_entry(map, entry, Key(std::string("resume")));
            Check(result.first == 0 && result.second,
                  "Reset-only did not reproduce the zero-progress heap gate");
            return true;
        });

    shards->ReleaseScanResultsAndWait(kSourceShard, scan);
    Check(scan.DataSyncVec().empty() && scan.ArchiveVec().empty(),
          "incremental source-shard release left scan records behind");
    Check(pk_payload.expired(), "source-shard release retained the PK payload");
    scan.Reset();
    Check(scan.DataSyncVec().size() == kBatchSize,
          "released zero-row batch cannot be reused");

    run_on_source(
        [&](CcShard &shard)
        {
            CcShardHeap *heap = shard.GetShardDataSyncScanHeap();
            mi_heap_t *previous = heap->SetAsDefaultHeap();
#if defined(WITH_JEMALLOC)
            auto previous_arena = heap->SetAsDefaultArena();
#endif
            Check(!heap->Full(&allocated_after),
                  "released scan heap is still full");
            mi_heap_set_default(previous);
#if defined(WITH_JEMALLOC)
            JemallocArenaSwitcher::SwitchToArena(previous_arena);
#endif
            Map map(&shard, 0, table, 1);
            Entry entry;
            entry.payload_.cur_payload_ = std::make_unique<Record>(11);
            entry.SetCommitTsPayloadStatus(10, RecordStatus::Normal);
            entry.SetCkptTs(10);
            const auto result =
                export_entry(map, entry, Key(std::string("resume")));
            Check(result.first == 1 && !result.second,
                  "scan did not resume after source-shard release");
            return true;
        });
    // DataSync transfers this reference to its flush task before releasing the
    // scan buffer. Releasing the scan must not destroy a transferred payload.
    TxKey flush_key = scan.DataSyncVec()[0].Key().Clone();
    auto flush_payload = scan.DataSyncVec()[0].ReleaseVersionedPayload();
    std::weak_ptr<TxRecord> transferred_payload = flush_payload;
    shards->ReleaseScanResultsAndWait(kSourceShard, scan);
    Check(flush_payload != nullptr,
          "release lost the transferred flush payload");
    Check(std::get<0>(static_cast<Record &>(*flush_payload).Tuple()) == 11 &&
              *flush_key.GetKey<Key>() == Key(std::string("resume")),
          "release invalidated the flush task's key or payload");
    flush_payload.reset();
    Check(transferred_payload.expired(),
          "scan kept another reference after flush ownership was released");
    Check(allocated_after < allocated_before,
          "source-shard release did not update heap accounting");
    std::cout << "PASS: source-shard release resumed export; scan heap bytes "
              << allocated_before << " -> " << allocated_after << std::endl;
}
}  // namespace
}  // namespace txservice

int main(int argc, char **argv)
{
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
    txservice::ResetReleasedBatch(true);
    txservice::ResetReleasedBatch(false);
    std::cout << "PASS: released full and non-full scan batches resume safely"
              << std::endl;
    txservice::ReleaseOnSourceShard();
}
