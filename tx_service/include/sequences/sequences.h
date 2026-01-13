/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the following license:
 *    1. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License V2
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
#pragma once

#include "tx_key.h"
#include "tx_record.h"
#include "tx_service.h"
#include "type.h"

namespace txservice
{

// A batch of consecutive ids prefetched at once.
struct SequenceBatch
{
    explicit SequenceBatch(std::string name) : seq_name_(name) {};
    SequenceBatch(std::string name, uint64_t key_schema_version)
        : seq_name_(name), key_schema_version_(key_schema_version) {};

    // lock when insert a record and need to apply an id
    bthread::Mutex mutex_id_;
    // Current sequence name
    std::string seq_name_;
    // Current id, for every apply, it will return current id, then add rec_step
    // and save into curr_id. (curr_id_ is the first unassigned value)
    int64_t curr_id_ = 0;
    // The end for current range, if curr_id = range1_end, it
    // means all ids in this range have been used and it need use ids in
    // range2_start or apply new id rang.
    int64_t range_end_ = -1;
    // The size of id range that acquired from server every time.
    int range_step_ = -1;
    // The step between two neighbor record.
    int rec_step_ = -1;
    // The version for the related table key schema
    uint64_t key_schema_version_;
    /**
     * @brief True, if the sequence is being advanced by one of the clients.
     *
     */
    bool seq_being_advanced_{false};
    /**
     * @brief In the thread-per-collection mode, a MySQL thread sleeps on this
     * condition variable, if it intends to advance the sequence but discovers
     * that another SQL thread is doing the job.
     *
     */
    bthread::ConditionVariable seq_cv_;
};

enum struct SequenceType
{
    AutoIncrementColumn = 0,
    RangePartitionId = 1
};

class Sequences
{
public:
    inline static const std::string_view table_name_sv_{
        txservice::sequence_table_name_sv};
    inline static const TableName table_name_{txservice::sequence_table_name};
    inline static const std::string_view kv_table_name_sv_{
        txservice::sequence_kv_table_name_sv};
    inline static const uint64_t seq_schema_version_{100U};

    static void InitSequence(TxService *tx_service,
                             store::DataStoreHandler *storage_hd)
    {
        instance_ = std::make_unique<Sequences>(tx_service, storage_hd);
    }
    static bool Initialized()
    {
        return instance_ != nullptr;
    }

    static void Destory()
    {
        instance_ = nullptr;
    }

    // When drop a table with auto increment id and range partition, call this
    // function to remove its sequence from here.
    static bool DeleteSequence(const TableName &table,
                               SequenceType seq_type,
                               bool only_clean_cache = false)
    {
        std::string seq_name = GenSeqName(table, seq_type);
        return DeleteSequenceInternal(seq_name, only_clean_cache);
    }

    // Apply a series of ids from a sequence with name='seq_name', If
    // increment>0, the step between two neighbor ids will be increment, else
    // the step will be rec_step_ in sequence. If it has not enough ids in this
    // range, reserved_vals will be surplus ids, else 'desired_vals' ids.
    // Return the first assigned id if success, else return -1.
    static int64_t ApplyIdOfAutoIncrColumn(
        const TableName &table,
        int64_t increment,
        int64_t desired_vals,
        int64_t &reserved_vals,
        uint64_t key_schema_version,
        std::pair<const std::function<void()> *, const std::function<void()> *>
            coro_functors,
        const std::function<void()> *long_resume_func,
        int16_t thd_group_id);

    static bool ApplyIdOfTableRangePartition(const TableName &table,
                                             int64_t desired_vals,
                                             int64_t &first_reserved_id,
                                             int64_t &reserved_vals,
                                             uint64_t key_schema_version);

    static TxKey GenKey(const std::string &seq_name);
    static std::unique_ptr<TxRecord> GenRecord();

    static int UpdateAutoIncrement(
        std::string content,
        std::string dbName,
        std::pair<const std::function<void()> *, const std::function<void()> *>
            coro_functors,
        int16_t group_id);

    static int32_t InitialRangePartitionIdOf(const TableName &table);
    static bool InitIdOfTableRangePartition(const TableName &table,
                                            int32_t last_range_id);

    /**
     * @param start_val The start id of sequence.
     * @param node_step_val The size of id range that acquired from server
     * every time.
     * @param rec_step_val The step between two neighbor record.
     * @param curr_val The first unassigned value.
     */
    static std::pair<txservice::TxKey, txservice::TxRecord::Uptr>
    GetSequenceKeyAndInitRecord(const txservice::TableName &table_name,
                                SequenceType seq_type,
                                int64_t start_val = 1,
                                int32_t node_step_val = 256,
                                int32_t rec_step_val = 1,
                                int64_t curr_val = 1)
    {
        std::string seq_name = GenSeqName(table_name, seq_type);

        TxKey seq_tx_key = GenKey(seq_name);
        std::unique_ptr<TxRecord> seq_tx_rec = GenRecord();
        std::string encoded_rec =
            EncodeSeqRecord(start_val, node_step_val, rec_step_val, curr_val);
        seq_tx_rec->SetEncodedBlob(
            reinterpret_cast<const unsigned char *>(encoded_rec.data()),
            encoded_rec.size());
        // unpack info is unused in sequence table

        return std::pair<txservice::TxKey, txservice::TxRecord::Uptr>(
            std::move(seq_tx_key), std::move(seq_tx_rec));
    }
    static bool InitIdOfAutoIncrementColumn(const TableName &table);

    Sequences(TxService *tx_service, store::DataStoreHandler *storage_hd);
    ~Sequences() = default;  // { delete tableSchema_.release(); }

private:
    static std::string GenSeqName(const TableName &tablename,
                                  SequenceType seq_type)
    {
        // TODO(lzx): generate with key_schema_version.
        if (seq_type == SequenceType::AutoIncrementColumn)
        {
            return "autoincr_" + tablename.String();
        }
        else
        {
            return "rangeid_" + tablename.String();
        }
    }

    static int ApplySequenceBatch(
        SequenceBatch *rid,
        int64_t desired_vals,
        std::pair<const std::function<void()> *, const std::function<void()> *>
            coro_functors,
        int16_t thd_group_id);

    static bool DeleteSequenceInternal(const std::string &seq_name,
                                       bool only_clean_cache);

    static std::string EncodeSeqRecord(int64_t start_val = 1,
                                       int32_t node_step_val = 256,
                                       int32_t rec_step_val = 1,
                                       int64_t curr_val = 1);

    static void DecodeSeqRecord(std::string_view encoded_data,
                                int64_t &start_val,
                                int32_t &node_step_val,
                                int32_t &rec_step_val,
                                int64_t &curr_val);

private:
    static std::unique_ptr<Sequences> instance_;

    std::unordered_map<std::string, std::unique_ptr<SequenceBatch>> seq_id_map_;
    bthread::Mutex mutex_;
    TxService *tx_service_{nullptr};
    store::DataStoreHandler *storage_hd_{nullptr};
};

}  // namespace txservice
