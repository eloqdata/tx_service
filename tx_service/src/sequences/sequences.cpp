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

#include "sequences/sequences.h"

#include "eloq_string_key_record.h"
#include "tx_util.h"

namespace txservice
{

std::unique_ptr<Sequences> Sequences::instance_;

Sequences::Sequences(TxService *tx_service, store::DataStoreHandler *storage_hd)
{
    tx_service_ = tx_service;
    storage_hd_ = storage_hd;
}

// When drop a table with auto increment id, call this function to remove its
// sequence from here.
bool Sequences::DeleteSequenceInternal(const std::string &seq_name,
                                       bool only_clean_cache)
{
    if (only_clean_cache)
    {
        if (instance_ != nullptr)
        {
            std::unique_lock<bthread::Mutex> lock(instance_->mutex_);
            instance_->seq_id_map_.erase(seq_name);
        }
        return true;
    }

    TransactionExecution *txm =
        NewTxInit(instance_->tx_service_,
                  txservice::IsolationLevel::RepeatableRead,
                  txservice::CcProtocol::Locking);

    if (txm == nullptr)
    {
        return false;
    }

    CatalogKey table_key(table_name_);
    TxKey tbl_tx_key(&table_key);
    CatalogRecord catalog_rec;
    ReadTxRequest read_catalog_req(&txservice::catalog_ccm_name,
                                   0,
                                   &tbl_tx_key,
                                   &catalog_rec,
                                   false,
                                   false,
                                   true,
                                   0,
                                   false,
                                   false,
                                   nullptr,
                                   nullptr,
                                   txm);

    bool exists = false;
    TxErrorCode err = TxReadCatalog(txm, read_catalog_req, exists);

    if (err != TxErrorCode::NO_ERROR || !exists)
    {
        txservice::AbortTx(txm);
        return false;
    }

    TxKey mkey = GenKey(seq_name);

    err = txm->TxUpsert(table_name_,
                        seq_schema_version_,
                        TxKey(std::move(mkey)),
                        nullptr,
                        OperationType::Delete);

    if (err != TxErrorCode::NO_ERROR)
    {
        LOG(ERROR) << "Failed to delete sequence item, name: " << seq_name;
        txservice::AbortTx(txm);
        return false;
    }

    auto [success, commit_err] = txservice::CommitTx(txm);
    if (success)
    {
        if (instance_ != nullptr)
        {
            std::unique_lock<bthread::Mutex> lock(instance_->mutex_);
            instance_->seq_id_map_.erase(seq_name);
        }
    }

    return success;
}

// Apply a series of ids from a sequence with name='seq_name', If increment>0,
// the step between two neighbor ids will be increment, else the step will be
// rec_step_ in sequence. If it has not enough ids in this range, reserved_vals
// will be surplus ids, else 'desired_vals' ids.
// Return the first assigned id if success, else return -1.
int64_t Sequences::ApplyIdOfAutoIncrColumn(
    const TableName &table,
    int64_t increment,
    int64_t desired_vals,
    int64_t &reserved_vals,
    uint64_t key_schema_version,
    std::pair<const std::function<void()> *, const std::function<void()> *>
        coro_functors,
    const std::function<void()> *long_resume_func,
    int16_t thd_group_id)
{
    assert(instance_ != nullptr);
    std::string seq_name = GenSeqName(table, SequenceType::AutoIncrementColumn);

    SequenceBatch *rid = nullptr;
    instance_->mutex_.lock();
    auto iter = instance_->seq_id_map_.find(seq_name);
    if (iter == instance_->seq_id_map_.end())
    {
        auto it = instance_->seq_id_map_.try_emplace(
            seq_name,
            std::make_unique<SequenceBatch>(seq_name, key_schema_version));
        rid = it.first->second.get();
    }
    else if (iter->second->key_schema_version_ != key_schema_version)
    {
        iter->second =
            std::make_unique<SequenceBatch>(seq_name, key_schema_version);
        rid = iter->second.get();
    }
    else
    {
        rid = iter->second.get();
    }
    instance_->mutex_.unlock();

    std::unique_lock<bthread::Mutex> seq_lk(rid->mutex_id_);
    while (rid->curr_id_ >= rid->range_end_)
    {
        if (rid->seq_being_advanced_)
        {
            if (thd_group_id < 0)
            {
                // The thread is in the thread-per-connection mode. Sleeps the
                // thread until the sequence has been advanced.
                rid->seq_cv_.wait(seq_lk);
            }
            else
            {
                // This thread is from a thread group and cannot be blocked.
                // Rather than sleeping, it releases the lock and yields to the
                // next command in the group.
                const std::function<void()> *yield_fp = coro_functors.first;
                // The resume functor first enqueues the current coroutine for
                // re-execution.
                (*long_resume_func)();
                seq_lk.unlock();
                // The yield functor yields the current thread to the next
                // coroutine in the group.
                (*yield_fp)();
                seq_lk.lock();
            }
        }
        else
        {
            rid->seq_being_advanced_ = true;
            seq_lk.unlock();

            int i = 0;
            for (; i < 3; i++)
            {
                int err = Sequences::ApplySequenceBatch(
                    rid, desired_vals, coro_functors, thd_group_id);
                if (err >= 0)
                {
                    break;
                }
            }

            seq_lk.lock();
            rid->seq_being_advanced_ = false;
            rid->seq_cv_.notify_all();

            if (i == 3)
            {
                reserved_vals = 0;
                return -1;
            }
        }
    }

    if (increment == -1)
    {
        increment = rid->rec_step_;
    }

    if (rid->curr_id_ + increment * (desired_vals - 1) < rid->range_end_)
    {
        reserved_vals = desired_vals;
    }
    else
    {
        reserved_vals = (rid->range_end_ - rid->curr_id_ - 1) / increment + 1;
    }

    int64_t val = rid->curr_id_;
    rid->curr_id_ += increment * reserved_vals;
    return val;
}

// This method used to generate TxKey.
TxKey Sequences::GenKey(const std::string &seq_name)
{
    assert(sequence_table_name.Engine() == TableEngine::InternalHash);
    return txservice::TxKey(std::make_unique<EloqStringKey>(
        reinterpret_cast<const char *>(seq_name.data()), seq_name.size()));
}

std::unique_ptr<TxRecord> Sequences::GenRecord()
{
    return EloqStringRecord::Create();
}

int Sequences::ApplySequenceBatch(
    SequenceBatch *rid,
    int64_t desired_vals,
    std::pair<const std::function<void()> *, const std::function<void()> *>
        coro_functors,
    int16_t thd_group_id)
{
    TransactionExecution *txm =
        NewTxInit(instance_->tx_service_,
                  txservice::IsolationLevel::RepeatableRead,
                  txservice::CcProtocol::Locking,
                  UINT32_MAX,
                  thd_group_id);

    if (txm == nullptr)
    {
        return -1;
    }

    CatalogKey table_key(table_name_);
    TxKey tbl_tx_key(&table_key);
    CatalogRecord catalog_rec;
    ReadTxRequest read_catalog_req(&txservice::catalog_ccm_name,
                                   0,
                                   &tbl_tx_key,
                                   &catalog_rec,
                                   false,
                                   false,
                                   true,
                                   0,
                                   false,
                                   false,
                                   coro_functors.first,
                                   coro_functors.second,
                                   txm);

    bool exists = false;
    TxErrorCode err = TxReadCatalog(txm, read_catalog_req, exists);

    if (err != TxErrorCode::NO_ERROR || !exists)
    {
        txservice::AbortTx(txm, coro_functors.first, coro_functors.second);
        return -1;
    }

    TxKey mkey = GenKey(rid->seq_name_);
    // std::unique_ptr<EloqRecord> mrec = std::make_unique<EloqRecord>();
    std::unique_ptr<TxRecord> mrec = GenRecord();
    // uint64_t schema_version = Sequences::GetTableSchema()->Version();
    ReadTxRequest read_req(&table_name_,
                           seq_schema_version_,
                           &mkey,
                           mrec.get(),
                           true,
                           false,
                           false,
                           0,
                           false,
                           true,
                           coro_functors.first,
                           coro_functors.second,
                           txm);

    txm->Execute(&read_req);
    read_req.Wait();

    if (read_req.IsError())
    {
        txservice::AbortTx(txm, coro_functors.first, coro_functors.second);
        return -1;
    }

    assert(read_req.Result().first == RecordStatus::Normal);

    std::string_view rec_sv{mrec->EncodedBlobData(), mrec->EncodedBlobSize()};

    int64_t start = 0;
    int32_t range_step = 0;
    int32_t rec_step = 0;
    int64_t curr_val = 0;
    DecodeSeqRecord(rec_sv, start, range_step, rec_step, curr_val);
    if (rid->range_step_ < 0)
    {
        rid->range_step_ = range_step;
        rid->rec_step_ = rec_step;
    }
    assert(rid->range_step_ == range_step);
    assert(rid->rec_step_ == rec_step);

    if (curr_val < start)
    {
        curr_val = start;
    }

    int64_t new_val = curr_val + rid->range_step_;

    if (new_val <= 0)
    {
        LOG(ERROR) << "Failed to apply sequence id for desired_vals exceeds "
                      "the limit.";
        assert(false);
        return -1;
    }

    std::string new_rec_str =
        EncodeSeqRecord(start, range_step, rec_step, new_val);
    mrec->SetEncodedBlob(
        reinterpret_cast<const unsigned char *>(new_rec_str.data()),
        new_rec_str.size());

    err = txm->TxUpsert(table_name_,
                        seq_schema_version_,
                        TxKey(std::move(mkey)),
                        std::move(mrec),
                        OperationType::Update);

    if (err != TxErrorCode::NO_ERROR)
    {
        txservice::AbortTx(txm, coro_functors.first, coro_functors.second);
        return -1;
    }

    auto [success, commit_err] =
        txservice::CommitTx(txm, coro_functors.first, coro_functors.second);
    if (success)
    {
        std::unique_lock<bthread::Mutex> lock(rid->mutex_id_);
        assert(rid->curr_id_ >= rid->range_end_);
        rid->curr_id_ = curr_val;
        rid->range_end_ = new_val;
    }

    return 0;
}

int Sequences::UpdateAutoIncrement(
    std::string content,
    std::string dbName,
    std::pair<const std::function<void()> *, const std::function<void()> *>
        coro_functors,
    int16_t group_id)
{
    if (content.size() == 0)
    {
        return 0;
    }
    int64_t start = -1;
    int incr = -1;
    int range = -1;
    std::string seq_name;

    size_t pos2 = 0;
    size_t pos1 = 0;
    while (pos1 < content.size())
    {
        pos2 = content.find(';', pos1);
        if (pos2 == std::string::npos)
            pos2 = content.size();

        std::string sbs = content.substr(pos1, pos2 - pos1);
        size_t pos3 = sbs.find('=');
        if (pos3 == std::string::npos)
            return -1;

        std::string key = sbs.substr(0, pos3);
        std::string val = sbs.substr(pos3 + 1);
        std::transform(key.begin(), key.end(), key.begin(), ::tolower);

        if (key.compare("table_name") == 0)
        {
            if (val[0] != '.')
            {
                val = "./" + dbName + "/" + val;
            }
            seq_name = val;
        }
        else if (key.compare("offset") == 0)
        {
            start = stoll(val);
        }
        else if (key.compare("increment") == 0)
        {
            incr = stoi(val);
        }
        else if (key.compare("range") == 0)
        {
            range = stoi(val);
        }
        else
        {
            return -1;
        }

        pos1 = pos2 + 1;
    }

    if (seq_name.size() == 0 || (start < 0 && incr < 0 && range < 0))
    {
        return -1;
    }

    seq_name = "autoincr_" + seq_name;

    TransactionExecution *txm =
        txservice::NewTxInit(instance_->tx_service_,
                             IsolationLevel::RepeatableRead,
                             CcProtocol::Locking,
                             UINT32_MAX,
                             group_id);
    if (txm == nullptr)
    {
        return -1;
    }

    TxKey mkey = GenKey(seq_name);
    std::unique_ptr<TxRecord> mrec = GenRecord();

    ReadTxRequest read_req(&table_name_,
                           seq_schema_version_,
                           &mkey,
                           mrec.get(),
                           true,
                           false,
                           false,
                           0,
                           false,
                           true,
                           coro_functors.first,
                           coro_functors.second,
                           txm);
    txm->Execute(&read_req);
    read_req.Wait();

    if (read_req.IsError())
    {
        txservice::AbortTx(txm, coro_functors.first, coro_functors.second);
        return -1;
    }

    assert(read_req.Result().first == RecordStatus::Normal);
    std::string_view rec_sv{mrec->EncodedBlobData(), mrec->EncodedBlobSize()};

    int64_t tmp_start = 0;
    int32_t tmp_range_step = 0;
    int32_t tmp_rec_step = 0;
    int64_t tmp_curr_val = 0;
    DecodeSeqRecord(
        rec_sv, tmp_start, tmp_range_step, tmp_rec_step, tmp_curr_val);

    if (tmp_curr_val > 1)
    {
        txservice::AbortTx(txm, coro_functors.first, coro_functors.second);
        LOG(ERROR)
            << "Sequences::UpdateAutoIncrement: Only support updating auto"
               " increment parameters in the case of the empty table.";
        return -1;
    }

    // sequneces table non-pk columns:
    // start bigint,node_step int,rec_step int,curr_val bigint
    if (start >= 0)
    {
        tmp_start = start;
    }

    if (range > 0)
    {
        tmp_range_step = range;
    }

    if (incr > 0)
    {
        tmp_rec_step = incr;
    }

    std::string new_rec_str =
        EncodeSeqRecord(tmp_start, tmp_range_step, tmp_rec_step, tmp_curr_val);
    mrec->SetEncodedBlob(
        reinterpret_cast<const unsigned char *>(new_rec_str.data()),
        new_rec_str.size());

    TxErrorCode err = txm->TxUpsert(table_name_,
                                    seq_schema_version_,
                                    TxKey(std::move(mkey)),
                                    std::move(mrec),
                                    OperationType::Update);
    if (err != TxErrorCode::NO_ERROR)
    {
        txservice::AbortTx(txm, coro_functors.first, coro_functors.second);
        LOG(ERROR) << "Sequences::UpdateAutoIncrement: Failed to Upsert "
                      "sequence table.";
        return -1;
    }

    auto [success, commit_err] =
        txservice::CommitTx(txm, coro_functors.first, coro_functors.second);

    return success && commit_err == TxErrorCode::NO_ERROR ? 0 : -1;
}

bool Sequences::ApplyIdOfTableRangePartition(const TableName &table,
                                             NodeGroupId ng_id,
                                             int64_t desired_vals,
                                             int64_t &first_reserved_id,
                                             int64_t &reserved_vals,
                                             uint64_t key_schema_version)
{
    std::string seq_name = GenSeqName(table, SequenceType::RangePartitionId);

    TransactionExecution *txm =
        NewTxInit(instance_->tx_service_,
                  txservice::IsolationLevel::RepeatableRead,
                  txservice::CcProtocol::Locking,
                  ng_id,
                  -1,
                  false,
                  nullptr,
                  nullptr,
                  true);

    if (txm == nullptr)
    {
        return false;
    }

    CatalogKey table_key(table_name_);
    TxKey tbl_tx_key(&table_key);
    CatalogRecord catalog_rec;
    ReadTxRequest read_catalog_req(&txservice::catalog_ccm_name,
                                   0,
                                   &tbl_tx_key,
                                   &catalog_rec,
                                   false,
                                   false,
                                   true,
                                   0,
                                   false,
                                   false,
                                   nullptr,
                                   nullptr,
                                   txm);

    bool exists = false;
    TxErrorCode err = TxReadCatalog(txm, read_catalog_req, exists);

    if (err != TxErrorCode::NO_ERROR || !exists)
    {
        txservice::AbortTx(txm);
        return false;
    }

    TxKey mkey = GenKey(seq_name);
    // std::unique_ptr<EloqRecord> mrec = std::make_unique<EloqRecord>();
    std::unique_ptr<TxRecord> mrec = GenRecord();
    // uint64_t schema_version = Sequences::GetTableSchema()->Version();
    ReadTxRequest read_req(&table_name_,
                           seq_schema_version_,
                           &mkey,
                           mrec.get(),
                           true,
                           false,
                           false,
                           0,
                           false,
                           true,
                           nullptr,
                           nullptr,
                           txm);

    txm->Execute(&read_req);
    read_req.Wait();

    if (read_req.IsError())
    {
        txservice::AbortTx(txm);
        return false;
    }

    assert(read_req.Result().first == RecordStatus::Normal);

    std::string_view rec_sv{mrec->EncodedBlobData(), mrec->EncodedBlobSize()};

    int64_t start = 0;
    int32_t range_step = 0;
    int32_t rec_step = 0;
    int64_t curr_val = 0;
    DecodeSeqRecord(rec_sv, start, range_step, rec_step, curr_val);
    assert(range_step == 1);
    assert(rec_step == 1);

    if (curr_val < start)
    {
        curr_val = start;
    }

    int64_t new_val = curr_val + desired_vals;
    if (new_val <= 0)
    {
        LOG(ERROR) << "Failed to apply sequence id for desired_vals exceeds "
                      "the limit.";
        assert(false);
        return false;
    }

    std::string new_rec_str =
        EncodeSeqRecord(start, range_step, rec_step, new_val);
    mrec->SetEncodedBlob(
        reinterpret_cast<const unsigned char *>(new_rec_str.data()),
        new_rec_str.size());

    err = txm->TxUpsert(table_name_,
                        seq_schema_version_,
                        TxKey(std::move(mkey)),
                        std::move(mrec),
                        OperationType::Update);

    if (err != TxErrorCode::NO_ERROR)
    {
        txservice::AbortTx(txm);
        return false;
    }

    auto [success, commit_err] = txservice::CommitTx(txm);
    assert(success);
    if (success)
    {
        first_reserved_id = curr_val;
        reserved_vals = desired_vals;
    }

    return success;
}

int32_t Sequences::InitialRangePartitionIdOf(const TableName &table)
{
    assert(table.Engine() != txservice::TableEngine::EloqKv);
    std::string_view sv = table.StringView();
    return (std::hash<std::string_view>()(sv)) & 0xFFF;
}

bool Sequences::InitIdOfTableRangePartition(const TableName &table,
                                            int32_t last_range_partition_id)
{
    std::string seq_name = GenSeqName(table, SequenceType::RangePartitionId);
    TransactionExecution *txm =
        NewTxInit(instance_->tx_service_,
                  txservice::IsolationLevel::RepeatableRead,
                  txservice::CcProtocol::Locking);

    if (txm == nullptr)
    {
        return false;
    }

    CatalogKey table_key(table_name_);
    TxKey tbl_tx_key(&table_key);
    CatalogRecord catalog_rec;
    ReadTxRequest read_catalog_req(&txservice::catalog_ccm_name,
                                   0,
                                   &tbl_tx_key,
                                   &catalog_rec,
                                   false,
                                   false,
                                   true,
                                   0,
                                   false,
                                   false,
                                   nullptr,
                                   nullptr,
                                   txm);

    bool exists = false;
    TxErrorCode err = TxReadCatalog(txm, read_catalog_req, exists);

    if (err != TxErrorCode::NO_ERROR || !exists)
    {
        txservice::AbortTx(txm);
        return false;
    }

    TxKey mkey = GenKey(seq_name);
    // std::unique_ptr<EloqRecord> mrec = std::make_unique<EloqRecord>();
    std::unique_ptr<TxRecord> mrec = GenRecord();
    // uint64_t schema_version = Sequences::GetTableSchema()->Version();
    ReadTxRequest read_req(&table_name_,
                           seq_schema_version_,
                           &mkey,
                           mrec.get(),
                           true,
                           false,
                           false,
                           0,
                           false,
                           true,
                           nullptr,
                           nullptr,
                           txm);

    txm->Execute(&read_req);
    read_req.Wait();

    if (read_req.IsError())
    {
        txservice::AbortTx(txm, nullptr, nullptr);
        return false;
    }

    if (read_req.Result().first == RecordStatus::Normal)
    {
        std::string_view rec_sv{mrec->EncodedBlobData(),
                                mrec->EncodedBlobSize()};
        int64_t tmp_start = 0;
        int32_t tmp_range_step = 0;
        int32_t tmp_rec_step = 0;
        int64_t tmp_curr_val = 0;
        DecodeSeqRecord(
            rec_sv, tmp_start, tmp_range_step, tmp_rec_step, tmp_curr_val);

        if (tmp_start != last_range_partition_id)
        {
            assert(false);
            LOG(ERROR) << "Sequences::UpdateLastRangePartition failed for "
                          "sequence key existed";
            txservice::AbortTx(txm, nullptr, nullptr);
            return false;
        }
        else
        {
            auto [success, commit_err] =
                txservice::CommitTx(txm, nullptr, nullptr);
            assert(success);
            return success;
        }
    }

    assert(read_req.Result().first == RecordStatus::Deleted);

    std::string new_rec_str = EncodeSeqRecord(
        last_range_partition_id, 1, 1, last_range_partition_id + 1);
    mrec->SetEncodedBlob(
        reinterpret_cast<const unsigned char *>(new_rec_str.data()),
        new_rec_str.size());

    err = txm->TxUpsert(table_name_,
                        seq_schema_version_,
                        TxKey(std::move(mkey)),
                        std::move(mrec),
                        OperationType::Update);

    if (err != TxErrorCode::NO_ERROR)
    {
        txservice::AbortTx(txm, nullptr, nullptr);
        return false;
    }

    auto [success, commit_err] = txservice::CommitTx(txm, nullptr, nullptr);
    assert(success);
    return success;
}

bool Sequences::InitIdOfAutoIncrementColumn(const TableName &table_name)
{
    TransactionExecution *txm =
        NewTxInit(instance_->tx_service_,
                  txservice::IsolationLevel::RepeatableRead,
                  txservice::CcProtocol::Locking);

    if (txm == nullptr)
    {
        return false;
    }

    CatalogKey table_key(table_name_);
    TxKey tbl_tx_key(&table_key);
    CatalogRecord catalog_rec;
    ReadTxRequest read_catalog_req(&txservice::catalog_ccm_name,
                                   0,
                                   &tbl_tx_key,
                                   &catalog_rec,
                                   false,
                                   false,
                                   true,
                                   0,
                                   false,
                                   false,
                                   nullptr,
                                   nullptr,
                                   txm);

    bool exists = false;
    TxErrorCode err = TxReadCatalog(txm, read_catalog_req, exists);

    if (err != TxErrorCode::NO_ERROR || !exists)
    {
        txservice::AbortTx(txm);
        return false;
    }

    auto [seq_tx_key, seq_tx_rec] = GetSequenceKeyAndInitRecord(
        table_name, SequenceType::AutoIncrementColumn);

    err = txm->TxUpsert(table_name_,
                        seq_schema_version_,
                        TxKey(std::move(seq_tx_key)),
                        std::move(seq_tx_rec),
                        OperationType::Update);

    if (err != TxErrorCode::NO_ERROR)
    {
        txservice::AbortTx(txm, nullptr, nullptr);
        return false;
    }

    auto [success, commit_err] = txservice::CommitTx(txm, nullptr, nullptr);
    assert(success);
    return success;
}

std::string Sequences::EncodeSeqRecord(int64_t start_val,
                                       int32_t node_step_val,
                                       int32_t rec_step_val,
                                       int64_t curr_val)
{
    // Sequences table columns:
    // (seq_name varchar(255),
    // start bigint,node_step int,rec_step int,curr_val bigint,
    // primary key(seq_name))
    // Non-pk initial records:
    // 1,           256,          1,           1
    std::string encoded_rec;

    // start
    encoded_rec.append((char *) &start_val, sizeof(start_val));

    // node_step
    encoded_rec.append((char *) &node_step_val, sizeof(node_step_val));

    // rec_step
    encoded_rec.append((char *) &rec_step_val, sizeof(rec_step_val));

    // curr_val
    encoded_rec.append((char *) &curr_val, sizeof(curr_val));

    return encoded_rec;
}

void Sequences::DecodeSeqRecord(std::string_view encoded_data,
                                int64_t &start_val,
                                int32_t &node_step_val,
                                int32_t &rec_step_val,
                                int64_t &curr_val)
{
    assert(encoded_data.size() == (sizeof(int64_t) * 2 + sizeof(int32_t) * 2));
    const char *buf = encoded_data.data();

    start_val = *(reinterpret_cast<const int64_t *>(buf));
    buf += sizeof(int64_t);

    node_step_val = *(reinterpret_cast<const int32_t *>(buf));
    buf += sizeof(int32_t);

    rec_step_val = *(reinterpret_cast<const int32_t *>(buf));
    buf += sizeof(int32_t);

    curr_val = *(reinterpret_cast<const int64_t *>(buf));
    buf += sizeof(int64_t);
}

}  // namespace txservice
