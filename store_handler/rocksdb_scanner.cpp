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
#include "rocksdb_scanner.h"

#include "eloqkv_key.h"
#include "redis_object.h"
#include "rocksdb_handler.h"

using namespace txservice;

namespace EloqKV
{

bool RocksDBScanner::Init()
{
    initialized_ = true;
    // rocksdb::Status status;
    rocksdb::ReadOptions read_options;
    iter_ = std::unique_ptr<rocksdb::Iterator>(
        db_->NewIterator(read_options, cfh_));
    assert(iter_->status().ok());
    const EloqKV::EloqKey *redis_key = &start_key_;
    if (redis_key != nullptr &&
        redis_key != EloqKV::EloqKey::NegativeInfinity() &&
        redis_key != EloqKV::EloqKey::PositiveInfinity())
    {
        rocksdb::Slice key =
            rocksdb::Slice(redis_key->Buf(), redis_key->Length());
        if (scan_forward_)
        {
            iter_->Seek(key);
            if (!inclusive_ && iter_->Valid())
            {
                rocksdb::Slice curr_key = iter_->key();
                if (curr_key.ToStringView() == key.ToStringView())
                {
                    iter_->Next();
                }
            }
        }
        else
        {
            iter_->SeekForPrev(key);
            if (!inclusive_ && iter_->Valid())
            {
                rocksdb::Slice curr_key = iter_->key();
                if (curr_key.ToStringView() == key.ToStringView())
                {
                    iter_->Prev();
                }
            }
        }
    }
    else
    {
        if (scan_forward_)
        {
            iter_->SeekToFirst();
        }
        else
        {
            iter_->SeekToLast();
        }
    }
    return true;
}

bool RocksDBScanner::MoveNext()
{
    if (!initialized_)
    {
        if (!Init())
        {
            return false;
        }
        return true;
    }

    if (!iter_->Valid())
    {
        return false;
    }

    if (scan_forward_)
    {
        iter_->Next();
    }
    else
    {
        iter_->Prev();
    }

    return iter_->Valid();
}
void RocksDBScanner::Current(txservice::TxKey &key,
                             const txservice::TxRecord *&rec,
                             uint64_t &version_ts,
                             bool &deleted_)
{
    assert(initialized_);
    while (true)
    {
        if (!iter_->Valid())
        {
            key = TxKey();
            rec = nullptr;
            version_ts = 0;
            deleted_ = false;
            return;
        }

        rocksdb::Slice key_slice = iter_->key();
        rocksdb::Slice value_slice = iter_->value();
        const char *payload = value_slice.data();
        const size_t payload_size = value_slice.size();
        bool is_deleted = false;
        int64_t version = 0;
        current_key_ = std::make_unique<EloqKV::EloqKey>(key_slice.data(),
                                                         key_slice.size());
        RocksDBHandler::DeserializeToTxRecord(
            payload, payload_size, current_rec_, is_deleted, version);

        bool fail = false;
        for (auto &cond : pushdown_condition_)
        {
            if (cond.field_name_ == "type")
            {
                EloqKV::RedisObjectType type_cond =
                    static_cast<EloqKV::RedisObjectType>(cond.val_str_[0]);
                EloqKV::RedisObjectType type_rec =
                    static_cast<EloqKV::RedisEloqObject *>(current_rec_.get())
                        ->ObjectType();
                if (type_cond != type_rec)
                {
                    fail = true;
                    break;
                }
            }
        }

        if (fail || is_deleted)
        {
            MoveNext();
            continue;
        }

        key = TxKey(current_key_.get());
        rec = current_rec_.get();
        version_ts = version;
        deleted_ = false;
        break;
    }
}

void RocksDBScanner::End()
{
}

}  // namespace EloqKV
