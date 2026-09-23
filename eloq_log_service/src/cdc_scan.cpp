#include "cdc_scan.h"

#include <algorithm>
#include <cstring>
#include <memory>

#include "log_utils.h"

#if defined(LOG_STATE_TYPE_RKDB_ALL)
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#endif

namespace txlog
{
namespace
{
/** @brief Page size when the request asks for none (`max_bytes == 0`). */
constexpr size_t kDefaultCdcScanPageBytes = 4 * 1024 * 1024;
/**
 * @brief Largest page size a request may ask for. A page exceeds it only when
 * one record alone does.
 */
constexpr size_t kMaxCdcScanPageBytes = 32 * 1024 * 1024;

/** @brief What a data record adds to the response besides its blob. */
constexpr size_t kCdcDataRecordOverhead = 32;

/**
 * @brief Length of a data-stream storage key:
 * `bswap64(ts) | bswap32(ng) | bswap64(txn)`.
 */
constexpr size_t kDataKeySize = 20;
/** @brief Length of a timestamp position (see CdcScanPosition). */
constexpr size_t kTimestampSize = sizeof(uint64_t);

/**
 * @brief Appends a big-endian u16.
 *
 * @param[out] out The buffer to append to.
 * @param value The value; only its low 16 bits are written.
 */
void AppendU16(std::string *out, size_t value)
{
    out->push_back(static_cast<char>((value >> 8) & 0xff));
    out->push_back(static_cast<char>(value & 0xff));
}

/**
 * @brief Reads one `u16 len | bytes` field off the front of a buffer.
 *
 * @param[in,out] in The buffer; the field is removed from its front.
 * @param[out] out Set to the field's bytes.
 * @return False if the buffer ends inside the field.
 */
bool ReadLengthPrefixed(std::string_view *in, std::string *out)
{
    if (in->size() < 2)
    {
        return false;
    }
    const size_t len =
        (static_cast<size_t>(static_cast<uint8_t>((*in)[0])) << 8) |
        static_cast<uint8_t>((*in)[1]);
    in->remove_prefix(2);
    if (in->size() < len)
    {
        return false;
    }
    out->assign(in->data(), len);
    in->remove_prefix(len);
    return true;
}
}  // namespace

std::string EncodeCdcContinuation(const CdcScanPosition &position)
{
    std::string out;
    out.reserve(4 + position.data.size() + position.ddl.size());
    AppendU16(&out, position.data.size());
    out.append(position.data);
    AppendU16(&out, position.ddl.size());
    out.append(position.ddl);
    return out;
}

bool DecodeCdcContinuation(std::string_view bytes,
                           CdcScanPosition *position,
                           std::string *error)
{
    CdcScanPosition decoded;
    if (!ReadLengthPrefixed(&bytes, &decoded.data) ||
        !ReadLengthPrefixed(&bytes, &decoded.ddl))
    {
        *error = "continuation is truncated";
        return false;
    }
    if (!bytes.empty())
    {
        *error = "continuation has trailing bytes";
        return false;
    }
    // A data position is empty, a timestamp, or a real data key; a DDL
    // position is empty, a timestamp, or a real DDL key, which is always
    // longer than its timestamp prefix.
    const size_t data_len = decoded.data.size();
    if (data_len != 0 && data_len != kTimestampSize && data_len != kDataKeySize)
    {
        *error = "continuation holds a data-stream key of " +
                 std::to_string(data_len) + " bytes";
        return false;
    }
    const size_t ddl_len = decoded.ddl.size();
    if (ddl_len != 0 && ddl_len < kTimestampSize)
    {
        *error = "continuation holds a DDL-stream key of " +
                 std::to_string(ddl_len) + " bytes";
        return false;
    }
    *position = std::move(decoded);
    return true;
}

std::string CdcTimestampPosition(uint64_t ts)
{
    const uint64_t be = __builtin_bswap64(ts);
    return std::string(reinterpret_cast<const char *>(&be), sizeof(be));
}

bool ParseCdcScanStart(const CdcScanRequest &request,
                       CdcScanPosition *start,
                       std::string *error)
{
    switch (request.start_case())
    {
    case CdcScanRequest::kContinuation:
        return DecodeCdcContinuation(request.continuation(), start, error);
    case CdcScanRequest::kFromTs:
    {
        // Timestamp 0 precedes every record, which is the empty position.
        const uint64_t from_ts = request.from_ts();
        std::string position =
            from_ts == 0 ? std::string() : CdcTimestampPosition(from_ts);
        start->data = position;
        start->ddl = std::move(position);
        return true;
    }
    case CdcScanRequest::START_NOT_SET:
    default:
        // The beginning of complete history. With the history boundary not
        // tracked yet, that is the beginning of each store.
        start->data.clear();
        start->ddl.clear();
        return true;
    }
}

size_t CdcScanPageBytes(uint32_t requested)
{
    if (requested == 0)
    {
        return kDefaultCdcScanPageBytes;
    }
    return std::min<size_t>(requested, kMaxCdcScanPageBytes);
}

bool RunCdcScan(CdcStreamCursor &data,
                CdcStreamCursor &ddl,
                const CdcScanPosition &start,
                uint64_t arrival_complete_ts,
                size_t page_bytes,
                CdcScanResponse *response,
                std::string *error)
{
    response->set_arrival_complete_ts(arrival_complete_ts);
    response->set_retention_floor_ts(0);

    data.Seek(start.data, arrival_complete_ts);
    ddl.Seek(start.ddl, arrival_complete_ts);

    // A stream that returns nothing keeps its starting position, which is
    // exact: every record it holds at or below the bound lies after it.
    CdcScanPosition position = start;
    size_t bytes = 0;
    bool has_more = false;
    bool page_empty = true;
    while (data.Ok() && ddl.Ok())
    {
        const bool data_valid = data.Valid();
        const bool ddl_valid = ddl.Valid();
        if (!data_valid && !ddl_valid)
        {
            // Both streams are exhausted through the bound.
            break;
        }
        // Data first at one timestamp; see the merge order in cdc_scan.h.
        const bool take_data =
            data_valid && (!ddl_valid || data.Timestamp() <= ddl.Timestamp());
        CdcStreamCursor &next = take_data ? data : ddl;

        const size_t size = next.ByteSize();
        if (!page_empty && bytes + size > page_bytes)
        {
            // `next` is a record at or below the bound beyond this page.
            has_more = true;
            break;
        }
        next.AppendTo(response);
        bytes += size;
        page_empty = false;
        std::string &stream_position = take_data ? position.data : position.ddl;
        stream_position.assign(next.Key().data(), next.Key().size());
        next.Next();
    }

    if (!data.Ok())
    {
        *error = "data stream: " + data.ErrorText();
        return false;
    }
    if (!ddl.Ok())
    {
        *error = "DDL stream: " + ddl.ErrorText();
        return false;
    }

    response->set_has_more(has_more);
    response->set_continuation(EncodeCdcContinuation(position));
    return true;
}

#if defined(LOG_STATE_TYPE_RKDB_ALL)
namespace
{
/**
 * @param p Eight bytes holding a big-endian timestamp.
 * @return The timestamp.
 */
uint64_t ReadBigEndianTimestamp(const char *p)
{
    uint64_t be;
    std::memcpy(&be, p, sizeof(be));
    return __builtin_bswap64(be);
}

/**
 * The data stream of a RocksDB-backed log state: the default column family,
 * keyed `bswap64(commit_ts) | bswap32(ng) | bswap64(txn)` with the per-ng
 * blob as the value. The iterator it opens at Seek is one consistent view of
 * the database.
 */
class RocksDBDataCursor final : public CdcStreamCursor
{
public:
    explicit RocksDBDataCursor(rocksdb::DB *db) : db_(db)
    {
    }

    void Seek(const std::string &after, uint64_t upper_ts) override
    {
        rocksdb::ReadOptions options;
        // The bounds must outlive the iterator, hence the members.
        if (upper_ts != UINT64_MAX)
        {
            // Keys at or below upper_ts sort before the timestamp key of
            // upper_ts + 1, and keys at upper_ts + 1 after it.
            upper_key_ = CdcTimestampPosition(upper_ts + 1);
            upper_bound_ = rocksdb::Slice(upper_key_);
            options.iterate_upper_bound = &upper_bound_;
        }
        if (!after.empty())
        {
            lower_key_ = after;
            lower_bound_ = rocksdb::Slice(lower_key_);
            options.iterate_lower_bound = &lower_bound_;
        }
        iter_.reset(db_->NewIterator(options));
        if (after.empty())
        {
            iter_->SeekToFirst();
        }
        else
        {
            iter_->Seek(lower_bound_);
            // A real key position was returned already; resume after it.
            if (iter_->Valid() && iter_->key() == lower_bound_)
            {
                iter_->Next();
            }
        }
        Load();
    }

    bool Valid() const override
    {
        return error_.empty() && iter_ != nullptr && iter_->Valid();
    }

    bool Ok() const override
    {
        return error_.empty() && (iter_ == nullptr || iter_->status().ok());
    }

    std::string ErrorText() const override
    {
        if (!error_.empty())
        {
            return error_;
        }
        return iter_ == nullptr ? std::string() : iter_->status().ToString();
    }

    uint64_t Timestamp() const override
    {
        return ts_;
    }

    std::string_view Key() const override
    {
        const rocksdb::Slice key = iter_->key();
        return std::string_view(key.data(), key.size());
    }

    size_t ByteSize() const override
    {
        return iter_->value().size() + kCdcDataRecordOverhead;
    }

    void AppendTo(CdcScanResponse *response) const override
    {
        CdcDataRecord *record = response->add_data();
        record->set_commit_ts(ts_);
        record->set_cc_ng_id(ng_);
        record->set_txn_number(txn_);
        const rocksdb::Slice value = iter_->value();
        record->set_blob(value.data(), value.size());
    }

    void Next() override
    {
        iter_->Next();
        Load();
    }

private:
    /**
     * @brief Decodes the key under the iterator, if any, into ts_, ng_ and
     * txn_; a key of the wrong length sets error_.
     */
    void Load()
    {
        if (!iter_->Valid())
        {
            return;
        }
        const rocksdb::Slice key = iter_->key();
        if (key.size() != kDataKeySize)
        {
            // Nothing else is stored in this column family, so this is
            // corruption; stopping keeps it from being skipped silently.
            error_ = "data-stream key of " + std::to_string(key.size()) +
                     " bytes, expected " + std::to_string(kDataKeySize);
            return;
        }
        const char *p = key.data();
        ts_ = ReadBigEndianTimestamp(p);
        uint32_t ng_be;
        std::memcpy(&ng_be, p + kTimestampSize, sizeof(ng_be));
        ng_ = __builtin_bswap32(ng_be);
        uint64_t txn_be;
        std::memcpy(
            &txn_be, p + kTimestampSize + sizeof(ng_be), sizeof(txn_be));
        txn_ = __builtin_bswap64(txn_be);
    }

    rocksdb::DB *db_;                          ///< Outlives the cursor.
    std::string upper_key_;                    ///< Backs upper_bound_.
    std::string lower_key_;                    ///< Backs lower_bound_.
    rocksdb::Slice upper_bound_;               ///< Must outlive iter_.
    rocksdb::Slice lower_bound_;               ///< Must outlive iter_.
    std::unique_ptr<rocksdb::Iterator> iter_;  ///< Opened by Seek().
    std::string error_;                        ///< Set by Load() on a bad key.
    uint64_t ts_{0};                           ///< Current record's commit ts.
    uint32_t ng_{0};                           ///< Current record's node group.
    uint64_t txn_{0};                          ///< Current record's txn number.
};
}  // namespace

CdcScanStatus CdcScanRocksDB(rocksdb::DB *db,
                             uint64_t arrival_complete_ts,
                             const CdcScanRequest &request,
                             CdcScanResponse *response,
                             std::string *error)
{
    CdcScanPosition start;
    if (!ParseCdcScanStart(request, &start, error))
    {
        return CdcScanStatus::InvalidArgument;
    }
    RocksDBDataCursor data(db);
    EmptyCdcStreamCursor ddl;
    if (!RunCdcScan(data,
                    ddl,
                    start,
                    arrival_complete_ts,
                    CdcScanPageBytes(request.max_bytes()),
                    response,
                    error))
    {
        return CdcScanStatus::StorageError;
    }
    return CdcScanStatus::Ok;
}
#endif
}  // namespace txlog
