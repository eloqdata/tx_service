/**
 * @file
 * The CDC log scan behind the CdcScan RPC.
 *
 * CdcScan reads a log group's two timestamp-ordered streams, the committed
 * data records and the DDL history, as one sequence merged by timestamp, and
 * returns it in pages. Its contract is in log.proto next to CdcScanRequest;
 * the pieces below implement it.
 *
 * Merge order: by timestamp; at one timestamp every data record precedes
 * every DDL record. The tie rule carries no meaning of its own, but it must be
 * fixed, because it is what lets each stream resume from its own last key: a
 * page is a prefix of the merged sequence, so if it ends inside a timestamp
 * the stream that comes second at that timestamp has returned none of it and
 * the other has returned all of it, or the page ends in the first stream.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "log.pb.h"

namespace rocksdb
{
class DB;
}

namespace txlog
{
/**
 * @brief Where each stream of a scan resumes.
 *
 * A position is a storage key read as "resume strictly after this key", in
 * the bytewise order the stores use:
 * - empty: the stream has returned nothing and starts at the beginning of
 *   complete history;
 * - eight bytes, the big-endian timestamp `ts`: the stream has returned
 *   nothing since a scan started at `from_ts = ts`. Every storage key starts
 *   with its big-endian timestamp and is longer than eight bytes, so each key
 *   at `ts` sorts after this one and each key below `ts` before it: "after"
 *   means "at the first key whose timestamp is >= ts";
 * - anything longer: the last storage key the stream returned.
 */
struct CdcScanPosition
{
    std::string data;  ///< Position in the data stream.
    std::string ddl;   ///< Position in the DDL stream.
};

/**
 * @brief Encodes a position as the `continuation` bytes.
 *
 * The layout is `u16 len | data position | u16 len | ddl position` with
 * big-endian lengths, shared with the EloqDoc runtime, which keeps
 * continuations as scan positions (log.proto).
 *
 * @param position The position to encode.
 * @return The continuation bytes.
 */
std::string EncodeCdcContinuation(const CdcScanPosition &position);

/**
 * @brief Decodes continuation bytes; the inverse of EncodeCdcContinuation.
 *
 * @param bytes The continuation a client passed back.
 * @param[out] position Set to the decoded position on success.
 * @param[out] error Set to the reason on failure.
 * @return False for bytes that are not a continuation this service could
 *         have issued.
 */
bool DecodeCdcContinuation(std::string_view bytes,
                           CdcScanPosition *position,
                           std::string *error);

/**
 * @brief The eight-byte position that starts a stream at a timestamp.
 *
 * @param ts The timestamp to start at (see CdcScanPosition).
 * @return The big-endian `ts`.
 */
std::string CdcTimestampPosition(uint64_t ts);

/**
 * @brief A DDL record's position in the DDL stream: its storage key.
 *
 * DDLs on one table hold its catalog write lock in turn, so they have distinct
 * timestamps; records at one timestamp belong to different transactions, and
 * the transaction number orders them stably. The position begins with the
 * big-endian timestamp and is longer than eight bytes, as the continuation's
 * timestamp form requires, and the runtime rebuilds it from a record's ddl_ts
 * and txn_number.
 *
 * @param ddl_ts The record's stream timestamp.
 * @param txn The schema op's transaction number.
 * @return be64 ddl_ts ‖ be64 txn.
 */
std::string CdcDdlPosition(uint64_t ddl_ts, uint64_t txn);

/**
 * @brief Where a scan starts.
 *
 * The request's continuation, both streams at `from_ts`, or, with neither,
 * the beginning of complete history.
 *
 * @param request The scan request.
 * @param[out] start Set to the starting position on success.
 * @param[out] error Set to the reason on failure.
 * @return False for a malformed continuation.
 */
bool ParseCdcScanStart(const CdcScanRequest &request,
                       CdcScanPosition *start,
                       std::string *error);

/**
 * @brief The page size to use for a request.
 *
 * @param requested The request's `max_bytes`; 0 means the default.
 * @return The size, reduced to the service's ceiling so that one page always
 *         fits a brpc message comfortably.
 */
size_t CdcScanPageBytes(uint32_t requested);

/**
 * @brief One of the two streams a scan walks, positioned by CdcScanPosition.
 */
class CdcStreamCursor
{
public:
    virtual ~CdcStreamCursor() = default;

    /**
     * @brief Positions the cursor at the first record strictly after `after`
     * whose timestamp is at most `upper_ts`.
     *
     * @param after The position to resume after (see CdcScanPosition).
     * @param upper_ts The highest timestamp the cursor yields, ever.
     */
    virtual void Seek(const std::string &after, uint64_t upper_ts) = 0;

    /**
     * @return False once the stream is exhausted through the upper bound, or
     *         has failed.
     */
    virtual bool Valid() const = 0;

    /** @return False if reading the store failed; ErrorText() says why. */
    virtual bool Ok() const = 0;

    /** @return Why reading the store failed; empty while Ok(). */
    virtual std::string ErrorText() const = 0;

    /** @return The current record's timestamp. Only while Valid(). */
    virtual uint64_t Timestamp() const = 0;

    /**
     * @return The current record's storage key, valid until Next(). Only
     *         while Valid().
     */
    virtual std::string_view Key() const = 0;

    /**
     * @return An estimate of what the current record adds to the response,
     *         for the page cut. Only while Valid().
     */
    virtual size_t ByteSize() const = 0;

    /**
     * @brief Appends the current record to a response. Only while Valid().
     *
     * @param response The page being filled.
     */
    virtual void AppendTo(CdcScanResponse *response) const = 0;

    /** @brief Moves to the next record. Only while Valid(). */
    virtual void Next() = 0;
};

/**
 * @brief The DDL stream: a log state's in-memory DDL history.
 *
 * Seek() has the source copy, under the log state's lock, every record
 * strictly after the position and at or below the upper bound; the cursor then
 * walks the copy. DDLs are rare, so the copy is small, and the history can
 * change under the scan without affecting it.
 */
class CdcDdlHistoryCursor final : public CdcStreamCursor
{
public:
    /**
     * @brief Copies the history's records strictly after `after` whose
     * ddl_ts is at most `upper_ts`, in position order.
     */
    using Source = std::function<void(const std::string &after,
                                      uint64_t upper_ts,
                                      std::vector<CdcDdlRecord> *records)>;

    /**
     * @param source Where Seek() copies the records from.
     */
    explicit CdcDdlHistoryCursor(Source source) : source_(std::move(source))
    {
    }

    void Seek(const std::string &after, uint64_t upper_ts) override;
    bool Valid() const override
    {
        return pos_ < records_.size();
    }
    bool Ok() const override
    {
        return true;
    }
    std::string ErrorText() const override
    {
        return {};
    }
    uint64_t Timestamp() const override
    {
        return records_[pos_].ddl_ts();
    }
    std::string_view Key() const override
    {
        return key_;
    }
    size_t ByteSize() const override
    {
        return records_[pos_].ByteSizeLong();
    }
    void AppendTo(CdcScanResponse *response) const override;
    void Next() override;

private:
    /** @brief Sets key_ to the current record's position, if there is one. */
    void LoadKey();

    Source source_;                      ///< The history's copy function.
    std::vector<CdcDdlRecord> records_;  ///< The copy Seek() took.
    size_t pos_{0};                      ///< The current record's index.
    std::string key_;                    ///< The current record's position.
};

/**
 * @brief Fills a response with the next page of the merged sequence.
 *
 * The page is a prefix of the merged sequence starting after `start`, holds
 * only records at or below `arrival_complete_ts`, and is cut once it reaches
 * `page_bytes`, but always holds at least one record when one is available.
 * Appends to `data` and `ddl` and sets `has_more`, `continuation` and
 * `arrival_complete_ts`; `retention_floor_ts` stays 0, since the history
 * boundary is not tracked yet, and `status` is the caller's.
 *
 * @param data The data stream.
 * @param ddl The DDL stream.
 * @param start Where both streams resume.
 * @param arrival_complete_ts The group's min_ckpt_ts_. It must have been read
 *        before either cursor opened its view of the store: a DDL record is
 *        appended above the bound current at its append, so one appended
 *        after the scan's view was taken lies above a bound read before it,
 *        and the page cannot miss it.
 * @param page_bytes The soft cap on the page size.
 * @param[out] response The page.
 * @param[out] error Set to the reason if a stream failed.
 * @return False if a stream failed.
 */
bool RunCdcScan(CdcStreamCursor &data,
                CdcStreamCursor &ddl,
                const CdcScanPosition &start,
                uint64_t arrival_complete_ts,
                size_t page_bytes,
                CdcScanResponse *response,
                std::string *error);

/** @brief How a log state answered a scan; see LogState::CdcScan. */
enum struct CdcScanStatus
{
    Ok,               ///< The page is filled.
    NotReady,         ///< No consistent view right now; retry shortly.
    NotLeader,        ///< Not the leader, or leadership changed mid-scan.
    Unsupported,      ///< This log state keeps no scannable history.
    InvalidArgument,  ///< The request is malformed.
    StorageError,     ///< Reading the store failed.
};

/**
 * @brief Runs a whole scan against a RocksDB-backed log state.
 *
 * The page's `status` is left to the caller.
 *
 * @param db The log state's data stream: an open database that outlives the
 *        call.
 * @param ddl The log state's DDL stream, positioned by the scan.
 * @param arrival_complete_ts Subject to the read-order rule of RunCdcScan.
 * @param request The scan request.
 * @param[out] response The page.
 * @param[out] error Set to the reason on failure.
 * @return Ok, InvalidArgument for a malformed continuation, or StorageError.
 */
CdcScanStatus CdcScanRocksDB(rocksdb::DB *db,
                             CdcStreamCursor &ddl,
                             uint64_t arrival_complete_ts,
                             const CdcScanRequest &request,
                             CdcScanResponse *response,
                             std::string *error);
}  // namespace txlog
