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
#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <iterator>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>  //move
#include <vector>

#include "constants.h"

namespace txservice
{
// define number of engines
#define NUM_EXTERNAL_ENGINES 3
#define KB(x) ((size_t) (x) << 10)
#define MB(x) ((size_t) (x) << 20)
#define GB(x) ((size_t) (x) << 30)

struct Void
{
};

constexpr Void void_ = Void();

#define void_return return void_;

// @brief OperationType contain SQL DML and SQL DDL.
enum class OperationType
{
    // Update is mainly used in DML. When used in DDL, it represents logically
    // alter a table, e.g change a singlekey index to multikey index.
    Update = 1,
    Delete,
    Insert,
    Upsert,
    CreateTable,
    DropTable,
    TruncateTable,
    AddIndex,
    DropIndex,

    // redis object command operation
    CommitCommands,
};

/**
 * @brief Operations of on KV store. This is used when a composite operation
 * needs to run multiple operation on KV store in different phases. Currently
 * used in SplitFlushOp.
 */
enum class DsOperation
{
    // Copy data from old partition to new partition and flush in memory data
    // to new partition.
    CopyAndFlush = 1,
    // Upsert new range to range table.
    UpsertRange,
    // Clean data that have been copied to new partition in old partition.
    CleanOldRange
};

enum class ClusterScaleOpType
{
    AddNodeGroup = 1,
    RemoveNode,
    AddNodeGroupPeers
};

enum class TxnStatus
{
    Ongoing = 0,
    Committed,
    Aborted,
    /**
     * @brief A tx starts committing after receiving the commit command from
     * query runtime. From this point forward, the tx runs toward the end,
     * either committed or aborted, and cannot be interrupted, e.g., the user
     * closes the connection to the runtime. This status marks the period
     * between when the tx starts committing and when the tx's fate is finalized
     * (committed or aborted).
     *
     */
    Committing,
    /**
     * @brief A tx has tried to commit, but log service is unreachable and the
     * tx's commit result is unknown. The tx should not do postprocess and
     * release the locks it holds.
     */
    Unknown,
    /**
     * @brief A tx has committed or aborted and finished post-processing. This
     * state signals that the tx state machine can be recycled. Note that this
     * state shall not be uploaded to the tx entry in the tx service, which
     * notifies other (local or remote) participants in the service the fate of
     * the tx, i.e., committed or aborted.
     *
     */
    Finished,
    /**
     * @brief A tx in the recovering state resumes execution of unfinished,
     * multi-stage operations that are guaranteed to succeed. Example operations
     * include schema evolution and range splitting and merging.
     *
     */
    Recovering,
    /**
     * @brief A tx is ready to be recycled for the next user tx.
     *
     */
    Recycled
};

using NodeId = uint32_t;
using NodeGroupId = uint32_t;

enum class TableType : uint8_t
{
    Primary = 0,
    Secondary,
    UniqueSecondary,
    Catalog,
    RangePartition,
    RangeBucket,
    ClusterConfig
};

enum class TableEngine : uint8_t
{
    None = 0,  // table that does not belong to any engine like bucket table.
    EloqSql = 1,
    EloqKv = 2,
    EloqDoc = 3,
    InternalRange = 4,
    InternalHash = 5,  // eg. Sequence table is a kind of internal hash table.
};

struct TableName
{
    TableName() = delete;
    TableName &operator=(const TableName &) = delete;

    TableName(std::string_view name_view, TableType type, TableEngine engine)
        : own_string_(false), type_(type), engine_(engine)
    {
        new (&name_view_) std::string_view(name_view);
    }

    TableName(const char *name_ptr,
              size_t name_len,
              TableType type,
              TableEngine engine)
        : own_string_(true), type_(type), engine_(engine)
    {
        new (&name_str_) std::string(name_ptr, name_len);
    }

    TableName(std::string name_str, TableType type, TableEngine engine)
        : own_string_(true), type_(type), engine_(engine)
    {
        new (&name_str_) std::string(std::move(name_str));
    }

    // Copy constructor always creates a string owner
    TableName(const TableName &rhs)
        : own_string_(true), type_(rhs.type_), engine_(rhs.engine_)
    {
        new (&name_str_) std::string(rhs.StringView());
    }

    // TableName needs to be MoveInsertable in case like
    // std::vector<txservice::TableName>
    TableName(TableName &&rhs) noexcept
        : own_string_(rhs.own_string_), type_(rhs.type_), engine_(rhs.engine_)
    {
        if (own_string_)
        {
            new (&name_str_) std::string(std::move(rhs.name_str_));
        }
        else
        {
            new (&name_view_) std::string_view(rhs.name_view_);
        }
    }

    TableName &operator=(TableName &&rhs) noexcept
    {
        if (this == &rhs)
        {
            return *this;
        }

        if (rhs.own_string_)
        {
            if (own_string_)
            {
                // Notice: If we only use MoveAssignment operator here
                // ("name_str_=std::move(rhs.name_str_);") ,
                // a "memory leak" error will be throwed when asan enabled.
                // So, we must free old value of "name_str_" before assign it.
                name_str_.~basic_string();
                // Here must re-create "name_str_" to string object instead of
                // assign it like ("name_str_=std::move(rhs.name_str_);").
                // Otherwise, a "heap-use-after-free" error will be exposed.
                new (&name_str_) std::string(std::move(rhs.name_str_));
            }
            else
            {
                new (&name_str_) std::string(std::move(rhs.name_str_));
            }
        }
        else
        {
            if (own_string_)
            {
                name_str_.~basic_string();
            }

            new (&name_view_) std::string_view(rhs.StringView());
        }

        type_ = rhs.type_;
        own_string_ = rhs.own_string_;
        engine_ = rhs.engine_;
        return *this;
    }

    ~TableName()
    {
        if (own_string_)
        {
            name_str_.~basic_string();
        }
    }

    bool operator==(const TableName &rhs) const
    {
        return type_ == rhs.type_ && this->StringView() == rhs.StringView() &&
               engine_ == rhs.engine_;
    }

    bool operator!=(const TableName &rhs) const
    {
        return !this->operator==(rhs);
    }

    bool operator<(const TableName &rhs) const
    {
        return engine_ < rhs.engine_ ||
               (engine_ == rhs.engine_ &&
                (type_ < rhs.type_ || (type_ == rhs.type_ &&
                                       this->StringView() < rhs.StringView())));
    }

    std::string_view StringView() const
    {
        if (own_string_)
        {
            return {name_str_.data(), name_str_.size()};
        }
        else
        {
            return name_view_;
        }
    }

    std::string String() const
    {
        if (own_string_)
        {
            return name_str_;
        }
        else
        {
            return std::string(name_view_);
        }
    }

    std::string Trace() const
    {
        if (own_string_)
        {
            if (type_ == TableType::RangePartition)
            {
                return name_str_ + "_ranges";
            }
            else
            {
                return name_str_;
            }
        }
        else
        {
            if (type_ == TableType::RangePartition)
            {
                return std::string(name_view_) + "_ranges";
            }
            else
            {
                return std::string(name_view_);
            }
        }
    }

    std::string Serialize() const
    {
        std::string str;
        if (own_string_)
        {
            str.reserve(name_str_.size() + sizeof(char));
            str = name_str_;
        }
        else
        {
            str.reserve(name_view_.size() + sizeof(char));
            str = name_view_;
        }

        int8_t engine_type = static_cast<int8_t>(engine_);
        assert(engine_type != 0);
        str.append(1, engine_type);
        return str;
    }

    void CopyFrom(const TableName &other)
    {
        if (other.own_string_)
        {
            if (own_string_)
            {
                name_str_ = other.name_str_;
            }
            else
            {
                new (&name_str_) std::string(other.name_str_);
            }
        }
        else
        {
            if (own_string_)
            {
                name_str_.~basic_string();
            }

            new (&name_view_) std::string_view(other.StringView());
        }

        type_ = other.type_;
        own_string_ = other.own_string_;
        engine_ = other.engine_;
    }

    const std::string_view GetBaseTableNameSV() const
    {
        std::string_view base_name_view = StringView();
        size_t pos = base_name_view.find(INDEX_NAME_PREFIX);
        if (pos != std::string_view::npos)
        {
            return base_name_view.substr(0, pos);
        }
        else
        {
            pos = base_name_view.find(UNIQUE_INDEX_NAME_PREFIX);
            if (pos != std::string_view::npos)
            {
                return base_name_view.substr(0, pos);
            }
            return base_name_view;
        }
    }

    // Check whether it is range table for primary key
    bool IsBase() const
    {
        return TableName::IsBase(this->StringView());
    }

    static bool IsBase(const std::string_view &table_name_sv)
    {
        size_t pos = table_name_sv.find(INDEX_NAME_PREFIX);
        if (pos == std::string_view::npos)
        {
            pos = table_name_sv.find(UNIQUE_INDEX_NAME_PREFIX);
        }
        return (pos == std::string_view::npos) ? true : false;
    }

    bool IsMeta() const
    {
        return TableName::IsMeta(type_);
    }

    static bool IsMeta(TableType type)
    {
        return type == TableType::RangeBucket || type == TableType::Catalog ||
               type == TableType::RangePartition ||
               type == TableType::ClusterConfig;
    }

    // Check whether it is range table for unique secondary key
    bool IsUniqueSecondary() const
    {
        return TableName::IsUniqueSecondary(this->StringView());
    }

    static bool IsUniqueSecondary(const std::string_view &table_name_sv)
    {
        size_t pos = table_name_sv.find(UNIQUE_INDEX_NAME_PREFIX);
        return (pos != std::string_view::npos) ? true : false;
    }

    bool IsHashPartitioned() const
    {
        return engine_ == TableEngine::EloqKv || engine_ == TableEngine::None ||
               engine_ == TableEngine::InternalHash;
    }

    bool IsObjectTable() const
    {
        return engine_ == TableEngine::EloqKv;
    }

    bool IsStringOwner() const
    {
        return own_string_;
    }

    const TableType &Type() const
    {
        return type_;
    }

    const TableEngine &Engine() const
    {
        return engine_;
    }

    // @brief Get table type base on table name, only return Primary and
    // Secondary
    static TableType Type(const std::string_view &table_name_sv)
    {
        if (IsBase(table_name_sv))
        {
            return TableType::Primary;
        }
        else if (IsUniqueSecondary(table_name_sv))
        {
            return TableType::UniqueSecondary;
        }
        else
        {
            return TableType::Secondary;
        }
    }

private:
    // base or index table name
    union
    {
        std::string name_str_;
        std::string_view name_view_;
    };

    bool own_string_;
    TableType type_;
    TableEngine engine_;
};

enum struct ReadType
{
    /**
     * @brief Starts concurrency control for the input key and returns the key's
     * value if the value is cached.
     *
     */
    Inside = 0,
    /**
     * @brief Starts concurrency control for the input key-value pair retrieved
     * from the data store.
     *
     */
    OutsideNormal,
    /**
     * @brief Starts concurrency control for the input key that does not exist
     * in the data store.
     *
     */
    OutsideDeleted,
    /**
     * @brief Given the input key k0, starts concurrency control for the range
     * [start, end) such that start <= k0 < end. This is to lock the next range
     * when scanning forward.
     *
     */
    RangeLeftInclusive,
    /**
     * @brief Given the input key k0, starts concurrency control for the range
     * [start, end) such that start < k0 <= end. This is to lock the next range
     * when scanning backward.
     *
     */
    RangeRightExclusive,
    /**
     * @breif Read in recovering status.
     */
    RecoveringRead
};

enum class PostWriteType
{
    // Single commit installs the committed value and removes the write
    // lock/intent.
    Commit,
    // PrepareCommit uploads a dirty value but does not release the write
    // intent/lock acquired previously. After the prepare commit log flushed,
    // the operation is guaranteed to succeed and can only roll forward upon
    // failures.
    PrepareCommit,
    // PostCommit releases the write lock/intent and turns the dirty value to
    // the committed value.
    PostCommit,
    // DowngradeLock is used to downgrade write lock to write intent. We use
    // this flag to resolve deadlock problem on DDL.
    DowngradeLock,
    // Update dirty value. Takes no effect on lock. An optional medium stage
    // between PrepareCommit and PostCommit.
    UpdateDirty,
};

enum class PostReadType
{
    Release,
    DecrReadIntent,
};

constexpr static std::string_view empty_sv{"__empty"};
constexpr static std::string_view catalog_ccm_name_sv{"__catalog"};
constexpr static std::string_view redis_table_name_sv{"redis_table"};
constexpr static std::string_view range_bucket_ccm_name_sv{"__range_bucket"};
constexpr static std::string_view cluster_config_ccm_name_sv{
    "__cluster_config"};
constexpr static std::string_view sequence_table_name_sv{"__sequence_table"};
constexpr static std::string_view internal_range_table_name_sv{
    "__internal_range_table"};
constexpr static std::string_view internal_hash_table_name_sv{
    "__internal_hash_table"};

inline static TableName catalog_ccm_name{
    catalog_ccm_name_sv, TableType::Catalog, TableEngine::None};
inline static TableName range_bucket_ccm_name{
    range_bucket_ccm_name_sv, TableType::RangeBucket, TableEngine::None};
inline static TableName cluster_config_ccm_name{
    cluster_config_ccm_name_sv, TableType::ClusterConfig, TableEngine::None};
inline static TableName internal_range_table_name{internal_range_table_name_sv,
                                                  TableType::Primary,
                                                  TableEngine::InternalRange};
inline static TableName internal_hash_table_name{
    internal_hash_table_name_sv, TableType::Primary, TableEngine::InternalHash};

// Sequence table is a special table for auto increment id and range partition
// id. It is used to generate auto increment id and range partition id for
// tables.
inline static TableName sequence_table_name{sequence_table_name_sv.data(),
                                            sequence_table_name_sv.size(),
                                            TableType::Primary,
                                            TableEngine::InternalHash};

// Set buckets count to be the same as the hash partition count.
inline static const uint16_t total_range_buckets = 0x400;  // 1024
inline static const uint16_t total_hash_partitions = 0x400;

enum struct SlicePosition
{
    Middle = 0,
    /**
     * @brief The scanned slice is the first slice in its range.
     *
     */
    FirstSliceInRange,
    /**
     * @brief The scanned slice is the last slice in its range.
     *
     */
    LastSliceInRange,
    /**
     * @brief The scanned slice is the first slice in the first range. In
     * other words, the slice's start key is negative infinity.
     *
     */
    FirstSlice,
    /**
     * @brief The scanned slice is the last slice in the last range. In
     * other words, the slice's end key is positive infinity.
     *
     */
    LastSlice
};

enum struct TxProcessorStatus
{
    Busy = 0,
    Sleep,
    Standby
};

enum struct WorkerStatus
{
    Active,
    Terminating,
    Terminated
};

enum struct UploadBatchType : int8_t
{
    // Upload SkIndex records by SkGenerator.
    SkIndexData = 0,
    // Upload the records to its bucket's new owner for cache during bucket
    // migrating.
    DirtyBucketData
};

struct WorkerThreadContext
{
    explicit WorkerThreadContext(int worker_num)
        : worker_num_(worker_num), status_(WorkerStatus::Active)
    {
    }

    void Terminate()
    {
        {
            std::unique_lock<std::mutex> lk(mux_);
            assert(status_ == WorkerStatus::Active);
            status_ = WorkerStatus::Terminated;
            cv_.notify_all();
        }

        // loop over worker threads and join them
        for (size_t i = 0; i < worker_thd_.size(); i++)
        {
            worker_thd_[i].join();
        }
    }

    const int worker_num_;
    std::vector<std::thread> worker_thd_;
    std::mutex mux_;
    std::condition_variable cv_;
    WorkerStatus status_{WorkerStatus::Active};
};

enum class DataStoreDataType
{
    Blob,
    Numeric,
    String,
    Bool
};

struct DataStoreSearchCond
{
    DataStoreSearchCond(std::string field_name,
                        std::string op,
                        std::string val_str,
                        DataStoreDataType data_type)
        : field_name_(field_name),
          op_(op),
          val_str_(val_str),
          data_type_(data_type)
    {
    }

    std::string field_name_;
    std::string op_;
    std::string val_str_;
    DataStoreDataType data_type_;
};
}  // namespace txservice

namespace std
{
template <>
struct hash<txservice::TableName>
{
    size_t operator()(const txservice::TableName &name) const
    {
        return std::hash<std::string_view>()(name.StringView());
    }
};
}  // namespace std

namespace txservice
{
template <typename T>
struct Copy
{
    constexpr void operator()(T &lhs, const T &rhs) const
    {
        lhs = rhs;
    }
};
}  // namespace txservice

namespace txservice
{
struct AlterTableInfo
{
    AlterTableInfo() : index_add_count_(0), index_drop_count_(0)
    {
    }

    /**
     * Serialized altered table info string:
     * ------------------------------------------------------------------------
     * | add index count | add index names len | add index names(consist of
     * ------------------------------------------------------------------------
     * ------------------------------------------------------------------------
     * TableName and kv table name) | drop index count | drop index names len |
     * ------------------------------------------------------------------------
     * ----------------------------------------------------------
     * drop index names(consist of TableName and kv table name) |
     * ----------------------------------------------------------
     */
    std::string SerializeAlteredTableInfo()
    {
        std::string res;
        if (index_add_count_ == 0 && index_drop_count_ == 0)
        {
            return res.append("");
        }
        // add index
        res.append(reinterpret_cast<const char *>(&(index_add_count_)),
                   sizeof(uint8_t));
        size_t index_name_len;
        std::string add_index_name;
        for (auto add_index_it = index_add_names_.cbegin();
             add_index_it != index_add_names_.cend();
             add_index_it++)
        {
            add_index_name.append(add_index_it->first.String())
                .append(" ")
                .append(add_index_it->second)
                .append(" ");
        }
        index_name_len = add_index_name.length();
        res.append(reinterpret_cast<const char *>(&index_name_len),
                   sizeof(add_index_name.length()));
        res.append(add_index_name.data(), add_index_name.length());

        // drop index
        std::string drop_index_name;
        res.append(reinterpret_cast<const char *>(&(index_drop_count_)),
                   sizeof(uint8_t));
        for (auto drop_index_it = index_drop_names_.cbegin();
             drop_index_it != index_drop_names_.cend();
             drop_index_it++)
        {
            drop_index_name.append(drop_index_it->first.String())
                .append(" ")
                .append(drop_index_it->second)
                .append(" ");
        }
        index_name_len = drop_index_name.length();
        res.append(reinterpret_cast<const char *>(&index_name_len),
                   sizeof(drop_index_name.length()));
        res.append(drop_index_name.data(), drop_index_name.length());

        return res;
    }

    void DeserializeAlteredTableInfo(
        const std::string &altered_table_info_image, TableEngine table_engine)
    {
        if (altered_table_info_image.length() <= 0)
        {
            index_add_count_ = 0;
            index_drop_count_ = 0;
            return;
        }
        size_t offset = 0;
        const char *buf = altered_table_info_image.data();

        index_add_count_ = *(uint8_t *) (buf + offset);
        offset += sizeof(uint8_t);
        size_t add_index_names_len = *(size_t *) (buf + offset);
        offset += sizeof(add_index_names_len);
        if (index_add_count_ > 0)
        {
            // Clear this buff.
            index_add_names_.clear();
            std::string add_index_names(buf + offset, add_index_names_len);

            std::stringstream add_ss(add_index_names);
            std::istream_iterator<std::string> begin(add_ss);
            std::istream_iterator<std::string> end;
            std::vector<std::string> tokens(begin, end);
            for (auto it = tokens.begin(); it != tokens.end(); ++it)
            {
                TableType table_type = TableName::Type(*it);
                assert(table_type == TableType::Secondary ||
                       table_type == TableType::UniqueSecondary);

                txservice::TableName add_index_name(
                    std::string_view(*it), table_type, table_engine);
                const std::string &add_index_kv_name = *(++it);
                index_add_names_.emplace(add_index_name, add_index_kv_name);
            }
        }
        else
        {
            index_add_names_.clear();
        }
        offset += add_index_names_len;

        index_drop_count_ = *(uint8_t *) (buf + offset);
        offset += sizeof(uint8_t);
        size_t drop_index_names_len = *(size_t *) (buf + offset);
        offset += sizeof(drop_index_names_len);
        if (index_drop_count_ > 0)
        {
            // Clear this buff.
            index_drop_names_.clear();
            std::string drop_index_names(buf + offset, drop_index_names_len);

            std::stringstream drop_ss(drop_index_names);
            std::istream_iterator<std::string> begin(drop_ss);
            std::istream_iterator<std::string> end;
            std::vector<std::string> tokens(begin, end);
            for (auto it = tokens.begin(); it != tokens.end(); ++it)
            {
                TableType table_type = TableName::Type(*it);
                assert(table_type == TableType::Secondary ||
                       table_type == TableType::UniqueSecondary);

                txservice::TableName drop_index_name(
                    std::string_view(*it), table_type, table_engine);
                const std::string &drop_index_kv_name = *(++it);
                index_drop_names_.emplace(drop_index_name, drop_index_kv_name);
            }
        }
        else
        {
            index_drop_names_.clear();
        }
        offset += drop_index_names_len;

        assert(offset == altered_table_info_image.length());
    }

    void Reset()
    {
        index_add_count_ = 0;
        index_drop_count_ = 0;
        index_add_names_.clear();
        index_drop_names_.clear();
    }

    uint8_t index_add_count_;
    uint8_t index_drop_count_;
    // map of <mysql_index_table_name, kv_index_table_name>
    std::unordered_map<txservice::TableName, std::string> index_add_names_;
    std::unordered_map<txservice::TableName, std::string> index_drop_names_;
};

struct HeapMemStats
{
    HeapMemStats() = default;
    int64_t allocated_{0};
    int64_t committed_{0};
    size_t wait_list_size_{0};
};

/**
 * SkEncoder depends on calculation engine supplied methods to generate
 * packed secondary key. Those methods might raise self-defined exceptions.
 * Capture and return them to calculation engine.
 */
struct PackSkError
{
    PackSkError() = default;
    PackSkError(int32_t code, std::string message)
        : code_(code), message_(std::move(message))
    {
    }

    void Reset()
    {
        code_ = 0;
        message_.clear();
    }

    int32_t code_{0};
    std::string message_;
};
}  // namespace txservice
