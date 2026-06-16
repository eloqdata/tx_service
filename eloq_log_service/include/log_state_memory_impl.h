#pragma once

#include <deque>
#include <memory>
#include <utility>

#include "log_utils.h"

#if defined(LOG_STATE_TYPE_MEM)
#include "log_state.h"

namespace txlog
{
class ItemIteratorMemoryImpl : public ItemIterator
{
public:
    explicit ItemIteratorMemoryImpl(std::vector<Item::Pointer> &&ddl_list,
                                    std::vector<Item::Pointer> &&data_list)
        : ItemIterator(std::move(ddl_list)),
          data_log_list_(std::move(data_list)),
          data_next_idx_(0) {};

    ~ItemIteratorMemoryImpl() override = default;

    void SeekToFirst() override
    {
        ddl_idx_ = 0;
        data_next_idx_ = 0;
    }

    bool Valid() override
    {
        return ddl_idx_ < ddl_list_.size() ||
               data_next_idx_ < data_log_list_.size();
    };

    void Next() override
    {
        if (ddl_idx_ < ddl_list_.size())
        {
            ddl_idx_++;
        }
        else if (data_next_idx_ < data_log_list_.size())
        {
            data_next_idx_++;
        }
    };

    const Item &GetItem() override
    {
        if (ddl_idx_ < ddl_list_.size())
        {
            return *ddl_list_.at(ddl_idx_);
        }
        else
        {
            return *data_log_list_.at(data_next_idx_);
        }
    };

    size_t IteratorNum() override
    {
        return 1;
    }

    void SeekToFirst(size_t idx) override
    {
        SeekToFirst();
    }

    bool Valid(size_t idx) override
    {
        return Valid();
    }

    void Next(size_t idx) override
    {
        Next();
    }

    const Item &GetItem(size_t idx) override
    {
        return GetItem();
    };

    void SeekToDDLFirst() override
    {
        ddl_idx_ = 0;
    }

    bool ValidDDL() override
    {
        return ddl_idx_ < ddl_list_.size();
    }

    void NextDDL() override
    {
        ddl_idx_++;
    }

    const Item &GetDDLItem() override
    {
        return *ddl_list_.at(ddl_idx_);
    }

private:
    std::vector<Item::Pointer> data_log_list_;
    size_t data_next_idx_;
};

class LogStateMemoryImpl : public LogState
{
public:
    void AddLogItem(uint32_t cc_ng_id,
                    uint64_t tx_number,
                    uint64_t timestamp,
                    const std::string &log_message) override;

    std::pair<bool, std::unique_ptr<ItemIterator>> GetLogReplayList(
        uint32_t node_group_id, uint64_t start_timestamp) override;

    /**
     * Searches the log state machine and returns the records committed by the
     * input tx in the specified cc node group. Returns null if there is no such
     * record.
     */
    std::pair<bool, Item::Pointer> SearchTxDataLog(
        uint64_t tx_number,
        uint32_t cc_ng_id,
        uint64_t lower_bound_ts = 0) override;

    int ReadSnapshot(const std::string &snapshot_path,
                     const std::vector<std::string> &files) override;

    std::vector<std::string> WriteSnapshot(
        const std::string &snapshot_path) override;

    void BeginSnapshot() override;

    void CleanSnapshotState() override;

    void Clear() override;

private:
    /**
     * Committed log records in the log state machine are organized by cc node
     * groups and within one group are ordered roughly by commit timestamps.
     * This organization facilitates log truncation but is less efficient when
     * looking for the log record committed by a specific tx.
     */
    std::unordered_map<uint32_t, std::deque<Item::Pointer>> cc_ng_state_;
    /**
     * To guard access to cc_ng_state from concurrent AddLogItem/DeleteLogItems
     * and SearchTxDataLog.
     */
    mutable std::shared_mutex ng_state_mutex_;
    std::unordered_map<uint32_t, std::deque<Item::Pointer>> snapshot_state_;

    void MakeCopyOfNgState();

    void LoadNgStatesFrom(std::ifstream &is);

    void WriteSnapshotStatesTo(std::ofstream &os);
};
}  // namespace txlog
#endif