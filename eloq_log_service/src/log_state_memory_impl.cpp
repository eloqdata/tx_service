#include "log_state_memory_impl.h"

#if defined(LOG_STATE_TYPE_MEM)

namespace txlog
{
void LogStateMemoryImpl::AddLogItem(uint32_t cc_ng_id,
                                    uint64_t tx_number,
                                    uint64_t timestamp,
                                    const std::string &log_message)
{
    std::unique_lock x_lk(ng_state_mutex_);
    auto shard = cc_ng_state_.find(cc_ng_id);
    if (shard != cc_ng_state_.end())
    {
        shard->second.emplace_back(std::make_shared<Item>(
            tx_number, timestamp, log_message, LogItemType::DataLog));
    }
    else
    {
        auto shard_it = cc_ng_state_.try_emplace(cc_ng_id);
        std::deque<Item::Pointer> &list = shard_it.first->second;
        list.emplace_back(std::make_shared<Item>(
            tx_number, timestamp, log_message, LogItemType::DataLog));
    }
}

std::pair<bool, std::unique_ptr<ItemIterator>>
LogStateMemoryImpl::GetLogReplayList(uint32_t node_group_id,
                                     uint64_t start_timestamp)
{
    std::vector<Item::Pointer> ddl_list;
    GetClusterScaleOpList(ddl_list);
    size_t scale_size = ddl_list.size();
    LOG(INFO) << "cluster_scale_list size: " << scale_size;

    GetSchemaOpList(ddl_list);
    size_t schema_size = ddl_list.size() - scale_size;
    LOG(INFO) << "schema_log_list size: " << schema_size;

    GetSplitRangeOpList(ddl_list);
    size_t rs_size = ddl_list.size() - scale_size - schema_size;
    LOG(INFO) << "split_range_op_list size: " << rs_size;

    std::vector<Item::Pointer> data_log_list;
    auto shard = cc_ng_state_.find(node_group_id);
    if (shard != cc_ng_state_.end())
    {
        for (std::deque<Item::Pointer>::iterator it = shard->second.begin();
             it != shard->second.end();
             ++it)
        {
            if ((*it)->timestamp_ > start_timestamp)
            {
                data_log_list.push_back(*it);
            }
        }
    }

    std::unique_ptr<ItemIterator> result =
        std::make_unique<ItemIteratorMemoryImpl>(std::move(ddl_list),
                                                 std::move(data_log_list));

    return std::make_pair(true, std::move(result));
}

std::pair<bool, Item::Pointer> LogStateMemoryImpl::SearchTxDataLog(
    uint64_t tx_number, uint32_t cc_ng_id, uint64_t write_lock_ts)
{
    std::shared_lock s_lk(ng_state_mutex_);
    auto ng_it = cc_ng_state_.find(cc_ng_id);
    if (ng_it != cc_ng_state_.end())
    {
        for (std::deque<Item::Pointer>::iterator it = ng_it->second.begin();
             it != ng_it->second.end();
             ++it)
        {
            if ((*it)->tx_number_ == tx_number)
            {
                return std::make_pair(true, *it);
            }
        }
    }

    return std::make_pair(true, nullptr);
}

int LogStateMemoryImpl::ReadSnapshot(const std::string &snapshot_path,
                                     const std::vector<std::string> &files)
{
    auto file_path = snapshot_path + "/data";
    std::ifstream is(file_path.c_str());
    LoadNgInfoAndCatalogOpsFrom(is);
    LoadNgStatesFrom(is);
    return 0;
}

std::vector<std::string> LogStateMemoryImpl::WriteSnapshot(
    const std::string &snapshot_path)
{
    auto path = snapshot_path + "/data";
    LOG(INFO) << "log state write snapshot path: " << snapshot_path
              << ", file path: " << path;
    {
        std::ofstream os(path.c_str());
        WriteSnapshotNgInfoAndCatalogOpsTo(os);
        WriteSnapshotStatesTo(os);
    }  // close file
    return std::vector<std::string>{"data"};
}

void LogStateMemoryImpl::BeginSnapshot()
{
    LogState::MakeCopyOfNgInfoAndCatalogOps();
    MakeCopyOfNgState();
}

void LogStateMemoryImpl::CleanSnapshotState()
{
    LogState::CleanSnapshotState();
    snapshot_state_.clear();
}

void LogStateMemoryImpl::Clear()
{
    LogState::Clear();
    cc_ng_state_.clear();
    snapshot_state_.clear();
}

void LogStateMemoryImpl::MakeCopyOfNgState()
{
    snapshot_state_ = cc_ng_state_;
}

void LogStateMemoryImpl::LoadNgStatesFrom(std::ifstream &is)
{
    uint32_t shard_size;
    is.read(reinterpret_cast<char *>(&shard_size), sizeof(shard_size));
    LOG(INFO) << "read snapshot log state shard_size : " << shard_size;
    for (uint32_t i = 0; i < shard_size; i++)
    {
        uint32_t shard_id;
        uint32_t count;
        is.read(reinterpret_cast<char *>(&shard_id), sizeof(shard_id));
        is.read(reinterpret_cast<char *>(&count), sizeof(count));
        LOG(INFO) << "read snapshot log state shard_id : " << shard_id
                  << " count : " << count;
        std::deque<Item::Pointer> list;

        for (uint32_t j = 0; j < count; j++)
        {
            uint64_t tx_number;
            uint64_t timestamp;
            std::string log_message;
            size_t message_size;

            is.read(reinterpret_cast<char *>(&tx_number), sizeof(uint64_t));
            is.read(reinterpret_cast<char *>(&timestamp), sizeof(uint64_t));
            is.read(reinterpret_cast<char *>(&message_size), sizeof(size_t));
            log_message.resize(message_size);
            is.read(log_message.data(), message_size);
            list.emplace_back(std::make_shared<Item>(tx_number,
                                                     timestamp,
                                                     std::move(log_message),
                                                     LogItemType::DataLog));
        }

        cc_ng_state_.emplace(std::make_pair(shard_id, std::move(list)));
    }
}

void LogStateMemoryImpl::WriteSnapshotStatesTo(std::ofstream &os)
{
    const uint32_t &log_shard_size = snapshot_state_.size();
    os.write(reinterpret_cast<const char *>(&log_shard_size),
             sizeof(log_shard_size));
    LOG(INFO) << "write snapshot log state shard_size : " << log_shard_size;
    for (std::unordered_map<uint32_t, std::deque<Item::Pointer>>::iterator it =
             snapshot_state_.begin();
         it != snapshot_state_.end();
         ++it)
    {
        const uint32_t &shard_id = it->first;
        os.write(reinterpret_cast<const char *>(&shard_id), sizeof(shard_id));
        const uint32_t &count = it->second.size();
        os.write(reinterpret_cast<const char *>(&count), sizeof(count));
        LOG(INFO) << "write snapshot log state shard_id : " << shard_id
                  << " log count : " << count;
        for (std::deque<Item::Pointer>::iterator list_item = it->second.begin();
             list_item != it->second.end();
             ++list_item)
        {
            const uint64_t &tx_number = (*list_item)->tx_number_;
            const uint64_t &timestamp = (*list_item)->timestamp_;
            const std::string &log_message = (*list_item)->log_message_;
            size_t message_size = log_message.size();
            os.write(reinterpret_cast<const char *>(&tx_number),
                     sizeof(uint64_t));
            os.write(reinterpret_cast<const char *>(&timestamp),
                     sizeof(uint64_t));
            os.write(reinterpret_cast<const char *>(&message_size),
                     sizeof(size_t));
            os.write(log_message.data(), message_size);
        }
    }
}
}  // namespace txlog
#endif