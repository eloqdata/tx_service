//
// Created by Kevin Chou on 2022/2/14.
//
#include <iostream>
#include <random>

#include "log_state.h"
#include "log_state_rocksdb_impl.h"

#if BRPC_WITH_GLOG
#include "glog_error_logging.h"
#endif

using namespace std;

std::random_device rd;
std::default_random_engine generator(rd());
std::uniform_int_distribution<uint64_t> distribution(0, 0xFFFFFFFF);

void test_add_and_replay_log()
{
    txlog::LogStateRocksDBImpl ls(
        "/tmp/log_state_test/rocksdb" + std::to_string(distribution(generator)),
        0,
        1);
    ls.Start();
    for (int i = 0; i < 10; i++)
    {
        uint32_t ng_id = distribution(generator) % 3;
        uint64_t tx_number = distribution(generator);
        uint64_t timestamp = distribution(generator);
        string message = "ng_id: " + std::to_string(ng_id) +
                         ",\ttx_no: " + std::to_string(tx_number) +
                         ",\ttimestamp: " + std::to_string(timestamp);
        cout << message << "\n";
        ls.AddLogItem(ng_id, tx_number, timestamp, "");
    }
    cout << endl;
    for (uint32_t ng_id = 0; ng_id < 3; ng_id++)
    {
        cout << "get replay log for node group: " << ng_id << ":\n";
        auto res = ls.GetLogReplayList(ng_id, 0);
        if (res.first && res.second)
        {
            for (res.second->SeekToFirst(); res.second->Valid();
                 res.second->Next())
            {
                const txlog::Item &item = res.second->GetItem();
                std::cout << "tx number: " << item.tx_number_
                          << ",\ttimestamp: " << item.timestamp_
                          << ",\tlog message: " << item.log_message_ << "\n";
            }
        }
    }
}

void test_truncate_log()
{
    txlog::LogStateRocksDBImpl ls(
        "/tmp/log_state_test/rocksdb" + std::to_string(distribution(generator)),
        0,
        1);
    ls.Start();
    uint32_t ng_id = 0;
    uint64_t truncate_timestamp = 0;
    for (int i = 0; i < 10; i++)
    {
        uint64_t tx_number = distribution(generator);
        uint64_t timestamp = distribution(generator);
        cout << "tx_number: " << tx_number << ",\ttimestamp: " << timestamp
             << "\n";
        if (i == 5)
            truncate_timestamp = timestamp;
        ls.AddLogItem(
            ng_id,
            tx_number,
            timestamp,
            "some message " + std::to_string(distribution(generator)));
    }
    cout << endl;
    cout << "before truncate: " << endl;
    auto res = ls.GetLogReplayList(0, 0);
    if (res.first && res.second)
    {
        for (res.second->SeekToFirst(); res.second->Valid(); res.second->Next())
        {
            const txlog::Item &item = res.second->GetItem();
            std::cout << "tx number: " << item.tx_number_
                      << ",\ttimestamp: " << item.timestamp_
                      << ",\tlog message: " << item.log_message_ << "\n";
        }
    }
    cout << "truncate log before(inclusive) timestamp: " << truncate_timestamp
         << endl;
    // NOTE: DeleteLogItems has been removed from the current API; truncation is
    // now driven by checkpoint timestamps via UpdateCkptTs /
    // TryCleanMultiStageOps.
    (void) truncate_timestamp;
    cout << "after truncate: " << endl;
    auto res2 = ls.GetLogReplayList(0, 0);
    if (res2.first && res2.second)
    {
        for (res2.second->SeekToFirst(); res2.second->Valid();
             res2.second->Next())
        {
            const txlog::Item &item = res2.second->GetItem();
            std::cout << "tx number: " << item.tx_number_
                      << ",\ttimestamp: " << item.timestamp_
                      << ",\tlog message: " << item.log_message_ << "\n";
        }
    }
}

void test_recover_tx()
{
    txlog::LogStateRocksDBImpl ls(
        "/tmp/log_state_test/rocksdb" + std::to_string(distribution(generator)),
        0,
        1);
    ls.Start();
    vector<vector<uint64_t>> txs(3);
    for (int i = 0; i < 10; i++)
    {
        uint32_t ng_id = distribution(generator) % 3;
        uint64_t tx_number = distribution(generator);
        uint64_t timestamp = distribution(generator);
        txs[ng_id].push_back(tx_number);
        string message = "ng_id: " + std::to_string(ng_id) +
                         ",\ttx_no: " + std::to_string(tx_number) +
                         ",\ttimestamp: " + std::to_string(timestamp);
        cout << message << "\n";
        ls.AddLogItem(ng_id, tx_number, timestamp, message);
    }
    cout << endl;
    for (uint32_t ng_id = 0; ng_id < 3; ng_id++)
    {
        cout << "get all transactions for node group: " << ng_id << ":\n";
        for (auto tx_number : txs[ng_id])
        {
            auto [found, p] = ls.SearchTxDataLog(tx_number, ng_id);
            if (found && p)
            {
                std::cout << "tx number: " << p->tx_number_
                          << ",\ttimestamp: " << p->timestamp_
                          << ",\tlog message: " << p->log_message_ << "\n";
            }
            else
            {
                std::cout << "tx number: " << tx_number << " not found\n";
            }
        }
    }
}

int main(int argc, char *argv[])
{
    InitGoogleLogging(argv);
    //    test_add_and_replay_log();
    //    test_truncate_log();
    test_recover_tx();
}
