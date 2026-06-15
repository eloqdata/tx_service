#pragma once

#include <brpc/server.h>
#include <brpc/stream.h>
#include <butil/logging.h>

#include <chrono>

#include "log.pb.h"

using namespace std::chrono;

// this class is used for client testing only
class ReplayService : public brpc::StreamInputHandler,
                      public txlog::LogReplayService
{
    using time_point = decltype(high_resolution_clock::now());

public:
    ReplayService() = default;
    ~ReplayService() = default;
    void Connect(::google::protobuf::RpcController *controller,
                 const ::txlog::LogReplayConnectRequest *request,
                 ::txlog::LogReplayConnectResponse *response,
                 ::google::protobuf::Closure *done) override
    {
        LOG(INFO) << "Replay service Connect RPC called";
        brpc::ClosureGuard done_guard(done);
        brpc::StreamId streamId;
        auto cntl = dynamic_cast<brpc::Controller *>(controller);
        brpc::StreamOptions stream_options;
        stream_options.handler = this;
        stream_options.idle_timeout_ms = idle_timeout_ms;
        if (brpc::StreamAccept(&streamId, *cntl, &stream_options) != 0)
        {
            cntl->SetFailed("Fail to accept stream");
            LOG(ERROR) << "Fail to accept stream";
            return;
        }
        LOG(INFO) << "Replay service accept stream success";
        response->set_success(true);
    }
    //    int on_received_messages(brpc::StreamId streamId,
    //                             butil::IOBuf *const messages[],
    //                             size_t size) override
    //    {
    //        for (size_t i = 0; i < size; i++)
    //        {
    //            if (!ongoing)
    //            {
    //                ongoing = true;
    //                replay_start_time = high_resolution_clock::now();
    //            }
    ////            record_cnt_++;
    //
    //            txlog::ReplayMessage msg;
    //            butil::IOBufAsZeroCopyInputStream wrapper(*messages[i]);
    //            msg.ParseFromZeroCopyStream(&wrapper);
    //            if (msg.has_log_record())
    //            {
    //                record_cnt_++;
    ////                const auto &record = msg.log_record();
    ////                LOG(INFO) << "Getting log record: " << record_cnt_
    ////                          << ",\tng_id: " << msg.cc_node_group_id()
    ////                          << ",\tcommit timestamp: " <<
    /// record.commit_ts() /                          << ",\tlog blob size: " <<
    /// record.log_blob().size();
    //            }
    //            if (msg.has_finish())
    //            {
    //                const auto &record = msg.finish();
    //                auto finish_time = high_resolution_clock::now();
    //                ongoing = false;
    //
    //                LOG(INFO) << "Getting replay finish message: ng_id: "
    //                          << msg.cc_node_group_id()
    //                          << ", lg_id: " << record.log_group_id();
    //                long us =
    //                    duration_cast<microseconds>(finish_time -
    //                    replay_start_time)
    //                        .count();
    //                LOG(INFO) << "Total processed " << record_cnt_
    //                          << " messages, takes " << us << " microseconds,"
    //                          << (us / 1000) << " milliseconds, "
    //                          << (us / record_cnt_) << " us per message";
    //                record_cnt_ = 0;
    //            }
    //        }
    //        return 0;
    //    }

    int on_received_messages(brpc::StreamId streamId,
                             butil::IOBuf *const messages[],
                             size_t size) override
    {
        auto start_time = high_resolution_clock::now();
        receive_cnt_++;
        if (!ongoing)
        {
            ongoing = true;
            replay_start_time = high_resolution_clock::now();
        }

        if (!parse_)
        {
            if (receive_cnt_ % 20 == 0)
            {
                long us = duration_cast<microseconds>(
                              high_resolution_clock::now() - replay_start_time)
                              .count();
                LOG(INFO) << (us / 1000) << " milliseconds since first message";
                LOG(INFO) << "on_received_message time: "
                          << on_received_message_time << "microseconds";
            }
            auto end_time = high_resolution_clock::now();
            on_received_message_time +=
                duration_cast<microseconds>(end_time - start_time).count();
            return 0;
        }
        else
        {
            for (size_t i = 0; i < size; i++)
            {
                auto parse_start = high_resolution_clock::now();
                txlog::ReplayMessage replay_msg;
                butil::IOBufAsZeroCopyInputStream wrapper(*messages[i]);
                data_size += messages[i]->size();
                replay_msg.ParseFromZeroCopyStream(&wrapper);
                parse_time += duration_cast<microseconds>(
                                  high_resolution_clock::now() - parse_start)
                                  .count();
                auto ng_id = replay_msg.cc_node_group_id();
                const std::string &log_records_blob =
                    replay_msg.binary_log_records();
                parse_log_records(log_records_blob);
                if (replay_msg.has_finish())
                {
                    const auto &record = replay_msg.finish();

                    LOG(INFO)
                        << "Getting replay finish message: ng_id: " << ng_id
                        << ", lg_id: " << record.log_group_id();
                    ongoing = false;
                    break;
                }
            }
        }
        auto end_time = high_resolution_clock::now();
        on_received_message_time +=
            duration_cast<microseconds>(end_time - start_time).count();
        if (!ongoing)
        {
            LOG(INFO) << "on_received_message cnt: " << receive_cnt_
                      << ", on_received_message_time: "
                      << on_received_message_time << " microseconds";
            auto finish_time = high_resolution_clock::now();
            long us =
                duration_cast<microseconds>(finish_time - replay_start_time)
                    .count();
            LOG(INFO) << "Total processed " << record_cnt_
                      << " messages, takes " << us << " microseconds, "
                      << (us / 1000) << " milliseconds";
            if (record_cnt_ > 0)
            {
                LOG(INFO) << (us / record_cnt_) << " us per message";
            }
            LOG(INFO) << "parse_time: " << parse_time << " microseconds, "
                      << parse_time / 1000 << " milliseconds";
            double throughput = double(data_size) / 1024;
            double throu_mb = throughput / 1024;
            LOG(INFO) << "throughput: " << throughput << " KB, " << throu_mb
                      << " MB";
            double replay_rate = throu_mb / (double(us) / 1000000);
            double parse_rate = throu_mb / (double(parse_time) / 1000000);
            LOG(INFO) << "replay data rate: " << replay_rate << " MB/s";
            LOG(INFO) << "parse data rate: " << parse_rate << " MB/s";
            clear();
        }
        return 0;
    }

    void on_idle_timeout(brpc::StreamId id) override
    {
        if (ongoing)
        {
            long us = duration_cast<microseconds>(high_resolution_clock::now() -
                                                  replay_start_time)
                          .count();
            LOG(INFO) << "Stream=" << id
                      << " has no data transmission for a while, "
                      << (us / 1000) - idle_timeout_ms
                      << " milliseconds since first message";
            LOG(INFO) << "on_received_message_time: "
                      << on_received_message_time << " microseconds";
            LOG(INFO) << "parse_time: " << parse_time << " microseconds";
            clear();
        }
    }
    void on_closed(brpc::StreamId id) override
    {
        LOG(INFO) << "Stream=" << id << " is closed";
    }

private:
    bool parse_ = true;
    int record_cnt_{};
    int receive_cnt_{};
    bool ongoing{};
    long on_received_message_time{};
    long parse_time{};
    int idle_timeout_ms = 1000;
    int data_size{};
    time_point replay_start_time;

    void parse_log_records(const std::string &log_records_blob)
    {
        const char *p = log_records_blob.data();
        size_t offset = 0;
        while (offset < log_records_blob.size())
        {
            record_cnt_++;
            offset += sizeof(uint64_t);
            uint32_t length = *(reinterpret_cast<const uint32_t *>(p + offset));
            offset += sizeof(uint32_t);
            std::string_view sv((p + offset), length);
            offset += length;
            //            LOG(INFO) << "timestamp: " << timestamp
            //                      << ", log_blob size: " << length << ",
            //                      log_blob: " << sv;
        }
        //        LOG(INFO) << "offset: " << offset
        //                  << ", log_records_blob size: " <<
        //                  log_records_blob.size();
    }

    void clear()
    {
        record_cnt_ = 0;
        receive_cnt_ = 0;
        ongoing = false;
        on_received_message_time = 0;
        parse_time = 0;
        data_size = 0;
    }
};
