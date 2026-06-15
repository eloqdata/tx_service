#pragma once

#include <brpc/server.h>
#include <brpc/stream.h>
#include <butil/logging.h>

#include "log.pb.h"

// this class is used for client testing only
class ReplayService : public brpc::StreamInputHandler,
                      public txlog::LogReplayService
{
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
        if (brpc::StreamAccept(&streamId, *cntl, &stream_options) != 0)
        {
            cntl->SetFailed("Fail to accept stream");
            LOG(ERROR) << "Fail to accept stream";
            return;
        }
        LOG(INFO) << "Replay service accept stream success";
        response->set_success(true);
    }
    int on_received_messages(brpc::StreamId streamId,
                             butil::IOBuf *const messages[],
                             size_t size) override
    {
        for (size_t i = 0; i < size; i++)
        {
            txlog::ReplayMessage replay_msg;
            butil::IOBufAsZeroCopyInputStream wrapper(*messages[i]);
            replay_msg.ParseFromZeroCopyStream(&wrapper);
            auto ng_id = replay_msg.cc_node_group_id();
            const std::string &log_records_blob =
                replay_msg.binary_log_records();
            parse_log_records(log_records_blob);
            if (replay_msg.has_finish())
            {
                const auto &record = replay_msg.finish();

                LOG(INFO) << "Getting replay finish message: ng_id: " << ng_id
                          << ", lg_id: " << record.log_group_id()
                          << ", total processed: " << record_cnt_ << " records";
                break;
            }
        }
        return 0;
    }
    void on_idle_timeout(brpc::StreamId id) override
    {
        LOG(INFO) << "Stream=" << id << " has no data transmission for a while";
    }
    void on_closed(brpc::StreamId id) override
    {
        LOG(INFO) << "Stream=" << id << " is closed";
    }

private:
    int record_cnt_{};

    void parse_log_records(const std::string &log_records_blob)
    {
        const char *p = log_records_blob.data();
        size_t offset = 0;
        while (offset < log_records_blob.size())
        {
            record_cnt_++;
            uint64_t timestamp = *((uint64_t *) (p + offset));
            offset += sizeof(uint64_t);
            uint32_t length = *((uint32_t *) (p + offset));
            offset += sizeof(uint32_t);
            std::string_view sv((p + offset), length);
            offset += length;
            LOG(INFO) << "timestamp: " << timestamp
                      << ",\tlog_blob size: " << length
                      << ",\tlog_blob: " << sv;
        }
    }
};
