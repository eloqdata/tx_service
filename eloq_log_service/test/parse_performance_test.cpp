#include <butil/iobuf.h>
#include <butil/logging.h>

#include <chrono>
#include <iostream>
#include <random>
#include <sstream>

#include "test.pb.h"

std::random_device rd;
std::default_random_engine generator(rd());
std::uniform_int_distribution<uint64_t> distribution(0, 0xFFFFFFFF);

int log_blob_length = 50;
int message_num = 1e6;

void compare_encode()
{
}

void compare_decode()
{
}

auto now()
{
    return std::chrono::high_resolution_clock::now();
}

void fill_batch(txlog::ReplayMessageBatch &batch)
{
    batch.set_cc_node_group_id(1);
    batch.set_cc_node_group_term(1);
    for (int i = 0; i < message_num; i++)
    {
        auto replay_msg = batch.add_messages();
        auto msg = replay_msg->mutable_log_record();
        uint64_t timestamp = now().time_since_epoch().count();
        msg->set_commit_ts(timestamp);
        msg->set_log_blob(std::string(log_blob_length, 'a'));
    }
}

void fill_batch2(txlog::Batch &batch)
{
    batch.set_cc_node_group_id(1);
    batch.set_cc_node_group_term(1);
    for (int i = 0; i < message_num; i++)
    {
        auto msg = batch.add_log_msgs();
        uint64_t timestamp = now().time_since_epoch().count();
        msg->set_commit_ts(timestamp);
        msg->set_log_blob(std::string(log_blob_length, 'a'));
    }
}

void record()
{
    using namespace std::chrono;
    txlog::ReplayMessageBatch batch;
    auto fill_start = now();
    fill_batch(batch);
    LOG(INFO) << "fill batch takes: "
              << duration_cast<microseconds>(now() - fill_start).count()
              << " microseconds";

    auto traverse = now();
    int cnt{};
    for (auto &msg : batch.messages())
    {
        if (msg.has_log_record())
        {
            cnt++;
        }
    }
    LOG(INFO) << "total traversed: " << cnt << " log record, takes: "
              << duration_cast<microseconds>(now() - traverse).count()
              << " microseconds";

    auto serialize_start = now();
    butil::IOBuf buf;
    butil::IOBufAsZeroCopyOutputStream wrapper(&buf);
    batch.SerializeToZeroCopyStream(&wrapper);
    double size = double(buf.size()) / (1024 * 1024);
    long t = duration_cast<microseconds>(now() - serialize_start).count();
    double rate = double(size) / (double(t) / 1000000);
    LOG(INFO) << "serialize takes: " << t << " microseconds, buf size: " << size
              << " MB, serialize data rate: " << rate << " MB/s";

    txlog::ReplayMessageBatch msg_batch;
    butil::IOBufAsZeroCopyInputStream wrap(buf);
    auto parse_start = now();
    msg_batch.ParseFromZeroCopyStream(&wrap);
    t = duration_cast<microseconds>(now() - parse_start).count();
    rate = double(size) / (double(t) / 1000000);
    LOG(INFO) << "parse takes: " << t << " microseconds, buf size: " << size
              << " MB, parse data rate: " << rate << " MB/s";

    auto traverse_start = now();
    auto replay_msgs = msg_batch.messages();
    int record_cnt{};
    for (auto &msg : replay_msgs)
    {
        if (msg.has_log_record())
        {
            record_cnt++;
        }
    }
    LOG(INFO) << "total processed: " << record_cnt << " log record, takes: "
              << duration_cast<microseconds>(now() - traverse_start).count()
              << " microseconds";
}

void record2()
{
    using namespace std::chrono;
    txlog::Batch batch;
    auto fill_start = now();
    fill_batch2(batch);
    LOG(INFO) << "fill batch takes: "
              << duration_cast<microseconds>(now() - fill_start).count()
              << " microseconds";

    auto traverse = now();
    int cnt{};
    for (auto &msg : batch.log_msgs())
    {
        if (msg.commit_ts() > 0)
        {
            cnt++;
        }
    }
    LOG(INFO) << "total traversed: " << cnt << " log record, takes: "
              << duration_cast<microseconds>(now() - traverse).count()
              << " microseconds";

    auto serialize_start = now();
    butil::IOBuf buf;
    butil::IOBufAsZeroCopyOutputStream wrapper(&buf);
    batch.SerializeToZeroCopyStream(&wrapper);
    double size = double(buf.size()) / (1024 * 1024);
    long t = duration_cast<microseconds>(now() - serialize_start).count();
    double rate = double(size) / (double(t) / 1000000);
    LOG(INFO) << "serialize takes: " << t << " microseconds, buf size: " << size
              << " MB, serialize data rate: " << rate << " MB/s";

    auto parse_start = now();
    txlog::Batch msg_batch;
    butil::IOBufAsZeroCopyInputStream wrap(buf);
    msg_batch.ParseFromZeroCopyStream(&wrap);
    t = duration_cast<microseconds>(now() - parse_start).count();
    rate = double(size) / (double(t) / 1000000);
    LOG(INFO) << "parse takes: " << t << " microseconds, buf size: " << size
              << " MB, parse data rate: " << rate << " MB/s";

    auto traverse_start = now();
    auto replay_msgs = msg_batch.log_msgs();
    int record_cnt{};
    for (auto &msg : replay_msgs)
    {
        if (msg.commit_ts() > 0)
        {
            record_cnt++;
        }
    }
    LOG(INFO) << "total processed: " << record_cnt << " log record, takes: "
              << duration_cast<microseconds>(now() - traverse_start).count()
              << " microseconds";
}

struct LogMessage
{
    uint64_t commit_ts;
    std::string log_blob;
};

void serialize(butil::IOBuf &buf,
               uint32_t &ng_id,
               uint64_t &term,
               uint32_t num = message_num)
{
    uint32_t length = log_blob_length;
    std::string log_blob(length, 'a');
    buf.append((char *) (&num), sizeof(num));
    buf.append((char *) (&ng_id), sizeof(ng_id));
    buf.append((char *) (&term), sizeof(term));
    for (size_t i = 0; i < num; i++)
    {
        uint64_t ts = now().time_since_epoch().count();
        buf.append((char *) (&ts), sizeof(ts));
        buf.append((char *) (&length), sizeof(length));
        buf.append(log_blob.data(), log_blob.size());
    }
}

void record3()
{
    using namespace std::chrono;
    butil::IOBuf buf;
    uint32_t ng_id = 1;
    uint64_t term = 1;
    auto start = now();
    serialize(buf, ng_id, term);
    double size = double(buf.size()) / (1024 * 1024);
    long t = duration_cast<microseconds>(now() - start).count();
    double rate = double(size) / (double(t) / 1000000);
    LOG(INFO) << "manually serialize takes: " << t
              << " microseconds, buf size: " << size
              << " MB, serialize data rate: " << rate << " MB/s";
}

void serialize(std::string &buf, uint32_t num = message_num)
{
    uint32_t length = log_blob_length;
    std::string log_blob(length, 'a');
    buf.append((char *) (&num), sizeof(num));
    uint64_t ts = now().time_since_epoch().count();
    for (size_t i = 0; i < num; i++)
    {
        buf.append((char *) (&ts), sizeof(ts));
        buf.append((char *) (&length), sizeof(length));
        buf.append(log_blob.data(), log_blob.size());
    }
}

void deserialize(const std::string &buf)
{
    LOG(INFO) << buf.size();
    //    std::stringstream ss(buf);

    const char *p = buf.data();
    //    ss.read((char *) (&num), sizeof(num));
    uint32_t num = *(uint32_t *) (p);
    LOG(INFO) << num;
    size_t offset = 4;
    for (int i = 0; i < num; i++)
    {
        uint64_t ts = 0;
        uint32_t length = 0;
        ts = *(uint64_t *) (p + offset);
        offset += sizeof(uint64_t);
        length = *(uint32_t *) (p + offset);
        offset += (sizeof(uint32_t) + length);
        //        ss.read((char *) (&ts), sizeof(ts));
        //        ss.read((char *) (&length), sizeof(length));
        //        ss >> length;
        //        ss.ignore(length);
        //                LOG(INFO) << "ts: " << ts << ", length: " << length;
    }
}

void record4()
{
    using namespace std::chrono;
    auto start = now();
    uint32_t ng_id = 1;
    int64_t term = 1;
    txlog::MyMessage m;
    m.set_cc_node_group_id(ng_id);
    m.set_cc_node_group_term(term);
    std::string *binary = m.mutable_binary_log_records();
    auto s_start = now();
    binary->reserve(256 * 1024 * 1024);
    serialize(*binary);
    LOG(INFO) << "serialize log_blob takes: "
              << duration_cast<microseconds>(now() - s_start).count() << "us";
    butil::IOBuf buf;
    butil::IOBufAsZeroCopyOutputStream wrapper(&buf);
    m.SerializeToZeroCopyStream(&wrapper);

    double size = double(buf.size()) / (1024 * 1024);
    long t = duration_cast<microseconds>(now() - start).count();
    double rate = double(size) / (double(t) / 1000000);
    LOG(INFO) << "serialize takes: " << t << " microseconds, buf size: " << size
              << " MB, serialize data rate: " << rate << " MB/s";

    auto des_start = now();
    txlog::MyMessage mm;
    butil::IOBufAsZeroCopyInputStream wrap(buf);
    mm.ParseFromZeroCopyStream(&wrap);
    const std::string &s = mm.binary_log_records();
    deserialize(s);
    t = duration_cast<microseconds>(now() - des_start).count();
    rate = double(size) / (double(t) / 1000000);
    LOG(INFO) << "parse takes: " << t << " microseconds, buf size: " << size
              << " MB, parse data rate: " << rate << " MB/s";
}

int main(int argc, char *argv[])
{
    using namespace std::chrono;
    record();
    LOG(INFO) << " ";
    record2();
    LOG(INFO) << " ";
    //    record3();
    //    LOG(INFO) << " ";
    record4();
}
