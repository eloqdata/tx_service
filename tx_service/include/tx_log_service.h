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

#include <bthread/moodycamelqueue.h>

#include <cstring>
#include <functional>
#include <mutex>
#include <thread>

#include "file-handler.h"
#include "txlog.h"

namespace txservice
{
struct FlushRequest
{
    const char *buf;
    size_t len;
    std::function<void()> notify;
};

class LogFlushService
{
public:
    LogFlushService() : terminate_(false)
    {
        flush_queue_.reserve(1024);
        FileHandlerFactory factory;
        logfile_hd_ = factory.NewFileHandler("txlog", true);
        thd_ = std::thread([this] { Run(); });
    }

    ~LogFlushService()
    {
        {
            std::unique_lock<std::mutex> lk(mux_);
            terminate_ = true;
        }
        cv_.notify_one();
        thd_.join();
        logfile_hd_->Close();
    }

    void Flush()
    {
        {
            std::unique_lock<std::mutex> lk(mux_);
            flush_queue_.swap(standby_queue_);
        }

        if (flush_queue_.size() == 0)
        {
            return;
        }

        for (FlushRequest *&req : flush_queue_)
        {
            logfile_hd_->Append(req->buf, req->len);
        }
        logfile_hd_->Sync();

        for (FlushRequest *&req : flush_queue_)
        {
            if (req->notify)
            {
                req->notify();
            }
        }

        flush_queue_.clear();
    }

    void Run()
    {
        using namespace std::chrono_literals;

        while (!terminate_)
        {
            Flush();

            std::unique_lock<std::mutex> lk(mux_);
            cv_.wait(
                lk, [this] { return terminate_ || standby_queue_.size() > 0; });
        }

        // Processes the residual request in the queue, if there are any.
        while (standby_queue_.size() > 0)
        {
            Flush();
        }
    }

    void Submit(FlushRequest *req)
    {
        std::unique_lock<std::mutex> lk(mux_);
        standby_queue_.emplace_back(req);
        cv_.notify_one();
    }

private:
    std::vector<FlushRequest *> standby_queue_;
    std::vector<FlushRequest *> flush_queue_;
    std::mutex mux_;
    std::condition_variable cv_;
    FileHandler::Pointer logfile_hd_;
    std::thread thd_;
    bool terminate_;
};

/// <summary>
/// ThreadLocalLog serves one thread executing transactions. The Flush() API
/// must be called by the thread periodically to flush tx logs to stable
/// storage. Forcing proactive flushing in the tx execution thread moves writing
/// to the log buffer from critical sections and reduces the contention between
/// the tx execution and the flushing threads.
/// </summary>
class ThreadLocalLog : public TxLog
{
public:
    ThreadLocalLog(LogFlushService *flu)
        : standby_buf_(1024),
          sta_offset_(0),
          flush_buf_(1024),
          flu_offset_(0),
          flushing_(false),
          flu_service_(flu)
    {
        flush_resps_.reserve(32);
        standby_resps_.reserve(32);

        req.notify = [this]()
        {
            for (const auto &res : flush_resps_)
            {
                res->store(true, std::memory_order_release);
            }
            flush_resps_.clear();

            flushing_.store(false, std::memory_order_release);
        };
    }

    void Append(const std::vector<transaction::ReadSetEntry> &write_set,
                const size_t size,
                TxId txid,
                uint64_t commit_ts,
                std::atomic<bool> &is_finish) override
    {
        assert(is_finish.load() == false);

        int64_t tx_idex = txid.GetTxId();
        char *txid_ptr = (char *) (&tx_idex);
        Write(txid_ptr, sizeof(int64_t));

        char *ts_ptr = (char *) (&commit_ts);
        Write(ts_ptr, sizeof(uint64_t));

        for (size_t idx = 0; idx < size; ++idx)
        {
        }

        standby_resps_.emplace_back(&is_finish);
    }

    void Flush() override
    {
        // The prior flush has not finished or there is nothing to flush.
        if (sta_offset_ == 0)
        {
            return;
        }

        if (flushing_.load(std::memory_order_acquire))
        {
            return;
        }

        flushing_.store(true, std::memory_order_release);

        flush_buf_.swap(standby_buf_);
        flu_offset_ = sta_offset_;
        sta_offset_ = 0;
        flush_resps_.swap(standby_resps_);

        req.buf = flush_buf_.data();
        req.len = flu_offset_;

        if (flu_service_ == nullptr)
        {
            req.notify();
        }
        else
        {
            flu_service_->Submit(&req);
        }
    }

private:
    void Write(const char *content, size_t len)
    {
        if (sta_offset_ + len >= standby_buf_.capacity())
        {
            standby_buf_.reserve(sta_offset_ + len);
        }

        std::memcpy(standby_buf_.data() + sta_offset_, content, len);
        sta_offset_ += len;
    }

    std::vector<char> standby_buf_;
    size_t sta_offset_;
    std::vector<char> flush_buf_;
    size_t flu_offset_;
    std::atomic<bool> flushing_;
    std::vector<std::atomic<bool> *> flush_resps_;
    std::vector<std::atomic<bool> *> standby_resps_;
    FlushRequest req;

    LogFlushService *flu_service_;
};

}  // namespace txservice