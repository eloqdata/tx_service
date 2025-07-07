#include "cc/reader_writer_cntl.h"

#include <cassert>

#include "cc/cc_shard.h"

namespace txservice
{
bool ReaderWriterCntl::AddReader()
{
    ReaderWriterCntlBlock cntl = cntl_block_.load(std::memory_order_relaxed);
    while (cntl.write_status_ == WriteStatus::NoWriter)
    {
        // The CAS succeeds only if there is no writer. If the CAS fails, the
        // control block is updated and the writer status is re-checked as the
        // loop continues.
        bool success = cntl_block_.compare_exchange_weak(
            cntl,
            {cntl.read_ref_cnt_ + 1, cntl.write_status_},
            std::memory_order_acq_rel);

        if (success)
        {
            return true;
        }
    }

    return false;
}

void ReaderWriterCntl::FinishReader()
{
    ReaderWriterCntlBlock cntl = cntl_block_.load(std::memory_order_relaxed);
    while (cntl.write_status_ != WriteStatus::Invalid &&
           !cntl_block_.compare_exchange_weak(
               cntl,
               {cntl.read_ref_cnt_ - 1, cntl.write_status_},
               std::memory_order_acq_rel))
        ;

    if (cntl.write_status_ == WriteStatus::Invalid)
    {
        return;
    }

    // If this is the last reader and there is a pending writer, tries to resume
    // the execution of the write request.
    if (cntl.read_ref_cnt_ - 1 == 0 &&
        cntl.write_status_ == WriteStatus::PendingWriter)
    {
        // If the load precedes the store of write_req_ in AddWriter(), the
        // loaded write request is nullptr. By the time AddWriter() finishes
        // store, the read ref count must be 0 and AddWriter() will return
        // successful. If the load succeeds the store, the last reader and the
        // writer will race to set the write status to ProcessedWriter. Whoever
        // wins will process the write request.
        CcRequestBase *write_req = write_req_.load(std::memory_order_relaxed);
        if (write_req != nullptr)
        {
            cntl = {0, WriteStatus::PendingWriter};
            if (cntl_block_.compare_exchange_strong(
                    cntl,
                    {0, WriteStatus::ProcessedWriter},
                    std::memory_order_acq_rel))
            {
                ccs_->Enqueue(write_req);
            }
        }
    }
}

AddWriterResult ReaderWriterCntl::AddWriter(CcRequestBase *write_req)
{
    ReaderWriterCntlBlock cntl = cntl_block_.load(std::memory_order_relaxed);
    while (true)
    {
        // Requests adding a writer are always processed serially at a
        // single core. So, there is no need to sync among writers.

        if (cntl.write_status_ == WriteStatus::PendingWriter)
        {
            if (write_req_.load(std::memory_order_relaxed) != write_req)
            {
                return AddWriterResult::WriteConflict;
            }
            else
            {
                // The writer was blocked previously and the request has not
                // been processed. Proceeds to process the write request.
                break;
            }
        }
        else if (cntl.write_status_ == WriteStatus::ProcessedWriter)
        {
            return write_req == write_req_.load(std::memory_order_relaxed)
                       ? AddWriterResult::Success
                       : AddWriterResult::WriteConflict;
        }
        else if (cntl.write_status_ == WriteStatus::Invalid)
        {
            // This reader-writer control block is invalid.
            return AddWriterResult::Invalid;
        }
        else
        {
            // The write status is NoWriter.
            if (cntl_block_.compare_exchange_weak(
                    cntl,
                    {cntl.read_ref_cnt_, WriteStatus::PendingWriter},
                    std::memory_order_acq_rel))
            {
                break;
            }
        }
    }
    // After the write status is set to PendingWriter, read ref count can only
    // decrease.

    write_req_.store(write_req, std::memory_order_relaxed);

    cntl = {0, WriteStatus::PendingWriter};
    // If the CAS succeeded, there must be no readers and the write status is
    // set to ProcessedWriter by the method's caller who proceeds to process the
    // request. If the CAS failed, either there is one or more readers or the
    // last reader first sets the write status to ProcessedWriter. The last
    // reader will see the write request and enqueues it for execution.
    bool success = cntl_block_.compare_exchange_strong(
        cntl, {0, WriteStatus::ProcessedWriter}, std::memory_order_acq_rel);
    return success ? AddWriterResult::Success : AddWriterResult::WritePending;
}

void ReaderWriterCntl::FinishWriter()
{
    assert(
        [&]()
        {
            ReaderWriterCntlBlock cntl =
                cntl_block_.load(std::memory_order_relaxed);
            return cntl.read_ref_cnt_ == 0 &&
                   cntl.write_status_ == WriteStatus::ProcessedWriter;
        }());

    write_req_.store(nullptr, std::memory_order_relaxed);
    cntl_block_.store({0, WriteStatus::Invalid}, std::memory_order_relaxed);
}

void ReaderWriterCntl::Invalidate()
{
    cntl_block_.store({0, WriteStatus::Invalid}, std::memory_order_relaxed);
}
}  // namespace txservice