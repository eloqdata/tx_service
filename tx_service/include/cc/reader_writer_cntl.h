#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

#include "cc_req_base.h"

namespace txservice
{
class CcShard;
struct CcRequestBase;

enum struct WriteStatus
{
    NoWriter = 0,
    /**
     * @brief A writer is pending, which blocks future readers and waits for
     * existing readers to finish.
     *
     */
    PendingWriter,
    /**
     * @brief A writer is being processed.
     *
     */
    ProcessedWriter,
    Invalid
};

struct ReaderWriterCntlBlock
{
    uint32_t read_ref_cnt_{0};
    WriteStatus write_status_{WriteStatus::NoWriter};
};

enum struct AddWriterResult : uint8_t
{
    Success = 0,
    WriteConflict,
    WritePending,
    Invalid
};

class ReaderWriterCntl
{
public:
    ReaderWriterCntl() = delete;
    ReaderWriterCntl(CcShard *ccs) : ccs_(ccs)
    {
    }

    bool AddReader();
    void FinishReader();
    AddWriterResult AddWriter(CcRequestBase *write_req);
    void FinishWriter(uint64_t tx_number);
    void Invalidate();

    bool HasNoWriter() const
    {
        return cntl_block_.load(std::memory_order_relaxed).write_status_ ==
               WriteStatus::NoWriter;
    }

private:
    std::atomic<ReaderWriterCntlBlock> cntl_block_;
    CcShard *ccs_{nullptr};
    // Tagged 64-bit slot: bit0=0 -> pointer, bit0=1 -> transaction id.
    std::atomic<uint64_t> writer_{0};
};

template <typename T>
class ReaderWriterObject : public ReaderWriterCntl
{
public:
    ReaderWriterObject(CcShard *ccs) : ReaderWriterCntl(ccs)
    {
    }

    void SetObject(std::shared_ptr<const T> obj)
    {
        obj_ = obj;
    }

    const T *GetObjectPtr() const
    {
        return obj_.get();
    }

private:
    std::shared_ptr<const T> obj_;
};
}  // namespace txservice
