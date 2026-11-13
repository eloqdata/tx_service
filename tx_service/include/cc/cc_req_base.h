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

#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>

#include "cc_protocol.h"
#include "error_messages.h"
#include "tx_id.h"
#include "type.h"

namespace txservice
{
class CcShard;

/**
 * @brief How long a request parked for shard memory may wait before it is
 * rejected with OUT_OF_MEMORY (CcRequestBase::MemoryWaitPolicy()).
 *
 * The shard cleaner applies it at the end of a pass
 * (CcShard::AbortRequestsAfterMemoryFree()), and a store slice load that
 * fails with OUT_OF_MEMORY applies it to the requests waiting on that slice
 * (RequestAborterCc).
 */
enum class MemoryWait : uint8_t
{
    /**
     * Gives up rather than waits: rejected at the end of the first cleaner
     * pass that finds the heap full or frees nothing, and aborted when a slice
     * load it waits on runs out of memory. The checkpoint-cycle bound applies
     * too. For requests whose wait could block a range split.
     */
    kFailFast,
    /**
     * Rejected once it has stayed parked across
     * FLAGS_shard_memory_wait_ckpt_cycles concluded checkpoint cycles;
     * re-enqueued, not aborted, when a slice load it waits on runs out of
     * memory. The default.
     */
    kBounded,
    /**
     * Never rejected for waiting, however many checkpoint cycles conclude;
     * re-enqueued when a slice load it waits on runs out of memory. For the
     * requests that must be applied: log replay (ReplayLogCc) and a write
     * forwarded to a standby (KeyObjectStandbyForwardCc).
     */
    kUnbounded,
};

/**
 * @brief The memory wait policy of a request issued on behalf of a
 * transaction, from the transaction's fail-fast flag.
 *
 * A transaction sets the flag (abort_if_oom) while it holds a range read
 * lock: waiting for memory then could block a range split. The request types
 * that carry the flag return this from CcRequestBase::MemoryWaitPolicy().
 * @param abort_if_oom The transaction's fail-fast flag, as the request
 * carries it.
 * @return MemoryWait::kFailFast if @p abort_if_oom is set, else
 * MemoryWait::kBounded.
 */
inline MemoryWait TxMemoryWaitPolicy(bool abort_if_oom)
{
    return abort_if_oom ? MemoryWait::kFailFast : MemoryWait::kBounded;
}

struct CcRequestBase
{
public:
    virtual ~CcRequestBase() = default;

    /**
     * @brief Processes the cc request toward the input concurrency control (cc)
     * shard.
     *
     * @param ccs The cc shard on which the cc request is processed.
     * @return true, if the request needs to be freed and recycled; false, if
     * the request should not be freed and recycled.
     */
    virtual bool Execute(CcShard &ccs) = 0;

    bool InUse() const
    {
        return in_use_.load(std::memory_order_acquire);
    }

    virtual void Free()
    {
        in_use_.store(false, std::memory_order_release);
    }

    /**
     * @brief Takes the request into use for one attempt.
     *
     * Clears the memory park stamp: a pooled request reused for a new attempt
     * must not inherit how long an earlier attempt waited.
     */
    void Use()
    {
        memory_park_stamp_ = 0;
        in_use_.store(true, std::memory_order_release);
    }

    /**
     * @brief The node's concluded checkpoint cycle count, plus one, at the
     * time this request first parked for memory in the current attempt.
     *
     * Zero means the request has not parked since it was taken into use. The
     * stamp survives release from the wait list and a later re-park, so the
     * cleaner measures the wait from the first park (see
     * CcShard::AbortRequestsAfterMemoryFree()).
     * @return The stamp, or 0.
     */
    uint64_t MemoryParkStamp() const
    {
        return memory_park_stamp_;
    }

    /**
     * @brief Records the first park of this attempt. Only the owning shard
     * calls this, from CcShard::EnqueueWaitListIfMemoryFull().
     * @param stamp The concluded cycle count plus one; never 0.
     */
    void SetMemoryParkStamp(uint64_t stamp)
    {
        assert(stamp != 0);
        memory_park_stamp_ = stamp;
    }

    TxNumber Txn() const
    {
        return tx_number_;
    }

    CcProtocol Protocol() const
    {
        return proto_;
    }

    IsolationLevel Isolation() const
    {
        return isolation_level_;
    }

    // Remember to call Free() when implement AbortCcRequest in case it may be
    // recycled in CcRequestPool
    virtual void AbortCcRequest(CcErrorCode err_code)
    {
        Free();
        assert(false && "Unimplemented virtual method");
    }

    virtual uint64_t SchemaVersion() const
    {
        return 0;
    }

    /**
     * @brief How long this request may wait for shard memory before it is
     * rejected with OUT_OF_MEMORY (see MemoryWait).
     *
     * A request that is not rejected is still released like any other, and
     * whether it is still wanted is its own check when it executes again: its
     * node group term may have changed while it was parked.
     * @return MemoryWait::kBounded unless the request type says otherwise.
     */
    virtual MemoryWait MemoryWaitPolicy() const
    {
        return MemoryWait::kBounded;
    }

protected:
    CcRequestBase() = default;

    std::atomic<bool> in_use_{false};
    TxNumber tx_number_{0};
    CcProtocol proto_{CcProtocol::OCC};
    IsolationLevel isolation_level_{IsolationLevel::ReadCommitted};
    /** See MemoryParkStamp(). Written only by the owning shard. */
    uint64_t memory_park_stamp_{0};

private:
    // Intrusive links, managed exclusively by CcRequestList. They let a shard
    // park a request -- e.g. on the memory wait list -- without allocating a
    // list node, which matters because that list grows precisely when the
    // shard heap is exhausted. The shard scheduler ensures that a request is
    // either executing, queued, or parked on one wait list; it cannot be
    // inserted into another wait list until the first one re-enqueues it.
    CcRequestBase *list_prev_{nullptr};
    CcRequestBase *list_next_{nullptr};

    friend class CcRequestList;
};

/**
 * @brief Intrusive doubly-linked list of cc requests, threaded through
 * CcRequestBase's own link fields.
 *
 * Parking and removal are O(1) and allocation-free. PushBack()/Remove() assert
 * list-local membership, including the one-element case where both intrusive
 * links are null. Not thread-safe: a list must only be mutated from its owning
 * shard.
 *
 * A request must be removed from the list before it is aborted or freed:
 * Free() returns it to a pool where another producer may reuse and re-link
 * it while the stale links still point into this list.
 */
class CcRequestList
{
public:
    bool Empty() const
    {
        return size_ == 0;
    }

    size_t Size() const
    {
        return size_;
    }

    CcRequestBase *Front() const
    {
        return head_;
    }

    bool Contains(const CcRequestBase *req) const
    {
        if (size_ == 0)
        {
            return false;
        }
        if (size_ == 1)
        {
            return head_ == req;
        }
        return req->list_prev_ != nullptr || req->list_next_ != nullptr;
    }

    static CcRequestBase *NextOf(const CcRequestBase *req)
    {
        return req->list_next_;
    }

    void PushBack(CcRequestBase *req)
    {
        assert(!Contains(req));
        assert(req->list_prev_ == nullptr && req->list_next_ == nullptr);

        req->list_prev_ = tail_;
        if (tail_ != nullptr)
        {
            tail_->list_next_ = req;
        }
        else
        {
            head_ = req;
        }
        tail_ = req;
        ++size_;
    }

    CcRequestBase *PopFront()
    {
        CcRequestBase *req = head_;
        if (req != nullptr)
        {
            Remove(req);
        }
        return req;
    }

    void Remove(CcRequestBase *req)
    {
        assert(Contains(req));
        if (req->list_prev_ != nullptr)
        {
            req->list_prev_->list_next_ = req->list_next_;
        }
        else
        {
            assert(head_ == req);
            head_ = req->list_next_;
        }

        if (req->list_next_ != nullptr)
        {
            req->list_next_->list_prev_ = req->list_prev_;
        }
        else
        {
            assert(tail_ == req);
            tail_ = req->list_prev_;
        }

        req->list_prev_ = nullptr;
        req->list_next_ = nullptr;
        assert(size_ > 0);
        --size_;
    }

private:
    CcRequestBase *head_{nullptr};
    CcRequestBase *tail_{nullptr};
    size_t size_{0};
};
}  // namespace txservice
