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

#include "cc_protocol.h"
#include "error_messages.h"
#include "tx_id.h"
#include "type.h"

namespace txservice
{
class CcShard;

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

    void Use()
    {
        in_use_.store(true, std::memory_order_release);
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

    virtual bool AbortIfOom() const
    {
        return false;
    }

protected:
    CcRequestBase() = default;

    std::atomic<bool> in_use_{false};
    TxNumber tx_number_{0};
    CcProtocol proto_{CcProtocol::OCC};
    IsolationLevel isolation_level_{IsolationLevel::ReadCommitted};

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
