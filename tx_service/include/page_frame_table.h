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

// The page manager of a paged large object (eloqkv docs/08-paged-objects.md
// §4 "Ownership: two layers"). Everything here is TYPE-INDEPENDENT: none of it
// depends on what a page contains, so one implementation serves every paged
// type of every API layer. What a page's bytes MEAN — layout, routing, splits,
// scan order — is the type's business, above this layer.
//
// PageFrameTable owns, under one roof:
//   - the resident-page frames (buffer, dirtiness, LRU position),
//   - BOTH views of the pin fact: the per-page aggregate
//     (PageSlot::pin_count_, what eviction consults) and the per-txn
//     decomposition (tx_contexts_[txn].pinned_, what release consults) —
//     state that must never desynchronize has one owner,
//   - the pending-fault set a yielding command records (§6),
//   - the whole runtime page-id lifecycle: free ranges, pending deletes, the
//     next-id high-water (§4 "the two id lists"),
//   - the metadata row's PAGE-MANAGER SECTION codec (§5): page size, id
//     high-water, pending deletes — serialized by this class so every type
//     reuses the encoding verbatim.
//
// PAYLOAD-scoped on purpose: the table is a member of PagedTxObject, so at a
// §7 swap the whole thing dies with its block — pins counted this block's
// slots and must not survive it. The ENTRY-scoped wake records (FetchHub,
// cc/page_fetch.h) deliberately do NOT live here.

#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace txservice
{
using PageId = uint32_t;
inline constexpr PageId kInvalidPageId = UINT32_MAX;

// Upper bound on an object's page-id high-water, enforced when a metadata
// row is parsed. The id space is u32, but a row claiming billions of pages
// is corruption, not a big object: the free-list rebuild is O(next_page_id)
// and allocates a bitmap of that size, so an unbounded value read from the
// store is an OOM/hang vector. 2^26 pages is ~8 TB at the 128 KB default and
// ~34 GB even at a 512-byte page size — far beyond any object the protocol
// layer admits (EloqKV caps an object at 256 MB), so a legitimate row can
// never approach it.
inline constexpr PageId kMaxPageCount = 1u << 26;

// Bounds on a per-object page size read from a metadata row. The lower bound
// must exceed the page header plus one slot (PageView asserts this); the
// upper bound is generous but finite so a corrupt value cannot drive an
// allocation from a single row.
inline constexpr uint32_t kMinPageSize = 64;
inline constexpr uint32_t kMaxPageSize = 64u * 1024 * 1024;

// ---------------------------------------------------------------------------
// Little-endian load/store and varint helpers (on-disk format, docs/08 §5).
// ---------------------------------------------------------------------------
namespace paged_detail
{
inline void StoreU16(uint8_t *p, uint16_t v)
{
    p[0] = static_cast<uint8_t>(v);
    p[1] = static_cast<uint8_t>(v >> 8);
}

inline uint16_t LoadU16(const uint8_t *p)
{
    return static_cast<uint16_t>(p[0]) | (static_cast<uint16_t>(p[1]) << 8);
}

inline void StoreU32(uint8_t *p, uint32_t v)
{
    p[0] = static_cast<uint8_t>(v);
    p[1] = static_cast<uint8_t>(v >> 8);
    p[2] = static_cast<uint8_t>(v >> 16);
    p[3] = static_cast<uint8_t>(v >> 24);
}

inline uint32_t LoadU32(const uint8_t *p)
{
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}

inline void StoreU64(uint8_t *p, uint64_t v)
{
    StoreU32(p, static_cast<uint32_t>(v));
    StoreU32(p + 4, static_cast<uint32_t>(v >> 32));
}

inline uint64_t LoadU64(const uint8_t *p)
{
    return static_cast<uint64_t>(LoadU32(p)) |
           (static_cast<uint64_t>(LoadU32(p + 4)) << 32);
}

// Unsigned LEB128. u64-capable: tagged length fields carry (len << 1), which
// outgrows u32.
inline size_t VarintSize(uint64_t v)
{
    size_t n = 1;
    while (v >= 0x80)
    {
        v >>= 7;
        ++n;
    }
    return n;
}

inline uint8_t *WriteVarint(uint8_t *p, uint64_t v)
{
    while (v >= 0x80)
    {
        *p++ = static_cast<uint8_t>(v) | 0x80;
        v >>= 7;
    }
    *p++ = static_cast<uint8_t>(v);
    return p;
}

inline const uint8_t *ReadVarint(const uint8_t *p,
                                 const uint8_t *end,
                                 uint64_t &v)
{
    v = 0;
    uint32_t shift = 0;
    while (p < end)
    {
        uint8_t byte = *p++;
        v |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0)
        {
            return p;
        }
        shift += 7;
        if (shift >= 64)
        {
            break;
        }
    }
    return nullptr;
}

inline void AppendVarint(std::string &out, uint64_t v)
{
    uint8_t buf[10];
    uint8_t *end = WriteVarint(buf, v);
    out.append(reinterpret_cast<const char *>(buf),
               static_cast<size_t>(end - buf));
}
}  // namespace paged_detail

/**
 * @brief A contiguous run of page ids. Bulk frees are near-contiguous, so
 * ranges keep both id lists tiny (docs/08 §4).
 */
struct PageIdRange
{
    PageId first_{kInvalidPageId};
    uint32_t count_{0};
};

/**
 * @brief Canonical interval set: sorted by first_, pairwise disjoint,
 * non-adjacent. Allocation takes only prefixes, so ranges shrink or vanish
 * but never split; coalescing happens on exactly one operation, Insert (§4).
 */
class FreeRanges
{
public:
    /**
     * @brief Allocates `count` contiguous ids if some range holds them
     * (prefix take). Best-effort contiguity (§4): first fit.
     * @return The first allocated id, or kInvalidPageId if no range fits —
     * the caller then allocates from the unallocated high-water.
     */
    PageId Allocate(uint32_t count)
    {
        for (size_t i = 0; i < ranges_.size(); ++i)
        {
            if (ranges_[i].count_ >= count)
            {
                PageId first = ranges_[i].first_;
                ranges_[i].first_ += count;
                ranges_[i].count_ -= count;
                if (ranges_[i].count_ == 0)
                {
                    ranges_.erase(ranges_.begin() + static_cast<ptrdiff_t>(i));
                }
                return first;
            }
        }
        return kInvalidPageId;
    }

    /**
     * @brief Inserts a range, coalescing with both neighbours. The range
     * must be disjoint from every existing range (a double free is a logic
     * bug).
     */
    void Insert(PageIdRange range)
    {
        assert(range.count_ > 0);
        auto it = std::lower_bound(ranges_.begin(),
                                   ranges_.end(),
                                   range.first_,
                                   [](const PageIdRange &r, PageId first)
                                   { return r.first_ < first; });
        // Coalesce with the left neighbour.
        if (it != ranges_.begin())
        {
            auto left = std::prev(it);
            assert(left->first_ + left->count_ <= range.first_);
            if (left->first_ + left->count_ == range.first_)
            {
                left->count_ += range.count_;
                // Maybe the grown left range now touches *it.
                if (it != ranges_.end() &&
                    left->first_ + left->count_ == it->first_)
                {
                    left->count_ += it->count_;
                    ranges_.erase(it);
                }
                return;
            }
        }
        // Coalesce with the right neighbour.
        if (it != ranges_.end())
        {
            assert(range.first_ + range.count_ <= it->first_);
            if (range.first_ + range.count_ == it->first_)
            {
                it->first_ = range.first_;
                it->count_ += range.count_;
                return;
            }
        }
        ranges_.insert(it, range);
    }

    /**
     * @brief Is `id` inside any range?
     * @return true iff the id is currently free. O(log ranges).
     */
    bool Contains(PageId id) const
    {
        auto it = std::upper_bound(ranges_.begin(),
                                   ranges_.end(),
                                   id,
                                   [](PageId v, const PageIdRange &r)
                                   { return v < r.first_; });
        if (it == ranges_.begin())
        {
            return false;
        }
        --it;
        return id >= it->first_ && id < it->first_ + it->count_;
    }

    void Clear()
    {
        ranges_.clear();
    }

    const std::vector<PageIdRange> &Ranges() const
    {
        return ranges_;
    }

    /**
     * @brief Test hook: verifies sorted, disjoint, non-adjacent, non-empty
     * ranges.
     */
    // GCOVR_EXCL_START: failure arms of an invariant self-check — no public
    // mutator can produce a non-canonical set, which passing tests prove.
    bool CheckCanonical() const
    {
        for (size_t i = 0; i < ranges_.size(); ++i)
        {
            if (ranges_[i].count_ == 0)
            {
                return false;
            }
            if (i > 0 && ranges_[i - 1].first_ + ranges_[i - 1].count_ >=
                             ranges_[i].first_)
            {
                return false;
            }
        }
        return true;
    }
    // GCOVR_EXCL_STOP

private:
    std::vector<PageIdRange> ranges_;
};

/**
 * @brief A freed range awaiting its store-row Delete, tagged with the
 * freeing transaction's commit ts (§4).
 */
struct PendingDelete
{
    PageIdRange range_;
    uint64_t freed_ts_{0};
};

/**
 * @brief The pending-delete list: append-only in non-decreasing freed_ts_
 * order, drained as a prefix; ranges with different freed_ts_ are never
 * coalesced (§4).
 */
class PendingDeletes
{
public:
    void Append(PageIdRange range, uint64_t freed_ts)
    {
        assert(range.count_ > 0);
        assert(entries_.empty() || entries_.back().freed_ts_ <= freed_ts);
        entries_.push_back(PendingDelete{range, freed_ts});
    }

    /**
     * @brief Pops and returns the prefix of entries with
     * freed_ts_ <= flushed_ts (the §9 post-flush drain guard).
     */
    std::vector<PendingDelete> DrainUpTo(uint64_t flushed_ts)
    {
        size_t n = 0;
        while (n < entries_.size() && entries_[n].freed_ts_ <= flushed_ts)
        {
            ++n;
        }
        std::vector<PendingDelete> drained(
            entries_.begin(), entries_.begin() + static_cast<ptrdiff_t>(n));
        entries_.erase(entries_.begin(),
                       entries_.begin() + static_cast<ptrdiff_t>(n));
        return drained;
    }  // GCOVR_EXCL_LINE: unreachable dtor code (copy elision)

    /**
     * @brief Is `id` inside any pending range?
     * @return true iff the id awaits its store-row Delete. O(entries), which
     * is fine: the list is almost always empty or tiny.
     */
    bool Contains(PageId id) const
    {
        for (const PendingDelete &pd : entries_)
        {
            if (id >= pd.range_.first_ &&
                id < pd.range_.first_ + pd.range_.count_)
            {
                return true;
            }
        }
        return false;
    }

    const std::vector<PendingDelete> &Entries() const
    {
        return entries_;
    }

    void Clear()
    {
        entries_.clear();
    }

private:
    std::vector<PendingDelete> entries_;
};

/**
 * @brief A refcounted, page_size-sized page buffer (§4). shared_ptr for two
 * reasons that both matter: the checkpoint exports pages BY REFERENCE so the
 * flush worker holds the bytes without a copy (§9), and use_count() is what
 * drives §7's copy-on-write decision.
 */
using PageBuf = std::shared_ptr<uint8_t[]>;

/**
 * @brief The admission gate for page-buffer allocation (docs/08 §8).
 *
 * Admission IS allocation: a command claims the memory for its missing pages
 * before issuing any fetch, and is refused — parked, or errored if its need
 * can never fit — rather than allowed to overshoot the shard budget. There is
 * deliberately NO shadow counter of reserved bytes: the allocator's own
 * accounting is the single ledger, so nothing can drift out of sync with it.
 *
 * Thread-local because every paged-object code path runs on its shard's core,
 * the same single-thread binding the engine's mi_heap override already relies
 * on; the engine installs the current shard's checker for exactly the window
 * in which the thread acts as that shard. Left null — unit tests, offline
 * tools, any thread with no shard identity — everything is admitted, which is
 * the pre-admission behaviour.
 */
struct PageAdmission
{
    /** @return true if `bytes` more may be allocated on this shard now. */
    using Fn = bool (*)(void *ctx, size_t bytes);
    /** @return the shard's total page-memory ceiling in bytes. */
    using CapFn = size_t (*)(void *ctx);

    Fn fn{nullptr};
    CapFn cap_fn{nullptr};
    void *ctx{nullptr};
};

inline thread_local PageAdmission tls_page_admission{};

/**
 * @brief Asks the installed gate whether `bytes` may be allocated.
 * @return true when admitted, and always true when no gate is installed.
 */
inline bool AdmitPageBytes(size_t bytes)
{
    const PageAdmission &gate = tls_page_admission;
    return gate.fn == nullptr || gate.fn(gate.ctx, bytes);
}

/**
 * @brief The installed gate's total capacity, independent of current usage —
 * the number a "could this EVER fit?" question must be asked against.
 * @return the ceiling in bytes; SIZE_MAX when no gate is installed.
 */
inline size_t PageAdmissionCeiling()
{
    const PageAdmission &gate = tls_page_admission;
    return gate.cap_fn == nullptr ? SIZE_MAX : gate.cap_fn(gate.ctx);
}

/**
 * @brief Installs an admission gate for the lifetime of the scope, restoring
 * whatever was there before. Nested scopes therefore compose, and an early
 * return cannot leave a stale shard's gate behind.
 */
class PageAdmissionScope
{
public:
    PageAdmissionScope(PageAdmission::Fn fn,
                       PageAdmission::CapFn cap_fn,
                       void *ctx)
        : saved_(tls_page_admission)
    {
        tls_page_admission.fn = fn;
        tls_page_admission.cap_fn = cap_fn;
        tls_page_admission.ctx = ctx;
    }

    ~PageAdmissionScope()
    {
        tls_page_admission = saved_;
    }

    PageAdmissionScope(const PageAdmissionScope &) = delete;
    PageAdmissionScope &operator=(const PageAdmissionScope &) = delete;

private:
    PageAdmission saved_;
};

/**
 * @brief The volatile per-page state (§4): never serialized. Dirtiness is the
 * flushed_ bit, guarded by a faithful last_modified_ts_ — which records when
 * the page's content actually changed and is NEVER overloaded as a
 * clean/dirty sentinel (0 means *unknown*, per the engine convention).
 */
struct PageSlot
{
    // Null => not resident. (Distinct from the null in a flush's page list,
    // which means "delete that page row".)
    PageBuf buf_;
    // In-flight commands needing this page resident (§6). Deliberately not
    // use_count(): that counts logical versions and answers "may I mutate in
    // place?", while this answers "may I evict?" (§7).
    uint32_t pin_count_{0};
    // commit_ts of the last content change; on load, the store row's
    // commit_ts.
    uint64_t last_modified_ts_{0};
    // Current content is durable; dirty <=> !flushed_.
    bool flushed_{false};
    // Intrusive LRU links (§8), by page id rather than pointer so they
    // survive a rehash of the frame map. lru_prev_ walks toward the hot end
    // (head). Mutable because LRU position is not part of the object's value:
    // every page access funnels through the read accessor, which is const
    // (ExecuteOn never mutates, #509).
    mutable PageId lru_prev_{kInvalidPageId};
    mutable PageId lru_next_{kInvalidPageId};
};

/**
 * @brief The reserved tx number under which the buffered-command drain
 * participates as a page-fetch waiter (docs/08 §10). Never issued to real
 * transactions.
 *
 * Replay, standby apply and migration all reach CommitOn with no transaction
 * of their own executing, so a page fault raised there has no real txn to
 * park. The drain registers under this sentinel instead, and its wake record
 * carries a null parked request on purpose: reaching an outstanding count of
 * zero means "run TryCommitBufferedCommands again", not "enqueue a request".
 */
inline constexpr uint64_t kDrainTxnNumber = UINT64_MAX;

/**
 * @brief Per-faulting-transaction PIN context (docs/08 §4) — nothing is
 * added to ApplyCc. It is the carrier that lets a write transaction's pins
 * survive from Execute to CommitOn inside PostWriteCc, which knows only the
 * tx number.
 *
 * Pins are this context's ONLY state, by design: the wake half — who is
 * parked, how many fetches it awaits, whether one errored — is ENTRY-scoped
 * and lives in FetchHub::TxWake (cc/page_fetch.h), because a payload swap or
 * DEL destroys this object and the record of who is waiting must outlive it
 * (§4: "requests with the entry, state with the payload").
 */
struct TxPageContext
{
    // The page ids whose per-page pin counts this txn incremented — the
    // per-txn decomposition of the per-page aggregate, so release knows what
    // to decrement (§4). Pins bridge bounded I/O gaps only: they must be
    // released at commit/abort and must never survive into an unbounded
    // condition park (§6).
    std::vector<PageId> pinned_;
};

/**
 * @brief All volatile per-page state, both pin views, the fault set, and the
 * page-id lifecycle of one paged object — see the header comment. All calls
 * run on the owning shard core.
 */
class PageFrameTable
{
public:
    /**
     * @brief An empty, page-less table for DeserializeMeta to fill.
     */
    PageFrameTable() = default;

    /**
     * @brief Prepares a fresh table for a newly created object. The type then
     * allocates its initial page ids (AllocatePageId) and materializes them
     * (CreateDirtyPage).
     */
    void InitFresh(uint32_t page_size)
    {
        assert(page_size > 0);
        page_size_ = page_size;
        next_page_id_ = 0;
    }

    /**
     * @brief Shares the resident buffers with `rhs` (§7 copy-on-write: the
     * clone and the source point at the same PageBufs; the write accessor
     * copies a page the first time either side writes it). Volatile
     * per-command state — pins, tx contexts, pending faults — does NOT carry
     * over: it belongs to the block whose slots counted it (§7). The LRU is
     * rebuilt in the source's order.
     */
    PageFrameTable(const PageFrameTable &rhs)
        : free_ranges_(rhs.free_ranges_),
          pending_delete_(rhs.pending_delete_),
          next_page_id_(rhs.next_page_id_),
          page_size_(rhs.page_size_),
          write_ts_(rhs.write_ts_)
    {
        for (const auto &[id, slot] : rhs.frames_)
        {
            PageSlot &copy = frames_.try_emplace(id).first->second;
            copy.last_modified_ts_ = slot.last_modified_ts_;
            copy.flushed_ = slot.flushed_;
            copy.buf_ = slot.buf_;
        }
        for (PageId id : rhs.LruColdToHot())
        {
            LruTouch(id);
        }
    }

    /**
     * @brief Moves leave `rhs` EMPTY-CONSISTENT, not merely unspecified: a
     * defaulted move would carry the LRU head/tail values while the frame
     * map moves away, so a later copy of the husk (which walks the LRU)
     * would dereference pages that no longer exist. The class-twin swap
     * (AddTTL/RemoveTTL) moves whole objects, so husks do occur.
     */
    PageFrameTable(PageFrameTable &&rhs) noexcept
        : frames_(std::move(rhs.frames_)),
          tx_contexts_(std::move(rhs.tx_contexts_)),
          pending_faults_(std::move(rhs.pending_faults_)),
          reserved_(std::move(rhs.reserved_)),
          free_ranges_(std::move(rhs.free_ranges_)),
          pending_delete_(std::move(rhs.pending_delete_)),
          next_page_id_(rhs.next_page_id_),
          page_size_(rhs.page_size_),
          lru_head_(rhs.lru_head_),
          lru_tail_(rhs.lru_tail_),
          write_ts_(rhs.write_ts_)
    {
        rhs.frames_.clear();
        rhs.tx_contexts_.clear();
        rhs.pending_faults_.clear();
        rhs.reserved_.clear();
        rhs.free_ranges_.Clear();
        rhs.pending_delete_.Clear();
        rhs.next_page_id_ = 0;
        rhs.lru_head_ = kInvalidPageId;
        rhs.lru_tail_ = kInvalidPageId;
    }

    PageFrameTable &operator=(PageFrameTable &&rhs) noexcept
    {
        if (this != &rhs)
        {
            PageFrameTable moved(std::move(rhs));
            frames_ = std::move(moved.frames_);
            tx_contexts_ = std::move(moved.tx_contexts_);
            pending_faults_ = std::move(moved.pending_faults_);
            reserved_ = std::move(moved.reserved_);  // claimed fetches follow
            free_ranges_ = std::move(moved.free_ranges_);
            pending_delete_ = std::move(moved.pending_delete_);
            next_page_id_ = moved.next_page_id_;
            page_size_ = moved.page_size_;
            lru_head_ = moved.lru_head_;
            lru_tail_ = moved.lru_tail_;
            write_ts_ = moved.write_ts_;
        }
        return *this;
    }
    PageFrameTable &operator=(const PageFrameTable &rhs)
    {
        PageFrameTable copy(rhs);
        *this = std::move(copy);
        return *this;
    }

    // ---- residency ------------------------------------------------------

    uint32_t PageSize() const
    {
        return page_size_;
    }

    bool IsResident(PageId id) const
    {
        return frames_.find(id) != frames_.end();
    }

    size_t ResidentPageCount() const
    {
        return frames_.size();
    }

    /**
     * @brief Bytes held by resident page buffers — the figure that shrinks
     * under partial eviction (§8), as distinct from the object's logical
     * size.
     */
    size_t ResidentBytes() const
    {
        return frames_.size() * page_size_;
    }

    // ---- id lifecycle (§4 "the two id lists") ---------------------------
    //
    // The three lists partition [0, next_page_id_): an id is FREE
    // (free_ranges_), PENDING DELETE (pending_delete_), or LIVE — referenced
    // by the type's structure. The partition is what makes liveness a
    // type-independent question: every id comes from AllocatePageId, frees go
    // through FreePage, and the free list is rebuilt from the type-enumerated
    // live set exactly once, at load.

    PageId AllocatePageId()
    {
        PageId id = free_ranges_.Allocate(1);
        if (id == kInvalidPageId)
        {
            id = next_page_id_++;
        }
        return id;
    }

    /**
     * @brief Frees a live page id: Live -> Pending delete (§4). Its buffer is
     * dropped — the page's *contents* must never be flushed again, only its
     * row deleted.
     */
    void FreePage(PageId id)
    {
        pending_delete_.Append(PageIdRange{id, 1}, write_ts_);
        LruUnlink(id);
        frames_.erase(id);
    }

    /**
     * @brief Is `id` part of the object's logical content (§4's Live state),
     * whether or not its bytes are in memory? Derived from the id partition:
     * allocated, not free, not pending delete.
     */
    bool IsLive(PageId id) const
    {
        return id < next_page_id_ && !free_ranges_.Contains(id) &&
               !pending_delete_.Contains(id);
    }

    /**
     * @brief Number of distinct live page ids.
     */
    size_t LivePageCount() const
    {
        size_t freed = 0;
        for (const PageIdRange &r : free_ranges_.Ranges())
        {
            freed += r.count_;
        }
        size_t pending = 0;
        for (const PendingDelete &pd : pending_delete_.Entries())
        {
            pending += pd.range_.count_;
        }
        assert(freed + pending <= next_page_id_);
        return static_cast<size_t>(next_page_id_) - freed - pending;
    }

    /**
     * @brief Invokes fn(id) for every live page id, ascending — the
     * whole-object delete fan-out (§9), which needs no page resident.
     */
    template <typename Fn>
    void ForEachLivePageId(Fn &&fn) const
    {
        for (PageId id = 0; id < next_page_id_; ++id)
        {
            if (!free_ranges_.Contains(id) && !pending_delete_.Contains(id))
            {
                fn(id);
            }
        }
    }

    /**
     * @brief Invokes fn(id) for every page id awaiting a store-row delete
     * (§4), ascending.
     */
    template <typename Fn>
    void ForEachPendingDeleteId(Fn &&fn) const
    {
        std::vector<PageId> ids;
        for (const PendingDelete &pd : pending_delete_.Entries())
        {
            for (uint32_t i = 0; i < pd.range_.count_; ++i)
            {
                ids.push_back(pd.range_.first_ + i);
            }
        }
        std::sort(ids.begin(), ids.end());
        for (PageId id : ids)
        {
            fn(id);
        }
    }

    const FreeRanges &FreeList() const
    {
        return free_ranges_;
    }

    const std::vector<PendingDelete> &PendingDeleteEntries() const
    {
        return pending_delete_.Entries();
    }

    // ---- buffer access for the type's layout code -----------------------

    /**
     * @brief The read accessor: raw bytes of a RESIDENT page. Every page
     * access funnels through here or BufForWrite, which makes these the
     * exact touch points for the eviction LRU (§8). Const — reads never
     * mutate the object (#509) — hence the mutable LRU state.
     */
    uint8_t *BufForRead(PageId id) const
    {
        auto it = frames_.find(id);
        assert(it != frames_.end() && it->second.buf_ != nullptr &&
               "page is not resident; the fetch path installs it first");
        LruTouch(id);
        return it->second.buf_.get();
    }

    /**
     * @brief The WRITE accessor: performs copy-on-write, then returns a
     * buffer this table exclusively owns (§7).
     *
     * A page buffer is shared when a copy-on-write clone points at it (a
     * transaction's dirty object, from the copy constructor) or when a flush
     * worker holds it by reference (§9's zero-copy export). Either way the
     * bytes must not be mutated in place — the clone would see another
     * transaction's uncommitted write, and the flush worker would write out a
     * page that changed underneath it. use_count() is exactly the "is anyone
     * else looking at this?" question.
     */
    uint8_t *BufForWrite(PageId id)
    {
        auto it = frames_.find(id);
        assert(it != frames_.end() && it->second.buf_ != nullptr &&
               "page is not resident");
        PageSlot &slot = it->second;
        if (slot.buf_.use_count() > 1)
        {
            // Unchecked: this runs inside CommitOn, which cannot fail (§8).
            PageBuf owned = AllocPageUnchecked();
            std::memcpy(owned.get(), slot.buf_.get(), page_size_);
            slot.buf_ = std::move(owned);
        }
        LruTouch(id);
        return slot.buf_.get();
    }

    /**
     * @brief Creates a fresh, zeroed, DIRTY resident page for `id` — used by
     * the type when it materializes a new page (initial page, splits). The
     * page is provisionally stamped with the current write ts; StampWrites
     * assigns the authoritative commit ts (§4).
     * @return the page's buffer for the type to initialize.
     */
    uint8_t *CreateDirtyPage(PageId id)
    {
        PageSlot &slot = frames_.try_emplace(id).first->second;
        // Unchecked: CommitOn cannot fail, and the caller writes through this
        // pointer immediately (§8).
        slot.buf_ = AllocPageUnchecked();
        slot.flushed_ = false;
        slot.last_modified_ts_ = write_ts_;
        LruTouch(id);
        return slot.buf_.get();
    }

    /**
     * @brief Marks a resident page's content as changed at the current write
     * ts (§4): faithful timestamp, and dirty via the separate flushed_ bit.
     * The stamp is provisional; StampWrites assigns the authoritative commit
     * ts at PostWriteCc, and nothing can observe this one before then (the
     * uncommitted dirty payload is invisible to the checkpoint).
     */
    void TouchPage(PageId id)
    {
        auto it = frames_.find(id);
        assert(it != frames_.end());
        it->second.flushed_ = false;
        it->second.last_modified_ts_ = write_ts_;
    }

    /**
     * @brief Sets the provisional ts stamped on subsequent mutations: one
     * CommitOn is one commit_ts for all of its writes (§4).
     */
    void SetWriteTs(uint64_t write_ts)
    {
        assert(write_ts != 0 && "0 means *unknown*, never a write ts");
        write_ts_ = write_ts;
    }

    /**
     * @brief Resolves the provisional ts on pages this object's uncommitted
     * writes touched, now that the transaction has committed at `commit_ts`
     * (§4). Every still-dirty page takes it — deliberately conservative: a
     * newer stamp on a page whose older flush is in flight fails the §9
     * post-flush guard, so the page stays dirty and is re-exported. Wasted
     * I/O in a narrow window, never a page marked clean whose bytes did not
     * reach the store.
     */
    void StampWrites(uint64_t commit_ts)
    {
        assert(commit_ts != 0 && "0 means *unknown*, never a commit ts");
        for (auto &[id, slot] : frames_)
        {
            if (slot.buf_ != nullptr && !slot.flushed_)
            {
                slot.last_modified_ts_ = commit_ts;
            }
        }
    }

    // ---- admission (§8) --------------------------------------------------

    /**
     * @brief Claims a page buffer for every id in `ids` that does not already
     * hold one, ALL-OR-NOTHING (docs/08 §8).
     *
     * This is the admission gate for the fault path, and it is a real
     * allocation rather than a reservation counter: on success the memory is
     * already off the shard budget, so concurrent faulters cannot each pass a
     * "there is room" test and jointly overshoot.
     *
     * On refusal every buffer taken in THIS call is released before returning
     * — a caller that parked while holding a partial claim would be waiting
     * for memory it is itself withholding, the textbook deadlock. Buffers
     * claimed by an earlier call for a still-in-flight fetch are left alone;
     * they belong to that fetch, not to this attempt.
     *
     * @param ids Page ids the caller is about to fetch.
     * @return true if every id now has a buffer waiting for it; false if the
     * shard refused, in which case the caller must park and retry whole.
     */
    bool ReservePageBuffers(const std::vector<PageId> &ids) const
    {
        std::vector<PageId> taken;
        taken.reserve(ids.size());
        for (PageId id : ids)
        {
            if (reserved_.find(id) != reserved_.end())
            {
                continue;  // already claimed by an in-flight fetch
            }
            PageBuf buf = AllocPage();
            if (buf == nullptr)
            {
                for (PageId undo : taken)
                {
                    reserved_.erase(undo);
                }
                return false;
            }
            reserved_.emplace(id, std::move(buf));
            taken.push_back(id);
        }
        return true;
    }

    /**
     * @brief Whether a fault set of `page_count` pages could EVER be admitted
     * on this shard, ignoring what is currently in use.
     *
     * Distinguishes "wait for memory" from "this can never work": parking a
     * command whose need exceeds the whole budget would stall it forever, so
     * that case must become a deterministic error instead (§8).
     *
     * @return true if the request is within the shard's capacity in principle.
     */
    static bool FaultSetCanEverFit(size_t page_count, uint32_t page_size)
    {
        // Against the CEILING, not against current usage: the question is
        // whether an otherwise-empty shard could hold this fault set. Asking
        // AdmitPageBytes instead would turn a merely-busy shard's refusal into
        // a permanent error.
        return static_cast<uint64_t>(page_count) * page_size <=
               PageAdmissionCeiling();
    }

    /** @brief Releases a claimed buffer, if any, for `id`. */
    PageBuf TakeReserved(PageId id) const
    {
        auto it = reserved_.find(id);
        if (it == reserved_.end())
        {
            return nullptr;
        }
        PageBuf buf = std::move(it->second);
        reserved_.erase(it);
        return buf;
    }

    /**
     * @brief Releases the claimed buffer for `id` without using it — the
     * error/discard half of the reservation lifecycle (§8): fetch failure,
     * missing row, rejected image, dead term. Idempotent.
     */
    void DropReserved(PageId id) const
    {
        reserved_.erase(id);
    }

    /** @brief Number of buffers currently claimed but not yet installed. */
    size_t ReservedCount() const
    {
        return reserved_.size();
    }

    // ---- install (fetch completion, §13 back-fill page mode) ------------

    /**
     * @brief Installs fetched bytes as page `id`. The page takes the store
     * row's commit ts and is clean by definition — it came from the store.
     * @return false if the id is no longer live (freed while the fetch was
     * in flight) or the byte count does not match; the caller discards.
     */
    bool InstallPage(PageId id, std::string_view bytes, uint64_t row_commit_ts)
    {
        if (bytes.size() != page_size_ || !IsLive(id))
        {
            // A refused install still ends the fetch this reservation was
            // claimed for (§8): release it, or it lingers until the payload
            // dies.
            DropReserved(id);
            return false;
        }
        PageSlot &slot = frames_.try_emplace(id).first->second;
        // A fresh buffer whenever the current one is absent OR shared: a
        // clone or a flush worker may be holding it, and overwriting it in
        // place would rewrite their page underneath them (§7).
        if (slot.buf_ == nullptr || slot.buf_.use_count() > 1)
        {
            // Normally this buffer was already claimed at admission (§8), so
            // take the reservation rather than allocating here — install runs
            // after the store read and must not be refusable. AllocPage is the
            // fallback for installs that never went through admission (the
            // shared-buffer path's sibling, tests, tools); it can return null,
            // and a null buffer means "install failed", never a null write.
            slot.buf_ = TakeReserved(id);
            if (slot.buf_ == nullptr)
            {
                // Unchecked: these bytes are already in memory, so refusing
                // the install would discard a completed read and send the
                // command back to fault on the very same page (§8).
                slot.buf_ = AllocPageUnchecked();
            }
        }
        std::memcpy(slot.buf_.get(), bytes.data(), bytes.size());
        slot.last_modified_ts_ = row_commit_ts;
        slot.flushed_ = true;
        LruTouch(id);
        return true;
    }

    /**
     * @brief Installs a page by SHARING an already-built buffer (§7): one
     * fetch completion can serve both the committed payload and a
     * transaction's dirty copy; copy-on-write splits the buffer the first
     * time either side writes it.
     * @return false if `id` is not live here, or the size does not match.
     */
    bool InstallPageShared(PageId id,
                           PageBuf buf,
                           size_t buf_size,
                           uint64_t row_commit_ts)
    {
        // The fetch this call completes is what the §8 admission claimed a
        // buffer for, so the reservation is consumed HERE — on every
        // outcome, refusals included. Leaving it would defeat admission on
        // the production path: the fault would retain the admitted buffer
        // AND install the caller's, doubling page memory per fault (a
        // review finding — only the InstallPage sibling consumed it).
        // Callers that route the reserved buffer in as `buf` (BackFillPage)
        // have already taken it, and this erase finds nothing.
        DropReserved(id);
        if (buf == nullptr || buf_size != page_size_ || !IsLive(id))
        {
            return false;
        }
        PageSlot &slot = frames_.try_emplace(id).first->second;
        slot.buf_ = std::move(buf);
        slot.last_modified_ts_ = row_commit_ts;
        slot.flushed_ = true;
        LruTouch(id);
        return true;
    }

    // ---- flush (§9) -----------------------------------------------------

    /**
     * @brief Invokes fn(id, buf) for every resident DIRTY page, ascending —
     * the export set of one flush cycle, sorted so the flush list needs no
     * later sort.
     */
    template <typename Fn>
    void ForEachDirtyPage(Fn &&fn) const
    {
        std::vector<PageId> ids;
        ids.reserve(frames_.size());
        for (const auto &[id, slot] : frames_)
        {
            if (slot.buf_ != nullptr && !slot.flushed_)
            {
                ids.push_back(id);
            }
        }
        std::sort(ids.begin(), ids.end());
        for (PageId id : ids)
        {
            fn(id, frames_.find(id)->second.buf_);
        }
    }

    /**
     * @brief The §9 post-flush callback, per page and per freed range, under
     * one guard: the flushed commit ts.
     *
     * Marking is guarded rather than unconditional because the export runs
     * on the shard core while the flush runs on a worker thread — in that
     * window the object can accept a write to an already-exported page. Such
     * a page carries a newer last_modified_ts_, fails the comparison, and
     * stays dirty (§4). The same ts scopes the pending-delete drain to
     * exactly what was written: anything freed after the export persists to
     * the next cycle rather than recycling ids whose Delete never went out.
     */
    void OnFlushApplied(uint64_t flushed_commit_ts)
    {
        for (auto &[id, slot] : frames_)
        {
            if (slot.buf_ != nullptr &&
                slot.last_modified_ts_ <= flushed_commit_ts)
            {
                slot.flushed_ = true;
            }
        }
        for (const PendingDelete &drained :
             pending_delete_.DrainUpTo(flushed_commit_ts))
        {
            free_ranges_.Insert(drained.range_);
        }
    }

    // ---- eviction (§8) --------------------------------------------------

    /**
     * @brief Drops a clean, unpinned page's buffer — the §8 shed primitive.
     * @return false if the page is dirty, pinned, or absent.
     */
    bool ShedPage(PageId id)
    {
        auto it = frames_.find(id);
        if (it == frames_.end() || it->second.buf_ == nullptr ||
            !it->second.flushed_ || it->second.pin_count_ != 0)
        {
            return false;
        }
        LruUnlink(id);
        frames_.erase(it);
        return true;
    }

    /**
     * @brief Pages partial eviction may actually take right now: resident,
     * clean, and unpinned (§8) — the base the 10 % policy is computed
     * against, so a fully dirty or fully pinned object correctly yields a
     * target of zero rather than one.
     */
    size_t EvictablePageCount() const
    {
        size_t n = 0;
        for (const auto &[id, slot] : frames_)
        {
            (void) id;
            if (slot.buf_ != nullptr && slot.flushed_ && slot.pin_count_ == 0)
            {
                ++n;
            }
        }
        return n;
    }

    /**
     * @brief Sheds up to `max_pages` pages from the cold end of the LRU,
     * skipping dirty and pinned ones — the §8 victim selection. Dirty pages
     * are skipped because the durability invariant is per page; pinned ones
     * because pins protect the working set of in-flight commands (§6).
     * @return the number of pages actually shed.
     */
    size_t ShedColdPages(size_t max_pages)
    {
        size_t shed = 0;
        PageId id = lru_tail_;
        while (shed < max_pages && id != kInvalidPageId)
        {
            auto it = frames_.find(id);
            assert(it != frames_.end() && "LRU names a non-resident page");
            // Step to the next colder-to-hotter candidate before any erase,
            // which invalidates this slot's links.
            PageId prev = it->second.lru_prev_;
            if (it->second.buf_ != nullptr && it->second.flushed_ &&
                it->second.pin_count_ == 0)
            {
                LruUnlink(id);
                frames_.erase(it);
                ++shed;
            }
            id = prev;
        }
        return shed;
    }

    /**
     * @brief The complete VOLATILE teardown of a deletion-retained block
     * (docs/08 §9): drops every resident frame, every §8 admission
     * reservation, the pending-fault set, every tx context, and the LRU —
     * keeping ONLY what the deletion fan-out reads: the page-id metadata
     * (next-id high-water, free ranges, pending deletes) plus the page
     * size. ForEachLivePageId/ForEachPendingDeleteId walk exactly that
     * metadata, never the frames, so the fan-out survives a total clear.
     *
     * Deliberately NOT pin-aware, unlike the shed paths: the caller has
     * already orphaned the entry's in-flight fetches and abandoned every
     * tx context, and a deleted object can never serve another read from
     * this block, so no pin has a live owner. An earlier version reused
     * AbandonAllTxContexts + a pin-respecting buffer release — but
     * AbandonAllTxContexts clears contexts WITHOUT decrementing the
     * per-slot aggregate pins (its contract is a payload swap, where the
     * slots die with the block), so pinned frames and all admission
     * reservations survived until the deletion checkpoint, charging the
     * shard heap indefinitely if that checkpoint stalled (a reported
     * defect).
     *
     * @return the number of resident frames dropped.
     */
    size_t TeardownForDeletion()
    {
        size_t dropped = frames_.size();
        frames_.clear();
        reserved_.clear();
        pending_faults_.clear();
        tx_contexts_.clear();
        lru_head_ = kInvalidPageId;
        lru_tail_ = kInvalidPageId;
        return dropped;
    }

    /**
     * @brief The §8 policy: shed 10 % of this object's evictable pages,
     * never fewer than one. The batch amortizes the visit — a paged object
     * is a single CcEntry, so one LRU sweep of the shard reaches it exactly
     * once. The floor of one guarantees forward progress for small objects,
     * where 10 % rounds to zero.
     * @return the number of pages shed; 0 means nothing was evictable.
     */
    size_t ShedByPolicy()
    {
        size_t evictable = EvictablePageCount();
        if (evictable == 0)
        {
            return 0;
        }
        return ShedColdPages(std::max<size_t>(1, evictable / 10));
    }

    /**
     * @brief Page ids currently on the LRU, cold end first. Test hook: the
     * shed order is a policy claim worth asserting directly.
     */
    std::vector<PageId> LruColdToHot() const
    {
        std::vector<PageId> out;
        for (PageId id = lru_tail_; id != kInvalidPageId;
             id = frames_.find(id)->second.lru_prev_)
        {
            out.push_back(id);
        }
        return out;
    }  // GCOVR_EXCL_LINE: unreachable dtor code (copy elision)

    /**
     * @brief Test/inspection hook: the per-page volatile state, or nullptr
     * if the page has no slot.
     */
    const PageSlot *SlotOf(PageId id) const
    {
        auto it = frames_.find(id);
        return it == frames_.end() ? nullptr : &it->second;
    }

    /**
     * @brief LRU structural check: forward and backward traversals agree,
     * every linked id is resident, and the list covers every resident page
     * exactly once.
     */
    // GCOVR_EXCL_START: the failure arms of this checker are unreachable
    // while the invariants hold — which passing tests prove. They exist to
    // catch future regressions, not to be covered.
    bool CheckLruInvariants() const
    {
        size_t forward = 0;
        PageId prev = kInvalidPageId;
        for (PageId id = lru_head_; id != kInvalidPageId;)
        {
            auto it = frames_.find(id);
            if (it == frames_.end() || it->second.lru_prev_ != prev)
            {
                return false;
            }
            prev = id;
            id = it->second.lru_next_;
            if (++forward > frames_.size())
            {
                return false;  // cycle
            }
        }
        if (prev != lru_tail_)
        {
            return false;
        }
        return forward == frames_.size();
    }
    // GCOVR_EXCL_STOP

    // ---- pins: BOTH views, updated together (§4) ------------------------

    void PinPage(PageId id)
    {
        auto it = frames_.find(id);
        assert(it != frames_.end());
        ++it->second.pin_count_;
    }

    void UnpinPage(PageId id)
    {
        auto it = frames_.find(id);
        if (it != frames_.end())
        {
            assert(it->second.pin_count_ > 0);
            --it->second.pin_count_;
        }
    }

    /**
     * @brief Creates `txn`'s fault context if it does not exist, so a fetch
     * completion has somewhere to record its pins. The wake record (parked
     * request, awaited count) is ENTRY-scoped and lives in the FetchHub, not
     * here.
     */
    void EnsureTxFaultContext(uint64_t txn)
    {
        tx_contexts_.try_emplace(txn);
    }

    /**
     * @brief Does this table hold a fault context for `txn`? The per-WAITER
     * routing test (§7): the object holding a txn's context is by
     * construction the one that faulted for it.
     */
    bool HasPageWaiter(uint64_t txn) const
    {
        return tx_contexts_.find(txn) != tx_contexts_.end();
    }

    /**
     * @brief Records a successful fetch for a waiting txn: pins the page
     * (aggregate) and remembers it in the txn's context (decomposition), so
     * the pin bridges the gap until the command re-runs (§6). No context —
     * the txn's fault state is gone (payload replaced, command finished) —
     * means nothing to pin.
     */
    void NotePageFetched(uint64_t txn, PageId page_id, bool success)
    {
        if (!success)
        {
            return;
        }
        auto it = tx_contexts_.find(txn);
        if (it == tx_contexts_.end())
        {
            return;
        }
        PinPage(page_id);
        it->second.pinned_.push_back(page_id);
    }

    /**
     * @brief Releases every pin `txn` holds and erases its context — at
     * commit from PostWriteCc, and on abort or term-change teardown.
     * Idempotent.
     */
    void ReleaseTxPins(uint64_t txn)
    {
        auto it = tx_contexts_.find(txn);
        if (it == tx_contexts_.end())
        {
            return;
        }
        for (PageId id : it->second.pinned_)
        {
            UnpinPage(id);
        }
        tx_contexts_.erase(it);
    }

    /**
     * @brief The §7 swap rule's context half: erases every fault context on
     * this (now superseded) block. No unpinning — the pins counted this
     * block's slots, which are being replaced wholesale. The parked requests
     * are not here either: wake records live on the entry's FetchHub, and
     * the swap takes them from there (TakeAllParked).
     */
    void AbandonAllTxContexts()
    {
        tx_contexts_.clear();
    }

    // ---- pending faults (§6) --------------------------------------------

    /**
     * @brief Records `id` as needed-but-missing. Deduplicated: a command
     * touching many fields on one page must fault it once. Const, mutating
     * only this volatile set, because fault-set computation happens inside
     * the object's const Execute methods (#509: ExecuteOn never mutates).
     */
    void RecordPendingFault(PageId id) const
    {
        for (PageId pending : pending_faults_)
        {
            if (pending == id)
            {
                return;
            }
        }
        pending_faults_.push_back(id);
    }

    /**
     * @brief Are any page ids recorded as missing by the last operation?
     * @return true iff a fault was recorded and not yet drained, which is
     * how a CommitOn reports that it did nothing (§10).
     */
    bool HasPendingFaults() const
    {
        return !pending_faults_.empty();
    }

    /**
     * @brief Moves out the pending fault set, ascending, and clears it. The
     * apply path drains this to issue the fetches (§6).
     * @return true if any ids were produced.
     */
    bool TakePendingFaults(std::vector<PageId> &out) const
    {
        if (pending_faults_.empty())
        {
            return false;
        }
        std::sort(pending_faults_.begin(), pending_faults_.end());
        out = std::move(pending_faults_);
        pending_faults_.clear();
        return true;
    }

    // ---- the metadata row's PAGE-MANAGER SECTION (§5) -------------------
    //
    // [section length varint][page_size u32][next_page_id u32]
    // [pending count varint][{first u32, count varint} ...]
    // freed_ts_ is NOT persisted: after a reload no flush is in flight, so
    // every reloaded range is drainable by the first checkpoint that writes
    // its Deletes; it reloads as 0. The length prefix makes the section
    // skippable by a type-unaware reader.

    size_t SerializedSize() const
    {
        size_t body = BodySize();
        return paged_detail::VarintSize(body) + body;
    }

    void SerializeMeta(std::string &out) const
    {
        namespace pd = paged_detail;
        pd::AppendVarint(out, BodySize());
        uint8_t u32[4];
        pd::StoreU32(u32, page_size_);
        out.append(reinterpret_cast<const char *>(u32), 4);
        pd::StoreU32(u32, next_page_id_);
        out.append(reinterpret_cast<const char *>(u32), 4);
        const auto &entries = pending_delete_.Entries();
        pd::AppendVarint(out, entries.size());
        for (const PendingDelete &e : entries)
        {
            pd::StoreU32(u32, e.range_.first_);
            out.append(reinterpret_cast<const char *>(u32), 4);
            pd::AppendVarint(out, e.range_.count_);
        }
    }

    /**
     * @brief Parses the page-manager section, resetting all frame state
     * (every page non-resident, §5). The free list is NOT rebuilt here — it
     * derives from the type's live set, which parses after this section; the
     * type calls RebuildFreeRanges once both are in.
     * @return false on malformed input; the table is then unusable.
     */
    bool DeserializeMeta(const char *buf, size_t len, size_t &offset)
    {
        namespace pd = paged_detail;
        frames_.clear();
        tx_contexts_.clear();
        pending_faults_.clear();
        free_ranges_.Clear();
        pending_delete_.Clear();
        lru_head_ = kInvalidPageId;
        lru_tail_ = kInvalidPageId;

        // Size-arithmetic bounds only: `len - offset` stays huge when len is
        // SIZE_MAX (the TxRecord::Deserialize path supplies no length), while
        // computing an end POINTER as base + SIZE_MAX would wrap.
        const uint8_t *base = reinterpret_cast<const uint8_t *>(buf);
        // The section-length varint is at most 10 bytes; bound its read by
        // what the caller allows.
        size_t varint_room = len - offset < 10 ? len - offset : 10;
        uint64_t body_len = 0;
        const uint8_t *p =
            pd::ReadVarint(base + offset, base + offset + varint_room,
                           body_len);
        if (p == nullptr)
        {
            return false;
        }
        offset = static_cast<size_t>(p - base);
        // Bound against what remains AFTER the length varint: comparing
        // against the pre-varint remainder would admit a body_len that
        // overruns `len` by up to the varint's own width.
        if (body_len > len - offset)
        {
            return false;
        }
        // From here the SECTION LENGTH is the bound — self-consistent even in
        // unbounded mode, and what makes the section skippable (§5).
        const size_t bend = offset + body_len;
        if (bend - offset < 8)
        {
            return false;
        }
        page_size_ = pd::LoadU32(base + offset);
        offset += 4;
        next_page_id_ = pd::LoadU32(base + offset);
        offset += 4;
        // Reject implausible values rather than acting on them: a corrupt
        // high-water would size the free-list bitmap and its scan.
        if (next_page_id_ > kMaxPageCount)
        {
            return false;
        }
        // A page size that is zero, absurd, or not a multiple of 8 cannot
        // have been written by this codec; every later size computation
        // (slot capacity, install byte count) trusts it.
        if (page_size_ < kMinPageSize || page_size_ > kMaxPageSize ||
            (page_size_ % 8) != 0)
        {
            return false;
        }
        uint64_t n = 0;
        p = pd::ReadVarint(base + offset, base + bend, n);
        if (p == nullptr)
        {
            return false;
        }
        offset = static_cast<size_t>(p - base);
        uint64_t prev_end = 0;
        for (uint64_t i = 0; i < n; ++i)
        {
            if (bend - offset < 4)
            {
                return false;
            }
            PageId first = pd::LoadU32(base + offset);
            offset += 4;
            uint64_t count = 0;
            p = pd::ReadVarint(base + offset, base + bend, count);
            if (p == nullptr || count == 0 || first < prev_end ||
                first + count > next_page_id_)
            {
                return false;
            }
            offset = static_cast<size_t>(p - base);
            prev_end = first + count;
            pending_delete_.Append(
                PageIdRange{first, static_cast<uint32_t>(count)}, 0);
        }
        return offset == bend;
    }

    /**
     * @brief Rebuilds the derived free list (§4: derived, not persisted) as
     * the complement of {live ∪ pending delete} over [0, next_page_id_).
     * `for_each_live_id` invokes its callback once per live id — the type
     * enumerates its structure (directory + large runs). Called exactly once,
     * after both metadata sections have parsed; from then on the partition
     * maintains itself through AllocatePageId/FreePage/OnFlushApplied.
     */
    template <typename Fn>
    bool RebuildFreeRanges(Fn &&for_each_live_id)
    {
        free_ranges_.Clear();
        // next_page_id_ is bounded at parse (kMaxPageCount), so this bitmap
        // is bounded too.
        std::vector<bool> used(next_page_id_, false);
        bool in_range = true;
        for_each_live_id(
            [&](PageId id)
            {
                // A live id at or past the high-water is a corrupt metadata
                // row. Report it; writing used[id] would be out of bounds,
                // and an assert would abort a node on bad STORAGE rather
                // than on a bug.
                if (id >= next_page_id_)
                {
                    in_range = false;
                    return;
                }
                used[id] = true;
            });
        if (!in_range)
        {
            return false;
        }
        for (const PendingDelete &pd : pending_delete_.Entries())
        {
            for (uint32_t i = 0; i < pd.range_.count_; ++i)
            {
                used[pd.range_.first_ + i] = true;
            }
        }
        PageId run_start = kInvalidPageId;
        for (PageId id = 0; id < next_page_id_; ++id)
        {
            if (!used[id])
            {
                if (run_start == kInvalidPageId)
                {
                    run_start = id;
                }
            }
            else if (run_start != kInvalidPageId)
            {
                free_ranges_.Insert(PageIdRange{run_start, id - run_start});
                run_start = kInvalidPageId;
            }
        }
        if (run_start != kInvalidPageId)
        {
            free_ranges_.Insert(
                PageIdRange{run_start, next_page_id_ - run_start});
        }
        return true;
    }

private:
    size_t BodySize() const
    {
        namespace pd = paged_detail;
        size_t body = 4 + 4;  // page_size + next_page_id
        const auto &entries = pending_delete_.Entries();
        body += pd::VarintSize(entries.size());
        for (const PendingDelete &e : entries)
        {
            body += 4 + pd::VarintSize(e.range_.count_);
        }
        return body;
    }

    /**
     * @brief Allocates one zeroed page buffer, subject to the §8 admission
     * gate.
     *
     * @return the buffer, or nullptr when the shard budget refuses it. EVERY
     * caller must handle null: that is what makes admission real rather than
     * advisory. (§4's aligned allocation — mi_heap aligned alloc + mi_free
     * deleter — arrives with the engine-resident allocator work; the gate is
     * independent of it.)
     */
    PageBuf AllocPage() const
    {
        if (!AdmitPageBytes(page_size_))
        {
            return nullptr;
        }
        return AllocPageUnchecked();
    }

    /**
     * @brief Allocates one zeroed page buffer, BYPASSING the §8 gate.
     *
     * For the paths that have no way to refuse and must not fail:
     *
     *  - the WRITE path (`CreateDirtyPage`, `BufForWrite`'s copy-on-write).
     *    `CommitOn` runs after the WAL and cannot fail; making these fallible
     *    without the §8 "blocked on memory" park would mean writing through a
     *    null buffer. Gating them belongs with that park, not before it.
     *  - `InstallPage`'s fallback, for bytes that arrived without going
     *    through admission. The memory holding those bytes is already spent;
     *    refusing the install would discard a completed read and send the
     *    command back to fault on the same page.
     *
     * Both are bounded by the §8 axiom: after a clean pass the budget holds
     * the largest object's dirty set plus one command's working allocations.
     *
     * @return the buffer; never null (allocation failure throws, as before).
     */
    PageBuf AllocPageUnchecked() const
    {
        return PageBuf(new uint8_t[page_size_]());
    }

    /**
     * @brief Moves `id` to the hot end of the per-object LRU, inserting it
     * if it is not yet linked. Idempotent and O(1).
     */
    void LruTouch(PageId id) const
    {
        if (lru_head_ == id)
        {
            return;
        }
        LruUnlink(id);
        auto it = frames_.find(id);
        assert(it != frames_.end());
        const PageSlot &slot = it->second;
        slot.lru_prev_ = kInvalidPageId;
        slot.lru_next_ = lru_head_;
        if (lru_head_ != kInvalidPageId)
        {
            frames_.find(lru_head_)->second.lru_prev_ = id;
        }
        lru_head_ = id;
        if (lru_tail_ == kInvalidPageId)
        {
            lru_tail_ = id;
        }
    }

    /**
     * @brief Detaches `id` from the LRU if linked. Must be called before any
     * erase from frames_, or the list would name a page that no longer
     * exists.
     */
    void LruUnlink(PageId id) const
    {
        auto it = frames_.find(id);
        if (it == frames_.end())
        {
            return;
        }
        const PageSlot &slot = it->second;
        const bool linked = slot.lru_prev_ != kInvalidPageId ||
                            slot.lru_next_ != kInvalidPageId ||
                            lru_head_ == id;
        if (!linked)
        {
            return;
        }
        if (slot.lru_prev_ != kInvalidPageId)
        {
            frames_.find(slot.lru_prev_)->second.lru_next_ = slot.lru_next_;
        }
        else
        {
            lru_head_ = slot.lru_next_;
        }
        if (slot.lru_next_ != kInvalidPageId)
        {
            frames_.find(slot.lru_next_)->second.lru_prev_ = slot.lru_prev_;
        }
        else
        {
            lru_tail_ = slot.lru_prev_;
        }
        slot.lru_prev_ = kInvalidPageId;
        slot.lru_next_ = kInvalidPageId;
    }

    std::unordered_map<PageId, PageSlot> frames_;
    absl::flat_hash_map<uint64_t, TxPageContext> tx_contexts_;
    // Page ids the current ExecuteOn found missing (§6). Mutable because
    // fault-set computation happens inside the object's const Execute
    // methods; volatile per-command state, never serialized, drained by the
    // apply path before the request parks.
    mutable std::vector<PageId> pending_faults_;
    // Page buffers CLAIMED at admission (§8) for pages whose fetch is in
    // flight, consumed by the matching install. Their existence is what makes
    // admission real: the memory a fault will need is already off the shard
    // budget before the fetch is issued, so nothing can overshoot between the
    // check and the arrival. Entries die with the payload if their fetch never
    // lands (superseded incarnation, term change), which bounds the hold by
    // the object's own live page count. Mutable for the same reason
    // pending_faults_ is: reservation happens inside const fault-set
    // computation.
    mutable std::unordered_map<PageId, PageBuf> reserved_;
    FreeRanges free_ranges_;
    PendingDeletes pending_delete_;
    PageId next_page_id_{0};
    uint32_t page_size_{0};
    // Hot and cold ends of the per-object eviction LRU (§8). Mutable for the
    // same reason PageSlot's links are: the read accessor is const.
    mutable PageId lru_head_{kInvalidPageId};
    mutable PageId lru_tail_{kInvalidPageId};
    // The provisional ts stamped on mutations: one CommitOn is one commit_ts
    // for all of its writes (§4). Starts at 1 — the engine's "exists, ts
    // unset" floor — never 0, which means *unknown*.
    uint64_t write_ts_{1};
};
}  // namespace txservice
