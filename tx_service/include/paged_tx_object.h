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

// PagedTxObject: the engine's SEAM to a paged large object, and the shared
// machinery behind it (eloqkv docs/08-paged-objects.md §4 "Ownership: two
// layers"). Every engine touchpoint reaches a paged payload only through this
// class, via TxObject::AsPaged() — the representation query (§6 "Dispatch
// prerequisite"): the replay/standby drain (TakePendingFaults,
// EnsureTxFaultContext), fetch completion (InstallPage, NotePageFetched), the
// §7 swap rule (AbandonAllTxContexts), the shard clean pass (ShedCleanPages),
// the checkpoint (ExportPagedFlush, OnPagedFlushApplied), and commit/abort
// (ReleaseTxPins, StampWrites).
//
// It is deliberately NOT a pure interface with a separate implementation
// mixin. The whole point of the shared layer is that the pin/fault/shed
// protocol has exactly ONE implementation — a contract that invites a second
// one invites a second pin-accounting bug farm — so the seam and the
// machinery are the same class: it CONTAINS the page manager
// (PageFrameTable) and implements the engine-facing virtuals over it, once.
// Only the type hooks are pure. A type that ever genuinely needed different
// behavior could still override (the virtuals stay virtual); none should.
//
// Holding state here is safe because a protocol layer's own object base
// (e.g. EloqKV's RedisEloqObject) is TxObject plus method defaults with no
// data members, and PagedTxObject does not derive from TxObject — so the
// multiple inheritance has no diamond, and the concrete object's state is
// exactly frames_ plus the type's own members.
//
// The §4/§7 scoping rule the split of state follows: page STATE (frames,
// pins, per-txn fault contexts) is PAYLOAD-scoped and lives here, dying with
// this block at a swap; I/O REQUESTS and WHO IS WAITING are ENTRY-scoped and
// live in the FetchHub on the entry's lock structure (cc/page_fetch.h).

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "page_frame_table.h"
#include "page_key_codec.h"

namespace txservice
{
/**
 * @brief One page row inside a paged flush (docs/08 §9): a dirty page BY
 * REFERENCE, or a pending delete. A null buf_ means "delete that page row" —
 * the same convention SetNonVersionedPayload(nullptr) already uses. (Note
 * the opposite sense in the object's resident-page slots, where null means
 * "not resident".)
 */
struct PagedFlushPage
{
    uint32_t page_id_{0};
    PageRowKind kind_{PageRowKind::HashPage};
    // Shared with the object's resident slot: §7's COW rule makes this safe
    // with no extra mechanism — a page the flush worker holds has
    // use_count() >= 2, so a concurrent write copies rather than mutating
    // the buffer mid-write. uint8_t rather than std::byte because the page
    // buffer is byte storage the store handler views as char — arithmetic
    // byte type, not an opaque one.
    std::shared_ptr<const uint8_t[]> buf_;
};

/**
 * @brief The §9 indivisible flush unit of one paged object: the metadata row
 * plus every dirty page plus every pending delete, packed into a single
 * FlushRecord payload so the batching layer structurally cannot split it.
 * Only metadata_ is materialized; page bytes travel by reference, which is
 * what keeps the checkpoint scan heap flat.
 */
struct PagedObjectFlush
{
    // The full metadata ROW value (object type tag [+ ttl] + version + both
    // metadata sections, §5) — what the object's Serialize() produces.
    std::string metadata_;
    // Row length of every non-null page (per object, from its metadata §4).
    uint32_t page_size_{0};
    // The store-TTL attribute for the metadata row: logical deadline plus
    // the protocol layer's slack, or 0 for none (docs/08 §9 "TTL and
    // store-side reclamation"). Page rows NEVER carry a TTL attribute; the
    // flush path hardcodes 0 for them.
    uint64_t metadata_row_ttl_{0};
    // Dirty pages and pending deletes, sorted by page id (§9: lets a
    // handler coalesce runs of consecutive deletes if the backend offers a
    // range delete).
    std::vector<PagedFlushPage> pages_;
};

/**
 * @brief The engine seam and shared machinery of a paged payload. All calls
 * run on the owning shard core.
 */
class PagedTxObject
{
public:
    PagedTxObject() = default;
    // Explicit because the user-declared destructor would otherwise suppress
    // the implicit MOVE operations, silently degrading a derived object's
    // move into a frames copy. Copying frames_ shares page buffers (§7 COW)
    // and drops volatile per-command state — see PageFrameTable's copy ctor.
    PagedTxObject(const PagedTxObject &) = default;
    PagedTxObject(PagedTxObject &&) noexcept = default;
    PagedTxObject &operator=(const PagedTxObject &) = default;
    PagedTxObject &operator=(PagedTxObject &&) noexcept = default;
    virtual ~PagedTxObject() = default;

    /**
     * @brief Installs fetched page bytes as page `page_id` (§13 BackFill
     * page mode: never touches the entry's commit ts or record status). The
     * page takes the store row's commit ts as its last_modified_ts_ and is
     * marked flushed (§4).
     * @return false if the id is no longer live — a benign discard (the page
     * was freed while the fetch was in flight); the caller drops the bytes.
     */
    virtual bool InstallPage(uint32_t page_id,
                             std::string_view bytes,
                             uint64_t row_commit_ts)
    {
        if (!ValidatePageImage(page_id, bytes))
        {
            // A rejected image still ends the fetch the §8 admission claimed
            // a buffer for; release it or it lingers until the payload dies.
            frames_.DropReserved(page_id);
            return false;
        }
        return frames_.InstallPage(page_id, bytes, row_commit_ts);
    }

    /**
     * @brief Installs a page by sharing an already-built buffer rather than
     * copying it (§7). One fetch completion can feed both the committed
     * payload and a transaction's dirty copy; copy-on-write splits the
     * buffer when either side later writes that page.
     * @return false if the id is not live here or the size does not match.
     */
    virtual bool InstallPageShared(uint32_t page_id,
                                   std::shared_ptr<uint8_t[]> buf,
                                   size_t buf_size,
                                   uint64_t row_commit_ts)
    {
        if (buf == nullptr ||
            !ValidatePageImage(
                page_id,
                std::string_view(reinterpret_cast<const char *>(buf.get()),
                                 buf_size)))
        {
            // A rejected image still ends the fetch the §8 admission claimed
            // a buffer for (frames_.InstallPageShared would have consumed it
            // on any outcome; this refusal happens before it runs).
            frames_.DropReserved(page_id);
            return false;
        }
        return frames_.InstallPageShared(
            page_id, std::move(buf), buf_size, row_commit_ts);
    }

    /**
     * @brief Is `page_id` part of the object's logical content (§4's Live
     * state), whether or not its bytes are in memory? Distinct from
     * IsPageResident: a live page may be evicted, and a resident page is
     * always live. A fetch that finds NO store row for a live id is
     * corruption (§13): every live id a checkpoint flushed has a row.
     */
    virtual bool IsPageLive(uint32_t page_id) const
    {
        return frames_.IsLive(page_id);
    }

    /**
     * @brief Is `page_id`'s buffer in memory?
     * @return true iff the bytes are present, so a command may read or write
     *         them without faulting. False means live-but-evicted OR not
     *         live at all — callers that need to tell those apart ask
     *         IsPageLive.
     */
    virtual bool IsPageResident(uint32_t page_id) const
    {
        return frames_.IsResident(page_id);
    }

    /**
     * @brief Is every live page resident?
     * @return true iff no command executing on this object can fault (§6).
     *         Used to scope the deferred-acquisition path: a fully resident
     *         paged object cannot yield, so it keeps today's
     *         acquire-then-execute sequence and pays nothing for machinery
     *         it will not use.
     */
    virtual bool IsFullyResident() const
    {
        return frames_.ResidentPageCount() == frames_.LivePageCount();
    }

    /**
     * @brief Moves out the page ids the last ExecuteOn found missing, in
     * ascending order, and clears the pending set.
     *
     * This is how a yield reaches the engine. The command computes its fault
     * set against the metadata (§3) and records whatever is not resident;
     * ExecuteOn then returns ExecResult::Yield having done nothing else. The
     * apply path — which holds the key and the shard, neither of which the
     * object has (§4 "the object does not know its own key") — drains this
     * set and issues the fetches. Inverting it this way keeps I/O out of the
     * object and leaves TxCommand::ExecuteOn's signature untouched, so no
     * monolithic command changes.
     *
     * @return true if any ids were produced.
     */
    virtual bool TakePendingFaults(std::vector<uint32_t> &out)
    {
        return frames_.TakePendingFaults(out);
    }

    /**
     * @brief Claims the page buffers this fault set will need, before any
     * fetch is issued (docs/08 §8 admission).
     *
     * All-or-nothing: a refusal leaves nothing claimed by this attempt, so a
     * caller that goes on to wait is not holding memory hostage while waiting
     * for memory.
     *
     * @return true when every page in `ids` has a buffer waiting; false when
     * the shard budget refused, and the caller must retry the whole set.
     */
    virtual bool ReserveFaultBuffers(const std::vector<uint32_t> &ids) const
    {
        return frames_.ReservePageBuffers(ids);
    }

    /**
     * @brief Takes the §8-admitted buffer for `page_id`, to become the
     * CANONICAL fetched buffer the completion installs (shared into every
     * applicable payload). Null when no reservation exists — the ungated
     * paths (the drain) and re-faults after a swap.
     */
    PageBuf TakeReservedBuffer(uint32_t page_id) const
    {
        return frames_.TakeReserved(page_id);
    }

    /**
     * @brief Releases the §8 reservation for `page_id` unused — the fetch it
     * was claimed for errored, found no row, or completed under a dead term.
     * Idempotent.
     */
    void DropPageReservation(uint32_t page_id) const
    {
        frames_.DropReserved(page_id);
    }

    /** @brief This object's page size in bytes (from its own metadata, §4). */
    uint32_t PageSizeBytes() const
    {
        return frames_.PageSize();
    }

    /** @brief Buffers claimed by §8 admission and not yet installed or
     * dropped. Diagnostic: tests assert it returns to zero on every
     * completion outcome. */
    size_t ReservedCount() const
    {
        return frames_.ReservedCount();
    }

    /**
     * @brief Could a fault set of `page_count` pages be admitted on an
     * otherwise-empty shard?
     *
     * Separates "wait for reclaim" from "impossible": retrying the latter
     * would stall the command forever, so it must become an error instead.
     *
     * @return true if the set is within the shard's capacity in principle.
     */
    virtual bool FaultSetCanEverFit(size_t page_count) const
    {
        return PageFrameTable::FaultSetCanEverFit(page_count,
                                                  frames_.PageSize());
    }

    /**
     * @brief Did the operation that just ran touch a non-resident page?
     * @return true iff faults are pending. This is the "not ready" channel
     *         of §10: CommitOn returns TxObject* and has no in-band way to
     *         say it did nothing. A true here means the object is UNCHANGED
     *         and the command must be retried once the recorded pages are
     *         fetched.
     */
    virtual bool HasPendingFaults() const
    {
        return frames_.HasPendingFaults();
    }

    /**
     * @brief Does this payload hold a fault context for `txn`?
     * @return true iff THIS object is the one `txn` faulted on, which is the
     *         per-WAITER routing test (§7): fetches coalesce, so one fetch
     *         can serve a reader waiting on the committed payload and a
     *         writer waiting on its dirty copy, and the object holding a
     *         txn's context is by construction the one that faulted for it.
     *         False means route the waiter elsewhere, not that the waiter is
     *         stale.
     */
    virtual bool HasPageWaiter(uint64_t txn) const
    {
        return frames_.HasPageWaiter(txn);
    }

    /**
     * @brief Ensures a per-txn fault context exists, so a later fetch
     * completion has somewhere to record its pins. The parked REQUEST is
     * registered on the entry's FetchHub, not here.
     */
    virtual void EnsureTxFaultContext(uint64_t txn)
    {
        frames_.EnsureTxFaultContext(txn);
    }

    /**
     * @brief Records the outcome of a page fetch this txn was waiting on:
     * pins the page for the txn on success (the pin bridges the gap until
     * the command re-runs, §6), and nothing more. PAGE-scoped state only —
     * the wake record lives on the ENTRY in FetchHub.
     */
    virtual void NotePageFetched(uint64_t txn, uint32_t page_id, bool success)
    {
        frames_.NotePageFetched(txn, page_id, success);
    }

    /**
     * @brief Assigns `commit_ts` to every page this object has dirty, at the
     * point the payload is installed as committed (§4). Writes carry only a
     * provisional timestamp before this — CommitOn receives no commit ts,
     * and needs none, because the uncommitted dirty payload is invisible to
     * the checkpoint. This is the first observable moment.
     */
    virtual void StampWrites(uint64_t commit_ts)
    {
        frames_.StampWrites(commit_ts);
    }

    /**
     * @brief The §7 swap rule's context half: erases every fault context on
     * this (now superseded) block. Pins die with the contexts — they counted
     * slots of this block, and the block is being replaced. The parked
     * requests are NOT here: wake records live on the entry's FetchHub
     * precisely so they survive payload replacement, and the swap takes them
     * with FetchHub::TakeAllParked and re-enqueues them.
     */
    virtual void AbandonAllTxContexts()
    {
        frames_.AbandonAllTxContexts();
    }

    /**
     * @brief Marks this block as a logically DELETED object retained only so
     * the deletion flush can read its page-id list (docs/08 §9, §16).
     *
     * The live commit path records deletion in the entry's status while
     * keeping the payload, but the replay/standby drain and its callers
     * derive status from payload NULLNESS — so a retained block would read
     * as Normal there without this flag (a reported defect: a replayed
     * deletion emitted only the metadata-row delete and orphaned every page
     * row). CcEntry::DrainedPayloadStatus consults it; a marked block is
     * treated as absent by later drained commands, so a replayed recreate
     * replaces it (the accepted §14 replacement-orphan case) instead of
     * mutating a dead object.
     */
    void MarkDeletionRetained()
    {
        deletion_retained_ = true;
    }

    /** @brief Whether this block is a retained deleted object (see above). */
    bool IsDeletionRetained() const
    {
        return deletion_retained_;
    }

    /**
     * @brief Releases every pin `txn` holds and erases its fault context —
     * at commit from PostWriteCc, and on abort or term-change teardown.
     * Idempotent. Pins live here keyed by tx number rather than on the
     * ApplyCc (§4): an ApplyCc is recycled at SetFinished(), but a write
     * transaction's pins must survive the WAL gap to CommitOn, and the only
     * handle that phase has is the tx number.
     */
    virtual void ReleaseTxPins(uint64_t txn)
    {
        frames_.ReleaseTxPins(txn);
    }

    /**
     * @brief Projects this object's §9 flush unit, on the shard core at
     * export time. Normal export: the metadata row + every DIRTY page (by
     * reference) + every pending-delete range (null buffers). Deletion
     * export (`for_deletion`): every LIVE page id with a null buffer + the
     * pending deletes — the whole-object delete fan-out, which needs no page
     * resident (§9).
     */
    virtual PagedObjectFlush ExportPagedFlush(bool for_deletion) const
    {
        PagedObjectFlush out;
        out.page_size_ = frames_.PageSize();
        PageRowKind kind = PageKind();
        if (for_deletion)
        {
            frames_.ForEachLivePageId(
                [&](PageId id) { out.pages_.push_back({id, kind, nullptr}); });
            frames_.ForEachPendingDeleteId(
                [&](PageId id) { out.pages_.push_back({id, kind, nullptr}); });
            return out;
        }
        // The metadata row is rewritten in EVERY dirty cycle (§4): its
        // commit_ts is the object's durable replay watermark, so skipping it
        // on a value-only update would let a page's stored ts outrun the
        // metadata's and replay would re-apply a non-idempotent command.
        SerializeMetadataRow(out.metadata_);
        out.metadata_row_ttl_ = MetadataRowTtl();
        // Dirty pages by reference, ascending — no page bytes are copied
        // (§9); COW makes the shared buffer safe for the worker thread (§7).
        frames_.ForEachDirtyPage(
            [&](PageId id, const PageBuf &buf)
            { out.pages_.push_back({id, kind, buf}); });
        frames_.ForEachPendingDeleteId(
            [&](PageId id) { out.pages_.push_back({id, kind, nullptr}); });
        return out;
    }  // GCOVR_EXCL_LINE: unreachable dtor code (copy elision)

    /**
     * @brief The post-flush callback (§9), fully generic: marks every page
     * whose last_modified_ts_ <= flushed_commit_ts as flushed — the per-page
     * transcription of EntryInfo::SetCkptTs's own guard — and drains pending
     * deletes whose freed_ts_ <= flushed_commit_ts to the free list.
     *
     * Guarding by timestamp rather than by exported page ids is equivalent
     * and simpler: every page dirty at export was exported (all-or-nothing)
     * and is dominated by the flushed commit ts (§9's invariant), while
     * anything written, freed, or created after the export — including a
     * whole replacement incarnation after a §7 swap — carries a newer ts and
     * fails the guard. That is also why the caller may invoke this on
     * whatever payload is current without identity checks.
     */
    virtual void OnPagedFlushApplied(uint64_t flushed_commit_ts)
    {
        frames_.OnFlushApplied(flushed_commit_ts);
    }

    /**
     * @brief Partial eviction (§8): shed a fraction of this object's clean,
     * unpinned pages from the cold end of its internal LRU, keeping the
     * metadata so the object stays routable and its pages re-fetchable.
     * Called by the shard's clean pass instead of freeing the whole CcEntry.
     * @return the number of pages shed; 0 means nothing was evictable.
     */
    virtual size_t ShedCleanPages()
    {
        return frames_.ShedByPolicy();
    }

    /**
     * @brief The complete volatile teardown of a deletion-retained block:
     * frames, admission reservations, pending faults, tx contexts, LRU —
     * keeping only the page-id metadata the deletion fan-out reads (§9).
     * See PageFrameTable::TeardownForDeletion.
     * @return the number of resident frames dropped.
     */
    virtual size_t TeardownForDeletion()
    {
        return frames_.TeardownForDeletion();
    }

    /**
     * @brief How many of this object's pages have their bytes in memory?
     * @return the resident page count; 0 means the object is metadata-only —
     *         still routable, but with nothing left to shed, so the entry
     *         becomes an ordinary whole-entry eviction candidate (§8's
     *         terminal state).
     */
    virtual size_t ResidentPageCount() const
    {
        return frames_.ResidentPageCount();
    }

    /**
     * @brief Read access to the page manager, for tests and inspection.
     */
    const PageFrameTable &Frames() const
    {
        return frames_;
    }

    /**
     * @brief Mutable access to the page manager — a TEST hook (driving shed,
     * install, and flush transitions directly). Production code inside the
     * type reaches frames_ as a protected member; engine code goes through
     * the virtuals above.
     */
    PageFrameTable &MutableFrames()
    {
        return frames_;
    }

protected:
    // ---- type hooks — all a concrete paged type implements besides its
    // layout: the "header" that interprets pages (its metadata) and the row
    // it persists.

    /**
     * @brief Serializes the FULL metadata row value — envelope (type tag
     * [+ ttl] + version) plus both metadata sections (§5) — exactly what a
     * store read of the row must hand back to Deserialize.
     */
    /**
     * @brief Is this byte image a well-formed page `page_id` OF THIS OBJECT?
     *
     * Called before any install, so a corrupt or mismatched store row is a
     * deterministic rejection (the fetch reports a corrupted object) rather
     * than out-of-bounds reads inside the type's accessors. The type checks
     * its own page format and cross-checks the image against its metadata —
     * for the hash: layout/framing via PageView::ValidateImage, the entry
     * count the metadata records for this page, the page's local depth, and
     * that every entry actually ROUTES to page_id, which is what detects a
     * valid page of some OTHER page id being served under this one.
     *
     * The base accepts: the generic layer cannot know the format. The size
     * and liveness checks stay in PageFrameTable, which owns those facts.
     */
    virtual bool ValidatePageImage(uint32_t page_id,
                                   std::string_view bytes) const
    {
        (void) page_id;
        (void) bytes;
        return true;
    }

    virtual void SerializeMetadataRow(std::string &out) const = 0;

    /**
     * @brief The page-row kind this type's pages are stored under (§5).
     */
    virtual PageRowKind PageKind() const = 0;

    /**
     * @brief The metadata row's store-TTL attribute (§9 interim scheme):
     * logical deadline plus slack, or 0 for "no TTL". A TTL twin overrides.
     */
    virtual uint64_t MetadataRowTtl() const
    {
        return 0;
    }

    // The page manager. Protected: the type's layout code drives it
    // directly (buffer access, id allocation, dirty marking).
    PageFrameTable frames_;
    // See MarkDeletionRetained.
    bool deletion_retained_{false};
};
}  // namespace txservice
