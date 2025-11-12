## Knowledge Base: Standby Message Buffer - Memory Bounded Implementation

### Metadata
- **Last Updated:** 2025-11-12
### 1. Executive Summary

Replaces the fixed-size count-based standby message buffer (`txservice_max_standby_lag` = 400,000 entries) with a memory-bounded queue that allocates entries on-demand and evicts oldest messages when memory limit is reached. This prevents unbounded memory growth while ensuring messages needed by followers or candidate subscribers are retained until no longer referenced.

### 2. Core Concepts & Definitions

- **Standby Forward Entry (`StandbyForwardEntry`)**: Contains a protobuf message (`remote::CcMessage`) with transaction data to forward to standby nodes. Each entry has a sequence ID and tracks memory usage.

- **History Queue (`history_standby_msg_`)**: Memory-bounded FIFO queue (`std::deque<std::unique_ptr<StandbyForwardEntry>>`) storing entries still needed for retry or candidate followers. Owns entry lifetime.

- **Sequence ID Map (`seq_id_to_entry_map_`)**: O(1) lookup map (`std::unordered_map<uint64_t, StandbyForwardEntry*>`) for finding entries by sequence ID during retry. Entries owned by history queue.

- **Candidate Follower**: Standby node that called `StandbyStartFollowing` but not yet `ResetStandbySequenceId`. Tracks `node_id → start_seq_id` to prevent freeing messages they'll need when subscribing.

- **Entry Ownership Flow**: cc entry → shard (when sending) → history queue (if still needed). Entries allocated on-demand via `std::make_unique<StandbyForwardEntry>()`, freed immediately when not referenced.

- **Memory Limit**: 10% of node memory limit per shard. Calculated as `(node_memory_limit_mb * MB) * 0.1 / core_cnt_`.

- **Entry Need Check**: Entry is needed if: (1) any follower's `last_sent_seq_id < seq_id` (retry needed), or (2) any candidate's `start_seq_id <= seq_id` (candidate needs it).

- **Immediate Freeing**: Entry freed as soon as not referenced by any follower or candidate. Checked after retry sending and when candidate removed.

### 3. Technical Architecture & Decisions

- **Why Memory-Bounded Instead of Count-Based**: Fixed-size buffer (`txservice_max_standby_lag`) wastes memory on small messages and can overflow on large ones. Memory-bounded approach adapts to actual message sizes and prevents unbounded growth.

- **Why On-Demand Allocation**: Preallocated pool (`standby_fwd_vec_`) wastes memory when not all entries are in use. On-demand allocation only uses memory for active messages.

- **Why Candidate Tracking**: New followers have a two-phase subscription (Phase 1: `StandbyStartFollowing` returns `start_seq_id`, Phase 2: `ResetStandbySequenceId` subscribes). Without tracking, messages between phases may be freed, causing subscription failure.

- **Why O(1) Operations Required**: All critical operations (allocate, send, find, remove) must be O(1) to maintain performance. Uses `std::deque` for O(1) front removal and `std::unordered_map` for O(1) lookup.

- **Why Immediate Freeing**: Entries freed immediately when not needed rather than waiting for eviction. Reduces memory pressure and ensures efficient resource usage.

- **Why Out-of-Sync Notification**: When retry mechanism can't find a message (evicted), sends out-of-sync message to notify standby to resubscribe. This handles the case where memory pressure evicted messages before all followers received them.

- **Key Dependencies**: 
  - `StandbyForwardEntry` structure (`tx_service/include/standby.h`)
  - `CcShard` class (`tx_service/include/cc/cc_shard.h`)
  - `CcEntry` and `KeyGapLockAndExtraData` for entry ownership (`tx_service/include/cc/cc_entry.h`, `tx_service/include/cc/non_blocking_lock.h`)
  - Stream sender for message delivery (`Sharder::Instance().GetCcStreamSender()`)

### 4. Implementation Blueprint

**File Structure:**
```
tx_service/include/cc/
  - cc_shard.h (data structures, method declarations)
  - cc_entry.h (SetForwardEntry, ReleaseForwardEntry)
  - non_blocking_lock.h (forward_entry_ as unique_ptr)
  - object_cc_map.h (allocation point, ownership transfer)
  - catalog_cc_map.h (catalog allocation point)

tx_service/src/cc/
  - cc_shard.cpp (ForwardStandbyMessage, ResendFailedForwardMessages, memory tracking, eviction)
  - cc_entry.cpp (SetForwardEntry, ReleaseForwardEntry implementations)
  - non_blocking_lock.cpp (SetForwardEntry, ReleaseForwardEntry implementations)

tx_service/src/remote/
  - cc_node_service.cpp (StandbyStartFollowing, ResetStandbySequenceId handlers)

tx_service/include/
  - standby.h (StandbyForwardEntry structure, Remove Reset() method)
```

**Core Code Patterns:**

**Entry Allocation (Phase 1):**
```cpp
// In object_cc_map.h when transaction needs forwarding:
auto forward_entry_ptr = std::make_unique<StandbyForwardEntry>();
StandbyForwardEntry *forward_entry = forward_entry_ptr.get();
cce->SetForwardEntry(std::move(forward_entry_ptr));  // Transfer ownership to cc entry
// Populate entry with data...
```

**Ownership Transfer to Shard (Phase 2):**
```cpp
// When message is ready to send:
std::unique_ptr<StandbyForwardEntry> entry_ptr = cce->ReleaseForwardEntry();
shard_->ForwardStandbyMessage(entry_ptr.release());  // Transfer ownership to shard
```

**Memory Size Calculation:**
```cpp
uint64_t CalculateEntryMemorySize(const StandbyForwardEntry *entry) const {
    // protobuf size + overhead (bool in_use_ + uint64_t sequence_id_)
    return entry->Message().ByteSizeLong() + sizeof(bool) + sizeof(uint64_t);
}
```

**Entry Need Check:**
```cpp
bool IsEntryNeeded(uint64_t seq_id) const {
    // Check retry needed: any follower's last_sent_seq_id < seq_id
    for (const auto &[node_id, last_sent_seq_id_and_term] : subscribed_standby_nodes_) {
        if (last_sent_seq_id_and_term.first < seq_id) return true;
    }
    // Check candidate needed: any candidate's start_seq_id <= seq_id
    for (const auto &[node_id, start_seq_id] : candidate_standby_nodes_) {
        if (start_seq_id <= seq_id) return true;
    }
    return false;
}
```

**History Queue Management (Phase 2):**
```cpp
// In ForwardStandbyMessage() after sending:
if (IsEntryNeeded(seq_id)) {
    StandbyForwardEntry *entry_raw = entry_ptr.get();
    history_standby_msg_.push_back(std::move(entry_ptr));  // Move ownership to queue
    seq_id_to_entry_map_[seq_id] = entry_raw;
    
    uint64_t entry_size = CalculateEntryMemorySize(entry_raw);
    total_standby_buffer_memory_usage_ += entry_size;
    
    // Evict if memory limit exceeded
    while (total_standby_buffer_memory_usage_ > standby_buffer_memory_limit_ && !history_standby_msg_.empty()) {
        std::unique_ptr<StandbyForwardEntry> oldest_entry = std::move(history_standby_msg_.front());
        history_standby_msg_.pop_front();
        uint64_t oldest_seq_id = oldest_entry->SequenceId();
        seq_id_to_entry_map_.erase(oldest_seq_id);
        total_standby_buffer_memory_usage_ -= CalculateEntryMemorySize(oldest_entry.get());
        // Clean up affected candidates (Phase 3)
        // Entry automatically freed when unique_ptr goes out of scope
    }
}
```

**Candidate Tracking (Phase 3):**
```cpp
// In StandbyStartFollowing handler:
ccs.AddCandidateStandby(node_id, start_seq_id);

// In ResetStandbySequenceId handler:
ccs.RemoveCandidateStandby(node_id);
ccs.AddSubscribedStandby(node_id, start_seq_id, standby_node_term);
ccs.CheckAndFreeUnneededEntries();  // Free entries no longer needed by this candidate
```

**Retry with Eviction Handling (Phase 4):**
```cpp
// In ResendFailedForwardMessages():
auto entry_it = seq_id_to_entry_map_.find(pending_seq_id);
if (entry_it != seq_id_to_entry_map_.end()) {
    // Send message...
} else {
    // Message evicted - send out-of-sync notification
    remote::CcMessage cc_msg;
    cc_msg.set_type(remote::CcMessage_MessageType::CcMessage_MessageType_KeyObjectStandbyForwardRequest);
    auto req = cc_msg.mutable_key_obj_standby_forward_req();
    req->set_forward_seq_grp(core_id_);
    req->set_forward_seq_id(next_forward_sequence_id_ - 1);
    req->set_primary_leader_term(Sharder::Instance().LeaderTerm(...));
    req->set_out_of_sync(true);
    stream_sender_->SendMessageToNode(node_id, cc_msg);
}
```

**Integration Points:**
- **Entry Allocation**: `object_cc_map.h:873` and `catalog_cc_map.h:804` - Replace `GetNextStandbyForwardEntry()` with `std::make_unique<StandbyForwardEntry>()`
- **Ownership Transfer**: `object_cc_map.h:951-952` - Use `ReleaseForwardEntry()` to get ownership, transfer to shard
- **Message Sending**: `cc_shard.cpp:2576-2638` (`ForwardStandbyMessage`) - Add need-check and history queue management
- **Retry Mechanism**: `cc_shard.cpp:2640-2735` (`ResendFailedForwardMessages`) - Replace circular buffer lookup with map lookup, handle evicted messages
- **Candidate Tracking**: `cc_node_service.cpp:1590-1642` (`StandbyStartFollowing`) and `cc_node_service.cpp:1817-1856` (`ResetStandbySequenceId`)

### 5. Data Models & APIs

**New Data Structures in CcShard:**
```cpp
// Memory-bounded queue (owns entries)
std::deque<std::unique_ptr<StandbyForwardEntry>> history_standby_msg_;

// O(1) lookup map (entries owned by queue)
std::unordered_map<uint64_t, StandbyForwardEntry *> seq_id_to_entry_map_;

// Candidate followers: node_id -> start_seq_id
std::unordered_map<uint32_t, uint64_t> candidate_standby_nodes_;

// Memory tracking
uint64_t total_standby_buffer_memory_usage_{0};
uint64_t standby_buffer_memory_limit_{0};  // 10% of node memory limit per shard
```

**Updated Ownership Model:**
```cpp
// In KeyGapLockAndExtraData (non_blocking_lock.h):
std::unique_ptr<StandbyForwardEntry> forward_entry_;  // Changed from raw pointer

void SetForwardEntry(std::unique_ptr<StandbyForwardEntry> entry);
std::unique_ptr<StandbyForwardEntry> ReleaseForwardEntry();
StandbyForwardEntry *ForwardEntry();  // Returns raw pointer for access
```

**New Methods in CcShard:**
```cpp
// Public methods:
void AddCandidateStandby(uint32_t node_id, uint64_t start_seq_id);
void RemoveCandidateStandby(uint32_t node_id);
void CheckAndFreeUnneededEntries();

// Private methods:
uint64_t CalculateEntryMemorySize(const StandbyForwardEntry *entry) const;
bool IsEntryNeeded(uint64_t seq_id) const;
```

**Removed Methods:**
- `GetNextStandbyForwardEntry()` - Replaced with direct `std::make_unique<StandbyForwardEntry>()` allocation
- `StandbyForwardEntry::Reset()` - Entries no longer reused, deleted immediately when not needed

### 6. Open Questions & Known Risks

- **Memory Calculation Accuracy**: Uses `msg.ByteSizeLong() + 9 bytes` overhead. Protobuf allocates content on heap, so actual memory may vary. Consider caching calculation per entry.

- **Concurrency**: Buffer is per-shard, single-threaded access only. No locks needed, but verify no cross-shard access.

- **Eviction Under High Load**: When memory limit reached, oldest messages evicted even if followers haven't received them. Retry mechanism handles this via out-of-sync notification, but may cause resubscription churn.

- **Candidate Cleanup on Eviction**: When message evicted, candidates with `start_seq_id <= evicted_seq_id` are removed. This prevents candidates from referencing non-existent messages, but may cause subscription failure if candidate hasn't subscribed yet.

- **Memory Limit Calculation**: Uses 10% of node memory limit per shard. May need tuning based on production workload characteristics.

- **Backward Compatibility**: Old followers will work but may receive out-of-sync notifications if messages are evicted. No data migration needed (runtime change only).

### 7. Keywords

standby message buffer, memory bounded, memory limit, eviction, candidate follower, entry ownership, sequence ID, retry mechanism, out-of-sync, ForwardStandbyMessage, ResendFailedForwardMessages, StandbyStartFollowing, ResetStandbySequenceId, history queue, memory tracking, immediate freeing, on-demand allocation

