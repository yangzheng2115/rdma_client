// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <type_traits>
#include <vector>
#include <map>
#include <iostream>
#include "device/file_system_disk.h"

#include "alloc.h"
#include "checkpoint_locks.h"
#include "checkpoint_state.h"
#include "constants.h"
#include "gc_state.h"
#include "grow_state.h"
#include "guid.h"
#include "hash_table.h"
#include "internal_contexts.h"
#include "key_hash.h"
#include "malloc_fixed_page_size.h"
#include "persistent_memory_malloc.h"
#include "record.h"
#include "recovery_status.h"
#include "state_transitions.h"
#include "status.h"
#include "utility.h"

using namespace std;
using namespace std::chrono_literals;

/// The FASTER key-value store, and related classes.

namespace FASTER {
namespace core {

class alignas(Constants::kCacheLineBytes) ThreadContext {
public:
    ThreadContext()
            : contexts_{}, cur_{0} {
    }

    inline const ExecutionContext &cur() const {
        return contexts_[cur_];
    }

    inline ExecutionContext &cur() {
        return contexts_[cur_];
    }

    inline const ExecutionContext &prev() const {
        return contexts_[(cur_ + 1) % 2];
    }

    inline ExecutionContext &prev() {
        return contexts_[(cur_ + 1) % 2];
    }

    inline void swap() {
        cur_ = (cur_ + 1) % 2;
    }

private:
    ExecutionContext contexts_[2];
    uint8_t cur_;
};

static_assert(sizeof(ThreadContext) == 448, "sizeof(ThreadContext) != 448");

/// The FASTER key-value store.
template<class K, class V, class D>
class FasterKv {
public:
    typedef FasterKv<K, V, D> faster_t;

    /// Keys and values stored in this key-value store.
    typedef K key_t;
    typedef V value_t;

    typedef D disk_t;
    typedef typename D::file_t file_t;
    typedef typename D::log_file_t log_file_t;

    typedef PersistentMemoryMalloc<disk_t> hlog_t;

    /// Contexts that have been deep-copied, for async continuations, and must be accessed via
    /// virtual function calls.
    typedef AsyncPendingReadContext<key_t> async_pending_read_context_t;
    typedef AsyncPendingUpsertContext<key_t> async_pending_upsert_context_t;
    typedef AsyncPendingDeleteContext<key_t> async_pending_delete_context_t;
    typedef AsyncPendingRmwContext<key_t> async_pending_rmw_context_t;

    FasterKv(uint64_t table_size, uint64_t log_size, const std::string &filename,
             double log_mutable_fraction = 0.9)
            : min_table_size_{table_size}, disk{filename, epoch_},
              hlog{log_size, epoch_, disk, disk.log(), log_mutable_fraction, 0},
              system_state_{Action::None, Phase::REST, 1}, num_pending_ios{0} {
        // ,system_state_{Action::None, Phase::REST, 1},num_pending_ios{0}
        // hlog{log_size, epoch_, disk, disk.log(), log_mutable_fraction,1},
        //hlog_t a[4];
        //for(uint32_t i=0;i<4;i++){
        static hlog_t a(log_size, epoch_, disk, disk.tlog(0), log_mutable_fraction, 0);
        //thlog[0] = new hlog_t(log_size, epoch_, disk, disk.tlog(0), log_mutable_fraction, 0);
        thlog[0] = &a;
        Address ad = thlog[0]->head_address.load();
        static hlog_t b(log_size, epoch_, disk, disk.tlog(1), log_mutable_fraction, 1);
        thlog[1] = &b;
        static hlog_t c(log_size, epoch_, disk, disk.tlog(2), log_mutable_fraction, 2);
        thlog[2] = &c;
        static hlog_t d(log_size, epoch_, disk, disk.tlog(3), log_mutable_fraction, 3);
        thlog[3] = &d;
        static hlog_t d1(log_size, epoch_, disk, disk.tlog(4), log_mutable_fraction, 4);
        thlog[4] = &d1;
        static hlog_t d2(log_size, epoch_, disk, disk.tlog(5), log_mutable_fraction, 5);
        thlog[5] = &d2;
        static hlog_t d3(log_size, epoch_, disk, disk.tlog(6), log_mutable_fraction, 6);
        thlog[6] = &d3;
        static hlog_t d4(log_size, epoch_, disk, disk.tlog(7), log_mutable_fraction, 7);
        thlog[7] = &d4;
        static hlog_t d5(log_size, epoch_, disk, disk.tlog(8), log_mutable_fraction, 8);
        thlog[8] = &d5;
        static hlog_t d6(log_size, epoch_, disk, disk.tlog(9), log_mutable_fraction, 9);
        thlog[9] = &d6;
        static hlog_t d7(log_size, epoch_, disk, disk.tlog(10), log_mutable_fraction, 10);
        thlog[10] = &d7;
        static hlog_t d8(log_size, epoch_, disk, disk.tlog(11), log_mutable_fraction, 11);
        thlog[11] = &d8;
        static hlog_t d9(log_size, epoch_, disk, disk.tlog(12), log_mutable_fraction, 12);
        thlog[12] = &d9;
        static hlog_t d10(log_size, epoch_, disk, disk.tlog(13), log_mutable_fraction, 13);
        thlog[13] = &d10;
        static hlog_t d11(log_size, epoch_, disk, disk.tlog(14), log_mutable_fraction, 14);
        thlog[14] = &d11;
        static hlog_t d12(log_size, epoch_, disk, disk.tlog(15), log_mutable_fraction, 15);
        thlog[15] = &d12;
        static hlog_t d13(log_size, epoch_, disk, disk.tlog(16), log_mutable_fraction, 16);
        thlog[16] = &d13;
        static hlog_t d14(log_size, epoch_, disk, disk.tlog(17), log_mutable_fraction, 17);
        thlog[17] = &d14;
        static hlog_t d15(log_size, epoch_, disk, disk.tlog(18), log_mutable_fraction, 18);
        thlog[18] = &d15;
        static hlog_t d16(log_size, epoch_, disk, disk.tlog(19), log_mutable_fraction, 19);
        thlog[19] = &d16;
        static hlog_t d17(log_size, epoch_, disk, disk.tlog(20), log_mutable_fraction, 20);
        thlog[20] = &d17;
        static hlog_t d18(log_size, epoch_, disk, disk.tlog(21), log_mutable_fraction, 21);
        thlog[21] = &d18;
        static hlog_t d19(log_size, epoch_, disk, disk.tlog(22), log_mutable_fraction, 22);
        thlog[22] = &d19;
        static hlog_t d20(log_size, epoch_, disk, disk.tlog(23), log_mutable_fraction, 23);
        thlog[23] = &d20;
        static hlog_t d21(log_size, epoch_, disk, disk.tlog(24), log_mutable_fraction, 24);
        thlog[24] = &d21;
        static hlog_t d22(log_size, epoch_, disk, disk.tlog(25), log_mutable_fraction, 25);
        thlog[25] = &d22;
        static hlog_t d23(log_size, epoch_, disk, disk.tlog(26), log_mutable_fraction, 26);
        thlog[26] = &d23;
        static hlog_t d24(log_size, epoch_, disk, disk.tlog(27), log_mutable_fraction, 27);
        thlog[27] = &d24;
        static hlog_t d25(log_size, epoch_, disk, disk.tlog(28), log_mutable_fraction, 28);
        thlog[28] = &d25;
        static hlog_t d26(log_size, epoch_, disk, disk.tlog(29), log_mutable_fraction, 29);
        thlog[29] = &d26;
        static hlog_t d27(log_size, epoch_, disk, disk.tlog(30), log_mutable_fraction, 30);
        thlog[30] = &d27;
        static hlog_t d28(log_size, epoch_, disk, disk.tlog(31), log_mutable_fraction, 31);
        thlog[31] = &d28;
        static hlog_t d29(log_size, epoch_, disk, disk.tlog(32), log_mutable_fraction, 32);
        thlog[32] = &d29;
        static hlog_t d30(log_size, epoch_, disk, disk.tlog(33), log_mutable_fraction, 33);
        thlog[33] = &d30;
        static hlog_t d31(log_size, epoch_, disk, disk.tlog(34), log_mutable_fraction, 34);
        thlog[34] = &d31;
        static hlog_t d32(log_size, epoch_, disk, disk.tlog(35), log_mutable_fraction, 35);
        thlog[35] = &d32;
        static hlog_t d33(log_size, epoch_, disk, disk.tlog(36), log_mutable_fraction, 36);
        thlog[36] = &d33;
        static hlog_t d34(log_size, epoch_, disk, disk.tlog(37), log_mutable_fraction, 37);
        thlog[37] = &d34;
        static hlog_t d35(log_size, epoch_, disk, disk.tlog(38), log_mutable_fraction, 38);
        thlog[38] = &d35;
        static hlog_t d36(log_size, epoch_, disk, disk.tlog(39), log_mutable_fraction, 39);
        thlog[39] = &d36;
        if (!Utility::IsPowerOfTwo(table_size)) {
            throw std::invalid_argument{" Size is not a power of 2"};
        }
        if (table_size > INT32_MAX) {
            throw std::invalid_argument{" Cannot allocate such a large hash table "};
        }

        resize_info_.version = 0;
        state_[0].Initialize(table_size, disk.log().alignment());
        overflow_buckets_allocator_[0].Initialize(disk.log().alignment(), epoch_);
    }


    FasterKv(int number, uint64_t table_size, uint64_t log_size, const std::string &filename,
             double log_mutable_fraction = 0.9)
            : min_table_size_{table_size}, min_log_size{log_size}, tlog_number{number}, disk{filename, epoch_},
              hlog{log_size, epoch_, disk, disk.log(), log_mutable_fraction, 0},
              system_state_{Action::None, Phase::REST, 1}, num_pending_ios{0} {
        // ,system_state_{Action::None, Phase::REST, 1},num_pending_ios{0}
        // hlog{log_size, epoch_, disk, disk.log(), log_mutable_fraction,1},
        //hlog_t a[4];
        //for(uint32_t i=0;i<4;i++){
        for (int i = 0; i < number; i++) {
            thlog[i] = new hlog_t(log_size, epoch_, disk, disk.tlog(i), log_mutable_fraction, i);
        }
        if (!Utility::IsPowerOfTwo(table_size)) {
            throw std::invalid_argument{" Size is not a power of 2"};
        }
        if (table_size > INT32_MAX) {
            throw std::invalid_argument{" Cannot allocate such a large hash table "};
        }

        resize_info_.version = 0;
        for (int i = 0; i < number; i++) {
            state_[i].Initialize(table_size / number, disk.log().alignment());
        }
        overflow_buckets_allocator_[0].Initialize(disk.log().alignment(), epoch_);
    }

    // No copy constructor.
    FasterKv(const FasterKv &other) = delete;

    void Create(int i) {
        thlog[i] = new hlog_t(min_log_size, epoch_, disk, disk.tlog(i), 0.9, i);
        state_[i].Initialize(min_table_size_ / tlog_number, disk.log().alignment());
    }

public:
    /// Thread-related operations
    Guid StartSession();

    uint64_t ContinueSession(const Guid &guid);

    void StopSession();

    void Refresh();

    void Refresh1();

    void Refresh2();

    /// Store interface
    template<class RC>
    inline Status Read(RC &context, AsyncCallback callback, uint64_t monotonic_serial_num);

    template<class UC>
    inline Status Upsert(UC &context, AsyncCallback callback, uint64_t monotonic_serial_num);

    template<class UC>
    inline Status UpsertT(UC &context, AsyncCallback callback, uint64_t monotonic_serial_num, uint16_t thread_number);


    template<class MC>
    inline Status Rmw(MC &context, AsyncCallback callback, uint64_t monotonic_serial_num);

    template<class DC>
    inline Status Delete(DC &context, AsyncCallback callback, uint64_t monotonic_serial_num);

    inline bool CompletePending(bool wait = false);

    /// Checkpoint/recovery operations.
    bool Checkpoint(void(*index_persistence_callback)(Status result),
                    void(*hybrid_log_persistence_callback)(Status result,
                                                           uint64_t persistent_serial_num), Guid &token, int number);

    bool CheckpointIndex(void(*index_persistence_callback)(Status result), Guid &token);

    bool CheckpointHybridLog(void(*hybrid_log_persistence_callback)(Status result,
                                                                    uint64_t persistent_serial_num), Guid &token);

    bool CheckpointCheck();

    Status Recover(const Guid &index_token, const Guid &hybrid_log_token, uint32_t &version,
                   std::vector<Guid> &session_ids);

    /// Truncating the head of the log.
    bool ShiftBeginAddress(Address address, GcState::truncate_callback_t truncate_callback,
                           GcState::complete_callback_t complete_callback);

    /// Garbage collection
    bool GrabageCollecton(Address address, GcState::truncate_callback_t truncate_callback,
                          GcState::complete_callback_t complete_callback);

    bool Gcflag = false;

    /// Make the hash table larger.
    bool GrowIndex(GrowState::callback_t caller_callback);

    /// Statistics
    inline uint64_t Size() const {
        return hlog.GetTailAddress().control();
    }

    inline void DumpDistribution() {
        state_[resize_info_.version].DumpDistribution(
                overflow_buckets_allocator_[resize_info_.version]);
    }

private:
    typedef Record<key_t, value_t> record_t;

    typedef PendingContext<key_t> pending_context_t;

    template<class C>
    inline OperationStatus InternalRead(C &pending_context) const;

    template<class C>
    inline OperationStatus InternalUpsert(C &pending_context);

    template<class C>
    inline OperationStatus InternalUpsertT(C &pending_context, uint16_t number);

    template<class C>
    inline OperationStatus InternalRmw(C &pending_context, bool retrying);

    template<class C>
    inline OperationStatus InternalDelete(C &pending_context);

    inline OperationStatus InternalRetryPendingRmw(async_pending_rmw_context_t &pending_context);

    OperationStatus InternalContinuePendingRead(ExecutionContext &ctx,
                                                AsyncIOContext &io_context);

    OperationStatus InternalContinuePendingRmw(ExecutionContext &ctx,
                                               AsyncIOContext &io_context);

    // Find the hash bucket entry, if any, corresponding to the specified hash.
    // The caller can use the "expected_entry" to CAS its desired address into the entry.
    inline const AtomicHashBucketEntry *
    FindEntry(const key_t &key, KeyHash hash, HashBucketEntry &expected_entry, HashInfo &expectded_info) const;

    // If a hash bucket entry corresponding to the specified hash exists, return it; otherwise,
    // create a new entry. The caller can use the "expected_entry" to CAS its desired address into
    // the entry.
    inline AtomicHashBucketEntry *
    FindOrCreateEntry(const key_t &key, KeyHash hash, HashBucketEntry &expected_entry, HashInfo &expectded_info);

    inline Address TraceBackForKeyMatch(const key_t &key, Address from_address,
                                        Address min_offset) const;

    inline Address TraceBackForKeyMatchH(const key_t &key, Address from_address,
                                         Address min_offset) const;

    inline bool TraceBackForGC(Address from_address, vector<Address> &vec) const;

    Address TraceBackForOtherChainStart(uint64_t old_size, uint64_t new_size, Address from_address,
                                        Address min_address, uint8_t side);

    // If a hash bucket entry corresponding to the specified hash exists, return it; otherwise,
    // return an unused bucket entry.
    inline AtomicHashBucketEntry *FindTentativeEntry(const key_t &key, KeyHash hash, HashBucket *bucket,
                                                     uint8_t version, HashBucketEntry &expected_entry,
                                                     HashInfo &expectded_info,uint64_t &i);

    // Looks for an entry that has the same
    inline bool HasConflictingEntry(const key_t &key, KeyHash hash, HashBucket *bucket, uint8_t version,
                                    AtomicHashBucketEntry *atomic_entry);

    inline Address BlockAllocate(uint32_t record_size);

    inline Address BlockAllocateT(uint32_t record_size, uint32_t j);

    inline Status HandleOperationStatus(ExecutionContext &ctx,
                                        pending_context_t &pending_context,
                                        OperationStatus internal_status, bool &async);

    inline Status PivotAndRetry(ExecutionContext &ctx, pending_context_t &pending_context,
                                bool &async);

    inline Status RetryLater(ExecutionContext &ctx, pending_context_t &pending_context,
                             bool &async);

    inline constexpr uint32_t MinIoRequestSize() const;

    inline Status IssueAsyncIoRequest(ExecutionContext &ctx, pending_context_t &pending_context,
                                      bool &async);

    void AsyncGetFromDisk(Address address, uint32_t num_records, AsyncIOCallback callback,
                          AsyncIOContext &context);

    static void AsyncGetFromDiskCallback(IAsyncContext *ctxt, Status result,
                                         size_t bytes_transferred);

    void CompleteIoPendingRequests(ExecutionContext &context);

    void CompleteRetryRequests(ExecutionContext &context);

    void InitializeCheckpointLocks();

    /// Checkpoint/recovery methods.
    void HandleSpecialPhases();

    bool GlobalMoveToNextState(SystemState current_state);

    Status CheckpointFuzzyIndex();

    Status CheckpointFuzzyIndexComplete();

    Status RecoverFuzzyIndex(int i);

    Status RecoverFuzzyIndexComplete(bool wait,int i);

    Status WriteIndexMetadata();

    Status ReadIndexMetadata(const Guid &token);

    Status ReadIndexVersionMetadata(uint32_t version);

    Status WriteCprMetadata();

    Status ReadCprMetadata(const Guid &token);

    Status WriteCprContext();

    Status ReadCprContexts(const Guid &token, const Guid *guids);

    Status RecoverHybridLog();

    Status RecoverHybridLog1(uint16_t rec);

    Status RecoverHybridLogFromSnapshotFile();

    Status RecoverFromPage(Address from_address, Address to_address);

    Status RecoverFromPage1(Address from_address, Address to_address, uint16_t rec);

    Status RestoreHybridLog();

    Status RestoreHybridLog1(uint16_t rec);

    void MarkAllPendingRequests();

    inline void HeavyEnter();

    bool CleanHashTableBuckets();

    void SplitHashTableBuckets();

    void CheckpointEntry(file_t &&file, int number, int count, int w);

    void AddHashEntry(HashBucket *&bucket, uint32_t &next_idx, uint8_t version,
                      HashBucketEntry entry);

    /// Access the current and previous (thread-local) execution contexts.
    const ExecutionContext &thread_ctx() const {
        return thread_contexts_[Thread::id()].cur();
    }

    ExecutionContext &thread_ctx() {
        return thread_contexts_[Thread::id()].cur();
    }

    ExecutionContext &prev_thread_ctx() {
        return thread_contexts_[Thread::id()].prev();
    }

private:
    LightEpoch epoch_;

public:
    disk_t disk;
    hlog_t hlog;
    hlog_t *thlog[112];
private:
    static constexpr bool kCopyReadsToTail = false;
    static constexpr uint64_t kGcHashTableChunkSize = 16384;
    static constexpr uint64_t kGrowHashTableChunkSize = 16384;

    bool fold_over_snapshot = true;

    /// Initial size of the table
    uint64_t min_table_size_;

    uint64_t min_log_size;

    int tlog_number;

    int check_thread_number;

    atomic<int> WriteHash[128];

    // Allocator for the hash buckets that don't fit in the hash table.
    MallocFixedPageSize<HashBucket, disk_t> overflow_buckets_allocator_[2];

    // An array of size two, that contains the old and new versions of the hash-table
    InternalHashTable<disk_t> state_[128];

    CheckpointLocks checkpoint_locks_;

    ResizeInfo resize_info_;

    AtomicSystemState system_state_;

    /// Checkpoint/recovery state.
    CheckpointState<file_t> checkpoint_;
    /// Garbage collection state.
    GcState gc_;
    /// Grow (hash table) state.
    GrowState grow_;

    /// Global count of pending I/Os, used for throttling.
    std::atomic<uint64_t> num_pending_ios;

    /// Space for two contexts per thread, stored inline.
    ThreadContext thread_contexts_[Thread::kMaxNumThreads];
};

// Implementations.
template<class K, class V, class D>
inline Guid FasterKv<K, V, D>::StartSession() {
    SystemState state = system_state_.load();
    if (state.phase != Phase::REST) {
        throw std::runtime_error{"Can acquire only in REST phase!"};
    }
    thread_ctx().Initialize(state.phase, state.version, Guid::Create(), 0);
    Refresh();
    return thread_ctx().guid;
}

template<class K, class V, class D>
inline uint64_t FasterKv<K, V, D>::ContinueSession(const Guid &session_id) {
    auto iter = checkpoint_.continue_tokens.find(session_id);
    if (iter == checkpoint_.continue_tokens.end()) {
        throw std::invalid_argument{"Unknown session ID"};
    }

    SystemState state = system_state_.load();
    if (state.phase != Phase::REST) {
        throw std::runtime_error{"Can continue only in REST phase!"};
    }
    thread_ctx().Initialize(state.phase, state.version, session_id, iter->second);
    Refresh();
    return iter->second;
}

template<class K, class V, class D>
inline void FasterKv<K, V, D>::Refresh() {
    epoch_.ProtectAndDrain();
    // We check if we are in normal mode
    SystemState new_state = system_state_.load();
    if (thread_ctx().phase == Phase::REST && new_state.phase == Phase::REST) {
        return;
    }
    HandleSpecialPhases();
}

template<class K, class V, class D>
inline void FasterKv<K, V, D>::Refresh1() {
    // We check if we are in normal mode
    SystemState new_state = system_state_.load();
    if (thread_ctx().phase == Phase::REST && new_state.phase == Phase::REST) {
        return;
    }
    HandleSpecialPhases();
}

template<class K, class V, class D>
inline void FasterKv<K, V, D>::Refresh2() {
    epoch_.ProtectAndDrain();
    // We check if we are in normal mode
}

template<class K, class V, class D>
inline void FasterKv<K, V, D>::StopSession() {
    // If this thread is still involved in some activity, wait until it finishes.
    while (thread_ctx().phase != Phase::REST ||
           !thread_ctx().pending_ios.empty() ||
           !thread_ctx().retry_requests.empty()) {
        CompletePending(false);
        std::this_thread::yield();
    }

    assert(thread_ctx().retry_requests.empty());
    assert(thread_ctx().pending_ios.empty());
    assert(thread_ctx().io_responses.empty());

    assert(prev_thread_ctx().retry_requests.empty());
    assert(prev_thread_ctx().pending_ios.empty());
    assert(prev_thread_ctx().io_responses.empty());

    assert(thread_ctx().phase == Phase::REST);

    epoch_.Unprotect();
}

template<class K, class V, class D>
inline const AtomicHashBucketEntry *FasterKv<K, V, D>::FindEntry(const key_t &key, KeyHash hash,
                                                                 HashBucketEntry &expected_entry,
                                                                 HashInfo &expectded_info) const {
    expected_entry = HashBucketEntry::kInvalidEntry;
    // Truncate the hash to get a bucket page_index < state[version].size.
    uint32_t version = resize_info_.version;
    uint64_t hash_number = hash.idx(min_table_size_) % (min_table_size_ / tlog_number);
    /*if(hash_number == 188&&hash.tag()==7944){
        cout<<hash_number<<"  "<<hash.tag()<<endl;
        //version = 0;
    }*/
    uint64_t v = hash.idx(min_table_size_) / (min_table_size_ / tlog_number);
    const HashBucket *bucket = &state_[v].bucket(hash_number);
    assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);

    while (true) {
        // Search through the bucket looking for our key. Last entry is reserved
        // for the overflow pointer.
        for (uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
            HashBucketEntry entry = bucket->entries[entry_idx].load();
            if (entry.unused()) {
                continue;
            }
            if (hash.tag() == entry.tag()) {
                // Found a matching tag. (So, the input hash matches the entry on 14 tag bits +
                // log_2(table size) address bits.)
                if (!entry.tentative() && key == bucket->entries[entry_idx].GetKey()) {
                    expected_entry = entry;
                    expectded_info = bucket->entries[entry_idx].GetInfo();
                    return &bucket->entries[entry_idx];
                }
            }
        }

        // Go to next bucket in the chain
        HashBucketOverflowEntry entry = bucket->overflow_entry.load();
        if (entry.unused()) {
            // No more buckets in the chain.
            return nullptr;
        }
        bucket = &overflow_buckets_allocator_[version].Get(entry.address());
        assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
    }
    assert(false);
    return nullptr; // NOT REACHED
}

template<class K, class V, class D>
inline AtomicHashBucketEntry *FasterKv<K, V, D>::FindTentativeEntry(const key_t &key, KeyHash hash,
                                                                    HashBucket *bucket,
                                                                    uint8_t version, HashBucketEntry &expected_entry,
                                                                    HashInfo &expectded_info,uint64_t &i) {
    expected_entry = HashBucketEntry::kInvalidEntry;
    expectded_info = HashInfo::kInvalidInfo;
    AtomicHashBucketEntry *atomic_entry = nullptr;
    // Try to find a slot that contains the right tag or that's free.
    while (true) {
        // Search through the bucket looking for our key. Last entry is reserved
        // for the overflow pointer.
        for (uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
            HashBucketEntry entry = bucket->entries[entry_idx].load();
            if (entry.unused()) {
                if (!atomic_entry) {
                    // Found a free slot; keep track of it, and continue looking for a match.
                    i=entry_idx;
                    atomic_entry = &bucket->entries[entry_idx];
                }
                continue;
            }
            if (hash.tag() == entry.tag() && !entry.tentative()) {
                // Found a match. (So, the input hash matches the entry on 14 tag bits +
                // log_2(table size) address bits.) Return it to caller.
                if (key == bucket->entries[entry_idx].GetKey()) {
                    expected_entry = entry;
                    expectded_info = bucket->entries[entry_idx].GetInfo();
                    return &bucket->entries[entry_idx];
                }
            }
        }
        // Go to next bucket in the chain
        HashBucketOverflowEntry overflow_entry = bucket->overflow_entry.load();
        if (overflow_entry.unused()) {
            // No more buckets in the chain.
            if (atomic_entry) {
                // We found a free slot earlier (possibly inside an earlier bucket).
                assert(expected_entry == HashBucketEntry::kInvalidEntry);
                return atomic_entry;
            }
            // We didn't find any free slots, so allocate new bucket.
            FixedPageAddress new_bucket_addr = overflow_buckets_allocator_[version].Allocate();
            bool success;
            do {
                HashBucketOverflowEntry new_bucket_entry{new_bucket_addr};
                success = bucket->overflow_entry.compare_exchange_strong(overflow_entry,
                                                                         new_bucket_entry);
            } while (!success && overflow_entry.unused());
            if (!success) {
                // Install failed, undo allocation; use the winner's entry
                overflow_buckets_allocator_[version].FreeAtEpoch(new_bucket_addr, 0);
            } else {
                // Install succeeded; we have a new bucket on the chain. Return its first slot.
                bucket = &overflow_buckets_allocator_[version].Get(new_bucket_addr);
                assert(expected_entry == HashBucketEntry::kInvalidEntry);
                return &bucket->entries[0];
            }
        }
        // Go to the next bucket.
        bucket = &overflow_buckets_allocator_[version].Get(overflow_entry.address());
        assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
    }
    assert(false);
    return nullptr; // NOT REACHED
}

template<class K, class V, class D>
bool FasterKv<K, V, D>::HasConflictingEntry(const key_t &key, KeyHash hash, HashBucket *bucket, uint8_t version,
                                            AtomicHashBucketEntry *atomic_entry) {
    uint16_t tag = atomic_entry->load().tag();
    while (true) {
        for (uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
            HashBucketEntry entry = bucket->entries[entry_idx].load();
            if (entry != HashBucketEntry::kInvalidEntry &&
                entry.tag() == tag &&
                atomic_entry != &bucket->entries[entry_idx]) {
                if (key == bucket->entries[entry_idx].GetKey())
                    // Found a conflict.
                    return true;
            }
        }
        // Go to next bucket in the chain
        HashBucketOverflowEntry entry = bucket->overflow_entry.load();
        if (entry.unused()) {
            // Reached the end of the bucket chain; no conflicts found.
            return false;
        }
        // Go to the next bucket.
        bucket = &overflow_buckets_allocator_[version].Get(entry.address());
        assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
    }
}

template<class K, class V, class D>
inline AtomicHashBucketEntry *FasterKv<K, V, D>::FindOrCreateEntry(const key_t &key, KeyHash hash,
                                                                   HashBucketEntry &expected_entry,
                                                                   HashInfo &expectded_info) {
    // Truncate the hash to get a bucket page_index < state[version].size.
    const uint32_t version = resize_info_.version;
    assert(version <= 1);
    uint64_t hash_number = hash.idx(min_table_size_) % (min_table_size_ / tlog_number);
    /*if(hash.tag()==4138){
        hash_number = hash_number +0;
        cout<<hash_number<<" "<<hash.tag()<<endl;
        uint32_t v = hash.idx(min_table_size_) / (min_table_size_ / tlog_number);
    }*/
    uint32_t v = hash.idx(min_table_size_) / (min_table_size_ / tlog_number);
    int w = v;
    while (true) {
        HashBucket *bucket = &state_[v].bucket(hash_number);
        assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
        uint64_t i = 0;
        AtomicHashBucketEntry *atomic_entry = FindTentativeEntry(key, hash, bucket, version,
                                                                 expected_entry, expectded_info,i);
        if (expected_entry != HashBucketEntry::kInvalidEntry) {
            // Found an existing hash bucket entry; nothing further to check.
            return atomic_entry;
        }
        // We have a free slot.
        assert(atomic_entry);
        //assert(expected_entry == HashBucketEntry::kInvalidEntry);
        // Try to install tentative tag in free slot.
        HashBucketEntry entry{Address::kInvalidAddress, hash.tag(), true};
        atom_t exchanged[2];
        atom_t compared[2];
        exchanged[0] = entry.control_;
        exchanged[1] = 0;
        compared[0] = 0;
        compared[1] = 0;
        //expected[0]=entry
        if (atomic_entry->compare_exchange_strong(exchanged, compared)) {
            // See if some other thread is also trying to install this tag.
            if (HasConflictingEntry(key, hash, bucket, version, atomic_entry)) {
                // Back off and try again.
                atomic_entry->store(HashBucketEntry::kInvalidEntry);
            } else {
                // No other thread was trying to install this tag, so we can clear our entry's "tentative"
                // bit.
                expected_entry = HashBucketEntry{Address::kInvalidAddress, hash.tag(), false};
                expectded_info = HashInfo{0,0,0,0,i,hash_number};
                atomic_entry->store(expected_entry);
                atomic_entry->setInfo(expectded_info);
                //if(hash_number == 134588)
                  //  cout<<atomic_entry->GetInfo().idx()<<" "<<expectded_info.idx()<<endl;
                return atomic_entry;
            }
        }
    }
    assert(false);
    return nullptr; // NOT REACHED
}

template<class K, class V, class D>
template<class RC>
inline Status FasterKv<K, V, D>::Read(RC &context, AsyncCallback callback,
                                      uint64_t monotonic_serial_num) {
    typedef RC read_context_t;
    typedef PendingReadContext<RC> pending_read_context_t;
    static_assert(std::is_base_of<value_t, typename read_context_t::value_t>::value,
                  "value_t is not a base class of read_context_t::value_t");
    static_assert(alignof(value_t) == alignof(typename read_context_t::value_t),
                  "alignof(value_t) != alignof(typename read_context_t::value_t)");

    pending_read_context_t pending_context{context, callback};
    OperationStatus internal_status = InternalRead(pending_context);
    Status status;
    if (internal_status == OperationStatus::SUCCESS) {
        status = Status::Ok;
    } else if (internal_status == OperationStatus::NOT_FOUND) {
        status = Status::NotFound;
    } else {
        assert(internal_status == OperationStatus::RECORD_ON_DISK);
        bool async;
        status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
    }
    thread_ctx().serial_num = monotonic_serial_num;
    return status;
}

template<class K, class V, class D>
template<class UC>
inline Status FasterKv<K, V, D>::Upsert(UC &context, AsyncCallback callback,
                                        uint64_t monotonic_serial_num) {
    typedef UC upsert_context_t;
    typedef PendingUpsertContext<UC> pending_upsert_context_t;
    static_assert(std::is_base_of<value_t, typename upsert_context_t::value_t>::value,
                  "value_t is not a base class of upsert_context_t::value_t");
    static_assert(alignof(value_t) == alignof(typename upsert_context_t::value_t),
                  "alignof(value_t) != alignof(typename upsert_context_t::value_t)");

    pending_upsert_context_t pending_context{context, callback};
    OperationStatus internal_status = InternalUpsert(pending_context);
    Status status;

    if (internal_status == OperationStatus::SUCCESS) {
        status = Status::Ok;
    } else {
        bool async;
        status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
    }
    thread_ctx().serial_num = monotonic_serial_num;
    return status;
}

template<class K, class V, class D>
template<class UC>
inline Status
FasterKv<K, V, D>::UpsertT(UC &context, AsyncCallback callback, uint64_t monotonic_serial_num, uint16_t thread_number) {
    typedef UC upsert_context_t;
    typedef PendingUpsertContext<UC> pending_upsert_context_t;
    static_assert(std::is_base_of<value_t, typename upsert_context_t::value_t>::value,
                  "value_t is not a base class of upsert_context_t::value_t");
    static_assert(alignof(value_t) == alignof(typename upsert_context_t::value_t),
                  "alignof(value_t) != alignof(typename upsert_context_t::value_t)");

    pending_upsert_context_t pending_context{context, callback};
    //OperationStatus internal_status = InternalUpsert(pending_context);
    OperationStatus internal_status = InternalUpsertT(pending_context, thread_number);
    Status status;

    if (internal_status == OperationStatus::SUCCESS) {
        status = Status::Ok;
    } else {
        bool async;
        //status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
    }
    thread_ctx().serial_num = monotonic_serial_num;
    return status;
}

template<class K, class V, class D>
template<class MC>
inline Status FasterKv<K, V, D>::Rmw(MC &context, AsyncCallback callback,
                                     uint64_t monotonic_serial_num) {
    typedef MC rmw_context_t;
    typedef PendingRmwContext<MC> pending_rmw_context_t;
    static_assert(std::is_base_of<value_t, typename rmw_context_t::value_t>::value,
                  "value_t is not a base class of rmw_context_t::value_t");
    static_assert(alignof(value_t) == alignof(typename rmw_context_t::value_t),
                  "alignof(value_t) != alignof(typename rmw_context_t::value_t)");

    pending_rmw_context_t pending_context{context, callback};
    OperationStatus internal_status = InternalRmw(pending_context, false);
    Status status;
    if (internal_status == OperationStatus::SUCCESS) {
        status = Status::Ok;
    } else {
        bool async;
        status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
    }
    thread_ctx().serial_num = monotonic_serial_num;
    return status;
}

template<class K, class V, class D>
template<class DC>
inline Status FasterKv<K, V, D>::Delete(DC &context, AsyncCallback callback,
                                        uint64_t monotonic_serial_num) {
    typedef DC delete_context_t;
    typedef PendingDeleteContext<DC> pending_delete_context_t;
    static_assert(std::is_base_of<value_t, typename delete_context_t::value_t>::value,
                  "value_t is not a base class of delete_context_t::value_t");
    static_assert(alignof(value_t) == alignof(typename delete_context_t::value_t),
                  "alignof(value_t) != alignof(typename delete_context_t::value_t)");

    pending_delete_context_t pending_context{context, callback};
    OperationStatus internal_status = InternalDelete(pending_context);
    Status status;
    if (internal_status == OperationStatus::SUCCESS) {
        status = Status::Ok;
    } else if (internal_status == OperationStatus::NOT_FOUND) {
        status = Status::NotFound;
    } else {
        bool async;
        status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
    }
    thread_ctx().serial_num = monotonic_serial_num;
    return status;
}

template<class K, class V, class D>
inline bool FasterKv<K, V, D>::CompletePending(bool wait) {
    do {
        disk.TryComplete();

        bool done = true;
        if (thread_ctx().phase != Phase::WAIT_PENDING && thread_ctx().phase != Phase::IN_PROGRESS) {
            CompleteIoPendingRequests(thread_ctx());
        }
        Refresh();
        CompleteRetryRequests(thread_ctx());

        done = (thread_ctx().pending_ios.empty() && thread_ctx().retry_requests.empty());

        if (thread_ctx().phase != Phase::REST) {
            CompleteIoPendingRequests(prev_thread_ctx());
            Refresh();
            CompleteRetryRequests(prev_thread_ctx());
            done = false;
        }
        if (done) {
            return true;
        }
    } while (wait);
    return false;
}

template<class K, class V, class D>
inline void FasterKv<K, V, D>::CompleteIoPendingRequests(ExecutionContext &context) {
    AsyncIOContext *ctxt;
    // Clear this thread's I/O response queue. (Does not clear I/Os issued by this thread that have
    // not yet completed.)
    while (context.io_responses.try_pop(ctxt)) {
        CallbackContext<AsyncIOContext> io_context{ctxt};
        CallbackContext<pending_context_t> pending_context{io_context->caller_context};
        // This I/O is no longer pending, since we popped its response off the queue.
        auto pending_io = context.pending_ios.find(io_context->io_id);
        assert(pending_io != context.pending_ios.end());
        context.pending_ios.erase(pending_io);

        // Issue the continue command
        OperationStatus internal_status;
        if (pending_context->type == OperationType::Read) {
            internal_status = InternalContinuePendingRead(context, *io_context.get());
        } else {
            assert(pending_context->type == OperationType::RMW);
            internal_status = InternalContinuePendingRmw(context, *io_context.get());
        }
        Status result;
        if (internal_status == OperationStatus::SUCCESS) {
            result = Status::Ok;
        } else if (internal_status == OperationStatus::NOT_FOUND) {
            result = Status::NotFound;
        } else {
            result = HandleOperationStatus(context, *pending_context.get(), internal_status,
                                           pending_context.async);
        }
        if (!pending_context.async) {
            pending_context->caller_callback(pending_context->caller_context, result);
        }
    }
}

template<class K, class V, class D>
inline void FasterKv<K, V, D>::CompleteRetryRequests(ExecutionContext &context) {
    // If we can't complete a request, it will be pushed back onto the deque. Retry each request
    // only once.
    size_t size = context.retry_requests.size();
    for (size_t idx = 0; idx < size; ++idx) {
        CallbackContext<pending_context_t> pending_context{context.retry_requests.front()};
        context.retry_requests.pop_front();
        // Issue retry command
        OperationStatus internal_status;
        switch (pending_context->type) {
            case OperationType::RMW:
                internal_status = InternalRetryPendingRmw(
                        *static_cast<async_pending_rmw_context_t *>(pending_context.get()));
                break;
            case OperationType::Upsert:
                internal_status = InternalUpsert(
                        *static_cast<async_pending_upsert_context_t *>(pending_context.get()));
                break;
            default:
                assert(false);
                throw std::runtime_error{"Cannot happen!"};
        }
        // Handle operation status
        Status result;
        if (internal_status == OperationStatus::SUCCESS) {
            result = Status::Ok;
        } else {
            result = HandleOperationStatus(context, *pending_context.get(), internal_status,
                                           pending_context.async);
        }

        // If done, callback user code.
        if (!pending_context.async) {
            pending_context->caller_callback(pending_context->caller_context, result);
        }
    }
}

template<class K, class V, class D>
template<class C>
inline OperationStatus FasterKv<K, V, D>::InternalRead(C &pending_context) const {
    typedef C pending_read_context_t;

    if (thread_ctx().phase != Phase::REST) {
        const_cast<faster_t *>(this)->HeavyEnter();
        //return OperationStatus::NOT_FOUND;
    }
    if (thread_ctx().phase == Phase::INDEX_CHKPT || thread_ctx().phase == Phase::PREP_INDEX_CHKPT)
        return OperationStatus::NOT_FOUND;
    const key_t &key = pending_context.key();
    KeyHash hash = key.GetHash();
    HashBucketEntry entry;
    HashInfo info;
    const AtomicHashBucketEntry *atomic_entry = FindEntry(key, hash, entry, info);
    if (!atomic_entry) {
        // no record found
        return OperationStatus::NOT_FOUND;
    }

    // HashBucketEntry entry = atomic_entry->load();
    Address address = entry.address();
    uint16_t k = address.h();
    Address begin_address = thlog[k]->begin_address.load();
    Address head_address = thlog[k]->head_address.load();
    Address safe_read_only_address = thlog[k]->safe_read_only_address.load();
    Address read_only_address = thlog[k]->read_only_address.load();
    //Address begin_address = hlog.begin_address.load();
    //Address head_address = hlog.head_address.load();
    //Address safe_read_only_address = hlog.safe_read_only_address.load();
    //Address read_only_address = hlog.read_only_address.load();
    uint64_t latest_record_version = 0;

    if (address >= head_address) {
        // Look through the in-memory portion of the log, to find the first record (if any) whose key
        // matches.
        //const record_t *record = reinterpret_cast<const record_t *>(thlog[k]->Get(address));
        latest_record_version = info.version();
    }

    switch (thread_ctx().phase) {
        case Phase::PREPARE:
            // Reading old version (v).
            if (latest_record_version > thread_ctx().version) {
                // CPR shift detected: we are in the "PREPARE" phase, and a record has a version later than
                // what we've seen.
                pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, entry);
                return OperationStatus::CPR_SHIFT_DETECTED;
            }
            break;
        default:
            break;
    }

    if (address >= safe_read_only_address) {
        // Mutable or fuzzy region
        // concurrent read
        //if (reinterpret_cast<const record_t *>(thlog[k]->Get(address))->header.tombstone) {
        if (info.tombtone()) {
            return OperationStatus::NOT_FOUND;
        }
        pending_context.GetAtomic(thlog[k]->Get(address));
        return OperationStatus::SUCCESS;
    } else if (address >= head_address) {
        // Immutable region
        // single-thread read
        //if (reinterpret_cast<const record_t *>(thlog[k]->Get(address))->header.tombstone) {
        if (info.tombtone()) {
            return OperationStatus::NOT_FOUND;
        }
        pending_context.Get(thlog[k]->Get(address));
        return OperationStatus::SUCCESS;
    } else if (address >= begin_address) {
        // Record not available in-memory
        pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, entry);
        return OperationStatus::RECORD_ON_DISK;
    } else {
        // No record found
        return OperationStatus::NOT_FOUND;
    }
}

template<class K, class V, class D>
template<class C>
inline OperationStatus FasterKv<K, V, D>::InternalUpsert(C &pending_context) {
    typedef C pending_upsert_context_t;

    if (thread_ctx().phase != Phase::REST) {
        HeavyEnter();
    }
    //cout<<Thread::id()<<endl;
    uint16_t j = Thread::id() % 1;
    //uint16_t j = 0;
    //j=1;
    const key_t &key = pending_context.key();
    KeyHash hash = key.GetHash();
    HashBucketEntry expected_entry;
    HashBucket *bucket;
    AtomicHashBucketEntry *atomic_entry;
    //= FindOrCreateEntry(hash, expected_entry);   //entry ??????

    // (Note that address will be Address::kInvalidAddress, if the atomic_entry was created.)
    Address address = expected_entry.address();
    //uint32_t k=address.h();
    uint16_t k = address.h();
    Address head_address = thlog[k]->head_address.load();
    //Address hea=hlog.head_address.load();
    //hea=thlog[0]->head_address.load();
    Address read_only_address = thlog[k]->read_only_address.load();
    //Address head_address = hlog.head_address.load();
    //Address read_only_address = hlog.read_only_address.load();
    uint64_t latest_record_version = 0;

    if (address >= head_address) {
        // Multiple keys may share the same hash. Try to find the most recent record with a matching
        // key that we might be able to update in place.
        record_t *record = reinterpret_cast<record_t *>(thlog[k]->Get(address));
        latest_record_version = record->header.checkpoint_version;
        /*
        if(latest_record_version != 0){
            cout<<"fails"<<latest_record_version<<endl;
        }

        if(latest_record_version == 0){
            cout<<"fails"<<latest_record_version<<endl;
        }
         */
        if (key != record->key()) {
            address = TraceBackForKeyMatch(key, record->header.previous_address(), head_address);
            k = address.h();
            read_only_address = thlog[k]->read_only_address.load();
        }
    }

    CheckpointLockGuard lock_guard{checkpoint_locks_, hash};

    // The common case
    if (thread_ctx().phase == Phase::REST && address >= read_only_address) {
        record_t *record = reinterpret_cast<record_t *>(thlog[k]->Get(address));
        if (!record->header.tombstone && pending_context.PutAtomic(record)) {
            return OperationStatus::SUCCESS;
        } else {
            // Must retry as RCU.
            goto create_record;
        }
    }

    // Acquire necessary locks.
    switch (thread_ctx().phase) {
        case Phase::PREPARE:
            // Working on old version (v).
            if (!lock_guard.try_lock_old()) {
                pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
                return OperationStatus::CPR_SHIFT_DETECTED;
            } else {
                if (latest_record_version > thread_ctx().version) {
                    // CPR shift detected: we are in the "PREPARE" phase, and a record has a version later than
                    // what we've seen.
                    pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
                    return OperationStatus::CPR_SHIFT_DETECTED;
                }
            }
            break;
        case Phase::IN_PROGRESS:
            // All other threads are in phase {PREPARE,IN_PROGRESS,WAIT_PENDING}.
            if (latest_record_version < thread_ctx().version) {
                // Will create new record or update existing record to new version (v+1).
                if (!lock_guard.try_lock_new()) {
                    pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
                    return OperationStatus::RETRY_LATER;
                } else {
                    // Update to new version (v+1) requires RCU.
                    goto create_record;
                }
            }
            break;
        case Phase::WAIT_PENDING:
            // All other threads are in phase {IN_PROGRESS,WAIT_PENDING,WAIT_FLUSH}.
            if (latest_record_version < thread_ctx().version) {
                if (lock_guard.old_locked()) {
                    pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
                    return OperationStatus::RETRY_LATER;
                } else {
                    // Update to new version (v+1) requires RCU.
                    goto create_record;
                }
            }
            break;
        case Phase::WAIT_FLUSH:
            // All other threads are in phase {WAIT_PENDING,WAIT_FLUSH,PERSISTENCE_CALLBACK}.
            if (latest_record_version < thread_ctx().version) {
                goto create_record;
            }
            break;
        default:
            break;
    }

    if (address >= read_only_address) {
        // Mutable region; try to update in place.
        if (atomic_entry->load() != expected_entry) {
            // Some other thread may have RCUed the record before we locked it; try again.
            return OperationStatus::RETRY_NOW;
        }
        // We acquired the necessary locks, so so we can update the record's bucket atomically.
        record_t *record = reinterpret_cast<record_t *>(thlog[k]->Get(address));
        if (!record->header.tombstone && pending_context.PutAtomic(record)) {
            // Host successfully replaced record, atomically.
            return OperationStatus::SUCCESS;
        } else {
            // Must retry as RCU.
            goto create_record;
        }
    }

    // Create a record and attempt RCU.
    create_record:
    uint32_t record_size = record_t::size(key, pending_context.value_size());
    Address new_address = BlockAllocateT(record_size, j);
    record_t *record = reinterpret_cast<record_t *>(thlog[j]->Get(new_address));
    new(record) record_t{
            RecordInfo{
                    static_cast<uint16_t>(thread_ctx().version), true, false, false,
                    expected_entry.address()},
            key};
    pending_context.Put(record);   //put ?????????
    // new_address+=Address{0,0,j}.control();
    //HashBucketEntry updated_entry{new_address, hash.tag(), false};
    HashBucketEntry updated_entry{new_address, hash.tag(), false};
    /*
    if (atomic_entry->compare_exchange_strong(expected_entry, updated_entry)) {
        // Installed the new record in the hash table.
        return OperationStatus::SUCCESS;
    } else {
        // Try again.
        record->header.invalid = true;
        return InternalUpsert(pending_context);
        //return InternalUpsert(pending_context);
    }*/
}

template<class K, class V, class D>
template<class C>
inline OperationStatus FasterKv<K, V, D>::InternalUpsertT(C &pending_context, uint16_t number) {
    typedef C pending_upsert_context_t;

    if (thread_ctx().phase != Phase::REST) {
        HeavyEnter();
    }
    //cout<<Thread::id()<<endl;/
    //uint16_t j = Thread::id() % number;
    //j=1;
    const key_t &key = pending_context.key();
    KeyHash hash = key.GetHash();
    uint16_t j = hash.idx(min_table_size_) / (min_table_size_ / tlog_number);
    HashBucketEntry expected_entry;
    HashBucket *bucket;
    HashInfo expected_info;
    AtomicHashBucketEntry *atomic_entry = FindOrCreateEntry(key, hash, expected_entry, expected_info);   //entry ??????

    // (Note that address will be Address::kInvalidAddress, if the atomic_entry was created.)
    Address address = expected_entry.address();
    //uint32_t k=address.h();
    uint16_t k = address.h();
    Address head_address = thlog[k]->head_address.load();
    //Address hea=hlog.head_address.load();
    //hea=thlog[0]->head_address.load();
    Address read_only_address = thlog[k]->read_only_address.load();
    //Address head_address = hlog.head_address.load();
    //Address read_only_address = hlog.read_only_address.load();
    uint64_t latest_record_version = 0;
    int key_flag = 0;

    if (address >= head_address) {
        // Multiple keys may share the same hash. Try to find the most recent record with a matching
        // key that we might be able to update in place.
        //record_t *record = reinterpret_cast<record_t *>(thlog[k]->Get(address));
        //latest_record_version = record->header.checkpoint_version;
        latest_record_version = expected_info.version();
        key_flag = 1;
        /*
        if(latest_record_version != 0){
            cout<<"fails"<<latest_record_version<<endl;
        }

        if(latest_record_version == 0){
            cout<<"fails"<<latest_record_version<<endl;
        }
         */
        /*
        if (key != record->key()) {
            address = TraceBackForKeyMatch(key, record->header.previous_address(), head_address);
            k = address.h();
            read_only_address = thlog[k]->read_only_address.load();
        }*/
    }

    CheckpointLockGuard lock_guard{checkpoint_locks_, hash};

    // The common case
    if (thread_ctx().phase == Phase::REST && address >= read_only_address) {
        if (!expected_info.tombtone() && pending_context.value_length() <= expected_info.value_length()) {
            record_t *record = reinterpret_cast<record_t *>(thlog[k]->Get(address));
            if (pending_context.PutAtomic(record)) {
                return OperationStatus::SUCCESS;
            } else {
                // Must retry as RCU.
                goto create_record;
            }
        } else {
            // Must retry as RCU.
            goto create_record;
        }
    }

    // Acquire necessary locks.
    switch (thread_ctx().phase) {
        case Phase::PREPARE:
            // Working on old version (v).
            if (!lock_guard.try_lock_old()) {
                pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
                return OperationStatus::CPR_SHIFT_DETECTED;
            } else {
                if (latest_record_version > thread_ctx().version) {
                    // CPR shift detected: we are in the "PREPARE" phase, and a record has a version later than
                    // what we've seen.
                    pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
                    return OperationStatus::CPR_SHIFT_DETECTED;
                }
            }
            break;
        case Phase::IN_PROGRESS:
            // All other threads are in phase {PREPARE,IN_PROGRESS,WAIT_PENDING}.
            if (latest_record_version < thread_ctx().version) {
                // Will create new record or update existing record to new version (v+1).
                if (!lock_guard.try_lock_new()) {
                    pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
                    return OperationStatus::RETRY_LATER;
                } else {
                    // Update to new version (v+1) requires RCU.
                    goto create_record;
                }
            }
            break;
        case Phase::WAIT_PENDING:
            // All other threads are in phase {IN_PROGRESS,WAIT_PENDING,WAIT_FLUSH}.
            if (latest_record_version < thread_ctx().version) {
                if (lock_guard.old_locked()) {
                    pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
                    return OperationStatus::RETRY_LATER;
                } else {
                    // Update to new version (v+1) requires RCU.
                    goto create_record;
                }
            }
            break;
        case Phase::WAIT_FLUSH:
            // All other threads are in phase {WAIT_PENDING,WAIT_FLUSH,PERSISTENCE_CALLBACK}.
            if (latest_record_version < thread_ctx().version) {
                goto create_record;
            }
            break;
        default:
            break;
    }

    if (address >= read_only_address) {
        // Mutable region; try to update in place.
        if (atomic_entry->load() != expected_entry) {
            // Some other thread may have RCUed the record before we locked it; try again.
            return OperationStatus::RETRY_NOW;
        }
        // We acquired the necessary locks, so so we can update the record's bucket atomically.
        record_t *record = reinterpret_cast<record_t *>(thlog[k]->Get(address));
        if (!record->header.tombstone && pending_context.PutAtomic(record)) {
            // Host successfully replaced record, atomically.
            return OperationStatus::SUCCESS;
        } else {
            // Must retry as RCU.
            goto create_record;
        }
    }

    // Create a record and attempt RCU.
    create_record:
    uint32_t record_size = record_t::size(key, pending_context.value_size());
    Address new_address = BlockAllocateT(record_size, j);
    record_t *record = reinterpret_cast<record_t *>(thlog[j]->Get(new_address));
    new(record) record_t{
            RecordInfo{
                    static_cast<uint16_t>(thread_ctx().version), true, false, false,
                    expected_entry.address()}};
    pending_context.Put(record);   //put ?????????
    if (!key_flag)
        key.Copy(atomic_entry->GetKey());
    //std::memcpy(buf, buf_, len_);
    // new_address+=Address{0,0,j}.control();
    //HashBucketEntry updated_entry{new_address, hash.tag(), false};
    HashBucketEntry updated_entry{new_address, hash.tag(), false};
    HashInfo updated_info{static_cast<uint16_t>(thread_ctx().version), pending_context.value_length(),
                          key.length(), 0,expected_info.location(),expected_info.idx()};
    atom_t compared[2], exchanged[2];
    exchanged[0] = updated_entry.control_;
    exchanged[1] = updated_info.control_;
    compared[0] = expected_entry.control_;
    compared[1] = expected_info.control_;
    if (atomic_entry->compare_exchange_strong(exchanged, compared)) {
        // Installed the new record in the hash table.
        return OperationStatus::SUCCESS;
    } else {
        // Try again.
        record->header.invalid = true;
        return InternalUpsertT(pending_context, number);
        //return InternalUpsert(pending_context);
    }
}


template<class K, class V, class D>
template<class C>
inline OperationStatus FasterKv<K, V, D>::InternalRmw(C &pending_context, bool retrying) {
    typedef C pending_rmw_context_t;

    Phase phase = retrying ? pending_context.phase : thread_ctx().phase;
    uint32_t version = retrying ? pending_context.version : thread_ctx().version;

    if (phase != Phase::REST) {
        HeavyEnter();
    }
    const key_t &key = pending_context.key();
    KeyHash hash = key.GetHash();
    uint16_t j = hash.idx(min_table_size_) / (min_table_size_ / tlog_number);
    HashBucketEntry expected_entry;
    HashBucket *bucket;
    HashInfo expected_info;
    AtomicHashBucketEntry *atomic_entry = FindOrCreateEntry(key, hash, expected_entry, expected_info);   //entry ??????

    // (Note that address will be Address::kInvalidAddress, if the atomic_entry was created.)
    Address address = expected_entry.address();
    //uint32_t k=address.h();
    uint16_t k = address.h();
    Address head_address = thlog[k]->head_address.load();
    Address read_only_address = thlog[k]->read_only_address.load();
    Address begin_address = thlog[k]->begin_address.load();
    Address safe_read_only_address = thlog[k]->safe_read_only_address.load();
    uint64_t latest_record_version = 0;
    int key_flag = 0;

    if (address >= head_address) {
        // Multiple keys may share the same hash. Try to find the most recent record with a matching
        // key that we might be able to update in place.
        latest_record_version = expected_info.version();
        key_flag = 1;
    }

    CheckpointLockGuard lock_guard{checkpoint_locks_, hash};

    // The common case.
    if (phase == Phase::REST && address >= read_only_address) {
        if (!expected_info.tombtone() && pending_context.value_length() <= expected_info.value_length()) {
            record_t *record = reinterpret_cast<record_t *>(thlog[k]->Get(address));
            if (pending_context.RmwAtomic(record)) {
                // In-place RMW succeeded.
                return OperationStatus::SUCCESS;
            } else {
                // Must retry as RCU.
                goto create_record;
            }
        } else {
            // Must retry as RCU.
            goto create_record;
        }
    }
    // Acquire necessary locks.
    switch (phase) {
        case Phase::PREPARE:
            // Working on old version (v).
            if (!lock_guard.try_lock_old()) {
                // If we're retrying the operation, then we already have an old lock, so we'll always
                // succeed in obtaining a second. Otherwise, another thread has acquired the new lock, so
                // a CPR shift has occurred.
                assert(!retrying);
                pending_context.go_async(phase, version, address, expected_entry);
                return OperationStatus::CPR_SHIFT_DETECTED;
            } else {
                if (latest_record_version > version) {
                    // CPR shift detected: we are in the "PREPARE" phase, and a mutable record has a version
                    // later than what we've seen.
                    assert(!retrying);
                    pending_context.go_async(phase, version, address, expected_entry);
                    return OperationStatus::CPR_SHIFT_DETECTED;
                }
            }
            break;
        case Phase::IN_PROGRESS:
            // All other threads are in phase {PREPARE,IN_PROGRESS,WAIT_PENDING}.
            if (latest_record_version < version) {
                // Will create new record or update existing record to new version (v+1).
                if (!lock_guard.try_lock_new()) {
                    if (!retrying) {
                        pending_context.go_async(phase, version, address, expected_entry);
                    } else {
                        pending_context.continue_async(address, expected_entry);
                    }
                    return OperationStatus::RETRY_LATER;
                } else {
                    // Update to new version (v+1) requires RCU.
                    goto create_record;
                }
            }
            break;
        case Phase::WAIT_PENDING:
            // All other threads are in phase {IN_PROGRESS,WAIT_PENDING,WAIT_FLUSH}.
            if (latest_record_version < version) {
                if (lock_guard.old_locked()) {
                    if (!retrying) {
                        pending_context.go_async(phase, version, address, expected_entry);
                    } else {
                        pending_context.continue_async(address, expected_entry);
                    }
                    return OperationStatus::RETRY_LATER;
                } else {
                    // Update to new version (v+1) requires RCU.
                    goto create_record;
                }
            }
            break;
        case Phase::WAIT_FLUSH:
            // All other threads are in phase {WAIT_PENDING,WAIT_FLUSH,PERSISTENCE_CALLBACK}.
            if (latest_record_version < version) {
                goto create_record;
            }
            break;
        default:
            break;
    }

    if (address >= read_only_address) {
        // Mutable region. Try to update in place.
        if (atomic_entry->load() != expected_entry) {
            // Some other thread may have RCUed the record before we locked it; try again.
            return OperationStatus::RETRY_NOW;
        }
        // We acquired the necessary locks, so so we can update the record's bucket atomically.
        if (!expected_info.tombtone() && pending_context.value_length() <= expected_info.value_length()) {
            record_t *record = reinterpret_cast<record_t *>(thlog[k]->Get(address));
            if (pending_context.RmwAtomic(record)) {
                // In-place RMW succeeded.
                return OperationStatus::SUCCESS;
            } else {
                // Must retry as RCU.
                goto create_record;
            }
        } else {
            // Must retry as RCU.
            goto create_record;
        }
    } else if (address >= safe_read_only_address &&
               !expected_info.tombtone()) {
        // Fuzzy Region: Must go pending due to lost-update anomaly
        if (!retrying) {
            pending_context.go_async(phase, version, address, expected_entry);
        } else {
            pending_context.continue_async(address, expected_entry);
        }
        return OperationStatus::RETRY_LATER;
    } else if (address >= head_address) {
        goto create_record;
    } else if (address >= begin_address) {
        // Need to obtain old record from disk.
        if (!retrying) {
            pending_context.go_async(phase, version, address, expected_entry);
        } else {
            pending_context.continue_async(address, expected_entry);
        }
        return OperationStatus::RECORD_ON_DISK;
    } else {
        // Create a new record.
        goto create_record;
    }

    // Create a record and attempt RCU.
    create_record:
    const record_t *old_record = nullptr;
    if (address >= head_address) {
        old_record = reinterpret_cast<const record_t *>(thlog[k]->Get(address));
        if (expected_info.tombtone()) {
            old_record = nullptr;
        }
    }
    //uint32_t record_size = old_record != nullptr ?
    //                      record_t::size(key, pending_context.value_size(old_record)) :
    //                    record_t::size(key, pending_context.value_size());
    uint32_t record_size = record_t::size(key, pending_context.value_size());
    Address new_address = BlockAllocateT(record_size, j);
    record_t *new_record = reinterpret_cast<record_t *>(thlog[j]->Get(new_address));

    // Allocating a block may have the side effect of advancing the head address.
    head_address = thlog[k]->head_address.load();
    // Allocating a block may have the side effect of advancing the thread context's version and
    // phase.
    if (!retrying) {
        phase = thread_ctx().phase;
        version = thread_ctx().version;
    }
    new(new_record) record_t{
            RecordInfo{
                    static_cast<uint16_t>(thread_ctx().version), true, false, false,
                    expected_entry.address()}};
    if (!key_flag)
        key.Copy(atomic_entry->GetKey());
    if (old_record == nullptr || address < thlog[k]->begin_address.load()) {
        pending_context.RmwInitial(new_record);
    } else if (address >= head_address) {
        pending_context.RmwCopy(old_record, new_record);
    } else {
        // The block we allocated for the new record caused the head address to advance beyond
        // the old record. Need to obtain the old record from disk.
        new_record->header.invalid = true;
        if (!retrying) {
            pending_context.go_async(phase, version, address, expected_entry);
        } else {
            pending_context.continue_async(address, expected_entry);
        }
        return OperationStatus::RECORD_ON_DISK;
    }
    HashBucketEntry updated_entry{new_address, hash.tag(), false};
    HashInfo updated_info{static_cast<uint16_t>(thread_ctx().version), pending_context.value_length(), key.length(), 0};
    atom_t compared[2], exchanged[2];
    exchanged[0] = updated_entry.control_;
    exchanged[1] = updated_info.control_;
    compared[0] = expected_entry.control_;
    compared[1] = expected_info.control_;
    if (atomic_entry->compare_exchange_strong(exchanged, compared)) {
        return OperationStatus::SUCCESS;
    } else {
        // CAS failed; try again.
        new_record->header.invalid = true;
        if (!retrying) {
            pending_context.go_async(phase, version, address, expected_entry);
        } else {
            pending_context.continue_async(address, expected_entry);
        }
        return OperationStatus::RETRY_NOW;
    }
}

template<class K, class V, class D>
inline OperationStatus FasterKv<K, V, D>::InternalRetryPendingRmw(
        async_pending_rmw_context_t &pending_context) {
    OperationStatus status = InternalRmw(pending_context, true);
    if (status == OperationStatus::SUCCESS && pending_context.version != thread_ctx().version) {
        status = OperationStatus::SUCCESS_UNMARK;
    }
    return status;
}

template<class K, class V, class D>
template<class C>
inline OperationStatus FasterKv<K, V, D>::InternalDelete(C &pending_context) {
    typedef C pending_delete_context_t;

    if (thread_ctx().phase != Phase::REST) {
        HeavyEnter();
    }

    const key_t &key = pending_context.key();
    KeyHash hash = key.GetHash();
    HashBucketEntry expected_entry;
    AtomicHashBucketEntry *atomic_entry;
    //= const_cast<AtomicHashBucketEntry *>(FindEntry(hash, expected_entry));
    if (!atomic_entry) {
        // no record found
        return OperationStatus::NOT_FOUND;
    }

    Address address = expected_entry.address();
    Address head_address = hlog.head_address.load();
    Address read_only_address = hlog.read_only_address.load();
    Address begin_address = hlog.begin_address.load();
    uint64_t latest_record_version = 0;

    if (address >= head_address) {
        const record_t *record = reinterpret_cast<const record_t *>(hlog.Get(address));
        latest_record_version = record->header.checkpoint_version;
        if (key != record->key()) {
            address = TraceBackForKeyMatch(key, record->header.previous_address(), head_address);
        }
    }

    CheckpointLockGuard lock_guard{checkpoint_locks_, hash};

    // NO optimization for most common case

    // Acquire necessary locks.
    switch (thread_ctx().phase) {
        case Phase::PREPARE:
            // Working on old version (v).
            if (!lock_guard.try_lock_old()) {
                pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
                return OperationStatus::CPR_SHIFT_DETECTED;
            } else if (latest_record_version > thread_ctx().version) {
                // CPR shift detected: we are in the "PREPARE" phase, and a record has a version later than
                // what we've seen.
                pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
                return OperationStatus::CPR_SHIFT_DETECTED;
            }
            break;
        case Phase::IN_PROGRESS:
            // All other threads are in phase {PREPARE,IN_PROGRESS,WAIT_PENDING}.
            if (latest_record_version < thread_ctx().version) {
                // Will create new record or update existing record to new version (v+1).
                if (!lock_guard.try_lock_new()) {
                    pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
                    return OperationStatus::RETRY_LATER;
                } else {
                    // Update to new version (v+1) requires RCU.
                    goto create_record;
                }
            }
            break;
        case Phase::WAIT_PENDING:
            // All other threads are in phase {IN_PROGRESS,WAIT_PENDING,WAIT_FLUSH}.
            if (latest_record_version < thread_ctx().version) {
                if (lock_guard.old_locked()) {
                    pending_context.go_async(thread_ctx().phase, thread_ctx().version, address, expected_entry);
                    return OperationStatus::RETRY_LATER;
                } else {
                    // Update to new version (v+1) requires RCU.
                    goto create_record;
                }
            }
            break;
        case Phase::WAIT_FLUSH:
            // All other threads are in phase {WAIT_PENDING,WAIT_FLUSH,PERSISTENCE_CALLBACK}.
            if (latest_record_version < thread_ctx().version) {
                goto create_record;
            }
            break;
        default:
            break;
    }

    // Mutable Region: Update the record in-place
    if (address >= read_only_address) {
        record_t *record = reinterpret_cast<record_t *>(hlog.Get(address));
        // If the record is the head of the hash chain, try to update the hash chain and completely
        // elide record only if the previous address points to invalid address
        if (expected_entry.address() == address) {
            Address previous_address = record->header.previous_address();
            if (previous_address < begin_address) {
                //atomic_entry->compare_exchange_strong(expected_entry, HashBucketEntry::kInvalidEntry);
            }
        }
        record->header.tombstone = true;
        return OperationStatus::SUCCESS;
    }

    create_record:
    uint32_t record_size = record_t::size(key, pending_context.value_size());
    Address new_address = BlockAllocate(record_size);
    record_t *record = reinterpret_cast<record_t *>(hlog.Get(new_address));
    new(record) record_t{
            RecordInfo{
                    static_cast<uint16_t>(thread_ctx().version), true, true, false,
                    expected_entry.address()},
            key};

    HashBucketEntry updated_entry{new_address, hash.tag(), false};

    /*if (atomic_entry->compare_exchange_strong(expected_entry, updated_entry)) {
        // Installed the new record in the hash table.
        return OperationStatus::SUCCESS;
    } else {
        // Try again.
        record->header.invalid = true;
        return OperationStatus::RETRY_NOW;
    }*/
}

template<class K, class V, class D>
inline Address FasterKv<K, V, D>::TraceBackForKeyMatch(const key_t &key, Address from_address,
                                                       Address min_offset) const {
    uint32_t h = from_address.h();
    while (from_address >= thlog[h]->head_address.load()) {
        const record_t *record = reinterpret_cast<const record_t *>(thlog[h]->Get(from_address));
        if (key == record->key()) {
            return from_address;
        } else {
            from_address = record->header.previous_address();
            h = from_address.h();
            continue;
        }
    }
    return from_address;
}

template<class K, class V, class D>
inline Address FasterKv<K, V, D>::TraceBackForKeyMatchH(const key_t &key, Address from_address,
                                                        Address min_offset) const {
    while (from_address >= min_offset) {
        const record_t *record = reinterpret_cast<const record_t *>(hlog.Get(from_address));
        if (key == record->key()) {
            return from_address;
        } else {
            from_address = record->header.previous_address();
            continue;
        }
    }
    return from_address;
}

template<class K, class V, class D>
inline bool FasterKv<K, V, D>::TraceBackForGC(Address from_address, vector<Address> &vec) const {
    std::map<key_t, Address> map;
    vector<key_t> w;
    size_t size = map.size();
    uint32_t h = from_address.h();
    while (from_address >= thlog[h]->head_address.load()) {
        const record_t *record = reinterpret_cast<const record_t *>(thlog[h]->Get(from_address));
        if (w.size() == 0) {
            w.push_back(record->key());
            vec.push_back(from_address);
        }
        for (int i = 0; i < w.size(); i++) {
            if (record->key() == w[i]) {
                break;
            }
            if (i == w.size() - 1) {
                w.push_back(record->key());
                vec.push_back(from_address);
            }
        }
        //map.insert(record->key(),from_address);
        from_address = record->header.previous_address();
        h = from_address.h();
        //if(map.size()!=size){
        //
    }
    if (vec.size() == 1)
        return true;
    else
        return false;
}

template<class K, class V, class D>
inline Status FasterKv<K, V, D>::HandleOperationStatus(ExecutionContext &ctx,
                                                       pending_context_t &pending_context,
                                                       OperationStatus internal_status, bool &async) {
    async = false;
    switch (internal_status) {
        case OperationStatus::RETRY_NOW:
            switch (pending_context.type) {
                case OperationType::Read: {
                    async_pending_read_context_t &read_context =
                            *static_cast<async_pending_read_context_t *>(&pending_context);
                    internal_status = InternalRead(read_context);
                    break;
                }
                case OperationType::Upsert: {
                    async_pending_upsert_context_t &upsert_context =
                            *static_cast<async_pending_upsert_context_t *>(&pending_context);
                    internal_status = InternalUpsert(upsert_context);
                    break;
                }
                case OperationType::Delete: {
                    async_pending_delete_context_t &delete_context =
                            *static_cast<async_pending_delete_context_t *>(&pending_context);
                    internal_status = InternalDelete(delete_context);
                    break;
                }
                case OperationType::RMW: {
                    async_pending_rmw_context_t &rmw_context =
                            *static_cast<async_pending_rmw_context_t *>(&pending_context);
                    internal_status = InternalRmw(rmw_context, false);
                    break;
                }
            }

            if (internal_status == OperationStatus::SUCCESS) {
                return Status::Ok;
            } else if (internal_status == OperationStatus::NOT_FOUND) {
                return Status::NotFound;
            } else {
                return HandleOperationStatus(ctx, pending_context, internal_status, async);
            }
        case OperationStatus::RETRY_LATER:
            if (thread_ctx().phase == Phase::PREPARE) {
                assert(pending_context.type == OperationType::RMW);
                // Can I be marking an operation again and again?
                if (!checkpoint_locks_.get_lock(pending_context.key().GetHash()).try_lock_old()) {
                    return PivotAndRetry(ctx, pending_context, async);
                }
            }
            return RetryLater(ctx, pending_context, async);
        case OperationStatus::RECORD_ON_DISK:
            if (thread_ctx().phase == Phase::PREPARE) {
                assert(pending_context.type == OperationType::Read ||
                       pending_context.type == OperationType::RMW);
                // Can I be marking an operation again and again?
                if (!checkpoint_locks_.get_lock(pending_context.key().GetHash()).try_lock_old()) {
                    return PivotAndRetry(ctx, pending_context, async);
                }
            }
            return IssueAsyncIoRequest(ctx, pending_context, async);
        case OperationStatus::SUCCESS_UNMARK:
            checkpoint_locks_.get_lock(pending_context.key().GetHash()).unlock_old();
            return Status::Ok;
        case OperationStatus::NOT_FOUND_UNMARK:
            checkpoint_locks_.get_lock(pending_context.key().GetHash()).unlock_old();
            return Status::NotFound;
        case OperationStatus::CPR_SHIFT_DETECTED:
            return PivotAndRetry(ctx, pending_context, async);
    }
    // not reached
    assert(false);
    return Status::Corruption;
}

template<class K, class V, class D>
inline Status FasterKv<K, V, D>::PivotAndRetry(ExecutionContext &ctx,
                                               pending_context_t &pending_context, bool &async) {
    // Some invariants
    assert(ctx.version == thread_ctx().version);
    assert(thread_ctx().phase == Phase::PREPARE);
    Refresh();
    // thread must have moved to IN_PROGRESS phase
    assert(thread_ctx().version == ctx.version + 1);
    // retry with new contexts
    pending_context.phase = thread_ctx().phase;
    pending_context.version = thread_ctx().version;
    return HandleOperationStatus(thread_ctx(), pending_context, OperationStatus::RETRY_NOW, async);
}

template<class K, class V, class D>
inline Status FasterKv<K, V, D>::RetryLater(ExecutionContext &ctx,
                                            pending_context_t &pending_context, bool &async) {
    IAsyncContext *context_copy;
    Status result = pending_context.DeepCopy(context_copy);
    if (result == Status::Ok) {
        async = true;
        ctx.retry_requests.push_back(context_copy);
        return Status::Pending;
    } else {
        async = false;
        return result;
    }
}

template<class K, class V, class D>
inline constexpr uint32_t FasterKv<K, V, D>::MinIoRequestSize() const {
    return static_cast<uint32_t>(
            sizeof(value_t) + pad_alignment(record_t::min_disk_key_size(),
                                            alignof(value_t)));
}

template<class K, class V, class D>
inline Status FasterKv<K, V, D>::IssueAsyncIoRequest(ExecutionContext &ctx,
                                                     pending_context_t &pending_context, bool &async) {
    // Issue asynchronous I/O request
    uint64_t io_id = thread_ctx().io_id++;
    thread_ctx().pending_ios.insert({io_id, pending_context.key().GetHash()});
    async = true;
    AsyncIOContext io_request{this, pending_context.address, &pending_context,
                              &thread_ctx().io_responses, io_id};
    AsyncGetFromDisk(pending_context.address, MinIoRequestSize(), AsyncGetFromDiskCallback,
                     io_request);
    return Status::Pending;
}

template<class K, class V, class D>
inline Address FasterKv<K, V, D>::BlockAllocate(uint32_t record_size) {
    uint32_t page;
    Address retval = hlog.Allocate(record_size, page);
    while (retval < hlog.read_only_address.load()) {
        Refresh();
        // Don't overrun the hlog's tail offset.
        bool page_closed = (retval == Address::kInvalidAddress);
        while (page_closed) {
            page_closed = !hlog.NewPage(page);
            Refresh();
        }
        retval = hlog.Allocate(record_size, page);
    }
    return retval;
}

template<class K, class V, class D>
inline Address FasterKv<K, V, D>::BlockAllocateT(uint32_t record_size, uint32_t j) {
    uint32_t page;
    Address retval = thlog[j]->Allocate(record_size, page);
    retval += Address{0, 0, j}.control();
    while (retval < thlog[j]->read_only_address.load()) {
        Refresh();
        Address test = Address{0, 0, j}.control() + Address::kInvalidAddress;
        // Don't overrun the hlog's tail offset.
        //bool page_closed = (retval == Address::kInvalidAddress);
        bool page_closed = (retval == test);
        while (page_closed) {
            page_closed = !thlog[j]->NewPage(page);
            Refresh();
        }
        retval = thlog[j]->Allocate(record_size, page);
        retval += Address{0, 0, j}.control();
    }
    return retval;
}

template<class K, class V, class D>
void FasterKv<K, V, D>::AsyncGetFromDisk(Address address, uint32_t num_records,
                                         AsyncIOCallback callback, AsyncIOContext &context) {
    if (epoch_.IsProtected()) {
        /// Throttling. (Thread pool, unprotected threads are not throttled.)
        while (num_pending_ios.load() > 120) {
            disk.TryComplete();
            std::this_thread::yield();
            epoch_.ProtectAndDrain();
        }
    }
    ++num_pending_ios;
    uint32_t h = address.h();
    thlog[h]->AsyncGetFromDisk(address, num_records, callback, context);
}

template<class K, class V, class D>
void FasterKv<K, V, D>::AsyncGetFromDiskCallback(IAsyncContext *ctxt, Status result,
                                                 size_t bytes_transferred) {
    CallbackContext<AsyncIOContext> context{ctxt};
    faster_t *faster = reinterpret_cast<faster_t *>(context->faster);
    /// Context stack is: AsyncIOContext, PendingContext.
    pending_context_t *pending_context = static_cast<pending_context_t *>(context->caller_context);

    /// This I/O is finished.
    --faster->num_pending_ios;
    /// Always "goes async": context is freed by the issuing thread, when processing thread I/O
    /// responses.
    context.async = true;

    pending_context->result = result;
    if (result == Status::Ok) {
        record_t *record = reinterpret_cast<record_t *>(context->record.GetValidPointer());
        // Size of the record we read from disk (might not have read the entire record, yet).
        size_t record_size = context->record.available_bytes;
        if (record->min_disk_key_size() > record_size) {
            // Haven't read the full record in yet; I/O is not complete!
            faster->AsyncGetFromDisk(context->address, record->min_disk_key_size(),
                                     AsyncGetFromDiskCallback, *context.get());
            context.async = true;
        } else if (record->min_disk_value_size() > record_size) {
            // Haven't read the full record in yet; I/O is not complete!
            faster->AsyncGetFromDisk(context->address, record->min_disk_value_size(),
                                     AsyncGetFromDiskCallback, *context.get());
            context.async = true;
        } else if (record->disk_size() > record_size) {
            // Haven't read the full record in yet; I/O is not complete!
            faster->AsyncGetFromDisk(context->address, record->disk_size(),
                                     AsyncGetFromDiskCallback, *context.get());
            context.async = true;
        } else if (pending_context->key() == record->key()) {
            //The keys are same, so I/O is complete
            context->thread_io_responses->push(context.get());
        } else {
            //keys are not same. I/O is not complete
            context->address = record->header.previous_address();
            if (context->address >= faster->hlog.begin_address.load()) {
                faster->AsyncGetFromDisk(context->address, faster->MinIoRequestSize(),
                                         AsyncGetFromDiskCallback, *context.get());
                context.async = true;
            } else {
                // Record not found, so I/O is complete.
                context->thread_io_responses->push(context.get());
            }
        }
    }
}

template<class K, class V, class D>
OperationStatus FasterKv<K, V, D>::InternalContinuePendingRead(ExecutionContext &context,
                                                               AsyncIOContext &io_context) {
    if (io_context.address >= hlog.begin_address.load()) {
        async_pending_read_context_t *pending_context = static_cast<async_pending_read_context_t *>(
                io_context.caller_context);
        record_t *record = reinterpret_cast<record_t *>(io_context.record.GetValidPointer());
        if (record->header.tombstone) {
            return (thread_ctx().version > context.version) ? OperationStatus::NOT_FOUND_UNMARK :
                   OperationStatus::NOT_FOUND;
        }
        pending_context->Get(record);
        assert(!kCopyReadsToTail);
        return (thread_ctx().version > context.version) ? OperationStatus::SUCCESS_UNMARK :
               OperationStatus::SUCCESS;
    } else {
        return (thread_ctx().version > context.version) ? OperationStatus::NOT_FOUND_UNMARK :
               OperationStatus::NOT_FOUND;
    }
}

template<class K, class V, class D>
OperationStatus FasterKv<K, V, D>::InternalContinuePendingRmw(ExecutionContext &context,
                                                              AsyncIOContext &io_context) {
    async_pending_rmw_context_t *pending_context = static_cast<async_pending_rmw_context_t *>(
            io_context.caller_context);

    // Find a hash bucket entry to store the updated value in.
    const key_t &key = pending_context->key();
    KeyHash hash = key.GetHash();
    HashBucketEntry expected_entry;
    AtomicHashBucketEntry *atomic_entry;
    //= FindOrCreateEntry(hash, expected_entry);

    // (Note that address will be Address::kInvalidAddress, if the atomic_entry was created.)
    Address address = expected_entry.address();
    Address head_address = hlog.head_address.load();

    // Make sure that atomic_entry is OK to update.
    if (address >= head_address) {
        record_t *record = reinterpret_cast<record_t *>(hlog.Get(address));
        if (key != record->key()) {
            address = TraceBackForKeyMatch(key, record->header.previous_address(), head_address);
        }
    }

    if (address > pending_context->entry.address()) {
        // We can't trace the current hash bucket entry back to the record we read.
        pending_context->continue_async(address, expected_entry);
        return OperationStatus::RETRY_NOW;
    }
    assert(address < hlog.begin_address.load() || address == pending_context->entry.address());

    // We have to do copy-on-write/RCU and write the updated value to the tail of the log.
    Address new_address;
    record_t *new_record;
    if (io_context.address < hlog.begin_address.load()) {
        // The on-disk trace back failed to find a key match.
        uint32_t record_size = record_t::size(key, pending_context->value_size());
        new_address = BlockAllocate(record_size);
        new_record = reinterpret_cast<record_t *>(hlog.Get(new_address));

        new(new_record) record_t{
                RecordInfo{
                        static_cast<uint16_t>(context.version), true, false, false,
                        expected_entry.address()},
                key};
        pending_context->RmwInitial(new_record);
    } else {
        // The record we read from disk.
        const record_t *disk_record = reinterpret_cast<const record_t *>(
                io_context.record.GetValidPointer());
        uint32_t record_size = record_t::size(key, pending_context->value_size(disk_record));
        new_address = BlockAllocate(record_size);
        new_record = reinterpret_cast<record_t *>(hlog.Get(new_address));

        new(new_record) record_t{
                RecordInfo{
                        static_cast<uint16_t>(context.version), true, false, false,
                        expected_entry.address()},
                key};
        pending_context->RmwCopy(disk_record, new_record);
    }

    HashBucketEntry updated_entry{new_address, hash.tag(), false};
    /*if (atomic_entry->compare_exchange_strong(expected_entry, updated_entry)) {
        assert(thread_ctx().version >= context.version);
        return (thread_ctx().version == context.version) ? OperationStatus::SUCCESS :
               OperationStatus::SUCCESS_UNMARK;
    } else {
        // CAS failed; try again.
        new_record->header.invalid = true;
        pending_context->continue_async(address, expected_entry);
        return OperationStatus::RETRY_NOW;
    }*/
}

template<class K, class V, class D>
void FasterKv<K, V, D>::InitializeCheckpointLocks() {
    uint32_t table_version = resize_info_.version;
    uint64_t size = state_[table_version].size();
    checkpoint_locks_.Initialize(size);
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::WriteIndexMetadata() {
    std::string filename = disk.version_path(system_state_.load().version) + "info.dat";
    // (This code will need to be refactored into the disk_t interface, if we want to support
    // unformatted disks.)
    std::FILE *file = std::fopen(filename.c_str(), "wb");
    if (!file) {
        return Status::IOError;
    }
    if (std::fwrite(&checkpoint_.index_metadata, sizeof(checkpoint_.index_metadata), 1, file) != 1) {
        std::fclose(file);
        return Status::IOError;
    }
    if (std::fclose(file) != 0) {
        return Status::IOError;
    }
    return Status::Ok;
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::ReadIndexMetadata(const Guid &token) {
    std::string filename = disk.index_checkpoint_path(token) + "info.dat";
    // (This code will need to be refactored into the disk_t interface, if we want to support
    // unformatted disks.)
    std::FILE *file = std::fopen(filename.c_str(), "rb");
    if (!file) {
        return Status::IOError;
    }
    if (std::fread(&checkpoint_.index_metadata, sizeof(checkpoint_.index_metadata), 1, file) != 1) {
        std::fclose(file);
        return Status::IOError;
    }
    if (std::fclose(file) != 0) {
        return Status::IOError;
    }
    return Status::Ok;
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::ReadIndexVersionMetadata(uint32_t version) {
    std::string filename = disk.version_path(version) + "info.dat";
    // (This code will need to be refactored into the disk_t interface, if we want to support
    // unformatted disks.)
    std::FILE *file = std::fopen(filename.c_str(), "rb");
    if (!file) {
        return Status::IOError;
    }
    if (std::fread(&checkpoint_.index_metadata, sizeof(checkpoint_.index_metadata), 1, file) != 1) {
        std::fclose(file);
        return Status::IOError;
    }
    if (std::fclose(file) != 0) {
        return Status::IOError;
    }
    return Status::Ok;
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::WriteCprMetadata() {
    std::string filename = disk.cpr_checkpoint_path(checkpoint_.hybrid_log_token) + "info.dat";
    // (This code will need to be refactored into the disk_t interface, if we want to support
    // unformatted disks.)
    std::FILE *file = std::fopen(filename.c_str(), "wb");
    if (!file) {
        return Status::IOError;
    }
    if (std::fwrite(&checkpoint_.log_metadata, sizeof(checkpoint_.log_metadata), 1, file) != 1) {
        std::fclose(file);
        return Status::IOError;
    }
    if (std::fclose(file) != 0) {
        return Status::IOError;
    }
    return Status::Ok;
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::ReadCprMetadata(const Guid &token) {
    std::string filename = disk.cpr_checkpoint_path(token) + "info.dat";
    // (This code will need to be refactored into the disk_t interface, if we want to support
    // unformatted disks.)
    std::FILE *file = std::fopen(filename.c_str(), "rb");
    if (!file) {
        return Status::IOError;
    }
    if (std::fread(&checkpoint_.log_metadata, sizeof(checkpoint_.log_metadata), 1, file) != 1) {
        std::fclose(file);
        return Status::IOError;
    }
    if (std::fclose(file) != 0) {
        return Status::IOError;
    }
    return Status::Ok;
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::WriteCprContext() {
    std::string filename = disk.cpr_checkpoint_path(checkpoint_.hybrid_log_token);
    const Guid &guid = prev_thread_ctx().guid;
    filename += guid.ToString();
    filename += ".dat";
    // (This code will need to be refactored into the disk_t interface, if we want to support
    // unformatted disks.)
    std::FILE *file = std::fopen(filename.c_str(), "wb");
    if (!file) {
        return Status::IOError;
    }
    if (std::fwrite(static_cast<PersistentExecContext *>(&prev_thread_ctx()),
                    sizeof(PersistentExecContext), 1, file) != 1) {
        std::fclose(file);
        return Status::IOError;
    }
    if (std::fclose(file) != 0) {
        return Status::IOError;
    }
    return Status::Ok;
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::ReadCprContexts(const Guid &token, const Guid *guids) {
    for (size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
        const Guid &guid = guids[idx];
        if (guid == Guid{}) {
            continue;
        }
        std::string filename = disk.cpr_checkpoint_path(token);
        filename += guid.ToString();
        filename += ".dat";
        // (This code will need to be refactored into the disk_t interface, if we want to support
        // unformatted disks.)
        std::FILE *file = std::fopen(filename.c_str(), "rb");
        if (!file) {
            return Status::IOError;
        }
        PersistentExecContext context{};
        if (std::fread(&context, sizeof(PersistentExecContext), 1, file) != 1) {
            std::fclose(file);
            return Status::IOError;
        }
        if (std::fclose(file) != 0) {
            return Status::IOError;
        }
        auto result = checkpoint_.continue_tokens.insert({context.guid, context.serial_num});
        assert(result.second);
    }
    if (checkpoint_.continue_tokens.size() != checkpoint_.log_metadata.num_threads) {
        return Status::Corruption;
    } else {
        return Status::Ok;
    }
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::CheckpointFuzzyIndex() {
    uint32_t hash_table_version = resize_info_.version;
    /*for (int i = 0; i < tlog_number; i++) {
        // Checkpoint the main hash table.
        file_t ht_file = disk.NewFile(disk.relative_index_checkpoint_path(checkpoint_.index_token) + std::to_string(i) +
                                      "ht.dat");
        RETURN_NOT_OK(ht_file.Open(&disk.handler()));
        RETURN_NOT_OK(state_[i].Checkpoint(disk, std::move(ht_file),
                                           checkpoint_.index_metadata.num_ht_bytes));
    }*/
    // Checkpoint the hash table's overflow buckets.
    file_t ofb_file = disk.NewFile(disk.relative_version_path(system_state_.load().version) +
                                   "ofb.dat");
    RETURN_NOT_OK(ofb_file.Open(&disk.handler()));
    RETURN_NOT_OK(overflow_buckets_allocator_[hash_table_version].Checkpoint(disk,
                                                                             std::move(ofb_file),
                                                                             checkpoint_.index_metadata.num_ofb_bytes));
    checkpoint_.index_checkpoint_started = true;
    return Status::Ok;
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::CheckpointFuzzyIndexComplete() {
    if (!checkpoint_.index_checkpoint_started) {
        return Status::Pending;
    }
    uint32_t hash_table_version = resize_info_.version;
    if(system_state_.load().version == 1){
    Status result = state_[hash_table_version].CheckpointComplete(false);
    if (result == Status::Pending) {
        return Status::Pending;
    } else if (result != Status::Ok) {
        return result;
    } else {
        return overflow_buckets_allocator_[hash_table_version].CheckpointComplete(false);
    }
    } else
        return overflow_buckets_allocator_[hash_table_version].CheckpointComplete(false);
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::RecoverFuzzyIndex(int i) {
    //uint8_t hash_table_version = resize_info_.version;
    uint8_t hash_table_version = i;
    assert(state_[hash_table_version].size() == checkpoint_.index_metadata.table_size);
    //checkpoint_.index_token = Guid::Parse("5cb1a9cd-3c75-4f3b-9ff1-13b5194c2e8b");
    // Recover the main hash table.
    file_t ht_file = disk.NewFile(disk.relative_version_path(1) +std::to_string(i)
            +"ht.dat");
    RETURN_NOT_OK(ht_file.Open(&disk.handler()));
    RETURN_NOT_OK(state_[hash_table_version].Recover(disk, std::move(ht_file),
                                                     checkpoint_.index_metadata.num_ht_bytes));
    // Recover the hash table's overflow buckets.
    if (i == 0) {
        file_t ofb_file = disk.NewFile(disk.relative_version_path(1) +
                                       "ofb.dat");
        RETURN_NOT_OK(ofb_file.Open(&disk.handler()));
        return overflow_buckets_allocator_[hash_table_version].Recover(disk, std::move(ofb_file),
                                                                       checkpoint_.index_metadata.num_ofb_bytes,
                                                                       checkpoint_.index_metadata.ofb_count);

    }
}
template<class K, class V, class D>
Status FasterKv<K, V, D>::RecoverFuzzyIndexComplete(bool wait,int i) {
    //uint8_t hash_table_version = resize_info_.version;
    uint8_t hash_table_version = i;
    Status result = state_[hash_table_version].RecoverComplete(true);
    if (result != Status::Ok) {
        return result;
    }
    if(i==0)
       result = overflow_buckets_allocator_[hash_table_version].RecoverComplete(true);
    if (result != Status::Ok) {
        return result;
    }

    // Clear all tentative entries.
    for (uint64_t bucket_idx = 0; bucket_idx < state_[hash_table_version].size(); ++bucket_idx) {
        HashBucket *bucket = &state_[hash_table_version].bucket(bucket_idx);
        while (true) {
            for (uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
                if (bucket->entries[entry_idx].load().tentative()) {
                    bucket->entries[entry_idx].store(HashBucketEntry::kInvalidEntry);
                }
            }
            // Go to next bucket in the chain
            HashBucketOverflowEntry entry = bucket->overflow_entry.load();
            if (entry.unused()) {
                // No more buckets in the chain.
                break;
            }
            bucket = &overflow_buckets_allocator_[hash_table_version].Get(entry.address());
            assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
        }
    }
    return Status::Ok;
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::RecoverHybridLog() {
    class Context : public IAsyncContext {
    public:
        /*
        Context(hlog_t &hlog_, uint32_t page_, RecoveryStatus &recovery_status_)
                : hlog{&hlog_}, page{page_}, recovery_status{&recovery_status_} {
        }
        */
        Context(hlog_t *hlog_, uint32_t page_, RecoveryStatus &recovery_status_)
                : hlog{hlog_}, page{page_}, recovery_status{&recovery_status_} {
        }

        /// The deep-copy constructor
        Context(const Context &other)
                : hlog{other.hlog}, page{other.page}, recovery_status{other.recovery_status} {
        }

    protected:
        Status DeepCopy_Internal(IAsyncContext *&context_copy) final {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

    public:
        hlog_t *hlog;
        uint32_t page;
        RecoveryStatus *recovery_status;
    };

    auto callback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<Context> context{ctxt};
        result = context->hlog->AsyncReadPagesFromLog(context->page, 1, *context->recovery_status);
    };

    //Address from_address = checkpoint_.index_metadata.checkpoint_start_address;
    //Address to_address = checkpoint_.log_metadata.final_address;
    Address from_address = checkpoint_.index_metadata.thlog_checkpoint_address[0];
    Address to_address = checkpoint_.log_metadata.tfinal_address[0];

    uint32_t start_page = from_address.page();
    uint32_t end_page = to_address.offset() > 0 ? to_address.page() + 1 : to_address.page();
    //uint32_t capacity = hlog.buffer_size();
    uint32_t capacity = thlog[0]->buffer_size();
    RecoveryStatus recovery_status{start_page, end_page};
    // Initially issue read request for all pages that can be held in memory
    uint32_t total_pages_to_read = end_page - start_page;
    uint32_t pages_to_read_first = std::min(capacity, total_pages_to_read);
    RETURN_NOT_OK(thlog[0]->AsyncReadPagesFromLog(start_page, pages_to_read_first, recovery_status));

    for (uint32_t page = start_page; page < end_page; ++page) {
        while (recovery_status.page_status(page) != PageRecoveryStatus::ReadDone) {
            disk.TryComplete();
            std::this_thread::sleep_for(10ms);
        }

        // handle start and end at non-page boundaries
        RETURN_NOT_OK(RecoverFromPage(page == start_page ? from_address : Address{page, 0},
                                      page + 1 == end_page ? to_address :
                                      Address{page, Address::kMaxOffset}));

        // OS thread flushes current page and issues a read request if necessary
        if (page + capacity < end_page) {
            Context context{thlog[0], page + capacity, recovery_status};
            RETURN_NOT_OK(thlog[0]->AsyncFlushPage(page, recovery_status, callback, &context));
        } else {
            RETURN_NOT_OK(thlog[0]->AsyncFlushPage(page, recovery_status, nullptr, nullptr));
        }
    }
    // Wait until all pages have been flushed
    for (uint32_t page = start_page; page < end_page; ++page) {
        while (recovery_status.page_status(page) != PageRecoveryStatus::FlushDone) {
            disk.TryComplete();
            std::this_thread::sleep_for(10ms);
        }
    }
    return Status::Ok;
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::RecoverHybridLog1(uint16_t rec) {
    class Context : public IAsyncContext {
    public:
        /*
        Context(hlog_t &hlog_, uint32_t page_, RecoveryStatus &recovery_status_)
                : hlog{&hlog_}, page{page_}, recovery_status{&recovery_status_} {
        }
        */
        Context(hlog_t *hlog_, uint32_t page_, RecoveryStatus &recovery_status_)
                : hlog{hlog_}, page{page_}, recovery_status{&recovery_status_} {
        }

        /// The deep-copy constructor
        Context(const Context &other)
                : hlog{other.hlog}, page{other.page}, recovery_status{other.recovery_status} {
        }

    protected:
        Status DeepCopy_Internal(IAsyncContext *&context_copy) final {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

    public:
        hlog_t *hlog;
        uint32_t page;
        RecoveryStatus *recovery_status;
    };

    auto callback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<Context> context{ctxt};
        result = context->hlog->AsyncReadPagesFromLog(context->page, 1, *context->recovery_status);
    };

    //Address from_address = checkpoint_.index_metadata.checkpoint_start_address;
    //Address to_address = checkpoint_.log_metadata.final_address;
    Address from_address = checkpoint_.index_metadata.thlog_checkpoint_address[rec];
    Address to_address = checkpoint_.log_metadata.tfinal_address[rec];

    uint32_t start_page = from_address.page();
    uint32_t end_page = to_address.offset() > 0 ? to_address.page() + 1 : to_address.page();
    //uint32_t capacity = hlog.buffer_size();
    uint32_t capacity = thlog[rec]->buffer_size();
    RecoveryStatus recovery_status{start_page, end_page};
    // Initially issue read request for all pages that can be held in memory
    uint32_t total_pages_to_read = end_page - start_page;
    uint32_t pages_to_read_first = std::min(capacity, total_pages_to_read);
    RETURN_NOT_OK(thlog[rec]->AsyncReadPagesFromLog(start_page, pages_to_read_first, recovery_status));

    for (uint32_t page = start_page; page < end_page; ++page) {
        while (recovery_status.page_status(page) != PageRecoveryStatus::ReadDone) {
            disk.TryComplete();
            std::this_thread::sleep_for(10ms);
        }

        // handle start and end at non-page boundaries
        //  RETURN_NOT_OK(RecoverFromPage(page == start_page ? from_address : Address{page, 0},
        //                              page + 1 == end_page ? to_address :
        //                            Address{page, Address::kMaxOffset}));
        //RETURN_NOT_OK(RecoverFromPage1(page == start_page ? from_address : Address{page, 0},
         //                              page + 1 == end_page ? to_address :
          //                             Address{page, Address::kMaxOffset}, rec));

        // OS thread flushes current page and issues a read request if necessary
        if (page + capacity < end_page) {
            Context context{thlog[rec], page + capacity, recovery_status};
            RETURN_NOT_OK(thlog[rec]->AsyncFlushPage(page, recovery_status, callback, &context));
        } else {
            RETURN_NOT_OK(thlog[rec]->AsyncFlushPage(page, recovery_status, nullptr, nullptr));
        }
    }
    // Wait until all pages have been flushed
    for (uint32_t page = start_page; page < end_page; ++page) {
        while (recovery_status.page_status(page) != PageRecoveryStatus::FlushDone) {
            disk.TryComplete();
            std::this_thread::sleep_for(10ms);
        }
    }
    return Status::Ok;
}


template<class K, class V, class D>
Status FasterKv<K, V, D>::RecoverHybridLogFromSnapshotFile() {
    class Context : public IAsyncContext {
    public:
        Context(hlog_t &hlog_, file_t &file_, uint32_t file_start_page_, uint32_t page_,
                RecoveryStatus &recovery_status_)
                : hlog{&hlog_}, file{&file_}, file_start_page{file_start_page_}, page{page_},
                  recovery_status{&recovery_status_} {
        }

        /// The deep-copy constructor
        Context(const Context &other)
                : hlog{other.hlog}, file{other.file}, file_start_page{other.file_start_page}, page{other.page},
                  recovery_status{other.recovery_status} {
        }

    protected:
        Status DeepCopy_Internal(IAsyncContext *&context_copy) final {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

    public:
        hlog_t *hlog;
        file_t *file;
        uint32_t file_start_page;
        uint32_t page;
        RecoveryStatus *recovery_status;
    };

    auto callback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<Context> context{ctxt};
        result = context->hlog->AsyncReadPagesFromSnapshot(*context->file,
                                                           context->file_start_page, context->page, 1,
                                                           *context->recovery_status);
    };

    Address file_start_address = checkpoint_.log_metadata.flushed_address;
    Address from_address = checkpoint_.index_metadata.checkpoint_start_address;
    Address to_address = checkpoint_.log_metadata.final_address;

    uint32_t start_page = file_start_address.page();
    uint32_t end_page = to_address.offset() > 0 ? to_address.page() + 1 : to_address.page();
    uint32_t capacity = hlog.buffer_size();
    RecoveryStatus recovery_status{start_page, end_page};
    checkpoint_.snapshot_file = disk.NewFile(disk.relative_cpr_checkpoint_path(
            checkpoint_.hybrid_log_token) + "snapshot.dat");
    RETURN_NOT_OK(checkpoint_.snapshot_file.Open(&disk.handler()));

    // Initially issue read request for all pages that can be held in memory
    uint32_t total_pages_to_read = end_page - start_page;
    uint32_t pages_to_read_first = std::min(capacity, total_pages_to_read);
    RETURN_NOT_OK(hlog.AsyncReadPagesFromSnapshot(checkpoint_.snapshot_file, start_page, start_page,
                                                  pages_to_read_first, recovery_status));

    for (uint32_t page = start_page; page < end_page; ++page) {
        while (recovery_status.page_status(page) != PageRecoveryStatus::ReadDone) {
            disk.TryComplete();
            std::this_thread::sleep_for(10ms);
        }

        // Perform recovery if page in fuzzy portion of the log
        if (Address{page + 1, 0} > from_address) {
            // handle start and end at non-page boundaries
            RETURN_NOT_OK(RecoverFromPage(page == from_address.page() ? from_address :
                                          Address{page, 0},
                                          page + 1 == end_page ? to_address :
                                          Address{page, Address::kMaxOffset}));
        }

        // OS thread flushes current page and issues a read request if necessary
        if (page + capacity < end_page) {
            Context context{hlog, checkpoint_.snapshot_file, start_page, page + capacity,
                            recovery_status};
            RETURN_NOT_OK(hlog.AsyncFlushPage(page, recovery_status, callback, &context));
        } else {
            RETURN_NOT_OK(hlog.AsyncFlushPage(page, recovery_status, nullptr, nullptr));
        }
    }
    // Wait until all pages have been flushed
    for (uint32_t page = start_page; page < end_page; ++page) {
        while (recovery_status.page_status(page) != PageRecoveryStatus::FlushDone) {
            disk.TryComplete();
            std::this_thread::sleep_for(10ms);
        }
    }
    return Status::Ok;
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::RecoverFromPage(Address from_address, Address to_address) {
    assert(from_address.page() == to_address.page());
    for (Address address = from_address; address < to_address;) {
        record_t *record = reinterpret_cast<record_t *>(hlog.Get(address));
        if (record->header.IsNull()) {
            address += sizeof(record->header);
            continue;
        }
        if (record->header.invalid) {
            address += record->size();
            continue;
        }
        const key_t &key = record->key();
        KeyHash hash = key.GetHash();
        HashBucketEntry expected_entry;
        HashInfo expected_info;
        AtomicHashBucketEntry *atomic_entry = FindOrCreateEntry(key,hash, expected_entry,expected_info);

        if (record->header.checkpoint_version <= checkpoint_.log_metadata.version) {
            HashBucketEntry new_entry{address, hash.tag(), false};
            atomic_entry->store(new_entry);
        } else {
            record->header.invalid = true;
            if (record->header.previous_address() < checkpoint_.index_metadata.checkpoint_start_address) {
                HashBucketEntry new_entry{record->header.previous_address(), hash.tag(), false};
                atomic_entry->store(new_entry);
            }
        }
        address += record->size();
    }

    return Status::Ok;
}


template<class K, class V, class D>
Status FasterKv<K, V, D>::RecoverFromPage1(Address from_address, Address to_address, uint16_t rec) {
    assert(from_address.page() == to_address.page());
    uint16_t k = rec;
    if (from_address.h() != k)
        from_address += Address{0, 0, k}.control();
    if (to_address.h() != k)
        to_address += Address{0, 0, k}.control();
    for (Address address = from_address; address < to_address;) {
        record_t *record = reinterpret_cast<record_t *>(thlog[rec]->Get(address));
        int a = sizeof(record->header);
        if (record->header.IsNull()) {
            address += sizeof(record->header);
            continue;
        }
        if (record->header.invalid) {
            address += record->size();
            continue;
        }
        //address += record->size();
        //continue;
        const key_t &key = record->key();
        KeyHash hash = key.GetHash();
        HashBucketEntry expected_entry;
        HashBucket *bucket;
        AtomicHashBucketEntry *atomic_entry = FindOrCreateEntry(hash, expected_entry);

        if (record->header.checkpoint_version <= checkpoint_.log_metadata.version) {
            HashBucketEntry new_entry{address, hash.tag(), false};
            atomic_entry->store(new_entry);
        } else {
            record->header.invalid = true;
            if (record->header.previous_address() < checkpoint_.index_metadata.thlog_checkpoint_address[rec]) {
                HashBucketEntry new_entry{record->header.previous_address(), hash.tag(), false};
                atomic_entry->store(new_entry);
            }
        }
        address += record->size();
    }

    return Status::Ok;
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::RestoreHybridLog() {
    Address tail_address = checkpoint_.log_metadata.final_address;
    uint32_t end_page = tail_address.offset() > 0 ? tail_address.page() + 1 : tail_address.page();
    uint32_t capacity = hlog.buffer_size();
    // Restore as much of the log as will fit in memory.
    uint32_t start_page;
    if (end_page < capacity - hlog.kNumHeadPages) {
        start_page = 0;
    } else {
        start_page = end_page - (capacity - hlog.kNumHeadPages);
    }
    RecoveryStatus recovery_status{start_page, end_page};

    uint32_t num_pages = end_page - start_page;
    RETURN_NOT_OK(hlog.AsyncReadPagesFromLog(start_page, num_pages, recovery_status));

    // Wait until all pages have been read.
    for (uint32_t page = start_page; page < end_page; ++page) {
        while (recovery_status.page_status(page) != PageRecoveryStatus::ReadDone) {
            disk.TryComplete();
            std::this_thread::sleep_for(10ms);
        }
    }
    // Skip the null page.
    Address head_address = start_page == 0 ? Address{0, Constants::kCacheLineBytes} :
                           Address{start_page, 0};
    hlog.RecoveryReset(checkpoint_.index_metadata.log_begin_address, head_address, tail_address);
    return Status::Ok;
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::RestoreHybridLog1(uint16_t rec) {
    Address tail_address = checkpoint_.log_metadata.tfinal_address[rec];
    uint32_t end_page = tail_address.offset() > 0 ? tail_address.page() + 1 : tail_address.page();
    uint32_t capacity = thlog[rec]->buffer_size();
    // Restore as much of the log as will fit in memory.
    uint32_t start_page;
    if (end_page < capacity - thlog[rec]->kNumHeadPages) {
        start_page = 0;
    } else {
        start_page = end_page - (capacity - thlog[rec]->kNumHeadPages);
    }
    RecoveryStatus recovery_status{start_page, end_page};

    uint32_t num_pages = end_page - start_page;
    RETURN_NOT_OK(thlog[rec]->AsyncReadPagesFromLog(start_page, num_pages, recovery_status));

    // Wait until all pages have been read.
    for (uint32_t page = start_page; page < end_page; ++page) {
        while (recovery_status.page_status(page) != PageRecoveryStatus::ReadDone) {
            disk.TryComplete();
            std::this_thread::sleep_for(10ms);
        }
    }
    // Skip the null page.
    Address head_address = start_page == 0 ? Address{0, Constants::kCacheLineBytes} :
                           Address{start_page, 0};
    head_address += Address{0, 0, rec}.control();
    thlog[rec]->RecoveryReset(checkpoint_.index_metadata.thlog_begin_address[rec], head_address, tail_address);
    return Status::Ok;
}

template<class K, class V, class D>
void FasterKv<K, V, D>::HeavyEnter() {
    if (thread_ctx().phase == Phase::GC_IO_PENDING || thread_ctx().phase == Phase::GC_IN_PROGRESS) {
        CleanHashTableBuckets();
        return;
    }
    while (thread_ctx().phase == Phase::GROW_PREPARE) {
        // We spin-wait as a simplification
        // Could instead do a "heavy operation" here
        std::this_thread::yield();
        Refresh();
    }
    if (thread_ctx().phase == Phase::GROW_IN_PROGRESS) {
        SplitHashTableBuckets();
    }
}

template<class K, class V, class D>
void FasterKv<K, V, D>::CheckpointEntry(file_t &&file, int number, int count, int w) {
    class AsyncIoContext : public IAsyncContext {
    public:
        AsyncIoContext(AtomicHashBucketEntry entry_)
                : entry{entry_} {
        }

        /// The deep-copy constructor
        AsyncIoContext(AsyncIoContext &other)
                : entry{other.entry} {
        }

    protected:
        Status DeepCopy_Internal(IAsyncContext *&context_copy) final {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

    public:
        AtomicHashBucketEntry entry;
    };
    uint32_t chunk_size = static_cast<uint32_t>(state_[w].size() / Constants::kNumMergeChunks);
    uint32_t write_size = static_cast<uint32_t>(chunk_size * sizeof(HashBucket));
    uint32_t version = system_state_.load().version;
    file_t hash_file = std::move(file);
    hash_file.set_device(32);
    int card = Constants::kNumMergeChunks / count;
    uint32_t cw;
    int test = 0;
    if (number != (count - 1))
        cw = card;
    else
        cw = Constants::kNumMergeChunks - card * number;
    for (uint32_t i = card * number * chunk_size; i < (card * number + cw) * chunk_size; i++) {
        HashBucket *bucket = &state_[w].bucket(i);
        for (uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
            HashBucketEntry entry = bucket->entries[entry_idx].load();
            HashInfo expectded_info = bucket->entries[entry_idx].GetInfo();
            if (entry.unused()) {
                continue;
            }

            if (expectded_info.version() == version) {
                //cout<<sizeof(AtomicHashBucketEntry)<< sizeof(HashBucket)<<endl;
                AsyncIoContext context{bucket->entries[entry_idx]};
                Status s = hash_file.WriteAsync(&bucket->entries[entry_idx], test * sizeof(AtomicHashBucketEntry),
                                                sizeof(AtomicHashBucketEntry),
                                                nullptr, context);
                test++;
            }
        }
    }
    hash_file.Close();
    cout << test << endl;
}

template<class K, class V, class D>
bool FasterKv<K, V, D>::CleanHashTableBuckets() {
    uint64_t chunk = gc_.next_chunk++;
    if (chunk >= gc_.num_chunks) {
        // No chunk left to clean.
        return false;
    }
    uint8_t version = resize_info_.version;
    Address begin_address = hlog.begin_address.load();
    uint64_t upper_bound;
    if (chunk + 1 < gc_.num_chunks) {
        // All chunks but the last chunk contain kGrowHashTableChunkSize elements.
        upper_bound = kGrowHashTableChunkSize;
    } else {
        // Last chunk might contain more or fewer elements.
        upper_bound = state_[version].size() - (chunk * kGcHashTableChunkSize);
    }
    for (uint64_t idx = 0; idx < upper_bound; ++idx) {
        HashBucket *bucket = &state_[version].bucket(chunk * kGcHashTableChunkSize + idx);
        /*
        if (chunk * kGcHashTableChunkSize + idx == 54895135)
            int jj = 0;
        Address a(0, 112, 0);
        const record_t *record = reinterpret_cast<const record_t *>(thlog[0]->Get(a));
        a = thlog[0]->head_address.load();
        */
        while (true) {
            for (uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
                AtomicHashBucketEntry &atomic_entry = bucket->entries[entry_idx];
                HashBucketEntry expected_entry = atomic_entry.load();
                uint32_t k = expected_entry.address().h();
                if (!expected_entry.unused() && expected_entry.address() != Address::kInvalidAddress &&
                    expected_entry.address() < thlog[k]->gc_address.load()) {
                    // The record that this entry points to was truncated; try to delete the entry.
                    //atomic_entry.compare_exchange_strong(expected_entry, HashBucketEntry::kInvalidEntry);
                    // If deletion failed, then some other thread must have added a new record to the entry.
                    std::map<K, Address> map;
                    vector<Address> vec;
                    Address address = expected_entry.address();
                    bool flag = TraceBackForGC(address, vec);
                    for (int i = vec.size() - 1; i > -1; i--) {
                        address = vec[i];
                        k = address.h();
                        expected_entry = atomic_entry.load();
                        const record_t *record1 = reinterpret_cast<const record_t *>(thlog[k]->Get(address));
                        //const record_t* record1 = reinterpret_cast<record_t*>(thlog[k]->Get(address));
                        uint32_t record_size = record1->size();
                        key_t a = record1->key();
                        Address new_address = BlockAllocateT(record_size, k);
                        record_t *record = reinterpret_cast<record_t *>(thlog[k]->Get(new_address));
                        new(record) record_t{
                                RecordInfo{
                                        static_cast<uint16_t>(thread_ctx().version), true, false, false,
                                        expected_entry.address()},
                                record1->key()};
                        //a = record->key();
                        //pending_context.Put(record);   //put ?????????
                        HashBucketEntry updated_entry{new_address, expected_entry.tag(), false};

                        //atomic_entry.compare_exchange_strong(expected_entry, updated_entry);
                        // Installed the new record in the hash table
                    }
                }
            }
            // Go to next bucket in the chain.
            HashBucketOverflowEntry overflow_entry = bucket->overflow_entry.load();
            if (overflow_entry.unused()) {
                // No more buckets in the chain.
                break;
            }
            bucket = &overflow_buckets_allocator_[version].Get(overflow_entry.address());
        }
    }
    // Done with this chunk--did some work.
    return true;
}

template<class K, class V, class D>
void FasterKv<K, V, D>::AddHashEntry(HashBucket *&bucket, uint32_t &next_idx, uint8_t version,
                                     HashBucketEntry entry) {
    if (next_idx == HashBucket::kNumEntries) {
        // Need to allocate a new bucket, first.
        FixedPageAddress new_bucket_addr = overflow_buckets_allocator_[version].Allocate();
        HashBucketOverflowEntry new_bucket_entry{new_bucket_addr};
        bucket->overflow_entry.store(new_bucket_entry);
        bucket = &overflow_buckets_allocator_[version].Get(new_bucket_addr);
        next_idx = 0;
    }
    bucket->entries[next_idx].store(entry);
    ++next_idx;
}

template<class K, class V, class D>
Address FasterKv<K, V, D>::TraceBackForOtherChainStart(uint64_t old_size, uint64_t new_size,
                                                       Address from_address, Address min_address, uint8_t side) {
    assert(side == 0 || side == 1);
    // Search back as far as min_address.
    while (from_address >= min_address) {
        const record_t *record = reinterpret_cast<const record_t *>(hlog.Get(from_address));
        KeyHash hash = record->key().GetHash();
        if ((hash.idx(new_size) < old_size) != (side == 0)) {
            // Record's key hashes to the other side.
            return from_address;
        }
        from_address = record->header.previous_address();
    }
    return from_address;
}

template<class K, class V, class D>
void FasterKv<K, V, D>::SplitHashTableBuckets() {
    // This thread won't exit until all hash table buckets have been split.
    Address head_address = hlog.head_address.load();
    Address begin_address = hlog.begin_address.load();
    for (uint64_t chunk = grow_.next_chunk++; chunk < grow_.num_chunks; chunk = grow_.next_chunk++) {
        uint64_t old_size = state_[grow_.old_version].size();
        uint64_t new_size = state_[grow_.new_version].size();
        assert(new_size == old_size * 2);
        // Split this chunk.
        uint64_t upper_bound;
        if (chunk + 1 < grow_.num_chunks) {
            // All chunks but the last chunk contain kGrowHashTableChunkSize elements.
            upper_bound = kGrowHashTableChunkSize;
        } else {
            // Last chunk might contain more or fewer elements.
            upper_bound = old_size - (chunk * kGrowHashTableChunkSize);
        }
        for (uint64_t idx = 0; idx < upper_bound; ++idx) {

            // Split this (chain of) bucket(s).
            HashBucket *old_bucket = &state_[grow_.old_version].bucket(
                    chunk * kGrowHashTableChunkSize + idx);
            HashBucket *new_bucket0 = &state_[grow_.new_version].bucket(
                    chunk * kGrowHashTableChunkSize + idx);
            HashBucket *new_bucket1 = &state_[grow_.new_version].bucket(
                    old_size + chunk * kGrowHashTableChunkSize + idx);
            uint32_t new_entry_idx0 = 0;
            uint32_t new_entry_idx1 = 0;
            while (true) {
                for (uint32_t old_entry_idx = 0; old_entry_idx < HashBucket::kNumEntries; ++old_entry_idx) {
                    HashBucketEntry old_entry = old_bucket->entries[old_entry_idx].load();
                    if (old_entry.unused()) {
                        // Nothing to do.
                        continue;
                    } else if (old_entry.address() < head_address) {
                        // Can't tell which new bucket the entry should go into; put it in both.
                        AddHashEntry(new_bucket0, new_entry_idx0, grow_.new_version, old_entry);
                        AddHashEntry(new_bucket1, new_entry_idx1, grow_.new_version, old_entry);
                        continue;
                    }

                    const record_t *record = reinterpret_cast<const record_t *>(hlog.Get(
                            old_entry.address()));
                    KeyHash hash = record->key().GetHash();
                    if (hash.idx(new_size) < old_size) {
                        // Record's key hashes to the 0 side of the new hash table.
                        AddHashEntry(new_bucket0, new_entry_idx0, grow_.new_version, old_entry);
                        Address other_address = TraceBackForOtherChainStart(old_size, new_size,
                                                                            record->header.previous_address(),
                                                                            head_address, 0);
                        if (other_address >= begin_address) {
                            // We found a record that either is on disk or has a key that hashes to the 1 side of
                            // the new hash table.
                            AddHashEntry(new_bucket1, new_entry_idx1, grow_.new_version,
                                         HashBucketEntry{other_address, old_entry.tag(), false});
                        }
                    } else {
                        // Record's key hashes to the 1 side of the new hash table.
                        AddHashEntry(new_bucket1, new_entry_idx1, grow_.new_version, old_entry);
                        Address other_address = TraceBackForOtherChainStart(old_size, new_size,
                                                                            record->header.previous_address(),
                                                                            head_address, 1);
                        if (other_address >= begin_address) {
                            // We found a record that either is on disk or has a key that hashes to the 0 side of
                            // the new hash table.
                            AddHashEntry(new_bucket0, new_entry_idx0, grow_.new_version,
                                         HashBucketEntry{other_address, old_entry.tag(), false});
                        }
                    }
                }
                // Go to next bucket in the chain.
                HashBucketOverflowEntry overflow_entry = old_bucket->overflow_entry.load();
                if (overflow_entry.unused()) {
                    // No more buckets in the chain.
                    break;
                }
                old_bucket = &overflow_buckets_allocator_[grow_.old_version].Get(overflow_entry.address());
            }
        }
        // Done with this chunk.
        if (--grow_.num_pending_chunks == 0) {
            // Free the old hash table.
            state_[grow_.old_version].Uninitialize();
            overflow_buckets_allocator_[grow_.old_version].Uninitialize();
            break;
        }
    }
    // Thread has finished growing its part of the hash table.
    thread_ctx().phase = Phase::REST;
    // Thread ack that it has finished growing the hash table.
    if (epoch_.FinishThreadPhase(Phase::GROW_IN_PROGRESS)) {
        // Let other threads know that they can use the new hash table now.
        GlobalMoveToNextState(SystemState{Action::GrowIndex, Phase::GROW_IN_PROGRESS,
                                          thread_ctx().version});
    } else {
        while (system_state_.load().phase == Phase::GROW_IN_PROGRESS) {
            // Spin until all other threads have finished splitting their chunks.
            std::this_thread::yield();
        }
    }
}

template<class K, class V, class D>
bool FasterKv<K, V, D>::GlobalMoveToNextState(SystemState current_state) {
    SystemState next_state = current_state.GetNextState();
    if (!system_state_.compare_exchange_strong(current_state, next_state)) {
        return false;
    }

    switch (next_state.action) {
        case Action::CheckpointFull:
        case Action::CheckpointIndex:
        case Action::CheckpointHybridLog:
            switch (next_state.phase) {
                case Phase::PREP_INDEX_CHKPT:
                    // This case is handled directly inside Checkpoint[Index]().
                    assert(false);
                    break;
                case Phase::INDEX_CHKPT:
                    assert(next_state.action != Action::CheckpointHybridLog);
                    // Issue async request for fuzzy checkpoint
                    assert(!checkpoint_.failed);
                    if (CheckpointFuzzyIndex() != Status::Ok) {
                        checkpoint_.failed = true;
                    }
                    break;
                case Phase::PREPARE:
                    // Index checkpoint will never reach this state; and CheckpointHybridLog() will handle this
                    // case directly.
                    assert(next_state.action == Action::CheckpointFull);
                    // INDEX_CHKPT -> PREPARE
                    // Get an overestimate for the ofb's tail, after we've finished fuzzy-checkpointing the ofb.
                    // (Ensures that recovery won't accidentally reallocate from the ofb.)
                    checkpoint_.index_metadata.ofb_count =
                            overflow_buckets_allocator_[resize_info_.version].count();
                    // Write index meta data on disk
                    if (WriteIndexMetadata() != Status::Ok) {
                        checkpoint_.failed = true;
                    }
                    if (checkpoint_.index_persistence_callback) {
                        // Notify the host that the index checkpoint has completed.
                        checkpoint_.index_persistence_callback(Status::Ok);
                    }
                    break;
                case Phase::IN_PROGRESS: {
                    assert(next_state.action != Action::CheckpointIndex);
                    // PREPARE -> IN_PROGRESS
                    // Do nothing
                    break;
                }
                case Phase::WAIT_PENDING:
                    assert(next_state.action != Action::CheckpointIndex);
                    // IN_PROGRESS -> WAIT_PENDING
                    // Do nothing
                    break;
                case Phase::WAIT_FLUSH:
                    assert(next_state.action != Action::CheckpointIndex);
                    // WAIT_PENDING -> WAIT_FLUSH
                    if (fold_over_snapshot) {
                        Address tail_address;
                        Address read_only_address;
                        //Address tail_address = hlog.ShiftReadOnlyToTail();
                        // Get final address for CPR
                        //tail_address = thlog[0]->GetTailAddress();
                        checkpoint_.log_metadata.final_address = thlog[0]->GetTailAddress();
                        //checkpoint_.log_metadata.final_address = thlog[0]->GetTailAddress();
                        for (int i = 0; i < tlog_number; i++) {
                            if (thlog[i]->GetTailAddress().page() == thlog[i]->read_only_address.page() &&
                                thlog[i]->GetTailAddress().offset() == thlog[i]->read_only_address.offset())
                                continue;
                            else {
                                read_only_address = thlog[i]->read_only_address.load();
                                tail_address = thlog[i]->GetTailAddress();
                                checkpoint_.log_metadata.tfinal_address[i] = tail_address;
                                thlog[i]->ShiftReadOnlyToTail();
                            }
                        }
                    } else {
                        Address tail_address = hlog.GetTailAddress();
                        // Get final address for CPR
                        checkpoint_.log_metadata.final_address = tail_address;
                        checkpoint_.snapshot_file = disk.NewFile(disk.relative_cpr_checkpoint_path(
                                checkpoint_.hybrid_log_token) + "snapshot.dat");
                        if (checkpoint_.snapshot_file.Open(&disk.handler()) != Status::Ok) {
                            checkpoint_.failed = true;
                        }
                        // Flush the log to a snapshot.
                        hlog.AsyncFlushPagesToFile(checkpoint_.log_metadata.flushed_address.page(),
                                                   checkpoint_.log_metadata.final_address, checkpoint_.snapshot_file,
                                                   checkpoint_.flush_pending);
                    }
                    // Write CPR meta data file
                    if (WriteCprMetadata() != Status::Ok) {
                        checkpoint_.failed = true;
                    }
                    break;
                case Phase::PERSISTENCE_CALLBACK:
                    assert(next_state.action != Action::CheckpointIndex);
                    // WAIT_FLUSH -> PERSISTENCE_CALLBACK
                    break;
                case Phase::REST:
                    // PERSISTENCE_CALLBACK -> REST or INDEX_CHKPT -> REST
                    if (next_state.action != Action::CheckpointIndex) {
                        // The checkpoint is done; we can reset the contexts now. (Have to reset contexts before
                        // another checkpoint can be started.)
                        checkpoint_.CheckpointDone();
                        // Free checkpoint locks!
                        checkpoint_locks_.Free();
                        // Checkpoint is done--no more work for threads to do.
                        system_state_.store(SystemState{Action::None, Phase::REST, next_state.version});
                    } else {
                        // Get an overestimate for the ofb's tail, after we've finished fuzzy-checkpointing the
                        // ofb. (Ensures that recovery won't accidentally reallocate from the ofb.)
                        checkpoint_.index_metadata.ofb_count =
                                overflow_buckets_allocator_[resize_info_.version].count();
                        // Write index meta data on disk
                        if (WriteIndexMetadata() != Status::Ok) {
                            checkpoint_.failed = true;
                        }
                        auto index_persistence_callback = checkpoint_.index_persistence_callback;
                        // The checkpoint is done; we can reset the contexts now. (Have to reset contexts before
                        // another checkpoint can be started.)
                        checkpoint_.CheckpointDone();
                        // Checkpoint is done--no more work for threads to do.
                        system_state_.store(SystemState{Action::None, Phase::REST, next_state.version});
                        if (index_persistence_callback) {
                            // Notify the host that the index checkpoint has completed.
                            index_persistence_callback(Status::Ok);
                        }
                    }
                    break;
                default:
                    // not reached
                    assert(false);
                    break;
            }
            break;
        case Action::GC:
            switch (next_state.phase) {
                case Phase::GC_IO_PENDING:
                    // This case is handled directly inside ShiftBeginAddress().
                    assert(false);
                    break;
                case Phase::GC_IN_PROGRESS:
                    // GC_IO_PENDING -> GC_IN_PROGRESS
                    // Tell the disk to truncate the log.
                    //hlog.Truncate(gc_.truncate_callback);
                    break;
                case Phase::REST:
                    // GC_IN_PROGRESS -> REST
                    // GC is done--no more work for threads to do.
                    if (gc_.complete_callback) {
                        gc_.complete_callback();
                    }
                    for (int i = 0; i < 40; i++) {
                        if (thlog[i]->GetTailAddress().page() == thlog[i]->head_address.page() &&
                            thlog[i]->GetTailAddress().offset() == thlog[i]->head_address.offset())
                            continue;
                        else {
                            thlog[i]->ShiftHeadAddress(thlog[i]->gc_address.load());
                        }
                    }
                    system_state_.store(SystemState{Action::None, Phase::REST, next_state.version});
                    Gcflag = true;
                    break;
                default:
                    // not reached
                    assert(false);
                    break;
            }
            break;
        case Action::GrowIndex:
            switch (next_state.phase) {
                case Phase::GROW_PREPARE:
                    // This case is handled directly inside GrowIndex().
                    assert(false);
                    break;
                case Phase::GROW_IN_PROGRESS:
                    // Swap hash table versions so that all threads will use the new version after populating it.
                    resize_info_.version = grow_.new_version;
                    break;
                case Phase::REST:
                    if (grow_.callback) {
                        grow_.callback(state_[grow_.new_version].size());
                    }
                    system_state_.store(SystemState{Action::None, Phase::REST, next_state.version});
                    break;
                default:
                    // not reached
                    assert(false);
                    break;
            }
            break;
        default:
            // not reached
            assert(false);
            break;
    }
    return true;
}

template<class K, class V, class D>
void FasterKv<K, V, D>::MarkAllPendingRequests() {
    uint32_t table_version = resize_info_.version;
    uint64_t table_size = state_[table_version].size();

    for (const IAsyncContext *ctxt : thread_ctx().retry_requests) {
        const pending_context_t *context = static_cast<const pending_context_t *>(ctxt);
        // We will succeed, since no other thread can currently advance the entry's version, since this
        // thread hasn't acked "PENDING" phase completion yet.
        bool result = checkpoint_locks_.get_lock(context->key().GetHash()).try_lock_old();
        assert(result);
    }
    for (const auto &pending_io : thread_ctx().pending_ios) {
        // We will succeed, since no other thread can currently advance the entry's version, since this
        // thread hasn't acked "PENDING" phase completion yet.
        bool result = checkpoint_locks_.get_lock(pending_io.second).try_lock_old();
        assert(result);
    }
}

template<class K, class V, class D>
void FasterKv<K, V, D>::HandleSpecialPhases() {
    SystemState final_state = system_state_.load();
    if (final_state.phase == Phase::REST) {
        // Nothing to do; just reset thread context.
        thread_ctx().phase = Phase::REST;
        thread_ctx().version = final_state.version;
        return;
    }
    SystemState previous_state{final_state.action, thread_ctx().phase, thread_ctx().version};
    do {
        // Identify the transition (currentState -> nextState)
        SystemState current_state = (previous_state == final_state) ? final_state :
                                    previous_state.GetNextState();
        switch (current_state.action) {
            case Action::CheckpointFull:
            case Action::CheckpointIndex:
            case Action::CheckpointHybridLog:
                switch (current_state.phase) {
                    case Phase::PREP_INDEX_CHKPT:
                        assert(current_state.action != Action::CheckpointHybridLog);
                        // Both from REST -> PREP_INDEX_CHKPT and PREP_INDEX_CHKPT -> PREP_INDEX_CHKPT
                        if (previous_state.phase == Phase::REST) {
                            int id = Thread::id() % check_thread_number;
                            int w = id / (check_thread_number / tlog_number);
                            int number = id % (check_thread_number / tlog_number);
                            int excepcted = 0;
                            if (system_state_.load().version == 1) {
                                if (WriteHash[w].compare_exchange_strong(excepcted, 1)) {
                                    file_t ht_file = disk.NewFile(
                                            disk.relative_version_path(system_state_.load().version) +
                                            std::to_string(w) +
                                            "ht.dat");
                                    ht_file.Open(&disk.handler());
                                    /*Status s = state_[w].CheckpointNumber(disk, std::move(ht_file),
                                                                          checkpoint_.index_metadata.num_ht_bytes,
                                                                          number, check_thread_number / tlog_number);
                                    if (s != Status::Ok)
                                        cout << "not ok" << endl;
                                        */
                                    state_[w].Checkpoint(disk, std::move(ht_file),
                                                         checkpoint_.index_metadata.num_ht_bytes);
                                }
                            } else {
                                if (WriteHash[id].compare_exchange_strong(excepcted, 1)) {
                                    std::string filename = disk.version_path(system_state_.load().version) +
                                                           std::to_string(w) + std::to_string(number) +
                                                           "ht.dat";
                                    std::FILE *file = std::fopen(filename.c_str(), "wb");
                                    std::fclose(file);
                                    //state_[w].CheckpointNumber(disk, std::move(ht_file),
                                    //                          checkpoint_.index_metadata.num_ht_bytes,
                                    //                          number, check_thread_number / tlog_number);
                                    state_[w].CheckpointEntry(disk, filename, system_state_.load().version, number,
                                                              check_thread_number / tlog_number,checkpoint_.index_metadata.num_entry[id]);
                                    checkpoint_.index_metadata.num_ht_bytes = min_table_size_/tlog_number * sizeof(HashBucket);
                                    //CheckpointEntry(std::move(ht_file),number,check_thread_number / tlog_number,w);
                                }
                            }
                            // Thread ack that we're performing a checkpoint.
                            if (epoch_.FinishThreadPhase(Phase::PREP_INDEX_CHKPT)) {
                                GlobalMoveToNextState(current_state);
                            }
                        }
                        break;
                    case Phase::INDEX_CHKPT: {
                        assert(current_state.action != Action::CheckpointHybridLog);
                        // Both from PREP_INDEX_CHKPT -> INDEX_CHKPT and INDEX_CHKPT -> INDEX_CHKPT
                        Status result = CheckpointFuzzyIndexComplete();
                        if (result != Status::Pending && result != Status::Ok) {
                            checkpoint_.failed = true;
                        }
                        if (result != Status::Pending) {
                            if (current_state.action == Action::CheckpointIndex) {
                                // This thread is done now.
                                thread_ctx().phase = Phase::REST;
                                // Thread ack that it is done.
                                if (epoch_.FinishThreadPhase(Phase::INDEX_CHKPT)) {
                                    GlobalMoveToNextState(current_state);
                                }
                            } else {
                                // Index checkpoint is done; move on to PREPARE phase.
                                GlobalMoveToNextState(current_state);
                            }
                        }
                        break;
                    }
                    case Phase::PREPARE:
                        assert(current_state.action != Action::CheckpointIndex);
                        // Handle (INDEX_CHKPT -> PREPARE or REST -> PREPARE) and PREPARE -> PREPARE
                        if (previous_state.phase != Phase::PREPARE) {
                            // mark pending requests
                            MarkAllPendingRequests();
                            // keep a count of number of threads
                            ++checkpoint_.log_metadata.num_threads;
                            // set the thread index
                            checkpoint_.log_metadata.guids[Thread::id()] = thread_ctx().guid;
                            // Thread ack that it has finished marking its pending requests.
                            if (epoch_.FinishThreadPhase(Phase::PREPARE)) {
                                GlobalMoveToNextState(current_state);
                            }
                        }
                        break;
                    case Phase::IN_PROGRESS:
                        assert(current_state.action != Action::CheckpointIndex);
                        // Handle PREPARE -> IN_PROGRESS and IN_PROGRESS -> IN_PROGRESS
                        if (previous_state.phase == Phase::PREPARE) {
                            assert(prev_thread_ctx().retry_requests.empty());
                            assert(prev_thread_ctx().pending_ios.empty());
                            assert(prev_thread_ctx().io_responses.empty());

                            // Get a new thread context; keep track of the old one as "previous."
                            thread_contexts_[Thread::id()].swap();
                            // initialize a new local context
                            thread_ctx().Initialize(Phase::IN_PROGRESS, current_state.version,
                                                    prev_thread_ctx().guid, prev_thread_ctx().serial_num);
                            // Thread ack that it has swapped contexts.
                            if (epoch_.FinishThreadPhase(Phase::IN_PROGRESS)) {
                                GlobalMoveToNextState(current_state);
                            }
                        }
                        break;
                    case Phase::WAIT_PENDING:
                        assert(current_state.action != Action::CheckpointIndex);
                        // Handle IN_PROGRESS -> WAIT_PENDING and WAIT_PENDING -> WAIT_PENDING
                        if (!epoch_.HasThreadFinishedPhase(Phase::WAIT_PENDING)) {
                            if (prev_thread_ctx().pending_ios.empty() &&
                                prev_thread_ctx().retry_requests.empty()) {
                                // Thread ack that it has completed its pending I/Os.
                                if (epoch_.FinishThreadPhase(Phase::WAIT_PENDING)) {
                                    GlobalMoveToNextState(current_state);
                                }
                            }
                        }
                        break;
                    case Phase::WAIT_FLUSH:
                        assert(current_state.action != Action::CheckpointIndex);
                        // Handle WAIT_PENDING -> WAIT_FLUSH and WAIT_FLUSH -> WAIT_FLUSH
                        if (!epoch_.HasThreadFinishedPhase(Phase::WAIT_FLUSH)) {
                            bool flushed;
                            if (fold_over_snapshot) {
                                //flushed = hlog.flushed_until_address.load() >= checkpoint_.log_metadata.final_address;
                                flushed = true;
                                for (int i = 0; i < checkpoint_.index_metadata.size; i++)
                                    if (thlog[i]->flushed_until_address.load() <
                                        checkpoint_.log_metadata.tfinal_address[i])
                                        flushed = false;

                            } else {
                                flushed = checkpoint_.flush_pending.load() == 0;
                            }
                            if (flushed) {
                                // write context info
                                WriteCprContext();
                                // Thread ack that it has written its CPU context.
                                if (epoch_.FinishThreadPhase(Phase::WAIT_FLUSH)) {
                                    GlobalMoveToNextState(current_state);
                                }
                            }
                        }
                        break;
                    case Phase::PERSISTENCE_CALLBACK:
                        assert(current_state.action != Action::CheckpointIndex);
                        // Handle WAIT_FLUSH -> PERSISTENCE_CALLBACK and PERSISTENCE_CALLBACK -> PERSISTENCE_CALLBACK
                        if (previous_state.phase == Phase::WAIT_FLUSH) {
                            // Persistence callback
                            if (checkpoint_.hybrid_log_persistence_callback) {
                                checkpoint_.hybrid_log_persistence_callback(Status::Ok, prev_thread_ctx().serial_num);
                            }
                            // Thread has finished checkpointing.
                            thread_ctx().phase = Phase::REST;
                            // Thread ack that it has finished checkpointing.
                            if (epoch_.FinishThreadPhase(Phase::PERSISTENCE_CALLBACK)) {
                                GlobalMoveToNextState(current_state);
                            }
                        }
                        break;
                    default:
                        // nothing to do.
                        break;
                }
                break;
            case Action::GC:
                switch (current_state.phase) {
                    case Phase::GC_IO_PENDING:
                        // Handle REST -> GC_IO_PENDING and GC_IO_PENDING -> GC_IO_PENDING.
                        if (previous_state.phase == Phase::REST) {
                            assert(prev_thread_ctx().retry_requests.empty());
                            assert(prev_thread_ctx().pending_ios.empty());
                            assert(prev_thread_ctx().io_responses.empty());
                            // Get a new thread context; keep track of the old one as "previous."
                            thread_contexts_[Thread::id()].swap();
                            // initialize a new local context
                            thread_ctx().Initialize(Phase::GC_IO_PENDING, current_state.version,
                                                    prev_thread_ctx().guid, prev_thread_ctx().serial_num);
                        }

                        // See if the old thread context has completed its pending I/Os.
                        if (!epoch_.HasThreadFinishedPhase(Phase::GC_IO_PENDING)) {
                            if (prev_thread_ctx().pending_ios.empty() &&
                                prev_thread_ctx().retry_requests.empty()) {
                                // Thread ack that it has completed its pending I/Os.
                                if (epoch_.FinishThreadPhase(Phase::GC_IO_PENDING)) {
                                    GlobalMoveToNextState(current_state);
                                }
                            }
                        }
                        break;
                    case Phase::GC_IN_PROGRESS:
                        // Handle GC_IO_PENDING -> GC_IN_PROGRESS and GC_IN_PROGRESS -> GC_IN_PROGRESS.
                        if (!epoch_.HasThreadFinishedPhase(Phase::GC_IN_PROGRESS)) {
                            if (!CleanHashTableBuckets()) {
                                // No more buckets for this thread to clean; thread has finished GC.
                                thread_ctx().phase = Phase::REST;
                                // Thread ack that it has finished GC.
                                if (epoch_.FinishThreadPhase(Phase::GC_IN_PROGRESS)) {
                                    GlobalMoveToNextState(current_state);
                                }
                            }
                        }
                        break;
                    default:
                        assert(false); // not reached
                        break;
                }
                break;
            case Action::GrowIndex:
                switch (current_state.phase) {
                    case Phase::GROW_PREPARE:
                        if (previous_state.phase == Phase::REST) {
                            // Thread ack that we're going to grow the hash table.
                            if (epoch_.FinishThreadPhase(Phase::GROW_PREPARE)) {
                                GlobalMoveToNextState(current_state);
                            }
                        } else {
                            // Wait for all other threads to finish their outstanding (synchronous) hash table
                            // operations.
                            std::this_thread::yield();
                        }
                        break;
                    case Phase::GROW_IN_PROGRESS:
                        SplitHashTableBuckets();
                        break;
                }
                break;
        }
        thread_ctx().phase = current_state.phase;
        thread_ctx().version = current_state.version;
        previous_state = current_state;
    } while (previous_state != final_state);
}

template<class K, class V, class D>
bool FasterKv<K, V, D>::Checkpoint(void(*index_persistence_callback)(Status result),
                                   void(*hybrid_log_persistence_callback)(Status result,
                                                                          uint64_t persistent_serial_num),
                                   Guid &token, int number) {
    // Only one thread can initiate a checkpoint at a time.
    SystemState expected{Action::None, Phase::REST, system_state_.load().version};
    SystemState desired{Action::CheckpointFull, Phase::REST, expected.version};
    if (!system_state_.compare_exchange_strong(expected, desired)) {
        // Can't start a new checkpoint while a checkpoint or recovery is already in progress.
        return false;
    }
    // We are going to start a checkpoint.
    epoch_.ResetPhaseFinished();
    // Initialize all contexts
    token = Guid::Create();
    //Guid index_token = Guid::Parse(std::to_string(expected.version));
    //disk.CreateIndexCheckpointDirectory(token);
    disk.CreateVersionCheckpointDirectory(system_state_.load().version);
    disk.CreateCprCheckpointDirectory(token);
    Address a[40];
    Address b[40];
    int h_size = 0;
    for (int i = 0; i < tlog_number; i++) {
        if (thlog[i]->GetTailAddress().page() == thlog[i]->read_only_address.page() &&
            thlog[i]->GetTailAddress().offset() == thlog[i]->read_only_address.offset())
            continue;
        else {
            a[i] = thlog[i]->begin_address.load();
            b[i] = thlog[i]->GetTailAddress();
            h_size++;
        }
    }
    for (int i = 0; i < 128; i++)
        WriteHash[i].store(0);
    check_thread_number = number;
    // Obtain tail address for fuzzy index checkpoint
    if (!fold_over_snapshot) {

        checkpoint_.InitializeCheckpoint(token, desired.version, state_[resize_info_.version].size(),
                                         hlog.begin_address.load(), hlog.GetTailAddress(), true,
                                         hlog.flushed_until_address.load(),
                                         index_persistence_callback,
                                         hybrid_log_persistence_callback);


    } else {
        /*
        checkpoint_.InitializeCheckpoint(token, desired.version, state_[resize_info_.version].size(),
                                         hlog.begin_address.load(), hlog.GetTailAddress(), false,
                                         Address::kInvalidAddress, index_persistence_callback,
                                         hybrid_log_persistence_callback);
        */
        checkpoint_.InitializeCheckpoint1(token, desired.version, state_[resize_info_.version].size(),
                                          hlog.begin_address.load(), hlog.GetTailAddress(),
                                          a, b, h_size, false,
                                          Address::kInvalidAddress, index_persistence_callback,
                                          hybrid_log_persistence_callback);
    }
    checkpoint_.index_metadata.num_table_chunk = number / tlog_number;
    InitializeCheckpointLocks();
    // Let other threads know that the checkpoint has started.
    system_state_.store(desired.GetNextState());
    return true;
}

template<class K, class V, class D>
bool FasterKv<K, V, D>::CheckpointCheck() {
    SystemState expected{Action::None, Phase::REST, system_state_.load().version};
    if (system_state_.load() == expected) {
        return true;  // check point done
    } else {
        return false;  //check point still run
    }
}

template<class K, class V, class D>
bool FasterKv<K, V, D>::CheckpointIndex(void(*index_persistence_callback)(Status result),
                                        Guid &token) {
    // Only one thread can initiate a checkpoint at a time.
    SystemState expected{Action::None, Phase::REST, system_state_.load().version};
    SystemState desired{Action::CheckpointIndex, Phase::REST, expected.version};
    if (!system_state_.compare_exchange_strong(expected, desired)) {
        // Can't start a new checkpoint while a checkpoint or recovery is already in progress.
        return false;
    }
    // We are going to start a checkpoint.
    epoch_.ResetPhaseFinished();
    // Initialize all contexts
    token = Guid::Create();
    disk.CreateIndexCheckpointDirectory(token);
    checkpoint_.InitializeIndexCheckpoint(token, desired.version,
                                          state_[resize_info_.version].size(),
                                          hlog.begin_address.load(), hlog.GetTailAddress(),
                                          index_persistence_callback);
    // Let other threads know that the checkpoint has started.
    system_state_.store(desired.GetNextState());
    return true;
}

template<class K, class V, class D>
bool FasterKv<K, V, D>::CheckpointHybridLog(void(*hybrid_log_persistence_callback)(Status result,
                                                                                   uint64_t persistent_serial_num),
                                            Guid &token) {
    // Only one thread can initiate a checkpoint at a time.
    SystemState expected{Action::None, Phase::REST, system_state_.load().version};
    SystemState desired{Action::CheckpointHybridLog, Phase::REST, expected.version};
    if (!system_state_.compare_exchange_strong(expected, desired)) {
        // Can't start a new checkpoint while a checkpoint or recovery is already in progress.
        return false;
    }
    // We are going to start a checkpoint.
    epoch_.ResetPhaseFinished();
    // Initialize all contexts
    token = Guid::Create();
    disk.CreateCprCheckpointDirectory(token);
    // Obtain tail address for fuzzy index checkpoint
    if (!fold_over_snapshot) {
        checkpoint_.InitializeHybridLogCheckpoint(token, desired.version, true,
                                                  hlog.flushed_until_address.load(), hybrid_log_persistence_callback);
    } else {
        checkpoint_.InitializeHybridLogCheckpoint(token, desired.version, false,
                                                  Address::kInvalidAddress, hybrid_log_persistence_callback);
    }
    InitializeCheckpointLocks();
    // Let other threads know that the checkpoint has started.
    system_state_.store(desired.GetNextState());
    return true;
}

template<class K, class V, class D>
Status FasterKv<K, V, D>::Recover(const Guid &index_token, const Guid &hybrid_log_token,
                                  uint32_t &version,
                                  std::vector<Guid> &session_ids) {
    version = 0;
    session_ids.clear();
    SystemState expected = SystemState{Action::None, Phase::REST, system_state_.load().version};
    if (!system_state_.compare_exchange_strong(expected,
                                               SystemState{Action::Recover, Phase::REST, expected.version})) {
        return Status::Aborted;
    }
    checkpoint_.InitializeRecover(index_token, hybrid_log_token);
    Status status;
#define BREAK_NOT_OK(s) \
    status = (s); \
    if (status != Status::Ok) break

    do {
        // Index and log metadata.
        //BREAK_NOT_OK(ReadIndexMetadata(index_token));
        BREAK_NOT_OK(ReadCprMetadata(hybrid_log_token));
        BREAK_NOT_OK(ReadIndexVersionMetadata(checkpoint_.log_metadata.version));
        if (checkpoint_.index_metadata.version != checkpoint_.log_metadata.version) {
            // Index and hybrid-log checkpoints should have the same version.
            status = Status::Corruption;
            break;
        }

        system_state_.store(SystemState{Action::Recover, Phase::REST,
                                        checkpoint_.log_metadata.version + 1});

        BREAK_NOT_OK(ReadCprContexts(hybrid_log_token, checkpoint_.log_metadata.guids));
        // The index itself (including overflow buckets).
        for(int i=0;i<checkpoint_.index_metadata.num_table;i++){
        BREAK_NOT_OK(RecoverFuzzyIndex(i));
        BREAK_NOT_OK(RecoverFuzzyIndexComplete(true,i));
        cout<<"hash table "<<i<<" recover successful"<<endl;
        }
        /*for(int j = system_state_.load().version-1;j>1;j--) {
            ReadIndexVersionMetadata(j);
            for (int w = 0; w < checkpoint_.index_metadata.num_table; w++) {
                for (int i = 0; i < checkpoint_.index_metadata.num_table_chunk; i++)
                    state_[w].RecoverEntry(j, i,
                                           checkpoint_.index_metadata.num_entry[
                                                   w * checkpoint_.index_metadata.num_table_chunk + i], w);
            }
        }*/
        uint16_t r = 0;
        // Any changes made to the log while the index was being fuzzy-checkpointed.
        if (fold_over_snapshot) {
            //BREAK_NOT_OK(RecoverHybridLog());
            for (int i = 0; i < checkpoint_.index_metadata.size; i++) {
                BREAK_NOT_OK(RecoverHybridLog1(r));
                r++;
            }
            if (status != Status::Ok)
                break;
        } else {
            BREAK_NOT_OK(RecoverHybridLogFromSnapshotFile());
        }
        r = 0;
        //BREAK_NOT_OK(RestoreHybridLog());
        for (int i = 0; i < checkpoint_.index_metadata.size; i++) {
            BREAK_NOT_OK(RestoreHybridLog1(r));
            r++;
        }
        if (status != Status::Ok)
            break;
    } while (false);
    if (status == Status::Ok) {
        for (const auto &token : checkpoint_.continue_tokens) {
            session_ids.push_back(token.first);
        }
        version = checkpoint_.log_metadata.version;
    }
    checkpoint_.RecoverDone();
    system_state_.store(SystemState{Action::None, Phase::REST,
                                    checkpoint_.log_metadata.version + 1});
    return status;
#undef BREAK_NOT_OK
}

template<class K, class V, class D>
bool FasterKv<K, V, D>::ShiftBeginAddress(Address address,
                                          GcState::truncate_callback_t truncate_callback,
                                          GcState::complete_callback_t complete_callback) {
    SystemState expected = SystemState{Action::None, Phase::REST, system_state_.load().version};
    if (!system_state_.compare_exchange_strong(expected,
                                               SystemState{Action::GC, Phase::REST, expected.version})) {
        // Can't start a GC while an action is already in progress.
        return false;
    }
    hlog.begin_address.store(address);
    // Each active thread will notify the epoch when all pending I/Os have completed.
    epoch_.ResetPhaseFinished();
    uint64_t num_chunks = std::max(state_[resize_info_.version].size() / kGcHashTableChunkSize,
                                   (uint64_t) 1);
    gc_.Initialize(truncate_callback, complete_callback, num_chunks);
    // Let other threads know to complete their pending I/Os, so that the log can be truncated.
    system_state_.store(SystemState{Action::GC, Phase::GC_IO_PENDING, expected.version});
    return true;
}

template<class K, class V, class D>
bool FasterKv<K, V, D>::GrabageCollecton(Address address,
                                         GcState::truncate_callback_t truncate_callback,
                                         GcState::complete_callback_t complete_callback) {
    SystemState expected = SystemState{Action::None, Phase::REST, system_state_.load().version};
    if (!system_state_.compare_exchange_strong(expected,
                                               SystemState{Action::GC, Phase::REST, expected.version})) {
        // Can't start a GC while an action is already in progress.
        return false;
    }
    uint16_t k = 0;
    Address a(0, 112, 0);
    const record_t *record = reinterpret_cast<const record_t *>(thlog[0]->Get(a));
    //hlog.begin_address.store(address);
    for (int i = 0; i < 40; i++) {
        //    a=address;
        //  a += Address{0, 0, k}.control();
        thlog[i]->gc_address.store(thlog[i]->flushed_until_address.load());
        k++;
    }
    // Each active thread will notify the epoch when all pending I/Os have completed.
    epoch_.ResetPhaseFinished();
    Gcflag = false;
    uint64_t num_chunks = std::max(state_[resize_info_.version].size() / kGcHashTableChunkSize,
                                   (uint64_t) 1);
    gc_.Initialize(truncate_callback, complete_callback, num_chunks);
    // Let other threads know to complete their pending I/Os, so that the log can be truncated.
    system_state_.store(SystemState{Action::GC, Phase::GC_IO_PENDING, expected.version});
    return true;
}

template<class K, class V, class D>
bool FasterKv<K, V, D>::GrowIndex(GrowState::callback_t caller_callback) {
    SystemState expected = SystemState{Action::None, Phase::REST, system_state_.load().version};
    if (!system_state_.compare_exchange_strong(expected,
                                               SystemState{Action::GrowIndex, Phase::REST, expected.version})) {
        // An action is already in progress.
        return false;
    }
    epoch_.ResetPhaseFinished();
    uint8_t current_version = resize_info_.version;
    assert(current_version == 0 || current_version == 1);
    uint8_t next_version = 1 - current_version;
    uint64_t num_chunks = std::max(state_[current_version].size() / kGrowHashTableChunkSize,
                                   (uint64_t) 1);
    grow_.Initialize(caller_callback, current_version, num_chunks);
    // Initialize the next version of our hash table to be twice the size of the current version.
    state_[next_version].Initialize(state_[current_version].size() * 2, disk.log().alignment());
    overflow_buckets_allocator_[next_version].Initialize(disk.log().alignment(), epoch_);

    SystemState next = SystemState{Action::GrowIndex, Phase::GROW_PREPARE, expected.version};
    system_state_.store(next);

    // Let this thread know it should be growing the index.
    Refresh();
    return true;
}

// Some printing support for gtest
inline std::ostream &operator<<(std::ostream &out, const Status s) {
    return out << (uint8_t) s;
}

inline std::ostream &operator<<(std::ostream &out, const Guid guid) {
    return out << guid.ToString();
}

inline std::ostream &operator<<(std::ostream &out, const FixedPageAddress address) {
    return out << address.control();
}

}
} // namespace FASTER::core
