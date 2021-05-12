//
// Created by yangzheng on 2020/6/22.
//
#include <atomic>
#include <bitset>
#include <cassert>
#include <cstring>
#include <iostream>

#include <thread>
#include <vector>
#include "tracer.h"
#include "../../src/core/address.h"
#include "../../src/core/alloc.h"
#include "../../src/core/key_hash.h"
#include "../../src/core/utility.h"
#include "lf_basic.c"
#include "lf_basic.h"
#include "OneFileLF.h"

//#include "../../src/device/serializablecontext.h"
typedef unsigned long long int atom_t;

uint64_t total_count = 1000000;

uint64_t timer_range = 1;

atomic<int> stopMeasure(0);

std::vector<ycsb::YCSB_request *> loads;

int root_capacity = 1024 * 1024;//67108864

constexpr uint64_t page_size = (1llu << 25);

uint64_t page_remaining = 0;

std::vector<uint64_t> pages;

uint64_t thread_number = 1;

std::atomic<uint64_t> total_time{0};

std::atomic<uint64_t> total_tick{0};

class Address {
public:
    friend class PageOffset;

    /// An invalid address, used when you need to initialize an address but you don't have a valid
    /// value for it yet. NOTE: set to 1, not 0, to distinguish an invalid hash bucket entry
    /// (initialized to all zeros) from a valid hash bucket entry that points to an invalid address.
    static constexpr uint64_t kInvalidAddress = 1;

    /// A logical address is 8 bytes.
    /// --of which 48 bits are used for the address. (The remaining 16 bits are used by the hash
    /// table, for control bits and the tag.)
    static constexpr uint64_t kAddressBits = 48;
    static constexpr uint64_t kMaxAddress = ((uint64_t) 1 << kAddressBits) - 1;
    /// --of which 25 bits are used for offsets into a page, of size 2^25 = 32 MB.
    static constexpr uint64_t kOffsetBits = 25;
    static constexpr uint32_t kMaxOffset = ((uint32_t) 1 << kOffsetBits) - 1;
    /// --and the remaining 23 bits are used for the page index, allowing for approximately 8 million
    /// pages.
    static uint64_t x;
    static constexpr uint64_t kHBits = 8;
    static constexpr uint64_t kPageBits = kAddressBits - kOffsetBits - kHBits;
    static constexpr uint32_t kMaxPage = ((uint32_t) 1 << kPageBits) - 1;

    /// Default constructor.
    Address() : control_{0} {
    }

    Address(uint32_t page, uint32_t offset) : reserved_{0}, h_{0}, page_{page}, offset_{offset} {
    }

    Address(uint32_t page, uint32_t offset, uint32_t h) : reserved_{0}, h_{h}, page_{page}, offset_{offset} {
    }

    /// Copy constructor.
    Address(const Address &other) : control_{other.control_} {
    }

    Address(uint64_t control) : control_{control} {
        assert(reserved_ == 0);
    }

    inline Address &operator=(const Address &other) {
        control_ = other.control_;
        return *this;
    }

    inline Address &operator+=(uint64_t delta) {
        //assert(delta < UINT32_MAX);
        control_ += delta;
        return *this;
    }

    inline Address operator-(const Address &other) {
        return control_ - other.control_;
    }

    /// Comparison operators.
    inline bool operator<(const Address &other) const {
        assert(reserved_ == 0);
        assert(other.reserved_ == 0);
        return control_ < other.control_;
    }

    inline bool operator<=(const Address &other) const {
        assert(reserved_ == 0);
        assert(other.reserved_ == 0);
        return control_ <= other.control_;
    }

    inline bool operator>(const Address &other) const {
        assert(reserved_ == 0);
        assert(other.reserved_ == 0);
        return control_ > other.control_;
    }

    inline bool operator>=(const Address &other) const {
        assert(reserved_ == 0);
        assert(other.reserved_ == 0);
        return control_ >= other.control_;
    }

    inline bool operator==(const Address &other) const {
        return control_ == other.control_;
    }

    inline bool operator!=(const Address &other) const {
        return control_ != other.control_;
    }

    /// Accessors.
    inline uint32_t page() const {
        return static_cast<uint32_t>(page_);
    }

    inline uint32_t offset() const {
        return static_cast<uint32_t>(offset_);
    }

    inline uint32_t h() const {
        return static_cast<uint32_t>(h_);
    }

    inline uint64_t control() const {
        return control_;
    }

private:
    union {
        struct {
            uint64_t offset_ : kOffsetBits;         // 25 bits
            uint64_t page_ : kPageBits;  // 15 bits
            uint64_t h_:kHBits; //8 bit
            uint64_t reserved_ : 64 - kAddressBits; // 16 bits
        };
        uint64_t control_;
    };
};

#define BIG_CONSTANT(x) (x##LLU)
constexpr uint64_t hashseedA = 151261303;

uint64_t MurmurHash64A(const void *key, int len, uint64_t seed) {
    const uint64_t m = BIG_CONSTANT(0xc6a4a7935bd1e995);
    const int r = 47;

    uint64_t h = seed ^(len * m);

    const uint64_t *data = (const uint64_t *) key;
    const uint64_t *end = data + (len / 8);

    while (data != end) {
        uint64_t k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const unsigned char *data2 = (const unsigned char *) data;

    switch (len & 7) {
        case 7:
            h ^= uint64_t(data2[6]) << 48;
        case 6:
            h ^= uint64_t(data2[5]) << 40;
        case 5:
            h ^= uint64_t(data2[4]) << 32;
        case 4:
            h ^= uint64_t(data2[3]) << 24;
        case 3:
            h ^= uint64_t(data2[2]) << 16;
        case 2:
            h ^= uint64_t(data2[1]) << 8;
        case 1:
            h ^= uint64_t(data2[0]);
            h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

struct HashBucketEntry {
    /// Invalid value in the hash table
    static constexpr uint64_t kInvalidEntry = 0;

    HashBucketEntry()
            : control_{0} {
    }

    HashBucketEntry(uint64_t code)
            : control_{code} {
    }

    HashBucketEntry(const HashBucketEntry &other)
            : control_{other.control_} {
    }

    HashBucketEntry(Address address, uint16_t tag, bool tentative)
            : address_{address.control()}, tag_{tag}, reserved_{0}, tentative_{tentative} {
    }

    inline HashBucketEntry &operator=(const HashBucketEntry &other) {
        control_ = other.control_;
        return *this;
    }

    inline bool operator==(const HashBucketEntry &other) const {
        return control_ == other.control_;
    }

    inline bool operator!=(const HashBucketEntry &other) const {
        return control_ != other.control_;
    }

    inline bool unused() const {
        return control_ == 0;
    }

    inline Address address() const {
        return Address{address_};
    }

    inline uint16_t tag() const {
        return static_cast<uint16_t>(tag_);
    }

    inline uint64_t code() const {
        return static_cast<uint64_t >(control_);
    }

    inline bool tentative() const {
        return static_cast<bool>(tentative_);
    }

    inline void set_tentative(bool desired) {
        tentative_ = desired;
    }

    union {
        struct {
            uint64_t address_ : 48; // corresponds to logical address
            uint64_t tag_ : 14;
            uint64_t reserved_ : 1;
            uint64_t tentative_ : 1;
        };
        uint64_t control_;
    };
};

struct KeyHash {
    KeyHash()
            : control_{0} {
    }

    explicit KeyHash(uint64_t code)
            : control_{code} {
    }

    KeyHash(const KeyHash &other)
            : control_{other.control_} {
    }

    KeyHash &operator=(const KeyHash &other) {
        control_ = other.control_;
        return *this;
    }

    /// Truncate the key hash's address to get the page_index into a hash table of specified size.
    inline uint64_t idx(uint64_t size) const {
        return address_ & (size - 1);
    }

    /// The tag (14 bits) serves as a discriminator inside a hash bucket. (Hash buckets use 2 bits
    /// for control and 48 bits for log-structured store offset; the remaining 14 bits discriminate
    /// between different key hashes stored in the same bucket.)
    inline uint16_t tag() const {
        return static_cast<uint16_t>(tag_);
    }

private:
    union {
        struct {
            uint64_t address_ : 48;
            uint64_t tag_ : 14;
            uint64_t not_used_ : 2;
        };
        uint64_t control_;
    };
};

struct Record {
    std::atomic<uint64_t> header1;
    uint8_t key[20];
    uint8_t value[20];
};

struct Entry {
    bool unused() {
        if (control[0] == 0)
            return true;
        return false;
    }

    Entry() {
        control[0] = 0;
        control[1] = 0;
    }

    void Copy(uint8_t *buffer, size_t len) {
        std::memcpy(key, buffer, len);
    }

    atom_t *load() {
        return control;
    }

    inline HashBucketEntry GetEntry() const {
        return HashBucketEntry{control[0]};
    }

    void store(const HashBucketEntry &desired) {
        control[0] = desired.control_;
    }

    inline void compare_exchange_strong(atom_t *exchanged, atom_t *compared) {
        abstraction_dcas(control, exchanged, compared);
    }

    atom_t control[2];
    uint8_t key[16];
};

struct AtomicEntry {
    bool unused() {
        if (control.load() == 0)
            return true;
        return false;
    }

    inline bool compare_exchange_strong(HashBucketEntry &expected, HashBucketEntry desired) {
        uint64_t expected_control = expected.control_;
        bool result = control.compare_exchange_strong(expected_control, desired.control_);
        expected = HashBucketEntry{expected_control};
        return result;
    }

    inline HashBucketEntry load() const {
        return HashBucketEntry{control.load()};
    }

    inline void store(const HashBucketEntry &desired) {
        control.store(desired.control_);
    }

    AtomicEntry() {
        control.store(0);
    }

    atomic<uint64_t> control;
};

struct Bucket {
    Entry entry[8];
};

struct AtomicBucket {
    AtomicEntry entry[8];
};

void EntryTest() {
    std::vector<std::thread> workers;
    AtomicBucket bucket1;
    cout << bucket1.entry[1].load().control_ << endl;
    cout << loads[0]->getKey() << endl;
    cout << loads[1]->getKey() << endl;
    AtomicBucket *bucket;
    bucket = reinterpret_cast<AtomicBucket *>(aligned_alloc(512,
                                                            root_capacity * sizeof(AtomicBucket)));
    std::memset(bucket, 0, root_capacity * sizeof(AtomicBucket));
    for (uint64_t i = 0; i < total_count; i++) {
        std::hash<uint8_t *> hash_fn;
        //cout << loads[i]->getKey() << endl;
        uint64_t mhash = MurmurHash64A((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey()), hashseedA);
        KeyHash keyHash(mhash);
        uint32_t hash = keyHash.idx(root_capacity);
        AtomicBucket *b = &bucket[hash];
        AtomicEntry *e;
        int flag = 0;
        for (int i = 0; i < 7; i++) {
            if (b->entry[i].unused()) {
                e = &b->entry[i];
                break;
            }
            if (i == 7)
                flag = 1;
        }
        if (flag)
            continue;
        if (page_remaining <= sizeof(Record)) {
            pages.push_back((uint64_t) std::malloc(page_size));
            page_remaining = page_size;
        }
        Address addresses = Address(pages.size() - 1, page_size - page_remaining);
        Record *ptr = (Record *) (pages[addresses.page()] + addresses.offset());
        ptr->header1.store(1);
        std::memcpy(ptr->key, (uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey()));
        std::memcpy(ptr->value, (uint8_t *) loads[i]->getVal(), std::strlen(loads[i]->getVal()));
        page_remaining -= sizeof(Record);
        HashBucketEntry entry(addresses, keyHash.tag(), 0);
        e->store(entry);
    }
    cout << "insert done" << endl;
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](AtomicBucket *bucket, uint64_t tid) {
            for (int i = tid * total_count / thread_number;
                 i < (tid + 1) * total_count / thread_number; i++) {
                uint64_t mhash = MurmurHash64A((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey()),
                                               hashseedA);
                KeyHash keyHash(mhash);
                uint32_t hash = keyHash.idx(root_capacity);
                AtomicBucket *b = &bucket[hash];
                AtomicEntry *e;
                int flag = 0;
                for (int i = 0; i < 7; i++) {
                    if (b->entry[i].load().tag() == keyHash.tag()) {
                        e = &b->entry[i];
                        break;
                    }
                    if (i == 7)
                        flag = 1;
                }
                if (flag)
                    continue;
                HashBucketEntry code = e->load();
                Address addresses = e->load().address();
                if (i == 0) {
                    cout << addresses.page() << "  " << addresses.offset() << endl;
                    cout << e->load().tag() << endl;
                    cout << keyHash.tag() << endl;
                }
                Record *ptr = (Record *) (pages[addresses.page()] + addresses.offset());
                if (std::memcmp(ptr->key, (uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey())) == 0) {

                }
                addresses = Address(pages.size() - 1, page_size - page_remaining);
                HashBucketEntry entry(addresses, keyHash.tag(), 0);
                e->compare_exchange_strong(code, entry);
            }
        }, bucket, t));
    }
    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();
}

std::vector<uint64_t> *heap;
uint64_t *heap_remaining;

void LocalAtomicEntryTest() {
    std::vector<std::thread> workers;
    AtomicBucket *bucket;
    bucket = reinterpret_cast<AtomicBucket *>(aligned_alloc(512,
                                                            root_capacity * sizeof(AtomicBucket)));
    std::memset(bucket, 0, root_capacity * sizeof(AtomicBucket));
    heap = new std::vector<uint64_t>[thread_number];
    heap_remaining = new uint64_t[thread_number];
    total_time.store(0);
    total_tick.store(0);
    for (uint64_t t = 0; t < thread_number; t++) {
        heap_remaining[t] = 0;
        workers.push_back(std::thread([](AtomicBucket *bucket, uint64_t tid) {
            Tracer tracer;
            tracer.startTime();
            uint64_t tick = 0;
            for (int i = tid * total_count / thread_number;
                 i < (tid + 1) * total_count / thread_number; i++) {
                std::hash<uint8_t *> hash_fn;
                //cout << loads[i]->getKey() << endl;
                uint64_t mhash = MurmurHash64A((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey()),
                                               hashseedA);
                KeyHash keyHash(mhash);
                uint32_t hash = keyHash.idx(root_capacity);
                AtomicBucket *b = &bucket[hash];
                AtomicEntry *e;
                int flag = 0;
                for (int i = 0; i < 8; i++) {
                    HashBucketEntry t = b->entry[i].load();
                    if (t.unused()) {
                        e = &b->entry[i];
                        break;
                    }
                    if (i == 7)
                        flag = 1;
                }
                if (flag)
                    continue;
                HashBucketEntry code = e->load();
                if (heap_remaining[tid] <= sizeof(Record)) {
                    heap[tid].push_back((uint64_t) std::malloc(page_size));
                    heap_remaining[tid] = page_size;
                }
                Address addresses = Address(heap[tid].size() - 1, page_size - heap_remaining[tid], tid);
                Record *ptr = (Record *) (heap[tid][addresses.page()] + addresses.offset());
                ptr->header1.store(1);
                std::memcpy(ptr->key, (uint8_t *) loads[i]->getKey(), 1);
                std::memcpy(ptr->value, (uint8_t *) loads[i]->getVal(), 8);
                heap_remaining[tid] -= sizeof(Record);
                HashBucketEntry entry(addresses, keyHash.tag(), 0);
                e->compare_exchange_strong(code, entry);
                tick++;
            }
            total_time.fetch_add(tracer.getRunTime());
            total_tick.fetch_add(tick);
        }, bucket, t));
    }
    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();
    workers.clear();
    cout << "insert done" << endl;
    std::random_shuffle(loads.begin(), loads.end());

    total_time.store(0);
    total_tick.store(0);
    Timer timer;
    timer.start();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](AtomicBucket *bucket, uint64_t tid) {
            Tracer tracer;
            tracer.startTime();
            uint64_t tick = 0;
            while (stopMeasure.load() == 0) {
                for (int i = tid * total_count / thread_number;
                     i < (tid + 1) * total_count / thread_number; i++) {
                    uint64_t mhash = MurmurHash64A((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey()),
                                                   hashseedA);
                    KeyHash keyHash(mhash);
                    uint32_t hash = keyHash.idx(root_capacity);
                    AtomicBucket *b = &bucket[hash];
                    AtomicEntry *e;
                    int flag = 0;
                    for (int i = 0; i < 8; i++) {
                        if (b->entry[i].load().tag() == keyHash.tag()) {
                            e = &b->entry[i];
                            break;
                        }
                        if (i == 7)
                            flag = 1;
                    }
                    if (flag)
                        continue;
                    HashBucketEntry code = e->load();
                    Address addresses = e->load().address();
                    uint32_t h = addresses.h();
                    Record *ptr = (Record *) (heap[h][addresses.page()] + addresses.offset());
                    if (std::memcmp(ptr->key, (uint8_t *) loads[i]->getKey(), 1) == 0) {

                    }
                    if (heap_remaining[tid] <= sizeof(Record)) {
                        heap[tid].push_back((uint64_t) std::malloc(page_size));
                        heap_remaining[tid] = page_size;
                    }
                    addresses = Address(heap[tid].size() - 1, page_size - heap_remaining[tid], tid);
                    ptr = (Record *) (heap[tid][addresses.page()] + addresses.offset());
                    ptr->header1.store(1);
                    std::memcpy(ptr->key, (uint8_t *) loads[i]->getKey(), 1);
                    std::memcpy(ptr->value, (uint8_t *) loads[i]->getVal(), 8);
                    heap_remaining[tid] -= sizeof(Record);
                    HashBucketEntry entry(addresses, keyHash.tag(), 0);
                    e->compare_exchange_strong(code, entry);
                    tick++;
                }
            }
            total_time.fetch_add(tracer.getRunTime());
            total_tick.fetch_add(tick);
        }, bucket, t));
    }
    while (timer.elapsedSeconds() < timer_range) {
        sleep(1);
    }
    stopMeasure.store(1, std::memory_order_relaxed);

    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();
    ::free(bucket);
    for (uint64_t t = 0; t < thread_number; t++) {
        for (uint64_t i = 0; i < pages.size(); i++)
            std::free((void *) heap[t][i]);
    }
    delete[] heap;
    delete[] heap_remaining;
    cout << "LocalAtomicEntryTest Tpt: " << (double) total_tick.load() * thread_number / total_time.load() << endl;
    cout << "second insert done" << endl;
}

void LocalEntryTest() {
    std::vector<std::thread> workers;
    Bucket *bucket;
    bucket = reinterpret_cast<Bucket *>(aligned_alloc(512,
                                                      root_capacity * sizeof(Bucket)));
    std::memset(bucket, 0, root_capacity * sizeof(Bucket));
    heap = new std::vector<uint64_t>[thread_number];
    heap_remaining = new uint64_t[thread_number];
    total_time.store(0);
    total_tick.store(0);
    for (uint64_t t = 0; t < thread_number; t++) {
        heap_remaining[t] = 0;
        workers.push_back(std::thread([](Bucket *bucket, uint64_t tid) {
            Tracer tracer;
            tracer.startTime();
            uint64_t tick = 0;
            for (int i = tid * total_count / thread_number;
                 i < (tid + 1) * total_count / thread_number; i++) {
                std::hash<uint8_t *> hash_fn;
                //cout << loads[i]->getKey() << endl;
                uint64_t mhash = MurmurHash64A((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey()),
                                               hashseedA);
                KeyHash keyHash(mhash);
                uint32_t hash = keyHash.idx(root_capacity);
                Bucket *b = &bucket[hash];
                Entry *e;
                int flag = 0;
                for (int i = 0; i < 8; i++) {
                    if (b->entry[i].unused()) {
                        e = &b->entry[i];
                        break;
                    }
                    if (i == 7)
                        flag = 1;
                }
                if (flag)
                    continue;
                atom_t code[2];
                code[0] = e->control[0];
                code[1] = e->control[1];
                if (i == 0)
                    cout << code[0] << endl;
                if (heap_remaining[tid] <= sizeof(Record)) {
                    heap[tid].push_back((uint64_t) std::malloc(page_size));
                    heap_remaining[tid] = page_size;
                }
                Address addresses = Address(heap[tid].size() - 1, page_size - heap_remaining[tid], tid);
                Record *ptr = (Record *) (heap[tid][addresses.page()] + addresses.offset());
                ptr->header1.store(1);
                std::memcpy(e->key, (uint8_t *) loads[i]->getKey(), 1);
                std::memcpy(ptr->value, (uint8_t *) loads[i]->getVal(), 7);
                heap_remaining[tid] -= sizeof(Record);

                HashBucketEntry entry(addresses, keyHash.tag(), 0);
                atom_t exchanged[2];
                exchanged[0] = entry.control_;
                exchanged[1] = 0;
                e->compare_exchange_strong(exchanged, code);
                /*if (i == 10) {
                    cout << e->control[0] << " " << e->control[1] << endl;
                    cout << entry.address().offset() << endl;
                    cout << addresses.page() << " " << addresses.offset() << endl;
                    cout << e->GetEntry().address().offset() << endl;
                }*/
                tick++;
            }
            total_tick.fetch_add(tick);
            total_time.fetch_add(tracer.getRunTime());
        }, bucket, t));
    }
    for (uint64_t i = 0; i < thread_number; i++) {
        workers[i].join();
    }
    workers.clear();
    //std::vector<std::thread> workers;
    cout << "insert done" << endl;
    total_time.store(0);
    total_tick.store(0);
    Timer timer;
    timer.start();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](Bucket *bucket, uint64_t tid) {
            Tracer tracer;
            tracer.startTime();
            uint64_t tick = 0;
            while (stopMeasure.load() == 0) {
                for (int i = tid * total_count / thread_number;
                     i < (tid + 1) * total_count / thread_number; i++) {
                    uint64_t mhash = MurmurHash64A((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey()),
                                                   hashseedA);
                    KeyHash keyHash(mhash);
                    uint32_t hash = keyHash.idx(root_capacity);
                    Bucket *b = &bucket[hash];
                    Entry *e;
                    int flag = 0;
                    for (int i = 0; i < 8; i++) {
                        if (b->entry[i].GetEntry().tag() == keyHash.tag()) {
                            e = &b->entry[i];
                            if (std::memcmp(e->key, (uint8_t *) loads[i]->getKey(), 1) == 0) {

                            }
                            break;
                        }
                        if (i == 7)
                            flag = 1;
                    }
                    if (flag)
                        continue;
                    atom_t code[2];
                    code[0] = e->control[0];
                    code[1] = e->control[1];
                    Address addresses = e->GetEntry().address();
                    if (heap_remaining[tid] <= sizeof(Record)) {
                        heap[tid].push_back((uint64_t) std::malloc(page_size));
                        heap_remaining[tid] = page_size;
                    }
                    addresses = Address(heap[tid].size() - 1, page_size - heap_remaining[tid], tid);
                    Record *ptr = (Record *) (heap[tid][addresses.page()] + addresses.offset());
                    ptr->header1.store(1);
                    std::memcpy(ptr->key, (uint8_t *) loads[i]->getKey(), 1);
                    std::memcpy(ptr->value, (uint8_t *) loads[i]->getVal(), 8);
                    heap_remaining[tid] -= sizeof(Record);
                    HashBucketEntry entry(addresses, keyHash.tag(), 0);
                    atom_t exchanged[2];
                    exchanged[0] = entry.control_;
                    exchanged[1] = 0;
                    e->compare_exchange_strong(exchanged, code);
                    tick++;
                }
            }
            total_tick.fetch_add(tick);
            total_time.fetch_add(tracer.getRunTime());
        }, bucket, t));
    }
    while (timer.elapsedSeconds() < timer_range) {
        sleep(1);
    }
    stopMeasure.store(1, std::memory_order_relaxed);
    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();
    ::free(bucket);
    for (uint64_t t = 0; t < thread_number; t++) {
        for (uint64_t i = 0; i < pages.size(); i++)
            std::free((void *) heap[t][i]);
    }
    delete[] heap;
    delete[] heap_remaining;
    cout << "LocalEntryTest Tpt: " << (double) total_tick.load() * thread_number / total_time.load() << endl;
    cout << "second insert done" << endl;
}
class A{
public:
    A(){
        cout << "A()" <<endl;
    }

    virtual ~A(){
        cout << "~A()" << endl;
    }

    void seta(int n){
        a = n;
    }
private:
    int a;
};

A fun(){
    A a;
    a.seta(5);
    return a;
}
int main(int argc, char **argv) {
    if (argc > 3) {
        thread_number = std::atol(argv[1]);
        total_count = std::atol(argv[2]);
        root_capacity = std::atol(argv[3]);
        timer_range = std::atol(argv[4]);
    }
    cout << sizeof(Record) << endl;
    cout << sizeof(AtomicBucket) << endl;
    cout << sizeof(Bucket) << endl;
    cout << sizeof(Entry) << endl;
    cout << sizeof(AtomicEntry) << endl;
    std::string a;
    a= "sdr";
    uint8_t * w = (uint8_t *)a.c_str();
    cout<<w<<endl;
    //ycsb::YCSBLoader loader(ycsb::loadpath, total_count);
    //loads = loader.load();
    //total_count = loader.size();
    //uint64_t mhash =MurmurHash64A((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey()), hashseedA);
    //cout<<buf[10]<<endl;
    //cout<< sizeof(a)<<endl;
}