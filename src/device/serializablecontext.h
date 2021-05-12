//
// Created by iclab on 5/22/20.
//

#ifndef HASHCOMP_SERIALIZABLECONTEXT_H
#define HASHCOMP_SERIALIZABLECONTEXT_H

#include "core/faster.h"
#include "core/key_hash.h"
#include "core/utility.h"
#include "../../playground/microtest/jhash.h"
using namespace FASTER::core;

namespace FASTER {
namespace api {
#define LIGHT_KV_COPY 1
#define LIGHT_CT_COPY 0
#define BIG_CONSTANT(x) (x##LLU)
constexpr uint64_t hashseedA = 151261303;
constexpr uint64_t hashseedB = 6722461;

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

class Key {
public:
    Key(uint8_t *buf, uint32_t len) : len_{len} {
#if LIGHT_KV_COPY == 1
        if (buf_ == nullptr) buf_ = buf;
        else std::memcpy(buf_, buf, len);
#else
        if (buf_ == nullptr) buf_ = new uint8_t[len];
        std::memcpy(buf_, buf, len);
#endif
    }

    Key(Key const &key) : Key(key.get(), key.len_) {}

    ~Key() {
#if LIGHT_KV_COPY == 0
        if (buf_ != (uint8_t *) this + sizeof(Key)) delete[] buf_;
        buf_ = nullptr;
#endif
    }

    uint8_t *get() const {
        return buf_;
    }

    inline uint32_t size() const {
        return static_cast<uint32_t>(sizeof(Key)) + len_;
    }

    inline uint32_t length() const {
        return len_;
    }

    inline KeyHash GetHash() const {
        std::hash<uint8_t *> hash_fn;
        return KeyHash{MurmurHash64A(buf_, len_, hashseedA)};
        //return KeyHash{jenkins_hash(buf_,len_)};
    }

    inline void setLocalBuffer() {
        buf_ = (uint8_t *) this + sizeof(Key);
    }

    inline void Copy(uint8_t *buf) const{
        //std::memcpy(buf, buf_, len_);
        std::memcpy(buf, buf_, 16);
    }

    inline void *operator new(size_t size) {
        return std::malloc(size);
    }

    inline void *operator new(size_t size, void *p) {
        Key *key = (Key *) p;
        key->setLocalBuffer();
        return p;
    }

    inline void *operator new(size_t size, size_t len) {
        return std::malloc(size + len);
    }

    inline void operator delete(void *p) {
        std::free(p);
    }

    /// Comparison operators.
    inline bool operator==(const Key &other) const {
        return len_ == other.len_ && std::memcmp(buf_, other.buf_, len_) == 0;
    }

    inline bool operator==(uint8_t *other) const {
        //return std::memcmp(buf_, other, len_) == 0;
        return std::memcmp(buf_, other, (len_>16)?16:len_) == 0;
    }

    inline bool operator==(const uint8_t *other) const {
        //return std::memcmp(buf_, other, len_) == 0;
        return std::memcmp(buf_, other, (len_>16)?16:len_) == 0;
    }

    inline bool operator!=(const Key &other) const {
        return !(*this == other);
    }

private:
    uint32_t len_ = 0;
    uint8_t *buf_ = nullptr;
};

class UpsertContext;

class DeleteContext;

class ReadContext;

class RmwContext;

class GenLock {
public:
    GenLock() : control_{0} {}

    GenLock(uint64_t control) : control_{control} {}

    inline GenLock &operator=(const GenLock &other) {
        control_ = other.control_;
        return *this;
    }

    union {
        struct {
            uint64_t gen_number : 62;
            uint64_t locked : 1;
            uint64_t replaced : 1;
        };
        uint64_t control_;
    };
};

static_assert(sizeof(GenLock) == 8, "sizeof(GenLock) != 8");

class AtomicGenLock {
public:
    AtomicGenLock() : control_{0} {}

    AtomicGenLock(uint64_t control) : control_{control} {}

    inline GenLock load() const {
        return GenLock{control_.load()};
    }

    inline void store(GenLock desired) {
        control_.store(desired.control_);
    }

    inline bool try_lock(bool &replaced) {
        replaced = false;
        GenLock expected{control_.load()};
        expected.locked = 0;
        expected.replaced = 0;
        GenLock desired{expected.control_};
        desired.locked = 1;

        if (control_.compare_exchange_strong(expected.control_, desired.control_)) {
            return true;
        }
        if (expected.replaced) {
            replaced = true;
        }
        return false;
    }

    inline void unlock(bool replaced) {
        if (!replaced) {
            // Just turn off "locked" bit and increase gen number.
            uint64_t sub_delta = ((uint64_t) 1 << 62) - 1;
            control_.fetch_sub(sub_delta);
        } else {
            // Turn off "locked" bit, turn on "replaced" bit, and increase gen number
            uint64_t add_delta = ((uint64_t) 1 << 63) - ((uint64_t) 1 << 62) + 1;
            control_.fetch_add(add_delta);
        }
    }

private:
    std::atomic<uint64_t> control_;
};

static_assert(sizeof(AtomicGenLock) == 8, "sizeof(AtomicGenLock) != 8");

class Value {
public:
    Value() : gen_lock_{0}, length_{0} {}

    Value(uint8_t *buf, uint32_t length) : gen_lock_{0}, length_(length) {
#if LIGHT_KV_COPY == 1
        if (value_ == nullptr) value_ = buf;
        else std::memcpy(value_, buf, length);
#else
        if (value_ == nullptr) value_ = new uint8_t[length];
        std::memcpy(value_, buf, length);
#endif
    }

    Value(Value const &value) : gen_lock_{0}, length_(value.length_) {
#if LIGHT_KV_COPY == 1
        if (value_ == nullptr) value_ = value.value_;
        else std::memcpy(value_, value.buffer(), value.length_);
#else
        if (value_ == nullptr) value_ = new uint8_t[length_];
        std::memcpy(value_, value.buffer(), value.length_);
#endif
    }

    ~Value() {
#if LIGHT_KV_COPY == 0
        if (value_ != (uint8_t *) this + sizeof(Value)) {
            delete[] value_;
            value_ = nullptr;
        }
#endif
        /*if (allocated) {
            delete[] value_;
            value_ = nullptr;
        }*/
    }

    void reset(uint8_t *value, uint32_t length) {
        gen_lock_.store(0);
        length_ = length;
        assert(value_ == nullptr || value_ == (uint8_t *) this + sizeof(Value));
        setLocalBuffer();
        /*delete[] value_;
        value_ = new uint8_t[length_];*/
        std::memcpy(value_, value, length);
        //allocated = true;
    }

    inline void *operator new(size_t size) {
        return std::malloc(size);
    }

    inline void setLocalBuffer() {
        value_ = (uint8_t *) this + sizeof(Value);
    }

    inline const uint8_t * getLocalBuffer() const {
        return (uint8_t *) this + sizeof(Value);;
    }

    inline void *operator new(size_t size, void *p) {
        Value *value = (Value *) p;
        value->setLocalBuffer();
        return p;
    }

    inline void *operator new(size_t size, size_t len) {
        return std::malloc(size + len);
    }

    inline void operator delete(void *p) {
        std::free(p);
    }

    inline uint32_t size() const {
        return sizeof(Value) + length_;
    }

    uint8_t *get() {
        return buffer();
    }

    uint32_t length() { return length_; }

    friend class UpsertContext;

    friend class DeleteContext;

    friend class ReadContext;

    friend class RmwContext;
private:
    AtomicGenLock gen_lock_;
    uint32_t length_;
    uint8_t *value_ = nullptr;

    inline const uint8_t *buffer() const {
        return value_;
    }

    inline uint8_t *buffer() {
        return value_;
    }
};

class RmwContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    RmwContext(Key key, Value value) : key_{key}, length_{value.length_}, input_buffer(new uint8_t[length_]) {
        std::memcpy(input_buffer, value.value_, length_);
    }


    RmwContext(const RmwContext &other) : key_{other.key_}, length_{other.length_} {
        input_buffer = new uint8_t[length_];
        std::memcpy(input_buffer, other.input_buffer, length_);
    }

    ~RmwContext() { delete[] input_buffer; }


    void reset(uint8_t *buffer) {
        std::memcpy(input_buffer, buffer, length_);
    }

    inline const Key &key() const {
        return key_;
    }

    inline uint32_t value_size() const {
        return sizeof(Value) + length_;
    }

    inline uint32_t value_size(const Value &value) const {
        return sizeof(value) + length_;
    }

    inline uint32_t value_length() const {
        return length_;
    }

    inline uint8_t *get() const {
        return input_buffer;
    }

    inline void RmwInitial(Value &value) {
        assert(input_buffer != value.value_);
        value.reset(input_buffer, length_);
    }

    inline void RmwCopy(const Value &old_value,Value &new_value) {
        assert(input_buffer != new_value.value_);
        uint8_t *w = new_value.value_;
        new_value.reset(input_buffer, length_);
    }

    inline bool RmwAtomic(Value &value) {
        bool replaced;
        while (!value.gen_lock_.try_lock(replaced) && !replaced) {
            std::this_thread::yield();
        }
        if (replaced) {
            return false;
        }
        if (value.length_ < length_) {
            value.gen_lock_.unlock(true);
            return false;
        }

        value.length_ = length_;
        if(value.buffer()[0]!='a'){
            value.buffer()[0]='a';
            value.gen_lock_.unlock(false);
            return true;}
        if(value.buffer()[0]=='a'){
            value.buffer()[0]='b';
            value.gen_lock_.unlock(false);
            return true;}
    }

protected:
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
    uint32_t length_;
    uint8_t *input_buffer;
};

class UpsertContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;
#if LIGHT_CT_COPY == 1
    UpsertContext(Key key, Value value) : key_{key}, length_{value.length_}, input_buffer(value.value_) {}

    /// Copy (and deep-copy) constructor.
    UpsertContext(const UpsertContext &other) : key_{other.key_}, length_{other.length_},
                                                input_buffer(other.input_buffer) {}

    ~UpsertContext() {}

    void init(Key &key, Value &value) {
        key_ = key;
        length_ = value.length_;
        input_buffer = value.value_;
    }
#else

    UpsertContext(Key key, Value value) : key_{key}, length_{value.length_}, input_buffer(new uint8_t[length_]) {
        std::memcpy(input_buffer, value.value_, length_);
    }

    /// Copy (and deep-copy) constructor.
    UpsertContext(const UpsertContext &other) : key_{other.key_}, length_{other.length_} {
        input_buffer = new uint8_t[length_];
        std::memcpy(input_buffer, other.input_buffer, length_);
    }

    ~UpsertContext() { delete[] input_buffer; }

#endif

    void reset(uint8_t *buffer) {
        std::memcpy(input_buffer, buffer, length_);
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key &key() const {
        return key_;
    }

    inline uint32_t value_size() const {
        return sizeof(Value) + length_;
    }

    inline uint32_t value_length() const {
        return length_;
    }

    inline uint8_t *get() const {
        return input_buffer;
    }

    /// Non-atomic and atomic Put() methods.
    inline void Put(Value &value) {
        assert(input_buffer != value.value_);
        value.reset(input_buffer, length_);
    }

    inline bool PutAtomic(Value &value) {
        bool replaced;
        while (!value.gen_lock_.try_lock(replaced) && !replaced) {
            std::this_thread::yield();
        }
        if (replaced) {
            // Some other thread replaced this record.
            return false;
        }
        if (value.length_ < length_) {
            // Current value is too small for in-place update.
            value.gen_lock_.unlock(true);
            /*std::cout << std::thread::id() << " " << (char *) key_.get() << " " << key_.size() << " " << sizeof(Value)
                      << " " << value.value_ << " " << value.size() << " " << value.length() << " "
                      << (char *) input_buffer << " " << sizeof(Value) + length_ << " " << length_ << std::endl;*/
            return false;
        }
        // In-place update overwrites length and buffer, but not size.
        value.length_ = length_;
        std::memcpy(value.buffer(), input_buffer, length_);
        value.gen_lock_.unlock(false);
        return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
    uint32_t length_;
    uint8_t *input_buffer;
};

class DeleteContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    DeleteContext(Key key) : key_{key} {}

    /// Copy (and deep-copy) constructor.
    DeleteContext(const DeleteContext &other) : key_{other.key_}, length_{other.length_} {}

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key &key() const {
        return key_;
    }

    inline uint32_t value_size() const {
        return sizeof(Value) + length_;
    }

    /// Non-atomic and atomic Put() methods.
    inline void Put(Value &value) {
        value.gen_lock_.store(0);
        value.length_ = length_;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
    uint32_t length_;
};

class ReadContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext(Key key) : key_{key}, output_length{0} {}

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext &other) : key_{other.key_}, output_length{0} {}

#if LIGHT_CT_COPY == 1
    void init(Key &key) {
        key_ = key;
        output_length = 0;
    }
#endif

    ~ReadContext() {
        delete[] output_bytes;
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key &key() const {
        return key_;
    }

    inline void set(Value &value) {
        value.length_ = output_length;
        value.value_ = new uint8_t[output_length];
        std::memcpy(value.value_, output_bytes, value.length_);
    }

    inline void Get(const Value &value) {
        // All reads should be atomic (from the mutable tail).
        //ASSERT_TRUE(false);
#if LIGHT_CT_COPY == 1
        if (output_bytes != nullptr && output_length < value.length_) {
            delete[] output_bytes;
            output_bytes = nullptr;
        }
        output_length = value.length_;
        if (output_bytes == nullptr) output_bytes = new uint8_t[output_length];
#else
        output_length = value.length_;
        if (output_bytes != nullptr) delete[] output_bytes;
        output_bytes = new uint8_t[output_length];
#endif
        std::memcpy(output_bytes, value.getLocalBuffer(), output_length);
    }

    inline void GetAtomic(const Value &value) {
        //value.setLocalBuffer();
        GenLock before, after;
        do {
            before = value.gen_lock_.load();
            Get(value);
            do {
                after = value.gen_lock_.load();
            } while (after.locked /*|| after.replaced*/);
        } while (before.gen_number != after.gen_number);
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
public:
    uint32_t output_length = 0;
    // Extract two bytes of output.
    uint8_t *output_bytes = nullptr;
};
}
}
#endif //HASHCOMP_SERIALIZABLECONTEXT_H
