//
// Created by iclab on 3/8/20.
//

#include "gtest/gtest.h"

#include <core/faster.h>
#include "device/kvcontext.h"

using namespace FASTER::api;
using namespace FASTER::core;
using namespace FASTER::device;
using namespace FASTER::environment;

#ifdef _WIN32
typedef hreadPoolIoHandler handler_t;
#else
typedef QueueIoHandler handler_t;
#endif
typedef FileSystemDisk<handler_t, 1073741824ull> disk_t;
FasterKv<Key, Value, disk_t> mapstore{128, 1073741824, ""};

class Latch {
private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool triggered_ = false;

public:
    void Wait() {
        std::unique_lock<std::mutex> lock{mutex_};
        while (!triggered_) {
            cv_.wait(lock);
        }
    }

    void Trigger() {
        std::unique_lock<std::mutex> lock{mutex_};
        triggered_ = true;
        cv_.notify_all();
    }

    void Reset() {
        triggered_ = false;
    }
};

template<typename Callable, typename... Args>
void run_threads(size_t num_threads, Callable worker, Args... args) {
    Latch latch;
    auto run_thread = [&latch, &worker, &args...](size_t idx) {
        latch.Wait();
        worker(idx, args...);
    };

    std::deque<std::thread> threads{};
    for (size_t idx = 0; idx < num_threads; ++idx) {
        threads.emplace_back(run_thread, idx);
    }

    latch.Trigger();
    for (auto &thread : threads) {
        thread.join();
    }
}

TEST(IntParallelTest, InMemoryTest) {
    static constexpr size_t kNumOps = 1024;
    static constexpr size_t kNumThreads = 2;

    FasterKv<Key, Value, disk_t> *store = &mapstore;

    auto upsert_worker = [&store](size_t thread_idx) {
        store->StartSession();

        for (size_t idx = 0; idx < kNumOps; ++idx) {
            auto callback = [](IAsyncContext *ctxt, Status result) {
                // In-memory test.
                ASSERT_TRUE(false);
            };
            UpsertContext context{idx, idx};
            Status result = store->Upsert(context, callback, 1);
            ASSERT_EQ(Status::Ok, result);
        }

        store->StopSession();
    };

    auto read_worker = [&store](size_t thread_idx, uint64_t expected_value) {
        store->StartSession();

        for (size_t idx = 0; idx < kNumOps; ++idx) {
            auto callback = [](IAsyncContext *ctxt, Status result) {
                // In-memory test.
                ASSERT_TRUE(false);
            };
            ReadContext context{idx};
            Status result = store->Read(context, callback, 1);
            ASSERT_EQ(Status::Ok, result);
            ASSERT_EQ(idx, context.Ret());
        }

        store->StopSession();
    };

    // Insert.
    run_threads(kNumThreads, upsert_worker);

    // Read.
    run_threads(kNumThreads, read_worker, 0x1717171717);

    // Update.
    run_threads(kNumThreads, upsert_worker);

    // Read again.
    run_threads(kNumThreads, read_worker, 0x2a2a2a2a2a2a2a);
}

TEST(IntParallelTest, CheckpointTest) {

}

TEST(IntParallelTest, RecoveryTest) {

}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}