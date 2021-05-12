#include <iostream>
#include "tracer.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <experimental/filesystem>
#include "../../src/core/faster.h"
#include "../../src/core/address.h"
#include "../../src/device/file_system_disk.h"
#include "../../src/device/null_disk.h"

//#include "../../src/device/stringcontext.h"
#include "../../src/device/serializablecontext.h"

#define DEFAULT_THREAD_NUM (4)
#define DEFAULT_KEYS_COUNT (1 << 20)
#define DEFAULT_KEYS_RANGE (1 << 2)

#define DEFAULT_STR_LENGTH 256
#define DEFAULT_KEY_LENGTH 8

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

using store_t = FasterKv<Key, Value, disk_t>;

size_t init_size = next_power_of_two(DEFAULT_STORE_BASE / 2);

store_t *store;

std::vector<ycsb::YCSB_request *> loads;

std::vector<ycsb::YCSB_request *> runs;

long total_time;

uint64_t exists = 0;

uint64_t read_success = 0, modify_success = 0, insert_success = 0;

uint64_t read_failure = 0, modify_failure = 0, insert_failure = 0;

//uint64_t total_count = DEFAULT_KEYS_COUNT;
uint64_t total_count = 10000;

//uint64_t timer_range = default_timer_range;
uint64_t timer_range = 1;

double skew = 0.0;

//int root_capacity = (1 << 16);
int root_capacity = 10000000;


int thread_number = DEFAULT_THREAD_NUM;

int rounds=0;
//int key_range = DEFAULT_KEYS_RANGE;
uint64_t key_range = 100000;

uint64_t kCheckpointInterval = 1 << 20;
uint64_t kRefreshInterval = 1 << 8;
uint64_t kCompletePendingInterval = 1 << 12;

stringstream *output;

atomic<int> stopMeasure(0);

int updatePercentage = 10;

int ereasePercentage = 0;

int totalPercentage = 100;

int readPercentage = (totalPercentage - updatePercentage - ereasePercentage);

int readflag = 0;

bool parallellog = true;

struct target {
    int tid;
    store_t *fmap;
};

pthread_t *workers;

struct target *parms;

using namespace std;

int level_calculate(size_t count) {
    int ret = 0;
    while (count >>= 1) {
        ret++;
    }
    return ret + 1;
}

void prepare() {
    cout << "prepare" << endl;
    workers = new pthread_t[thread_number];
    parms = new struct target[thread_number];
    output = new stringstream[thread_number];
    for (int i = 0; i < thread_number; i++) {
        parms[i].tid = i;
        parms[i].fmap = store;
    }
}

void finish() {
    cout << "finish" << endl;
    delete[] parms;
    delete[] workers;
    delete[] output;
}

void simpleInsert() {
    Tracer tracer;
    tracer.startTime();
    int inserted = 0;
    for (int i = 0; i < key_range; i++) {
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<UpsertContext> context{ctxt};
        };
        UpsertContext context{Key((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey())),
                              Value((uint8_t *) loads[i]->getVal(), std::strlen(loads[i]->getVal()))};
        Status stat = store->UpsertT(context, callback, 1, 1);
        if (stat == Status::Ok) {
            inserted++;
        }
    }
    cout << inserted << " " << tracer.getRunTime() << endl;
}

void simpleRead() {
    uint64_t hit = 0;
    uint64_t fail = 0;
    for (uint64_t i = 0; i < total_count; i++) {
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<ReadContext> context{ctxt};
        };
        ReadContext context{Key((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey()))};
        Status result = store->Read(context, callback, 1);
        if (result == Status::Ok)
            hit++;
        else
            fail++;
    }
    cout << hit << " " << fail << endl;
}

void *insertWorker(void *args) {
    Tracer tracer;
    tracer.startTime();
    store->StartSession();
    struct target *work = (struct target *) args;
    uint64_t hit = 0;
    uint64_t fail = 0;
    uint8_t value[DEFAULT_STR_LENGTH];
    for (int i = work->tid * total_count / thread_number;
         i < (work->tid + 1) * total_count / thread_number; i++) {
        Status stat;
        if (readflag) {
            auto callback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<ReadContext> context{ctxt};
            };
            ReadContext context{Key((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey()))};
            stat = store->Read(context, callback, 1);
        } else {
            auto callback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<UpsertContext> context{ctxt};
            };
            UpsertContext context{Key((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey())),
                                  Value((uint8_t *) loads[i]->getVal(), 1/*std::strlen(loads[i]->getVal())*/)};
            stat = store->UpsertT(context, callback, 1, parallellog ? thread_number : 1);
        }
        if (stat == Status::Ok) hit++;
        else fail++;
    }
    store->StopSession();
    long elipsed = tracer.getRunTime();
    output[work->tid] << work->tid << " " << elipsed << " " << hit << " " << fail << endl;
    __sync_fetch_and_add(&total_time, elipsed);
    __sync_fetch_and_add(&insert_success, hit);
    __sync_fetch_and_add(&insert_failure, fail);
}

void multiInsert() {
    output = new stringstream[thread_number];
    Timer timer;
    timer.start();
    for (int i = 0; i < thread_number; i++) {
        pthread_create(&workers[i], nullptr, insertWorker, &parms[i]);
    }
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], nullptr);
        string outstr = output[i].str();
        cout << outstr;
    }
    cout << "Gathering ..." << endl;
}

void *measureWorker(void *args) {
    Tracer tracer;
    tracer.startTime();
    store->StartSession();
    struct target *work = (struct target *) args;
    uint64_t mhit = 0, rhit = 0;
    uint64_t mfail = 0, rfail = 0;
    uint8_t value[DEFAULT_STR_LENGTH];
    //while (stopMeasure.load(memory_order_relaxed) == 0) {
        for (int i = work->tid * total_count / thread_number;
             i < (work->tid + 1) * total_count / thread_number; i++) {
            switch (static_cast<int>(runs[i]->getOp())) {
                case 0: {break;
                    auto callback = [](IAsyncContext *ctxt, Status result) {
                        CallbackContext<ReadContext> context{ctxt};
                    };
                    ReadContext context{Key((uint8_t *) runs[i]->getKey(), std::strlen(runs[i]->getKey()))};
                    Status stat = store->Read(context, callback, 1);
                    if (stat == Status::Ok) rhit++;
                    else rfail++;
                    break;
                }
                case 1:
                case 3: {
                    if(readflag){
                        auto callback = [](IAsyncContext *ctxt, Status result) {
                            CallbackContext<ReadContext> context{ctxt};
                        };
                        ReadContext context{Key((uint8_t *) runs[i]->getKey(), std::strlen(runs[i]->getKey()))};
                        Status stat = store->Read(context, callback, 1);
                        if (stat == Status::Ok) rhit++;
                        else rfail++;
                        if(context.output_length == std::strlen(runs[i]->getVal()) && (std::memcmp(context.output_bytes,(uint8_t *) runs[i]->getVal(), std::strlen(runs[i]->getVal()))) == 0)
                            rfail++;
                        break;
                    }
                    auto callback = [](IAsyncContext *ctxt, Status result) {
                        CallbackContext<RmwContext> context{ctxt};
                    };
                    RmwContext context{Key((uint8_t *) runs[i]->getKey(), std::strlen(runs[i]->getKey())),
                                          Value((uint8_t *) runs[i]->getVal(), 2/*std::strlen(runs[i]->getVal())*/)};
                    Status stat = store->Rmw(context, callback, 1);
                    if (stat == Status::Ok) mhit++;
                    else mfail++;
                    break;
                }
                case 2: {
                    auto callback = [](IAsyncContext *ctxt, Status result) {
                        CallbackContext<DeleteContext> context{ctxt};
                    };
                    DeleteContext context{Key((uint8_t *) runs[i]->getKey(), std::strlen(runs[i]->getKey()))};
                    Status stat = store->Delete(context, callback, 1);
                    if (stat == Status::Ok) mhit++;
                    else mfail++;
                    break;
                }
                default:
                    break;
            }
        }
    //}
    store->StopSession();
    long elipsed = tracer.getRunTime();
    output[work->tid] << work->tid << " " << elipsed << " " << mhit << " " << rhit << endl;
    __sync_fetch_and_add(&total_time, elipsed);
    __sync_fetch_and_add(&read_success, rhit);
    __sync_fetch_and_add(&read_failure, rfail);
    __sync_fetch_and_add(&modify_success, mhit);
    __sync_fetch_and_add(&modify_failure, mfail);
}

void multiWorkers() {
    output = new stringstream[thread_number];
    Timer timer;
    timer.start();
    for (int i = 0; i < thread_number; i++) {
        pthread_create(&workers[i], nullptr, measureWorker, &parms[i]);
    }
    while (timer.elapsedSeconds() < timer_range) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], nullptr);
        string outstr = output[i].str();
        cout << outstr;
    }
    cout << "Gathering ..." << endl;
    //finish();
}

void *checkWorker(void *args) {
    int inserted = 0;
    int j = 0;
    struct target *work = (struct target *) args;
    int k = work->tid;
    auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
        if (result != Status::Ok) {
            printf("Thread %" PRIu32 " reports checkpoint failed.\n",
                   Thread::id());
        } else {
            printf("Thread %" PRIu32 " reports persistence until %" PRIu64 "\n",
                   Thread::id(), persistent_serial_num);
        }
    };
    store->StartSession();
    s:
    for (int i = 0; i < total_count; i++) {
        store->Refresh1();
        switch (static_cast<int>(runs[i]->getOp())) {
            case 0: {
                auto callback = [](IAsyncContext *ctxt, Status result) {
                    CallbackContext<ReadContext> context{ctxt};
                };
                ReadContext context{Key((uint8_t *) runs[i]->getKey(), std::strlen(runs[i]->getKey()))};
                Status stat = store->Read(context, callback, 1);
                break;
            }
            case 1:
            case 3: {
                auto callback = [](IAsyncContext *ctxt, Status result) {
                    CallbackContext<UpsertContext> context{ctxt};
                };
                UpsertContext context{Key((uint8_t *) runs[i]->getKey(), std::strlen(runs[i]->getKey())),
                                      Value((uint8_t *) runs[i]->getVal(), std::strlen(runs[i]->getVal()))};
                Status stat = store->UpsertT(context, callback, 1, parallellog ? thread_number : 1);
                break;
            }
            case 2: {
                auto callback = [](IAsyncContext *ctxt, Status result) {
                    CallbackContext<DeleteContext> context{ctxt};
                };
                DeleteContext context{Key((uint8_t *) runs[i]->getKey(), std::strlen(runs[i]->getKey()))};
                Status stat = store->Delete(context, callback, 1);
                break;
            }
            default:
                break;
        }
        int excepcted = 0;
        if (i % kCheckpointInterval == 0 && i != 0 && stopMeasure.compare_exchange_strong(excepcted, 1)) {
            Guid token;
            cout << "checkpoint start in" << i << endl;
            if (store->Checkpoint(nullptr, hybrid_log_persistence_callback, token)) {
                printf("Calling Checkpoint(), token = %s\n", token.ToString().c_str());
                cout << "thread id" << k << endl;
            }
        }
        if (i % kCompletePendingInterval == 0) {
            store->CompletePending(false);
        } else if (i % kRefreshInterval == 0) {
            store->Refresh2();
            if (stopMeasure.load() == 1 && store->CheckpointCheck()) {
                j++;
                if (j == 10) {
                    j = i;
                    break;
                }

            }
        }
    }
    store->StopSession();
}

void multiPoints() {
    output = new stringstream[thread_number];
    for (int i = 0; i < thread_number; i++) {
        pthread_create(&workers[i], nullptr, checkWorker, &parms[i]);
    }
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], nullptr);
    }
    cout << "checkpoint done ..." << endl;
}

int main(int argc, char **argv) {
    if (argc > 7) {
        thread_number = std::atol(argv[1]);
        key_range = std::atol(argv[2]);
        total_count = std::atol(argv[3]);
        timer_range = std::atol(argv[4]);
        skew = std::atof(argv[5]);
        updatePercentage = std::atoi(argv[6]);
        ereasePercentage = std::atoi(argv[7]);
        readPercentage = totalPercentage - updatePercentage - ereasePercentage;
    }
    if (argc > 9) {
        root_capacity = std::atoi(argv[8]);
        parallellog = (std::atoi(argv[9]) == 1 ? true : false);
    }
    if (argc > 10) {
        rounds = std::atoi(argv[10]);
    }
    store = new store_t(2,next_power_of_two(root_capacity / 2), 17179869184, "storage");
    ycsb::YCSBLoader loader(ycsb::loadpath, key_range);
    loads = loader.load();
    key_range = loader.size();
    prepare();
    cout << "multi insert" << endl;
    multiInsert();
    /*store->StartSession();
    auto callback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<UpsertContext> context{ctxt};
    };
    UpsertContext context{Key((uint8_t *) loads[0]->getKey(), std::strlen(loads[0]->getKey())),
                          Value((uint8_t *) loads[0]->getVal(), std::strlen(loads[0]->getVal())+5)};
    Status  stat = store->UpsertT(context, callback, 1, parallellog ? thread_number : 1);
    store->StopSession();*/
    //total_time = 0;
    //cout << "multi read" << endl;
    //readflag = 1;
    //insert_success = 0;
    //insert_failure = 0;
    //multiInsert();
    cout << "insert operations: " << insert_success << " insert failure: " << insert_failure << " throughput: "
         << (double) (insert_success + insert_failure) * thread_number / total_time
         << endl;
    total_time = 0;
    ycsb::YCSBLoader runner(ycsb::runpath, total_count);
    runs = runner.load();
    total_count = runner.size();
    cout << " threads: " << thread_number << " range: " << key_range << " count: " << total_count << " timer: "
         << timer_range << " skew: " << skew << " u:e:r = " << updatePercentage << ":" << ereasePercentage << ":"
         << readPercentage << endl;
    cout << "multiinsert" << endl;
    store->Gcflag = true;
    multiWorkers();
    cout << "read operations: " << read_success << " read failure: " << read_failure << " modify operations: "
         << modify_success << " modify failure: " << modify_failure << " throughput: "
         << (double) (read_success + read_failure + modify_success + modify_failure) * thread_number / total_time
         << endl;
    for (int i = 0; i < rounds; i++) {
        //stopMeasure.store(0, memory_order_relaxed);
        //cout << "checkpoint begin" << endl;
        //multiPoints();
        readflag=1;
        modify_success = 0;
        read_success = 0;
        insert_success = 0;
        total_time = 0;
        modify_failure = 0;
        read_failure = 0;
        insert_failure = 0;
        stopMeasure.store(0, memory_order_relaxed);
        cout << "multi insert" << endl;
        multiWorkers();
        cout << "read operations: " << read_success << " read failure: " << read_failure << " modify operations: "
             << modify_success << " modify failure: " << modify_failure << " throughput: "
             << (double) (read_success + read_failure + modify_success + modify_failure) * thread_number / total_time
             << endl;
    }
    finish();
    loads.clear();
    runs.clear();
    delete store;
    return 0;
}
