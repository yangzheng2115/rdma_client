//
// Created by yangzheng on 2020/8/19.
//
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
#include <numa.h>
//#include "../../src/device/stringcontext.h"
#include "../../src/device/serializablecontext.h"

#define DEFAULT_THREAD_NUM (4)
#define DEFAULT_KEYS_COUNT (1 << 20)
#define DEFAULT_KEYS_RANGE (1 << 2)

#define DEFAULT_STR_LENGTH 256
#define DEFAULT_KEY_LENGTH 8
#define MAX_SOCKET 4
#define MAX_CORES 128
#define MAX_CORES_PER_SOCKET (MAX_CORES / MAX_SOCKET)

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

#define COUNT_HASH         1


void pin_to_core(size_t core) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

size_t cpus_per_socket = 8;

int coreToSocket[MAX_CORES];
int socketToCore[MAX_SOCKET][MAX_CORES_PER_SOCKET];

size_t count_of_socket = 1;


store_t *store1[MAX_SOCKET];

int mapping = 0x0f;

int numaScheme = 0x7f;     // 0th, 1st, 2nd numa-based initialization, insert, measure will be numa-based

int *card;

int *size;

size_t init_size = next_power_of_two(DEFAULT_STORE_BASE / 2);

store_t *store;

std::vector<ycsb::YCSB_request *> loads;

std::vector<ycsb::YCSB_request *> runs;

std::vector<ycsb::YCSB_request *> *localruns;

std::vector<ycsb::YCSB_request *> *localloads;

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
int root_capacity = 1000000;


int thread_number = DEFAULT_THREAD_NUM;

int hlog_number = 2;

int rounds = 0;
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
    int core;
    int socket;
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

/*
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
*/
void prepare() {
    cout << "prepare " << thread_number << endl;
    workers = new pthread_t[thread_number];
    parms = new struct target[thread_number];
    output = new stringstream[thread_number];
    int current_socket = -1;
    int current_core = cpus_per_socket;
    for (int i = 0; i < thread_number; i++) {
        parms[i].tid = i;
        if (current_core == cpus_per_socket) {
            do {
                current_socket++;
            } while (socketToCore[current_socket][0] < 0);
            current_core = 0;
        }
        parms[i].socket = current_socket;
        parms[i].core = socketToCore[current_socket][current_core++];
        //cout << parms[i].tid << " " << parms[i].core << " " << parms[i].socket << endl;
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
    struct target *work = (struct target *) args;
    if ((numaScheme & 0x2) != 0) pin_to_core(work->core);
    store->StartSession();
    uint64_t hit = 0;
    uint64_t fail = 0;
    uint8_t value[DEFAULT_STR_LENGTH];
    uint64_t location = work->tid / (thread_number / hlog_number);
    int tid = work->tid % (thread_number / hlog_number);
    for (int i = tid * total_count / (thread_number / hlog_number);
         i < (tid + 1) * total_count / (thread_number / hlog_number); i++) {
    //for (uint64_t i = 0; i < localloads[work->tid/(thread_number/hlog_number)].size(); i++) {
    //for (int i = tid * localloads[location].size() / (thread_number / hlog_number);
    //     i < (tid + 1) * localloads[location].size() / (thread_number / hlog_number); i++) {
        uint64_t hash = MurmurHash64A((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey()), hashseedA);
        hash = hash % init_size;
        uint64_t lid = hash * hlog_number / init_size;
        if(lid != location)
            continue;
        Status stat;
        if (readflag) {
            auto callback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<ReadContext> context{ctxt};
            };
            ReadContext context{
                    Key((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey()))};
            stat = store->Read(context, callback, 1);
        } else {
            auto callback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<UpsertContext> context{ctxt};
            };
            UpsertContext context{
                    Key((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey())),
                    Value((uint8_t *) loads[i]->getVal(),
                          2/*std::strlen(localloads[location][i]->getVal())*/)};
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
    struct target *work = (struct target *) args;
    if ((numaScheme & 0x4) != 0) pin_to_core(work->core);
    store->StartSession();
    uint64_t mhit = 0, rhit = 0;
    uint64_t mfail = 0, rfail = 0;
    uint8_t value[DEFAULT_STR_LENGTH];
    uint64_t location = work->tid / (thread_number / hlog_number);
    int tid = work->tid % (thread_number / hlog_number);
    //uint64_t location = work->tid * hlog_number / thread_number;
    //double number = hlog_number;
    int j=2;
    //int tid = fmod(work->tid, thread_number / number);
    //while (true){
    while (stopMeasure.load(memory_order_relaxed) == 0) {
        for (int i = tid * total_count / (thread_number / hlog_number);
             i < (tid + 1) * total_count / (thread_number / hlog_number); i++) {
        //for (uint64_t i = 0; i < localruns[work->tid / (thread_number / hlog_number)].size(); i++) {
        //cout<<location<<endl;
            uint64_t hash = MurmurHash64A((uint8_t *) runs[i]->getKey(), std::strlen(runs[i]->getKey()), hashseedA);
            hash = hash % init_size;
            uint64_t lid = hash * hlog_number / init_size;
            if(lid != location)
                continue;
            switch (static_cast<int>(runs[i]->getOp())) {
                case 0: {
                    //break;
                    auto callback = [](IAsyncContext *ctxt, Status result) {
                        CallbackContext<ReadContext> context{ctxt};
                    };
                    ReadContext context{Key((uint8_t *) runs[i]->getKey(),
                                            std::strlen(runs[i]->getKey()))};
                    Status stat = store->Read(context, callback, 1);
                    if (stat == Status::Ok) rhit++;
                    else rfail++;
                    break;
                }
                case 1:
                case 3: {//break;
                    if (readflag) {
                        auto callback = [](IAsyncContext *ctxt, Status result) {
                            CallbackContext<ReadContext> context{ctxt};
                        };
                        ReadContext context{Key((uint8_t *) runs[i]->getKey(),
                                                std::strlen(runs[i]->getKey()))};
                        Status stat = store->Read(context, callback, 1);
                        if (stat == Status::Ok) rhit++;
                        else rfail++;
                        //if (context.output_length == std::strlen(localruns[location][i]->getVal()) &&
                        //    (std::memcmp(context.output_bytes, (uint8_t *) localruns[location][i]->getVal(),
                        //                 std::strlen(localruns[location][i]->getVal()))) == 0)
                        //    rfail++;
                        break;
                    }
                    auto callback = [](IAsyncContext *ctxt, Status result) {
                        CallbackContext<UpsertContext> context{ctxt};
                    };
                    UpsertContext context{Key((uint8_t *) runs[i]->getKey(),
                                           std::strlen(runs[i]->getKey())),
                                       Value((uint8_t *) runs[i]->getVal(),
                                             2/*std::strlen(localruns[location][i]->getVal())*/)};
                    Status stat = store->UpsertT(context, callback, 1, parallellog ? thread_number : 1);
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
        //j++;
        //if(j==17)
        //    break;
    }
    r:
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
    int c = thread_number / 4 ;
    for (int i = 0; i < thread_number; i++) {
        //parms[i%c+28*i/c].tid= thread_number;
        //pthread_create(&workers[i], nullptr, measureWorker, &parms[i%c+28*i/c]);
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
    Tracer tracer;
    tracer.startTime();
    auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
        if (result != Status::Ok) {
            printf("Thread %" PRIu32 " reports checkpoint failed.\n",
                   Thread::id());
        } else {
            printf("Thread %" PRIu32 " reports persistence until %" PRIu64 "\n",
                   Thread::id(), persistent_serial_num);
        }
    };
    uint64_t location = work->tid * hlog_number / thread_number;
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
                break;
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
            if (store->Checkpoint(nullptr, hybrid_log_persistence_callback, token,thread_number)) {
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
    long elipsed = tracer.getRunTime();
    __sync_fetch_and_add(&total_time, elipsed);
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

void *createWorker(void *args) {
    struct target *work = (struct target *) args;
    if ((numaScheme & 0x4) != 0) pin_to_core(work->core);
    uint8_t value[DEFAULT_STR_LENGTH];
    int i = work->tid / (thread_number / hlog_number);
    store->Create(i);
    output[i] << work->tid << " " << i << " " << coreToSocket[work->core] << " " << endl;
}

void multiCreate() {
    output = new stringstream[hlog_number];
    int count = hlog_number / count_of_socket;
    int card = thread_number / hlog_number;
    for (int i = 0; i < hlog_number; i++) {
        pthread_create(&workers[i * card], nullptr, createWorker, &parms[i * card]);
    }
    for (int i = 0; i < hlog_number; i++) {
        pthread_join(workers[i * card], nullptr);
        string outstr = output[i * card].str();
        cout << outstr;
    }
    cout << "create done ..." << endl;
}

void RecoverAndTest(const Guid &index_token, const Guid &hybrid_log_token) {
    uint32_t version;
    uint64_t hit = 0;
    uint64_t fail = 0;
    std::vector<Guid> session_ids;
    store->Recover(index_token, hybrid_log_token, version, session_ids);
    cout << "recover successful" << endl;
    for (uint64_t i = 0; i < total_count; i++) {
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<ReadContext> context{ctxt};
        };
        ReadContext context{Key((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey()))};
        Status result = store->Read(context, callback, 1);
        if (result == Status::Ok)
            hit++;
        else{
            fail++;
            store->Read(context, callback, 1);
        }
    }
    cout << hit << "  " << fail << endl;
}

int main(int argc, char **argv) {
    if (argc > 8) {
        thread_number = std::atol(argv[1]);
        hlog_number = std::atol(argv[2]);
        key_range = std::atol(argv[3]);
        total_count = std::atol(argv[4]);
        timer_range = std::atol(argv[5]);
        skew = std::atof(argv[6]);
        updatePercentage = std::atoi(argv[7]);
        ereasePercentage = std::atoi(argv[8]);
        readPercentage = totalPercentage - updatePercentage - ereasePercentage;
    }
    if (argc > 10) {
        root_capacity = std::atoi(argv[9]);
        parallellog = (std::atoi(argv[10]) == 1 ? true : false);
    }
    if (argc > 11) {
        rounds = std::atoi(argv[11]);
    }
    int l = thread_number;
    //hlog_number = 4;
    count_of_socket = numa_max_node() + 1; // 2; //
    cout << count_of_socket << endl;
    cpus_per_socket = numa_num_task_cpus() / count_of_socket;
    struct bitmask *bm = numa_bitmask_alloc(numa_num_task_cpus());
    /*int pseudomapping[2][8] = {{1, 1, 1, 1, 0, 0, 0, 0},
                               {0, 0, 0, 0, 1, 1, 1, 1}};*/
    size_t mask = 0;
    thread_number = 0;
    for (int i = 0; i < MAX_CORES; i++) coreToSocket[i] = -1;
    for (int i = 0; i < MAX_SOCKET; i++) for (int j = 0; j < MAX_CORES_PER_SOCKET; j++) socketToCore[i][j] = -1;
    for (int i = 0; i < count_of_socket; i++) {
        mask |= (1 << i);
        numa_node_to_cpus(i, bm);
        if ((mapping & (1 << i)) != 0) {
            for (int j = 0, idx = 0; j < bm->size /*8*/; j++) {
                if (1 == /*pseudomapping[i][j]*/numa_bitmask_isbitset(bm, j)) {
                    socketToCore[i][idx++] = j;
                    coreToSocket[j] = i;
                }
            }
            thread_number += cpus_per_socket;
        }
    }
    size_t oldm = mapping;
    mapping &= mask;
    cout << count_of_socket << " " << cpus_per_socket << " " << mapping << " " << oldm << " " << mask << " " << bm->size
         << endl;
    store = new store_t(hlog_number, next_power_of_two(root_capacity / 2), 17179869184, "storage");//,68719476736ull,
    prepare();
    //store->Create(0);
    //store->Create(1);
    output = new stringstream[hlog_number];
    int cd = thread_number / hlog_number;
    for (int i = 0; i < hlog_number; i++) {
        pthread_create(&workers[i * cd], nullptr, createWorker, &parms[i * cd]);
        pthread_join(workers[i * cd], nullptr);
        string outstr = output[i].str();
        cout << outstr;
    }
    thread_number=l;
    //multiCreate();
    ycsb::YCSBLoader loader(ycsb::loadpath, key_range);
    loads = loader.load();
    key_range = loader.size();
    //prepare();
    init_size = next_power_of_two(root_capacity / 2);
    cout << "multi insert" << endl;
    multiInsert();
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
    //store->Gcflag = true;
    multiWorkers();
    cout << "read operations: " << read_success << " read failure: " << read_failure << " modify operations: "
         << modify_success << " modify failure: " << modify_failure << " throughput: "
         << (double) (read_success + read_failure + modify_success + modify_failure) * thread_number / total_time
         << endl;
    for (int i = 0; i < rounds; i++) {
        total_time = 0;
        stopMeasure.store(0, memory_order_relaxed);
        cout << "checkpoint begin" << endl;
        multiPoints();
        cout<<"total time: "<<total_time<<"  averagetime:"<<total_time/thread_number<<endl;
        //readflag = 1;
        modify_success = 0;
        read_success = 0;
        insert_success = 0;
        total_time = 0;
        modify_failure = 0;
        read_failure = 0;
        insert_failure = 0;
        stopMeasure.store(0, memory_order_relaxed);
        for (int i = 0; i < hlog_number; i++) {
            //size[i] = 250000;
        }
        cout << "multi insert" << endl;
        multiInsert();
        cout << "read operations: " << read_success << " read failure: " << read_failure << " modify operations: "
             << modify_success << " modify failure: " << modify_failure << " throughput: "
             << (double) (read_success + read_failure + modify_success + modify_failure) * thread_number / total_time
             << endl;
    }
    finish();
    delete store;
    return 0;
}
