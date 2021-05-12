#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <string.h>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <assert.h>
#include <mutex>
#include "Connection.h"
#include "settings.h"
#include "tracer.h"
#include "hash.h"
#include "generator.h"
#include <vector>
#include <random>
#include <sys/stat.h>
#include <atomic>
#include <iostream>
#include <random>
#include <chrono>
#include <fstream>
#include <pthread.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <math.h>


using namespace std;

std::vector<ycsb::YCSB_request *> loads;

enum instructs {
    GET,
    SET,
    GETB,
    SETB,
};


char *my_database;

typedef struct Send_info {
    int thread_index;
    int fd;
    uint64_t send_bytes;
} send_info;

send_info **info_matrix;

uint64_t g_offset = 0;
uint64_t g_count = 0;
int myround = ROUND_SET;
bool stop = false;
bool clean = false;
mutex g_mutex;


void con_database();

unsigned char get_opcode(instructs inst);

void data_dispatch(int tid);

void show_send_info();


instructs inst;
int thread_num = 1;
//int batch_num = 1;

const string server_ip = "127.0.0.1";

long *timelist;


int main(int argc, char **argv) {

    string in_inst;
    if (argc == 4) {
        thread_num = atol(argv[1]);
        port_num = atol(argv[2]);
        in_inst = string(argv[3]);
       // batch_num = atol(argv[3]); //not used

    } else {
        printf("./micro_test <thread_num>  <port_num> <instruct> \n");
        return 0;
    }
    //thread_num = 4;
    //port_num = 4;
    in_inst = "set";
    timelist = (long *) calloc(thread_num, sizeof(long));


    double kv_n = KV_NUM;
    double p_l = PACKAGE_LEN;
    double data_size = (kv_n * p_l * ROUND_SET) / 1000000000;
    cout << "worker : " << thread_num << "\tport num : " << port_num << endl
         << "kv_num : " << KV_NUM << endl
         << "data size : " << data_size << "GB" << endl
         << "port base : " << PORT_BASE << endl;

    if (in_inst == "get") inst = GET;
    else if (in_inst == "getb") inst = GETB;
    else if (in_inst == "set") inst = SET;
    else if (in_inst == "setb") inst = SETB;
    else {
        perror("please input correct instruction");
        return -1;
    }

    hash_init();

    con_database();

    vector<thread> threads;

    for (int i = 0; i < thread_num; i++) {
        //printf("creating thread %d\n",i);
        threads.push_back(thread(data_dispatch, i));
    }
    for (int i = 0; i < thread_num; i++) {
        threads[i].join();
        //printf("thread %d stoped \n",i);
    }

//    show_send_info();

    long avg_runtime = 0;
    for (int i = 0; i < thread_num; i++) {
        avg_runtime += timelist[i];
    }
    avg_runtime /= thread_num;
    cout << "\n ** average runtime : " << avg_runtime << endl;


}


void con_database() {
    double skew = SKEW;
    uint64_t range = KEY_RANGE;
    uint64_t count = KV_NUM;
    ycsb::YCSBLoader loader(ycsb::loadpath, count);
    loads = loader.load();
    count = loader.size();
    cout << "count:" << count << endl;
    /*uint64_t *array =( uint64_t * ) calloc(count, sizeof(uint64_t));

    struct stat buffer;
    if (stat(existingFilePath, &buffer) == 0) {
        cout << "read generation" << endl;
        FILE *fp = fopen(existingFilePath, "rb+");
        fread(array, sizeof(uint64_t), count, fp);
        fclose(fp);
    }else{
        if (skew < zipf_distribution<uint64_t>::epsilon) {
            std::default_random_engine engine(
                    static_cast<uint64_t>(chrono::steady_clock::now().time_since_epoch().count()));
            std::uniform_int_distribution<size_t> dis(0, range + 0);
            for (size_t i = 0; i < count; i++) {
                array[i] = static_cast<uint64_t >(dis(engine));
            }
        } else {
            zipf_distribution<uint64_t> engine(range, skew);
            mt19937 mt;
            for (size_t i = 0; i < count; i++) {
                array[i] = engine(mt);
            }
        }
        FILE *fp = fopen(existingFilePath, "wb+");
        fwrite(array, sizeof(uint64_t), count, fp);
        fclose(fp);
        cout << "write generation" << endl;
    }*/



    my_database = (char *) calloc(KV_NUM, PACKAGE_LEN);
    if (my_database == NULL) {
        perror("calloc database error\n");
    }

    unsigned long offset = 0;

    uint8_t Magic = 0x80;
    uint8_t Opcode = get_opcode(inst);
    uint16_t Key_length = KEY_LEN;
    uint16_t Batch_num = 1;
    uint8_t Pre_hash = 0;
    uint8_t Retain = 0;
    uint32_t Total_body_length = KEY_LEN + VALUE_LEN;

    char package_buf[100];
    char key_buf[KEY_LEN + 1];
    char value_buf[VALUE_LEN + 1];


    for (size_t i = 0; i < KV_NUM; i++) {

        //uint64_t n = array[i] / 26;
        //uint8_t c = array[i] % 26;

        memset(package_buf, 0, sizeof(package_buf));
        memset(key_buf, 0, sizeof(key_buf));
        memset(value_buf, 0, sizeof(value_buf));

        *(uint8_t *) HEAD_MAGIC(package_buf) = Magic;
        *(uint8_t *) HEAD_OPCODE(package_buf) = Opcode;
        *(uint16_t *) HEAD_KEY_LENGTH(package_buf) = htons(Key_length);
        *(uint16_t *) HEAD_BATCH_NUM(package_buf) = htons(Batch_num);
        *(uint8_t *) HEAD_RETAIN(package_buf) = Retain;
        *(uint32_t *) HEAD_BODY_LENGTH(package_buf) = htonl(Total_body_length);

        /*sprintf(key_buf, "%d", n);
        sprintf(value_buf, "%d", n);
        memset(key_buf + strlen(key_buf), 'a'+c, KEY_LEN - strlen(key_buf));
        memset(value_buf + strlen(value_buf), 'a'+c+1 , VALUE_LEN - strlen(value_buf));*/
        memcpy(key_buf, (uint8_t *) loads[0]->getKey(), KEY_LEN);
        memcpy(value_buf, (uint8_t *) loads[0]->getVal(), VALUE_LEN);
        /*uint8_t *r =(uint8_t *) "8741836187007784";
        if(memcmp(key_buf,r,16)== 0)
            cout<<i<<" "<<(uint8_t *) loads[i]->getKey()<<" "<<(uint8_t *) loads[i]->getVal()<<endl;*/
        //Pre_hash = static_cast<uint8_t > ((hash_func(key_buf, KEY_LEN)) % port_num);
        //uint8_t t =((hash_func(key_buf, KEY_LEN))%67108864)%(port_num/4);
        //Pre_hash = static_cast<uint8_t > (((hash_func(key_buf, KEY_LEN))%67108864) /(67108864/4));
        //Pre_hash = Pre_hash * (port_num / 4)+t;
        Pre_hash = i % port_num;
        memcpy(PACKAGE_KEY(package_buf), key_buf, KEY_LEN);
        memcpy(PACKAGE_VALUE(package_buf), value_buf, VALUE_LEN);
        *(uint8_t *) HEAD_PRE_HASH(package_buf) = Pre_hash;

        memcpy(my_database + offset, package_buf, PACKAGE_LEN);
        offset += PACKAGE_LEN;

    }
}

unsigned char get_opcode(instructs inst) {
    switch (inst) {
        case GET :
            return 0x01;
        case SET :
            return 0x04;
        case GETB :
            return 0x03;
        case SETB :
            return 0x06;
        default:
            perror("invalid instruct");
            return 0;
    }
}

void data_dispatch(int tid) {
    Tracer t;

    vector<Connection> cons(port_num);

    for (int i = 0; i < port_num; i++) {
        cons[i].init(i, server_ip);
        if (cons[i].get_fd() == -1) return;
    }


    t.startTime();
    char *work_buf;
    bool end = false;
    while (!stop) {
        int local_count;
        g_mutex.lock();
        if (stop) {   //other thread changed the stop signal before lock
            g_mutex.unlock();
            break;
        }
        work_buf = my_database + g_offset;
        if (WORK_OP_NUM >= KV_NUM - g_count) {
            printf("myround %d end,g_count %d\n", myround, g_count);
            end = true;
            local_count = g_count;
            if (--myround <= 0) {
                stop = true;
            } else {
                stop = false;
                g_count = 0;  //for the next round
                g_offset = 0;
            }
        } else {
            end = false;
            g_offset += WORK_LEN;
            g_count += WORK_OP_NUM;
        }

        g_mutex.unlock();
        if (!end) {
            for (int i = 0; i < WORK_OP_NUM; i++) {
                uint8_t pre_hash = *(uint8_t *) HEAD_PRE_HASH(GET_PACKAGE(work_buf, i));
                package_obj p;
                p.package_ptr = GET_PACKAGE(work_buf, i);
                p.package_len = PACKAGE_LEN;
                cons[pre_hash].fetch_and_send(p);
            }
        } else {  //processing tail data
            for (int i = 0; i < KV_NUM - local_count; i++) {
                uint8_t pre_hash = *(uint8_t *) HEAD_PRE_HASH(GET_PACKAGE(work_buf, i));
                package_obj p;
                p.package_ptr = GET_PACKAGE(work_buf, i);
                p.package_len = PACKAGE_LEN;
                cons[pre_hash].fetch_and_send(p);
            }
        }

    }

    for (int i = 0; i < CONNECTION_NUM; i++) {
        cons[i].clean();
    }


/*
#define GET_LOCAL_PACKAGE(i)  (my_database + local_offset + i * PACKAGE_LEN )

    uint64_t local_send_num = KV_NUM / thread_num;

    uint64_t local_offset = tid * local_send_num * PACKAGE_LEN;

    for(int i = 0 ; i< local_send_num ; i ++){
        uint8_t pre_hash =*(uint8_t *) HEAD_PRE_HASH(GET_LOCAL_PACKAGE(i));
        package_obj p;
        p.package_ptr = GET_LOCAL_PACKAGE(i);
        p.package_len = PACKAGE_LEN;
        cons[pre_hash].fetch_and_send(p);
    }


    for(int i = 0; i < CONNECTION_NUM; i++){
        cons[i].clean();
    } */



//    for(int i = 0; i < CONNECTION_NUM; i++){
//        send_info ts;
//        ts.thread_index = tid;
//        ts.fd = cons[i].get_fd();
//        ts.send_bytes = cons[i].get_send_bytes();
//        info_matrix[tid][i] = ts;
//    }


    uint64_t g_totalbytes = 0;
    for (int i = 0; i < CONNECTION_NUM; i++) {
        g_totalbytes += cons[i].get_send_bytes();
    }
//    printf("[%d] total send bytes :%lu\n",tid,g_totalbytes);

    timelist[tid] += t.getRunTime();

}

void show_send_info() {
    for (int i = 0; i < thread_num; i++) {
        printf("thread %d :\n", i);
        for (int j = 0; j < CONNECTION_NUM; j++) {
            printf("[%d,%d] : %lu\n", i, info_matrix[i][j].fd, info_matrix[i][j].send_bytes);
        }
    }
}