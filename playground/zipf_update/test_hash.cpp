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


#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event_struct.h>
#include <event2/event_compat.h>
#include <vector>
#include <signal.h>
using namespace std;

std::vector<ycsb::YCSB_request *> loads;

enum instructs {
    GET,
    SET,
    GETB,
    SETB,
};


char *my_database;
char *send_database[96];
uint64_t send_offset[96];


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

void data_send(int tid);

void data_recv(int tid);

void show_send_info();


instructs inst;
int thread_num = 1;
int recv_num = 1;
//int batch_num = 1;

const string server_ip = "172.168.204.99";//"127.0.0.1";

long *timelist;

vector<Connection> cons(400);

vector<Connection> recv_cons(400);

Connection local_cons[96][96];

atomic<int> send_stop{0};

atomic<bool> stop_flag[96];

uint64_t *send_bytes;

uint64_t *recv_bytes;

uint64_t **recv_number;

int recv_sfd[96];
uint64_t num = 0;

int main(int argc, char **argv) {

    string in_inst;
    if (argc == 5) {
        thread_num = atol(argv[1]);
        port_num = atol(argv[2]);
        recv_num = atol(argv[3]);
        in_inst = string(argv[4]);
        //in_inst = string(argv[3]);
       // batch_num = atol(argv[3]); //not used

    } else {
        printf("./micro_test <thread_num>  <port_num> <instruct> \n");
        return 0;
    }
    //thread_num = 2;
    //port_num = 2;
    //in_inst = "set";
    timelist = (long *) calloc(thread_num, sizeof(long));
    send_bytes = (uint64_t *) calloc(96, sizeof(uint64_t));
    recv_bytes = (uint64_t *) calloc(port_num, sizeof(uint64_t));
    recv_number = new uint64_t *[port_num];
    for (int i = 0; i < port_num; i++) {
        recv_number[i] = new uint64_t[thread_num];
        stop_flag[i].store(false);
    }

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
      
    signal(SIGPIPE, SIG_IGN);    


    for (int i = 0; i < recv_num; i++) {
        //cons[i].init_server(i, server_ip);
        int cd = recv_num / 16;
        recv_cons[i].init_server(i/cd*6+i%cd+96*3, server_ip);
        //recv_cons[i].init_server(i+96*3, server_ip);
        if (recv_cons[i].get_fd() == -1) {
              cout<<"connnect error"<<endl;
              return 0;
        }
        recv_sfd[i] = recv_cons[i].get_fd() ;
    }


    for (int i = 0; i < port_num; i++) {
        //cons[i].init_server(i, server_ip);
        cons[i].init_server(i * 96 / port_num+ 96 *3, server_ip);
        if (cons[i].get_fd() == -1) {
              cout<<"connnect error"<<endl; 
              return 0;
        }
    }
    /* 
       for (int j = 0; j < 96/port_num; j++) {
        evutil_socket_t listener;

        struct sockaddr_in sin;
        sin.sin_family = AF_INET;
        sin.sin_addr.s_addr = 0;
        sin.sin_port = htons(PORT_BASE+i* 96 / port_num+j);
        cout<<i* 96 / port_num+j<<endl;
        listener = socket(AF_INET, SOCK_STREAM, 0);
        //evutil_make_socket_nonblocking(listener);


        if (bind(listener, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
            perror("bind");
            return -1;
        }

        if (listen(listener, 16) < 0) {
            perror("listen");
            return -1;
        }
        struct sockaddr_storage ss;
        socklen_t slen = sizeof(ss);
        int fd = accept(listener, (struct sockaddr *) &ss, &slen);

        if (fd == -1) {
            perror("accept()");
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                //these are transient, so don't log anything /
                return 0;
            } else if (errno == EMFILE) {
                fprintf(stderr, "Too many open connections\n");
                exit(1);
            } else {
                perror("accept()");
                return 0;
            }
        }

        if (fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK) < 0) {
            perror("setting O_NONBLOCK");
            close(fd);
            return 0;
        }
        cout<<fd<<endl;
        //cout<<i* 96 / port_num+j<<endl;
        recv_sfd[i* 96 / port_num+j] = fd ;
    }
    }*/
    for (int i = 0; i < thread_num; i++) {
        for (int j = 0; j < port_num; j++)
            local_cons[i][j].init();
    }

    vector<thread> threads;

    for (int i = 0; i < thread_num; i++) {
        //printf("creating thread %d\n",i);
        threads.push_back(thread(data_dispatch, i));
    }
    for (int i = thread_num; i < thread_num * 2; i++) {
        //printf("creating thread %d\n",i);
        threads.push_back(thread(data_send, i));
    }
    for (int i = thread_num * 2; i < thread_num * 2 + recv_num; i++) {
        //printf("creating thread %d\n",i);
        //threads.push_back(thread(data_recv, i));
    }
    for (int i = 0; i < thread_num * 1; i++) {
        threads[i].join();
        //printf("thread %d stoped \n",i);
    }
    cout<<"success"<<endl;
    for (int i = thread_num; i < thread_num * 2; i++) {
        //printf("creating thread %d\n",i);
         cout << "port num " << i-thread_num << " send bytes:" << send_bytes[i-thread_num] <<endl;
         threads[i].join();
        //threads.push_back(thread(data_send, i));
    }
    cout<<"success"<<endl;
    sleep(5);
    for (int i = thread_num * 2; i < thread_num * 2 + recv_num; i++) {
        //printf("creating thread %d\n",i);
        //threads[i].join();
        cout<<"port num " <<i-thread_num * 2<<"  "<<recv_bytes[i-thread_num * 2]<<endl;
        //threads[i].join();
        //threads.push_back(thread(data_recv, i));
    }
    cout<<"success"<<endl;

//    show_send_info();

    long avg_runtime = 0;
    for (int i = 0; i < port_num; i++) {
        uint64_t sum =0;
        for(int j =0; j< recv_num/port_num; j++)
              sum += recv_bytes[i* recv_num / port_num+j];
        //for(int j =0; j< 96/port_num; j++)
        //      sum += recv_bytes[i* 96 / port_num+j];
        cout << "port num " << i << " send bytes:" << send_bytes[i] <<" recv bytes:"<<sum<< endl;
    }
    for (int i = 0; i < thread_num; i++) {
        avg_runtime += timelist[i];
    }
    avg_runtime /= thread_num;
    cout << "\n ** average runtime : " << avg_runtime << endl;

}


void con_database() {
    double skew = SKEW;
    uint64_t range = KEY_RANGE;
    uint64_t count = KV_NUM*6;
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

    //uint64_t num = 0;
    for (size_t i = 0; i < count/*KV_NUM*/; i++) {

        //uint64_t n = array[i] / 26;
        //uint8_t c = array[i] % 26;

        memset(package_buf, 0, sizeof(package_buf));
        memset(key_buf, 0, sizeof(key_buf));
        memset(value_buf, 0, sizeof(value_buf));

        *(uint8_t *) HEAD_MAGIC(package_buf) = Magic;
        *(uint8_t *) HEAD_OPCODE(package_buf) = Opcode;
        *(uint16_t *) HEAD_KEY_LENGTH(package_buf) = htons(Key_length);
        *(uint16_t *) HEAD_BATCH_NUM(package_buf) = htons(500/*SEND_BATCH*/);
        *(uint8_t *) HEAD_RETAIN(package_buf) = Retain;
        *(uint16_t *) HEAD_BODY_LENGTH(package_buf) = htons(Total_body_length);

        /*sprintf(key_buf, "%d", n);
        sprintf(value_buf, "%d", n);
        memset(key_buf + strlen(key_buf), 'a'+c, KEY_LEN - strlen(key_buf));
        memset(value_buf + strlen(value_buf), 'a'+c+1 , VALUE_LEN - strlen(value_buf));*/
        memcpy(key_buf, (uint8_t *) loads[i]->getKey(), KEY_LEN);
        memcpy(value_buf, (uint8_t *) loads[i]->getVal(), VALUE_LEN);
        /*uint8_t *r =(uint8_t *) "8741836187007784";
        if(memcmp(key_buf,r,16)== 0)
            cout<<i<<" "<<(uint8_t *) loads[i]->getKey()<<" "<<(uint8_t *) loads[i]->getVal()<<endl;*/
        //Pre_hash = static_cast<uint8_t > ((hash_func(key_buf, KEY_LEN)) % port_num);
        uint8_t t =((hash_func(key_buf, KEY_LEN))%67108864)%(port_num/2);
        Pre_hash = static_cast<uint8_t > (((hash_func(key_buf, KEY_LEN))%67108864) /(67108864/8));
        //Pre_hash = Pre_hash * (port_num / 4)+t;
        //Pre_hash = i % port_num;
        if(Pre_hash == 6 || Pre_hash == 7){
        Pre_hash -=6;
        Pre_hash = Pre_hash * (port_num / 2)+t;
        memcpy(PACKAGE_KEY(package_buf), key_buf, KEY_LEN);
        memcpy(PACKAGE_VALUE(package_buf), value_buf, VALUE_LEN);
        *(uint8_t *) HEAD_PRE_HASH(package_buf) = Pre_hash;

        memcpy(my_database + offset, package_buf, PACKAGE_LEN);
        offset += PACKAGE_LEN;
        num++;
        //if(num == KV_NUM)
        //   break;
        }

    }
    cout<<KV_NUM<<" "<<num<<endl;
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
    int id = tid;
    int f = 0;
    Tracer t;
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
        if (WORK_OP_NUM >= num/*KV_NUM*/ - g_count) {
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
                *(uint16_t *) HEAD_THREAD_NUM(GET_PACKAGE(work_buf, i)) = htons(id);
                package_obj p;
                p.package_ptr = GET_PACKAGE(work_buf, i);
                p.package_len = PACKAGE_LEN;
                local_cons[id][pre_hash].fetch_and_send(p);
            }
        } else {  //processing tail data
            for (int i = 0; i < num/*KV_NUM*/ - local_count; i++) {
                uint8_t pre_hash = *(uint8_t *) HEAD_PRE_HASH(GET_PACKAGE(work_buf, i));
                package_obj p;
                p.package_ptr = GET_PACKAGE(work_buf, i);
                p.package_len = PACKAGE_LEN;
                local_cons[id][pre_hash].fetch_and_send(p);
            }
        }

    }
    send_stop.fetch_add(1);
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


    /*uint64_t g_totalbytes = 0;
    for (int i = 0; i < CONNECTION_NUM; i++) {
        g_totalbytes += cons[i].get_send_bytes();
    }
     */
//    printf("[%d] total send bytes :%lu\n",tid,g_totalbytes);

    timelist[tid] += t.getRunTime();

}

void data_send(int tid) {
    int id = tid - thread_num;
    uint64_t g_totalbytes = 0;
    int offset = 0;
    while (send_stop.load() != port_num) {
        for (int i = 0; i < port_num; i++) {
            if (local_cons[i][id].flag.load() == 1) {
                while(true){
                int ret = write(cons[id].get_fd(), local_cons[i][id].get_buf()+offset, local_cons[i][id].get_offset()-offset);
                if(ret >0)
                   offset+=ret;
                if (offset == local_cons[i][id].get_offset()) {
                    //actually not error;we didn't take this condition into consideration before,so we let it crash here
                    //once the program exit here, old test data need to be reconsidered.
                    //perror("write error");
                    //exit(-1);
                    g_totalbytes += offset;
                    offset = 0;
                    local_cons[i][id].flag.fetch_sub(1);
                    break;
                }
               }
                //local_cons[i][id].flag.fetch_sub(1);
                //g_totalbytes += ret;
            }
        }
        std::this_thread::sleep_for( std::chrono::nanoseconds(5));
    }
    /*
    offset = 0;
    for (int i = 0; i < port_num; i++) {
        while(true){
                if(local_cons[i][id].get_offset() == 0)
                         break;
                int ret = write(cons[id].get_fd(), local_cons[i][id].get_buf()+offset, local_cons[i][id].get_offset()-offset);
                if(ret >0)
                   offset+=ret;
                if (offset == local_cons[i][id].get_offset()) {
                    //actually not error;we didn't take this condition into consideration before,so we let it crash here
                    //once the program exit here, old test data need to be reconsidered.
                    //perror("write error");
                    //exit(-1);
                    g_totalbytes += offset;
                    offset = 0;
                    //local_cons[i][id].flag.fetch_sub(1);
                    break;
                }
     }

    }
    */
    send_bytes[id] += g_totalbytes;
    stop_flag[id].store(true);
    close(cons[id].get_fd());
    //if(id == 3)
    //    cout<<send_bytes[id]<<endl;
    //cout<<"end"<<endl;
    //cout << g_totalbytes << endl;
}

void data_recv(int tid) {
    int id = tid - thread_num * 2;
    char rec_buf[500 * PACKAGE_LEN];
    int offset = 0;
    int qw = 0;
    recv_bytes[id] = 0;
    while (true) {
        //int ret = read(cons[id/*+thread_num*/].get_fd()+2, rec_buf + offset, SEND_BATCH * PACKAGE_LEN - offset);
        int ret = read(recv_sfd[id], rec_buf + offset, 500 * PACKAGE_LEN - offset);
        if (ret == 0) {
            //if(id == 2)
            //cout<<id<<" " << recv_bytes[id] <<" "<<send_bytes[id]<< endl;
            break;
            //perror("server error");
            //exit(-1);
        }
        if (ret == -1) {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                //printf("read again\n");
            } else {
                perror("read error");
                exit(-1);
            }
           //if (stop_flag[id].load()) {
           //     if (recv_bytes[id] >= send_bytes[id]/*||recv_bytes[id] >= send_bytes[id] - SEND_BATCH * PACKAGE_LEN*/) {
           //         break;
           //     }
           //}
        } else {
            recv_bytes[id] += ret;
            //if(id == 4)
             //cout<<recv_bytes[id]<<endl;
             //cout<<"33333  "<<send_bytes[4]<<endl;
            offset += ret;
            if (offset == 500 * PACKAGE_LEN) {
                offset = 0;
                /*for (int i = 0; i < SEND_BATCH; i++) {
                    protocol_binary_request_header *req;
                    req = (protocol_binary_request_header *) GET_PACKAGE(rec_buf, i);
                    uint16_t num = ntohs(req->threadnum);
                    recv_number[id][num]++;
                }*/
            }
            //cout<<ret<<endl;
            //if (stop_flag[id].load()) {
                //if (recv_bytes[id] >= send_bytes[id]/*||recv_bytes[id] >= send_bytes[id] - SEND_BATCH * PACKAGE_LEN*/) {
                    //cout << recv_bytes[id] << endl;
                    //cout<<recv_number[0][0]<<endl;
                //    break;
              //  }
            //}
        }
        std::this_thread::sleep_for( std::chrono::nanoseconds(5));
    }
    //close(cons[id].get_fd()+2);
    //if(id == 0)
    //cout<<"end"<<endl;
}

void show_send_info() {
    for (int i = 0; i < thread_num; i++) {
        printf("thread %d :\n", i);
        for (int j = 0; j < CONNECTION_NUM; j++) {
            printf("[%d,%d] : %lu\n", i, info_matrix[i][j].fd, info_matrix[i][j].send_bytes);
        }
    }
}
