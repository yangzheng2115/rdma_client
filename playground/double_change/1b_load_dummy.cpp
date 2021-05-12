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
#include <boost/unordered_map.hpp>

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
char *send_database[384];
char *push_database[384];
char *change_offset[384];
uint64_t send_offset[384];
uint64_t send_num[384];

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

void con_database_run();

unsigned char get_opcode(instructs inst);

void data_dispatch(int tid);

void data_send(int tid);

void data_recv(int tid);

void data_gettime(int tid);

void data_push(int tid);

void show_send_info();


instructs inst;
int thread_num = 1;
int recv_num = 1;
//int batch_num = 1;

string server_ip = "172.168.204.99";//"127.0.0.1";

long *timelist;

vector<Connection> cons(400);

vector<Connection> recv_cons(400);

Connection local_cons[384][384];

uint64_t thread_offset[400][100];

uint64_t thread_recv[400][100];

atomic<int> head{0};

atomic<int> tail{0};

int limit = 20;

atomic<uint64_t> send_stop{0};

atomic<uint64_t> send_start{0};

atomic<uint64_t> stop_time{0};

atomic<bool> stop_flag[96];

atomic<int> recv_flag[96];

uint64_t *send_bytes;

uint64_t *recv_bytes;

uint64_t **recv_number;

uint32_t timestamp = 0;

int recv_sfd[96];
uint64_t num = 0;

void pin_to_core(size_t core) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    //for(int i=0;i<48;i++){
         CPU_SET(core+0, &cpuset);
    //}
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

int main(int argc, char **argv) {

    string in_inst;
    if (argc == 5) {
        thread_num = atol(argv[1]);
        port_num = atol(argv[2]);
        send_batch = atol(argv[3]);
        //recv_num = atol(argv[3]);
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
    sleep(300);
    recv_num = port_num;
    timelist = (long *) calloc(thread_num, sizeof(long));
    send_bytes = (uint64_t *) calloc(port_num, sizeof(uint64_t));
    recv_bytes = (uint64_t *) calloc(port_num, sizeof(uint64_t));
    recv_number = new uint64_t *[port_num];
    timestamp = 0;
    for (int i = 0; i < port_num; i++) {
        recv_number[i] = new uint64_t[thread_num];
        stop_flag[i].store(false);
        recv_flag[i].store(0);
    }
    send_stop.store(0);
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

    for (int i = 0; i < port_num; i++) {
        send_database[i] = (char *) calloc(/*KV_NUM*/WORK_OP_NUM*limit, PACKAGE_LEN);
        //push_database[i] = (char *) calloc(KV_NUM, PACKAGE_LEN);
        //change_offset[i] = (char *) calloc(KV_NUM, 8);
        if (send_database[i] == NULL) {
            perror("calloc database error\n");
        }
        send_offset[i] = 0;
        send_num[i]=0;
    }

    //con_database();

    /*for (int i = 0; i < port_num; i++) 
            cout<<i<<" "<<send_offset[i]/36<<endl;
    return 0;*/ 
    signal(SIGPIPE, SIG_IGN);    


    /*for (int i = 0; i < recv_num; i++) {
        //cons[i].init_server(i, server_ip);
        int cd = recv_num / 16;
        recv_cons[i].init_server(i/cd*6+i%cd+96*3, server_ip);
        //recv_cons[i].init_server(i+96*3, server_ip);
        if (recv_cons[i].get_fd() == -1) {
              cout<<"connnect error"<<endl;
              return 0;
        }
        recv_sfd[i] = recv_cons[i].get_fd() ;
    }*/


    for (int i = 0; i < port_num; i++) {
        //cons[i].init_server(i, server_ip);
        int id = i % 96 ;
        if(i/96 == 0)
           server_ip = "172.168.204.90";
        else if(i/96 == 1)
           server_ip = "172.168.204.97";
        else if(i/96 == 2)
           server_ip = "172.168.204.98";
        else
           server_ip = "172.168.204.99";
        cons[i].init_server(i, server_ip);
        //cons[i].init_server(id * 96 / 96+ 96 *3, server_ip);
        if (cons[i].get_fd() == -1) {
              cout<<"connnect error"<<endl; 
              return 0;
        }
    }
    server_ip = "172.168.204.90";
    cons[port_num].init_server(port_num, server_ip);
    if (cons[port_num].get_fd() == -1) {
              cout<<"connnect error"<<endl;
              return 0;
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
    
    for(int i =0;i<port_num;i++){
        for (int j = 0; j < limit; j++)
            {
            thread_offset[i][j] = 0;
            thread_recv[i][j] = 0;
            }
    }

    vector<thread> threads;
    
    for (int i = 0; i < thread_num; i++) {
        //printf("creating thread %d\n",i);
        //threads.push_back(thread(data_dispatch, i));
    }
    for (int i = thread_num*0; i < thread_num * 1; i++) {
        //printf("creating thread %d\n",i);
        threads.push_back(thread(data_send, i));
        //threads[i+1].join();
        //cout<<i<<endl;
    }
    for (int i = thread_num * 1; i < thread_num * 1 + 1; i++) {
        //printf("creating thread %d\n",i);
        threads.push_back(thread(data_gettime, i));
        //threads.push_back(thread(data_recv, i));
    }
    for (int i = thread_num * 1 +1; i < thread_num * 1 + 2; i++) {
        threads.push_back(thread(data_push, i));
    }
    for (int i = 0; i < thread_num * 1; i++) {
        //threads[i].join();
        //printf("thread %d stoped \n",i);
    }
    cout<<"success"<<endl;
    for (int i = thread_num*0; i < thread_num * 1; i++) {
        //printf("creating thread %d\n",i);
         //cout << "port num " << i-thread_num << " send bytes:" << send_bytes[i-thread_num] <<endl;
         threads[thread_num -1-i].join();
         //cout<<"join"<<i<<endl;
        //threads.push_back(thread(data_send, i));
    }
    stop_time.fetch_add(1);
    cout<<"success"<<endl;
    sleep(5);
    for (int i = thread_num * 1; i < thread_num * 1 + 2; i++) {
        //printf("creating thread %d\n",i);
        threads[i].join();
        //cout<<"port num " <<i-thread_num * 2<<"  "<<recv_bytes[i-thread_num * 2]<<endl;
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
    uint64_t count = KV_NUM;
    cout<<"begin read"<<endl;
    //sleep(10);
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
    uint16_t Pre_hash = 0;
    uint8_t Retain = 0;
    uint32_t Total_body_length = KEY_LEN + VALUE_LEN;
    uint32_t Time = 1;

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

        Total_body_length = strlen(loads[i]->getKey())+VALUE_LEN;
        //Total_body_length = 16 + VALUE_LEN;
        Key_length = strlen(loads[i]->getKey());

        *(uint8_t *) HEAD_MAGIC(package_buf) = Magic;
        *(uint8_t *) HEAD_OPCODE(package_buf) = Opcode;
        *(uint16_t *) HEAD_KEY_LENGTH(package_buf) = htons(Key_length);
        *(uint16_t *) HEAD_BATCH_NUM(package_buf) = htons(SEND_BATCH);
        *(uint8_t *) HEAD_RETAIN(package_buf) = Retain;
        *(uint16_t *) HEAD_BODY_LENGTH(package_buf) = htons(Total_body_length);
        *(uint32_t *) HEAD_TIME(package_buf) = htonl(Time);

        /*sprintf(key_buf, "%d", n);
        sprintf(value_buf, "%d", n);
        memset(key_buf + strlen(key_buf), 'a'+c, KEY_LEN - strlen(key_buf));
        memset(value_buf + strlen(value_buf), 'a'+c+1 , VALUE_LEN - strlen(value_buf));*/
        memcpy(key_buf, (uint8_t *) loads[i]->getKey(), Key_length);
        memcpy(value_buf, (uint8_t *) loads[i]->getVal(), VALUE_LEN);
        /*uint8_t *r =(uint8_t *) "8741836187007784";
        if(memcmp(key_buf,r,16)== 0)
            cout<<i<<" "<<(uint8_t *) loads[i]->getKey()<<" "<<(uint8_t *) loads[i]->getVal()<<endl;*/
        //Pre_hash = static_cast<uint8_t > ((hash_func(key_buf, KEY_LEN)) % port_num);
        uint8_t t =((hash_func(key_buf, KEY_LEN))%67108864)%(48);
        Pre_hash = static_cast<uint8_t > (((hash_func(key_buf, Key_length))%67108864) /(67108864/8));
        //Pre_hash = Pre_hash * (port_num / 4)+t;
        //Pre_hash = i % port_num;
        //if(Pre_hash < 2){
        //Pre_hash -=6;
        Pre_hash = Pre_hash * (48)+t;
        memcpy(PACKAGE_KEY(package_buf), key_buf, Key_length);
        memcpy(package_buf+Key_length+HEAD_LEN, value_buf, VALUE_LEN);
        *(uint16_t *) HEAD_PRE_HASH(package_buf) = Pre_hash;

        memcpy(my_database + offset, package_buf, HEAD_LEN + Total_body_length);
        offset += HEAD_LEN + Total_body_length;
        
        
        //memcpy(send_database[Pre_hash] + send_offset[Pre_hash], package_buf, HEAD_LEN + Total_body_length);
        //send_offset[Pre_hash] += HEAD_LEN + Total_body_length;
        //int c = send_num[Pre_hash] * 8;
        //*(uint64_t *)(change_offset[Pre_hash]+c) = send_offset[Pre_hash];
        //send_num[Pre_hash]++;
        //send_offset[Pre_hash] += HEAD_LEN + Total_body_length;
 
        //if(i %10000  == 0)
        //    cout<<i<<endl;
        //   cout<<send_offset[Pre_hash]<<"  "<<*(uint64_t *)(change_offset[Pre_hash]+c)<<" "<<c<<endl;                
        //*(uint32_t *) HEAD_TIME(package_buf) = htonl(Time+1);
        //memcpy(send_database[Pre_hash+96] + send_offset[Pre_hash+96], package_buf, PACKAGE_LEN);
        //send_offset[Pre_hash+96] += PACKAGE_LEN;
 
        num++;
        //if(num == KV_NUM)
        //   break;
        //}

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

void data_push(int tid) {
    uint64_t  snum  =0;
    int round = ROUND_SET;
   boost::unordered_map<string,int> map;
   for (int i = 0; i < port_num; i++) {
            send_offset[i] = 16;
   }
    while (!stop) {
        if (WORK_OP_NUM > KV_NUM - g_count) {
            printf("myround %d end,g_count %d\n", myround, g_count);
            if (--round <= 0) {
                stop = true;
            } else {
                stop = false;
                g_count = 0;  //for the next round
                g_offset = 0;
                continue;
            }
        } else {
            //g_offset += WORK_LEN;
            g_count += WORK_OP_NUM;
        }

        //while (recv_flag[1].load() != 0){
        //    std::this_thread::sleep_for( std::chrono::nanoseconds(1));
        //}
        
        uint64_t h, t;
        h = head.load();
        t = tail.load();
        while((h + limit - t) % limit == 1){
            h = head.load();
            std::this_thread::sleep_for(std::chrono::nanoseconds(1));
        }

        //send_offset[pre_hash] = 0;
        int map_num = 0;
        /*for (int i = 0; i < WORK_OP_NUM; i++) {
            uint16_t pre_hash = *(uint16_t *) HEAD_PRE_HASH(my_database + g_offset);
            //pre_hash = i;
           
            package_obj p;
            uint16_t body = *(uint16_t *) HEAD_BODY_LENGTH(my_database + g_offset);
            //cout<<*(uint16_t *) HEAD_BODY_LENGTH(my_database + g_offset)<<endl;
            p.package_ptr = my_database + g_offset;
            p.package_len = ntohs(body) + HEAD_LEN;
            //uint16_t  key_len = *(uint16_t *) HEAD_KEY_LENGTH(my_database + g_offset);
            //cout<<a<<endl;
            //if(snum % 2 == 0)
            //    memcpy(send_database[pre_hash]+send_offset[pre_hash], p.package_ptr, p.package_len);
            //else
            //    memcpy(push_database[pre_hash] + send_offset[pre_hash], p.package_ptr, p.package_len);
            memcpy(send_database[pre_hash] + t*WORK_OP_NUM*PACKAGE_LEN+send_offset[pre_hash], p.package_ptr, p.package_len);
            //memcpy(send_database[pre_hash] + send_offset[pre_hash], p.package_ptr, p.package_len);
            send_offset[pre_hash] += p.package_len;
            g_offset += p.package_len;
            thread_recv[pre_hash][t]+=p.package_len-VALUE_LEN+8;
        }*/
        for (int i = 0; i < port_num; i++) {
            thread_offset[i][t] = send_offset[i];
            send_offset[i] = 16;
        }
         if (t < limit - 1)
            tail.fetch_add(1);
        else
            tail.fetch_sub(limit - 1);
        //recv_flag[1].fetch_add(1);
        snum++;
        //cout<<snum<<endl;
        //if(g_count == 250000)
        //    continue;
        if(snum == 2500000 / SEND_BATCH * myround)
            break;
    }
}

void data_gettime(int tid) {
    int id = tid ;
    uint64_t snum = 0;
    int recv_offset = 0;
    char * dummy = (char *) calloc(1, 4);
    char * gettime = (char *) calloc(1, 4);
    uint32_t ts;
    uint64_t h, t;
     while(stop_time.load() == 0){
           send_stop.fetch_add(1);
           snum++;
           //if(num == 250000 / SEND_BATCH * myround)
           //     break;
          int j =0;
           while(send_stop.load()%(/*port_num*/96+1)!=0)
           {  
                  //j++;
                  //if(j == 1000000)
                  //  cout<<head.load()<<" "<<tail.load()<<" "<<send_stop.load()<<endl;
                  std::this_thread::sleep_for( std::chrono::nanoseconds(1));
           }
           if(snum > 1){
            h = head.load();
            if (h < limit - 1)
                head.fetch_add(1);
            else
                //head.store(0);
                head.fetch_sub(limit - 1);
        }
        h = head.load();
        t = tail.load();
        while (h == t) {
            t = tail.load();
            std::this_thread::sleep_for(std::chrono::nanoseconds(1));
        }
           //if(snum >1)
           //   recv_flag[1].fetch_sub(1);
           int ret = write(cons[id].get_fd(), dummy, 4);
           while(true){
            int re = read(cons[id].get_fd(), gettime, 4 - recv_offset);
            if(re>0){
                 //recv_bytes[id] += re;
                 recv_offset += re;
            }
            if (recv_offset == 4) {
                recv_offset = 0;
                break;
            }
            std::this_thread::sleep_for(std::chrono::nanoseconds(5));
           }
           ts = *(uint32_t *)gettime;
           timestamp = ntohl(ts);
           recv_flag[0].fetch_add(1);
           if(snum == 2500000 / SEND_BATCH * myround)
                break;
     }
     cout<<snum<<endl;
     close(cons[id].get_fd());
}

void data_send(int tid) {
    int id = tid ;
    //pin_to_core(id%96);
    uint64_t g_totalbytes = 0;
    Tracer t;
    t.startTime();
    char rec_buf[10000 * 2 * PACKAGE_LEN];
    int recv_offset = 0;
    int offset = 0;
    uint32_t s=0;
    uint64_t snum = 0;
    char * package_buf ; 
    char * dummy = (char *) calloc(1, PACKAGE_LEN);
    *(uint16_t *) HEAD_PRE_HASH(dummy) = htons(0);
    char * size_buf = (char *) calloc(1, PACKAGE_LEN);
    if(id>=96)
        return ;
    //cout<<"begin"<<endl;
    recv_bytes[id] = 0;
    uint64_t data_offset = 2500000;//send_offset[id];
    uint64_t num = 0;
    //goto y;
    for(int i =0;i<myround;i++){
        while(num!=data_offset){
            s = SEND_BATCH;
            if(data_offset - num < SEND_BATCH)
                 break;
                 //s = data_offset;
            //if(g_totalbytes != 0){
            send_stop.fetch_add(1);
            snum++;
            while(recv_flag[0].load()!=snum)
            {
                  std::this_thread::sleep_for( std::chrono::nanoseconds(5));
            }
            if(snum == 2500000 / SEND_BATCH * myround)
                break;
            //continue;
            //package_buf = send_database[id]+offset;
            //*(uint32_t *) HEAD_TIME(package_buf) = htonl(timestamp);
            num += SEND_BATCH;
            uint64_t h ;//= head.load();
            h = (snum -1) % limit;
            s = thread_offset[id][h];
            if(s < 0)
               cout<<s<<" "<<id<<endl;
            if(s == 0){
               //write(cons[id].get_fd(), dummy, HEAD_LEN);
               //continue;
            }else{
            *(uint32_t *) HEAD_PRE_HASH(send_database[id]+h*WORK_OP_NUM*PACKAGE_LEN) = htonl(s);
            //continue;
            int ret = write(cons[id].get_fd(), send_database[id] + h*WORK_OP_NUM*PACKAGE_LEN, s);
            s = thread_offset[id+96][h];
            *(uint32_t *) HEAD_PRE_HASH(send_database[id+96]+h*WORK_OP_NUM*PACKAGE_LEN) = htonl(s);
            ret = write(cons[id+96].get_fd(), send_database[id+96] + h*WORK_OP_NUM*PACKAGE_LEN, s);
            s = thread_offset[id+192][h];
            *(uint32_t *) HEAD_PRE_HASH(send_database[id+192]+h*WORK_OP_NUM*PACKAGE_LEN) = htonl(s);
            ret = write(cons[id+192].get_fd(), send_database[id+192] + h*WORK_OP_NUM*PACKAGE_LEN, s); 
            s = thread_offset[id+288][h];
            *(uint32_t *) HEAD_PRE_HASH(send_database[id+288]+h*WORK_OP_NUM*PACKAGE_LEN) = htonl(s);
            ret = write(cons[id+288].get_fd(), send_database[id+288] + h*WORK_OP_NUM*PACKAGE_LEN, s); 
            if (ret != s) {
                //actually not error;we didn't take this condition into consideration before,so we let it crash here
                //once the program exit here, old test data need to be reconsidered.
                cout<<"error"<<endl;
                perror("write error");
                exit(-1);
            }
            if(ret > 0 ){
              g_totalbytes += ret;
              for(int j=0;j<384;j+=96){
              while(true){
                 s = thread_recv[id+j][h];
                 if(s == 0)
                     s = 16;
                 ret = read(cons[id+j].get_fd(), rec_buf + recv_offset, s - recv_offset);
                 if(ret>0){
                   recv_bytes[id] += ret;
                   recv_offset += ret;
                 }
                 if(ret == 0){
                    perror("write error");
                    exit(-1);
                    cout<<send_stop.load()<<" "<<send_start.load()<<endl;
                 }
                 if (recv_offset == s) {
                      thread_recv[id+j][h] = 0;
                      recv_offset = 0;
                      break;
                 }
                 std::this_thread::sleep_for( std::chrono::nanoseconds(5));
               
               }
             }
               //goto y;
            }
          }
          //goto y;
        }
        num = 0;
        data_offset =  2500000;//send_offset[id];
        offset = 0;
        if(id == 0)
            cout<<i<<endl;
    }
    y:
    timelist[tid] += t.getRunTime();
    //if(snum != scp -r -P 55555 pcl_liwh@172.168.204.78:/home/pcl_liwh/yangzheng/recv/faster/cc/build/double_change ./500 * myround)
    //     cout<<"fail "<<id<<endl;
    /*int id = tid - thread_num;
    uint64_t g_totalbytes = 0;
    int offset = 0;
    char rec_buf[500 * PACKAGE_LEN];
    while (send_stop.load() != port_num) {
        for (int i = 0; i < port_num; i++) {
            if (local_cons[i][id].flag.load() == 1) {
                while(true){
                int ret = write(cons[id].get_fd(), local_cons[i][id].get_buf()+offset, 500*PACKAGE_);
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
           }
         }
                //local_cons[i][id].flag.fetch_sub(1);
                //g_totalbytes += ret;
             int j = 0;
              while (true) {
                for(int i = 0;i<1;i++){
                int ret = read(recv_sfd[id*1+i], rec_buf + offset, 500 * PACKAGE_LEN - offset);
                if (ret == 0) {
                 break;
                 }
               if (ret == -1) {
                   if (errno == EWOULDBLOCK || errno == EAGAIN) {
                    //printf("read again\n");
                   } else {
                      perror("read error");
                      exit(-1);
                   }
               } else {
                  recv_bytes[id*1 +i] += ret;
                  offset += ret;
                  if (offset == 500 * PACKAGE_LEN) {
                           offset = 0;
                           j++;
                           break;   
                  }
                }
              }
              if(j==1)
                 break;
           }
           cout<<"success"<<endl;
        }
      }
        std::this_thread::sleep_for( std::chrono::nanoseconds(5));
    }*/
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
    close(cons[id+96].get_fd());
    close(cons[id+192].get_fd());
    close(cons[id+288].get_fd());
    //if(id == 3)
    //    cout<<send_bytes[id]<<endl;
    //cout<<"end"<<endl;
    //cout << g_totalbytes << endl;
}

void data_recv(int tid) {
    int id = tid - thread_num * 1;
    char rec_buf[SEND_BATCH * PACKAGE_LEN];
    int offset = 0;
    int qw = 0;
    recv_bytes[id] = 0;
    while (true) {
        //int ret = read(cons[id/*+thread_num*/].get_fd()+2, rec_buf + offset, SEND_BATCH * PACKAGE_LEN - offset);
        int ret = read(recv_sfd[id], rec_buf + offset, SEND_BATCH * PACKAGE_LEN - offset);
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
            if (offset == SEND_BATCH * PACKAGE_LEN) {
                recv_flag[id].fetch_add(1);
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
