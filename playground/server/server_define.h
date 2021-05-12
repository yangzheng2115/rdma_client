#ifndef MULTIPORT_NETWORK_DEFINE_H
#define MULTIPORT_NETWORK_DEFINE_H


#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event_struct.h>
#include <event2/event_compat.h>
#include <vector>

#include <queue>
#include "settings.h"
#include "kvobj.h"
#include <iostream>
#include <stdio.h>
#include <atomic>
#include <thread>

#include <semaphore.h>

#include <mutex>
#include <condition_variable>
using namespace std;
typedef struct {
    uint8_t magic;
    uint8_t opcode;
    uint16_t keylen;
    uint16_t batchnum;
    uint8_t prehash;
    uint8_t reserved;
    uint16_t totalbodylen;
    uint16_t  threadnum;
    uint32_t  time;
} protocol_binary_request_header;

typedef enum {
    PROTOCOL_BINARY_CMD_GET = 0x01,
    PROTOCOL_BINARY_CMD_GETBATCH_HEAD = 0x03,
    PROTOCOL_BINARY_CMD_SET = 0x04
} protocol_binary_command;

enum conn_queue_item_modes {
    queue_new_conn,   /* brand new connection. */
    queue_redispatch, /* redispatching from side thread */ /*not used*/
};

enum conn_states {
    conn_parse_cmd,

    conn_count,


    conn_deal_with,

    conn_new_cmd,    /**< Prepare connection for next command */
    conn_waiting,    /**< waiting for a readable socket */
    conn_read,       /**< reading in a command line */
    //   conn_parse_cmd,  /**< try to parse a command from the input buffer */
            conn_write,      /**< writing out a simple response */
    conn_nread,      /**< reading in a fixed number of bytes */
    conn_swallow,    /**< swallowing unnecessary bytes w/o storing */
    conn_closing,    /**< closing this connection */
    conn_mwrite,     /**< writing out many items sequentially */
    conn_closed,     /**< connection is closed */
    conn_watch,      /**< held by the logger thread as a watcher */
    conn_max_state   /**< Max state value (used for assertion) */
};

enum work_states {
    parse_head,
    read_key_value,
    store_kvobj,
    query_key,
    write_bcak,
};


int *portList;


typedef struct CONNECTION CONNECTION;
typedef struct CONNITEM CONN_ITEM;
typedef struct THREADINFO THREAD_INFO;

typedef struct {
    char *buf;
    uint64_t datalen;
    uint64_t offset;
} batchbuf;

class Semaphore {
public:
    Semaphore(int count_ = 0)
            : count(count_) {}

    inline void notify() {
        std::unique_lock<std::mutex> lock(mtx);
        count++;
        cv.notify_one();
    }

    inline void wait() {
        std::unique_lock<std::mutex> lock(mtx);

        while (count == 0) {
            cv.wait(lock);
        }
        count--;
    }

private:
    std::mutex mtx;
    std::condition_variable cv;
    int count;
};

class atomiccirculequeue {
    constexpr static size_t limit = 2;//(batch_size * 2);
    atomic<uint64_t> head{0}, tail{0}, flag{0};
    //Semaphore active;
    //atomic<bool> full{false};
    int count=0;
    uint64_t write_offset = 0;
    uint64_t read_offset = 0;
    uint64_t w = 0;
    uint64_t r = 0;
    char queue[(limit + 1) * INIT_READ_BUF_SIZE];
    char copy[INIT_READ_BUF_SIZE];
    char *re;
    int size[limit];
    int he, ta;
public:
    inline int push(int sfd) {
        /*if (thread_id == 2)
            std::cout << (head + limit - tail) << ":" << (head + limit - tail) % limit << ":" << head << ":" << tail
                      << ":" << limit << std::endl;*/
        //for(int i=0;i<10;i++)
        //    std::this_thread::yield();
        start:
        uint64_t h, t;
        uint64_t a, b;
        h = head.load();
        t = tail.load();
        //while ((h + limit - t) % limit == 1) {
        if((h + limit - t) % limit == 1){
            //std::this_thread::yield();
           // h = head.load();
            return -1;
        }
        int res;
        int qw = 0;
	int offset =0 ;
        /*res = read(sfd, copy,INIT_READ_BUF_SIZE);
        if(res == 0){
            flag.fetch_add(1);
            return 0;
        }
        else if(res <0)
            return -1;
        else
            return res;*/       
        //if(write_offset == 17000)
        //   cout<<"a"<<endl;
	while(true) {
            res = read(sfd, queue + write_offset % (limit * INIT_READ_BUF_SIZE), INIT_READ_BUF_SIZE - offset);
            if (res == 0) {
                if (write_offset % INIT_READ_BUF_SIZE != 0) {
                    a = write_offset / INIT_READ_BUF_SIZE;
                    size[a] = write_offset % INIT_READ_BUF_SIZE;
                    size[(a + 1) % limit] = 0;
                    if (t < limit - 1)
                        tail.fetch_add(1);
                    else
                        tail.fetch_sub(limit - 1);
                } else {
                    a = write_offset / INIT_READ_BUF_SIZE;
                    size[a] = 0;
                }
                flag.fetch_add(1);
                return 0;
            } else if (res > 0) {
                a = write_offset / INIT_READ_BUF_SIZE;
                b = (write_offset + res) / INIT_READ_BUF_SIZE;
                write_offset = write_offset + res;
                w = w + res;
                offset += res;
                write_offset = write_offset % (limit * INIT_READ_BUF_SIZE);
                if ((b - a) == 1) {
                    size[a] = INIT_READ_BUF_SIZE;
                    if (t < limit - 1)
                        tail.fetch_add(1);
                    else
                        tail.fetch_sub(limit - 1);
                    break;
                }
            } else {
                std::this_thread::sleep_for( std::chrono::nanoseconds(5));        
            }
        }
        return res;
        /*if (write_offset / INIT_READ_BUF_SIZE == (limit - 1)) {
            res = read(sfd, queue + write_offset % (limit * INIT_READ_BUF_SIZE),
                       INIT_READ_BUF_SIZE - write_offset % (INIT_READ_BUF_SIZE));
        } else {
            res = read(sfd, queue + write_offset % (limit * INIT_READ_BUF_SIZE), INIT_READ_BUF_SIZE);
        }
        if (res == 0) {
            if (write_offset % INIT_READ_BUF_SIZE != 0) {
                a = write_offset / INIT_READ_BUF_SIZE;
                size[a] = write_offset % INIT_READ_BUF_SIZE;
                size[(a + 1) % limit] = 0;
                if (t < limit - 1)
                    tail.fetch_add(1);
                else
                    //tail.store(0);
                    tail.fetch_sub(limit - 1);
            } else {
                a = write_offset / INIT_READ_BUF_SIZE;
                size[a] = 0;
            }
            flag.fetch_add(1);
            return 0;
        } else if (res > 0) {
            a = write_offset / INIT_READ_BUF_SIZE;
            b = (write_offset + res) / INIT_READ_BUF_SIZE;
            if ((b - a) == 1) {
                size[a] = INIT_READ_BUF_SIZE;
                if (t < limit - 1)
                    tail.fetch_add(1);
                else
                    //tail.store(0);
                    tail.fetch_sub(limit - 1);
                //qw = 1;
            }
        } else {
            return -2;
            goto start;
        }
        write_offset = write_offset + res;
        w = w + res;
        h=head.load();
        //cout<<w<<" "<<h<<" "<<t<<endl;
        write_offset = write_offset % (limit * INIT_READ_BUF_SIZE);
        //tail.store((t+1)%limit);
        //tail = (tail + 1) % limit;
        //if(qw != 0) 
        //     return 1;
        return res;
	*/
    }

    inline int pread(char **buf, int worker, int &f) {
        /*if (thread_id == 2)
            std::cout << (head + limit - tail) << "-" << (head + limit - tail) % limit << "-" << head << "-" << tail
                      << "-" << limit << std::endl;*/
        read_start:
        uint64_t h, t;
        uint64_t a, b;
        int avail;
        h = head.load();
        /*if(flag.load() == 0){
           // std::this_thread::yield();
           return -1;
        }
        else{
           return 0;
        }*/
        /*t = tail.load();
        while (h == t) {
            std::this_thread::yield();
            t = tail.load();
        }*/
        ta = t;
        he = h;
        a = read_offset / INIT_READ_BUF_SIZE;
        b = (read_offset + worker) / INIT_READ_BUF_SIZE;
	read_offset = read_offset + worker;
        r = r + worker;
        read_offset = read_offset % (limit * INIT_READ_BUF_SIZE);
        if ((b - a) == 1) {
            //size[a]=0;
            if (h < limit - 1)
                head.fetch_add(1);
            else
                //head.store(0);
                head.fetch_sub(limit - 1);
            if(f == 1)
                count++;
        }
        if(count==5) {
            f = 10;
            count=0;
        }
        h = head.load();
        t = tail.load();
        while (h == t) {
            if (flag.load() == 1)
                break;
            std::this_thread::yield();
            t = tail.load();
            return -1;
        }
        //read_offset = read_offset + worker;
        //r = r + worker;
        //read_offset = read_offset % (limit * INIT_READ_BUF_SIZE);
        re = queue + read_offset % (limit * INIT_READ_BUF_SIZE);
        *buf = queue + read_offset % (limit * INIT_READ_BUF_SIZE);
        a = read_offset / INIT_READ_BUF_SIZE;
        b = (read_offset + INIT_READ_BUF_SIZE - 1) % (limit * INIT_READ_BUF_SIZE);
        b = b / INIT_READ_BUF_SIZE;
        if (a == b) {
            avail = size[a] - read_offset % INIT_READ_BUF_SIZE;
            //memcpy(copy, queue + read_offset % (limit * INIT_READ_BUF_SIZE), avail);
            //*buf = copy;
            return avail;
        }
        while ((h + 1) % limit == t) {
            std::this_thread::yield();
            t = tail.load();
            if (flag.load() == 1)
                break;
            return -1;
        }
        if (a == limit - 1) {
            avail = size[a] - read_offset % INIT_READ_BUF_SIZE;
            if (avail > 40) {
                //memcpy(copy, queue + read_offset % (limit * INIT_READ_BUF_SIZE), avail);
                //*buf = copy;
                return avail;
            }
            if (avail == 0)
                return 0;
            if (size[0] == 0){
                //memcpy(copy, queue + read_offset % (limit * INIT_READ_BUF_SIZE), avail);
                //*buf = copy;
                return avail;
            }
            int offset = (size[0] > 60) ? 60 : size[0];
            memcpy(queue + limit * INIT_READ_BUF_SIZE, queue, offset);
            //memcpy(copy, queue + read_offset % (limit * INIT_READ_BUF_SIZE), avail);
            //memcpy(copy + avail, queue, offset);
            avail += offset;
            //*buf = copy;
            //test =1;
            return avail;
        }
        if (size[b] == 0) {
            avail = size[a] - read_offset % INIT_READ_BUF_SIZE;
        } else if (size[b] > (read_offset + INIT_READ_BUF_SIZE - 1) % INIT_READ_BUF_SIZE) {
            avail = INIT_READ_BUF_SIZE;
        } else {
            avail = size[b] + size[a] - read_offset % INIT_READ_BUF_SIZE;
        }
        //memcpy(copy, queue + read_offset % (limit * INIT_READ_BUF_SIZE), avail);
        //*buf = copy;
        return avail;
    }

    void init() {
        for (int i = 0; i < limit; i++)
            size[i] = 0;
        //  queue[i]=new uint8_t[INIT_READ_BUF_SIZE];
    }

    void clean(){
        if(flag.load()==1)
            return;
        uint64_t h, t;
        uint64_t a, b;
        h = head.load();
        t = tail.load();
        //while ((h + limit - t) % limit == 1) {
        while((h + limit - t) % limit == 1){
            std::this_thread::yield();
            h = head.load();
        }
        if (write_offset % INIT_READ_BUF_SIZE != 0) {
            a = write_offset / INIT_READ_BUF_SIZE;
            size[a] = write_offset % INIT_READ_BUF_SIZE;
            size[(a + 1) % limit] = 0;
            if (t < limit - 1)
                tail.fetch_add(1);
            else
                tail.fetch_sub(limit - 1);
        } else {
            a = write_offset / INIT_READ_BUF_SIZE;
            size[a] = 0;
        }
        flag.fetch_add(1);
    }
};

struct Item {
    uint32_t time;
    //int index;
    int offset;
    int count;
    int sfd;
    bool operator <(const Item& rhs) const // 升序排序函数
    {
        return time< rhs.time;
    }
};

struct CONNECTION {
    int sfd;
    int recv_sfd;
    int num;
    int thread_index;

    int read_buf_size;          //read buf size
    char *read_buf;               //read buf
    int recv_bytes;
    int total_bytes;
    char *working_buf;             //deal offset
    int worked_bytes;
    int remaining_bytes;      //bytes not deal with

    int ret_buf_size;
    char *ret_buf;
    int ret_buf_offset;
    int ret_bytes;

    std::vector<batchbuf> batch_ret_vector;

    unsigned long bytes_processed_in_this_connection;
    unsigned long bytes_wrote_back_in_this_connection;


    struct event event;
    struct event pro_event;

    conn_states conn_state;
    work_states work_state;


    protocol_binary_request_header binary_header;

    short cmd;

    std::string key;
    std::string value;

    bool query_hit;

    THREAD_INFO *thread;

    kvobj *kv;

    atomiccirculequeue acq;
    atomiccirculequeue aq[15];
    Semaphore active;
    atomic<bool> full{false};

    vector<Item> time_vec;
    uint16_t batch_num;
    uint16_t batch_count;
    bool in_batch_get;
};

struct CONNITEM {
    int sfd;    //socket handler
    enum conn_queue_item_modes mode;
    THREAD_INFO *thread;
};

struct THREADINFO {
    int thread_index;
    //thread id
    pthread_t thread_id;        /* unique ID of this thread */
    //thread event base
    struct event_base *base;    /* libevent handle this thread uses */
    //Asynchronous event event
    struct event notify_event;  /* listen event for notify pipe */
    //pipe receive
    int notify_receive_fd;      /* receiving end of notify pipe */
    //pipe send
    int notify_send_fd;         /* sending end of notify pipe */

    std::queue<CONN_ITEM *> *connQueueList; /* queue of new connections to handle */

    pthread_mutex_t conqlock;
    // struct queue_class * bufqueue;
};

enum try_read_result {
    READ_DATA_RECEIVED,
    READ_NO_DATA_RECEIVED,
    READ_ERROR,            /* an error occurred (on the socket) (or client closed connection) */
    READ_MEMORY_ERROR      /* failed to allocate more memory */
};

//std::queue<CONN_ITEM>  * connQueueList;
//
THREAD_INFO *threadInfoList;

CONNECTION **conns;


typedef enum {
    PROTOCOL_BINARY_REQ = 0x80,
    PROTOCOL_BINARY_RES = 0x81
} protocol_binary_magic;

#endif //MULTIPORT_NETWORK_DEFINE_H
