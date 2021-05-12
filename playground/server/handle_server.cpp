#include <iostream>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/unistd.h>
#include <netinet/in.h>
#include <pthread.h>
#include <assert.h>
#include <fcntl.h>
#include <signal.h>


#include "server_define.h"
#include "assoc.h"
#include <iostream>
#include "../sum_store-dir/tracer.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <experimental/filesystem>
#include "../../src/core/faster.h"
#include "../../src/core/address.h"
#include "../../src/device/file_system_disk.h"
#include "../../src/device/null_disk.h"
#include <numa.h>
#include "../../src/device/serializablecontext.h"

#define MAX_SOCKET 4
#define MAX_CORES 128
#define MAX_CORES_PER_SOCKET (MAX_CORES / MAX_SOCKET)
#define DEFAULT_THREAD_NUM (4)
#define DEFAULT_KEYS_COUNT (1 << 20)
#define DEFAULT_KEYS_RANGE (1 << 2)

#define DEFAULT_STR_LENGTH 256
#define DEFAULT_KEY_LENGTH 8
#define MAX_SOCKET 8
#define MAX_CORES 512
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

using namespace std;

size_t init_size = next_power_of_two(DEFAULT_STORE_BASE / 2);

store_t *store;

int root_capacity = 100000000;

int thread_number = DEFAULT_THREAD_NUM;

int hlog_number = 4;

int server_num = 1;

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

atomic<uint64_t > w{0},m{0};
atomic<uint64_t > cl[384];
store_t *store1[MAX_SOCKET];

int mapping = 0xff;

int numaScheme = 0x7f;     // 0th, 1st, 2nd numa-based initialization, insert, measure will be numa-based

struct target {
    int tid;
    int core;
    int socket;
    store_t *fmap;
};

pthread_t *workers;

struct target *parms;

atomiccirculequeue *acq[200];

void conn_state_jump(conn_states &s_state, conn_states t_state) {
    s_state = t_state;
}

void work_state_jump(work_states &s_state, work_states t_state) {
    s_state = t_state;
}

void prepare() {
    cout << "prepare " << thread_number << endl;
    workers = new pthread_t[thread_number];
    parms = new struct target[thread_number];
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

static void con_ret_buf(CONNECTION *c) {
    //simply assume get write back
    // char * buf = c->ret_buf + c->ret_buf_offset;
    int headlen = sizeof(protocol_binary_request_header);
    int keylen = c->key.length();
    uint64_t valuelen = c->query_hit ? c->value.size() : 8;

    char *buf;
    if (c->in_batch_get) {
        //valuelen should be calculated in advance based on query result
        //may cause bug here c->value.size()
        if (c->batch_ret_vector.back().datalen + headlen + keylen + c->value.size() > BATCH_BUF_SIZE) {
            //need new buf
            batchbuf tmp;
            tmp.buf = (char *) malloc(BATCH_BUF_SIZE);
            tmp.offset = 0;
            tmp.datalen = 0;
            c->batch_ret_vector.push_back(tmp);
        }
        buf = c->batch_ret_vector.back().buf + c->batch_ret_vector.back().datalen;
    } else {
        buf = c->ret_buf + c->ret_buf_offset;
    }

    protocol_binary_request_header resp_head;

    resp_head.magic = 0x81;
    resp_head.opcode = 0x01;
    resp_head.keylen = htons(c->key.length());
    resp_head.batchnum = c->in_batch_get ? c->batch_count++ : htons(1);
    resp_head.prehash = 0;
    resp_head.reserved = 0;
    resp_head.totalbodylen = htonl(c->key.length() + c->value.length());

    *(protocol_binary_request_header *) buf = resp_head;

    memcpy(buf + headlen, c->key.c_str(), keylen);

    char notfound_ans[9] = "NOTFOUND";

    //if query hit , return key-value else return key-NOTFOUND
    if (c->query_hit) {
        memcpy(buf + headlen + keylen, c->value.c_str(), valuelen);
    } else {
        memcpy(buf + headlen + keylen, notfound_ans, 8);
        c->ret_bytes += headlen + keylen + 8;
    }

    if (c->in_batch_get) {
        c->batch_ret_vector.back().datalen += headlen + keylen + valuelen;
    } else {
        c->ret_bytes += headlen + keylen + valuelen;
    }

    work_state_jump(c->work_state, write_bcak);
}

static void send_batch(CONNECTION *c) {
    c->bytes_processed_in_this_connection += sizeof(c->binary_header) + c->binary_header.keylen;
    if (c->batch_count < c->batch_num) {
        //return package has been writen to the cache;just ready to parse new cmd
        work_state_jump(c->work_state, parse_head);
        conn_state_jump(c->conn_state, conn_new_cmd);
        return;
    } else if (c->batch_count == c->batch_num) {
        //write back
        auto it = c->batch_ret_vector.begin();

        int ret = write(c->sfd, it->buf + it->offset, it->datalen - it->offset);
        if (ret <= 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                conn_state_jump(c->conn_state, conn_waiting);
            } else if (errno == SIGPIPE) {
                perror("client close");
                conn_state_jump(c->conn_state, conn_closing);
            } else {
                perror("write error");
                conn_state_jump(c->conn_state, conn_closing);
            }
        } else {
            if (ret == it->datalen - it->offset) {
                //finish write  free buf
                free(it->buf);
                c->batch_ret_vector.erase(it);

                if (c->batch_ret_vector.size() == 0) {
                    //finish batch send
                    c->in_batch_get = false;
                    c->batch_num = 0;
                    c->batch_count = 0;
                    work_state_jump(c->work_state, parse_head);
                    conn_state_jump(c->conn_state, conn_new_cmd);
                } else {
                    //still have buf ,ready to write again
                    conn_state_jump(c->conn_state, conn_waiting);
                }
            } else if (ret < it->datalen - it->offset) {
                //only part of data was wrote back, continue to write
                it->offset += ret;
                conn_state_jump(c->conn_state, conn_waiting);
            }
            c->bytes_wrote_back_in_this_connection += ret;
        }

    }
}

static int try_read_key_value(CONNECTION *c) {
    switch (c->cmd) {

        case PROTOCOL_BINARY_CMD_SET : {
            uint32_t kvbytes = c->binary_header.totalbodylen;
            if (c->remaining_bytes < kvbytes)
                return 0;

            c->key = std::string(c->working_buf, c->binary_header.keylen);
            c->value = std::string(c->working_buf + c->binary_header.keylen, kvbytes - c->binary_header.keylen);
            //assert(c->value == "[ field0");
            c->remaining_bytes -= kvbytes;
            c->working_buf += kvbytes;
            c->worked_bytes += kvbytes;

            work_state_jump(c->work_state, store_kvobj);
            break;
        }
        case PROTOCOL_BINARY_CMD_GET : {
            int keybytes = c->binary_header.keylen;
            if (c->remaining_bytes < keybytes)
                return 0;

            c->key = std::string(c->working_buf, keybytes);

            c->remaining_bytes -= keybytes;
            c->working_buf += keybytes;
            c->worked_bytes += keybytes;

            work_state_jump(c->work_state, query_key);

            break;
        }
        default:
            perror("binary instruction error");
            return -1;
    }
    return 1;
}


static void dispatch_bin_command(CONNECTION *c) {


    uint16_t keylen = c->binary_header.keylen;
    uint32_t bodylen = c->binary_header.totalbodylen;

    if (keylen > bodylen) {
        perror("PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND");
        conn_state_jump(c->conn_state, conn_closing);
        return;
    }

    switch (c->cmd) {

        case PROTOCOL_BINARY_CMD_SET: /* FALLTHROUGH */
            if (keylen != 0 && bodylen >= (keylen + 8)) {
                work_state_jump(c->work_state, read_key_value);
            } else {
                perror("PROTOCOL_BINARY_CMD_SET_ERROR");
                return;
            }
            break;
        case PROTOCOL_BINARY_CMD_GETBATCH_HEAD:
            c->batch_num = c->binary_header.batchnum;
            c->batch_count = 0;
            c->in_batch_get = true;
            batchbuf bf;
            bf.buf = (char *) malloc(BATCH_BUF_SIZE);
            bf.offset = 0;
            bf.datalen = 0;
            c->batch_ret_vector.push_back(bf);
            work_state_jump(c->work_state, parse_head);
            break;
        case PROTOCOL_BINARY_CMD_GET:
            if (keylen != 0 && bodylen == keylen) {
                work_state_jump(c->work_state, read_key_value);
            } else {
                perror("PROTOCOL_BINARY_CMD_SET_ERROR");
                return;
            }
            break;
        default:
            perror("PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND");
    }
}


//try parse binary header
static int try_read_head_binary(CONNECTION *c) {
    /* Do we have the complete packet header? */
    if (c->remaining_bytes < sizeof(c->binary_header)) {
        /* need more data! */
        return 0;
    }

    protocol_binary_request_header *req;
    req = (protocol_binary_request_header *) c->working_buf;

    c->binary_header.magic = req->magic;
    c->binary_header.opcode = req->opcode;
    c->binary_header.keylen = ntohs(req->keylen);
    c->binary_header.batchnum = ntohs(req->batchnum);
    c->binary_header.prehash = (req->prehash);
    c->binary_header.totalbodylen = ntohl(req->totalbodylen);


    c->cmd = c->binary_header.opcode;

    if (c->in_batch_get && c->batch_count != c->binary_header.batchnum) {
        perror("batchnum error");
        exit(-1);
    }

    //set next working state in this function
    dispatch_bin_command(c);

    c->remaining_bytes -= sizeof(c->binary_header);
    c->worked_bytes += sizeof(c->binary_header);
    c->working_buf += sizeof(c->binary_header);//working buf move

    return 1;
}


void init_portlist() {
    portList = (int *) (calloc(PORT_NUM, sizeof(int)));
    for (int i = 0; i < PORT_NUM; i++) portList[i] = PORT_BASE + i;
}

void init_conns() {
    int max_fds = MAX_CONN;
    if ((conns = (CONNECTION **) calloc(max_fds, sizeof(CONNECTION *))) == NULL) {
        fprintf(stderr, "Failed to allocate connection structures\n");
        /* This is unrecoverable so bail out early. */
        exit(1);
    }
}

void *worker(void *args) {
    int tid = *(int *) args;
    int id =tid % THREAD_NUM;
    if(id>THREAD_NUM)
        pin_to_core(parms[id-THREAD_NUM + 48 * 3].core);
    else
        pin_to_core(parms[id + 48 * 3].core);
    //if ((numaScheme & 0x2) != 0) pin_to_core(parms[tid].core);
    //if ((numaScheme & 0x2) != 0) pin_to_core(parms[tid%16+24*(tid/16)].core);
//    printf("worker %d start \n", tid);

    THREAD_INFO *me = &threadInfoList[tid];

    event_base_loop(me->base, 0);

    event_base_free(me->base);
    return NULL;

}

void reset_cmd_handler(CONNECTION *c) {

    if (c->remaining_bytes > 0) {
        conn_state_jump(c->conn_state, conn_deal_with);
    } else {
        conn_state_jump(c->conn_state, conn_waiting);
    }
}

enum try_read_result try_read_network(CONNECTION *c) {
    enum try_read_result gotdata = READ_NO_DATA_RECEIVED;
    int res;
    int num_allocs = 0;
    assert(c != NULL);

    /*
    if (c->working_buf != c->read_buf) {
        if (c->remaining_bytes != 0) {
            memmove(c->read_buf, c->working_buf, c->remaining_bytes);
            //           printf("remaining %d bytes move ; ",c->remaining_bytes);
        } // otherwise there's nothing to copy
        c->working_buf = c->read_buf;
    }
    */

    int avail = c->read_buf_size - c->remaining_bytes;
    CONNECTION *b = conns[c->sfd];
    int i = c->thread_index-b->thread_index;
    int flag = 0;
    //if(c->full.load())
    //    flag = 1;
    //res = c->acq.pread(&c->read_buf, c->worked_bytes,flag);
    res = b->aq[i].pread(&c->read_buf, c->worked_bytes,flag);
    if(flag == 10){
        //c->full.store(false);
        //c->active.notify();
    }

    //res = c->acq.pread(&c->read_buf, c->worked_bytes);
    //m.fetch_add(1);
    c->working_buf = c->read_buf;
    //res = read(c->sfd, c->read_buf + c->remaining_bytes, avail);

//    printf("receive %d\n",res);

    if (res > 0) {
        //w.fetch_add(1);
        gotdata = READ_DATA_RECEIVED;
        c->recv_bytes = res;
        c->total_bytes = c->remaining_bytes + c->recv_bytes;
        c->remaining_bytes = res;
        c->worked_bytes = 0;
    }
    if (res == 0) {
        return READ_ERROR;
    }

    if (res == -1) {
        c->worked_bytes = 0;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return gotdata;
        }
    }
    return gotdata;
}

void conn_close(CONNECTION *c) {
    assert(c != NULL);

    event_del(&c->event);

    cl[c->thread_index/server_num].fetch_add(1);

    if(cl[c->thread_index/server_num].load()==server_num){
       close(c->sfd);
       cout<<"close"<<endl;
       cl[c->thread_index/server_num].store(0);
    }
    return;
}


void process_func(CONNECTION *c) {
    bool stop = false;

    int inst_count = 0;

    while (!stop) {

        switch (c->conn_state) {

            case conn_read: {
                //        printf("[%d:%d] conn_read : ",c->thread_index, c->sfd);
                try_read_result res = try_read_network(c);
                switch (res) {
                    case READ_NO_DATA_RECEIVED:
                        conn_state_jump(c->conn_state, conn_waiting);
                        break;
                    case READ_DATA_RECEIVED:
                        conn_state_jump(c->conn_state, conn_deal_with);
                        break;
                    case READ_ERROR:
                        conn_state_jump(c->conn_state, conn_closing);
                        break;
                    case READ_MEMORY_ERROR: /* Failed to allocate more memory */
                        /* State already set by try_read_network */
                        perror("mem error");
                        exit(1);
                }
                break;
            }

            case conn_new_cmd : {
                //printf("conn_new_cmd :");
                if (++inst_count < ROUND_NUM) {
                    reset_cmd_handler(c);
                    //printf("reset and continue\n");
                } else {
                    stop = true;
                    //                printf("[%d:%d] stop and restart\n", c->thread_index,c->sfd);
                }
                break;
            }


            case conn_deal_with : {

                switch (c->work_state) {

                    case parse_head : {
                        //assert(c->in_batch_get == false);
                        //check threre is at least one byte to parse
                        if (c->remaining_bytes < 1) {
                            conn_state_jump(c->conn_state, conn_waiting);
                            break;
                        }

                        if ((unsigned char) c->working_buf[0] == (unsigned char) PROTOCOL_BINARY_REQ) {
                            //binary_protocol
                            if (!try_read_head_binary(c)) {
                                conn_state_jump(c->conn_state, conn_waiting);
                            }
                        } else {
                            perror("error magic\n");
                            exit(-1);
                        }
                        break;
                    }

                    case read_key_value : {
                        //work state change in this function
                        int re = try_read_key_value(c);
                        if (re == 0) {
                            //wait for more data
                            conn_state_jump(c->conn_state, conn_waiting);
                        } else if (re == -1) {
                            //error, close connection
                            conn_state_jump(c->conn_state, conn_closing);
                        }
                        break;
                    }
                    case store_kvobj : {
                        //std::cout << "store " << c->key << ":" << c->value << endl;
                        /*auto callback = [](IAsyncContext *ctxt, Status result) {
                            CallbackContext<UpsertContext> context{ctxt};
                        };
                        UpsertContext context{
                                Key((uint8_t *) c->key.c_str(), 16),
                                Value((uint8_t *) c->value.c_str(),
                                      8)};
                        Status stat = store->UpsertT(context, callback, 1, 1);
                        */                
                        //if(stat == Status::Ok)
                        //    cout<<"ok"<<endl;
                        //hashTable.insert_or_assign(c->key,c->value);
                        //assert(c->value == "[ field0");
        
                        c->bytes_processed_in_this_connection +=
                                sizeof(c->binary_header) + c->binary_header.totalbodylen;
                        work_state_jump(c->work_state, parse_head);
                        conn_state_jump(c->conn_state, conn_new_cmd);
                        break;
                    }
                    case query_key : {
                        //std::cout << "query:" << c->key;
                        auto callback = [](IAsyncContext *ctxt, Status result) {
                            CallbackContext<ReadContext> context{ctxt};
                        };
                        ReadContext context{
                                Key((uint8_t *) c->key.c_str(), 16)};
                        Status stat = store->Read(context, callback, 1);
                        if (stat == Status::Ok) {
                            //  std::cout << ":find" << c->value << endl;
                            c->query_hit = true;
                        } else {
                            //std::cout << "not found" << endl;
                            c->query_hit = false;
                        }
                        //work_state change in this function
                        con_ret_buf(c);
                        break;
                    }
                    case write_bcak : {
                        if (c->in_batch_get) {
                            send_batch(c);
                            break;
                        }
                        int ret = write(c->sfd, c->ret_buf + c->ret_buf_offset, c->ret_bytes);
                        if (ret <= 0) {
                            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                                conn_state_jump(c->conn_state, conn_waiting);
                            } else {
                                perror("write error");
                                conn_state_jump(c->conn_state, conn_closing);
                            }
                        } else {
                            if (ret == c->ret_bytes) {
                                //finish write ,ready to parse new instruction
                                c->ret_buf_offset = 0;
                                c->ret_bytes = 0;
                                work_state_jump(c->work_state, parse_head);
                                conn_state_jump(c->conn_state, conn_new_cmd);
                            } else if (ret < c->ret_bytes) {
                                //only part of data was wrote back, continue to write
                                c->ret_buf_offset += ret;
                                c->ret_bytes -= ret;
                                conn_state_jump(c->conn_state, conn_waiting);
                            }
                            c->bytes_wrote_back_in_this_connection += ret;
                        }
                        break;
                    }
                }
                break;
            }


            case conn_waiting : {
                //          printf("[%d:%d] conn_waiting\n",c->thread_index,c->sfd);
                if (c->work_state == write_bcak) {
                    conn_state_jump(c->conn_state, conn_deal_with);
                } else if (c->work_state == parse_head
                           || c->work_state == read_key_value) {
                    conn_state_jump(c->conn_state, conn_read);
                } else {
                    perror("work state error ");
                    conn_state_jump(c->conn_state, conn_closing);
                }
                stop = true;
                break;
            }

            case conn_closing : {
                printf("[%d:%d] conn_closing ,processed bytes: %lu ,wrote bytes: %lu\n",
                       c->thread_index,
                       c->sfd,
                       c->bytes_processed_in_this_connection,
                       c->bytes_wrote_back_in_this_connection);
                conn_close(c);
                //pthread_join(workers[c->sfd], nullptr);
                stop = true;
                break;
            }

        }
    }
}

void *read_handler(void *arg) {
    CONNECTION *c = (CONNECTION *) arg;
    //if ((numaScheme & 0x2) != 0) pin_to_core(parms[c->thread_index].core);
    while (true) {
        if (!c->acq.push(c->sfd))
            break;
    }
}

void event_handler(const int fd, const short which, void *arg) {


    CONNECTION *c = (CONNECTION *) arg;
//    printf("[%d:%d] starting working,worked bytes:%d \n",c->thread_index, c->sfd, c->bytes_processed_in_this_connection);

    assert(c != NULL);

    /* sanity */
    if (fd != c->sfd) {
        fprintf(stderr, "Catastrophic: event fd doesn't match conn fd!\n");
        return;
    }

    process_func(c);

    /* wait for next event */
    return;
}

void event_handler_pro(const int fd, const short which, void *arg) {


    CONNECTION *c = (CONNECTION *) arg;
    /*CONNECTION *b;
    int index = c->thread_index;
    index = index % 48 ;
    if(index > 23)
        b = conns[fd - 24];
    else
        b = conns[fd + 24];*/
//    printf("[%d:%d] starting working,worked bytes:%d \n",c->thread_index, c->sfd, c->bytes_processed_in_this_connection);

    assert(c != NULL);

    if (fd != c->sfd) {
        fprintf(stderr, "Catastrophic: event fd doesn't match conn fd!\n");
        return;
    }

    int t;
    int b = 0;
    //while (true) {
	//b = 0;
	for(int i =0;i<server_num;i++){
            t = c->aq[i].push(c->sfd);
	    //t = c->acq.push(c->sfd);
	    if(t == 0)
		break;    
            b+=t;
            //if(t==-1)
            //std::this_thread::sleep_for( std::chrono::nanoseconds(5));
        }
	if(b == -1)
	   std::this_thread::sleep_for( std::chrono::nanoseconds(5));
	if(t==0){
	    for(int i =0;i<server_num;i++){
                c->aq[i].clean();
	    }
	    cout<<"end"<<endl;
            //break;
	}
	//b = 0;
        /*t = c->acq.push(c->sfd);
	//if (b != NULL) {
            t = b->acq.push(b->sfd);
        }
        if (t >0&&t<100){
            //w.fetch_add(1);
            //std::this_thread::sleep_for( std::chrono::nanoseconds(5));
        }
        //w.fetch_add(1);
        //if(t>0)
           //m.fetch_add(1);
         if(t==-1){
             break;
             std::this_thread::sleep_for( std::chrono::nanoseconds(5));
             //c->full.store(true);
             //c->active.wait();
             //c->full.store(false);
        }
        if(t==-2){
            break;
            std::this_thread::sleep_for( std::chrono::nanoseconds(5));
        }
        if(t==0)
            break;
        */
    //}
    //if(t<0||t==0)
    //w.fetch_add(1);
    if (t == 0) {
        if(c->thread_index == 0){
            cout<<w.load()<<" "<<m.load()<<endl;
        }
        event_del(&c->pro_event);
        //conns[fd] = nullptr;
    }
    //std::this_thread::sleep_for( std::chrono::nanoseconds(5));
    /* wait for next event */
    return;
}

void conn_new(int sfd, struct event_base *base, int thread_index) {
    CONNECTION *c;
    if (thread_index < THREAD_NUM) {
        assert(sfd >= 0 && sfd < MAX_CONN);
        //cout << "connection" << sfd << endl;
        //c = conns[sfd];

        if (c == NULL) {
            if (!(c = new(std::nothrow) CONNECTION)) {
                fprintf(stderr, "Failed to allocate connection object\n");
                return;
            }
            c->read_buf_size = INIT_READ_BUF_SIZE;
            //c->read_buf = (char *) malloc(c->read_buf_size);

            c->ret_buf_size = INIT_RET_BUF_SIZE;
            c->ret_buf = (char *) malloc(c->ret_buf_size);

        }

        c->sfd = sfd;
        c->worked_bytes = 0;
        c->working_buf = c->read_buf;
        c->recv_bytes = 0;
        c->remaining_bytes = 0;
        c->bytes_processed_in_this_connection = 0;
        c->bytes_wrote_back_in_this_connection = 0;

        c->query_hit = false;
        c->ret_buf_offset = 0;
        c->ret_bytes = 0;

        c->in_batch_get = false;
        c->batch_num = 0;
        c->batch_count = 0;

        c->acq.init();

        c->thread_index = thread_index;
        c->conn_state = conn_read;  // default to read socket after accepting ;
        c->work_state = parse_head; //default to parse head
        // worker will block if there comes no data in socket
        //pthread_create(&workers[c->sfd], nullptr, (read_handler), (void *)c);
        event_set(&c->event, c->sfd, EV_READ | EV_PERSIST, event_handler, (void *) c);
        event_base_set(base, &c->event);

        if (event_add(&c->event, 0) == -1) {
            perror("event_add");
            return;
        }
        if(thread_index % server_num == 0){ 
        for(int i =0; i< server_num;i++)
            c->aq[i].init();
        conns[sfd] = c;
        CONN_ITEM *citem = (CONN_ITEM *) malloc(sizeof(CONN_ITEM));
        citem->sfd = sfd;
        citem->thread = &threadInfoList[thread_index + THREAD_NUM];
        citem->mode = queue_new_conn;

        pthread_mutex_lock(&citem->thread->conqlock);
        citem->thread->connQueueList->push(citem);
        //printf("thread %d push citem, now cq size:%d\n",port_index,citem->thread->connQueueList->size());
        pthread_mutex_unlock(&citem->thread->conqlock);

        char buf[1];
        buf[0] = 'c';
        if (write(citem->thread->notify_send_fd, buf, 1) != 1) {
            perror("Writing to thread notify pipe");
        }
	
        for(int i =1 ;i< server_num;i++){
              CONN_ITEM *citem = (CONN_ITEM *) malloc(sizeof(CONN_ITEM));
              citem->sfd = sfd;
              citem->thread = &threadInfoList[thread_index + i];
              citem->mode = queue_new_conn;

              pthread_mutex_lock(&citem->thread->conqlock);
              citem->thread->connQueueList->push(citem);
              //printf("thread %d push citem, now cq size:%d\n",port_index,citem->thread->connQueueList->size());
              pthread_mutex_unlock(&citem->thread->conqlock);

              char buf[1];
              buf[0] = 'c';
              if (write(citem->thread->notify_send_fd, buf, 1) != 1) {
                  perror("Writing to thread notify pipe");
              }
          }
      }
    } else {
        c = conns[sfd];
        event_set(&c->pro_event, sfd, EV_READ | EV_PERSIST, event_handler_pro, (void *) c);
        event_base_set(base, &c->pro_event);
        if (event_add(&c->pro_event, 0) == -1) {
            perror("event_add");
            return;
        }

    }
}

void thread_libevent_process(int fd, short which, void *arg) {

    THREAD_INFO *me = (THREAD_INFO *) arg;
    CONN_ITEM *citem = NULL;

    // printf("thread %d awaked \n",me->thread_index);
    char buf[1];

    if (read(fd, buf, 1) != 1) {
        fprintf(stderr, "Can't read from libevent pipe\n");
        return;
    }

    switch (buf[0]) {
        case 'c':
            pthread_mutex_lock(&me->conqlock);
            if (!me->connQueueList->empty()) {
                citem = me->connQueueList->front();
                me->connQueueList->pop();
            }
            pthread_mutex_unlock(&me->conqlock);
            if (citem == NULL) {
                break;
            }
            switch (citem->mode) {
                case queue_new_conn: {
                    conn_new(citem->sfd, me->base, me->thread_index);
                    break;
                }

                case queue_redispatch: {
                    //conn_worker_readd(citem);
                    break;
                }

            }
            break;
        default:
            fprintf(stderr, "pipe should send 'c'\n");
            return;

    }
    free(citem);

}


void setup_worker(int i) {
    THREAD_INFO *me = &threadInfoList[i];
    struct event_config *ev_config;
    ev_config = event_config_new();
    event_config_set_flag(ev_config, EVENT_BASE_FLAG_NOLOCK);
    me->base = event_base_new_with_config(ev_config);
    event_config_free(ev_config);


    if (!me->base) {
        fprintf(stderr, "Can't allocate event base\n");
        exit(1);
    }

    /* Listen for notifications from other threads */
    event_set(&me->notify_event, me->notify_receive_fd,
              EV_READ | EV_PERSIST, thread_libevent_process, me);
    event_base_set(me->base, &me->notify_event);

    if (event_add(&me->notify_event, 0) == -1) {
        fprintf(stderr, "Can't monitor libevent notify pipe\n");
        exit(1);
    }


}

void init_workers() {
    //connQueueList=(std::queue<CONN_ITEM>*)calloc(THREAD_NUM,sizeof(std::queue<CONN_ITEM>));
    threadInfoList = (THREAD_INFO *) calloc(THREAD_NUM * 2, sizeof(THREAD_INFO));
    //pthread_t tids[THREAD_NUM];
    for (int i = 0; i < THREAD_NUM * 2; i++) {
        int fds[2];
        if (pipe(fds)) {
            perror("Can't create notify pipe");
            exit(1);
        }
        threadInfoList[i].notify_receive_fd = fds[0];
        threadInfoList[i].notify_send_fd = fds[1];

        threadInfoList[i].thread_index = i;

        threadInfoList[i].connQueueList = new queue<CONN_ITEM *>;


        setup_worker(i);
    }

    int ret;
    for (int i = 0; i < THREAD_NUM * 2; i++) {
        if ((ret = pthread_create(&threadInfoList[i].thread_id,
                                  NULL,
                                  worker,
                                  (void *) &threadInfoList[i].thread_index)) != 0) {
            fprintf(stderr, "Can't create thread: %s\n",
                    strerror(ret));
            exit(1);
        }
        //pthread_exit(NULL);
    }

}


void conn_dispatch(evutil_socket_t listener, short event, void *args) {
    int port_index = *(int *) args;
    //printf("listerner :%d  port %d\n", listener, portList[port_index]);
    cout<<port_index<<endl;
    struct sockaddr_storage ss;
    socklen_t slen = sizeof(ss);
    int fd = accept(listener, (struct sockaddr *) &ss, &slen);

    if (fd == -1) {
        perror("accept()");
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            /* these are transient, so don't log anything */
            return;
        } else if (errno == EMFILE) {
            fprintf(stderr, "Too many open connections\n");
            exit(1);
        } else {
            perror("accept()");
            return;
        }
    }
    
    if (fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK) < 0) {
        perror("setting O_NONBLOCK");
        close(fd);
        return;
    }

    assert(fd > 2);


    CONN_ITEM *citem = (CONN_ITEM *) malloc(sizeof(CONN_ITEM));
    citem->sfd = fd;
    citem->thread = &threadInfoList[port_index];
    citem->mode = queue_new_conn;

    pthread_mutex_lock(&citem->thread->conqlock);
    citem->thread->connQueueList->push(citem);
    //printf("thread %d push citem, now cq size:%d\n",port_index,citem->thread->connQueueList->size());
    pthread_mutex_unlock(&citem->thread->conqlock);

    char buf[1];
    buf[0] = 'c';
    if (write(citem->thread->notify_send_fd, buf, 1) != 1) {
        perror("Writing to thread notify pipe");
    }
    /*
    citem = (CONN_ITEM *) malloc(sizeof(CONN_ITEM));
    citem->sfd = fd;
    citem->thread = &threadInfoList[port_index+THREAD_NUM];
    citem->mode = queue_new_conn;

    pthread_mutex_lock(&citem->thread->conqlock);
    citem->thread->connQueueList->push(citem);
    //printf("thread %d push citem, now cq size:%d\n",port_index,citem->thread->connQueueList->size());
    pthread_mutex_unlock(&citem->thread->conqlock);

    buf[0] = 'c';
    if (write(citem->thread->notify_send_fd, buf, 1) != 1) {
        perror("Writing to thread notify pipe");
    }
    // printf("awake thread %d\n", port_index);
    */
}

void *createWorker(void *args) {
    struct target *work = (struct target *) args;
    if ((numaScheme & 0x4) != 0) pin_to_core(work->core + 48 * 3);
    uint8_t value[DEFAULT_STR_LENGTH];
    int i = work->tid / (thread_number / hlog_number);
    store->Create(i);
}


int main(int argc, char **argv) {

    if (argc == 2) {
        port_num = atol(argv[1]);

        // batch_num = atol(argv[3]); //not used

    } else {
        printf("./multiport_network  <port_num> \n");
        return 0;
    }
    //port_num = 4;
    store = new store_t(hlog_number, next_power_of_two(root_capacity / 2), 17179869184, "storage");//,68719476736ull,
    cout << " port_num : " << port_num << endl;
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
    prepare();
    thread_number = port_num;
    int cd = thread_number / hlog_number;
    for (int i = 0; i < hlog_number; i++) {
        pthread_create(&workers[i * cd], nullptr, createWorker, &parms[i * cd]);
        pthread_join(workers[i * cd], nullptr);
    }
    
    for (int i = 0; i < PORT_NUM; i++)
	    cl[i].store(0);

    init_portlist();

    init_conns();

    init_workers();

    //do not stop the server when writing to a closed socket
    signal(SIGPIPE, SIG_IGN);

    struct event_base *base;
    base = event_base_new();
    if (!base)
        return -1; /*XXXerr*/

    for (int i = 0; i < PORT_NUM; i++) {
        evutil_socket_t listener;
        struct event *listener_event;

        struct sockaddr_in sin;
        sin.sin_family = AF_INET;
        sin.sin_addr.s_addr = 0;
        sin.sin_port = htons(portList[i]);

        listener = socket(AF_INET, SOCK_STREAM, 0);
        evutil_make_socket_nonblocking(listener);


        if (bind(listener, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
            perror("bind");
            return -1;
        }

        if (listen(listener, 16) < 0) {
            perror("listen");
            return -1;
        }

        listener_event = event_new(base, listener,
                                   EV_READ | EV_PERSIST,
                                   conn_dispatch,
                                   (void *) &threadInfoList[i].thread_index);
        /*XXX check it */
        event_add(listener_event, NULL);
    }

    event_base_dispatch(base);

}


