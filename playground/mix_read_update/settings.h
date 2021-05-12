
#ifndef SETTINGS_H
#define SETTINGS_H

#define HEAD_LEN 16

#define KEY_LEN 30
#define VALUE_LEN 8//32

#define NUM 5000000
#define KV_NUM  (NUM * 10)

#define KEY_RANGE KV_NUM
#define SKEW 0.0

#define ROUND_SET 100//200

#define PACKAGE_LEN    (HEAD_LEN + KEY_LEN + VALUE_LEN)
#define DATABASE_LEN    (KV_NUM * PACKAGE_LEN)

//#define DATA_SIZE ((KV_NUM * PACKAGE_LEN ) / 1000000000.0 )

#define PORT_BASE 8033

int port_num;
int send_batch;
//#define PORT_NUM 4
#define CONNECTION_NUM port_num


#define HEAD_MAGIC(buf)           (buf)
#define HEAD_OPCODE(buf)          (buf + 1)
#define HEAD_KEY_LENGTH(buf)      (buf + 2)
#define HEAD_BATCH_NUM(buf)       (buf + 4)
#define HEAD_PRE_HASH(buf)        (buf + 6)
#define HEAD_RETAIN(buf)          (buf + 7)
#define HEAD_BODY_LENGTH(buf)     (buf + 8)
#define HEAD_THREAD_NUM(buf)      (buf + 10)
#define HEAD_TIME(buf)      (buf + 12)
#define PACKAGE_KEY(buf)             (buf + HEAD_LEN)
#define PACKAGE_VALUE(buf)           (buf + HEAD_LEN + KEY_LEN)

#define SEND_BATCH send_batch//20000

#define WORK_OP_NUM (SEND_BATCH * port_num)
#define WORK_LEN (WORK_OP_NUM * PACKAGE_LEN )

#define GET_PACKAGE(buf,i)  (buf + i * PACKAGE_LEN)



#define DATABASE_LEN KV_NUM * PACKAGE_LEN

typedef struct Package_obj{
    char * package_ptr  ;
    size_t  package_len;
}package_obj;

typedef struct {
    uint8_t magic;
    uint8_t opcode;
    uint16_t keylen;
    uint16_t batchnum;
    uint8_t prehash;
    uint8_t reserved;
    uint16_t totalbodylen;
    uint16_t  threadnum;
} protocol_binary_request_header;

#endif
