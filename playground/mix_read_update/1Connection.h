#ifndef MULTIPORT_NETWORK_CONNECTION_H
#define MULTIPORT_NETWORK_CONNECTION_H

#include <string>
#include <arpa/inet.h>
#include "settings.h"
#include <atomic>
#include <fcntl.h>
using namespace std;

class Connection {
public:

    Connection():fd(-1), offset(0), op_count(0), send_bytes(0){};
    ~Connection();

    void init_server(int con_id_,const string server_ip_);
    void init();
    bool fetch_and_send(package_obj p);
    void clean();

    int get_fd(){return this->fd;}
    char *get_buf(){ return  this->send_buf;}
    uint32_t get_offset(){return this->offset;}
    uint64_t get_send_bytes(){return this->send_bytes;}

    atomic<int> flag;

private:
    int con_id;
    int fd ;
    char * send_buf;
    uint32_t offset;
    uint32_t op_count;
    uint64_t send_bytes;
    void connect_server(const string server_ip);
};


void Connection::init_server(int con_id_,const string server_ip_) {
        this->con_id = con_id_;
        connect_server(server_ip_);
        this->send_buf = (char *)calloc(PACKAGE_LEN, SEND_BATCH + 100);

};

void Connection::init() {
    this->send_buf = (char *)calloc(PACKAGE_LEN, SEND_BATCH + 100);
};

Connection::~Connection(){
    close(this->fd);
}

void Connection::connect_server(const string server_ip) {
    unsigned int connect_fd;
    static struct sockaddr_in srv_addr;
    //create  socket
    connect_fd = socket(AF_INET, SOCK_STREAM, 0);
    if(connect_fd < 0) {
        perror("cannot create communication socket");
        return ;
    }

    srv_addr.sin_family = AF_INET;
    srv_addr.sin_port = htons(PORT_BASE + con_id);
    srv_addr.sin_addr.s_addr = inet_addr(server_ip.c_str());


    //connect server;
    if( connect(connect_fd, (struct sockaddr*)&srv_addr, sizeof(srv_addr)) == -1) {
        perror("cannot connect to the server");
        close(connect_fd);
        return ;
    }
    /*
    if (fcntl(connect_fd, F_SETFL, fcntl(connect_fd, F_GETFL) | O_NONBLOCK) < 0) {
        perror("setting O_NONBLOCK");
        close(connect_fd);
        return ;
    }
    */
    this->fd = connect_fd;

    //printf("connect to port %d \n",PORT_BASE + con_id);
}

bool Connection::fetch_and_send(package_obj p) {
    memcpy(send_buf + offset, p.package_ptr, p.package_len);
    offset += p.package_len;
    ++ op_count;
    if(op_count == SEND_BATCH){
        flag.fetch_add(1);
        while(flag.load()==1){
            std::this_thread::sleep_for( std::chrono::nanoseconds(5));
        }
        /*int ret = write(this->fd, send_buf, offset); //The offset is equal to the amount of data
        if(ret != offset){
            //actually not error;we didn't take this condition into consideration before,so we let it crash here
            //once the program exit here, old test data need to be reconsidered.
            perror("write error");
            exit(-1);
        }*/
        send_bytes += offset;
        op_count = 0;
        offset = 0;
        return true;
    }else{
        return false;
    }
}

void Connection::clean() {
    int ret = write(this->fd, send_buf, offset); //The offset is equal to the amount of data
    if(ret != offset){
        //actually not error;we didn't take this condition into consideration before,so we let it crash here
        //once the program exit here, old test data need to be reconsidered.
        perror("write error");
        exit(-1);
    }
    send_bytes += offset;
    op_count = 0;
    offset = 0;
    printf("Connection %d send %lu bytes\n",con_id, send_bytes);
}

#endif //MULTIPORT_NETWORK_CONNECTION_H
