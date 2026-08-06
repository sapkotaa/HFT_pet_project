#pragma once
#include <sys/event.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <unordered_map>
#include <iostream>
#include "frame_reader.hpp"
#include "session_state.hpp"

struct ConnectionState{
    FrameReader reader;
    SessionState session;
};

class GatewayServer{
    public:
     explicit GatewayServer(uint16_t port) : port_(port){}

     bool start(){
        listen_fd_ = socket(AF_INET,SOCK_STREAM,0);
        if (listen_fd_ < 0) return false;

        int opt = 1;
        setsockopt(listen_fd_,SOL_SOCKET,SO_REUSEADDR, &opt,sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family =  AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port_);

        if(bind(listen_fd_,(sockaddr*)&addr,sizeof(addr)) < 0) return false;
        if(listen(listen_fd_, /*backlog=*/128) < 0) return false;
        set_nonblocking(listen_fd_);

        kq_ = kqueue();

        register_read(listen_fd_);
        return true;

     }


     // call in a loop from main(). Blocks until at least one fd is ready
     void poll_once(){
        struct kevent events[64];
        int n_ready = kevent(kq_,nullptr,0,events,64,nullptr);

        for(int i = 0;i<n_ready;++i){
            int fd = static_cast<int>(events[i].ident);

            if(fd == listen_fd_){
                accept_new_connetion();
                continue;
            }

            if(events[i].flags & EV_EOF){
                close_connection(fd);
                continue;
            }
            handle_readable(fd);
        }


     }
    private:
     void set_nonblocking(int fd){
        int flags = fcntl(fd,F_GETFL,0);
        fcntl(fd,F_SETFL,flags | O_NONBLOCK);
     }

     void register_read(int fd){
        struct kevent ev;
        EV_SET(&ev,fd,EVFILT_READ,EV_ADD,0,0,nullptr);
        kevent(kq_,&ev,1,nullptr,0,nullptr);
     }

     void accept_new_connection(){
        int client_fd = accept(listen_fd_,nullptr,nullptr);
        if(client_fd < 0) return;

         set_nonblocking(client_fd);
         register_read(client_fd);
         connections_[client_fd] = ConnectionState{};
         std::cout << "accepted fd=" << client_fd << "\n";
 }

  void handle_readable(int fd){
    auto it = connections_.find(fd);
    if (it==connections_.end()) return;

    uint8_t buf[4096];
   // Edge triggered by default on kqueue too : drain until EAGAIN
   // same discipline as epoll;
   while(true){
     ssize_t n = recv(fd,buf,sizeof(buf),0);
    if(n>0){

    } else if(n==0){
        close_connection(fd);
        return;
    } else {
        if(errno == EAGAIN || errno == EWOULDBLOCK) return; // drained
        close_connection(fd);
        return;
    }
     }
 

  }

  void close_connection(int fd){
    struct kevent ev;
    EV_SET(&ev,fd,EVFILT_READ,EV_DELETE,0,0,nullptr);
    kevent(kq_, &ev, 1, nullptr, 0, nullptr);
    close(fd);
    connections_.erase(fd);
    std::cout<< "closed fd=" << fd << "\n" ;
  }


void dispatch(int fd, const std::vector<uint8_t>& frame, SessionState& session){
    // TODO: parse MsgHeader, switch on msg_type, call session.handle_logon /
        // track_inbound_seq / push NewOrder onto the SPSC queue to the engine
}




uint16_t port_;
int listen_fd_ = -1;
int kq_ = -1; //linux uses epoll 
std::unordered_map<int,ConnectionState> connections_;
};