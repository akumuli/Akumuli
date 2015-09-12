#include "udp_server.h"

#include <thread>

#include <netinet/ip.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>

namespace Akumuli {

UdpServer::UdpServer(int nworkers, int port, std::shared_ptr<IngestionPipeline> pipeline)
    : start_barrier_(nworkers + 1)
    , stop_barrier_(nworkers + 1)
    , stop_{0}
    , port_(port)
{
    // Create workers
    for (int i = 0; i < nworkers; i++) {
        auto spout = pipeline->make_spout();
        std::thread thread(std::bind(&UdpServer::worker, shared_from_this(), spout));
        thread.detach();
    }
}


void UdpServer::start() {
    start_barrier_.wait();
}


void UdpServer::stop() {
    stop_.store(1, std::memory_order_relaxed);
    stop_barrier_.wait();
}


void UdpServer::worker(std::shared_ptr<PipelineSpout> spout, std::shared_ptr<IOBuf> iobuf) {
    start_barrier_.wait();

    int sockfd, retval;
    sockaddr_in sa;

    // Create socket
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd == -1) {
        perror("socket()");
        std::cerr << "FAILURE" << std::endl;
        return;
    }

    // Set socket options
    int optval = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval)) == -1) {
        perror("setsockopt()");
        std::cerr << "FAILURE" << std::endl;
        return;
    }

    // Bind socket to port
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_ANY);
    sa.sin_port = htons(port_);

    if (bind(sockfd, (sockaddr *) &sa, sizeof(sa)) == -1) {
        std::cerr << "FAILURE" << std::endl;
        return;
    }

    while(!stop_.load(std::memory_order_relaxed)) {
        retval = recvmmsg(sockfd, iobuf->msgs, NPACKETS, MSG_WAITFORONE, nullptr);
        if (retval == -1) {
            // TODO: better error processing
            perror("recvmmsg()");
            std::cerr << "FAILURE" << std::endl;
            return;
        }

        iobuf->pps++;

        for (int i = 0; i < retval; i++) {
            // reset buffer to receive new message
            iobuf->bps += iobuf->msgs[i].msg_len;
            iobuf->msgs[i].msg_len = 0;
        }
    }
    stop_barrier_.wait();
}

}

