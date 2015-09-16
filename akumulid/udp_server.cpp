#include "udp_server.h"

#include <thread>

#include <netinet/ip.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>

namespace Akumuli {

UdpServer::UdpServer(std::shared_ptr<IngestionPipeline> pipeline, int nworkers, int port)
    : start_barrier_(nworkers + 1)
    , stop_barrier_(nworkers + 1)
    , stop_{0}
    , port_(port)
    , logger_("UdpServer", 128)
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


void UdpServer::worker(std::shared_ptr<PipelineSpout> spout) {
    start_barrier_.wait();

    int sockfd, retval;
    sockaddr_in sa;

    try {

        ProtocolParser parser(spout);

        // Create socket
        sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        if (sockfd == -1) {
            const char* msg = strerror(errno);
            std::stringstream fmt;
            fmt << "can't create socket: " << msg;
            std::runtime_error err(fmt.str());
            BOOST_THROW_EXCEPTION(err);
        }

        // Set socket options
        int optval = 1;
        if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval)) == -1) {
            const char* msg = strerror(errno);
            std::stringstream fmt;
            fmt << "can't set socket options: " << msg;
            std::runtime_error err(fmt.str());
            BOOST_THROW_EXCEPTION(err);
        }

        // Bind socket to port
        sa.sin_family = AF_INET;
        sa.sin_addr.s_addr = htonl(INADDR_ANY);
        sa.sin_port = htons(port_);

        if (bind(sockfd, (sockaddr *) &sa, sizeof(sa)) == -1) {
            const char* msg = strerror(errno);
            std::stringstream fmt;
            fmt << "can't bind socket: " << msg;
            std::runtime_error err(fmt.str());
            BOOST_THROW_EXCEPTION(err);
        }

        while(!stop_.load(std::memory_order_relaxed)) {
            std::shared_ptr<IOBuf> iobuf(new IOBuf());
            retval = recvmmsg(sockfd, iobuf->msgs, NPACKETS, MSG_WAITFORONE, nullptr);
            // TODO: uset timeout to wake up thread periodically
            if (retval == -1) {
                const char* msg = strerror(errno);
                std::stringstream fmt;
                fmt << "socket read error: " << msg;
                std::runtime_error err(fmt.str());
                BOOST_THROW_EXCEPTION(err);
            }

            iobuf->pps++;

            for (int i = 0; i < retval; i++) {
                // reset buffer to receive new message
                iobuf->bps += iobuf->msgs[i].msg_len;
                iobuf->msgs[i].msg_len = 0;

                // parse message content
                PDU pdu = {
                    std::shared_ptr<Byte>(iobuf, iobuf->bufs[i]),
                    MSS,
                    0u,
                };

                parser.parse_next(pdu);
            }
        }
    } catch(...) {
        logger_.error() << boost::current_exception_diagnostic_information();
    }

    stop_barrier_.wait();
}

}

