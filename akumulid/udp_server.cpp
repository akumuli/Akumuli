#include "udp_server.h"

#include <thread>

#include <netinet/ip.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>

#include <boost/bind.hpp>

namespace Akumuli {

UdpServer::UdpServer(std::shared_ptr<IngestionPipeline> pipeline, int nworkers, int port)
    : pipeline_(pipeline)
    , start_barrier_(nworkers + 1)
    , stop_barrier_(nworkers + 1)
    , stop_{0}
    , port_(port)
    , nworkers_(nworkers)
    , logger_("UdpServer", 128)
{
}


void UdpServer::start(SignalHandler *sig, int id) {
    auto self = shared_from_this();
    sig->add_handler(boost::bind(&UdpServer::stop, std::move(self)), id);

    auto logger = &logger_;
    auto error_cb = [logger](aku_Status status, uint64_t counter) {
        const char* msg = aku_error_message(status);
        logger->error() << msg;
    };
    // Create workers
    for (int i = 0; i < nworkers_; i++) {
        auto spout = pipeline_->make_spout();
        spout->set_error_cb(error_cb);
        std::thread thread(std::bind(&UdpServer::worker, shared_from_this(), spout));
        thread.detach();
    }
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

    ProtocolParser parser(spout);
    try {

        parser.start();

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

        timeval tval;
        tval.tv_sec = 0;
        tval.tv_usec = 1000;  // 1ms
        if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tval, sizeof(tval)) == -1) {
            const char* msg = strerror(errno);
            std::stringstream fmt;
            fmt << "can't set socket timeout: " << msg;
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

        auto iobuf = std::make_shared<IOBuf>();

        while(!stop_.load(std::memory_order_relaxed)) {
            retval = recvmmsg(sockfd, iobuf->msgs, NPACKETS, MSG_WAITFORONE, nullptr);
            if (retval == -1) {
                if (errno == EAGAIN || errno == EINTR) {
                    continue;
                }
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
                size_t mlen = iobuf->msgs[i].msg_len;
                iobuf->msgs[i].msg_len = 0;

                // parse message content
                PDU pdu = {
                    std::shared_ptr<Byte>(iobuf, iobuf->bufs[i]),
                    mlen,
                    0u,
                };

                parser.parse_next(pdu);
            }
            if (retval != 0) {
                iobuf = std::make_shared<IOBuf>();
            }
        }
    } catch(...) {
        logger_.error() << boost::current_exception_diagnostic_information();
    }

    parser.close();

    stop_barrier_.wait();
}


struct UdpServerBuilder {

    UdpServerBuilder() {
        ServerFactory::instance().register_type("UDP", *this);
    }

    std::shared_ptr<Server> operator () (std::shared_ptr<IngestionPipeline> pipeline,
                                         std::shared_ptr<ReadOperationBuilder>,
                                         const ServerSettings& settings) {
        return std::make_shared<UdpServer>(pipeline, settings.nworkers, settings.port);
    }
};

static UdpServerBuilder reg_type;


}

