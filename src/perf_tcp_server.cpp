/** This is a TcpServer's performance test.
  * It could be run in N different modes:
  * 1) Local throughput test:
  *     Clients and server created on the same machine and communicates through the loopback.
  *     This mode is designated to test server performance locally, on a single machine. And
  *     this mode is very limited because clients affects the server performance.
  * 2) Server mode:
  *     Application starts in server mode and accepts incomming connections. It can be termia
  *     ted using ^C signal. This mode can be used to test server performance in isolation.
  *     Also, this test mode takes network into account. Application should dump number of me
  *     ssages per second to stdout.
  * 3) Client mode:
  *     Application started in client mode can connect to server (the same application should
  *     be started on another node in server mode) and dump specified number of messages to
  *     the server.
  *
  * Parameters:
  * a) `mode` can accept three parameters - 'client', 'server' or 'local'.
  * b) `host` url or ip of the server if app was started in client mode.
  * c) `count` number of messages to send inf started in client mode.
  * d) `njobs` number of threads to use
  */
#include <iostream>
#include <thread>
#include <boost/thread/barrier.hpp>
#include <boost/program_options.hpp>

#include "tcp_server.h"
#include "perftest_tools.h"
#include <sys/time.h>

using namespace Akumuli;
namespace po = boost::program_options;

struct DbMock : DbConnection {
    typedef std::tuple<aku_ParamId, aku_TimeStamp, double> ValueT;
    aku_ParamId idsum;
    aku_TimeStamp tssum;
    double valsum;

    DbMock() {
        idsum  = 0;
        tssum  = 0;
        valsum = 0;
    }

    void write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
        idsum  += param;
        tssum  += ts;
        valsum += data;
    }
};

enum Mode {
    CLIENT,
    SERVER,
    LOCAL,
};


Mode str_to_mode(std::string str) {
    if (str == "client" || str == "CLIENT") {
        return CLIENT;
    } else if (str == "server" || str == "SERVER") {
        return SERVER;
    } else if (str == "local" || str == "LOCAL") {
        return LOCAL;
    }
    throw std::runtime_error("Bad mode value");
}

struct Server {

    Mode                                mode;
    std::shared_ptr<IngestionPipeline>  pline;
    std::shared_ptr<DbMock>             dbcon;
    std::shared_ptr<TcpServer>          serv;
    boost::asio::io_service             ioA;
    std::vector<IOService*>             iovec       = { &ioA };
    boost::barrier                      barrier;

    Server(Mode mode)
        : mode(mode)
        , barrier(iovec.size() + 1)
    {
        dbcon = std::make_shared<DbMock>();
        pline = std::make_shared<IngestionPipeline>(dbcon, AKU_LINEAR_BACKOFF);
        int port = 4096;
        serv = std::make_shared<TcpServer>(iovec, port, pline);
        pline->start();
        serv->start();
    }

    //! Run IO service
    void start() {
        auto iorun = [](IOService& io, boost::barrier& bar) {
            auto fn = [&]() {
                io.run();
                bar.wait();
            };
            return fn;
        };

        for (auto io: iovec) {
            std::thread iothread(iorun(*io, barrier));
            iothread.detach();
        }
    }

    void stop() {
        serv->stop();
        std::cout << "TcpServer stopped" << std::endl;

        // No need to joint I/O threads, just wait until they completes.
        barrier.wait();
        std::cout << "I/O threads stopped" << std::endl;

        pline->stop();
        std::cout << "Pipeline stopped" << std::endl;

        for (auto io: iovec) {
            io->stop();
        }
        std::cout << "I/O service stopped" << std::endl;

        std::cout << dbcon->idsum << " messages received" << std::endl;
    }
};


struct Client {
    int nthreads;
    int count;
    boost::barrier start_barrier, stop_barrier;
    EndpointT endpoint;

    Client(EndpointT ep, int nthreads = 4, int count = 2500000)
        : nthreads(nthreads)
        , count(count)
        , start_barrier(nthreads)
        , stop_barrier(nthreads + 1)
        , endpoint(ep)
    {
    }

    void start() {
        auto self = this;
        auto push = [self]() {
            IOService io;
            TcpSocket socket(io);
            std::cout << "Connecting to server at " << self->endpoint << std::endl;
            socket.connect(self->endpoint);
            self->start_barrier.wait();

            boost::asio::streambuf stream;
            std::ostream os(&stream);
            os << ":1\r\n" ":2\r\n" "+3.14\r\n";
            for (int i = self->count; i --> 0; ) {
                boost::asio::write(socket, stream);
            }
            socket.shutdown(TcpSocket::shutdown_both);
            self->stop_barrier.wait();
            std::cout << "Push process completed" << std::endl;
        };

        for (int i = 0; i < nthreads; i++) {
            std::thread th(push);
            th.detach();
        }
    }

    void wait() {
        stop_barrier.wait();
    }
};

int main(int argc, char *argv[]) {
    std::cout << "Tcp server performance test" << std::endl;
    po::options_description desc("Allowed options");
    /*
     * Parameters:
     * a) `mode` can accept three parameters - 'client', 'server' or 'local'.
     * b) `host` url or ip of the server if app was started in client mode.
     * c) `count` number of messages to send inf started in client mode.
     * d) `njobs` number of threads to use
     */
    std::string strmode;
    std::string host;
    int num_messages;
    desc.add_options()
        ("help",                                                                "produce help message")
        ("mode",    po::value<std::string>(&strmode)->default_value("local"),   "test mode")
        ("host",    po::value<std::string>(&host)->default_value("localhost"),  "server host in client mode")
        ("count",   po::value<int>(&num_messages)->default_value(1000000),      "number of messages to send")
    ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    Mode mode = str_to_mode(strmode);

    switch(mode) {
    case LOCAL: {
        Server server(mode);
        EndpointT ep(boost::asio::ip::address_v4::loopback(), 4096);
        Client client(ep);
        server.start();
        client.start();
        client.wait();
        server.stop();
        break;
    }
    case SERVER:
        break;
    case CLIENT:
        break;
    }


    return 0;
}
