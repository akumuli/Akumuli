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
    PerfTimer *timer;

    Client(EndpointT ep, PerfTimer *timer, int nthreads = 4, int count = 2500000)
        : nthreads(nthreads)
        , count(count)
        , start_barrier(nthreads + 1)
        , stop_barrier(nthreads + 1)
        , endpoint(ep)
        , timer(timer)
    {
    }

    void start() {
        auto self = this;
        auto push = [self]() {
            std::vector<double> threshold_values;
            IOService io;
            TcpSocket socket(io);
            std::cout << "Connecting to server at " << self->endpoint << std::endl;
            socket.connect(self->endpoint);
            self->start_barrier.wait();

            boost::asio::streambuf stream;
            std::ostream os(&stream);
            size_t nsent = 0u;
            double tm = self->timer->elapsed();
            for (int i = self->count; i --> 0; ) {
                os << ":1\r\n" ":2\r\n" "+3.14\r\n";
                nsent += boost::asio::write(socket, stream);
                if (nsent >= 1024*1024) {  // +1Mb was sent
                    double newtm = self->timer->elapsed();
                    threshold_values.push_back(newtm - tm);
                    nsent = 0u;
                    tm = newtm;
                }
            }
            socket.shutdown(TcpSocket::shutdown_both);
            self->stop_barrier.wait();
            std::cout << "Push process completed" << std::endl;
            if (threshold_values.size()) {
                std::sort(threshold_values.begin(), threshold_values.end());
                double min = threshold_values.front(), max = threshold_values.back();
                double sum = 0, avg = 0, med = threshold_values[threshold_values.size() / 2];
                for (auto x: threshold_values) { sum += x; }
                avg = sum / threshold_values.size();
                auto convert = [&](double val) {
                    // Convert seconds per Mb to Mb per second
                    return 1.0/val;
                };
                std::cout << "Push process performance" << std::endl;
                std::cout << "max: " << convert(min) << " Mb/sec" << std::endl;  // min and max should be
                std::cout << "min: " << convert(max) << " Mb/sec" << std::endl;  // swapped because of conv
                std::cout << "avg: " << convert(avg) << " Mb/sec" << std::endl;
                std::cout << "med: " << convert(med) << " Mb/sec" << std::endl;
            }
        };

        for (int i = 0; i < nthreads; i++) {
            std::thread th(push);
            th.detach();
        }

        start_barrier.wait();
        timer->restart();
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
    bool graphite_enabled = false;
    desc.add_options()
        ("help",                                                                 "produce help message")
        ("mode",     po::value<std::string>(&strmode)->default_value("local"),   "test mode")
        ("host",     po::value<std::string>(&host)->default_value("localhost"),  "server host in client mode")
        ("count",    po::value<int>(&num_messages)->default_value(1000000),      "number of messages to send")
        ("graphite", po::value<bool>(&graphite_enabled)->default_value(false),   "push result to graphite")
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
        PerfTimer tm;
        Server server(mode);
        EndpointT ep(boost::asio::ip::address_v4::loopback(), 4096);
        Client client(ep, &tm, 4, 2500000);
        server.start();
        client.start();
        client.wait();
        server.stop();
        double elapsed = tm.elapsed();
        std::cout << "Local test completed in " << elapsed << " seconds" << std::endl;
        if (graphite_enabled) {
            push_metric_to_graphite("tcp_server_local", elapsed);
        }
        break;
    }
    case SERVER:
        break;
    case CLIENT:
        break;
    }


    return 0;
}
