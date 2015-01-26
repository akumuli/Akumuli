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
    size_t nrec = 0u;
    PerfTimer tm;
    Logger logger_ = Logger("dbmock", 100);

    void write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
        static const int N = 1000000;
        if (nrec++ % N == 0) {
            double elapsed = tm.elapsed();
            auto throughput = static_cast<int>(N*(1.0/elapsed));
            logger_.info() << "Server throughput " << throughput << " msg/sec";
            tm.restart();
        }
    }
};


struct Server {

    std::shared_ptr<IngestionPipeline>  pline;
    std::shared_ptr<DbMock>             dbcon;
    std::shared_ptr<TcpAcceptor>          serv;
    boost::asio::io_service             ioA;
    std::vector<IOServiceT*>             iovec       = { &ioA };
    boost::barrier                      barrier;
    boost::asio::signal_set             sig;
    std::atomic<int>                    stopped = {0};

    Server()
        : barrier(iovec.size() + 1)
        , sig(ioA, SIGINT)
    {
        dbcon = std::make_shared<DbMock>();
        pline = std::make_shared<IngestionPipeline>(dbcon, AKU_LINEAR_BACKOFF);
        int port = 4096;
        serv = std::make_shared<TcpAcceptor>(iovec, port, pline);
        pline->start();
        serv->start();
    }

    //! Run IO service
    void start() {

        sig.async_wait(
                    // Wait for sigint
                    boost::bind(&Server::handle_sigint,
                                this,
                                boost::asio::placeholders::error)
                    );

        auto iorun = [](IOServiceT& io, boost::barrier& bar) {
            auto fn = [&]() {
                try {
                    io.run();
                    bar.wait();
                } catch (RESPError const& e) {
                    std::cout << e.what() << std::endl;
                    std::cout << e.get_bottom_line() << std::endl;
                    throw;
                }
            };
            return fn;
        };

        for (auto io: iovec) {
            std::thread iothread(iorun(*io, barrier));
            iothread.detach();
        }
    }

    void handle_sigint(boost::system::error_code err) {
        if (!err) {
            if (stopped++ == 0) {
                std::cout << "SIGINT catched, stopping pipeline" << std::endl;
                for (auto io: iovec) {
                    io->stop();
                }
                pline->stop();
                serv->stop();
                barrier.wait();
                std::cout << "Server stopped" << std::endl;
            } else {
                std::cout << "Already stopped (sigint)" << std::endl;
            }
        } else {
            std::cout << "Signal handler error " << err.message() << std::endl;
        }
    }

    void stop() {
        if (stopped++ == 0) {
            serv->stop();
            std::cout << "TcpServer stopped" << std::endl;

            sig.cancel();

            // No need to joint I/O threads, just wait until they completes.
            barrier.wait();
            std::cout << "I/O threads stopped" << std::endl;

            pline->stop();
            std::cout << "Pipeline stopped" << std::endl;

            for (auto io: iovec) {
                io->stop();
            }
            std::cout << "I/O service stopped" << std::endl;

            std::cout << dbcon->nrec << " messages received" << std::endl;
        } else {
            std::cout << "Already stopped" << std::endl;
        }
    }

    void wait() {
        while(!stopped) {
            sleep(1);
        }
    }
};

int main(int argc, char *argv[]) {
    std::cout << "Tcp server performance test" << std::endl;
    Server server;
    server.start();
    server.wait();
    server.stop();
}
