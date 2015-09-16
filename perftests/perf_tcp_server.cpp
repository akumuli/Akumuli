#include <iostream>
#include <thread>

#include "tcp_server.h"
#include "perftest_tools.h"
#include <sys/time.h>

using namespace Akumuli;

struct DbMock : DbConnection {
    typedef std::tuple<aku_ParamId, aku_Timestamp, double> ValueT;
    size_t nrec = 0u;
    PerfTimer tm;
    Logger logger_ = Logger("dbmock", 100);

    aku_Status write(aku_Sample const&) {
        static const int N = 1000000;
        if (nrec++ % N == 0) {
            double elapsed = tm.elapsed();
            auto throughput = static_cast<int>(N*(1.0/elapsed));
            logger_.info() << "Server throughput " << throughput << " msg/sec";
            tm.restart();
        }
        return AKU_SUCCESS;
    }

    virtual std::shared_ptr<DbCursor> search(std::string query) {
        throw "not implemented";
    }

    int param_id_to_series(aku_ParamId id, char *buffer, size_t buffer_size) {
        throw "not implemented";
    }

    aku_Status series_to_param_id(const char *name, size_t size, aku_Sample *sample) {
        throw "not implemented";
    }
};

int main(int argc, char *argv[]) {
    std::cout << "Tcp server performance test" << std::endl;
    auto con = std::make_shared<DbMock>();
    auto ppl = std::make_shared<IngestionPipeline>(con, AKU_LINEAR_BACKOFF);
    auto server = std::make_shared<TcpServer>(ppl, 4, 4111);
    server->start();
    server->wait_for_signal();
    server->stop();
}
