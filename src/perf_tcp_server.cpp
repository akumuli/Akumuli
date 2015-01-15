#include <iostream>
#include <thread>
#include <boost/thread/barrier.hpp>

#include "tcp_server.h"
#include <sys/time.h>

using namespace Akumuli;

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

class Timer
{
public:
    Timer() { gettimeofday(&_start_time, nullptr); }
    void   restart() { gettimeofday(&_start_time, nullptr); }
    double elapsed() const {
        timeval curr;
        gettimeofday(&curr, nullptr);
        return double(curr.tv_sec - _start_time.tv_sec) +
               double(curr.tv_usec - _start_time.tv_usec)/1000000.0;
    }
private:
    timeval _start_time;
};

int main() {
    std::cout << "Tcp server performance test" << std::endl;

    // Create mock pipeline
    auto dbcon = std::make_shared<DbMock>();
    auto pline = std::make_shared<IngestionPipeline>(dbcon, AKU_LINEAR_BACKOFF);
    pline->start();

    // Run server
    // boost::asio::io_service ioA, ioB, ioC;  // Several io-services version
    boost::asio::io_service ioA;
    //std::vector<IOService*> iovec = { &ioA , &ioB, &ioC };  // Several io-services version
    std::vector<IOService*> iovec = { &ioA };
    int port = 4096;
    auto serv = std::make_shared<TcpServer>(iovec, port, pline);
    serv->start();

    // Run IO service
    auto iorun = [](IOService& io) {
        auto fn = [&]() {
            io.run();
        };
        return fn;
    };

    std::thread iothreadA(iorun(ioA));
    std::thread iothreadB(iorun(ioA));
    std::thread iothreadC(iorun(ioA));

    std::this_thread::sleep_for(std::chrono::seconds(1));

    boost::barrier barrier(4);

    // Push data to server
    auto push = [&]() {
        IOService io;
        const int COUNT = 2500000;  // 25M
        TcpSocket socket(io);
        auto loopback = boost::asio::ip::address_v4::loopback();
        boost::asio::ip::tcp::endpoint peer(loopback, 4096);
        socket.connect(peer);

        sleep(1);
        barrier.wait();

        boost::asio::streambuf stream;
        std::ostream os(&stream);
        for (int i = COUNT; i --> 0; ) {
            os << ":1\r\n" ":2\r\n" "+3.14\r\n";
            size_t n = socket.send(stream.data());
            stream.consume(n);
        }
        socket.shutdown(TcpSocket::shutdown_both);
        std::cout << "Push process completed" << std::endl;
    };

    Timer tm;
    std::thread pusherA(push);
    std::thread pusherB(push);
    std::thread pusherC(push);
    std::thread pusherD(push);

    pusherA.join();
    pusherB.join();
    pusherC.join();
    pusherD.join();

    serv->stop();
    std::cout << "TcpServer stopped" << std::endl;

    iothreadA.join();
    iothreadB.join();
    iothreadC.join();
    std::cout << "I/O thread stopped" << std::endl;

    pline->stop();
    std::cout << "Pipeline stopped" << std::endl;

    ioA.stop();
    //ioB.stop();
    //ioC.stop();
    std::cout << "I/O service stopped" << std::endl;

    std::cout << dbcon->idsum << " messages received" << std::endl;

    // Every message is processed here
    double elapsed = tm.elapsed();
    std::cout << "100M sent in " << elapsed << "s" << std::endl;

    return 0;
}
