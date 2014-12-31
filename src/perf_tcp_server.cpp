#include <iostream>
#include <thread>

#include "tcp_server.h"
#include <boost/timer.hpp>

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

int main() {
    std::cout << "Tcp server performance test" << std::endl;

    // Create mock pipeline
    auto dbcon = std::make_shared<DbMock>();
    auto pline = std::make_shared<IngestionPipeline>(dbcon, AKU_LINEAR_BACKOFF);
    pline->start();

    // Run server
    boost::asio::io_service io;
    int port = 4096;
    auto serv = std::make_shared<TcpServer>(&io, port, pline);
    serv->start();

    // Run IO service
    auto iorun = [&]() {
        io.run();
    };

    std::thread iothreadA(iorun);
    //std::thread iothreadB(iorun);

    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Push data to server
    const int COUNT = 10000000;  // 10M
    TcpSocket socket(io);
    auto loopback = boost::asio::ip::address_v4::loopback();
    boost::asio::ip::tcp::endpoint peer(loopback, 4096);
    socket.connect(peer);

    auto push = [&]() {
        boost::asio::streambuf stream;
        std::ostream os(&stream);
        for (int i = COUNT/2; i --> 0; ) {
            os << ":1\r\n" ":2\r\n" "+3.14\r\n";
            size_t n = socket.send(stream.data());
            stream.consume(n);
        }
    };

    boost::timer tm;
    std::thread pusherA(push);
    std::thread pusherB(push);

    pusherA.join();
    pusherB.join();

    pline->stop();
    // Every message is processed here
    double elapsed = tm.elapsed();
    std::cout << "10M sent in " << elapsed << "s" << std::endl;

    io.stop();
    iothreadA.join();
    //iothreadB.join();
}
