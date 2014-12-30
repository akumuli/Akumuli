#include <iostream>
#include <memory>
#include <thread>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include "tcp_server.h"

using namespace Akumuli;

struct DbMock : DbConnection {
    typedef std::tuple<aku_ParamId, aku_TimeStamp, double> ValueT;
    std::vector<ValueT> results;

    void write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
        results.push_back(std::make_tuple(param, ts, data));
    }
};

BOOST_AUTO_TEST_CASE(Test_tcp_server_loopback_1) {

    // Create mock pipeline
    auto dbcon = std::make_shared<DbMock>();
    auto pline = std::make_shared<IngestionPipeline>(dbcon, AKU_LINEAR_BACKOFF);

    // Run server
    boost::asio::io_service io;
    auto io_run = [&]() {
        io.run();
    };
    std::thread worker(io_run);
    int port = 4096;
    TcpServer serv(&io, port, pline);
    serv.start();

    // Connect to server
    boost::asio::ip::tcp::socket socket(io);
    auto loopback = boost::asio::ip::address_v4::loopback();
    boost::asio::ip::tcp::endpoint peer(loopback, 4096);
    socket.connect(peer);

    boost::asio::streambuf stream;
    std::ostream os(&stream);
    os << ":1\r\n" << ":2\r\n" << "+3.14\r\n";
    socket.send(stream.data());

    // Check
    BOOST_REQUIRE_EQUAL(dbcon->results.size(), 1);
}

