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


struct TCPServerTestSuite {
    std::shared_ptr<DbMock>             dbcon;
    std::shared_ptr<IngestionPipeline>  pline;
    IOService                           io;
    std::shared_ptr<TcpServer>          serv;

    TCPServerTestSuite() {
        // Create mock pipeline
        dbcon = std::make_shared<DbMock>();
        pline = std::make_shared<IngestionPipeline>(dbcon, AKU_LINEAR_BACKOFF);
        pline->start();

        // Run server
        int port = 4096;
        std::vector<IOService*> iovec = { &io };
        serv = std::make_shared<TcpServer>(iovec, port, pline);
        serv->start();

    }

    ~TCPServerTestSuite() {
        serv->stop();
    }

    template<class Fn>
    void run(Fn const& fn) {
        // Connect to server
        TcpSocket socket(io);
        auto loopback = boost::asio::ip::address_v4::loopback();
        boost::asio::ip::tcp::endpoint peer(loopback, 4096);
        socket.connect(peer);

        // Run tests
        fn(socket);
    }
};

BOOST_AUTO_TEST_CASE(Test_tcp_server_loopback_1) {

    TCPServerTestSuite suite;

    suite.run([&](TcpSocket& socket) {
        boost::asio::streambuf stream;
        std::ostream os(&stream);
        os << ":1\r\n" << ":2\r\n" << "+3.14\r\n";
        socket.send(stream.data());

        // TCPSession.handle_read
        suite.io.run_one();
        suite.pline->stop();

        // Check
        BOOST_REQUIRE_EQUAL(suite.dbcon->results.size(), 1);
        aku_ParamId id;
        aku_TimeStamp ts;
        double value;
        std::tie(id, ts, value) = suite.dbcon->results.at(0);
        BOOST_REQUIRE_EQUAL(id, 1);
        BOOST_REQUIRE_EQUAL(ts, 2);
        BOOST_REQUIRE_CLOSE_FRACTION(value, 3.14, 0.00001);
    });
}

BOOST_AUTO_TEST_CASE(Test_tcp_server_loopback_2) {

    TCPServerTestSuite suite;

    suite.run([&](TcpSocket& socket) {
        boost::asio::streambuf stream;
        std::ostream os(&stream);
        os << ":1\r\n" << ":2\r\n";
        size_t n = socket.send(stream.data());
        stream.consume(n);

        // Process first part of the message
        suite.io.run_one();

        os << "+3.14\r\n";
        n = socket.send(stream.data());
        // Process last
        suite.io.run_one();

        suite.pline->stop();

        // Check
        BOOST_REQUIRE_EQUAL(suite.dbcon->results.size(), 1);
        aku_ParamId id;
        aku_TimeStamp ts;
        double value;
        std::tie(id, ts, value) = suite.dbcon->results.at(0);
        BOOST_REQUIRE_EQUAL(id, 1);
        BOOST_REQUIRE_EQUAL(ts, 2);
        BOOST_REQUIRE_CLOSE_FRACTION(value, 3.14, 0.00001);
    });
}


BOOST_AUTO_TEST_CASE(Test_tcp_server_loopback_3) {

    TCPServerTestSuite suite;

    suite.run([&](TcpSocket& socket) {
        boost::asio::streambuf stream;
        std::ostream os(&stream);

        // Fist message
        os << ":1\r\n" << ":2\r\n" << "+3.14\r\n";
        size_t n = socket.send(stream.data());
        stream.consume(n);

        // Process first part of the message
        suite.io.run_one();

        // Second message
        os << ":3\r\n" << ":4\r\n" << "+1.61\r\n";
        n = socket.send(stream.data());
        // Process last
        suite.io.run_one();

        suite.pline->stop();

        // Check
        BOOST_REQUIRE_EQUAL(suite.dbcon->results.size(), 2);
        aku_ParamId id;
        aku_TimeStamp ts;
        double value;

        // First message
        std::tie(id, ts, value) = suite.dbcon->results.at(0);
        BOOST_REQUIRE_EQUAL(id, 1);
        BOOST_REQUIRE_EQUAL(ts, 2);
        BOOST_REQUIRE_CLOSE_FRACTION(value, 3.14, 0.00001);

        // Second message
        std::tie(id, ts, value) = suite.dbcon->results.at(1);
        BOOST_REQUIRE_EQUAL(id, 3);
        BOOST_REQUIRE_EQUAL(ts, 4);
        BOOST_REQUIRE_CLOSE_FRACTION(value, 1.61, 0.00001);
    });
}
