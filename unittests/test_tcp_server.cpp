#include <iostream>
#include <memory>
#include <thread>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "tcp_server.h"
#include "logger.h"

using namespace Akumuli;


static Logger logger_ = Logger("tcp-server-test");
typedef std::tuple<aku_ParamId, aku_Timestamp, double> ValueT;


struct SessionMock : DbSession {
    std::vector<ValueT>& results;

    SessionMock(std::vector<ValueT>& results)
        : results(results) {}

    virtual aku_Status write(const aku_Sample &sample) override {
        logger_.trace() << "write_double(" << sample.paramid << ", " << sample.timestamp << ", " << sample.payload.float64 << ")";
        results.push_back(std::make_tuple(sample.paramid, sample.timestamp, sample.payload.float64));
        return AKU_SUCCESS;
    }

    virtual std::shared_ptr<DbCursor> query(std::string) override {
        throw "not implemented";
    }

    virtual std::shared_ptr<DbCursor> suggest(std::string) override {
        throw "not implemented";
    }

    virtual std::shared_ptr<DbCursor> search(std::string) override {
        throw "not implemented";
    }

    virtual int param_id_to_series(aku_ParamId id, char* buf, size_t sz) override {
        auto str = std::to_string(id);
        assert(str.size() <= sz);
        memcpy(buf, str.data(), str.size());
        return static_cast<int>(str.size());
    }

    virtual aku_Status series_to_param_id(const char* begin, size_t sz, aku_Sample* sample) override {
        std::string num(begin, begin + sz);
        sample->paramid = boost::lexical_cast<u64>(num);
        return AKU_SUCCESS;
    }

    virtual int name_to_param_id_list(const char* begin, const char* end, aku_ParamId* ids, u32 cap) override {
        auto nelem = std::count(begin, end, ':') + 1;
        if (nelem > cap) {
            return -1*static_cast<int>(nelem);
        }
        const char* it_begin = begin;
        const char* it_end = begin;
        for (int i = 0; i < nelem; i++) {
            //move it_end
            while(*it_end != ':' && it_end != end) {
                it_end++;
            }
            std::string val(it_begin, it_end);
            ids[i] = boost::lexical_cast<u64>(val);
        }
        return static_cast<int>(nelem);
    }
};


struct ConnectionMock : DbConnection {
    std::vector<ValueT> results;

    virtual std::string get_all_stats() override { throw "not impelemnted"; }

    virtual std::shared_ptr<DbSession> create_session() override {
        return std::make_shared<SessionMock>(results);
    }
};


template<aku_Status ERR>
struct DbSessionErrorMock : DbSession {
    aku_Status err = ERR;

    virtual aku_Status write(const aku_Sample&) override {
        return err;
    }
    virtual std::shared_ptr<DbCursor> query(std::string) override {
        throw "not implemented";
    }
    virtual std::shared_ptr<DbCursor> suggest(std::string) override {
        throw "not implemented";
    }
    virtual std::shared_ptr<DbCursor> search(std::string) override {
        throw "not implemented";
    }
    virtual int param_id_to_series(aku_ParamId id, char* buf, size_t sz) override {
        auto str = std::to_string(id);
        assert(str.size() <= sz);
        memcpy(buf, str.data(), str.size());
        return static_cast<int>(str.size());
    }
    virtual aku_Status series_to_param_id(const char* begin, size_t sz, aku_Sample* sample) override {
        std::string num(begin, begin + sz);
        sample->paramid = boost::lexical_cast<u64>(num);
        return AKU_SUCCESS;
    }
    virtual int name_to_param_id_list(const char* begin, const char* end, aku_ParamId* ids, u32 cap) override {
        auto nelem = std::count(begin, end, '|') + 1;
        if (nelem > cap) {
            return -1*static_cast<int>(nelem);
        }
        const char* it_begin = begin;
        const char* it_end = begin;
        for (int i = 0; i < nelem; i++) {
            //move it_end
            while(*it_end != '|' && it_end != end) {
                it_end++;
            }
            std::string val(it_begin, it_end);
            ids[i] = boost::lexical_cast<u64>(val);
        }
        return static_cast<int>(nelem);
    }
};

template<aku_Status ERR>
struct DbConnectionErrorMock : DbConnection {
    virtual std::string get_all_stats() override { throw "not impelemented"; }

    virtual std::shared_ptr<DbSession> create_session() override {
        return std::make_shared<DbSessionErrorMock<ERR>>();
    }
};

const int PORT = 14096;

template<class Mock>
struct TCPServerTestSuite {
    std::shared_ptr<Mock>               dbcon;
    IOServiceT                          io;
    std::shared_ptr<TcpAcceptor>        serv;

    TCPServerTestSuite() {
        // Create mock pipeline
        dbcon = std::make_shared<Mock>();

        // Run server
        std::vector<IOServiceT*> iovec = { &io };
        serv = std::make_shared<TcpAcceptor>(iovec, PORT, dbcon);

        // Start reading but don't start iorun thread
        serv->_start();
    }

    ~TCPServerTestSuite() {
        logger_.info() << "Clean up suite resources";
        if (serv) {
            serv->_stop();
        }
    }

    template<class Fn>
    void run(Fn const& fn) {
        // Connect to server
        SocketT socket(io);
        auto loopback = boost::asio::ip::address_v4::loopback();
        boost::asio::ip::tcp::endpoint peer(loopback, PORT);
        socket.connect(peer);
        serv->_run_one();  // run handle_accept one time

        // Run tests
        fn(socket);
    }
};


BOOST_AUTO_TEST_CASE(Test_tcp_server_loopback_1) {

    TCPServerTestSuite<ConnectionMock> suite;

    suite.run([&](SocketT& socket) {
        boost::asio::streambuf stream;
        std::ostream os(&stream);
        os << "+1\r\n" << ":2\r\n" << "+3.14\r\n";

        boost::asio::write(socket, stream);

        // TCPSession.handle_read
        suite.io.run_one();

        // Check
        if (suite.dbcon->results.size() != 1) {
            logger_.error() << "Error detected";
            BOOST_REQUIRE_EQUAL(suite.dbcon->results.size(), 1);
        }
        aku_ParamId id;
        aku_Timestamp ts;
        double value;
        std::tie(id, ts, value) = suite.dbcon->results.at(0);
        BOOST_REQUIRE_EQUAL(id, 1);
        BOOST_REQUIRE_EQUAL(ts, 2);
        BOOST_REQUIRE_CLOSE_FRACTION(value, 3.14, 0.00001);
    });
}


BOOST_AUTO_TEST_CASE(Test_tcp_server_loopback_2) {

    TCPServerTestSuite<ConnectionMock> suite;

    suite.run([&](SocketT& socket) {
        boost::asio::streambuf stream;
        std::ostream os(&stream);
        os << "+1\r\n" << ":2\r\n";
        size_t n = boost::asio::write(socket, stream);
        stream.consume(n);

        // Process first part of the message
        suite.io.run_one();

        os << "+3.14\r\n";
        n = boost::asio::write(socket, stream);
        // Process last
        suite.io.run_one();

        // Check
        BOOST_REQUIRE_EQUAL(suite.dbcon->results.size(), 1);
        aku_ParamId id;
        aku_Timestamp ts;
        double value;
        std::tie(id, ts, value) = suite.dbcon->results.at(0);
        BOOST_REQUIRE_EQUAL(id, 1);
        BOOST_REQUIRE_EQUAL(ts, 2);
        BOOST_REQUIRE_CLOSE_FRACTION(value, 3.14, 0.00001);
    });
}


BOOST_AUTO_TEST_CASE(Test_tcp_server_loopback_3) {

    TCPServerTestSuite<ConnectionMock> suite;

    suite.run([&](SocketT& socket) {
        boost::asio::streambuf stream;
        std::ostream os(&stream);

        // Fist message
        os << "+1\r\n" << ":2\r\n" << "+3.14\r\n";
        size_t n = boost::asio::write(socket, stream);
        stream.consume(n);

        // Process first part of the message
        suite.io.run_one();

        // Second message
        os << "+3\r\n" << ":4\r\n" << "+1.61\r\n";
        n = boost::asio::write(socket, stream);

        // Process last
        suite.io.run_one();

        // Check
        BOOST_REQUIRE_EQUAL(suite.dbcon->results.size(), 2);
        aku_ParamId id;
        aku_Timestamp ts;
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


BOOST_AUTO_TEST_CASE(Test_tcp_server_parser_error_handling) {

    TCPServerTestSuite<ConnectionMock> suite;

    suite.run([&](SocketT& socket) {
        boost::asio::streambuf stream;
        std::ostream os(&stream);
        os << "+1\r\n:E\r\n+3.14\r\n";
        //      error ^

        boost::asio::streambuf instream;
        std::istream is(&instream);
        boost::asio::write(socket, stream);

        bool handler_called = false;
        auto cb = [&](boost::system::error_code err) {
            BOOST_REQUIRE(err == boost::asio::error::eof);
            handler_called = true;
        };
        boost::asio::async_read(socket, instream, boost::bind<void>(cb, boost::asio::placeholders::error));

        // TCPSession.handle_read
        suite.io.run_one();  // run message handler (should send error back to us)
        while(!handler_called) {
            suite.io.run_one();  // run error handler
        }

        BOOST_REQUIRE(handler_called);
        // Check
        BOOST_REQUIRE_EQUAL(suite.dbcon->results.size(), 0);
        char buffer[0x1000];
        is.getline(buffer, 0x1000);
        BOOST_REQUIRE_EQUAL(std::string(buffer, buffer + 7), "-PARSER");
    });
}


BOOST_AUTO_TEST_CASE(Test_tcp_server_backend_error_handling) {

    TCPServerTestSuite<DbConnectionErrorMock<AKU_EBAD_DATA>> suite;

    suite.run([&](SocketT& socket) {
        boost::asio::streambuf stream;
        std::ostream os(&stream);
        os << "+1\r\n:2\r\n+3.14\r\n";

        boost::asio::streambuf instream;
        std::istream is(&instream);
        boost::asio::write(socket, stream);

        bool handler_called = false;
        auto cb = [&](boost::system::error_code err) {
            BOOST_REQUIRE(err == boost::asio::error::eof);
            handler_called = true;
        };
        boost::asio::async_read(socket, instream, boost::bind<void>(cb, boost::asio::placeholders::error));

        // TCPSession.handle_read
        suite.io.run_one();  // run message handler (should send error back to us)
        while(!handler_called) {
            suite.io.run_one();  // run error handler
        }

        BOOST_REQUIRE(handler_called);
        // Check
        char buffer[0x1000];
        is.getline(buffer, 0x1000);
        BOOST_REQUIRE_EQUAL(std::string(buffer, buffer + 3), "-DB");
    });
}

