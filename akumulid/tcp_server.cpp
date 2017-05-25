#include "tcp_server.h"
#include "utility.h"
#include <thread>
#include <atomic>
#include <boost/function.hpp>
#include <boost/exception/diagnostic_information.hpp>

namespace Akumuli {

//                     //
//     Tcp Session     //
//                     //

std::string make_unique_session_name() {
    static std::atomic<int> counter = {0};
    std::stringstream str;
    str << "tcp-session-" << counter.fetch_add(1);
    return str.str();
}

TcpSession::TcpSession(IOServiceT *io, std::shared_ptr<DbSession> spout, bool parallel)
    : parallel_(parallel)
    , io_(io)
    , socket_(*io)
    , strand_(*io)
    , spout_(spout)
    , parser_(spout)
    , logger_(make_unique_session_name(), 10)
{
    logger_.info() << "Session created";
    parser_.start();
}

TcpSession::~TcpSession() {
    logger_.info() << "Session destroyed";
}

SocketT& TcpSession::socket() {
    return socket_;
}

std::tuple<TcpSession::BufferT, size_t> TcpSession::get_next_buffer() {
    Byte *buffer = parser_.get_next_buffer();
    return std::make_tuple(buffer, BUFFER_SIZE);
}

void TcpSession::start() {
    BufferT buf;
    size_t buf_size;
    std::tie(buf, buf_size) = get_next_buffer();
    if (parallel_) {
        socket_.async_read_some(
                boost::asio::buffer(buf, buf_size),
                strand_.wrap(
                    boost::bind(&TcpSession::handle_read,
                                shared_from_this(),
                                buf,
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred)));
    } else {
        // Strand is not used here
        socket_.async_read_some(
                boost::asio::buffer(buf, buf_size),
                boost::bind(&TcpSession::handle_read,
                            shared_from_this(),
                            buf,
                            boost::asio::placeholders::error,
                            boost::asio::placeholders::bytes_transferred));
    }
}

ErrorCallback TcpSession::get_error_cb() {
    logger_.info() << "Creating error handler for session";
    auto self = shared_from_this();
    auto weak = std::weak_ptr<TcpSession>(self);
    auto fn = [weak](aku_Status status, u64) {
        auto session = weak.lock();
        if (session) {
            const char* msg = aku_error_message(status);
            session->logger_.trace() << msg;
            boost::asio::streambuf stream;
            std::ostream os(&stream);
            os << "-DB " << msg << "\r\n";
            boost::asio::async_write(session->socket_,
                                     stream,
                                     boost::bind(&TcpSession::handle_write_error,
                                                 session,
                                                 boost::asio::placeholders::error));
        }
    };
    return ErrorCallback(fn);
}

void TcpSession::handle_read(BufferT buffer,
                             boost::system::error_code error,
                             size_t nbytes) {
    if (error) {
        logger_.error() << error.message();
        parser_.close();
    } else {
        try {
            parser_.parse_next(buffer, static_cast<u32>(nbytes));
            start();
        } catch (StreamError const& resp_err) {
            // This error is related to client so we need to send it back
            logger_.error() << resp_err.what();
            boost::asio::streambuf stream;
            std::ostream os(&stream);
            os << "-PARSER " << resp_err.what() << "\r\n";
            boost::asio::async_write(socket_, stream,
                                     boost::bind(&TcpSession::handle_write_error,
                                                 shared_from_this(),
                                                 boost::asio::placeholders::error)
                                     );
        } catch (DatabaseError const& dberr) {
            // Database error
            logger_.error() << boost::current_exception_diagnostic_information();
            boost::asio::streambuf stream;
            std::ostream os(&stream);
            os << "-DB " << dberr.what() << "\r\n";
            boost::asio::async_write(socket_, stream,
                                     boost::bind(&TcpSession::handle_write_error,
                                                 shared_from_this(),
                                                 boost::asio::placeholders::error)
                                     );
        } catch (...) {
            // Unexpected error
            logger_.error() << boost::current_exception_diagnostic_information();
            boost::asio::streambuf stream;
            std::ostream os(&stream);
            os << "-ERR " << boost::current_exception_diagnostic_information() << "\r\n";
            boost::asio::async_write(socket_, stream,
                                     boost::bind(&TcpSession::handle_write_error,
                                                 shared_from_this(),
                                                 boost::asio::placeholders::error)
                                     );
        }
    }
}

void TcpSession::handle_write_error(boost::system::error_code error) {
    if (!error) {
        logger_.info() << "Clean shutdown";
        socket_.shutdown(SocketT::shutdown_both);
    } else {
        logger_.error() << "Error sending error message to client";
        logger_.error() << error.message();
        parser_.close();
    }
}

//                      //
//     Tcp Acceptor     //
//                      //

TcpAcceptor::TcpAcceptor(// Server parameters
                        std::vector<IOServiceT *> io, int port,
                        // Storage & pipeline
                        std::shared_ptr<DbConnection> connection , bool parallel)
    : parallel_(parallel)
    , acceptor_(own_io_, EndpointT(boost::asio::ip::tcp::v4(), static_cast<u16>(port)))
    , sessions_io_(io)
    , connection_(connection)
    , io_index_{0}
    , start_barrier_(2)
    , stop_barrier_(2)
    , logger_("tcp-acceptor", 10)
{
    logger_.info() << "Server created!";
    logger_.info() << "Port: " << port;

    // Blocking I/O services
    for (auto io: sessions_io_) {
        sessions_work_.emplace_back(*io);
    }
}

TcpAcceptor::~TcpAcceptor() {
    logger_.info() << "TCP acceptor destroyed";
}

void TcpAcceptor::start() {
    WorkT work(own_io_);

    // Run detached thread for accepts
    auto self = shared_from_this();
    std::thread accept_thread([self]() {
#ifdef __gnu_linux__
        // Name the thread
        auto thread = pthread_self();
        pthread_setname_np(thread, "TCP-accept");
#endif
        self->logger_.info() << "Starting acceptor worker thread";
        self->start_barrier_.wait();
        self->logger_.info() << "Acceptor worker thread have started";

        try {
            self->own_io_.run();
        } catch (...) {
            self->logger_.error() << "Error in acceptor worker thread: " << boost::current_exception_diagnostic_information();
            throw;
        }

        self->logger_.info() << "Stopping acceptor worker thread";
        self->stop_barrier_.wait();
        self->logger_.info() << "Acceptor worker thread have stopped";
    });
    accept_thread.detach();

    start_barrier_.wait();

    logger_.info() << "Start listening";
    _start();
}

void TcpAcceptor::_run_one() {
    own_io_.run_one();
}

void TcpAcceptor::_start() {
    std::shared_ptr<TcpSession> session;
    auto con = connection_.lock();
    if (con) {
        auto spout = con->create_session();
        session = std::make_shared<TcpSession>(sessions_io_.at(io_index_++ % sessions_io_.size()), std::move(spout), parallel_);
    } else {
        logger_.error() << "Database was already closed";
    }
    // attach session to spout
    // run session
    acceptor_.async_accept(
                session->socket(),
                boost::bind(&TcpAcceptor::handle_accept,
                            shared_from_this(),
                            session,
                            boost::asio::placeholders::error)
                );
}

void TcpAcceptor::stop() {
    logger_.info() << "Stopping acceptor";
    acceptor_.cancel();
    own_io_.stop();
    sessions_work_.clear();
    logger_.info() << "Trying to stop acceptor";
    stop_barrier_.wait();
    logger_.info() << "Acceptor successfully stopped";
}

void TcpAcceptor::_stop() {
    logger_.info() << "Stopping acceptor (test runner)";
    acceptor_.close();
    own_io_.stop();
    sessions_work_.clear();
}

void TcpAcceptor::handle_accept(std::shared_ptr<TcpSession> session, boost::system::error_code err) {
    if (AKU_LIKELY(!err)) {
        session->start();
        _start();
    } else {
        logger_.error() << "Acceptor error " << err.message();
    }
}

//                    //
//     Tcp Server     //
//                    //

TcpServer::TcpServer(std::shared_ptr<DbConnection> connection, int concurrency, int port, TcpServer::Mode mode)
    : connection_(connection)
    , barrier(static_cast<u32>(concurrency) + 1)
    , stopped{0}
    , logger_("tcp-server", 32)
{
    logger_.info() << "TCP server created, concurrency: " << concurrency;
    if (mode == Mode::EVENT_LOOP_PER_THREAD) {
        for(int i = 0; i < concurrency; i++) {
            IOPtr ptr = IOPtr(new IOServiceT(1));
            iovec.push_back(ptr.get());
            ios_.push_back(std::move(ptr));
        }
    } else {
        IOPtr ptr = IOPtr(new IOServiceT(static_cast<size_t>(concurrency)));
        iovec.push_back(ptr.get());
        ios_.push_back(std::move(ptr));
    }
    bool parallel = mode == Mode::SHARED_EVENT_LOOP;
    auto con = connection_.lock();
    if (con) {
        serv = std::make_shared<TcpAcceptor>(iovec, port, con, parallel);
        serv->start();
    } else {
        logger_.error() << "Can't start TCP server, database closed";
        std::runtime_error err("DB connection closed");
        BOOST_THROW_EXCEPTION(err);
    }
}

TcpServer::~TcpServer() {
    logger_.info() << "TCP server destroyed";
}

void TcpServer::start(SignalHandler* sig, int id) {

    auto self = shared_from_this();
    sig->add_handler(boost::bind(&TcpServer::stop, self), id);

    auto iorun = [self](IOServiceT& io, int cnt) {
        auto fn = [self, &io, cnt]() {
#ifdef __gnu_linux__
            // Name the thread
            auto thread = pthread_self();
            pthread_setname_np(thread, "TCP-worker");
#endif
            Logger logger("tcp-server-worker", 10);
            try {
                logger.info() << "Event loop " << cnt << " started";
                io.run();
                logger.info() << "Event loop " << cnt << " stopped";
                self->barrier.wait();
            } catch (RESPError const& e) {
                logger.error() << e.what();
                throw;
            } catch (...) {
                logger.error() << "Error in event loop " << cnt << ": " << boost::current_exception_diagnostic_information();
                throw;
            }
            logger.info() << "Worker thread " << cnt << " stopped";
        };
        return fn;
    };

    int cnt = 0;
    for (auto io: iovec) {
        std::thread iothread(iorun(*io, cnt++));
        iothread.detach();
    }
}

void TcpServer::stop() {
    if (stopped++ == 0) {
        serv->stop();
        logger_.info() << "TcpServer stopped";

        for(auto& svc: ios_) {
            svc->stop();
            logger_.info() << "I/O service stopped";
        }

        barrier.wait();
        logger_.info() << "I/O threads stopped";
    }
}

struct TcpServerBuilder {

    TcpServerBuilder() {
        ServerFactory::instance().register_type("TCP", *this);
    }

    std::shared_ptr<Server> operator () (std::shared_ptr<DbConnection> con,
                                         std::shared_ptr<ReadOperationBuilder>,
                                         ServerSettings& settings) {
        if ((int)std::thread::hardware_concurrency() <= 4) {
            settings.nworkers = 1;
        }
        else {
            settings.nworkers =  (int)std::thread::hardware_concurrency() * 0.75;
        }
        return std::make_shared<TcpServer>(con, settings.nworkers, settings.port);
    }
};

static TcpServerBuilder reg_type;

}

