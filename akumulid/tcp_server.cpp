#include "tcp_server.h"
#include "utility.h"
#include <thread>
#include <boost/function.hpp>

namespace Akumuli {

//                     //
//     Tcp Session     //
//                     //

TcpSession::TcpSession(IOServiceT *io, std::shared_ptr<AkumuliSession> spout)
    : io_(io)
    , socket_(*io)
    , strand_(*io)
    , spout_(spout)
    , parser_(spout)
    , logger_("tcp-session", 10)
{
    logger_.info() << "Session created";
    parser_.start();
}

SocketT& TcpSession::socket() {
    return socket_;
}

std::tuple<TcpSession::BufferT, size_t, size_t> TcpSession::get_next_buffer(BufferT prev_buf,
                                                                            size_t size,
                                                                            size_t pos,
                                                                            size_t bytes_read)
{
    Byte *buffer = (Byte*)malloc(BUFFER_SIZE);
    auto deleter = [](Byte* p) {
        free((void*)p);
    };
    std::shared_ptr<Byte> bufptr(buffer, deleter);
    return std::make_tuple(bufptr, BUFFER_SIZE, 0u);
}

void TcpSession::start(BufferT buf, size_t buf_size, size_t pos, size_t bytes_read) {
    std::tie(buf, buf_size, pos) = get_next_buffer(buf, buf_size, pos, bytes_read);
    socket_.async_read_some(
                boost::asio::buffer(buf.get() + pos, buf_size - pos),
                strand_.wrap(
                    boost::bind(&TcpSession::handle_read,
                                shared_from_this(),
                                buf,
                                pos,
                                buf_size,
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred)
                ));
}

PipelineErrorCb TcpSession::get_error_cb() {
    logger_.info() << "Creating error handler for session";
    auto self = shared_from_this();
    auto weak = std::weak_ptr<TcpSession>(self);
    auto fn = [weak](aku_Status status, u64 counter) {
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
    return PipelineErrorCb(fn);
}

std::shared_ptr<Byte> TcpSession::NO_BUFFER = std::shared_ptr<Byte>();

void TcpSession::handle_read(BufferT buffer,
                             size_t pos,
                             size_t buf_size,
                             boost::system::error_code error,
                             size_t nbytes) {
    if (error) {
        logger_.error() << error.message();
        parser_.close();
    } else {
        try {
            PDU pdu = {
                buffer,
                nbytes,
                pos
            };
            parser_.parse_next(pdu);
            start(buffer, buf_size, pos, nbytes);
        } catch (RESPError const& resp_err) {
            // This error is related to client so we need to send it back
            logger_.error() << resp_err.what();
            logger_.error() << resp_err.get_bottom_line();
            boost::asio::streambuf stream;
            std::ostream os(&stream);
            os << "-PARSER " << resp_err.what() << "\r\n";
            os << "-PARSER " << resp_err.get_bottom_line() << "\r\n";
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
                        std::shared_ptr<AkumuliConnection> connection )
    : acceptor_(own_io_, EndpointT(boost::asio::ip::tcp::v4(), port))
    , sessions_io_(io)
    , pipeline_(connection)
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

void TcpAcceptor::start() {
    WorkT work(own_io_);

    // Run detached thread for accepts
    auto self = shared_from_this();
    std::thread accept_thread([self]() {

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
    auto spout = pipeline_->make_spout();
    session.reset(new TcpSession(sessions_io_.at(io_index_++ % sessions_io_.size()), spout));
    // attach session to spout
    spout->set_error_cb(session->get_error_cb());
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
    logger_.error() << "Stopping acceptor";
    acceptor_.close();
    own_io_.stop();
    sessions_work_.clear();
    logger_.info() << "Trying to stop acceptor";
    stop_barrier_.wait();
    logger_.info() << "Acceptor successfully stopped";
}

void TcpAcceptor::_stop() {
    logger_.error() << "Stopping acceptor";
    acceptor_.close();
    own_io_.stop();
    sessions_work_.clear();
}

void TcpAcceptor::handle_accept(std::shared_ptr<TcpSession> session, boost::system::error_code err) {
    if (AKU_LIKELY(!err)) {
        session->start(TcpSession::NO_BUFFER, 0u, 0u, 0u);
        _start();
    } else {
        logger_.error() << "Acceptor error " << err.message();
    }
}

//                    //
//     Tcp Server     //
//                    //

TcpServer::TcpServer(std::shared_ptr<AkumuliConnection> connection, int concurrency, int port)
    : connection_(connection)
    , barrier(concurrency)
    , stopped{0}
    , logger_("tcp-server", 32)
{
    for(;concurrency --> 0;) {
        iovec.push_back(&io);
    }
    serv = std::make_shared<TcpAcceptor>(iovec, port, connection_);
    connection_->start();
    serv->start();
}

void TcpServer::start(SignalHandler* sig, int id) {

    auto self = shared_from_this();
    sig->add_handler(boost::bind(&TcpServer::stop, std::move(self)), id);

    auto logger = &logger_;
    auto iorun = [logger](IOServiceT& io, boost::barrier& bar) {
        auto fn = [&]() {
            try {
                io.run();
                bar.wait();
            } catch (RESPError const& e) {
                logger->error() << e.what();
                logger->error() << e.get_bottom_line();
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

void TcpServer::stop() {
    if (stopped++ == 0) {
        serv->stop();
        logger_.info() << "TcpServer stopped";

        // No need to joint I/O threads, just wait until they completes.
        barrier.wait();
        logger_.info() << "I/O threads stopped";

        connection_->stop();
        logger_.info() << "Pipeline stopped";

        for (auto io: iovec) {
            io->stop();
        }
        logger_.info() << "I/O service stopped";
    }
}

struct TcpServerBuilder {

    TcpServerBuilder() {
        ServerFactory::instance().register_type("TCP", *this);
    }

    std::shared_ptr<Server> operator () (std::shared_ptr<IngestionPipeline> pipeline,
                                         std::shared_ptr<ReadOperationBuilder>,
                                         const ServerSettings& settings) {
        return std::make_shared<TcpServer>(pipeline, settings.nworkers, settings.port);
    }
};

static TcpServerBuilder reg_type;

}

