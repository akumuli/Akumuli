#include "tcp_server.h"
#include "utility.h"
#include <thread>
#include <boost/function.hpp>

namespace Akumuli {

//                     //
//     Tcp Session     //
//                     //

TcpSession::TcpSession(IOServiceT *io, std::shared_ptr<PipelineSpout> spout)
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
    auto fn = [weak](aku_Status status, uint64_t counter) {
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
    if (!error) {
        try {
            start(buffer, buf_size, pos, nbytes);
            PDU pdu = {
                buffer,
                nbytes,
                pos
            };
            parser_.parse_next(pdu);
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

    } else {
        logger_.error() << error.message();
        parser_.close();
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
                        std::shared_ptr<IngestionPipeline> pipeline )
    : acceptor_(own_io_, EndpointT(boost::asio::ip::tcp::v4(), port))
    , sessions_io_(io)
    , pipeline_(pipeline)
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
    } else {
        logger_.error() << "Acceptor error " << err.message();
    }
    _start();
}

//                    //
//     Tcp Server     //
//                    //

TcpServer::TcpServer(std::shared_ptr<IngestionPipeline> pipeline, int concurrency, int port)
    : pline(pipeline)
    , barrier(concurrency + 1)
    , sig(io, SIGINT)
    , stopped{0}
{
    for(;concurrency --> 0;) {
        iovec.push_back(&io);
    }
    serv = std::make_shared<TcpAcceptor>(iovec, port, pline);
    pline->start();
    serv->start();
}

void TcpServer::start() {

    sig.async_wait(
                // Wait for sigint
                boost::bind(&TcpServer::handle_sigint,
                            shared_from_this(),
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

void TcpServer::handle_sigint(boost::system::error_code err) {
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

void TcpServer::stop() {
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
    } else {
        std::cout << "Already stopped" << std::endl;
    }
}

void TcpServer::wait_for_signal() {
    // TODO: use cond var
    while(!stopped) {
        sleep(1);
    }
}

}

