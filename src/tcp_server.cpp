#include "tcp_server.h"
#include "utility.h"
#include <thread>

namespace Akumuli {

//                     //
//     Tcp Session     //
//                     //

TcpSession::TcpSession(IOService *io, std::shared_ptr<PipelineSpout> spout)
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

TcpSocket& TcpSession::socket() {
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

std::shared_ptr<Byte> TcpSession::NO_BUFFER = std::shared_ptr<Byte>();

void TcpSession::handle_read(BufferT buffer,
                             size_t pos,
                             size_t buf_size,
                             boost::system::error_code error,
                             size_t nbytes) {
    if (!error) {
        start(buffer, buf_size, pos, nbytes);
        PDU pdu = {
            buffer,
            nbytes,
            pos
        };
        // TODO: remove
        parser_.parse_next(pdu);
    } else {
        logger_.error() << error.message();
        parser_.close();
    }
}

//                    //
//     Tcp Server     //
//                    //

TcpServer::TcpServer(// Server parameters
                        std::vector<IOService *> io, int port,
                     // Storage & pipeline
                        std::shared_ptr<IngestionPipeline> pipeline
                     )
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

void TcpServer::start() {
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

void TcpServer::_run_one() {
    own_io_.run_one();
}

void TcpServer::_start() {
    std::shared_ptr<TcpSession> session;
    session.reset(new TcpSession(sessions_io_.at(io_index_++ % sessions_io_.size()), pipeline_->make_spout()));
    acceptor_.async_accept(
                session->socket(),
                boost::bind(&TcpServer::handle_accept,
                            shared_from_this(),
                            session,
                            boost::asio::placeholders::error)
                );
}

void TcpServer::stop() {
    logger_.error() << "Stopping acceptor";
    acceptor_.close();
    own_io_.stop();
    sessions_work_.clear();
    logger_.info() << "Trying to stop acceptor";
    stop_barrier_.wait();
    logger_.info() << "Acceptor successfully stopped";
}

void TcpServer::_stop() {
    logger_.error() << "Stopping acceptor";
    acceptor_.close();
    own_io_.stop();
    sessions_work_.clear();
}

void TcpServer::handle_accept(std::shared_ptr<TcpSession> session, boost::system::error_code err) {
    if (AKU_LIKELY(!err)) {
        session->start(TcpSession::NO_BUFFER, 0u, 0u, 0u);
    } else {
        logger_.error() << "Acceptor error " << err.message();
    }
    _start();
}

}

