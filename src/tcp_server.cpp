#include "tcp_server.h"
#include "utility.h"
#include <thread>

namespace Akumuli {

static Logger logger_ = Logger("tcp-server", 64);

//                     //
//     Tcp Session     //
//                     //

TcpSession::TcpSession(IOService *io, std::shared_ptr<PipelineSpout> spout)
    : io_(io)
    , socket_(*io)
    , strand_(*io)
    , spout_(spout)
    , parser_(spout)
{
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
        parser_.parse_next(pdu);
    } else {
        logger_.error() << error.message();
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
    , acceptor_state_(UNDEFINED)
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

        self->acceptor_state_ = STARTED;
        self->cond_.notify_one();

        try {
            self->own_io_.run();
        } catch (...) {
            logger_.error() << "Error in acceptor worker thread: " << boost::current_exception_diagnostic_information();
            throw;
        }
        logger_.info() << "Acceptor worker thread stopped.";

        self->acceptor_state_ = STOPPED;
        self->cond_.notify_one();
    });
    accept_thread.detach();

    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait(lock);
    if (acceptor_state_ != STARTED) {
        logger_.error() << "Invalid acceptor state";
    } else {
        logger_.info() << "Acceptor worker thread started.";
    }

    logger_.info() << "Start listening";
    _start();
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
    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait(lock);
    if (acceptor_state_ != STOPPED) {
        logger_.error() << "Invalid acceptor state after stopping attempt";
    }
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

