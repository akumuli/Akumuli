#include "tcp_server.h"
#include "utility.h"

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
                        IOService *io, int port,
                     // Storage & pipeline
                        std::shared_ptr<IngestionPipeline> pipeline
                     )
    : io_(io)
    , acceptor_(*io_, EndpointT(boost::asio::ip::tcp::v4(), port))
    , pipeline_(pipeline)
{
    logger_.info() << "Server created!";
    logger_.info() << "Port: " << port;
}

void TcpServer::start() {
    logger_.info() << "Start listening";
    _start();
}

void TcpServer::_start() {
    std::shared_ptr<TcpSession> session;
    session.reset(new TcpSession(io_, pipeline_->make_spout()));
    acceptor_.async_accept(
                session->socket(),
                boost::bind(&TcpServer::handle_accept,
                            shared_from_this(),
                            session,
                            boost::asio::placeholders::error)
                );
}

void TcpServer::stop() {
    acceptor_.close();
}

void TcpServer::handle_accept(std::shared_ptr<TcpSession> session, boost::system::error_code err) {
    if (AKU_LIKELY(!err)) {
        session->start(TcpSession::NO_BUFFER, 0u, 0u, 0u);
    } else {
        logger_.error() << "Acceptor error " << err.message();
    }
    start();
}

}

