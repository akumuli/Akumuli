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
    , spout_(spout)
    , parser_(spout)
{
    parser_.start();
}

TcpSocket& TcpSession::socket() {
    return socket_;
}

void TcpSession::start() {
    auto bufptr = get_next_buffer();
    socket_.async_read_some(
                boost::asio::buffer(bufptr.get(), BUFFER_SIZE),
                boost::bind(&TcpSession::handle_read,
                            shared_from_this(),
                            bufptr,
                            boost::asio::placeholders::error,
                            boost::asio::placeholders::bytes_transferred)
                );
}

void TcpSession::handle_read(std::shared_ptr<Byte> buffer,
                             boost::system::error_code error,
                             size_t nbytes) {
    if (!error) {
        start();
        PDU pdu = {
            buffer,
            nbytes,
            0u
        };
        parser_.parse_next(pdu);
        // TODO: reuse buffer if it is almost empty
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
    , acceptor_(*io_, ip::tcp::endpoint(ip::tcp::v4(), port))
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

void TcpServer::handle_accept(std::shared_ptr<TcpSession> session, boost::system::error_code err) {
    if (AKU_LIKELY(!err)) {
        session->start();
    } else {
        logger_.error() << "Acceptor error " << err.message();
    }
    start();
}

}

