#include <iostream>
#include <memory>

#include <boost/asio.hpp>
#include <boost/bind.hpp>

#include "logger.h"
#include "protocolparser.h"
#include "ingestion_pipeline.h"

using namespace boost::asio;

namespace Akumuli {

typedef io_service IOService;
typedef ip::tcp::acceptor TcpAcceptor;
typedef ip::tcp::socket TcpSocket;

/** Server session. Reads data from socket.
 *  Must be created in the heap.
  */
class TcpSession : public std::enable_shared_from_this<TcpSession> {
    IOService *io_;
    TcpSocket socket_;
    const size_t BUFFER_SIZE = 0x1000;
public:
    TcpSession(IOService *io)
        : io_(io)
        , socket_(*io)
    {
    }

    TcpSocket& socket() {
        return socket_;
    }

    void start() {
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
private:
    std::shared_ptr<void> get_next_buffer() {
        void* buffer = malloc(BUFFER_SIZE);
        std::shared_ptr<void> bufptr(buffer, &free);
        return bufptr;
    }

    void return_buffer(std::shared_ptr<void> buffer) {
    }

    void handle_read(std::shared_ptr<void> buffer, boost::system::error_code error, size_t nbytes) {
        if (!error) {
            start();
            // processing goes here
            // done processing
        } else {
            // error reporting goes here
            std::cout << "read error " << error << std::endl;
        }
    }
};

class TcpServer : public std::enable_shared_from_this<TcpServer>
{
    IOService *io_;
    TcpAcceptor acceptor_;
public:
    TcpServer(IOService *io, int port)
        : io_(io)
        , acceptor_(*io_, ip::tcp::endpoint(ip::tcp::v4(), port))
    {
    }

    void start() {
        std::shared_ptr<TcpSession> session;
        session.reset(new TcpSession(io_));
        acceptor_.async_accept(
                    session->socket(),
                    boost::bind(&TcpServer::handle_accept,
                                shared_from_this(),
                                session,
                                boost::asio::placeholders::error)
                    );
    }
private:
    void handle_accept(std::shared_ptr<TcpSession> session, boost::system::error_code err) {
        if (!err) {
            session->start();
        } else {
            // error reporting
        }
        start();
    }
};

}

int main(int argc, char** argv) {
    Akumuli::Logger logger("test", 10);
    logger.info() << "Info";
    for (int i = 0; i < 10; i++)
        logger.trace() << "Trace msg " << i;
    logger.error() << "Hello " << "world";
    return 0;
}

