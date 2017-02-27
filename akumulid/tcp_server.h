/**
 * Copyright (c) 2014 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread/barrier.hpp>

#include "logger.h"
#include "protocolparser.h"
#include "server.h"

namespace Akumuli {

//                                                //
//          Type aliases from boost.asio          //
//                                                //

typedef boost::asio::io_service              IOServiceT;
typedef boost::asio::ip::tcp::acceptor       AcceptorT;
typedef boost::asio::ip::tcp::socket         SocketT;
typedef boost::asio::ip::tcp::endpoint       EndpointT;
typedef boost::asio::strand                  StrandT;
typedef boost::asio::io_service::work        WorkT;
typedef std::function<void(aku_Status, u64)> ErrorCallback;

/** Server session. Reads data from socket.
 *  Must be created in the heap.
  */
class TcpSession : public std::enable_shared_from_this<TcpSession> {
    // TODO: Unique session ID
    enum {
        BUFFER_SIZE = ProtocolParser::RDBUF_SIZE,  //< Buffer size
    };
    IOServiceT*                     io_;
    SocketT                         socket_;
    StrandT                         strand_;
    std::shared_ptr<DbSession>      spout_;
    ProtocolParser                  parser_;
    Logger                          logger_;

public:
    typedef Byte* BufferT;
    TcpSession(IOServiceT* io, std::shared_ptr<DbSession> spout);

    ~TcpSession();

    SocketT& socket();

    void start();

    ErrorCallback get_error_cb();

private:
    /** Allocate new buffer.
      */
    std::tuple<BufferT, size_t> get_next_buffer();

    void handle_read(BufferT buffer, boost::system::error_code error, size_t nbytes);

    void handle_write_error(boost::system::error_code error);

    void drain_pipeline_spout();
};


/** Tcp server.
  * Accepts connections and creates new client sessions
  */
class TcpAcceptor : public std::enable_shared_from_this<TcpAcceptor> {
    IOServiceT                           own_io_;  //< Acceptor's own io-service
    AcceptorT                          acceptor_;  //< Acceptor
    std::vector<IOServiceT*>        sessions_io_;  //< List of io-services for sessions
    std::vector<WorkT>            sessions_work_;  //< Work to block io-services from completing too early
    std::weak_ptr<DbConnection>      connection_;  //< DB connection
    std::atomic<int>                   io_index_;  //< I/O service index

    boost::barrier start_barrier_;  //< Barrier to start worker thread
    boost::barrier stop_barrier_;   //< Barrier to stop worker thread

    Logger logger_;

public:
    /** C-tor. Should be created in the heap.
      * @param io io-service instance
      * @param port port to listen for new connections
      * @param pipeline ingestion pipeline
      */
    TcpAcceptor(  // Server parameters
        std::vector<IOServiceT*> io, int port,
        // Storage & pipeline
        std::shared_ptr<DbConnection> connection);

    ~TcpAcceptor();

    //! Start listening on socket
    void start();

    //! Stop listening on socket
    void stop();

    //! Stop listening on socket (for testing)
    void _stop();

    //! Start implementation (this method is public only for testing purposes)
    void _start();

    //! Run one handler (should be used only for testing)
    void _run_one();

private:
    //! Accept event handler
    void handle_accept(std::shared_ptr<TcpSession> session, boost::system::error_code err);
};


struct TcpServer : std::enable_shared_from_this<TcpServer>, Server {
    typedef std::unique_ptr<IOServiceT>  IOPtr;
    std::weak_ptr<DbConnection>          connection_;
    std::shared_ptr<TcpAcceptor>         serv;
    std::vector<IOPtr>                   ios_;
    std::vector<IOServiceT*>             iovec;
    boost::barrier                       barrier;
    std::atomic<int>                     stopped;
    Logger                               logger_;

    TcpServer(std::shared_ptr<DbConnection> connection, int concurrency, int port);
    ~TcpServer();

    //! Run IO service
    virtual void start(SignalHandler* sig_handler, int id);

    //! Stop the server (should be called from signal handler)
    void stop();
};
}
