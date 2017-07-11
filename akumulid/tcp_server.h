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


/**
 * Common interface for all protocol session (RESP, line, etc)
 */
struct ProtocolSession {

    virtual ~ProtocolSession() = default;

    /**
     * Returns socket instance (used by acceptor to esteblish new connection)
     */
    virtual SocketT& socket() = 0;

    /**
     * Initiates data ingestion
     */
    virtual void start() = 0;

    /**
     * Returns error callback that can be used by the other code
     * to report errors.
     */
    virtual ErrorCallback get_error_cb() = 0;
};


/**
 * Object of this class can be used by the TCP-server to build
 * protocol sessions.
 */
struct ProtocolSessionBuilder {

    /**
     * @brief create new ProtocolSession instance
     * @param io is an IOServiceT instance
     * @param session is a database session instance
     */
    virtual std::shared_ptr<ProtocolSession> create(IOServiceT* io, std::shared_ptr<DbSession> session) = 0;

    /**
     * Get the name of the protocol
     */
    virtual std::string name() const = 0;

    /**
     * @brief Create RESP parser builder
     * @param parallel use thread safe implementation if true
     * @return newly created object
     */
    static std::unique_ptr<ProtocolSessionBuilder> create_resp_builder(bool parallel=true);

    /**
     * @brief Create OpenTSDB parser builder
     * @param parallel use thread safe implementation if true
     * @return newly created object
     */
    static std::unique_ptr<ProtocolSessionBuilder> create_opentsdb_builder(bool parallel=true);
};


/** Tcp server.
  * Accepts connections and creates new client sessions
  */
class TcpAcceptor : public std::enable_shared_from_this<TcpAcceptor> {
    typedef std::unique_ptr<ProtocolSessionBuilder> ProtocolSessionBuilderT;

    const bool                         parallel_;  //< Flag for TcpSession instances
    IOServiceT                           own_io_;  //< Acceptor's own io-service
    AcceptorT                          acceptor_;  //< Acceptor
    ProtocolSessionBuilderT            protocol_;  //< Protocol builder
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
      * @param connection to the database
      */
    TcpAcceptor(
        std::vector<IOServiceT*> io, int port,
        std::shared_ptr<DbConnection> connection,
        bool parallel=true);

    /**
     * Create multiprotocol c-tor
      * @param io io-service instance
      * @param port port to listen for new connections
      * @param protocol is a protocol builder
      * @param connection to the database
     */
    TcpAcceptor(
        std::vector<IOServiceT*> io,
        int port,
        std::unique_ptr<ProtocolSessionBuilder> protocol,
        std::shared_ptr<DbConnection> connection,
        bool parallel=true);

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

    std::string name() const;

private:
    //! Accept event handler
    void handle_accept(std::shared_ptr<ProtocolSession> session, boost::system::error_code err);
};


struct TcpServer : std::enable_shared_from_this<TcpServer>, Server {
    enum class Mode {
        EVENT_LOOP_PER_THREAD,
        SHARED_EVENT_LOOP,
    };
    typedef std::unique_ptr<IOServiceT>  IOPtr;
    std::weak_ptr<DbConnection>          connection_;
    std::vector<std::shared_ptr<TcpAcceptor>> acceptors_;
    std::vector<IOPtr>                   ios_;
    std::vector<IOServiceT*>             iovec;
    boost::barrier                       barrier;
    std::atomic<int>                     stopped;
    Logger                               logger_;

    /**
     * @brief Creates TCP server that accepts only RESP connections
     * @param connection is a pointer to opened database connection
     * @param concurrency is a concurrency hint (how many threads should be used)
     * @param port is a port number to listen
     * @param mode is a server mode (event loop per thread or one shared event loop)
     * @note I've found that on Linux Mode::EVENT_LOOP_PER_THREAD gives more consistent results (the other option should be used for windows)
     */
    TcpServer(std::shared_ptr<DbConnection> connection, int concurrency, int port, Mode mode=Mode::EVENT_LOOP_PER_THREAD);

    TcpServer(std::shared_ptr<DbConnection> connection,
              int concurrency,
              std::map<int, std::unique_ptr<ProtocolSessionBuilder>> protocol_map,
              Mode mode=Mode::EVENT_LOOP_PER_THREAD);

    ~TcpServer();

    //! Run IO service
    virtual void start(SignalHandler* sig_handler, int id);

    //! Stop the server (should be called from signal handler)
    void stop();
};
}
