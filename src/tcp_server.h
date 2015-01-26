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
#include "ingestion_pipeline.h"

namespace Akumuli {

//                                                //
//          Type aliases from boost.asio          //
//                                                //

typedef boost::asio::io_service         IOServiceT;
typedef boost::asio::ip::tcp::acceptor  AcceptorT;
typedef boost::asio::ip::tcp::socket    SocketT;
typedef boost::asio::ip::tcp::endpoint  EndpointT;
typedef boost::asio::strand             StrandT;
typedef boost::asio::io_service::work   WorkT;

/** Server session. Reads data from socket.
 *  Must be created in the heap.
  */
class TcpSession : public std::enable_shared_from_this<TcpSession> {
    enum {
        BUFFER_SIZE           = 0x1000,  //< Buffer size
        BUFFER_SIZE_THRESHOLD = 0x0200,  //< Min free buffer space
    };
    IOServiceT *io_;
    SocketT socket_;
    StrandT strand_;
    std::shared_ptr<PipelineSpout> spout_;
    ProtocolParser parser_;
    Logger logger_;
public:
    typedef std::shared_ptr<Byte> BufferT;
    TcpSession(IOServiceT *io, std::shared_ptr<PipelineSpout> spout);

    SocketT& socket();

    void start(BufferT buf,
               size_t buf_size,
               size_t pos,
               size_t bytes_read);

    static BufferT NO_BUFFER;
private:

    /** Allocate new buffer or reuse old if there is enough space in there.
      * @param prev_buf previous buffer or NO_BUFFER
      * @param size buffer full size
      * @param pos position in the buffer
      * @param bytes_read number of newly overwritten bytes in the buffer
      * @return buffer (allocated or reused), full buffer size and write position (three element tuple)
      */
    std::tuple<BufferT, size_t, size_t> get_next_buffer(BufferT prev_buf,
                                                        size_t size,
                                                        size_t pos,
                                                        size_t bytes_read);

    void handle_read(BufferT buffer,
                     size_t pos,
                     size_t buf_size,
                     boost::system::error_code error,
                     size_t nbytes);
};


/** Tcp server.
  * Accepts connections and creates new client sessions
  */
class TcpAcceptor : public std::enable_shared_from_this<TcpAcceptor>
{
    IOServiceT                          own_io_;         //< Acceptor's own io-service
    AcceptorT                           acceptor_;       //< Acceptor
    std::vector<IOServiceT*>            sessions_io_;    //< List of io-services for sessions
    std::vector<WorkT>                  sessions_work_;  //< Work to block io-services from completing too early
    std::shared_ptr<IngestionPipeline>  pipeline_;       //< Pipeline instance
    std::atomic<int>                    io_index_;       //< I/O service index

    boost::barrier                      start_barrier_;  //< Barrier to start worker thread
    boost::barrier                      stop_barrier_;   //< Barrier to stop worker thread

    // Logger
    Logger logger_;
public:
    /** C-tor. Should be created in the heap.
      * @param io io-service instance
      * @param port port to listen for new connections
      * @param pipeline ingestion pipeline
      */
    TcpAcceptor(// Server parameters
                std::vector<IOServiceT*> io, int port,
                // Storage & pipeline
                std::shared_ptr<IngestionPipeline> pipeline);

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


struct TcpServer : public std::enable_shared_from_this<TcpServer>
{

    std::shared_ptr<IngestionPipeline>  pline;
    std::shared_ptr<DbConnection>       dbcon;
    std::shared_ptr<TcpAcceptor>        serv;
    boost::asio::io_service             ioA;
    std::vector<IOServiceT*>            iovec       = { &ioA };
    boost::barrier                      barrier;
    boost::asio::signal_set             sig;
    std::atomic<int>                    stopped = {0};

    TcpServer(std::shared_ptr<DbConnection> con);

    //! Run IO service
    void start();

    void handle_sigint(boost::system::error_code err);

    void stop();

    void wait();
};
}
