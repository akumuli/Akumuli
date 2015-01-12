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

#include "logger.h"
#include "protocolparser.h"
#include "ingestion_pipeline.h"

namespace Akumuli {

//                                                //
//          Type aliases from boost.asio          //
//                                                //

typedef boost::asio::io_service         IOService;
typedef boost::asio::ip::tcp::acceptor  TcpAcceptor;
typedef boost::asio::ip::tcp::socket    TcpSocket;
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
    IOService *io_;
    TcpSocket socket_;
    StrandT strand_;
    std::shared_ptr<PipelineSpout> spout_;
    ProtocolParser parser_;
public:
    typedef std::shared_ptr<Byte> BufferT;
    TcpSession(IOService *io, std::shared_ptr<PipelineSpout> spout);

    TcpSocket& socket();

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
class TcpServer : public std::enable_shared_from_this<TcpServer>
{
    IOService                           own_io_;         //< Acceptor's own io-service
    TcpAcceptor                         acceptor_;       //< Acceptor
    std::vector<IOService*>             sessions_io_;    //< List of io-services for sessions
    std::vector<WorkT>                  sessions_work_;  //< Work to block io-services from completing too early
    std::shared_ptr<IngestionPipeline>  pipeline_;       //< Pipeline instance
    std::atomic<int>                    io_index_;       //< I/O service index

    // Acceptor thread control
    std::mutex                          mutex_;
    std::condition_variable             cond_;
    enum {
        UNDEFINED,
        STARTED,
        STOPPED,
    }                                   acceptor_state_;
public:
    /** C-tor. Should be created in the heap.
      * @param io io-service instance
      * @param port port to listen for new connections
      * @param pipeline ingestion pipeline
      */
    TcpServer(// Server parameters
                 std::vector<IOService*> io, int port,
              // Storage & pipeline
                 std::shared_ptr<IngestionPipeline> pipeline
              );

    //! Start listening on socket
    void start();

    //! Stop listening on socket
    void stop();
private:

    //! Start implementation
    void _start();

    //! Accept event handler
    void handle_accept(std::shared_ptr<TcpSession> session, boost::system::error_code err);
};

}
