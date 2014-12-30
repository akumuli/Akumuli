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

using namespace boost::asio;

namespace Akumuli {

typedef io_service IOService;
typedef ip::tcp::acceptor TcpAcceptor;
typedef ip::tcp::socket TcpSocket;

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
    std::shared_ptr<PipelineSpout> spout_;
    ProtocolParser parser_;
public:
    TcpSession(IOService *io, std::shared_ptr<PipelineSpout> spout);

    TcpSocket& socket();

    void start();

private:

    std::shared_ptr<Byte> get_next_buffer() {
        Byte *buffer = (Byte*)malloc(BUFFER_SIZE);
        auto deleter = [](Byte* p) {
            free((void*)p);
        };
        std::shared_ptr<Byte> bufptr(buffer, deleter);
        return bufptr;
    }

    void return_buffer(std::shared_ptr<void> buffer) {
    }

    void handle_read(std::shared_ptr<Byte> buffer,
                     boost::system::error_code error,
                     size_t nbytes);
};


/** Tcp server.
  * Accepts connections and creates new client sessions
  */
class TcpServer : public std::enable_shared_from_this<TcpServer>
{
    IOService *io_;
    TcpAcceptor acceptor_;
    std::shared_ptr<IngestionPipeline> pipeline_;
public:
    /** C-tor. Should be created in the heap.
      * @param io io-service instance
      * @param port port to listen for new connections
      * @param pipeline ingestion pipeline
      */
    TcpServer(// Server parameters
                 IOService *io, int port,
              // Storage & pipeline
                 std::shared_ptr<IngestionPipeline> pipeline
              );

    //! Start listening on socket
    void start();

private:

    //! Start implementation
    void _start();

    //! Accept event handler
    void handle_accept(std::shared_ptr<TcpSession> session, boost::system::error_code err);
};

}
