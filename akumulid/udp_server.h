/**
 * Copyright (c) 2015 Eugene Lazin <4lazin@gmail.com>
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

#include <atomic>
#include <memory>

#include <boost/thread/barrier.hpp>

#include "ingestion_pipeline.h"
#include "logger.h"
#include "protocolparser.h"
#include "server.h"


namespace Akumuli {


/** UDP server for data ingestion.
  */
class UdpServer : public std::enable_shared_from_this<UdpServer>, public Server {
    std::shared_ptr<DbConnection>      db_;
    boost::barrier                     start_barrier_;  //< Barrier to start worker thread
    boost::barrier                     stop_barrier_;   //< Barrier to stop worker thread
    std::atomic<int>                   stop_;
    const int                          port_;
    const int                          nworkers_;
    int                                sockfd_;         //< UDP socket file descriptor

    Logger logger_;

    static const int MSS      = 0x10000;  // 64K
    static const int NPACKETS = 16;

    struct IOBuf {
        // Counters
        std::atomic<u64> pps;
        std::atomic<u64> bps;

        // Packet recv structs
        mmsghdr msgs[NPACKETS];
        iovec   iovecs[NPACKETS];
        char    bufs[NPACKETS][MSS];  // 1MB

        IOBuf() {
            memset(this, 0, sizeof(IOBuf));
            for (int i = 0; i < NPACKETS; i++) {
                iovecs[i].iov_base         = bufs[i];
                iovecs[i].iov_len          = MSS;
                msgs[i].msg_hdr.msg_iov    = &iovecs[i];
                msgs[i].msg_hdr.msg_iovlen = 1;
            }
        }

    };

public:
    /** C-tor.
      * @param nworker number of workers
      * @param port port number
      * @param pipeline pointer to ingestion pipeline
      */
    UdpServer(std::shared_ptr<DbConnection> pipeline, int nworkers, int port);

    //! Start processing packets
    virtual void start(SignalHandler* sig, int id);

private:
    //! Stop processing packets, close the socket
    void stop();

    void worker(std::shared_ptr<DbSession> spout);
};

}  // namespace
