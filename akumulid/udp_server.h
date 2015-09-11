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

#include <memory>

#include <boost/thread/barrier.hpp>

#include "ingestion_pipeline.h"
#include "logger.h"

namespace Akumuli {


/** UDP server for data ingestion.
  */
class UdpServer : public std::enable_shared_from_this<UdpServer>
{
    std::shared_ptr<IngestionPipeline> pipeline_;
    boost::barrier start_barrier_;  //< Barrier to start worker thread
    boost::barrier stop_barrier_;   //< Barrier to stop worker thread

    Logger logger_;

public:

    /** C-tor.
      * @param nworker number of workers
      * @param port port number
      * @param pipeline pointer to ingestion pipeline
      */
    UdpServer(int nworkers, int port, std::shared_ptr<IngestionPipeline> pipeline);

    //! Start processing packets
    void start();

    //! Stop processing packets
    void stop();

private:
    void worker(std::shared_ptr<PipelineSpout> spout);
};

}  // namespace

