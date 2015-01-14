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
#include <string>
#include <memory>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include <boost/lockfree/queue.hpp>

#include "protocol_consumer.h"
#include "logger.h"
// akumuli-storage API
#include "akumuli.h"
#include "akumuli_config.h"

namespace Akumuli {

struct DbConnection {
    virtual ~DbConnection() {}
    virtual void write_double(aku_ParamId param, aku_TimeStamp ts, double data) = 0;
};


//! Object of this class writes everything to the database
class AkumuliConnection : public DbConnection
{
public:
    enum Durability {
        MaxDurability = 1,
        RelaxedDurability = 2,
        MaxThroughput = 4,
    };
private:
    std::string     dbpath_;
    aku_Database   *db_;
public:
    AkumuliConnection(const char* path, bool hugetlb, Durability durability);

    // ProtocolConsumer interface
public:
    void write_double(aku_ParamId param, aku_TimeStamp ts, double data);
};

using boost::lockfree::queue;
using boost::lockfree::capacity;


enum BackoffPolicy {
    AKU_THROTTLE,
    AKU_LINEAR_BACKOFF,
};


/** Pipeline's spout.
  * Object of this class can be used to ingest data to pipeline.
  * It should be connected with IngestionPipeline instance with
  * the shared queue. Pooling is used to simplify allocator's
  * life. All TVals should be deleted in the same thread where
  * they was created. This shuld minimize contention inside
  * allocator and limit overall memory usage (no need to create
  * pool of objects beforehand).
  */
struct PipelineSpout : ProtocolConsumer {

    // Constants
    enum {
        //! PVal pool size
        POOL_SIZE = 0x200,
        QCAP      = 0x10,
    };

    // Typedefs
    typedef struct { char emptybits[64]; }       Padding;        //< Padding
    typedef std::atomic<uint64_t>                SpoutCounter;   //< Shared counter
    typedef struct {
        aku_ParamId     id;                                      //< Measurement ID
        aku_TimeStamp   ts;                                      //< Measurement timestamp
        double          value;                                   //< Value (TODO: should be variant type)
        SpoutCounter   *cnt;                                     //< Pointer to spout's shared counter
    }                                            TVal;           //< Value
    typedef std::shared_ptr<TVal>                PVal;           //< Pointer to value
    typedef queue<TVal*>                         Queue;          //< Queue class
    typedef std::shared_ptr<Queue>               PQueue;         //< Pointer to queue

    // Data
    SpoutCounter        created_;                                //< Created elements counter
    Padding             pad0;
    SpoutCounter        deleted_;                                //< Deleted elements counter
    std::vector<PVal>   pool_;                                   //< TVal pool
    Padding             pad1;
    PQueue              queue_;                                  //< Queue
    const BackoffPolicy backoff_;
    Logger              logger_;                                 //< Logger instance

    // C-tor
    PipelineSpout(std::shared_ptr<Queue> q, BackoffPolicy bp);

    // ProtocolConsumer
    virtual void write_double(aku_ParamId param, aku_TimeStamp ts, double data);
    virtual void add_bulk_string(const Byte *buffer, size_t n);

    // Utility
    //! Reserve index for the next TVal in the pool or negative value on error.
    int get_index_of_empty_slot();
};

class IngestionPipeline : public std::enable_shared_from_this<IngestionPipeline>
{
    enum {
        N_QUEUES = 8,
    };
    typedef std::mutex                 Mtx;
    std::shared_ptr<DbConnection>      con_;        //< DB connection
    std::vector<PipelineSpout::PQueue> queues_;     //< Queues collection
    std::atomic<int>                   ixmake_;     //< Index for the make_spout mehtod
    std::shared_ptr<Mtx>               mutex_;      //< Mutex to wait thread completion
    std::condition_variable            cvar_;       //< Cond. variable for stopping
    bool                               stopped_;    //< Stopping flag
    static PipelineSpout::TVal        *POISON;      //< Poisoned object to stop worker thread
    static int                         TIMEOUT;     //< Close timeout
    const BackoffPolicy                backoff_;    //< Back-pressure policy
    Logger                             logger_;     //< Logger instance
public:
    /** Create new pipeline topology.
      */
    IngestionPipeline(std::shared_ptr<DbConnection> con, BackoffPolicy bp = AKU_THROTTLE);

    /** Run pipeline topology.
      */
    void start();

    /** Add new pipeline spout. */
    std::shared_ptr<PipelineSpout> make_spout();

    void stop();
};

}  // namespace Akumuli

