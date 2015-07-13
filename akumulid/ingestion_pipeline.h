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
#include <functional>

#include <boost/lockfree/queue.hpp>
#include <boost/thread/barrier.hpp>

#include "protocol_consumer.h"
#include "logger.h"
// akumuli-storage API
#include "akumuli.h"
#include "akumuli_config.h"

namespace Akumuli {

//! Abstraction layer above aku_Cursor
struct DbCursor {
    //! Read data from cursor
    virtual aku_Status read( aku_Sample       *dest
                           , size_t            dest_size) = 0;

    //! Check is cursor is done reading
    virtual int is_done() = 0;

    //! Check for error condition
    virtual aku_Status is_error(int* out_error_code_or_null) = 0;

    //! Close cursor
    virtual void close() = 0;
};

//! Abstraction layer above aku_Database
struct DbConnection {

    virtual ~DbConnection() {}

    virtual aku_Status write(const aku_Sample &sample) = 0;

    virtual std::shared_ptr<DbCursor> search(std::string query) = 0;
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

    virtual aku_Status write(const aku_Sample &sample);

    virtual std::shared_ptr<DbCursor> search(std::string query);
};

using boost::lockfree::queue;
using boost::lockfree::capacity;


enum BackoffPolicy {
    AKU_THROTTLE,
    AKU_LINEAR_BACKOFF,
};


//! Callback from pipeline to session
typedef std::function<void(aku_Status, uint64_t)> PipelineErrorCb;


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
        aku_ParamId            id;                               //< Measurement ID
        aku_Timestamp          ts;                               //< Measurement timestamp
        double                 value;                            //< Value (TODO: should be variant type)
        SpoutCounter          *cnt;                              //< Pointer to spout's shared counter
        PipelineErrorCb       *on_error;                         //< On error callback
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
    PipelineErrorCb     on_error_;                               //< Session callback

    // C-tor
    PipelineSpout(std::shared_ptr<Queue> q, BackoffPolicy bp);
   ~PipelineSpout();

    void set_error_cb(PipelineErrorCb cb);

    // ProtocolConsumer
    virtual void write_double(aku_ParamId param, aku_Timestamp ts, double data);
    virtual void add_bulk_string(const Byte *buffer, size_t n);

    // Utility
    //! Reserve index for the next TVal in the pool or negative value on error.
    int get_index_of_empty_slot();

    /** Dump all errors to ostr or report that everything is OK
      * @param ostr stream to write
      */
    void get_error(std::ostream& ostr);
};

class IngestionPipeline : public std::enable_shared_from_this<IngestionPipeline>
{
    enum {
        N_QUEUES = 8,
    };
    typedef boost::barrier             Barr;
    std::shared_ptr<DbConnection>      con_;        //< DB connection
    std::vector<PipelineSpout::PQueue> queues_;     //< Queues collection
    std::atomic<int>                   ixmake_;     //< Index for the make_spout mehtod
    Barr                               stopbar_;    //< Stopping barrier
    Barr                               startbar_;   //< Stopping barrier
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

