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

#include <boost/lockfree/queue.hpp>

#include "protocol_consumer.h"
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
    // Typedefs
    typedef struct { char emptybits[64]; }      Padding;        //< Padding
    typedef std::atomic<uint64_t>               SpoutCounter;   //< Shared counter
    typedef struct {
        aku_ParamId     id;                                     //< Measurement ID
        aku_TimeStamp   ts;                                     //< Measurement timestamp
        double          value;                                  //< Value (TODO: should be variant type)
        SpoutCounter   *cnt;                                    //< Pointer to spout's shared counter
    }                                           TVal;           //< Value
    typedef std::unique_ptr<TVal>               PVal;           //< Pointer to value
    typedef boost::lockfree::queue<TVal*>       Queue;          //< Queue class

    // Constants
    enum {
        //! PVal pool size
        POOL_SIZE = 0x1000,
    };

    // Data
    SpoutCounter counter_;
    Padding pad0;
    uint64_t created_;
    uint64_t deleted_;
    std::vector<PVal> pool_;
    Padding pad1;
    std::shared_ptr<Queue> queue_;

    // C-tor
    PipelineSpout(std::shared_ptr<Queue> q);

    // ProtocolConsumer
    virtual void write_double(aku_ParamId param, aku_TimeStamp ts, double data);
    virtual void add_bulk_string(const Byte *buffer, size_t n);

    // Utility
    //! Reserve index for the next TVal in the pool or negative value on error.
    int get_index_of_empty_slot();
    //! Delete processed items from the pool.
    void gc();
};

class IngestionPipeline : public std::enable_shared_from_this<IngestionPipeline>
{
    std::shared_ptr<DbConnection> con_;
    std::shared_ptr<PipelineSpout::Queue> queue_;
    static PipelineSpout::TVal* POISON;
public:
    /** Create new pipeline topology.
      */
    IngestionPipeline(std::shared_ptr<DbConnection> con);

    /** Run pipeline topology.
      */
    void run();

    /** Add new pipeline spout. */
    std::shared_ptr<PipelineSpout> make_spout();

    void close();
};

}  // namespace Akumuli

