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

class IngestionPipeline : public ProtocolConsumer
{
    typedef std::tuple<aku_ParamId, aku_TimeStamp, double, bool> TVal;
    typedef boost::lockfree::queue<const TVal*> Queue;

    std::shared_ptr<DbConnection> con_;
    Queue queue_;

    void start();

    // ProtocolConsumer interface
public:
    IngestionPipeline(std::shared_ptr<DbConnection> con);
    virtual void write_double(aku_ParamId param, aku_TimeStamp ts, double data);
    virtual void add_bulk_string(const Byte *buffer, size_t n);
};

}  // namespace Akumuli

