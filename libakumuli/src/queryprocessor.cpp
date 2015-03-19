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

#include "queryprocessor.h"
#include "util.h"

#include <random>

#include <boost/lexical_cast.hpp>

namespace Akumuli {

BoltException::BoltException(Bolt::BoltType type, const char* msg)
    : std::runtime_error(msg)
    , type_(type)
{
}

Bolt::BoltType BoltException::get_type() const {
    return type_;
}

struct RandomSamplingBolt : Bolt {
    size_t                              buffer_size_;
    std::vector<std::shared_ptr<Bolt>>  outputs_;
    std::vector<aku_TimeStamp>          timestamps_;
    std::vector<aku_ParamId>            paramids_;
    std::vector<double>                 values_;
    Rand                                random_;

    // TODO: check buffer_size_ to have less then max uint32_t value

    // Bolt interface
    virtual void add_next(std::shared_ptr<Bolt> next) {
        outputs_.push_back(next);
    }

    virtual void complete() {
        if (outputs_.empty()) {
            BoltException except(Bolt::RandomSampler, "no output bolt");
            BOOST_THROW_EXCEPTION(except);
        }
        // TODO: sort data by timestamp
        for(auto i = 0u; i < timestamps_.size(); i++) {
            for(auto& bolt: outputs_) {
                bolt->put(timestamps_.at(i),
                         paramids_.at(i),
                         values_.at(i));
            }
        }
    }

    virtual void put(aku_TimeStamp ts, aku_ParamId id, double value) {
        if (timestamps_.size() < buffer_size_) {
            // Just append new values
            timestamps_.push_back(ts);
            paramids_.push_back(id);
            values_.push_back(value);
        } else {
            // Flip a coin
            uint32_t ix = random_() % timestamps_.size();
            if (ix < buffer_size_) {
                timestamps_.at(ix) =    ts;
                paramids_.at(ix)   =    id;
                values_.at(ix)     = value;
            }
        }
    }
};

std::shared_ptr<Bolt> BoltsBuilder::make_random_sampler(std::string type,
                                                        size_t buffer_size,
                                                        aku_logger_cb_t logger)
{
    {
        std::stringstream logfmt;
        logfmt << "Creating random sampler of type " << type << " with buffer size " << buffer_size;
        (*logger)(AKU_LOG_TRACE, logfmt.str().c_str());
    }
    // only reservoir sampling is supported
    if (type != "reservoir") {
        BoltException except(Bolt::RandomSampler, "unsupported sampler type");
        BOOST_THROW_EXCEPTION(except);
    }
    // Build object
    return std::shared_ptr<Bolt>();
}

QueryProcessor::QueryProcessor()
    : namesofinterest(StringTools::create_table(0x1000))
{
}

int QueryProcessor::match(uint64_t param_id) {
    return -1;
}

}

