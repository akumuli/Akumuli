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
#include <algorithm>

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

struct RandomSamplingBolt : std::enable_shared_from_this<RandomSamplingBolt>, Bolt {
    const uint32_t                      buffer_size_;
    std::vector<std::shared_ptr<Bolt>>  outputs_;
    std::vector<std::weak_ptr<Bolt>>    inputs_;
    std::vector<aku_TimeStamp>          timestamps_;
    std::vector<aku_ParamId>            paramids_;
    std::vector<double>                 values_;
    Rand                                random_;

    RandomSamplingBolt(uint32_t buffer_size)
        : buffer_size_(buffer_size)
    {
    }

    // Bolt interface
    virtual void add_output(std::shared_ptr<Bolt> next) {
        outputs_.push_back(next);
    }

    virtual void add_input(std::weak_ptr<Bolt> input) {
        inputs_.push_back(input);
    }

    virtual BoltType get_bolt_type() const {
        return Bolt::RandomSampler;
    }

    virtual std::vector<std::shared_ptr<Bolt>> get_bolt_outputs() const {
        return outputs_;
    }

    virtual std::vector<std::shared_ptr<Bolt>> get_bolt_inputs() const {
        std::vector<std::shared_ptr<Bolt>> result;
        for(auto wref: inputs_) {
            result.push_back(wref.lock());
        }
        return result;
    }

    virtual void complete(std::shared_ptr<Bolt> caller) {

        // Reset caller in inputs list
        for(auto& wref: inputs_) {                              // TODO: this logic should be
            auto sref = wref.lock();                            // moved to separate mixin class
            if (sref && sref == caller) {                       // tested and inherited by all
                wref.reset();                                   // blots.
                break;
            }
        }

        // Check precondition (all inputs was gone)
        for(auto& wref: inputs_) {
            if (wref.expired() == false) {
                return;
            }
        }
        if (outputs_.empty()) {
            BoltException except(Bolt::RandomSampler, "no output bolt");
            BOOST_THROW_EXCEPTION(except);
        }

        // Do the actual job
        auto& tsarray = timestamps_;
        auto predicate = [&tsarray](uint32_t lhs, uint32_t rhs) {
            return tsarray.at(lhs) < tsarray.at(rhs);
        };

        std::vector<uint32_t> indexes;
        uint32_t gencnt = 0u;
        std::generate_n(std::back_inserter(indexes), timestamps_.size(), [&gencnt]() { return gencnt++; });
        std::stable_sort(indexes.begin(), indexes.end(), predicate);

        for(auto ix: indexes) {
            for(auto& bolt: outputs_) {
                bolt->put(timestamps_.at(ix),
                         paramids_.at(ix),
                         values_.at(ix));
            }
        }

        for(auto& bolt: outputs_) {
            bolt->complete(shared_from_this());
        }
    }

    virtual void put(aku_TimeStamp ts, aku_ParamId id, double value) {
        if (outputs_.empty()) {
            BoltException except(Bolt::RandomSampler, "no output bolt");
            BOOST_THROW_EXCEPTION(except);
        }
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
    return std::make_shared<RandomSamplingBolt>(buffer_size);
}

QueryProcessor::QueryProcessor()
    : namesofinterest(StringTools::create_table(0x1000))
{
}

int QueryProcessor::match(uint64_t param_id) {
    return -1;
}

}

