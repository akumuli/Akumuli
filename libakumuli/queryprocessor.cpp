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


/** Abstract graph node. Handles inputs and outputs.
  * Implements part of the Bolt interface.
  */
struct AcyclicGraphNode : Bolt
{
    std::vector<std::shared_ptr<Bolt>>  outputs_;
    std::vector<std::weak_ptr<Bolt>>    inputs_;

    virtual void add_output(std::shared_ptr<Bolt> next) {
        outputs_.push_back(next);
    }

    virtual void add_input(std::weak_ptr<Bolt> input) {
        inputs_.push_back(input);
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

    /** Remove input from inputs list.
      * @return true if all inputs was gone and false otherwise
      * @param caller is a pointer to input node that needs to be removed
      */
    bool remove_input(std::shared_ptr<Bolt> caller) {
        // Reset caller in inputs list
        for(auto& wref: inputs_) {
            auto sref = wref.lock();
            if (sref && sref == caller) {
                wref.reset();
                break;
            }
        }

        // Check precondition (all inputs was gone)
        for(auto& wref: inputs_) {
            if (wref.expired() == false) {
                return false;
            }
        }
        return true;
    }

    void distribute(aku_Timestamp ts, aku_ParamId pid, double value) {
        for(auto& bolt: outputs_) {
            bolt->put(ts, pid, value);
        }
    }

    /** Throw exception if there is no output node
      * @throw BoltException
      */
    void throw_if_no_output() {
        if (outputs_.empty()) {
            BoltException except(Bolt::RandomSampler, "no output bolt");
            BOOST_THROW_EXCEPTION(except);
        }
    }

    template<class Derived>
    void complete_children(std::shared_ptr<Derived> caller) {
        for(auto& bolt: outputs_) {
            bolt->complete(caller);
        }
    }
};


struct RandomSamplingBolt : std::enable_shared_from_this<RandomSamplingBolt>, AcyclicGraphNode {
    const uint32_t                      buffer_size_;
    std::vector<aku_Timestamp>          timestamps_;
    std::vector<aku_ParamId>            paramids_;
    std::vector<double>                 values_;
    Rand                                random_;

    RandomSamplingBolt(uint32_t buffer_size)
        : buffer_size_(buffer_size)
    {
    }

    // Bolt interface
    virtual BoltType get_bolt_type() const {
        return Bolt::RandomSampler;
    }

    virtual void complete(std::shared_ptr<Bolt> caller) {
        if (remove_input(caller)) {
            throw_if_no_output();

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
                distribute(timestamps_.at(ix),
                           paramids_.at(ix),
                           values_.at(ix));
            }

            complete_children(shared_from_this());
        }
    }

    virtual void put(aku_Timestamp ts, aku_ParamId id, double value) {
        throw_if_no_output();
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

/*
struct RandomSamplingBolt : AcyclicGraphNode<RandomSamplingBolt> {

    // Bolt interface
    virtual void complete(std::shared_ptr<Bolt> caller);
    virtual void put(aku_Timestamp ts, aku_ParamId id, double value);
    virtual void add_output(std::shared_ptr<Bolt> next);
    virtual void add_input(std::weak_ptr<Bolt> input);
    virtual BoltType get_bolt_type() const;
    virtual std::vector<std::shared_ptr<Bolt> > get_bolt_inputs() const;
    virtual std::vector<std::shared_ptr<Bolt> > get_bolt_outputs() const;
};
*/

//                                   //
//         Factory methods           //
//                                   //

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

QueryProcessor::QueryProcessor(std::shared_ptr<Bolt> root,
               std::vector<std::string> metrics,
               aku_Timestamp begin,
               aku_Timestamp end)
    : lowerbound(std::min(begin, end))
    , upperbound(std::max(begin, end))
    , direction(begin > end ? AKU_CURSOR_DIR_FORWARD : AKU_CURSOR_DIR_BACKWARD)
    , metrics(metrics)
    , namesofinterest(StringTools::create_table(0x1000))
    , root_bolt(root)
{
}

int QueryProcessor::match(uint64_t param_id) {
    return -1;
}

}

