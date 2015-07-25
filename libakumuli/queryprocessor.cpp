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
#include <unordered_set>

#include <boost/lexical_cast.hpp>

namespace Akumuli {
namespace QP {

NodeException::NodeException(Node::NodeType type, const char* msg)
    : std::runtime_error(msg)
    , type_(type)
{
}

Node::NodeType NodeException::get_type() const {
    return type_;
}

struct RandomSamplingNode : std::enable_shared_from_this<RandomSamplingNode>, Node {
    const uint32_t                      buffer_size_;
    std::vector<aku_Sample>             samples_;
    Rand                                random_;
    std::shared_ptr<Node>               next_;

    RandomSamplingNode(uint32_t buffer_size, std::shared_ptr<Node> next)
        : buffer_size_(buffer_size)
        , next_(next)
    {
        samples_.reserve(buffer_size);
    }

    // Bolt interface
    virtual NodeType get_type() const {
        return Node::RandomSampler;
    }

    virtual void complete() {
        if (!next_) {
            AKU_PANIC("bad query processor node, next not set");
        }

        // Do the actual job
        auto predicate = [](aku_Sample const& lhs, aku_Sample const& rhs) {
            auto l = std::make_tuple(lhs.timestamp, lhs.paramid);
            auto r = std::make_tuple(rhs.timestamp, rhs.paramid);
            return l < r;
        };

        std::stable_sort(samples_.begin(), samples_.end(), predicate);

        for(auto const& sample: samples_) {
            next_->put(sample);
        }

        next_->complete();
    }

    virtual bool put(const aku_Sample& sample) {
        if (!next_) {
            AKU_PANIC("bad query processor node, next not set");
        }
        if (samples_.size() < buffer_size_) {
            // Just append new values
            samples_.push_back(sample);
        } else {
            // Flip a coin
            uint32_t ix = random_() % samples_.size();
            if (ix < buffer_size_) {
                samples_.at(ix) = sample;
            }
        }
        return true;
    }

    void set_error(aku_Status status) {
        if (!next_) {
            AKU_PANIC("bad query processor node, next not set");
        }
        next_->set_error(status);
    }
};


/** Filter ids using predicate.
  * Predicate is an unary functor that accepts parameter of type aku_ParamId - fun(aku_ParamId) -> bool.
  */
template<class Predicate>
struct FilterByIdNode : std::enable_shared_from_this<FilterByIdNode<Predicate>>, Node {
    //! Id matching predicate
    Predicate op_;
    std::shared_ptr<Node> next_;

    FilterByIdNode(Predicate pred, std::shared_ptr<Node> next)
        : op_(pred)
        , next_(next)
    {
    }

    // Bolt interface
    virtual void complete() {
        if (!next_) {
            AKU_PANIC("bad query processor node, next not set");
        }
        next_->complete();
    }

    virtual bool put(const aku_Sample& sample) {
        if (!next_) {
            AKU_PANIC("bad query processor node, next not set");
        }
        return op_(sample.paramid) ? next_->put(sample) : true;
    }

    void set_error(aku_Status status) {
        if (!next_) {
            AKU_PANIC("bad query processor node, next not set");
        }
        next_->set_error(status);
    }

    virtual NodeType get_type() const {
        return NodeType::FilterById;
    }
};

struct MovingAverage : Node {
    struct MACounter {
        double acc;
        size_t num;
    };

    aku_Timestamp const step_;
    aku_Timestamp lowerbound_, upperbound_;
    std::shared_ptr<Node> next_;

    MovingAverage(aku_Timestamp step, aku_Timestamp ts, std::shared_ptr<Node> next)
        : step_(step)
        , lowerbound_(ts)
        , upperbound_(ts + step)
        , next_(next)
    {
    }

    std::unordered_map<aku_ParamId, MACounter> counters_;

    bool average_samples() {
        for (auto& cnt: counters_) {
            aku_Sample sample;
            sample.paramid = cnt.first;
            sample.payload.value.float64 = 0.0;
            if (cnt.second.num) {
                sample.payload.value.float64 = cnt.second.acc / cnt.second.num;
            }
            sample.payload.type = aku_PData::FLOAT;
            sample.timestamp = upperbound_;
            cnt.second = {};
            if (!next_->put(sample)) {
                return false;
            }
        }
        return true;
    }

    virtual void complete() {
        average_samples();
        next_->complete();
    }

    virtual bool put(const aku_Sample &sample) {
        // ignore BLOBs
        if (sample.payload.type == aku_PData::FLOAT) {
            aku_ParamId id = sample.paramid;
            aku_Timestamp ts = sample.timestamp;
            double value = sample.payload.value.float64;
            if (ts > upperbound_) {
                // Forward direction
                if (!average_samples()) {
                    return false;
                }
                lowerbound_ += step_;
                upperbound_ += step_;
            } else if (ts < lowerbound_) {
                // Backward direction
                if (!average_samples()) {
                    return false;
                }
                lowerbound_ -= step_;
                upperbound_ -= step_;
            } else {
                auto& cnt = counters_[id];
                cnt.acc += value;
                cnt.num += 1;
            }
        }
        return true;
    }

    virtual void set_error(aku_Status status) {
        next_->set_error(status);
    }

    virtual NodeType get_type() const {
        return Node::MovingAverage;
    }
};

//                                   //
//         Factory methods           //
//                                   //

std::shared_ptr<Node> NodeBuilder::make_random_sampler(std::string type,
                                                       size_t buffer_size,
                                                       std::shared_ptr<Node> next,
                                                       aku_logger_cb_t logger)
{
    {
        std::stringstream logfmt;
        logfmt << "Creating random sampler of type " << type << " with buffer size " << buffer_size;
        (*logger)(AKU_LOG_TRACE, logfmt.str().c_str());
    }
    // only reservoir sampling is supported
    if (type != "reservoir") {
        NodeException except(Node::RandomSampler, "unsupported sampler type");
        BOOST_THROW_EXCEPTION(except);
    }
    // Build object
    return std::make_shared<RandomSamplingNode>(buffer_size, next);
}

std::shared_ptr<Node> NodeBuilder::make_filter_by_id(aku_ParamId id, std::shared_ptr<Node> next, aku_logger_cb_t logger) {
    struct Fun {
        aku_ParamId id_;
        bool operator () (aku_ParamId id) {
            return id == id_;
        }
    };
    typedef FilterByIdNode<Fun> NodeT;
    Fun fun = { id };
    std::stringstream logfmt;
    logfmt << "Creating id filter node for id " << id;
    (*logger)(AKU_LOG_TRACE, logfmt.str().c_str());
    return std::make_shared<NodeT>(fun, next);
}

std::shared_ptr<Node> NodeBuilder::make_filter_by_id_list(std::vector<aku_ParamId> ids,
                                                          std::shared_ptr<Node> next,
                                                          aku_logger_cb_t logger) {
    struct Matcher {
        std::unordered_set<aku_ParamId> idset;

        bool operator () (aku_ParamId id) {
            return idset.count(id) > 0;
        }
    };
    typedef FilterByIdNode<Matcher> NodeT;
    std::unordered_set<aku_ParamId> idset(ids.begin(), ids.end());
    Matcher fn = { idset };
    std::stringstream logfmt;
    logfmt << "Creating id-list filter node (" << ids.size() << " ids in a list)";
    (*logger)(AKU_LOG_TRACE, logfmt.str().c_str());
    return std::make_shared<NodeT>(fn, next);
}

std::shared_ptr<Node> NodeBuilder::make_filter_out_by_id_list(std::vector<aku_ParamId> ids,
                                                              std::shared_ptr<Node> next,
                                                              aku_logger_cb_t logger)
{
    struct Matcher {
        std::unordered_set<aku_ParamId> idset;

        bool operator () (aku_ParamId id) {
            return idset.count(id) == 0;
        }
    };
    typedef FilterByIdNode<Matcher> NodeT;
    std::unordered_set<aku_ParamId> idset(ids.begin(), ids.end());
    Matcher fn = { idset };
    std::stringstream logfmt;
    logfmt << "Creating id-list filter out node (" << ids.size() << " ids in a list)";
    (*logger)(AKU_LOG_TRACE, logfmt.str().c_str());
    return std::make_shared<NodeT>(fn, next);
}

std::shared_ptr<Node> NodeBuilder::make_moving_average(std::shared_ptr<Node> next,
                                                       aku_Timestamp step,
                                                       aku_Timestamp threshold,
                                                       aku_logger_cb_t logger)
{
    return std::make_shared<MovingAverage>(step, threshold, next);
}


ScanQueryProcessor::ScanQueryProcessor(std::shared_ptr<Node> root,
               std::vector<std::string> metrics,
               aku_Timestamp begin,
               aku_Timestamp end)
    : lowerbound_(std::min(begin, end))
    , upperbound_(std::max(begin, end))
    , direction_(begin > end ? AKU_CURSOR_DIR_BACKWARD : AKU_CURSOR_DIR_FORWARD)
    , metrics_(metrics)
    , namesofinterest_(StringTools::create_table(0x1000))
    , root_node_(root)
{
}

bool ScanQueryProcessor::start() {
    return true;
}

bool ScanQueryProcessor::put(const aku_Sample &sample) {
    return root_node_->put(sample);
}

void ScanQueryProcessor::stop() {
    root_node_->complete();
}

void ScanQueryProcessor::set_error(aku_Status error) {
    root_node_->set_error(error);
}

aku_Timestamp ScanQueryProcessor::lowerbound() const {
    return lowerbound_;
}

aku_Timestamp ScanQueryProcessor::upperbound() const {
    return upperbound_;
}

int ScanQueryProcessor::direction() const {
    return direction_;
}

MetadataQueryProcessor::MetadataQueryProcessor(std::vector<aku_ParamId> ids, std::shared_ptr<Node> node)
    : ids_(ids)
    , root_(node)
{
}

aku_Timestamp MetadataQueryProcessor::lowerbound() const {
    return AKU_MAX_TIMESTAMP;
}

aku_Timestamp MetadataQueryProcessor::upperbound() const {
    return AKU_MAX_TIMESTAMP;
}

int MetadataQueryProcessor::direction() const {
    return AKU_CURSOR_DIR_FORWARD;
}

bool MetadataQueryProcessor::start() {
    for (aku_ParamId id: ids_) {
        aku_Sample s;
        s.paramid = id;
        s.timestamp = 0;
        s.payload.type = aku_PData::NONE;
        if (!root_->put(s)) {
            return false;
        }
    }
    return true;
}

bool MetadataQueryProcessor::put(const aku_Sample &sample) {
    // no-op
    return false;
}

void MetadataQueryProcessor::stop() {
    root_->complete();
}

void MetadataQueryProcessor::set_error(aku_Status error) {
    root_->set_error(error);
}

}} // namespace
