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
#include "datetime.h"
#include "anomalydetector.h"

#include <random>
#include <algorithm>
#include <unordered_set>

#include <boost/lexical_cast.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

namespace Akumuli {
namespace QP {

static aku_Sample EMPTY_SAMPLE = {};

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

    bool flush() {
        auto predicate = [](aku_Sample const& lhs, aku_Sample const& rhs) {
            auto l = std::make_tuple(lhs.timestamp, lhs.paramid);
            auto r = std::make_tuple(rhs.timestamp, rhs.paramid);
            return l < r;
        };

        std::stable_sort(samples_.begin(), samples_.end(), predicate);

        for(auto const& sample: samples_) {
            if (next_->put(sample) == false) {
                return false;
            }
        }
        samples_.clear();
        return true;
    }

    virtual void complete() {
        flush();
        next_->complete();
    }

    virtual bool put(const aku_Sample& sample) {
        if (sample.payload.type == aku_PData::EMPTY) {
            return flush();
        } else {
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
        }
        return true;
    }

    void set_error(aku_Status status) {
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
        next_->complete();
    }

    virtual bool put(const aku_Sample& sample) {
        if (sample.payload.type == aku_PData::EMPTY) {
            return next_->put(sample);
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

// Generic sliding window

template<class State>
struct SlidingWindow : Node {
    aku_Timestamp const step_;
    bool first_hit_;
    aku_Timestamp lowerbound_, upperbound_;
    std::shared_ptr<Node> next_;
    std::unordered_map<aku_ParamId, State> counters_;

    SlidingWindow(aku_Timestamp step, std::shared_ptr<Node> next)
        : step_(step)
        , first_hit_(true)
        , lowerbound_(AKU_MIN_TIMESTAMP)
        , upperbound_(AKU_MIN_TIMESTAMP)
        , next_(next)
    {
    }

    bool average_samples() {
        for (auto& pair: counters_) {
            State& state = pair.second;
            if (state.ready()) {
                aku_Sample sample;
                sample.paramid = pair.first;
                sample.payload.value.float64 = state.value();
                sample.payload.type = AKU_PAYLOAD_FLOAT;
                sample.timestamp = upperbound_;
                state.reset();
                if (!next_->put(sample)) {
                    return false;
                }
            }
        }
        if (!next_->put(EMPTY_SAMPLE)) {
            return false;
        }
        return true;
    }

    virtual void complete() {
        average_samples();
        next_->complete();
    }

    virtual bool put(const aku_Sample &sample) {
        // ignore BLOBs
        if (sample.payload.type == AKU_PAYLOAD_FLOAT) {
            aku_ParamId id = sample.paramid;
            aku_Timestamp ts = sample.timestamp;
            if (AKU_UNLIKELY(first_hit_ == true)) {
                first_hit_ = false;
                aku_Timestamp aligned = ts / step_ * step_;
                // aligned <= ts
                lowerbound_ = aligned;
                upperbound_ = aligned + step_;
            }
            if (ts >= upperbound_) {
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
            }
            auto& state = counters_[id];
            state.add(sample);
        }
        return true;
    }

    virtual void set_error(aku_Status status) {
        next_->set_error(status);
    }

    virtual NodeType get_type() const {
        return Node::Resampler;
    }
};

struct MovingAverageCounter {
    double acc = 0;
    size_t num = 0;

    void reset() {
        acc = 0;
        num = 0;
    }

    double value() const {
        return acc/num;
    }

    bool ready() const {
        return num != 0;
    }

    void add(aku_Sample const& value) {
        acc += value.payload.value.float64;
        num++;
    }
};

struct MovingAverage : SlidingWindow<MovingAverageCounter> {

    MovingAverage(aku_Timestamp step, std::shared_ptr<Node> next)
        : SlidingWindow<MovingAverageCounter>(step, next)
    {
    }

    virtual NodeType get_type() const override {
        return Node::MovingAverage;
    }
};

struct MovingMedianCounter {
    // NOTE: median-of-medians or some approx. estimation method can be used here
    mutable std::vector<double> acc;

    void reset() {
        std::vector<double> tmp;
        std::swap(tmp, acc);
    }

    double value() const {
        if (acc.empty()) {
            AKU_PANIC("`ready` should be called first");
        }
        if (acc.size() < 2) {
            return acc.at(0);
        }
        auto middle = acc.begin();
        std::advance(middle, acc.size() / 2);
        std::partial_sort(acc.begin(), middle, acc.end());
        return *middle;
    }

    bool ready() const {
        return !acc.empty();
    }

    void add(aku_Sample const& value) {
        acc.push_back(value.payload.value.float64);
    }
};

struct MovingMedian : SlidingWindow<MovingMedianCounter> {

    MovingMedian(aku_Timestamp step, std::shared_ptr<Node> next)
        : SlidingWindow<MovingMedianCounter>(step, next)
    {
    }

    virtual NodeType get_type() const override {
        return Node::MovingMedian;
    }
};

template<bool weighted>
struct SpaceSaver : Node {
    std::shared_ptr<Node> next_;

    struct Item {
        double count;
        double error;
    };

    std::unordered_map<aku_ParamId, Item> counters_;
    //! Capacity
    double N;
    const size_t M;
    const double P;

    /** C-tor.
      * @param error is a allowed error value between 0 and 1
      * @param portion is a frequency (or weight) portion that we interested in
      * Object should report all items wich frequencies is greater then (portion-error)*N
      * where N is a number of elements (or total weight of all items in a stream).
      */
    SpaceSaver(double error, double portion, std::shared_ptr<Node> next)
        : next_(next)
        , N(0)
        , M(ceil(1.0/error))
        , P(portion)  // between 0 and 1
    {
        assert(P >= 0.0);
        assert(P <= 1.0);
    }

    bool count() {
        std::vector<aku_Sample> samples;
        auto support = N*P;
        for (auto it: counters_) {
            auto estimate = it.second.count - it.second.error;
            if (support < estimate) {
                aku_Sample s;
                s.paramid = it.first;
                s.payload.type = aku_PData::PARAMID_BIT|aku_PData::FLOAT_BIT;
                s.payload.value.float64 = it.second.count;
                samples.push_back(s);
            }
        }
        std::sort(samples.begin(), samples.end(), [](const aku_Sample& lhs, const aku_Sample& rhs) {
            return lhs.payload.value.float64 > rhs.payload.value.float64;
        });
        for (const auto& s: samples) {
            if (!next_->put(s)) {
                return false;
            }
        }
        counters_.clear();
        return true;
    }

    virtual void complete() {
        count();
        next_->complete();
    }

    virtual bool put(const aku_Sample &sample) {
        if (sample.payload.type == aku_PData::EMPTY) {
            return count();
        }
        if (weighted) {
            if ((sample.payload.type&aku_PData::FLOAT_BIT) == 0) {
                return true;
            }
        }
        auto id = sample.paramid;
        auto weight = weighted ? sample.payload.value.float64 : 1.0;
        auto it = counters_.find(id);
        if (it == counters_.end()) {
            // new element
            double count = weight;
            double error = 0;
            if (counters_.size() == M) {
                // remove element with smallest count
                size_t min = std::numeric_limits<size_t>::max();
                auto min_iter = it;
                for (auto i = counters_.begin(); i != counters_.end(); i++) {
                    if (i->second.count < min) {
                        min_iter = i;
                        min = i->second.count;
                    }
                }
                counters_.erase(min_iter);
                count += min;
                error  = min;
            }
            counters_[id] = { count, error };
        } else {
            // increment
            it->second.count += weight;
        }
        N += weight;
        return true;
    }

    virtual void set_error(aku_Status status) {
        next_->set_error(status);
    }

    virtual NodeType get_type() const {
        return Node::SpaceSaver;
    }
};


struct AnomalyDetector : Node {
    typedef std::unique_ptr<ForecastingMethod>          PSlidingWindow;
    typedef std::unique_ptr<CountingSketchProcessor>    PDetector;

    enum FcastMethod {
        SMA,
        EWMA
    };

    std::shared_ptr<Node> next_;
    PDetector detector_;

    AnomalyDetector(uint32_t nhashes, uint32_t bits, double threshold, uint32_t window_depth, FcastMethod method, std::shared_ptr<Node> next)
        : next_(next)
    {
        PSlidingWindow window;
        if (method == SMA) {
            window.reset(new SMASlidingWindow(window_depth));
        } else if (method == EWMA) {
            window.reset(new EWMASlidingWindow(window_depth));
        } else {
            AKU_PANIC("Unknown forecasting method");
        }
        detector_.reset(new CountingSketchProcessor(nhashes, 1 << bits, threshold, std::move(window)));
    }

    virtual void complete() {
        next_->complete();
    }

    virtual bool put(const aku_Sample &sample) {
        if (sample.payload.type == aku_PData::EMPTY) {
            detector_->move_sliding_window();
            return next_->put(sample);
        } else if (sample.payload.type & aku_PData::FLOAT_BIT) {
            if (sample.payload.value.float64 < 0.0) {
                set_error(AKU_EANOMALY_NEG_VAL);
                return false;
            }
            detector_->add(sample.paramid, sample.payload.value.float64);
            if (detector_->is_anomaly_candidate(sample.paramid)) {
                aku_Sample anomaly = sample;
                anomaly.payload.type |= aku_PData::URGENT;
                return next_->put(anomaly);
            }
        }
        // Ignore BLOBs
        return true;
    }

    virtual void set_error(aku_Status status) {
        next_->set_error(status);
    }

    virtual NodeType get_type() const {
        return Node::AnomalyDetector;
    }
};



//                                   //
//         Factory methods           //
//                                   //

std::shared_ptr<Node> NodeBuilder::make_sampler(boost::property_tree::ptree const& ptree,
                                                std::shared_ptr<Node> next,
                                                aku_logger_cb_t logger)
{
    // ptree = { "algorithm": "reservoir", "size": "1000" }
    // or
    // ptree = { "algorithm": "moving-average", "window": "100" }
    // or
    // ptree = { "algorithm": "moving-median", "window": "100" }
    // or
    // ptree = { "algorithm": "frequent-items", "capacity": "1000" }
    try {
        std::string name;
        name = ptree.get<std::string>("name");
        if (name == "reservoir") {
            // Reservoir sampling
            std::string size = ptree.get<std::string>("size");
            uint32_t nsize = boost::lexical_cast<uint32_t>(size);
            return std::make_shared<RandomSamplingNode>(nsize, next);
        } else if (name == "moving-average") {
            // Moving average
            std::string width = ptree.get<std::string>("window");  // sliding window width
            auto nwidth = DateTimeUtil::parse_duration(width.data(), width.size());
            return std::make_shared<MovingAverage>(nwidth, next);
        } else if (name == "moving-median") {
            // Moving median
            std::string width = ptree.get<std::string>("window");  // sliding window width
            aku_Timestamp nwidth = DateTimeUtil::parse_duration(width.data(), width.size());
            return std::make_shared<MovingMedian>(nwidth, next);
        } else if (name == "frequent-items") {
            std::string serror = ptree.get<std::string>("error");
            std::string sportion = ptree.get<std::string>("portion");
            double error = boost::lexical_cast<double>(serror);
            double portion = boost::lexical_cast<double>(sportion);
            return std::make_shared<SpaceSaver<false>>(error, portion, next);
        } else if (name == "heavy-hitters") {
            std::string serror = ptree.get<std::string>("error");
            std::string sportion = ptree.get<std::string>("portion");
            double error = boost::lexical_cast<double>(serror);
            double portion = boost::lexical_cast<double>(sportion);
            return std::make_shared<SpaceSaver<true>>(error, portion, next);
        } else if (name == "anomaly-detector") {
            std::string sbits = ptree.get<std::string>("bits", "16");
            std::string shash = ptree.get<std::string>("hashes", "3");
            std::string sthreshold = ptree.get<std::string>("threshold");
            std::string swindow = ptree.get<std::string>("window");
            std::string smethod = ptree.get<std::string>("method");
            uint32_t bits = boost::lexical_cast<uint32_t>(sbits);
            uint32_t window = boost::lexical_cast<uint32_t>(swindow);
            uint32_t hashes = boost::lexical_cast<uint32_t>(shash);
            double threshold = boost::lexical_cast<double>(sthreshold);
            AnomalyDetector::FcastMethod method;
            if (smethod == "ewma") {
                method = AnomalyDetector::EWMA;
            } else if (smethod == "sma") {
                method = AnomalyDetector::SMA;
            } else {
                QueryParserError err("Unknown forecasting method");
                BOOST_THROW_EXCEPTION(err);
            }
            return std::make_shared<AnomalyDetector>(hashes, bits, threshold, window, method, next);
        } else {
            // only this one is implemented
            NodeException except(Node::RandomSampler, "invalid sampler description, unknown algorithm");
            BOOST_THROW_EXCEPTION(except);
        }
    } catch (const boost::property_tree::ptree_error&) {
        NodeException except(Node::RandomSampler, "invalid sampler description");
        BOOST_THROW_EXCEPTION(except);
    } catch (const boost::bad_lexical_cast&) {
        NodeException except(Node::RandomSampler, "invalid sampler description, valid integer expected");
        BOOST_THROW_EXCEPTION(except);
    }
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
        s.payload.type = aku_PData::PARAMID_BIT;
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
