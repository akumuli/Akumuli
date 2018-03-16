#pragma once

/**
  * @file This file contains various sliding window methods
  * for query processor.
  * The methods are:
  * - Simple moving average
  * - Exponentially weighted moving average
  * - Cumulative moving average
  */

#include <memory>

#include "../queryprocessor_framework.h"
#include <boost/circular_buffer.hpp>

namespace Akumuli {
namespace QP {

class EWMA {
    u32 warmup_;
    double value_;
    aku_Timestamp last_;
    double decay_;
public:
    EWMA();
    EWMA(double decay);

    // Update sliding window
    void add(double value);

    // Return current prediction (default_value will be used for warmup period)
    double get(double default_value) const;
};

struct EWMAPrediction : Node {
    double decay_;
    std::unordered_map<std::tuple<aku_ParamId, u32>, EWMA, KeyHash, KeyEqual> swind_;
    std::shared_ptr<Node> next_;
    const bool delta_;

    EWMAPrediction(double decay, bool calculate_delta, std::shared_ptr<Node> next);

    EWMAPrediction(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next);

    virtual void complete();

    virtual bool put(MutableSample& sample);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};


// -------------------
// SimpleMovingAverage
// -------------------

struct SMA {
    boost::circular_buffer<double> buffer_;
    double sum_;

    SMA();

    SMA(size_t n);

    void add(double value);

    double get() const;
};

struct SMAPrediction : Node {
    size_t width_;
    std::unordered_map<std::tuple<aku_ParamId, u32>, SMA, KeyHash, KeyEqual> swind_;
    std::shared_ptr<Node> next_;
    const bool delta_;

    SMAPrediction(size_t window_width, bool calculate_delta, std::shared_ptr<Node> next);

    SMAPrediction(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next);

    virtual void complete();

    virtual bool put(MutableSample& mut);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};

// -------------------------
// Cumulative moving average
// -------------------------

struct CMAPrediction : Node {
    std::unordered_map<std::tuple<aku_ParamId, u32>, std::pair<double, size_t>, KeyHash, KeyEqual> swind_;
    std::shared_ptr<Node> next_;

    CMAPrediction(std::shared_ptr<Node> next);

    CMAPrediction(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next);

    virtual void complete();

    virtual bool put(MutableSample& mut);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};

}
}  // namespace
