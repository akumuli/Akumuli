#pragma once

#include <memory>

#include "../queryprocessor_framework.h"

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

    // Update sliding window (irregullar one)
    void add(double value, aku_Timestamp next);

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

    virtual bool put(const aku_Sample& sample);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};

}
}  // namespace
