#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "../queryprocessor_framework.h"

namespace Akumuli {
namespace QP {


//! Generic piecewise aggregate approximation
template<class State>
struct PAA : Node {
    std::shared_ptr<Node> next_;
    std::unordered_map<aku_ParamId, State> counters_;

    PAA(boost::property_tree::ptree const&, std::shared_ptr<Node> next)
        : next_(next)
    {
    }

    bool average_samples(aku_Timestamp ts) {
        for (auto& pair: counters_) {
            State& state = pair.second;
            if (state.ready()) {
                aku_Sample sample;
                sample.paramid = pair.first;
                sample.payload.float64 = state.value();
                sample.payload.type = AKU_PAYLOAD_FLOAT;
                sample.payload.size = sizeof(aku_Sample);
                sample.timestamp = ts;
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
        next_->complete();
    }

    virtual bool put(const aku_Sample &sample) {
        // ignore BLOBs
        if (sample.payload.type == aku_PData::EMPTY) {
            if (!average_samples(sample.timestamp)) {
                return false;
            }
        } else {
            auto& state = counters_[sample.paramid];
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


struct MeanCounter {
    double acc = 0;
    size_t num = 0;

    void reset();

    double value() const;

    bool ready() const;

    void add(aku_Sample const& value);
};

struct MeanPAA : PAA<MeanCounter> {

    MeanPAA(std::shared_ptr<Node> next);

    virtual NodeType get_type() const override;
};

struct MedianCounter {
    mutable std::vector<double> acc;

    void reset();

    double value() const;

    bool ready() const;

    void add(aku_Sample const& value);
};

struct MedianPAA : PAA<MedianCounter> {

    MedianPAA(std::shared_ptr<Node> next);

    virtual NodeType get_type() const override;
};

}}  // namespace
