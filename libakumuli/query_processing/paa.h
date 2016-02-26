#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "../queryprocessor_framework.h"

namespace Akumuli {
namespace QP {


//! Generic piecewise aggregate approximation
template <class State> struct PAA : Node {
    std::shared_ptr<Node> next_;
    std::unordered_map<aku_ParamId, State> counters_;

    PAA(std::shared_ptr<Node> next)
        : next_(next) {}

    bool average_samples(aku_Sample const& margin) {
        std::vector<aku_ParamId> ids;
        for (auto& pair : counters_) {
            ids.push_back(pair.first);
        }
        if (margin.payload.type == aku_PData::LO_MARGIN) {
            // Moving in backward direction
            std::sort(ids.begin(), ids.end(), std::greater<aku_ParamId>());
        } else {
            // Moving forward
            std::sort(ids.begin(), ids.end(), std::less<aku_ParamId>());
        }
        for (auto id : ids) {
            State& state = counters_[id];
            if (state.ready()) {
                aku_Sample sample;
                sample.paramid         = id;
                sample.payload.float64 = state.value();
                sample.payload.type    = AKU_PAYLOAD_FLOAT;
                sample.payload.size    = sizeof(aku_Sample);
                sample.timestamp       = margin.timestamp;
                state.reset();
                if (!next_->put(sample)) {
                    return false;
                }
            }
        }
        if (!next_->put(margin)) {
            return false;
        }
        return true;
    }

    virtual void complete() { next_->complete(); }

    virtual bool put(const aku_Sample& sample) {
        if (sample.payload.type > aku_PData::MARGIN) {
            if (!average_samples(sample)) {
                return false;
            }
        } else {
            auto& state = counters_[sample.paramid];
            state.add(sample);
        }
        return true;
    }

    virtual void set_error(aku_Status status) { next_->set_error(status); }

    virtual int get_requirements() const { return GROUP_BY_REQUIRED; }
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

    MeanPAA(boost::property_tree::ptree const&, std::shared_ptr<Node> next);
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

    MedianPAA(boost::property_tree::ptree const&, std::shared_ptr<Node> next);
};

template <class SelectFn> struct ValueSelector {
    double acc;
    size_t num;

    void reset() {
        acc = 0;
        num = 0;
    }

    double value() const { return acc; }

    bool ready() const { return num != 0; }

    void add(aku_Sample const& value) {
        if (!num) {
            acc = value.payload.float64;
        } else {
            SelectFn fn;
            acc = fn(acc, value.payload.float64);
        }
        num++;
    }
};

template <class SelectFn> struct GenericPAA : PAA<ValueSelector<SelectFn>> {
    GenericPAA(std::shared_ptr<Node> next)
        : PAA<ValueSelector<SelectFn>>(next) {}

    GenericPAA(boost::property_tree::ptree const&, std::shared_ptr<Node> next)
        : PAA<ValueSelector<SelectFn>>(next) {}
};
}
}  // namespace
