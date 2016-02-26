#pragma once

#include <memory>
#include <unordered_map>

#include "../queryprocessor_framework.h"

namespace Akumuli {
namespace QP {

template <bool weighted> struct SpaceSaver : Node {
    std::shared_ptr<Node> next_;

    struct Item {
        double count;
        double error;
    };

    std::unordered_map<aku_ParamId, Item> counters_;
    //! Capacity
    double N;
    size_t M;
    double P;

    /** C-tor.
      * @param error is a allowed error value between 0 and 1
      * @param portion is a frequency (or weight) portion that we interested in
      * Object should report all items wich frequencies is greater then (portion-error)*N
      * where N is a number of elements (or total weight of all items in a stream).
      */
    SpaceSaver(double error, double portion, std::shared_ptr<Node> next)
        : next_(next)
        , N(0)
        , M(ceil(1.0 / error))
        , P(portion)  // between 0 and 1
    {
        assert(P >= 0.0);
        assert(P <= 1.0);
    }

    SpaceSaver(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next)
        : next_(next) {
        double error   = ptree.get<double>("error");
        double portion = ptree.get<double>("portion");
        if (error == 0.0) {
            QueryParserError error("`error` can't be 0.");
            BOOST_THROW_EXCEPTION(error);
        }
        M = ceil(1.0 / error);
        P = portion;
        if (P < 0.0) {
            QueryParserError error("`portion` can't be negative");
            BOOST_THROW_EXCEPTION(error);
        }
        if (P > 1.0) {
            QueryParserError error("`portion` can't be greater then 1.");
            BOOST_THROW_EXCEPTION(error);
        }
    }

    bool count() {
        std::vector<aku_Sample> samples;
        auto                    support = N * P;
        for (auto it : counters_) {
            auto estimate = it.second.count - it.second.error;
            if (support < estimate) {
                aku_Sample s;
                s.paramid         = it.first;
                s.payload.type    = aku_PData::PARAMID_BIT | aku_PData::FLOAT_BIT;
                s.payload.float64 = it.second.count;
                s.payload.size    = sizeof(aku_Sample);
                samples.push_back(s);
            }
        }
        std::sort(samples.begin(), samples.end(), [](const aku_Sample& lhs, const aku_Sample& rhs) {
            return lhs.payload.float64 > rhs.payload.float64;
        });
        for (const auto& s : samples) {
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

    virtual bool put(const aku_Sample& sample) {
        if (sample.payload.type > aku_PData::MARGIN) {
            return count();
        }
        if (weighted) {
            if ((sample.payload.type & aku_PData::FLOAT_BIT) == 0) {
                return true;
            }
        }
        auto id     = sample.paramid;
        auto weight = weighted ? sample.payload.float64 : 1.0;
        auto it     = counters_.find(id);
        if (it == counters_.end()) {
            // new element
            double count = weight;
            double error = 0;
            if (counters_.size() == M) {
                // remove element with smallest count
                size_t min      = std::numeric_limits<size_t>::max();
                auto   min_iter = it;
                for (auto i = counters_.begin(); i != counters_.end(); i++) {
                    if (i->second.count < min) {
                        min_iter = i;
                        min      = i->second.count;
                    }
                }
                counters_.erase(min_iter);
                count += min;
                error = min;
            }
            counters_[id] = { count, error };
        } else {
            // increment
            it->second.count += weight;
        }
        N += weight;
        return true;
    }

    virtual void set_error(aku_Status status) { next_->set_error(status); }

    virtual int get_requirements() const { return EMPTY | TERMINAL; }
};
}
}  // namespace
