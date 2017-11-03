#pragma once

#include <memory>
#include <unordered_map>

#include "../queryprocessor_framework.h"

namespace Akumuli {
namespace QP {

template <bool weighted>
struct SpaceSaver : Node {
    std::shared_ptr<Node> next_;

    struct Item {
        double        count;
        double        error;
        aku_Timestamp time;
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
        : next_(next)
        , N(0)
    {
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
        typedef std::unique_ptr<MutableSample> SamplePtr;
        std::vector<SamplePtr> samples;
        auto                   support = N * P;
        for (auto it : counters_) {
            auto estimate = it.second.count - it.second.error;
            if (support < estimate) {
                aku_Sample s;
                s.paramid         = it.first;
                s.payload.type    = AKU_PAYLOAD_FLOAT;
                s.payload.float64 = it.second.count;
                s.payload.size    = sizeof(aku_Sample);
                s.timestamp       = it.second.time;
                SamplePtr mut;
                mut.reset(new MutableSample(&s));
                samples.push_back(std::move(mut));
            }
        }
        std::sort(samples.begin(), samples.end(), [](const SamplePtr& lhs, const SamplePtr& rhs) {
            // Both values are guaranteed to be a scalars
            return *(*lhs)[0] > *(*rhs)[0];
        });
        for (const auto& s : samples) {
            if (!next_->put(*s)) {
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

    virtual bool put(MutableSample& sample) {
        // Require scalar
        if ((sample.payload_.sample.payload.type & AKU_PAYLOAD_FLOAT) != AKU_PAYLOAD_FLOAT) {
            // Query doesn't work with tuples
            set_error(AKU_EHIGH_CARDINALITY);
            return false;
        }
        double* val = sample[0];
        auto weight = 1.0;
        auto id     = sample.get_paramid();
        auto it     = counters_.find(id);
        if (weighted) {
            if (val) {
                weight = *val;
            } else {
                set_error(AKU_EMISSING_DATA_NOT_SUPPORTED);
                return false;
            }
        }
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
            counters_[id] = { count, error, sample.get_timestamp() };
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
