#include "queryprocessor_framework.h"
#include "storage_engine/tuples.h"
#include <map>

namespace Akumuli {
namespace QP {

struct QueryParserRegistry {
    std::map<std::string, BaseQueryParserToken const*> registry;
    static QueryParserRegistry& get() {
        static QueryParserRegistry inst;
        return inst;
    }
};

void add_queryparsertoken_to_registry(const BaseQueryParserToken *ptr) {
    QueryParserRegistry::get().registry[ptr->get_tag()] = ptr;
}

std::shared_ptr<Node> create_node(std::string tag, boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next) {
    auto& registry = QueryParserRegistry::get().registry;
    auto it = registry.find(tag);
    if (it == registry.end()) {
        std::string msg = "bad query, unknown tag: " + tag;
        QueryParserError except(msg.c_str());
        BOOST_THROW_EXCEPTION(except);
    }
    return it->second->create(ptree, next);
}

GroupByTime::GroupByTime()
    : step_(0)
    , first_hit_(true)
    , lowerbound_(AKU_MIN_TIMESTAMP)
    , upperbound_(AKU_MIN_TIMESTAMP)
{
}

GroupByTime::GroupByTime(aku_Timestamp step)
    : step_(step)
    , first_hit_(true)
    , lowerbound_(AKU_MIN_TIMESTAMP)
    , upperbound_(AKU_MIN_TIMESTAMP)
{
}

GroupByTime::GroupByTime(const GroupByTime& other)
    : step_(other.step_)
    , first_hit_(other.first_hit_)
    , lowerbound_(other.lowerbound_)
    , upperbound_(other.upperbound_)
{
}

GroupByTime& GroupByTime::operator = (const GroupByTime& other) {
    step_ = other.step_;
    first_hit_ = other.first_hit_;
    lowerbound_ = other.lowerbound_;
    upperbound_ = other.upperbound_;
    return *this;
}

bool GroupByTime::put(aku_Sample const& sample, Node& next) {
    if (step_ && sample.payload.type != aku_PData::EMPTY) {
        aku_Timestamp ts = sample.timestamp;
        if (AKU_UNLIKELY(first_hit_ == true)) {
            first_hit_ = false;
            aku_Timestamp aligned = ts / step_ * step_;
            lowerbound_ = aligned;
            upperbound_ = aligned + step_;
        }
        if (ts >= upperbound_) {
            // Forward direction
            aku_Sample empty = SAMPLING_HI_MARGIN;
            empty.timestamp = upperbound_;
            if (!next.put(empty)) {
                return false;
            }
            lowerbound_ += step_;
            upperbound_ += step_;
        } else if (ts < lowerbound_) {
            // Backward direction
            aku_Sample empty = SAMPLING_LO_MARGIN;
            empty.timestamp = upperbound_;
            if (!next.put(empty)) {
                return false;
            }
            lowerbound_ -= step_;
            upperbound_ -= step_;
        }
    }
    return next.put(sample);
}

bool GroupByTime::empty() const {
    return step_ == 0;
}

// ----------
// SampleUtil
// ----------

std::tuple<double, SampleUtil::Context> SampleUtil::get_value(const aku_Sample& sample) {
    double value = 0;
    Context chan = ERROR;
    bool is_tuple = TupleOutputUtils::is_one_element_tuple(&sample);
    if (sample.payload.type != AKU_PAYLOAD_FLOAT && !is_tuple) {
        return std::make_tuple(0.0, ERROR);
    }
    if (is_tuple) {
        value = TupleOutputUtils::get_first_value(&sample);
        chan  = TUPLE;
    } else {
        value = sample.payload.float64;
        chan  = SCALAR;
    }
    return std::make_tuple(value, chan);
}

bool SampleUtil::publish(SampleUtil::Context ctx, double newvalue, const aku_Sample& sample, Node* next) {
   switch (ctx) {
   case ERROR:
       return false;
   case SCALAR: {
       aku_Sample mut = sample;
       mut.payload.float64 = newvalue;
       return next->put(mut);
   }
   case TUPLE: {
       const size_t buffersize = sizeof(aku_Sample) + sizeof(double);
       char buffer[buffersize];
       auto mut = TupleOutputUtils::copy_sample(&sample, buffer, buffersize);
       TupleOutputUtils::set_first_value(mut, newvalue);
       return next->put(*mut);
   }
   };
   return false;
}

}}  // namespace
