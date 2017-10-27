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

std::vector<std::string> list_query_registry() {
    std::vector<std::string> names;
    for (auto kv: QueryParserRegistry::get().registry) {
        names.push_back(kv.first);
    }
    return names;
}

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
