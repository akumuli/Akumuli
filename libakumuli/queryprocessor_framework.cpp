#include "queryprocessor_framework.h"
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

std::ostream& operator << (std::ostream& str, QueryRange const& range) {
    auto qtype2text = [](QueryRange::QueryRangeType t) {
        if (t == QueryRange::QueryRangeType::CONTINUOUS) {
            return "CONTINUOUS";
        }
        return "INSTANT";
    };
    str << "[QueryRange| " << range.lowerbound << ", " << range.upperbound << ", "
        << (range.direction == AKU_CURSOR_DIR_FORWARD ? "forward" : "backward")
        << ", " << qtype2text(range.type) << "]";
    return str;
}


}}  // namespace
