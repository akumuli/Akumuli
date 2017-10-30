#include "queryprocessor_framework.h"
#include "storage_engine/tuples.h"
#include "util.h"
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


// -------------
// MutableSample
// -------------

MutableSample::MutableSample(const aku_Sample* source)
    : istuple_((source->payload.type & AKU_PAYLOAD_TUPLE) == AKU_PAYLOAD_TUPLE)
{
    auto size = std::max(sizeof(aku_Sample), static_cast<size_t>(source->payload.size));
    memcpy(payload_.raw, source, size);
    if (!istuple_) {
        size_ = 1;
        bitmap_ = 1;
    } else {
        std::tie(size_, bitmap_) = TupleOutputUtils::get_size_and_bitmap(source->payload.float64);
    }
}

u32 MutableSample::size() const {
    return size_;
}

void MutableSample::collapse() {
    if (istuple_ == false || size_ == 1) {
        // Nothing to do
        return;
    }
    size_ = 1;
    bitmap_ = 1;
    payload_.sample.payload.size = sizeof(aku_Sample) + sizeof(double);
    union {
        double d;
        u64 u;
    } bits;
    bits.u = 0x400000000000001ul;
    payload_.sample.payload.float64 = bits.d;
    double* first = reinterpret_cast<double*>(payload_.sample.payload.data);
    *first = 0.0;
}

static int count_ones(u64 value) {
    return value == 0 ? 0 : (64 - __builtin_clzll(value));
}

double* MutableSample::operator[] (u32 index) {
    if (istuple_) {
        const auto bit = static_cast<u32>(1 << index);
        // If `index` is out of range this branch wouldn't be taken
        // and nullptr will be returned.
        if (bitmap_ & bit) {
            // value is present
            // count 1's before index
            const auto mask = bit - 1;
            const auto tail = mask & bitmap_;
            const auto offset = count_ones(tail);
            double* tuple = reinterpret_cast<double*>(payload_.sample.payload.data);
            return tuple + offset;
        }
    } else if (index == 0) {
        return &payload_.sample.payload.float64;
    }
    return nullptr;
}

bool MutableSample::publish(Node* next) {
    return next->put(payload_.sample);
}

}}  // namespace
