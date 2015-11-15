#include "randomsamplingnode.h"
#include <algorithm>

namespace Akumuli {
namespace QP {

RandomSamplingNode::RandomSamplingNode(uint32_t buffer_size, std::shared_ptr<Node> next)
    : buffer_size_(buffer_size)
    , next_(next)
{
    samples_.reserve(buffer_size);
}

RandomSamplingNode::RandomSamplingNode(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next)
    : buffer_size_(0)
    , next_(next)
{
    uint32_t size = ptree.get<uint32_t>("size");
    auto pbs = const_cast<uint32_t*>(&buffer_size_);
    *pbs = size;
    samples_.reserve(size);
}

int RandomSamplingNode::get_requirements() const {
    return EMPTY;
}

bool RandomSamplingNode::flush() {
    auto predicate = [](aku_Sample const& lhs, aku_Sample const& rhs) {
        auto l = std::make_tuple(lhs.timestamp, lhs.paramid);
        auto r = std::make_tuple(rhs.timestamp, rhs.paramid);
        return l < r;
    };

    std::stable_sort(samples_.begin(), samples_.end(), predicate);

    for(auto const& sample: samples_) {
        if (next_->put(sample) == false) {
            return false;
        }
    }
    samples_.clear();
    return true;
}

void RandomSamplingNode::complete() {
    flush();
    next_->complete();
}

bool RandomSamplingNode::put(const aku_Sample& sample) {
    if (sample.payload.type == aku_PData::MARGIN) {
        return flush();
    } else {
        if (samples_.size() < buffer_size_) {
            // Just append new values
            samples_.push_back(sample);
        } else {
            // Flip a coin
            uint32_t ix = random_() % samples_.size();
            if (ix < buffer_size_) {
                samples_.at(ix) = sample;
            }
        }
    }
    return true;
}

void RandomSamplingNode::set_error(aku_Status status) {
    next_->set_error(status);
}

static QueryParserToken<RandomSamplingNode> token("reservoir");

}}  // namespace
