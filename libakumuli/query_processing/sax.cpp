#include "sax.h"

namespace Akumuli {
namespace QP {

SAXNode::SAXNode(int alphabet_size, int window_width, bool disable_original_value, std::shared_ptr<Node> next)
    : next_(next)
    , window_width_(window_width)
    , alphabet_size_(alphabet_size)
    , disable_value_(disable_original_value)
{
}

void SAXNode::complete() {
    next_->complete();
}

bool SAXNode::put(const aku_Sample &sample) {
    if (sample.payload.type != aku_PData::EMPTY) {
        SAX::SAXWord word;
        auto it = encoders_.find(sample.paramid);
        if (it == encoders_.end()) {
            encoders_[sample.paramid] = SAX::SAXEncoder(alphabet_size_, window_width_);
            it = encoders_.find(sample.paramid);
        }
        size_t ssize = sizeof(aku_Sample) + window_width_;
        void* ptr = alloca(ssize);
        aku_Sample* psample = new (ptr) aku_Sample();
        *psample = sample;
        psample->payload.size = ssize;
        psample->payload.type |= aku_PData::SAX_WORD;
        if (disable_value_) {
            psample->payload.type &= ~aku_PData::FLOAT_BIT;
        }
        if (it->second.encode(sample.payload.float64, psample->payload.data, window_width_)) {
            return next_->put(*psample);
        }
    }
    return true;
}

void SAXNode::set_error(aku_Status status) {
    next_->set_error(status);
}

Node::NodeType SAXNode::get_type() const {
    return Node::SAX;
}

}}  // namespace
