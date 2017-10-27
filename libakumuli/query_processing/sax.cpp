#include "sax.h"

namespace Akumuli {
namespace QP {

SAXNode::SAXNode(int alphabet_size, int window_width, bool disable_original_value, std::shared_ptr<Node> next)
    : next_(next)
    , window_width_(window_width)
    , alphabet_size_(alphabet_size)
    , disable_value_(disable_original_value)
    , inverse_(false)
{
    if (alphabet_size_ > 20 || alphabet_size_ < 1) {
        QueryParserError err("`alphabet_size` should be in [1, 20] range");
        BOOST_THROW_EXCEPTION(err);
    }
    if (window_width_ > 100 || window_width_ < 4) {
        QueryParserError err("`window_width` should be in [4, 100] range");
        BOOST_THROW_EXCEPTION(err);
    }
}

SAXNode::SAXNode(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next)
    : next_(next)
    , inverse_(false)
{
    alphabet_size_ = ptree.get<int>("alphabet_size");
    window_width_  = ptree.get<int>("window_width");
    disable_value_ = ptree.get<bool>("no_value", true);
    if (alphabet_size_ > 20 || alphabet_size_ < 1) {
        QueryParserError err("`alphabet_size` should be in [1, 20] range");
        BOOST_THROW_EXCEPTION(err);
    }
    if (window_width_ > 100 || window_width_ < 4) {
        QueryParserError err("`window_width` should be in [4, 100] range");
        BOOST_THROW_EXCEPTION(err);
    }
}

void SAXNode::complete() {
    next_->complete();
}

bool SAXNode::put(const aku_Sample &sample) {
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
        if (inverse_) {
            std::reverse(psample->payload.data, psample->payload.data + window_width_);
        }
        return next_->put(*psample);
    }
    return true;
}

void SAXNode::set_error(aku_Status status) {
    next_->set_error(status);
}

int SAXNode::get_requirements() const {
    return GROUP_BY_REQUIRED;
}

//static QueryParserToken<SAXNode> sax_token("sax");

}}  // namespace
