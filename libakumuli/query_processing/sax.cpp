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
    if (static_cast<size_t>(window_width_) > MutableSample::MAX_PAYLOAD_SIZE || window_width_ < 4) {
        std::stringstream msg;
        msg << "`window_width` should be in [4, " << MutableSample::MAX_PAYLOAD_SIZE
            << "] range";
        QueryParserError err(msg.str().c_str());
        BOOST_THROW_EXCEPTION(err);
    }
}

void SAXNode::complete() {
    next_->complete();
}

bool SAXNode::put(MutableSample &sample) {
    if (sample.size() != 1) {
        // Not supported, SAX works only with scalars
        set_error(AKU_EHIGH_CARDINALITY);
        return false;
    }
    if ((sample.payload_.sample.payload.type & aku_PData::REGULLAR) == 0) {
        // Not supported, SAX require regullar data
        set_error(AKU_EREGULLAR_EXPECTED);
        return false;
    }
    auto key = sample.get_paramid();
    auto it = encoders_.find(key);
    if (it == encoders_.end()) {
        encoders_[key] = SAX::SAXEncoder(alphabet_size_, window_width_);
        it = encoders_.find(key);
    }
    double* value = sample[0];
    if (value) {
        if (it->second.encode(*value, buffer_, static_cast<size_t>(window_width_))) {
            sample.convert_to_sax_word(static_cast<u32>(window_width_));
            memcpy(sample.get_payload(), buffer_, static_cast<size_t>(window_width_));
            if (inverse_) {
                std::reverse(sample.get_payload(), sample.get_payload() + window_width_);
            }
            return next_->put(sample);
        }
    } else {
        // Missing data is not supported
        return false;
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
