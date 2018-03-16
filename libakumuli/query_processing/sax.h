#pragma once

#include <memory>
#include <unordered_map>

#include "../queryprocessor_framework.h"
#include "../saxencoder.h"


namespace Akumuli {
namespace QP {

//                      //
//      SAX Encoder     //
//                      //

struct SAXNode : Node {

    std::shared_ptr<Node> next_;
    std::unordered_map<aku_ParamId, SAX::SAXEncoder> encoders_;
    int  window_width_;
    int  alphabet_size_;
    bool disable_value_;
    bool inverse_;
    char buffer_[MutableSample::MAX_PAYLOAD_SIZE];

    SAXNode(int alphabet_size, int window_width, bool disable_original_value,
            std::shared_ptr<Node> next);

    SAXNode(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next);

    virtual void complete();

    virtual bool put(MutableSample &sample);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};
}
}  // namespace
