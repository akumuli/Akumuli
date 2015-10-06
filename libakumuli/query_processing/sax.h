#pragma once

#include <memory>
#include <unordered_map>

#include "../queryprocessor_fwd.h"
#include "../saxencoder.h"


namespace Akumuli {
namespace QP {

//                      //
//      SAX Encoder     //
//                      //

struct SAXNode : Node {

    std::shared_ptr<Node> next_;
    std::unordered_map<aku_ParamId, SAX::SAXEncoder> encoders_;
    int window_width_;
    int alphabet_size_;
    bool disable_value_;

    SAXNode(int alphabet_size, int window_width, bool disable_original_value, std::shared_ptr<Node> next);

    void complete();

    bool put(const aku_Sample &sample);

    void set_error(aku_Status status);

    NodeType get_type() const;
};

}}  // namespace
