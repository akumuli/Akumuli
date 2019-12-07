#pragma once

#include <memory>
#include <string>
#include "../queryprocessor_framework.h"

namespace Akumuli {
namespace QP {

/** Multiplies each value by it's weight
  */
struct Eval : Node {

    std::string expr_;
    std::shared_ptr<Node> next_;

    Eval(const std::string& expr, std::shared_ptr<Node> next);

    Eval(const boost::property_tree::ptree&, std::shared_ptr<Node> next);

    virtual void complete();

    virtual bool put(MutableSample& sample);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};

}
}  // namespace
