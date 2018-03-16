#pragma once

#include <memory>
#include "../queryprocessor_framework.h"

namespace Akumuli {
namespace QP {

/** Returns absolute value
  */
struct Absolute : Node {

    std::vector<double> weights_;
    std::shared_ptr<Node> next_;

    Absolute(std::shared_ptr<Node> next);

    Absolute(const boost::property_tree::ptree&, std::shared_ptr<Node> next);

    virtual void complete();

    virtual bool put(MutableSample& sample);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};

}
}  // namespace
