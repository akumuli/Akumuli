#pragma once

#include <memory>
#include "../queryprocessor_framework.h"

namespace Akumuli {
namespace QP {

/** Multiplies each value by it's weight
  */
struct Scale : Node {

    std::vector<double> weights_;
    std::shared_ptr<Node> next_;

    Scale(std::vector<double> weights, std::shared_ptr<Node> next);

    Scale(const boost::property_tree::ptree&, std::shared_ptr<Node> next);

    virtual void complete();

    virtual bool put(MutableSample& sample);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};

}
}  // namespace
