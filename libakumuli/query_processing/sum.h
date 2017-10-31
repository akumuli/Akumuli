#pragma once

#include <memory>
#include "../queryprocessor_framework.h"

namespace Akumuli {
namespace QP {

/** Returns sum of all metrics
  */
struct Sum : Node {

    std::shared_ptr<Node> next_;
    bool ignore_missing_;

    Sum(bool ignore_missing, std::shared_ptr<Node> next);

    Sum(const boost::property_tree::ptree&ptree, std::shared_ptr<Node> next);

    virtual void complete();

    virtual bool put(MutableSample& sample);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};

}
}  // namespace
