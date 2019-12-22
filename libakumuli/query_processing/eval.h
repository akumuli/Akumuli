#pragma once

#include <memory>
#include <string>
#include "../queryprocessor_framework.h"

namespace Akumuli {
namespace QP {

class ExpressionNode;

/** Multiplies each value by it's weight
  */
struct Eval : Node {

    std::unique_ptr<ExpressionNode> expr_;
    std::shared_ptr<Node> next_;

    //! Bild eval node using 'expr' field of the ptree object.
    Eval(const boost::property_tree::ptree& ptree, std::shared_ptr<Node> next);

    //! Bild eval node treating expr as a full expression tree (last parameter is
    //! ignored and needed only for overload resolution).
    Eval(const boost::property_tree::ptree& expr, std::shared_ptr<Node> next, bool);

    virtual void complete();

    virtual bool put(MutableSample& sample);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};

}
}  // namespace
