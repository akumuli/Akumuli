#pragma once

#include <memory>
#include <string>
#include "../queryprocessor_framework.h"

namespace Akumuli {
namespace QP {

struct MuparserEvalImpl;

struct ExprEval : Node {
    std::shared_ptr<MuparserEvalImpl> impl_;

    ExprEval(const ExprEval&) = delete;
    ExprEval& operator = (const ExprEval&) = delete;

    //! Bild eval node using 'expr' field of the ptree object.
    ExprEval(const boost::property_tree::ptree& expr,
             const ReshapeRequest&              req,
             std::shared_ptr<Node>              next);

    virtual void complete();

    virtual bool put(MutableSample& sample);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};

class ExpressionNode;

/** Multiplies each value by it's weight
  */
struct Eval : Node {

    std::shared_ptr<ExpressionNode> expr_;
    std::shared_ptr<Node> next_;

    Eval(const Eval&) = delete;
    Eval& operator = (const Eval&) = delete;

    //! Bild eval node using 'expr' field of the ptree object.
    Eval(const boost::property_tree::ptree& expr, const ReshapeRequest &req, std::shared_ptr<Node> next);

    //! Bild eval node treating expr as a full expression tree (last parameter is
    //! ignored and needed only for overload resolution).
    Eval(const boost::property_tree::ptree& expr, const ReshapeRequest &req, std::shared_ptr<Node> next, bool);

    virtual void complete();

    virtual bool put(MutableSample& sample);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};

}
}  // namespace
