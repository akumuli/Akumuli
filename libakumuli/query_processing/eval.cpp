#include "eval.h"
#include <sstream>

namespace Akumuli {
namespace QP {


class ExpressionNode {
public:
    virtual ~ExpressionNode() = default;
    virtual double eval(MutableSample& mut) = 0;
};

class ConstantNode : public ExpressionNode {
    const double cval_;
public:
    ConstantNode(double value)
        : cval_(value)
    {
    }

    double eval(MutableSample&) override {
        return cval_;
    }
};

class ValueNode : public ExpressionNode {
    int ixval_;
public:
    ValueNode(int ixval)
        : ixval_(ixval)
    {
    }

    double eval(MutableSample& mut) override {
        return *mut[ixval_];
    }
};

enum class ExpressionOperator {
    SUM,
    MUL,
};

std::ostream& operator << (std::ostream& s, ExpressionOperator op) {
    switch (op) {
    case ExpressionOperator::MUL:
        s << "*";
        break;
    case ExpressionOperator::SUM:
        s << "+";
        break;
    }
    return s;
}

std::istream& operator >> (std::istream& s, ExpressionOperator& op) {
    char b;
    s.read(&b, 1);
    switch (b) {
    case '*':
        op = ExpressionOperator::MUL;
        break;
    case '+':
        op = ExpressionOperator::SUM;
        break;
    }
    return s;
}

class OperatorNode : public ExpressionNode {
    ExpressionOperator op_;
    std::vector<std::unique_ptr<ExpressionNode>> children_;
public:
    OperatorNode(ExpressionOperator op, std::vector<std::unique_ptr<ExpressionNode>>&& args)
        : op_(op)
        , children_(std::move(args))
    {
    }

    double eval(MutableSample& mut) override {
        std::vector<double> args;
        std::transform(children_.begin(), children_.end(), std::back_inserter(args),
                       [&mut](std::unique_ptr<ExpressionNode>& node) {
                           return node->eval(mut);
                       });
        switch(op_) {
        case ExpressionOperator::MUL:
            return std::accumulate(args.begin(), args.end(), 1.0, [](double a, double b) {
                return a * b;
            });
        case ExpressionOperator::SUM:
            return std::accumulate(args.begin(), args.end(), 0.0, [](double a, double b) {
                return a + b;
            });
        }
        return NAN;
    }
};

class ParseError : public std::exception {
    const std::string message_;

public:
    ParseError(const char* msg)
        : message_(msg)
    {
    }

    const char* what() const noexcept override {
        return message_.c_str();
    }
};

template<class FwdIt>
ExpressionOperator parseOperator(FwdIt origin, FwdIt& begin, FwdIt end) {
    // Invariant: *begin == operator
    int pos = static_cast<int>(begin - origin);
    if (begin == end) {
        ParseError error("Operator expected", pos);
        BOOST_THROW_EXCEPTION(error);
    }

    const auto& sym = *begin;
    begin++;

    if (sym == "+") {
        return ExpressionOperator::SUM;
    }
    else if (sym == "*") {
        return ExpressionOperator::MUL;
    }
    ParseError error("Unexpected operator", pos);
    BOOST_THROW_EXCEPTION(error);
}

typedef boost::property_tree::ptree PTree;
static const int DEPTH_LIMIT = 10;

std::unique_ptr<ExpressionNode> buildNode(int depth, const PTree& node) {
    if (depth == DEPTH_LIMIT) {
        ParseError err("expression depth limit exceded");
        BOOST_THROW_EXCEPTION(err);
    }
    // Expect array of: [op, arg1, arg2, ...,argN]
    // i-th arg can be a node in which case recursive call
    // have to be made.
    std::string op;
    std::vector<std::unique_ptr<ExpressionNode>> args;
    for (auto it = node.begin(); it != node.end(); it++) {
        if (it == node.begin()) {
            // Parse operator
            if (!it->first.empty()) {
                // Expect value here
                ParseError err("operator or function expected");
                BOOST_THROW_EXCEPTION(err);
            }
            else {
                op = it->second.data();
            }
        }
        else {
            // Parse arguments
            if (!it->first.empty()) {
                // Build sub-node
                auto arg = buildNode(depth + 1, it->second);
                args.push_back(std::move(arg));
            }
            else {
                std::unique_ptr<ExpressionNode> node;
                auto value = it->second.data();
                std::stringstream str(value);
                double xs;
                str >> xs;
                if (!str.fail()) {
                    node.reset(new ConstantNode(xs));
                }
                else {
                    // TODO: lookup value in the dictionary
                    throw "Not implemented";
                }
                args.push_back(std::move(node));
            }
        }
    }
    std::unique_ptr<ExpressionNode> res;
    std::stringstream sop(op);
    ExpressionOperator exop;
    sop >> exop;
    res.reset(new OperatorNode(exop, std::move(args)));
    return res;
}

static std::unique_ptr<ExpressionNode> buildTree(const PTree& expr) {
    return buildNode(0, expr);
}


// ----
// Eval
// ----


Eval::Eval(const boost::property_tree::ptree& ptree, std::shared_ptr<Node> next)
    : next_(next)
{
    auto const& expr = ptree.get_child_optional("expr");
    if (expr) {
        expr_ = buildTree(*expr);
    }
}

Eval::Eval(const boost::property_tree::ptree& expr, std::shared_ptr<Node> next, bool)
    : next_(next)
{
    expr_ = buildTree(expr);
}

void Eval::complete() {
    next_->complete();
}

bool Eval::put(MutableSample &mut) {
    double val = expr_->eval(mut);
    mut.collapse();
    *mut[0] = val;
    return next_->put(mut);
}

void Eval::set_error(aku_Status status) {
    next_->set_error(status);
}

int Eval::get_requirements() const {
    return TERMINAL;
}

static QueryParserToken<Eval> scale_token("eval");

}}  // namespace
