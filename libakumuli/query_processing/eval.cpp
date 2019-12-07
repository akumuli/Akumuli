#include "eval.h"
#include <sstream>

namespace Akumuli {
namespace QP {


class ExpressionNode {
public:
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
        return mut[ixval_];
    }
};

enum class Operator {
    SUM,
    MUL,
};

class OperatorNode : public ExpressionNode {
    Operator op_;
    std::vector<const std::unique_ptr<ExpressionNode>> children_;
public:
    template<class It>
    OperatorNode(Operator op, It begin, It end)
        : op_(op)
        , children_(begin, end)
    {
    }
};

class ParseError : public std::runtime_error {
    int linenum_;
    std::string message_;
public:
    ParseError(const char* msg, int linenum)
        : linenum_(linenum)
        , message_(msg)
    {}

    std::string what() override {
        std::stringstream s;
        s << message_ << " at token " << linenum_;
        return s.str();
    }
};

template<class FwdIt>
Operator parseOperator(FwdIt origin, FwdIt& begin, FwdIt end) {
    // Invariant: *begin == operator
    int pos = static_cast<int>(begin - origin);
    if (begin == end) {
        ParseError error("Operator expected", pos);
        BOOST_THROW_EXCEPTION(error);
    }

    const auto& sym = *begin;
    begin++;

    if (sym == "+") {
        return Operator::SUM;
    }
    else if (sym == "*") {
        return Operator::MUL;
    }
    ParseError error("Unexpected operator", pos);
    BOOST_THROW_EXCEPTION(error);
}

template<class FwdIt>
std::unique_ptr<ExpressionNode> buildNextNode(FwdIt origin, FwdIt& begin, FwdIt end) {
    // Invariant: *begin == '('
    if (begin == end) {
        int pos = static_cast<int>(begin - origin);
        ParseError error("Empty expression", pos);
        BOOST_THROW_EXCEPTION(error);
    }
    Operator op = parseOperator(origin, begin, end);
    // Parse input sequence until matching ')' will be found
}

static std::unique_ptr<ExpressionNode> parseExpression(std::string const& expr) {
    // TODO: split expr by token
    // TODO: invoke buildNextNode
}


// ----
// Eval
// ----

Eval::Eval(const std::string& expr, std::shared_ptr<Node> next)
    : expr_(expr)
    , next_(next)
{
}

Eval::Eval(const boost::property_tree::ptree& ptree, std::shared_ptr<Node> next)
    : next_(next)
{
    auto const& expr = ptree.get_child_optional("expr");
    if (expr) {
        expr_ = expr->get_value<std::string>();
    }
}

void Eval::complete() {
    next_->complete();
}

bool Eval::put(MutableSample &mut) {
    // TODO: run expr
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
