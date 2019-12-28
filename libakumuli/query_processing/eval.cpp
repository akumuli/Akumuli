#include "eval.h"
#include <sstream>

namespace Akumuli {
namespace QP {


class ParseError : public std::exception {
    const std::string message_;

public:
    ParseError(const char* msg)
        : message_(msg)
    {
    }

    ParseError(std::string msg)
        : message_(std::move(msg))
    {
    }

    const char* what() const noexcept override {
        return message_.c_str();
    }
};

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
        return *mut[static_cast<u32>(ixval_)];
    }
};

enum class ExpressionOperator {
    UNKNOWN,
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
    case ExpressionOperator::UNKNOWN:
        s << "_unknown_";
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
    default:
        op = ExpressionOperator::UNKNOWN;
        break;
    }
    return s;
}

class OperatorNode : public ExpressionNode {
    ExpressionOperator op_;
    std::vector<std::unique_ptr<ExpressionNode>> children_;
    std::vector<double> args_;
public:
    OperatorNode(ExpressionOperator op, std::vector<std::unique_ptr<ExpressionNode>>&& args)
        : op_(op)
        , children_(std::move(args))
        , args_(children_.size())
    {
    }

    double eval(MutableSample& mut) override {
        std::transform(children_.begin(), children_.end(), args_.begin(),
                       [&mut](std::unique_ptr<ExpressionNode>& node) {
                           return node->eval(mut);
                       });
        switch(op_) {
        case ExpressionOperator::MUL:
            return std::accumulate(args_.begin(), args_.end(), 1.0, [](double a, double b) {
                return a * b;
            });
        case ExpressionOperator::SUM:
            return std::accumulate(args_.begin(), args_.end(), 0.0, [](double a, double b) {
                return a + b;
            });
        case ExpressionOperator::UNKNOWN:
            break;
        }
        return NAN;
    }
};

/**
 * Function call expression node.
 * Base interface:
 * - double apply(It begin, It end);
 * - bool check_arity(size_t n, string* errormsg);
 * - static const char* func_name
 * - static const char* func_help
 */
template<class Base>
struct FunctionCallNode : ExpressionNode, Base
{
    std::vector<std::unique_ptr<ExpressionNode>> children_;
    std::vector<double> args_;

    FunctionCallNode(ExpressionOperator op, std::vector<std::unique_ptr<ExpressionNode>>&& args)
        : children_(std::move(args))
        , args_(children_.size())
    {
        std::string errormsg;
        if (!static_cast<Base*>(this)->check_arity(children_.size(), &errormsg)) {
            ParseError err("function " + Base::func_name + " error: " + errormsg);
            BOOST_THROW_EXCEPTION(err);
        }
    }

    double eval(MutableSample& mut) override {
        std::transform(children_.begin(), children_.end(), args_.begin(),
                       [&mut](std::unique_ptr<ExpressionNode>& node) {
                           return node->eval(mut);
                       });
        return static_cast<Base*>(this)->apply(args_.begin(), args_.end());
    }
};

struct BuiltInFunctions {
    struct Min {
        template<class It>
        double apply(It begin, It end) {
            return std::min_element(begin, end);
        }
        bool check_arity(size_t n, std::string* error) const {
            if (n == 0) {
                *error = "function require at least one parameter";
                return false;
            }
            return true;
        }
        constexpr static const char* func_name = "min";
    };

    struct Max {
        template<class It>
        double apply(It begin, It end) {
            return std::max_element(begin, end);
        }
        bool check_arity(size_t n, std::string* error) const {
            if (n == 0) {
                *error = "function require at least one parameter";
                return false;
            }
            return true;
        }
        constexpr static const char* func_name = "max";
    };

    struct Abs {
        template<class It>
        double apply(It begin, It end) {
            return std::abs(*begin);
        }
        bool check_arity(size_t n, std::string* error) const {
            if (n == 1) {
                return true;
            }
            *error = "single argument expected";
            return false;
        }
        constexpr static const char* func_name = "abs";
    };
};

typedef boost::property_tree::ptree PTree;
static const int DEPTH_LIMIT = 10;

template<class LookupFn>
std::unique_ptr<ExpressionNode> buildNode(int depth, const PTree& node, const LookupFn& lookup) {
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
            auto value = it->second.data();
            if (value.empty()) {
                // Build sub-node
                auto arg = buildNode(depth + 1, it->second, lookup);
                args.push_back(std::move(arg));
            }
            else {
                std::unique_ptr<ExpressionNode> node;
                std::stringstream str(value);
                double xs;
                str >> xs;
                if (!str.fail()) {
                    node.reset(new ConstantNode(xs));
                }
                else {
                    auto ix = lookup(value);
                    if (ix < 0) {
                        // Field can't be found
                        ParseError err("unknown field '" + value + "'");
                        BOOST_THROW_EXCEPTION(err);
                    }
                    node.reset(new ValueNode(ix));
                }
                args.push_back(std::move(node));
            }
        }
    }
    std::unique_ptr<ExpressionNode> res;
    std::stringstream sop(op);
    ExpressionOperator exop;
    sop >> exop;
    if (exop == ExpressionOperator::UNKNOWN) {
        ParseError err("unknown operator '" + op + "'");
        BOOST_THROW_EXCEPTION(err);
    }
    res.reset(new OperatorNode(exop, std::move(args)));
    return res;
}


// ----
// Eval
// ----


static std::unordered_map<std::string, int> buildNameToIndexMapping(const QP::ReshapeRequest& req)
{
    std::unordered_map<std::string, int> result;
    const int ncol = static_cast<int>(req.select.columns.size());
    for(int ix = 0; ix < ncol; ix++) {
        if (req.select.columns[ix].ids.empty()) {
            continue;
        }
        auto idcol = req.select.columns[ix].ids.front();
        auto rawstr = req.select.matcher->id2str(idcol);
        // copy metric name from the begining until the ' ' or ':'
        std::string sname(rawstr.first, rawstr.first + rawstr.second);
        auto it = std::find_if(sname.begin(), sname.end(), [](char c) {
            return std::isspace(c) || c == ':';
        });
        result[std::string(sname.begin(), it)] = ix;
    }
    return result;
}

Eval::Eval(const boost::property_tree::ptree& ptree, const ReshapeRequest& req, std::shared_ptr<Node> next)
    : next_(next)
{
    auto const& expr = ptree.get_child_optional("expr");
    if (expr) {
        std::unordered_map<std::string, int> lazyInitMap;
        bool initialized = false;
        auto lookupFn = [&](const std::string& fld) {
            if (!initialized) {
                initialized = true;
                lazyInitMap = buildNameToIndexMapping(req);
            }
            auto it = lazyInitMap.find(fld);
            if (it == lazyInitMap.end()) {
                return -1;
            }
            return it->second;
        };
        expr_ = buildNode(0, *expr, lookupFn);
    }
}

Eval::Eval(const boost::property_tree::ptree& expr, const ReshapeRequest& req, std::shared_ptr<Node> next, bool)
    : next_(next)
{
    std::unordered_map<std::string, int> lazyInitMap;
    bool initialized = false;
    auto lookupFn = [&](const std::string& fld) {
        if (!initialized) {
            initialized = true;
            lazyInitMap = buildNameToIndexMapping(req);
        }
        auto it = lazyInitMap.find(fld);
        if (it == lazyInitMap.end()) {
            return -1;
        }
        return it->second;
    };
    expr_ = buildNode(0, expr, lookupFn);
}

void Eval::complete() {
    next_->complete();
}

bool Eval::put(MutableSample &mut) {
    double val = expr_->eval(mut);
    mut.collapse();
    if (std::isnormal(val)) {
        *mut[0] = val;
        return next_->put(mut);
    }
    return true;
}

void Eval::set_error(aku_Status status) {
    next_->set_error(status);
}

int Eval::get_requirements() const {
    return TERMINAL;
}

static QueryParserToken<Eval> scale_token("eval");

}}  // namespace
