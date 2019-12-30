#include "eval.h"
#include <sstream>
#include <unordered_map>
#include <functional>

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

struct FunctionCallRegistry {
    typedef std::unique_ptr<ExpressionNode> NodeT;
    typedef std::function<NodeT (std::vector<NodeT>&&)> CtorT;

private:
    std::unordered_map<std::string, CtorT> registry_;
    FunctionCallRegistry() = default;

public:
    static FunctionCallRegistry& get() {
        static FunctionCallRegistry s_registry;
        return s_registry;
    }

    void add(std::string name, CtorT&& ctor) {
        registry_[std::move(name)] = ctor;
    }

    NodeT create(const std::string& fname, std::vector<NodeT>&& args) {
        NodeT res;
        auto it = registry_.find(fname);
        if (it == registry_.end()) {
            return res;
        }
        return it->second(std::move(args));
    }
};

/**
 * Function call expression node.
 * Base interface:
 * - double apply(It begin, It end);
 * - bool check_arity(size_t n, string* errormsg);
 * - static const char* func_name
 */
template<class Base>
struct FunctionCallNode : ExpressionNode, Base
{
    typedef FunctionCallNode<Base> NodeT;

    std::vector<std::unique_ptr<ExpressionNode>> children_;
    std::vector<double> args_;

    FunctionCallNode(FunctionCallNode const&) = delete;
    FunctionCallNode& operator = (FunctionCallNode const&) = delete;

    template<class ArgT>
    FunctionCallNode(ArgT&& args)
        : children_(std::forward<ArgT>(args))
        , args_(children_.size())
    {
        std::string errormsg;
        if (!static_cast<Base*>(this)->check_arity(children_.size(), &errormsg)) {
            ParseError err(std::string("function ") + Base::func_name + " error: " + errormsg);
            BOOST_THROW_EXCEPTION(err);
        }
        if (!static_cast<Base*>(this)->carry(children_, &errormsg)) {
            ParseError err(std::string("function ") + Base::func_name + " error: " + errormsg);
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

    static std::unique_ptr<NodeT> create_node(std::vector<std::unique_ptr<ExpressionNode>>&& args) {
        std::unique_ptr<NodeT> result;
        result.reset(new NodeT(std::move(args)));
        return result;
    }

private:
    struct RegistryToken {
        RegistryToken() {
            FunctionCallRegistry::get().add(Base::func_name, &create_node);
        }
    };

    static RegistryToken regtoken_;
};

template<class Base>
typename FunctionCallNode<Base>::RegistryToken FunctionCallNode<Base>::regtoken_;

struct BuiltInFunctions {
    struct BuiltInFn {
        bool carry(std::vector<std::unique_ptr<ExpressionNode>>&, std::string*) {
            // Default implementation
            return true;
        }
    };

    // Arithmetics
    struct Sum : BuiltInFn {
        template<class It>
        double apply(It begin, It end) {
            auto res = std::accumulate(begin, end, 0.0, [](double a, double b) {
                return a + b;
            });
            return res;
        }
        bool check_arity(size_t n, std::string* error) const {
            if (n == 0) {
                *error = "function require at least one parameter";
                return false;
            }
            return true;
        }
        constexpr static const char* func_name = "+";
    };

    struct Mul : BuiltInFn {
        template<class It>
        double apply(It begin, It end) {
            auto res = std::accumulate(begin, end, 1.0, [](double a, double b) {
                return a * b;
            });
            return res;
        }
        bool check_arity(size_t n, std::string* error) const {
            if (n == 0) {
                *error = "function require at least one parameter";
                return false;
            }
            return true;
        }
        constexpr static const char* func_name = "*";
    };

    // General
    struct Min : BuiltInFn {
        template<class It>
        double apply(It begin, It end) {
            auto it = std::min_element(begin, end);
            if (it != end) {
                return *it;
            }
            return NAN;
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

    struct Max : BuiltInFn {
        template<class It>
        double apply(It begin, It end) {
            auto it = std::max_element(begin, end);
            if (it != end) {
                return *it;
            }
            return NAN;
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

    struct Abs : BuiltInFn {
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

    // Windowed functions
    struct SMA : BuiltInFn {
        int N;
        int pos;
        double sum;
        std::vector<double> queue_;

        SMA() : N(0), pos(0), sum(0) {}

        bool carry(std::vector<std::unique_ptr<ExpressionNode>>& children, std::string* err) {
            static aku_Sample empty;
            static MutableSample mempty(&empty);
            // First parameter is supposed to be constant
            auto& c = children.front();
            auto cnode = dynamic_cast<ConstantNode*>(c.get());
            if (cnode == nullptr) {
                *err = "first 'sma' parameter should be constant";
                return false;
            }
            double val = cnode->eval(mempty);
            N = static_cast<int>(val);
            queue_.resize(N);
            children.erase(children.begin());
            return true;
        }

        template<class It>
        double apply(It begin, It end) {
            assert(begin != end);
            queue_.at(pos % N) = *begin;
            pos++;
            if (pos > N) {
                double prev = queue_.at(static_cast<size_t>((pos - N) % N));
                sum -= prev;
                return sum / N;
            }
            return sum / pos;
        }
        bool check_arity(size_t n, std::string* error) const {
            if (n == 2) {
                return true;
            }
            *error = "two arguments expected";
            return false;
        }
        constexpr static const char* func_name = "sma";
    };
};

// Arithmetic
template struct FunctionCallNode<BuiltInFunctions::Sum>;
template struct FunctionCallNode<BuiltInFunctions::Mul>;
// General
template struct FunctionCallNode<BuiltInFunctions::Max>;
template struct FunctionCallNode<BuiltInFunctions::Min>;
template struct FunctionCallNode<BuiltInFunctions::Abs>;
// Window methods
template struct FunctionCallNode<BuiltInFunctions::SMA>;

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
    res = FunctionCallRegistry::get().create(op, std::move(args));
    if (!res) {
        ParseError err("unknown operation '" + op + "'");
        BOOST_THROW_EXCEPTION(err);
    }
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
