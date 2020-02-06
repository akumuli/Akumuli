#include "eval.h"
#include <sstream>
#include <unordered_map>
#include <functional>
#include <set>

#include <boost/algorithm/string.hpp>

#include "muParser.h"

namespace Akumuli {
namespace QP {

static std::unordered_map<std::string, int> buildNameToIndexMapping(const QP::ReshapeRequest& req)
{
    std::unordered_map<std::string, int> result;
    const int ncol = static_cast<int>(req.select.columns.size());
    const SeriesMatcherBase* matcher = req.select.global_matcher;
    for(int ix = 0; ix < ncol; ix++) {
        if (req.select.columns[ix].ids.empty()) {
            continue;
        }
        auto idcol = req.select.columns[ix].ids.front();
        auto rawstr = matcher->id2str(idcol);
        // copy metric name from the begining until the ' ' or ':'
        std::string sname(rawstr.first, rawstr.first + rawstr.second);
        auto it = std::find_if(sname.begin(), sname.end(), [](char c) {
            return std::isspace(c) || c == ':';
        });
        result[std::string(sname.begin(), it)] = ix;
    }
    return result;
}

// Muparser based implementation
struct MuparserEvalImpl : Node {
    enum {
        MAX_VALUES = MutableSample::MAX_PAYLOAD_SIZE / sizeof(double)
    };
    u32 nfields_;
    std::array<u32, MAX_VALUES> indexes_;
    std::array<double, MAX_VALUES> values_;
    mu::Parser parser_;
    std::shared_ptr<Node> next_;

    MuparserEvalImpl(const MuparserEvalImpl&) = delete;
    MuparserEvalImpl& operator = (const MuparserEvalImpl&) = delete;

    static std::string preProcessExpression(std::string input,
                                            const ReshapeRequest& req,
                                            std::map<std::string, std::string>* varmap)
    {
        std::vector<std::string> vars;
        for (auto const& col: req.select.columns) {
            if (col.ids.empty()) {
                continue;
            }
            auto id = col.ids.front();
            auto st = req.select.global_matcher->id2str(id);
            vars.push_back(std::string(st.first, st.first + st.second));
        }
        for (u32 i = 0; i < vars.size(); i++) {
            std::string varname = "_var_" + std::to_string(i);
            varmap->insert(std::make_pair(varname, vars[i]));
            boost::algorithm::replace_all(input, vars[i], varname);
        }
        return input;
    }

    //! Bild eval node using 'expr' field of the ptree object.
    MuparserEvalImpl(const boost::property_tree::ptree& ptree,
                     const ReshapeRequest&              req,
                     std::shared_ptr<Node>              next)
        : next_(next)
    {
        std::unordered_map<std::string, int> fields = buildNameToIndexMapping(req);
        std::map<std::string, std::string> varmap;
        auto const& expr = ptree.get_child_optional("expr");
        if (expr) {
            try {
                parser_.EnableOptimizer(true);
                auto str = expr->get_value<std::string>("");
                auto pstr = preProcessExpression(str, req, &varmap);
                parser_.SetExpr(str);
            } catch (mu::ParserError const& error) {
                std::stringstream msg;
                msg << "Expression parsing error at: " << static_cast<int>(error.GetPos())
                    << " token: " << error.GetToken()
                    << " message: " << error.GetMsg();
                QueryParserError qerr(msg.str());
            }
        }
        else {
            QueryParserError err("'expr' field required");
            BOOST_THROW_EXCEPTION(err);
        }
        auto used = parser_.GetUsedVar();
        nfields_ = static_cast<u32>(used.size());
        auto ix = indexes_.begin();
        auto vx = values_.begin();
        std::set<std::string> defined;
        // The indexes in indexes_ array should be sorted to
        // improve runtime performance.
        for (auto const& kv: fields) {
            auto varname = varmap[kv.first];
            if (used.count(varname)) {
                parser_.DefineVar(varname, vx);
                *vx++ = 0.;
                *ix++ = static_cast<u32>(kv.second);
                defined.insert(varname);
            }
        }
        std::stringstream msg;
        msg << "Unknown variable [";
        bool error = false;
        for (auto kv: used) {
            if (!defined.count(kv.first)) {
                if (!error) {
                    msg << kv.first;
                }
                else {
                    msg << ", " << kv.first;
                }
                error = true;
            }
        }
        if (error) {
            msg << "]";
            QueryParserError parsererr(msg.str());
            BOOST_THROW_EXCEPTION(parsererr);
        }
        if (parser_.GetNumResults() != 1) {
            QueryParserError parsererr("Mutiple results are not supported");
            BOOST_THROW_EXCEPTION(parsererr);
        }
    }

    virtual void complete() {
        next_->complete();
    }

    virtual bool put(MutableSample& mut) {
        for (u32 i = 0; i < nfields_; i++) {
            auto xs = mut[indexes_[i]];
            values_[i] = xs ? *xs : 0.0;
        }
        double val = parser_.Eval();
        mut.collapse();
        if (!std::isnan(val)) {
            *mut[0] = val;
            return next_->put(mut);
        }
        return true;
    }

    virtual void set_error(aku_Status status) {
        next_->set_error(status);
    }

    virtual int get_requirements() const {
        return TERMINAL;
    }
};

// ExprtkEval

ExprEval::ExprEval(const boost::property_tree::ptree& expr,
                   const ReshapeRequest&              req,
                   std::shared_ptr<Node>              next)
    : impl_(std::make_shared<MuparserEvalImpl>(expr, req, next))
{
}

void ExprEval::complete()
{
    impl_->complete();
}

bool ExprEval::put(MutableSample& sample)
{
    return impl_->put(sample);
}

void ExprEval::set_error(aku_Status status)
{
    impl_->set_error(status);
}

int ExprEval::get_requirements() const
{
    return impl_->get_requirements();
}


// ------------------ native impl ----------------

class ExpressionNode {
public:
    virtual ~ExpressionNode() = default;
    virtual double eval(MutableSample& mut) = 0;
    virtual std::tuple<double, bool> fold() = 0;
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

    std::tuple<double, bool> fold() override {
        return std::make_tuple(cval_, true);
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

    std::tuple<double, bool> fold() override {
        return std::make_tuple(0.0, false);
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
 * - double call(It begin, It end);
 * - static const char* func_name
 * - bool apply(args, string* errormsg);
 *   - Partially apply function arguments
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
    {
        std::string errormsg;
        if (!static_cast<Base*>(this)->apply(children_, &errormsg)) {
            QueryParserError err(std::string("function ") + Base::func_name + " error: " + errormsg);
            BOOST_THROW_EXCEPTION(err);
        }
        args_.resize(children_.size(), 0);
    }

    std::tuple<double, bool> fold() override {
        if (args_.empty()) {
            // Can do this since it's folded, stateful functions can't be called this way
            // but they won't be folded. Constant folding can fully simplify only pure functions.
            double res = static_cast<Base*>(this)->call(0, 0, args_.begin(), args_.end());
            return std::make_tuple(res, true);
        }
        return std::make_tuple(0.0, false);
    }

    double eval(MutableSample& mut) override {
        std::transform(children_.begin(), children_.end(), args_.begin(),
                       [&mut](std::unique_ptr<ExpressionNode>& node) {
                           return node->eval(mut);
                       });
        return static_cast<Base*>(this)->call(mut.get_paramid(), mut.get_timestamp(), args_.begin(), args_.end());
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

// Common namespace for all built-in functions and operators
namespace Builtins {
    /*
     * Arithmetic:
     * +, -, *, /, modulo power
     *
     * Comparisons:
     * equal, not equal, greater, greater or equal, less, less or equal
     *
     * Math:
     * min, max, abs, sum, avg, stddev, stdvar, count, quantile, top/bottom (n largest/smallest values)
     * ceil, floor, exp, log, count_unique_values, count_value_changes
     *
     * Sliding window:
     * simple moving average
     *
     * Calc:
     * derivative
     *
     */

    // Arithmetics

    /** Sum all arguments: [+ 1 2 3 4] -> (1 + 2 + 3 + 4) -> 10
      */
    struct Sum {
        double unit_;

        Sum() : unit_(0) {}

        template<class It>
        double call(aku_ParamId, aku_Timestamp, It begin, It end) {
            auto res = std::accumulate(begin, end, unit_, [](double a, double b) {
                return a + b;
            });
            return res;
        }
        bool check_arity(size_t n, std::string* error) const {
            if (n == 0) {
                *error = "operator + require at least one parameter";
                return false;
            }
            return true;
        }
        bool apply(std::vector<std::unique_ptr<ExpressionNode>>& args, std::string* err) {
            if (!check_arity(args.size(), err)) {
                return false;
            }
            double sum = 0.0;
            auto it = std::remove_if(args.begin(), args.end(), [&sum](std::unique_ptr<ExpressionNode>& n) {
                bool folded;
                double value;
                std::tie(value, folded) = n->fold();
                if (folded) {
                    sum += value;
                    return true;
                }
                return false;
            });
            unit_ = sum;
            args.erase(it, args.end());
            return true;
        }
        constexpr static const char* func_name = "+";
    };

    /** Substitue elements from the first one: [- 10 1 2 3] -> (10 - (1 + 2 + 3)) = 4
      * Negate element if only single argument provided: [- 10] -> -10
      */
    struct Sub {
        double unit_;
        bool   negate_;

        Sub() : unit_(0) {}

        template<class It>
        double call(aku_ParamId, aku_Timestamp, It begin, It end) {
            auto it = begin;
            double res = unit_;
            if (it != end) {
                double first = *it;
                res += (negate_ ? -1 : 1) * first;
                it++;
            }
            res -= std::accumulate(it, end, 0.0, [](double a, double b) {
                return a + b;
            });
            return res;
        }
        bool check_arity(size_t n, std::string* error) const {
            if (n == 0) {
                *error = "operator - require at least one parameter";
                return false;
            }
            return true;
        }
        bool apply(std::vector<std::unique_ptr<ExpressionNode>>& args, std::string* err) {
            if (!check_arity(args.size(), err)) {
                return false;
            }
            negate_ = args.size() == 1;
            double sum = 0.0;
            bool tail = false;
            auto it = std::remove_if(args.begin(), args.end(), [this, &sum, &tail](std::unique_ptr<ExpressionNode>& n) {
                bool folded;
                double value;
                std::tie(value, folded) = n->fold();
                if (folded) {
                    sum += (tail ? -1 : 1) * value;
                    if (!tail) {
                        // First element was copied to the unit_. We need to start
                        // multiplying by -1 starting from the first 'call' argument.
                        negate_ = true;
                    }
                }
                tail = true;
                return folded;
            });
            unit_ = sum;
            if (args.size() == 1) {
                unit_ *= -1;
            }
            args.erase(it, args.end());
            return true;
        }
        constexpr static const char* func_name = "-";
    };

    /** Multiply all elements [* 1 2 3] -> (1 * 2 * 3) -> 6
      */
    struct Mul {
        double unit_;

        Mul() : unit_(1.0) {}

        template<class It>
        double call(aku_ParamId, aku_Timestamp, It begin, It end) {
            auto res = std::accumulate(begin, end, unit_, [](double a, double b) {
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
        bool apply(std::vector<std::unique_ptr<ExpressionNode>>& args, std::string* err) {
            if (!check_arity(args.size(), err)) {
                return false;
            }
            double unit = 1.0;
            auto it = std::remove_if(args.begin(), args.end(), [&unit](std::unique_ptr<ExpressionNode>& n) {
                bool folded;
                double value;
                std::tie(value, folded) = n->fold();
                if (folded) {
                    unit *= value;
                    return true;
                }
                return false;
            });
            unit_ = unit;
            args.erase(it, args.end());
            return true;
        }
        constexpr static const char* func_name = "*";
    };

    /** Divide all elements [/ 9 3 2] -> (9 / 3 / 2) -> 1.5
      * Invert single element [/ 2] -> 0.5
      */
    struct Div {
        double unit_;
        bool invert_;

        Div() : unit_(1.0) {}

        template<class It>
        double call(aku_ParamId, aku_Timestamp, It begin, It end) {
            auto it = begin;
            double res = unit_;
            if (it != end) {
                double first = *it;
                if (invert_) {
                    if (first != 0) {
                        res /= first;
                    }
                    else {
                        return NAN;
                    }
                }
                else {
                    res *= first;
                }
                it++;
            }
            auto mul = std::accumulate(it, end, 1.0, [](double a, double b) {
                return a * b;
            });
            if (mul == 0) {
                return NAN;
            }
            res /= mul;
            return res;
        }
        bool check_arity(size_t n, std::string* error) const {
            if (n == 0) {
                *error = "/ operator require at least one parameter";
                return false;
            }
            return true;
        }
        bool apply(std::vector<std::unique_ptr<ExpressionNode>>& args, std::string* err) {
            if (!check_arity(args.size(), err)) {
                return false;
            }
            invert_ = args.size() == 1;
            double mul = 1.0;
            bool tail = false;
            auto it = std::remove_if(args.begin(), args.end(), [this, &mul, &tail, &args](std::unique_ptr<ExpressionNode>& n) {
                bool folded;
                double value;
                std::tie(value, folded) = n->fold();
                if (folded) {
                    if (tail) {
                        if (value != 0) {
                            mul /= value;
                        }
                        else {
                            mul = NAN;
                        }
                    }
                    else {
                        mul *= value;
                    }
                    if (!tail) {
                        // First element was copied to the unit_. We need to start
                        // with the division instead of multiplication.
                        invert_ = true;
                    }
                }
                tail = true;
                return folded;
            });
            unit_ = mul;
            if (args.size() == 1) {
                if (unit_ != 0) {
                    unit_ = 1.0 / unit_;
                }
                else {
                    unit_ = NAN;
                    it = args.begin();
                }
            }
            args.erase(it, args.end());
            return true;
        }
        constexpr static const char* func_name = "/";
    };

    template<class It, class Fn>
    bool check_order(It begin, It end, const Fn& func) {
        auto it = begin;
        auto prev = begin;
        it++;
        while (it < end) {
            if (!func(*prev, *it)) {
                return false;
            }
            it++;
            prev++;
        }
        return true;
    }

    // Comparisons
    template<class OrderingTrait>
    struct IsOrdered {
        enum class CState {
            HAS_RESULT,
            HAS_OPERAND,
            EMPTY,
        };

        OrderingTrait trait_;
        double const_;
        CState has_const_;

        IsOrdered() : const_(0), has_const_(CState::EMPTY) {}

        template<class It>
        double call(aku_ParamId, aku_Timestamp, It begin, It end) {
            switch(has_const_) {
            case CState::HAS_RESULT:
                return const_;
            case CState::EMPTY:
                return 1.0 * check_order(begin, end, trait_);
            case CState::HAS_OPERAND:
                return 1.0 * (check_order(begin, end, trait_) && trait_(const_, *begin));
            }
            return 0;
        }
        bool check_arity(size_t n, std::string* error) const {
            if (n < 2) {
                *error = OrderingTrait::error_msg;
                return false;
            }
            return true;
        }
        bool apply(std::vector<std::unique_ptr<ExpressionNode>>& args, std::string* err) {
            if (!check_arity(args.size(), err)) {
                return false;
            }
            if (OrderingTrait::disable_folding) {
                return true;
            }
            std::vector<double> constpart;
            auto it = std::remove_if(args.begin(), args.end(), [&constpart](std::unique_ptr<ExpressionNode>& n) {
                bool folded;
                double value;
                std::tie(value, folded) = n->fold();
                if (folded) {
                    constpart.push_back(value);
                    return true;
                }
                return false;
            });
            bool eq = check_order(constpart.begin(), constpart.end(), trait_);
            if (!eq) {
                // doesn't matter if args are consumed or not, some arguments are not ordered
                // and the whole expression will be always evaluated to 0
                has_const_ = CState::HAS_RESULT;
                const_ = 0.0;
                args.clear();
            }
            else if (!constpart.empty()) {
                // there're some folded args, depending of the number of folded args we can
                // have expression evaluated on this stage (n == 0) or only partially evaluated
                int n = static_cast<int>(args.size() - constpart.size());
                if (n == 0) {  // args list fully consumed
                    const_ = 1.0;
                    has_const_ = CState::HAS_RESULT;
                }
                else {
                    const_ = constpart.front();
                    has_const_ = CState::HAS_OPERAND;
                }
                args.erase(it, args.end());
            }
            else {
                // no args was folded
                has_const_ = CState::EMPTY;
            }
            return true;
        }
        constexpr static const char* func_name = OrderingTrait::function_name;
    };

    struct EqualsTrait {
        bool operator() (double a, double b) const { return a == b; }
        constexpr static const char* function_name = "==";
        constexpr static const char* error_msg = "operator == require at least two parameters";
        constexpr static bool disable_folding = false;
    };

    struct NotEqualsTrait {
        bool operator() (double a, double b) const { return a != b; }
        constexpr static const char* function_name = "!=";
        constexpr static const char* error_msg = "operator != require at least two parameters";
        constexpr static bool disable_folding = false;
    };

    struct LessThanTrait {
        bool operator() (double a, double b) const { return a < b; }
        constexpr static const char* function_name = "<";
        constexpr static const char* error_msg = "operator < require at least two parameters";
        constexpr static bool disable_folding = true;
    };

    struct LessOrEqualTrait {
        bool operator() (double a, double b) const { return a <= b; }
        constexpr static const char* function_name = "<=";
        constexpr static const char* error_msg = "operator <= require at least two parameters";
        constexpr static bool disable_folding = true;
    };

    struct GreaterThanTrait {
        bool operator() (double a, double b) const { return a > b; }
        constexpr static const char* function_name = ">";
        constexpr static const char* error_msg = "operator > require at least two parameters";
        constexpr static bool disable_folding = true;
    };

    struct GreaterOrEqualTrait {
        bool operator() (double a, double b) const { return a >= b; }
        constexpr static const char* function_name = ">=";
        constexpr static const char* error_msg = "operator >= require at least two parameters";
        constexpr static bool disable_folding = true;
    };

    // General
    struct Min {
        double baseline_;

        Min() : baseline_(std::numeric_limits<double>::max()) {}

        template<class It>
        double call(aku_ParamId, aku_Timestamp, It begin, It end) {
            auto it = std::min_element(begin, end);
            if (it != end) {
                return std::min(*it, baseline_);
            }
            return baseline_;
        }
        bool check_arity(size_t n, std::string* error) const {
            if (n == 0) {
                *error = "function require at least one parameter";
                return false;
            }
            return true;
        }
        bool apply(std::vector<std::unique_ptr<ExpressionNode>>& args, std::string* err) {
            if (!check_arity(args.size(), err)) {
                return false;
            }
            double minval = baseline_;
            auto it = std::remove_if(args.begin(), args.end(), [&minval](std::unique_ptr<ExpressionNode>& n) {
                bool folded;
                double value;
                std::tie(value, folded) = n->fold();
                if (folded) {
                    minval = std::min(minval, value);
                    return true;
                }
                return false;
            });
            baseline_ = minval;
            args.erase(it, args.end());
            return true;
        }
        constexpr static const char* func_name = "min";
    };

    struct Max {
        double baseline_;

        Max() : baseline_(std::numeric_limits<double>::lowest()) {}

        template<class It>
        double call(aku_ParamId, aku_Timestamp, It begin, It end) {
            auto it = std::max_element(begin, end);
            if (it != end) {
                return std::max(*it, baseline_);
            }
            return baseline_;
        }
        bool check_arity(size_t n, std::string* error) const {
            if (n == 0) {
                *error = "function require at least one parameter";
                return false;
            }
            return true;
        }
        bool apply(std::vector<std::unique_ptr<ExpressionNode>>& args, std::string* err) {
            if (!check_arity(args.size(), err)) {
                return false;
            }
            double maxval = baseline_;
            auto it = std::remove_if(args.begin(), args.end(), [&maxval](std::unique_ptr<ExpressionNode>& n) {
                bool folded;
                double value;
                std::tie(value, folded) = n->fold();
                if (folded) {
                    maxval = std::max(maxval, value);
                    return true;
                }
                return false;
            });
            baseline_ = maxval;
            args.erase(it, args.end());
            return true;
        }
        constexpr static const char* func_name = "max";
    };

    struct Abs {
        bool folded_;
        double abs_;

        Abs() : folded_(false), abs_(0) {}

        template<class It>
        double call(aku_ParamId, aku_Timestamp, It begin, It end) {
            if (folded_) {
                return abs_;
            }
            return std::abs(*begin);
        }
        bool check_arity(size_t n, std::string* error) const {
            if (n == 1) {
                return true;
            }
            *error = "single argument expected";
            return false;
        }
        bool apply(std::vector<std::unique_ptr<ExpressionNode>>& children, std::string* err) {
            if (!check_arity(children.size(), err)) {
                return false;
            }
            double value;
            std::tie(value, folded_) = children.front()->fold();
            if (folded_) {
                abs_ = std::abs(value);
                children.clear();
            }
            return true;
        }
        constexpr static const char* func_name = "abs";
    };

    // Windowed functions
    struct SMA {
        int N;

        struct State {
            int pos;
            double sum;
            std::vector<double> queue;
        };

        std::unordered_map<aku_ParamId, State> table_;

        template<class It>
        double call(aku_ParamId id, aku_Timestamp, It begin, It end) {
            assert(begin != end);
            auto& state = table_[id];
            if (state.queue.size() != static_cast<size_t>(N)) {
                state.queue.resize(N);
            }
            state.sum -= state.queue.at(state.pos % N);
            state.queue.at(state.pos % N) = *begin;
            state.sum += *begin;
            state.pos++;
            return state.sum / std::min(state.pos, N);
        }

        bool apply(std::vector<std::unique_ptr<ExpressionNode>>& children, std::string* err) {
            if (!check_arity(children.size(), err)) {
                return false;
            }
            // First parameter is supposed to be constant
            auto& c = children.front();
            bool success;
            double val;
            std::tie(val, success) = c->fold();
            if (!success) {
                *err = "first 'sma' parameter should be constant";
                return false;
            }
            N = static_cast<int>(val);
            children.erase(children.begin());
            return true;
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

    // Calc

    struct Derivative {
        // Prev
        struct State {
            double xs_;
            aku_Timestamp ts_;
        };
        std::unordered_map<aku_ParamId, State> table_;

        template<class It>
        double call(aku_ParamId id, aku_Timestamp ts, It begin, It end) {
            auto& state = table_[id];
            const double nsec = 1000000000;
            const double next = *begin;
            if (ts == state.ts_) {
                state.xs_ = *begin;
                return NAN;
            }
            // Formula: rate = Δx/Δt
            double dX = (next - state.xs_) / (ts - state.ts_) * nsec;
            state.ts_ = ts;
            state.xs_ = next;
            return dX;
        }
        bool apply(std::vector<std::unique_ptr<ExpressionNode>>& children, std::string* err) {
            return check_arity(children.size(), err);
        }
        bool check_arity(size_t n, std::string* error) const {
            if (n == 1) {
                return true;
            }
            *error = "one argument expected";
            return false;
        }
        constexpr static const char* func_name = "deriv1";
    };
}

// Arithmetic
template struct FunctionCallNode<Builtins::Sum>;
template struct FunctionCallNode<Builtins::Sub>;
template struct FunctionCallNode<Builtins::Mul>;
template struct FunctionCallNode<Builtins::Div>;
// Comparisons
template struct FunctionCallNode<Builtins::IsOrdered<Builtins::EqualsTrait>>;
template struct FunctionCallNode<Builtins::IsOrdered<Builtins::NotEqualsTrait>>;
template struct FunctionCallNode<Builtins::IsOrdered<Builtins::LessThanTrait>>;
template struct FunctionCallNode<Builtins::IsOrdered<Builtins::LessOrEqualTrait>>;
template struct FunctionCallNode<Builtins::IsOrdered<Builtins::GreaterThanTrait>>;
template struct FunctionCallNode<Builtins::IsOrdered<Builtins::GreaterOrEqualTrait>>;
// General
template struct FunctionCallNode<Builtins::Max>;
template struct FunctionCallNode<Builtins::Min>;
template struct FunctionCallNode<Builtins::Abs>;
// Window methods
template struct FunctionCallNode<Builtins::SMA>;
// Calc
template struct FunctionCallNode<Builtins::Derivative>;

typedef boost::property_tree::ptree PTree;
static const int DEPTH_LIMIT = 20;

template<class LookupFn>
std::unique_ptr<ExpressionNode> buildNode(int depth, const PTree& node, const LookupFn& lookup) {
    if (depth == DEPTH_LIMIT) {
        QueryParserError err("expression depth limit exceded");
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
                QueryParserError err("operator or function expected");
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
                        QueryParserError err("unknown field '" + value + "'");
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
        QueryParserError err("unknown operation '" + op + "'");
        BOOST_THROW_EXCEPTION(err);
    }
    return res;
}


// ----
// Eval
// ----

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
    if (!std::isnan(val)) {
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

static QueryParserToken<Eval> eval_token("eval");
static QueryParserToken<ExprEval> eval2_token("eval2");

}}  // namespace
