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
            // extract series name
            std::string metric(st.first, st.first + st.second);
            auto pos = metric.find(' ');
            if (pos != std::string::npos) {
                metric.resize(pos);
            }
            vars.push_back(metric);
        }
        for (u32 i = 0; i < vars.size(); i++) {
            std::string varname = "_var_" + std::to_string(i);
            varmap->insert(std::make_pair(vars[i], varname));
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
        std::map<std::string, double*> used;
        if (expr) {
            try {
                parser_.EnableOptimizer(true);
                auto str = expr->get_value<std::string>("");
                auto pstr = preProcessExpression(str, req, &varmap);
                parser_.SetExpr(pstr);
                used = parser_.GetUsedVar();
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

static QueryParserToken<ExprEval> eval_token("eval");

}}  // namespace
