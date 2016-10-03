#include "queryparser.h"
#include "log_iface.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "datetime.h"


namespace Akumuli {
namespace QP {


static std::tuple<aku_Status, bool, std::string> parse_select_stmt(boost::property_tree::ptree const& ptree) {
    auto select = ptree.get_child_optional("select");
    if (select && select->empty()) {
        // simple select query
        auto str = select->get_value<std::string>("");
        if (str == "names") {
            // the only supported select query for now
            return std::make_tuple(AKU_SUCCESS, true, str);
        }
        Logger::msg(AKU_LOG_ERROR, "Invalid `select` query");
        return std::make_tuple(AKU_EQUERY_PARSING_ERROR, false, "");
    }
    return std::make_tuple(AKU_SUCCESS, false, "");
}

static std::tuple<aku_Status, OrderBy> parse_orderby(boost::property_tree::ptree const& ptree) {
    auto orderby = ptree.get_child_optional("order-by");
    if (orderby) {
        auto stringval = orderby->get_value<std::string>();
        if (stringval == "time") {
            return std::make_tuple(AKU_SUCCESS, OrderBy::TIME);
        } else if (stringval == "series") {
            return std::make_tuple(AKU_SUCCESS, OrderBy::SERIES);
        } else {
            Logger::msg(AKU_LOG_ERROR, "Invalid 'order-by' statement");
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR, OrderBy::TIME);
        }
    }
    // Default is order by time
    return std::make_tuple(AKU_SUCCESS, OrderBy::TIME);
}

static std::tuple<GroupByTime, std::vector<std::string>> parse_groupby(boost::property_tree::ptree const& ptree) {
    std::vector<std::string> tags;
    aku_Timestamp duration = 0u;
    auto groupby = ptree.get_child_optional("group-by");
    if (groupby) {
        for(auto child: *groupby) {
            if (child.first == "time") {
                std::string str = child.second.get_value<std::string>();
                duration = DateTimeUtil::parse_duration(str.c_str(), str.size());
            } else if (child.first == "tag") {
                if (!child.second.empty()) {
                    for (auto tag: child.second) {
                        tags.push_back(tag.second.get_value<std::string>());
                    }
                } else {
                    auto tag = child.second.get_value<std::string>();
                    tags.push_back(tag);
                }
            }
        }
    }
    return std::make_tuple(QP::GroupByTime(duration), tags);
}

static std::pair<u64, u64> parse_limit_offset(boost::property_tree::ptree const& ptree) {
    u64 limit = 0ul, offset = 0ul;
    auto optlimit = ptree.get_child_optional("limit");
    if (optlimit) {
        limit = optlimit->get_value<u64>();
    }
    auto optoffset = ptree.get_child_optional("offset");
    if (optoffset) {
        limit = optoffset->get_value<u64>();
    }
    return std::make_pair(limit, offset);
}

static std::tuple<bool, std::string> parse_metric(boost::property_tree::ptree const& ptree) {
    auto opt = ptree.get_child_optional("metric");
    if (opt) {
        auto single = opt->get_value<std::string>();
        return std::make_pair(true, single);
    }
    return std::make_pair(false, "");
}

static std::tuple<aku_Status, aku_Timestamp> parse_range_timestamp(boost::property_tree::ptree const& ptree, std::string const& name)
{
    auto range = ptree.get_child("range");
    for(auto child: range) {
        if (child.first == name) {
            auto iso_string = child.second.get_value<std::string>();
            auto ts = DateTimeUtil::from_iso_string(iso_string.c_str());
            return std::make_tuple(AKU_SUCCESS, ts);
        }
    }
    return std::make_tuple(AKU_EQUERY_PARSING_ERROR, 0);
}

static std::tuple<aku_Status, std::vector<aku_ParamId>> parse_where_clause(boost::property_tree::ptree const& ptree,
                                                                           bool metric_is_set,
                                                                           std::string metric,
                                                                           SeriesMatcher const& matcher)
{
    std::vector<aku_ParamId> output;
    std::shared_ptr<RegexFilter> result;
    auto where = ptree.get_child_optional("where");
    if (where) {
        if (!metric_is_set) {
            Logger::msg(AKU_LOG_ERROR, "Metric is not set");
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR, output);
        }
        for (auto item: *where) {
            bool firstitem = true;
            std::stringstream series_regexp;
            std::string tag = item.first;
            auto idslist = item.second;
            // Read idlist
            for (auto idnode: idslist) {
                std::string value = idnode.second.get_value<std::string>();
                if (firstitem) {
                    firstitem = false;
                    series_regexp << "(?:";
                } else {
                    series_regexp << "|";
                }
                series_regexp << "(" << metric << R"((?:\s\w+=\w+)*\s)"
                              << tag << "=" << value << R"((?:\s\w+=\w+)*))";
            }
            series_regexp << ")";
            std::string regex = series_regexp.str();
            RegexFilter filter(regex, matcher);
            output = filter.get_ids();
        }
    } else if (metric_is_set) {
        // only metric is specified
        std::stringstream series_regex;
        series_regex << metric << "(?:\\s\\w+=\\w+)*";
        std::string regex = series_regex.str();
        RegexFilter filter(regex, matcher);
        output = filter.get_ids();
    } else {
        // we need to include all series
        // were stmt is not used
        output = matcher.get_all_ids();
    }
    return std::make_tuple(AKU_SUCCESS, output);
}

static std::string to_json(boost::property_tree::ptree const& ptree, bool pretty_print = true) {
    std::stringstream ss;
    boost::property_tree::write_json(ss, ptree, pretty_print);
    return ss.str();
}


// ///////////////// //
// QueryParser class //
// ///////////////// //

std::tuple<aku_Status, boost::property_tree::ptree> QueryParser::parse_json(const char* query) {
    //! C-string to streambuf adapter
    struct MemStreambuf : std::streambuf {
        MemStreambuf(const char* buf) {
            char* p = const_cast<char*>(buf);
            setg(p, p, p+strlen(p));
        }
    };

    boost::property_tree::ptree ptree;
    MemStreambuf strbuf(query);
    std::istream stream(&strbuf);
    try {
        boost::property_tree::json_parser::read_json(stream, ptree);
    } catch (boost::property_tree::json_parser_error& e) {
        // Error, bad query
        Logger::msg(AKU_LOG_ERROR, e.what());
        return std::make_tuple(AKU_EQUERY_PARSING_ERROR, ptree);
    }
    return std::make_tuple(AKU_SUCCESS, ptree);
}

std::tuple<aku_Status, QueryKind> QueryParser::get_query_kind(boost::property_tree::ptree const& ptree) {
    aku_Status status;
    bool sel;
    std::string dummy;
    std::tie(status, sel, dummy) = parse_select_stmt(ptree);
    if (status != AKU_SUCCESS) {
        QueryKind empty;
        return std::make_tuple(status, empty);
    }
    if (sel) {
        return std::make_tuple(status, QueryKind::SCAN);
    }
    return std::make_tuple(status, QueryKind::SELECT);
}

std::tuple<aku_Status, ReshapeRequest> QueryParser::parse_scan_query(
        boost::property_tree::ptree const& ptree,
        const SeriesMatcher &matcher)
{
    aku_Status status;
    ReshapeRequest result = {};

    Logger::msg(AKU_LOG_INFO, "Parsing query:");
    Logger::msg(AKU_LOG_INFO, to_json(ptree, true).c_str());

    // Metric name
    std::string metric;
    bool metric_set = false;
    std::tie(metric_set, metric) = parse_metric(ptree);

    // Group-by statement
    std::vector<std::string> tags;
    GroupByTime groupbytime;
    std::tie(groupbytime, tags) = parse_groupby(ptree);
    auto groupbytag = std::shared_ptr<GroupByTag>();
    if (!tags.empty()) {
        groupbytag.reset(new GroupByTag(matcher, metric, tags));
    }

    // Order-by statment
    OrderBy order;
    std::tie(status, order) = parse_orderby(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result);
    }

    // Where statement
    std::vector<aku_ParamId> ids;
    std::tie(status, ids) = parse_where_clause(ptree, metric_set, metric, matcher);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result);
    }

    // Read timestamps
    aku_Timestamp ts_begin, ts_end;
    std::tie(status, ts_begin) = parse_range_timestamp(ptree, "from");
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result);
    }
    std::tie(status, ts_end)   = parse_range_timestamp(ptree, "to");
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result);
    }

    // Initialize request

    result.select.begin = ts_begin;
    result.select.end = ts_end;
    result.select.ids = ids;

    result.order_by = order;

    result.group_by.enabled = static_cast<bool>(groupbytag);
    if (groupbytag) {
        result.group_by.transient_map = groupbytag->get_mapping();
        result.group_by.matcher = std::shared_ptr<SeriesMatcher>(groupbytag, &groupbytag->local_matcher_);
    }

    return std::make_tuple(AKU_SUCCESS, result);
}

struct TerminalNode : QP::Node {

    Caller &caller;
    InternalCursor* cursor;

    TerminalNode(Caller& ca, InternalCursor* cur)
        : caller(ca)
        , cursor(cur)
    {
    }

    // Node interface

    void complete() {
        cursor->complete(caller);
    }

    bool put(const aku_Sample& sample) {
        if (sample.payload.type != aku_PData::MARGIN) {
            return cursor->put(caller, sample);
        }
        return true;
    }

    void set_error(aku_Status status) {
        cursor->set_error(caller, status);
        throw std::runtime_error("search error detected");
    }

    int get_requirements() const {
        return TERMINAL;
    }
};


std::tuple<aku_Status, std::shared_ptr<Node>>
    make_sampler(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next)
{
    try {
        std::string name;
        name = ptree.get<std::string>("name");
        return std::make_tuple(AKU_SUCCESS, QP::create_node(name, ptree, next));
    } catch (const boost::property_tree::ptree_error& e) {
        Logger::msg(AKU_LOG_ERROR, std::string("Can't parse query: ") + e.what());
        return std::make_tuple(AKU_EQUERY_PARSING_ERROR, nullptr);
    }
}

std::tuple<aku_Status, GroupByTime, std::vector<std::shared_ptr<Node>>> parse_processing_topology(
    boost::property_tree::ptree const& ptree,
    Caller& caller,
    InternalCursor* cursor)
{
    std::vector<std::string> tags;
    GroupByTime groupbytime;
    std::tie(groupbytime, tags) = parse_groupby(ptree);
    AKU_UNUSED(tags);
    // TODO: all processing steps are bypassed now, this should be fixed
    AKU_UNUSED(ptree);
    std::vector<std::shared_ptr<Node>> res = {std::make_shared<TerminalNode>(caller, cursor)};
    return std::make_tuple(AKU_SUCCESS, groupbytime, res);
}

}}  // namespace
