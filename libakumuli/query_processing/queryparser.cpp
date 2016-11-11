#include "queryparser.h"
#include "log_iface.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "datetime.h"
#include "query_processing/limiter.h"

namespace Akumuli {
namespace QP {

SeriesRetreiver::SeriesRetreiver()
{
}

//! Matches all series from one metric
SeriesRetreiver::SeriesRetreiver(std::string metric)
    : metric_(metric)
{
}

//! Add tag-name and tag-value pair
aku_Status SeriesRetreiver::add_tag(std::string name, std::string value) {
    if (!metric_) {
        Logger::msg(AKU_LOG_ERROR, "Metric not set");
        return AKU_EBAD_ARG;
    }
    if (tags_.count(name)) {
        // Duplicates not allowed
        Logger::msg(AKU_LOG_ERROR, "Duplicate tag '" + name + "' found");
        return AKU_EBAD_ARG;
    }
    tags_[name] = { value };
    return AKU_SUCCESS;
}

//! Add tag name and set of possible values
aku_Status SeriesRetreiver::add_tags(std::string name, std::vector<std::string> values) {
    if (!metric_) {
        Logger::msg(AKU_LOG_ERROR, "Metric not set");
        return AKU_EBAD_ARG;
    }
    if (tags_.count(name)) {
        // Duplicates not allowed
        Logger::msg(AKU_LOG_ERROR, "Duplicate tag '" + name + "' found");
        return AKU_EBAD_ARG;
    }
    tags_[name] = std::move(values);
    return AKU_SUCCESS;
}

std::tuple<aku_Status, std::vector<aku_ParamId>> SeriesRetreiver::extract_ids(SeriesMatcher const& matcher) const {
    std::vector<aku_ParamId> ids;
    // Three cases, no metric (get all ids), only metric is set and both metric and tags are set.
    if (!metric_) {
        // Case 1, metric not set.
        ids = matcher.get_all_ids();
    } else if (tags_.empty()) {
        // Case 2, only metric is set
        std::stringstream regex;
        regex << metric_.get() << "(?:\\s\\w+=\\w+)*";
        std::string expression = regex.str();
        auto results = matcher.regex_match(expression.c_str());
        for (auto res: results) {
            ids.push_back(std::get<2>(res));
        }
    } else {
        // Case 3, both metric and tags are set
        std::stringstream regexp;
        regexp << metric_.get();
        for (auto kv: tags_) {
            auto const& key = kv.first;
            bool first = true;
            regexp << "(?:";
            for (auto const& val: kv.second) {
                if (first) {
                    first = false;
                } else {
                    regexp << "|";
                }
                regexp << "(?:\\s\\w+=\\w+)*\\s" << key << "=" << val << "(?:\\s\\w+=\\w+)*";
            }
            regexp << ")";
        }
        std::string expression = regexp.str();
        auto results = matcher.regex_match(expression.c_str());
        for (auto res: results) {
            ids.push_back(std::get<2>(res));
        }
    }
    return std::make_tuple(AKU_SUCCESS, ids);
}


static std::tuple<aku_Status, bool, std::string> parse_select_stmt(boost::property_tree::ptree const& ptree) {
    auto select = ptree.get_child_optional("select");
    if (select && select->empty()) {
        // select query
        auto str = select->get_value<std::string>("");
        return std::make_tuple(AKU_SUCCESS, true, str);
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
    aku_Status status = AKU_SUCCESS;
    std::vector<aku_ParamId> output;
    auto where = ptree.get_child_optional("where");
    if (where) {
        if (!metric_is_set) {
            Logger::msg(AKU_LOG_ERROR, "Metric is not set");
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR, output);
        }
        typedef std::pair<std::string, boost::property_tree::ptree> PTreeItem;
        std::vector<PTreeItem> taglist;
        for (auto item: *where) {
            taglist.push_back(item);
        }
        SeriesRetreiver retreiver(metric.c_str());
        for (auto item: taglist) {
            std::string tag = item.first;
            auto idslist = item.second;
            // Read idlist
            if (!idslist.empty()) {
                std::vector<std::string> tag_values;
                for (auto idnode: idslist) {
                    tag_values.push_back(idnode.second.get_value<std::string>());
                }
                retreiver.add_tags(tag, tag_values);
            } else {
                retreiver.add_tag(tag, idslist.get_value<std::string>());
            }
        }
        std::tie(status, output) = retreiver.extract_ids(matcher);
    } else if (metric_is_set) {
        // only metric is specified
        SeriesRetreiver retreiver(metric);
        std::tie(status, output) = retreiver.extract_ids(matcher);
    } else {
        // we need to include all series
        // were stmt is not used
        SeriesRetreiver retreiver;
        std::tie(status, output) = retreiver.extract_ids(matcher);
    }
    return std::make_tuple(status, output);
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
    std::string series;
    std::tie(status, sel, series) = parse_select_stmt(ptree);
    if (status != AKU_SUCCESS) {
        QueryKind empty;
        return std::make_tuple(status, empty);
    }
    if (!sel) {
        // Join or Aggregate
        QueryKind empty;
        return std::make_tuple(AKU_ENOT_IMPLEMENTED, empty);
    }
    if (series == "meta:names") {
        return std::make_tuple(AKU_SUCCESS, QueryKind::SELECT_META);
    }
    return std::make_tuple(AKU_SUCCESS, QueryKind::SELECT);
}

/** Select statement should look like this:
 * { "select": "meta:*", ...}
 */
std::tuple<aku_Status, std::vector<aku_ParamId> > QueryParser::parse_select_meta_query(
        boost::property_tree::ptree const& ptree,
        SeriesMatcher const& matcher)
{
    // FIXME:
    // TODO: filter `select meta:names` not only by tags but by metric and tags (different
    //       syntax required).
    aku_Status status;
    bool sel;
    std::string name;
    std::tie(status, sel, name) = parse_select_stmt(ptree);
    std::vector<aku_ParamId> ids;
    if (status != AKU_SUCCESS || name != "meta:name") {
        return std::make_tuple(status, ids);
    }
    if (sel) {
        aku_Status status;
        std::tie(status, ids) = parse_where_clause(ptree, false, "", matcher);
        if (status == AKU_SUCCESS) {
            return std::make_tuple(AKU_SUCCESS, ids);
        }
    }
    return std::make_tuple(AKU_EQUERY_PARSING_ERROR, ids);
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

std::tuple<aku_Status, GroupByTime, std::vector<std::shared_ptr<Node>>> QueryParser::parse_processing_topology(
    boost::property_tree::ptree const& ptree,
    Caller& caller,
    InternalCursor* cursor)
{
    std::vector<std::string> tags;
    GroupByTime groupbytime;
    std::tie(groupbytime, tags) = parse_groupby(ptree);
    AKU_UNUSED(tags);

    // TODO: all processing steps are bypassed now, this should be fixed
    auto terminal = std::make_shared<TerminalNode>(caller, cursor);
    std::vector<std::shared_ptr<Node>> result;

    auto limoff = parse_limit_offset(ptree);
    if (limoff.first != 0 || limoff.second != 0) {
        auto node = std::make_shared<QP::Limiter>(limoff.first, limoff.second, terminal);
        result.push_back(node);
    }

    result.push_back(terminal);
    return std::make_tuple(AKU_SUCCESS, groupbytime, result);
}

}}  // namespace
