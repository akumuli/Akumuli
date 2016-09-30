#include "queryparser.h"
#include "log_iface.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "datetime.h"


namespace Akumuli {
namespace QP {

struct RegexFilter {
    std::string regex_;
    std::unordered_set<aku_ParamId> ids_;
    SeriesMatcher const& matcher_;
    StringPoolOffset offset_;
    size_t prev_size_;

    RegexFilter(std::string regex, SeriesMatcher const& matcher)
        : regex_(regex)
        , matcher_(matcher)
        , offset_{}
        , prev_size_(0ul)
    {
        refresh();
    }

    void refresh() {
        std::vector<SeriesMatcher::SeriesNameT> results = matcher_.regex_match(regex_.c_str(), &offset_, &prev_size_);
        for (SeriesMatcher::SeriesNameT item: results) {
            ids_.insert(std::get<2>(item));
        }
    }

    std::vector<aku_ParamId> get_ids() {
        std::vector<aku_ParamId> result;
        std::copy(ids_.begin(), ids_.end(), std::back_inserter(result));
        std::sort(result.begin(), result.end());
        return result;
    }
};

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

static std::tuple<aku_Status, bool, OrderBy> parse_orderby(boost::property_tree::ptree const& ptree) {
    auto orderby = ptree.get_child_optional("order-by");
    if (orderby) {
        auto stringval = orderby->get_value<std::string>();
        if (stringval == "time") {
            return std::make_tuple(AKU_SUCCESS, true, OrderBy::TIME);
        } else if (stringval == "series") {
            return std::make_tuple(AKU_SUCCESS, true, OrderBy::SERIES);
        } else {
            Logger::msg(AKU_LOG_ERROR, "Invalid 'order-by' statement");
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR, false, OrderBy::TIME);
        }
    }
    // Default is order by time
    return std::make_tuple(AKU_SUCCESS, true, OrderBy::TIME);
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
                                                                           std::string metric,
                                                                           std::string pred,
                                                                           SeriesMatcher const& matcher)
{
    std::vector<aku_ParamId> output;
    std::shared_ptr<RegexFilter> result;
    auto where = ptree.get_child_optional("where");
    if (where) {
        if (metric.empty()) {
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
    } else if (!metric.empty()) {
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

}}  // namespace
