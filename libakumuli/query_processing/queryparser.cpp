#include "queryparser.h"
#include "log_iface.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/exception/diagnostic_information.hpp>

#include <set>
#include <regex>
#include <array>

#include "datetime.h"
#include "query_processing/limiter.h"

namespace Akumuli {
namespace QP {

SeriesRetreiver::SeriesRetreiver()
{
}

//! Matches all series from one metric
SeriesRetreiver::SeriesRetreiver(std::vector<std::string> const& metric)
    : metric_(metric)
{
}

//! Add tag-name and tag-value pair
aku_Status SeriesRetreiver::add_tag(std::string name, std::string value) {
    if (metric_.empty()) {
        Logger::msg(AKU_LOG_ERROR, "Metric not set");
        return AKU_EBAD_ARG;
    }
    if (!series_.empty()) {
        Logger::msg(AKU_LOG_ERROR, "Series already set");
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
    if (metric_.empty()) {
        Logger::msg(AKU_LOG_ERROR, "Metric not set");
        return AKU_EBAD_ARG;
    }
    if (!series_.empty()) {
        Logger::msg(AKU_LOG_ERROR, "Series already set");
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

aku_Status SeriesRetreiver::add_series_name(std::string name) {
    if (!tags_.empty()) {
        Logger::msg(AKU_LOG_ERROR, "Tags already set");
        return AKU_EBAD_ARG;
    }
    size_t size = name.size();
    std::string canonical;
    canonical.resize(size);
    const char* keystr_begin, *keystr_end;
    auto status = SeriesParser::to_canonical_form(name.data(), name.data() + size,
                                                  &canonical[0], &canonical[0] + size,
                                                  &keystr_begin, &keystr_end);
    if (status != AKU_SUCCESS) {
        return status;
    }
    series_.push_back(std::string(canonical.data(), keystr_end));
    return AKU_SUCCESS;
}

std::tuple<aku_Status, std::vector<aku_ParamId>> SeriesRetreiver::extract_ids(SeriesMatcher const& matcher) const {
    std::vector<aku_ParamId> ids;
    // Three cases, no metric (get all ids), only metric is set and both metric and tags are set.
    if (!series_.empty()) {
        // Case 1, specific series names are set.
        for(const auto& name: series_) {
            auto id = matcher.match(name.data(), name.data() + name.size());
            if (id) {
                ids.push_back(id);
            }
        }
        if (ids.empty()) {
            return std::make_tuple(AKU_ENOT_FOUND, ids);
        }
    } else if (metric_.empty()) {
        // Case 2, metric not set.
        // get all ids
        auto sids = matcher.get_all_ids();
        for (i64 id: sids) {
            ids.push_back(static_cast<aku_ParamId>(id));
        }
    } else {
        // Case 3, metric is set
        auto first_metric = metric_.front();
        IncludeMany2Many query(first_metric, tags_);
        auto search_results = matcher.search(query);
        for (auto tup: search_results) {
            ids.push_back(static_cast<aku_ParamId>(std::get<2>(tup)));
        }
        if (metric_.size() > 1) {
            std::vector<std::string> tail(metric_.begin() + 1, metric_.end());
            std::vector<aku_ParamId> full(ids);
            for (auto metric: tail) {
                for (auto id: ids) {
                    StringT name = matcher.id2str(static_cast<i64>(id));
                    if (name.second == 0) {
                        // This shouldn't happen but it can happen after memory corruption or data-race.
                        // Clearly indicates an error.
                        Logger::msg(AKU_LOG_ERROR, "Matcher data is broken, can read series name for " + std::to_string(id));
                        AKU_PANIC("Matcher data is broken");
                    }
                    std::string series_tags(name.first + first_metric.size(), name.first + name.second);
                    std::string alt_name = metric + series_tags;
                    auto sid = matcher.match(alt_name.data(), alt_name.data() + alt_name.size());
                    full.push_back(static_cast<aku_ParamId>(sid));
                                          // NOTE: sid (secondary id) can be = 0. This means that there is no such
                                          // combination of metric and tags. Different strategies can be used to deal with
                                          // such cases. Query can leave this element of the tuple blank or discard it.
                }
            }
            ids.swap(full);
        }
    }
    return std::make_tuple(AKU_SUCCESS, ids);
}

std::tuple<aku_Status, std::vector<aku_ParamId>> SeriesRetreiver::extract_ids(PlainSeriesMatcher const& matcher) const {
    std::vector<aku_ParamId> ids;
    // Three cases, no metric (get all ids), only metric is set and both metric and tags are set.
    if (metric_.empty()) {
        // Case 1, metric not set.
        auto sids = matcher.get_all_ids();
        for (i64 id: sids) {
            ids.push_back(static_cast<aku_ParamId>(id));
        }
    } else {
        auto first_metric = metric_.front();
        if (tags_.empty()) {
            // Case 2, only metric is set
            std::stringstream regex;
            regex << first_metric << "(?:\\s[\\w\\.\\-]+=[\\w\\.\\-]+)*";
            std::string expression = regex.str();
            auto results = matcher.regex_match(expression.c_str());
            for (auto res: results) {
                ids.push_back(static_cast<aku_ParamId>(std::get<2>(res)));
            }
        } else {
            // Case 3, both metric and tags are set
            std::stringstream regexp;
            regexp << first_metric;
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
                    regexp << "(?:\\s[\\w\\.\\-]+=[\\w\\.\\-]+)*\\s" << key << "=" << val << "(?:\\s[\\w\\.\\-]+=[\\w\\.\\-]+)*";
                }
                regexp << ")";
            }
            std::string expression = regexp.str();
            auto results = matcher.regex_match(expression.c_str());
            for (auto res: results) {
                ids.push_back(static_cast<aku_ParamId>(std::get<2>(res)));
            }
        }

        if (metric_.size() > 1) {
            std::vector<std::string> tail(metric_.begin() + 1, metric_.end());
            std::vector<aku_ParamId> full(ids);
            for (auto metric: tail) {
                for (auto id: ids) {
                    StringT name = matcher.id2str(static_cast<i64>(id));
                    if (name.second == 0) {
                        // This shouldn't happen but it can happen after memory corruption or data-race.
                        // Clearly indicates an error.
                        Logger::msg(AKU_LOG_ERROR, "Matcher data is broken, can read series name for " + std::to_string(id));
                        AKU_PANIC("Matcher data is broken");
                    }
                    std::string series_tags(name.first + first_metric.size(), name.first + name.second);
                    std::string alt_name = metric + series_tags;
                    auto sid = matcher.match(alt_name.data(), alt_name.data() + alt_name.size());
                    full.push_back(static_cast<aku_ParamId>(sid));
                                          // NOTE: sid (secondary id) can be = 0. This means that there is no such
                                          // combination of metric and tags. Different strategies can be used to deal with
                                          // such cases. Query can leave this element of the tuple blank or discard it.
                }
            }
            ids.swap(full);
        }
    }
    return std::make_tuple(AKU_SUCCESS, ids);
}

std::tuple<aku_Status, std::vector<aku_ParamId>> SeriesRetreiver::fuzzy_match(PlainSeriesMatcher const& matcher) const {
    std::vector<aku_ParamId> ids;
    // Three cases, no metric (get all ids), only metric is set and both metric and tags are set.
    if (metric_.empty()) {
        // Case 1, metric not set.
        return std::make_tuple(AKU_EBAD_ARG, ids);
    } else {
        auto first_metric = metric_.front();
        if (tags_.empty()) {
            // Case 2, only metric is set
            std::stringstream regex;
            regex << first_metric << "\\S*(?:\\s[\\w\\.\\-]+=[\\w\\.\\-]+)*";
            std::string expression = regex.str();
            auto results = matcher.regex_match(expression.c_str());
            for (auto res: results) {
                ids.push_back(static_cast<aku_ParamId>(std::get<2>(res)));
            }
        } else {
            // Case 3, both metric and tags are set
            std::stringstream regexp;
            regexp << first_metric << "\\S*";
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
                    regexp << "(?:\\s[\\w\\.\\-]+=[\\w\\.\\-]+)*\\s" << key << "=" << val << "(?:\\s[\\w\\.\\-]+=[\\w\\.\\-]+)*";
                }
                regexp << ")";
            }
            std::string expression = regexp.str();
            auto results = matcher.regex_match(expression.c_str());
            for (auto res: results) {
                ids.push_back(static_cast<aku_ParamId>(std::get<2>(res)));
            }
        }

        if (metric_.size() > 1) {
            std::vector<std::string> tail(metric_.begin() + 1, metric_.end());
            std::vector<aku_ParamId> full(ids);
            for (auto metric: tail) {
                for (auto id: ids) {
                    StringT name = matcher.id2str(static_cast<i64>(id));
                    if (name.second == 0) {
                        // This shouldn't happen but it can happen after memory corruption or data-race.
                        // Clearly indicates an error.
                        Logger::msg(AKU_LOG_ERROR, "Matcher data is broken, can read series name for " + std::to_string(id));
                        AKU_PANIC("Matcher data is broken");
                    }
                    std::string series_tags(name.first + first_metric.size(), name.first + name.second);
                    std::string alt_name = metric + series_tags;
                    auto sid = matcher.match(alt_name.data(), alt_name.data() + alt_name.size());
                    full.push_back(sid);  // NOTE: sid (secondary id) can be = 0. This means that there is no such
                                          // combination of metric and tags. Different strategies can be used to deal with
                                          // such cases. Query can leave this element of the tuple blank or discard it.
                }
            }
            ids.swap(full);
        }
    }
    return std::make_tuple(AKU_SUCCESS, ids);
}


static const std::set<std::string> META_QUERIES = {
    "meta:names"
};

// TODO: remove depricated
bool is_meta_query(std::string name) {
    for (auto perf: META_QUERIES) {
        if (boost::starts_with(name, perf)) {
            return true;
        }
    }
    return false;
}

static std::tuple<aku_Status, std::string, ErrorMsg> parse_search_select_stmt(boost::property_tree::ptree const& ptree) {
    // This should allow to search both metric events or all (if empty string is passed)
    auto select = ptree.get_child_optional("select");
    if (select && select->empty()) {
        // select query
        auto str = select->get_value<std::string>("");
        return std::make_tuple(AKU_SUCCESS, str, ErrorMsg());
    }
    return std::make_tuple(AKU_EQUERY_PARSING_ERROR, "", "Query object doesn't have a 'select' field");
}

static std::tuple<aku_Status, std::string, ErrorMsg> parse_select_stmt(boost::property_tree::ptree const& ptree) {
    auto select = ptree.get_child_optional("select");
    if (select && select->empty()) {
        // select query
        auto str = select->get_value<std::string>("");
        if (!str.empty() && str.front() != '!') {
            return std::make_tuple(AKU_SUCCESS, str, ErrorMsg());
        }
        else {
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR, "", "Metric name can't be empty or start with '!' symbol");
        }
    }
    return std::make_tuple(AKU_EQUERY_PARSING_ERROR, "", "Query object doesn't have a 'select' field");
}

static std::tuple<aku_Status, std::string, ErrorMsg> parse_select_events_stmt(boost::property_tree::ptree const& ptree) {
    auto select = ptree.get_child_optional("select-events");
    if (select && select->empty()) {
        // select query
        auto str = select->get_value<std::string>("");
        if (!str.empty() && str.front() == '!') {
            return std::make_tuple(AKU_SUCCESS, str, ErrorMsg());
        }
        else {
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR, "", "Event name should start with '!' symbol");
        }
    }
    return std::make_tuple(AKU_EQUERY_PARSING_ERROR, "", "Query object doesn't have a 'select-events' field");
}

static std::tuple<aku_Status, std::string, ErrorMsg> parse_select_events_filter_field(boost::property_tree::ptree const& ptree) {
    auto flt = ptree.get_child_optional("filter");
    if (flt && flt->empty()) {
        // select query
        auto str = flt->get_value<std::string>("");
        if (!str.empty()) {
            try {
                std::regex rexp(str.data(), std::regex_constants::ECMAScript);
            } catch (const std::regex_error& w) {
                return std::make_tuple(AKU_EBAD_ARG, "", w.what());
            }
            return std::make_tuple(AKU_SUCCESS, str, ErrorMsg());
        }
    }
    return std::make_tuple(AKU_SUCCESS, "", "");
}

/** Parse `join` statement, format:
  * { "join": [ "metric1", "metric2", ... ], ... }
  */
static std::tuple<aku_Status, std::vector<std::string>, ErrorMsg>
    parse_join_stmt(boost::property_tree::ptree const& ptree)
{
    auto join = ptree.get_child_optional("join");
    // value is a list of metric names in proper order
    std::vector<std::string> result;
    if (join) {
        for (auto item: *join) {
            auto value = item.second.get_value_optional<std::string>();
            if (value) {
                auto str = *value;
                if (!str.empty() && str.front() != '!') {
                    result.push_back(str);
                } else {
                    return std::make_tuple(AKU_EQUERY_PARSING_ERROR, result, "Metric name can't be empty or start with '!' symbol");
                }
            } else {
                return std::make_tuple(AKU_EQUERY_PARSING_ERROR, result, "Metric name expected");
            }
        }
    }
    if (result.empty()) {
        return std::make_tuple(AKU_EQUERY_PARSING_ERROR, result, "Metric name should be set");
    }
    return std::make_tuple(AKU_SUCCESS, result, ErrorMsg());
}

/** Parse `aggregate` statement, format:
  * { "aggregate": { "metric": "func" }, ... }
  */
static std::tuple<aku_Status, std::vector<std::string>, std::vector<std::string>, ErrorMsg> parse_aggregate_stmt(boost::property_tree::ptree const& ptree) {
    const static std::vector<std::string> EMPTY;
    auto aggregate = ptree.get_child_optional("aggregate");
    if (aggregate) {
        std::vector<std::string> metrics, functions;
        // select query
        for (auto kv: *aggregate) {
            auto metric_name = kv.first;
            auto func = kv.second.get_value<std::string>("count");
            if (!metric_name.empty() && metric_name.front() != '!') {
                metrics.push_back(metric_name);
            } else {
                return std::make_tuple(AKU_EQUERY_PARSING_ERROR, EMPTY, EMPTY, "Metric name can't be empty or start with '!' symbol");
            }
            functions.push_back(func);
        }
        if (metrics.empty()) {
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR, EMPTY, EMPTY, "Query object has empty `aggregate` field");
        }
        return std::make_tuple(AKU_SUCCESS, metrics, functions, ErrorMsg());
    } else {
        return std::make_tuple(AKU_EQUERY_PARSING_ERROR, EMPTY, EMPTY, "Query object doesn't have `aggregate` field");
    }
    return std::make_tuple(AKU_EQUERY_PARSING_ERROR, EMPTY, EMPTY, "Can't parse `aggregate` field");
}

/**
 * Result of the group-aggregate stmt parsing
 */
struct GroupAggregate {
    std::vector<std::string> metric;
    std::vector<AggregationFunction> func;
    aku_Duration step;
};

/** Parse `group-aggregate` statement, format:
  * { "group-aggregate": { "step": "30s", "metric": "name", "func": ["cnt", "avg"] }, ... }
  * { "group-aggregate": { "step": "30s", "metric": ["foo", "bar"], "func": ["cnt", "avg"] }, ... }
  * @return status, metric name, functions array, step (as timestamp)
  */
static std::tuple<aku_Status, GroupAggregate, ErrorMsg> parse_group_aggregate_stmt(boost::property_tree::ptree const& ptree) {
    bool components[] = {
        false, false, false
    };
    GroupAggregate result;
    std::stringstream error_fmt;
    auto aggregate = ptree.get_child_optional("group-aggregate");
    if (aggregate) {
        // select query
        for (auto kv: *aggregate) {
            auto tag_name = kv.first;
            auto value = kv.second.get_value_optional<std::string>();
            if (tag_name == "step") {
                if (components[0]) {
                    // Duplicate "step" tag
                    Logger::msg(AKU_LOG_ERROR, "Duplicate `step` tag in `group-aggregate` statement");
                    error_fmt << "duplicate `step` tag in `group-aggregate` statement";
                    break;
                } else {
                    if (!value) {
                        Logger::msg(AKU_LOG_ERROR, "Tag `step` is not set in `group-aggregate` statement");
                        error_fmt << "tag `step` is not set in `group-aggregate` statement";
                        break;
                    }
                    try {
                        aku_Duration step = DateTimeUtil::parse_duration(value.get().data(), value.get().size());
                        result.step = step;
                        components[0] = true;
                    } catch (const BadDateTimeFormat& e) {
                        Logger::msg(AKU_LOG_ERROR, "Can't parse time-duration: " + *value);
                        Logger::msg(AKU_LOG_ERROR, boost::current_exception_diagnostic_information());
                        error_fmt << "can't parse time-duration: " << *value;
                        break;
                    }
                }
            } else if (tag_name == "metric") {
                if (!value) {
                    Logger::msg(AKU_LOG_ERROR, "Tag `metric` is not set in `group-aggregate` statement");
                    error_fmt << "tag `metric` is not set in `group-aggregate` statement";
                    break;
                }
                if (components[1]) {
                    // Duplicate "metric" tag
                    Logger::msg(AKU_LOG_ERROR, "Duplicate `metric` tag in `group-aggregate` statement");
                    error_fmt << "duplicate `metric` tag in `group-aggregate` statement";
                    break;
                } else {
                    for (auto child: aggregate->get_child("metric")) {
                        auto metric = child.second.get_value_optional<std::string>();
                        if (metric) {
                            if (!metric->empty() && metric->front() != '!') {
                                result.metric.push_back(*metric);
                            } else {
                                Logger::msg(AKU_LOG_ERROR, "Metric name can't be empty or start with '!' symbol");
                                error_fmt << "metric name can't be empty or start with '!' symbol";
                                break;
                            }
                        }
                    }
                    if (result.metric.empty()) {
                        auto mname = value.get();
                        if (!mname.empty() && mname.front() != '!') {
                            result.metric.push_back(mname);
                        } else {
                            Logger::msg(AKU_LOG_ERROR, "Metric name can't be empty or start with '!' symbol");
                            error_fmt << "metric name can't be empty or start with '!' symbol";
                            break;
                        }
                    }
                    components[1] = true;
                }
            } else if (tag_name == "func") {
                if (components[2]) {
                    // Duplicate "func" tag
                    Logger::msg(AKU_LOG_ERROR, "Duplicate `func` tag in `group-aggregate` statement");
                    error_fmt << "duplicate `func` tag in `group-aggregate` statement";
                    break;
                } else {
                    int n = 0;
                    bool error = false;
                    for (auto child: aggregate->get_child("func")) {
                        auto fn = child.second.get_value_optional<std::string>();
                        if (fn) {
                            aku_Status status;
                            AggregationFunction func;
                            std::tie(status, func) = Aggregation::from_string(*fn);
                            if (status == AKU_SUCCESS) {
                                result.func.push_back(func);
                                n++;
                            } else {
                                Logger::msg(AKU_LOG_ERROR, "Invalid aggregation function `" + fn.get() + "`");
                                error_fmt << "invalid aggregation function `" << fn.get() << "`";
                                error = true;
                                break;
                            }
                        }
                    }
                    if (result.func.empty() && !error && value) {
                        aku_Status status;
                        AggregationFunction func;
                        std::tie(status, func) = Aggregation::from_string(value.get());
                        if (status == AKU_SUCCESS) {
                            result.func.push_back(func);
                            n++;
                        } else {
                            Logger::msg(AKU_LOG_ERROR, "Aggregation function can't be found in the query");
                            error_fmt << "aggregation function can't be found in the query";
                            error = true;
                        }
                    }
                    if (error) {
                        break;
                    }
                    components[2] = n;
                }
            }
        }
    }
    bool complete = components[0] && components[1] && components[2];
    std::stringstream fullerr;
    if (complete) {
        return std::make_tuple(AKU_SUCCESS, result, ErrorMsg());
    } else if (components[0] == false) {
        Logger::msg(AKU_LOG_ERROR, "Can't validate `group-aggregate` statement, `step` field required");
        fullerr << "Can't validate `group-aggregate` statement, `step` field required, " << error_fmt.str();
    } else if (components[1] == false) {
        Logger::msg(AKU_LOG_ERROR, "Can't validate `group-aggregate` statement, `metric` field required");
        fullerr << "Can't validate `group-aggregate` statement, `metric` field required, " << error_fmt.str();
    } else if (components[2] == false) {
        Logger::msg(AKU_LOG_ERROR, "Can't validate `group-aggregate` statement, `func` field required");
        fullerr << "Can't validate `group-aggregate` statement, `func` field required, " << error_fmt.str();
    }
    return std::make_tuple(AKU_EQUERY_PARSING_ERROR, result, fullerr.str());
}

/** Parse `oreder-by` statement, format:
  * { "oreder-by": "series", ... }
  */
static std::tuple<aku_Status, OrderBy, ErrorMsg> parse_orderby(boost::property_tree::ptree const& ptree) {
    auto orderby = ptree.get_child_optional("order-by");
    if (orderby) {
        auto stringval = orderby->get_value<std::string>();
        if (stringval == "time") {
            return std::make_tuple(AKU_SUCCESS, OrderBy::TIME, ErrorMsg());
        } else if (stringval == "series") {
            return std::make_tuple(AKU_SUCCESS, OrderBy::SERIES, ErrorMsg());
        } else {
            Logger::msg(AKU_LOG_ERROR, "Invalid 'order-by' statement");
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR,
                                   OrderBy::TIME,
                                   "Unexpected `order-by` field value `" + stringval + "`");
        }
    }
    // Default is order by time
    return std::make_tuple(AKU_SUCCESS, OrderBy::TIME, ErrorMsg());
}

/** Parse `group-by` statement, format:
 *  { ..., "group-by": [ "tag1", "tag2" ] }
 *  or
 *  { ..., "group-by": "tag1" }
 */
static std::tuple<aku_Status, std::vector<std::string>, GroupByOpType, ErrorMsg> parse_groupby(boost::property_tree::ptree const& ptree) {
    std::vector<std::string> tags;
    GroupByOpType op = GroupByOpType::PIVOT;
    auto groupby = ptree.get_child_optional("group-by");
    if (groupby) {
        Logger::msg(AKU_LOG_ERROR, "'group-by' field is depricated, consider using 'group-by-tag' or 'pivot-by-tag'");
    }
    if (!groupby) {
        groupby = ptree.get_child_optional("pivot-by-tag");
    }
    if (!groupby) {
        groupby = ptree.get_child_optional("group-by-tag");
        op = GroupByOpType::GROUP;
    }
    if (groupby) {
        for (auto item: *groupby) {
            auto val = item.second.get_value_optional<std::string>();
            if (val) {
                tags.push_back(*val);
            } else {
                return std::make_tuple(AKU_EQUERY_PARSING_ERROR,
                                       tags,
                                       op,
                                       "Query object has ill-formed `group-by` field");
            }
        }
    }
    return std::make_tuple(AKU_SUCCESS, tags, op, ErrorMsg());
}

/** Parse `limit` and `offset` statements, format:
  * { "limit": 10, "offset": 200, ... }
  */
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

static std::tuple<aku_Status, aku_Timestamp, aku_Timestamp, ErrorMsg> parse_range_timestamp(boost::property_tree::ptree const& ptree,
                                                                                            bool allow_empty=false)
{
    aku_Timestamp begin = 0, end = 0;
    bool begin_set = false, end_set = false;
    auto range = ptree.get_child_optional("range");
    bool error = false;
    std::stringstream fmt;
    if (range) {
        for(auto child: *range) {
            if (child.first == "from") {
                auto iso_string = child.second.get_value<std::string>();
                try {
                    begin = DateTimeUtil::from_iso_string(iso_string.c_str());
                    begin_set = true;
                } catch (std::exception const& e) {
                    Logger::msg(AKU_LOG_ERROR, std::string("Can't parse begin timestmp, ") + e.what());
                    fmt << "can't parse " << iso_string << " as begin timestamp, " << e.what();
                    error = true;
                }
            } else if (child.first == "to") {
                auto iso_string = child.second.get_value<std::string>();
                try {
                    end = DateTimeUtil::from_iso_string(iso_string.c_str());
                    end_set = true;
                } catch (std::exception const& e) {
                    Logger::msg(AKU_LOG_ERROR, std::string("Can't parse end timestmp, ") + e.what());
                    if (!begin_set) {
                        fmt << ", ";
                    }
                    fmt << "can't parse " << iso_string << " as end timestamp, " << e.what();
                    error = true;
                }
            }
        }
    }
    if (allow_empty && !begin_set && !end_set && !error) {
        // Shortcut for the case when empty range statment means the whole interval.
        // The branch is not triggered if it's empty because of parsing error. It's
        // also not triggered if only one field is not set.
        return std::make_tuple(AKU_SUCCESS,
                               std::numeric_limits<aku_Timestamp>::min(),
                               std::numeric_limits<aku_Timestamp>::max(),
                               ErrorMsg());
    }
    if (!begin_set || !end_set) {
        if (error) {
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR, begin, end, "Range field error: " + fmt.str());
        } else {
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR, begin, end, "Range field is not set");
        }
    }
    return std::make_tuple(AKU_SUCCESS, begin, end, ErrorMsg());
}

/** Parse `where` statement, format:
  * "where": { "tag": [ "value1", "value2" ], ... },
  * or
  * "where": [ { "tag1": "value1", "tag2": "value2" },
  *            { "tag1": "value3", "tag2": "value4" } ]
  */
static std::tuple<aku_Status, std::vector<aku_ParamId>, ErrorMsg> parse_where_clause(boost::property_tree::ptree const& ptree,
                                                                                     std::vector<std::string> metrics,
                                                                                     SeriesMatcher const& matcher)
{
    aku_Status status = AKU_SUCCESS;
    std::vector<aku_ParamId> output;
    auto where = ptree.get_child_optional("where");
    if (where) {
        if (metrics.empty()) {
            Logger::msg(AKU_LOG_ERROR, "Metric is not set");
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR, output, "Invalid where clause, metric is not set");
        }
        SeriesRetreiver retreiver(metrics);
        for (auto item: *where) {
            std::string tag = item.first;
            if (tag.empty()) {
                for (const auto& m: metrics) {
                    std::stringstream series;
                    series << m << " ";
                    for (auto tgv: item.second) {
                        auto tagname = tgv.first;
                        auto tagvalue = tgv.second.get_value<std::string>();
                        series << tagname << "=" << tagvalue << " ";
                    }
                    retreiver.add_series_name(series.str());
                }
            } else {
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
        }
        std::tie(status, output) = retreiver.extract_ids(matcher);
    } else if (metrics.size()) {
        // only metric is specified
        SeriesRetreiver retreiver(metrics);
        std::tie(status, output) = retreiver.extract_ids(matcher);
    } else {
        // we need to include all series
        // were stmt is not used
        SeriesRetreiver retreiver;
        std::tie(status, output) = retreiver.extract_ids(matcher);
    }
    return std::make_tuple(status, output, ErrorMsg());
}

static std::string to_json(boost::property_tree::ptree const& ptree, bool pretty_print = true) {
    std::stringstream ss;
    boost::property_tree::write_json(ss, ptree, pretty_print);
    return ss.str();
}


/**
 * @brief Parse filter clause
 *
 * Query form 1:
 * "filter": { "metric_name": { "gt": 100 }}
 * This format allows multiple metric names.
 *
 * Query form 2:
 * "filter": { "gt": 100 }
 * This is a shorthand for queries that return one single metric. Can only be used in this
 * case since there is no ambiguity. The first form can be used with the join query.
 *
 * @param ptree is a query data
 * @param metrics is a list of metrics extracted by the query
 * @return filter value per metric
 */
static std::tuple<aku_Status, std::vector<Filter>, FilterCombinationRule, ErrorMsg>
    parse_filter(boost::property_tree::ptree const& ptree, std::vector<std::string> metrics)
{
    std::stringstream error_fmt;
    aku_Status status = AKU_SUCCESS;
    std::vector<Filter> result;
    FilterCombinationRule rule = FilterCombinationRule::ALL;  // Default value
    for (size_t ix = 0; ix < metrics.size(); ix++) {
        // Initially everything is disabled
        result.push_back({false});
    }
    auto filter = ptree.get_child_optional("filter");
    bool found_at_least_one = false;
    int nfound = 0;
    const char* names[] = { "gt", "lt", "ge", "le" };
    int flags[] = { Filter::GT, Filter::LT, Filter::GE, Filter::LE };
    const size_t nitems = 4;
    if (filter) {
        for (size_t ix = 0; ix < metrics.size(); ix++) {
            // Form 1 query
            auto child = filter->get_child_optional(metrics[ix]);
            if (child) {
                found_at_least_one = true;
                nfound++;
                for (size_t i = 0; i < nitems; i++) {
                    auto item = child->get_child_optional(names[i]);
                    if (item) {
                        result[ix].flags |= flags[i];
                        auto value = item->get_value<std::string>("");
                        try {
                            double* pval = &result[ix].gt;
                            pval[i] = boost::lexical_cast<double>(value);
                        } catch (boost::bad_lexical_cast const&) {
                            Logger::msg(AKU_LOG_ERROR, metrics[ix] + " has bad filter value, can't parse floating point");
                            error_fmt << "Query object filter field: " << metrics[ix]
                                      << " has bad value, can't parse floating point";
                            found_at_least_one = false;
                            nfound = 0;
                            status = AKU_EBAD_ARG;
                            break;
                        }
                        result[ix].enabled = true;
                    }
                }
            }
        }
        if (nfound > 1) {
            // Parse combiner
            auto child = filter->get_child_optional("=");
            if (child) {
                for (auto sub: *child) {
                    if (sub.first == "require") {
                        auto value = sub.second.get_value<std::string>("");
                        if (value == "all") {
                            rule = FilterCombinationRule::ALL;
                        }
                        else if (value == "any") {
                            rule = FilterCombinationRule::ANY;
                        } else {
                            Logger::msg(AKU_LOG_ERROR, std::string("Unknown filter combiner ") + value);
                            error_fmt << "Query object filter field has unknown filter combiner " << value;
                            return std::make_tuple(AKU_EQUERY_PARSING_ERROR, result, rule, error_fmt.str());
                        }
                    } else {
                        Logger::msg(AKU_LOG_ERROR, std::string("Unknown filter meta key ") + sub.first);
                        error_fmt << "Unknown filter meta key" << sub.first;
                        return std::make_tuple(AKU_EQUERY_PARSING_ERROR, result, rule, error_fmt.str());
                    }
                }
            }
        }
        if (!found_at_least_one && metrics.size() == 1) {
            // Form 2 query
            for (size_t i = 0; i < nitems; i++) {
                auto item = filter->get_child_optional(names[i]);
                if (item) {
                    result[0].flags |= flags[i];
                    auto value = item->get_value<std::string>("");
                    try {
                        double* pval = &result[0].gt;
                        pval[i] = boost::lexical_cast<double>(value);
                    } catch (boost::bad_lexical_cast const&) {
                        Logger::msg(AKU_LOG_ERROR, metrics[0] + " has bad filter value, can't parse floating point");
                        error_fmt << "Query object filter field: " << metrics[0]
                                  << " has bad value, can't parse floating point";
                        found_at_least_one = false;
                        status = AKU_EBAD_ARG;
                        break;
                    }
                    result[0].enabled = true;
                }
            }
        }
    }
    return std::make_tuple(status, result, rule, error_fmt.str());
}


// ///////////////// //
// QueryParser class //
// ///////////////// //

std::tuple<aku_Status, boost::property_tree::ptree, ErrorMsg> QueryParser::parse_json(const char* query) {
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
    } catch (boost::property_tree::json_parser_error const& e) {
        // Error, bad query
        std::stringstream error;
        error << "JSON parsing error at line " << std::to_string(e.line()) << ", " << e.message();
        Logger::msg(AKU_LOG_ERROR, error.str());
        return std::make_tuple(AKU_EQUERY_PARSING_ERROR, ptree, error.str());
    }
    return std::make_tuple(AKU_SUCCESS, ptree, ErrorMsg());
}

std::tuple<aku_Status, QueryKind, ErrorMsg> QueryParser::get_query_kind(boost::property_tree::ptree const& ptree) {
    aku_Status status;
    for (const auto& item: ptree) {
        if (item.first == "select") {
            std::string series;
            ErrorMsg error;
            std::tie(status, series, error) = parse_select_stmt(ptree);
            if (status != AKU_SUCCESS) {
                return std::make_tuple(status, QueryKind::SELECT, error);
            } else if (is_meta_query(series)) {
                // TODO: Depricated
                return std::make_tuple(AKU_SUCCESS, QueryKind::SELECT_META, ErrorMsg());
            } else {
                return std::make_tuple(AKU_SUCCESS, QueryKind::SELECT, ErrorMsg());
            }
        } else if (item.first == "aggregate") {
            return std::make_tuple(AKU_SUCCESS, QueryKind::AGGREGATE, ErrorMsg());
        } else if (item.first == "join") {
            return std::make_tuple(AKU_SUCCESS, QueryKind::JOIN, ErrorMsg());
        } else if (item.first == "group-aggregate") {
            return std::make_tuple(AKU_SUCCESS, QueryKind::GROUP_AGGREGATE, ErrorMsg());
        } else if (item.first == "select-events") {
            return std::make_tuple(AKU_SUCCESS, QueryKind::SELECT_EVENTS, ErrorMsg());
        }
    }
    static const char* error_message = "Query object type is undefined. "
                                       "One of the following fields should be added: "
                                       "select, aggregate, join, group-aggregate";
    return std::make_tuple(AKU_EQUERY_PARSING_ERROR, QueryKind::SELECT, error_message);
}

std::tuple<aku_Status, ErrorMsg> validate_query(boost::property_tree::ptree const& ptree) {
    static const std::vector<std::string> UNIQUE_STMTS = {
        "select",
        "aggregate",
        "join",
        "group-aggregate",
        "select-events",
    };
    static const std::set<std::string> ALLOWED_STMTS = {
        "select",
        "aggregate",
        "join",
        "output",
        "order-by",
        "group-by",
        "group-by-tag",
        "pivot-by-tag",
        "limit",
        "offset",
        "range",
        "where",
        "group-aggregate",
        "apply",
        "filter",
        "select-events",
    };
    std::set<std::string> keywords;
    for (const auto& item: ptree) {
        std::string keyword = item.first;
        if (ALLOWED_STMTS.count(keyword) == 0) {
            Logger::msg(AKU_LOG_ERROR, "Unexpected `" + keyword + "` statement");
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR,
                                   "Query object contains unexpected field `" + keyword + "`");
        }
        if (keywords.count(keyword)) {
            Logger::msg(AKU_LOG_ERROR, "Duplicate `" + keyword + "` statement");
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR,
                                   "Query object contains duplicate field `" + keyword + "`");
        }
        for (auto kw: UNIQUE_STMTS) {
            if (keywords.count(kw)) {
                Logger::msg(AKU_LOG_ERROR, "Statement `" + keyword + "` can't be used with `" + kw + "`");
                return std::make_tuple(AKU_EQUERY_PARSING_ERROR,
                                       "Field `" + keyword + "` can't be used with `" + kw + "`");
            }
        }
    }
    return std::make_tuple(AKU_SUCCESS, ErrorMsg());
}

/** DEPRICATED! Select statement should look like this:
 * { "select": "meta:*", ...}
 */
std::tuple<aku_Status, std::vector<aku_ParamId>, ErrorMsg> QueryParser::parse_select_meta_query(
        boost::property_tree::ptree const& ptree,
        SeriesMatcher const& matcher)
{
    std::vector<aku_ParamId> ids;
    ErrorMsg error;
    aku_Status status;
    std::tie(status, error) = validate_query(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, ids, error);
    }
    std::string name;
    std::tie(status, name, error) = parse_select_stmt(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, ids, error);
    }
    if (!is_meta_query(name)) {
        return std::make_tuple(AKU_EQUERY_PARSING_ERROR, ids,
                               "Invalid query object, \"select\": \"meta:names\" expected");
    }

    std::vector<std::string> metrics;
    if (name.length() > 10 && boost::starts_with(name, "meta:names")) {
        boost::erase_first(name, "meta:names:");
        metrics.push_back(name);
    }

    std::tie(status, ids, error) = parse_where_clause(ptree, metrics, matcher);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, ids, error);
    }
    return std::make_tuple(AKU_SUCCESS, ids, ErrorMsg());
}


/**
 * Search query parser
 *
 * The only supported search query:
 * ```
 * {
 *  "select": "metric",
 *  "where": { "tag": ["value1", "value2"], ... }
 * }
 * ```
 *
 * or
 *
 * ```
 * {
 *  "select": "metric",
 *  "where": [ { "tag1": "value1", "tag2": "value2" },
 *             { "tag1": "value3", "tag2": "value4" } ]
 * }
 * ```
 *
 * Returns list of series names.
 *
 * Limit/offset/output statements also supported.
 */

std::tuple<aku_Status, std::vector<aku_ParamId>, ErrorMsg>
    QueryParser::parse_search_query(boost::property_tree::ptree const& ptree,
                                    SeriesMatcher const& matcher)
{
    std::vector<aku_ParamId> ids;
    ErrorMsg error;
    aku_Status status;
    std::tie(status, error) = validate_query(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, ids, error);
    }
    std::string name;
    std::tie(status, name, error) = parse_search_select_stmt(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, ids, error);
    }
    std::vector<std::string> metrics;
    if (!name.empty()) {
        metrics.push_back(name);
    }
    std::tie(status, ids, error) = parse_where_clause(ptree, metrics, matcher);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, ids, error);
    }
    return std::make_tuple(AKU_SUCCESS, ids, ErrorMsg());
}

/**
 * Suggest query parser
 *
 * 1) Suggest metric name
 *
 * Return all metric names that starts with 'df.'
 * ```json
 * {
 *      "select": "metric-names",
 *      "starts-with": "df."
 * }
 * ```
 *
 * Return all metric names
 * ```json
 * {
 *      "select": "metric-names"
 * }
 * ```
 *
 * 2) Suggest tag names
 *
 * Return all tag names that starts with 'h' and used with 'df.used' metric
 * ```json
 * {
 *      "select": "tag-names",
 *      "metric": "df.used",
 *      "starts-with": "h"
 * }
 * ```
 *
 * Return all tag names that was used with 'df.used' metric
 * ```json
 * {
 *      "select": "tag-names",
 *      "metric": "df.used",
 * }
 * ```
 *
 * 3) Suggest tag values
 *
 * Return all values for the 'host' tag that starts with '192.' and was used with 'df.used' metric.
 * ```json
 * {
 *      "select": "tag-values",
 *      "metric": "df.used",
 *      "tag": "host"
 *      "starts-with": "192."
 * }
 * ```
 *
 * Return all values for the 'host' tag that was used with 'df.used' metric.
 * ```json
 * {
 *      "select": "tag-values",
 *      "metric": "df.used",
 *      "tag": "host"
 * }
 * ```
 */

enum class SuggestQueryKind {
    SUGGEST_METRIC_NAMES,
    SUGGEST_TAG_NAMES,
    SUGGEST_TAG_VALUES,
    SUGGEST_ERROR,
};

static std::tuple<aku_Status, ErrorMsg> validate_suggest_query(boost::property_tree::ptree const& ptree) {
    static const std::set<std::string> ALLOWED_STMTS = {
        "select",
        "metric",
        "tag",
        "starts-with",
        "output"
    };
    for (const auto& item: ptree) {
        std::string keyword = item.first;
        if (ALLOWED_STMTS.count(keyword) == 0) {
            Logger::msg(AKU_LOG_ERROR, "Unexpected `" + keyword + "` statement");
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR, "Unexpected field `" + keyword + "` in query object");
        }
    }
    return std::make_tuple(AKU_SUCCESS, ErrorMsg());
}

static std::tuple<SuggestQueryKind, aku_Status, ErrorMsg> get_suggest_query_type(boost::property_tree::ptree const& ptree) {
    auto child = ptree.get_child_optional("select");
    if (!child) {
        Logger::msg(AKU_LOG_ERROR, "Query object missing `select` field");
        return std::make_tuple(SuggestQueryKind::SUGGEST_ERROR,
                               AKU_EQUERY_PARSING_ERROR,
                               "Query object missing `select` field");
    }
    auto value = child->get_value_optional<std::string>();
    if (!value) {
        Logger::msg(AKU_LOG_ERROR, "Query object has invalid `select` field, single value expected");
        return std::make_tuple(SuggestQueryKind::SUGGEST_ERROR,
                               AKU_EQUERY_PARSING_ERROR,
                               "Query object has invalid `select` field, single value expected");
    }
    auto strval = value.get();
    if (strval == "metric-names") {
        return std::make_tuple(SuggestQueryKind::SUGGEST_METRIC_NAMES, AKU_SUCCESS, ErrorMsg());
    } else if (strval == "tag-names") {
        return std::make_tuple(SuggestQueryKind::SUGGEST_TAG_NAMES, AKU_SUCCESS, ErrorMsg());
    } else if (strval == "tag-values") {
        return std::make_tuple(SuggestQueryKind::SUGGEST_TAG_VALUES, AKU_SUCCESS, ErrorMsg());
    }
    Logger::msg(AKU_LOG_ERROR, "Query object has invalid `select` field, unknown target " + strval);
    return std::make_tuple(SuggestQueryKind::SUGGEST_ERROR,
                           AKU_EQUERY_PARSING_ERROR,
                           "Query object has invalid `select` field, unknown target " + strval);
}

static std::string get_starts_with(boost::property_tree::ptree const& ptree) {
    auto child = ptree.get_child_optional("starts-with");
    if (!child) {
        return std::string();
    }
    auto value = child->get_value_optional<std::string>();
    if (!value) {
        return std::string();
    }
    auto strval = value.get();
    return strval;
}

static std::tuple<aku_Status, std::string> get_property(std::string name, boost::property_tree::ptree const& ptree) {
    auto child = ptree.get_child_optional(name);
    if (!child) {
        return std::make_tuple(AKU_ENOT_FOUND, std::string());
    }
    auto value = child->get_value_optional<std::string>();
    if (!value) {
        return std::make_tuple(AKU_EBAD_ARG, std::string());
    }
    auto strval = value.get();
    return std::make_tuple(AKU_SUCCESS, strval);
}

std::tuple<aku_Status, std::shared_ptr<PlainSeriesMatcher>, std::vector<aku_ParamId>, ErrorMsg>
    QueryParser::parse_suggest_query(boost::property_tree::ptree const& ptree, SeriesMatcher const& matcher)
{
    std::shared_ptr<PlainSeriesMatcher> substitute;
    std::vector<aku_ParamId> ids;
    ErrorMsg error;
    aku_Status status;
    std::tie(status, error) = validate_suggest_query(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, substitute, ids, error);
    }
    SuggestQueryKind kind;
    std::tie(kind, status, error) = get_suggest_query_type(ptree);
    std::string starts_with = get_starts_with(ptree);
    std::vector<StringT> results;
    std::string metric_name;
    std::string tag_name;
    switch (kind) {
    case SuggestQueryKind::SUGGEST_METRIC_NAMES:
        // This should work for empty 'starts_with' values. Method should return all metric names.
        results = matcher.suggest_metric(starts_with);
    break;
    case SuggestQueryKind::SUGGEST_TAG_NAMES:
        std::tie(status, metric_name) = get_property("metric", ptree);
        if (status != AKU_SUCCESS) {
            Logger::msg(AKU_LOG_ERROR, "Metric name expected");
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR, substitute, ids, "Metric name expected");
        }
        results = matcher.suggest_tags(metric_name, starts_with);
    break;
    case SuggestQueryKind::SUGGEST_TAG_VALUES:
        std::tie(status, metric_name) = get_property("metric", ptree);
        if (status != AKU_SUCCESS) {
            Logger::msg(AKU_LOG_ERROR, "Metric name expected");
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR, substitute, ids, "Metric name expected");
        }
        std::tie(status, tag_name) = get_property("tag", ptree);
        if (status != AKU_SUCCESS) {
            Logger::msg(AKU_LOG_ERROR, "Tag name expected");
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR, substitute, ids, "Tag name expected");
        }
        results = matcher.suggest_tag_values(metric_name, tag_name, starts_with);
    break;
    case SuggestQueryKind::SUGGEST_ERROR:
        return std::make_tuple(AKU_EQUERY_PARSING_ERROR, substitute, ids, error);
    };

    substitute.reset(new PlainSeriesMatcher());
    for (auto mname: results) {
        auto mid = substitute->add(mname.first, mname.first + mname.second);
        ids.push_back(mid);
    }

    return std::make_tuple(AKU_SUCCESS, substitute, ids, ErrorMsg());
}

std::tuple<aku_Status, ReshapeRequest, ErrorMsg> QueryParser::parse_select_query(
                                                    boost::property_tree::ptree const& ptree,
                                                    const SeriesMatcher &matcher)
{
    ReshapeRequest result = {};
    ErrorMsg error;
    aku_Status status;
    std::tie(status, error) = validate_query(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    Logger::msg(AKU_LOG_INFO, "Parsing query:");
    Logger::msg(AKU_LOG_INFO, to_json(ptree, true).c_str());

    // Metric name
    std::string metric;
    std::tie(status, metric, error) = parse_select_stmt(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    // Group-by statement
    GroupByOpType op;
    std::vector<std::string> tags;
    std::tie(status, tags, op, error) = parse_groupby(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }
    auto groupbytag = std::shared_ptr<GroupByTag>();
    if (!tags.empty()) {
        groupbytag.reset(new GroupByTag(matcher, metric, tags, op));
    }

    // Order-by statment
    OrderBy order;
    std::tie(status, order, error) = parse_orderby(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    // Where statement
    std::vector<aku_ParamId> ids;
    std::tie(status, ids, error) = parse_where_clause(ptree, {metric}, matcher);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    // Read timestamps
    aku_Timestamp ts_begin, ts_end;
    std::tie(status, ts_begin, ts_end, error) = parse_range_timestamp(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    // Initialize request
    result.agg.enabled = false;
    result.select.begin = ts_begin;
    result.select.end = ts_end;
    result.select.columns.push_back(Column{ids});

    result.order_by = order;

    result.group_by.enabled = static_cast<bool>(groupbytag);
    if (groupbytag) {
        result.group_by.transient_map = groupbytag->get_mapping();
        result.select.matcher = std::shared_ptr<PlainSeriesMatcher>(groupbytag, &groupbytag->get_series_matcher());
        if (result.group_by.transient_map.empty()) {
            return std::make_tuple(AKU_ENO_DATA, result, "Group-by statement doesn't match any series");
        }
    }
    else {
        result.select.global_matcher = &matcher;
    }

    std::tie(status, result.select.filters, result.select.filter_rule, error) = parse_filter(ptree, {metric});
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    return std::make_tuple(AKU_SUCCESS, result, ErrorMsg());
}

std::tuple<aku_Status, ReshapeRequest, ErrorMsg> QueryParser::parse_select_events_query(
                                                    boost::property_tree::ptree const& ptree,
                                                    const SeriesMatcher &matcher)
{
    ReshapeRequest result = {};
    result.select.events = true;
    ErrorMsg error;
    aku_Status status;
    std::tie(status, error) = validate_query(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    Logger::msg(AKU_LOG_INFO, "Parsing query:");
    Logger::msg(AKU_LOG_INFO, to_json(ptree, true).c_str());

    // Metric name
    std::string metric;
    std::tie(status, metric, error) = parse_select_events_stmt(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    // Filter
    std::string flt;
    std::tie(status, flt, error) = parse_select_events_filter_field(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }
    if (!flt.empty()) {
        result.select.event_body_regex = flt;
    }

    // Group-by statement
    GroupByOpType op;
    std::vector<std::string> tags;
    std::tie(status, tags, op, error) = parse_groupby(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }
    auto groupbytag = std::shared_ptr<GroupByTag>();
    if (!tags.empty()) {
        groupbytag.reset(new GroupByTag(matcher, metric, tags, op));
    }

    // Order-by statment
    OrderBy order;
    std::tie(status, order, error) = parse_orderby(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    // Where statement
    std::vector<aku_ParamId> ids;
    std::tie(status, ids, error) = parse_where_clause(ptree, {metric}, matcher);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    // Read timestamps
    aku_Timestamp ts_begin, ts_end;
    std::tie(status, ts_begin, ts_end, error) = parse_range_timestamp(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    // Initialize request
    result.agg.enabled = false;
    result.select.begin = ts_begin;
    result.select.end = ts_end;
    result.select.columns.push_back(Column{ids});

    result.order_by = order;

    result.group_by.enabled = static_cast<bool>(groupbytag);
    if (groupbytag) {
        result.group_by.transient_map = groupbytag->get_mapping();
        result.select.matcher = std::shared_ptr<PlainSeriesMatcher>(groupbytag, &groupbytag->get_series_matcher());
        if (result.group_by.transient_map.empty()) {
            return std::make_tuple(AKU_ENO_DATA, result, "Group-by statement doesn't match any series");
        }
    }
    else {
        result.select.global_matcher = &matcher;
    }

    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    return std::make_tuple(AKU_SUCCESS, result, ErrorMsg());
}

/** Initialize 'req.select.matcher' object with modified matcher that overrides series names
 * using the function name. E.g. cpu.system host=abc -> cpu.system:max host=abc.
 */
static std::tuple<aku_Status, ErrorMsg>
    init_matcher_in_aggregate(
            ReshapeRequest*                         req,
            SeriesMatcher const&                    global_matcher)
{
    const auto& ids = req->select.columns.at(0).ids;
    auto matcher = std::make_shared<PlainSeriesMatcher>();
    for (u32 ix = 0; ix < ids.size(); ix++) {
        auto id = ids.at(ix);
        auto fn = req->agg.func.at(ix);
        auto sname = global_matcher.id2str(id);
        std::string name(sname.first, sname.first + sname.second);
        auto mpos = name.find_first_of(" ");
        if (mpos == std::string::npos) {
            Logger::msg(AKU_LOG_ERROR, "Matcher initialization failed. Invalid series name.");
            return std::make_tuple(AKU_EBAD_DATA, "Invalid series name `" + name + "`");
        }
        std::string str = name.substr(0, mpos) + ":" + Aggregation::to_string(fn) + name.substr(mpos);
        matcher->_add(str, id);
    }
    req->select.matcher = matcher;
    return std::make_tuple(AKU_SUCCESS, ErrorMsg());
}

std::tuple<aku_Status, ReshapeRequest, ErrorMsg> QueryParser::parse_aggregate_query(
        boost::property_tree::ptree const& ptree,
        SeriesMatcher const& matcher)
{
    ReshapeRequest result = {};

    ErrorMsg error;
    aku_Status status;
    std::tie(status, error) = validate_query(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    Logger::msg(AKU_LOG_INFO, "Parsing query:");
    Logger::msg(AKU_LOG_INFO, to_json(ptree, true).c_str());

    // Metric name
    std::vector<std::string> metrics;
    std::vector<std::string> aggfun;
    std::tie(status, metrics, aggfun, error) = parse_aggregate_stmt(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }
    std::vector<AggregationFunction> func;
    for (auto af: aggfun) {
        AggregationFunction f;
        std::tie(status, f) = Aggregation::from_string(af);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, result, "Unknown aggregate function `" + af + "`");
        }
        func.push_back(f);
    }

    // Group-by statement
    GroupByOpType op;
    std::vector<std::string> tags;
    std::tie(status, tags, op, error) = parse_groupby(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }
    auto groupbytag = std::shared_ptr<GroupByTag>();
    if (!tags.empty()) {
        groupbytag.reset(new GroupByTag(matcher, metrics, aggfun, tags, op));
    }

    // Order-by statment is disallowed
    auto orderby = ptree.get_child_optional("order-by");
    if (orderby) {
        Logger::msg(AKU_LOG_INFO, "Unexpected `order-by` statement found in `aggregate` query");
        return std::make_tuple(AKU_EQUERY_PARSING_ERROR,
                               result,
                               "Unexpected `order-by` statement found in `aggregate` query");
    }

    // Where statement
    std::vector<aku_ParamId> ids;
    std::vector<AggregationFunction> id2func;
    for (u32 ix = 0; ix < metrics.size(); ix++) {
        std::vector<aku_ParamId> subids;
        std::tie(status, subids, error) = parse_where_clause(ptree, {metrics.at(ix)}, matcher);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, result, error);
        }
        std::vector<AggregationFunction> subfun(subids.size(), func.at(ix));
        std::copy(subids.begin(), subids.end(), std::back_inserter(ids));
        std::copy(subfun.begin(), subfun.end(), std::back_inserter(id2func));
    }
    {
        std::vector<aku_ParamId> sids;
        std::copy(ids.begin(), ids.end(), std::back_inserter(sids));
        std::sort(sids.begin(), sids.end());
        if (std::unique(sids.begin(), sids.end()) != sids.end()){
            Logger::msg(AKU_LOG_INFO, "Duplicate metric name found in `aggregate` query");
            return std::make_tuple(AKU_EQUERY_PARSING_ERROR,
                                   result,
                                   "Duplicate metric name found in `aggregate` query");
        }
    }

    // Read timestamps
    aku_Timestamp ts_begin, ts_end;
    std::tie(status, ts_begin, ts_end, error) = parse_range_timestamp(ptree, true);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    // Initialize request
    result.agg.enabled = true;
    result.agg.func = id2func;

    result.select.begin = ts_begin;
    result.select.end = ts_end;
    result.select.columns.push_back(Column{ids});

    result.order_by = OrderBy::SERIES;

    std::tie(status, error) = init_matcher_in_aggregate(&result, matcher);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    result.group_by.enabled = static_cast<bool>(groupbytag);
    if (groupbytag) {
        result.group_by.transient_map = groupbytag->get_mapping();
        result.select.matcher = std::shared_ptr<PlainSeriesMatcher>(groupbytag, &groupbytag->local_matcher_);
        if (result.group_by.transient_map.empty()) {
            return std::make_tuple(AKU_ENO_DATA, result, "Group-by statement doesn't match any series");
        }
    }
    else {
        result.select.global_matcher = &matcher;
    }

    return std::make_tuple(AKU_SUCCESS, result, ErrorMsg());
}

static std::tuple<aku_Status, ErrorMsg>
    init_matcher_in_group_aggregate(
            ReshapeRequest*                         req,
            SeriesMatcher const&                    global_matcher,
            std::vector<AggregationFunction> const& func_names)
{
    std::vector<aku_ParamId> ids = req->select.columns.at(0).ids;
    auto matcher = std::make_shared<PlainSeriesMatcher>();
    for (auto id: ids) {
        auto sname = global_matcher.id2str(id);
        std::string name(sname.first, sname.first + sname.second);
        auto npos = name.find_first_of(' ');
        if (npos == std::string::npos) {
            Logger::msg(AKU_LOG_ERROR, "Matcher initialization failed. Invalid series name.");
            return std::make_tuple(AKU_EBAD_DATA, "Invalid series name `" + name + "`");
        }
        std::string metric_name = name.substr(0, npos);
        auto tags = name.substr(npos);
        std::stringstream str;
        bool first = true;
        for (auto func: func_names) {
            if (first) {
                first = false;
            } else {
                str << '|';
            }
            str << metric_name << ":" << Aggregation::to_string(func);
        }
        str << tags;
        matcher->_add(str.str(), id);
    }
    req->select.matcher = matcher;
    return std::make_tuple(AKU_SUCCESS, ErrorMsg());
}

static std::tuple<aku_Status, ErrorMsg>
    init_matcher_in_group_aggregate(
            ReshapeRequest*                         req,
            std::shared_ptr<GroupByTag>             groupbytag,
            std::vector<AggregationFunction> const& func_names)
{
    auto matcher = std::make_shared<PlainSeriesMatcher>();
    auto& gbtmatcher = groupbytag->get_series_matcher();
    const auto& gbtmap = groupbytag->get_mapping();
    std::vector<aku_ParamId> ids;
    for (auto kv: gbtmap) {
        ids.push_back(kv.second);
    }
    std::sort(ids.begin(), ids.end());
    auto it = std::unique(ids.begin(), ids.end());
    ids.erase(it, ids.end());
    for (auto id: ids) {
        auto sname = gbtmatcher.id2str(id);
        std::string name(sname.first, sname.first + sname.second);
        auto npos = name.find_first_of(' ');
        if (npos == std::string::npos) {
            Logger::msg(AKU_LOG_ERROR, "Matcher initialization failed. Invalid series name.");
            return std::make_tuple(AKU_EBAD_DATA, "Invalid series name `" + name + "`");
        }
        std::string metric_name = name.substr(0, npos);
        auto tags = name.substr(npos);
        std::stringstream str;
        bool first = true;
        for (auto func: func_names) {
            if (first) {
                first = false;
            } else {
                str << '|';
            }
            str << metric_name << ":" << Aggregation::to_string(func);
        }
        str << tags;
        matcher->_add(str.str(), id);
    }
    req->select.matcher = matcher;
    return std::make_tuple(AKU_SUCCESS, ErrorMsg());
}

std::tuple<aku_Status, ReshapeRequest, ErrorMsg> QueryParser::parse_group_aggregate_query(
        boost::property_tree::ptree const& ptree,
        SeriesMatcher const& matcher)
{
    ReshapeRequest result = {};
    ErrorMsg error;
    aku_Status status;
    std::tie(status, error) = validate_query(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    Logger::msg(AKU_LOG_INFO, "Parsing query:");
    Logger::msg(AKU_LOG_INFO, to_json(ptree, true).c_str());

    // Metric name
    GroupAggregate gagg;
    std::tie(status, gagg, error) = parse_group_aggregate_stmt(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }
    if (gagg.func.empty()) {
        Logger::msg(AKU_LOG_ERROR, "Aggregation fuction is not set");
        return std::make_tuple(status, result, "Aggregation fuction is not set");
    }
    if (gagg.step == 0) {
        Logger::msg(AKU_LOG_ERROR, "Step can't be zero");
        return std::make_tuple(status, result, "Step can't be zero");
    }

    // Group-by statement
    GroupByOpType op;
    std::vector<std::string> tags;
    std::tie(status, tags, op, error) = parse_groupby(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }
    auto groupbytag = std::shared_ptr<GroupByTag>();
    if (!tags.empty()) {
        std::vector<std::string> fnames;
        groupbytag.reset(new GroupByTag(matcher, gagg.metric, fnames, tags, op));
    }

    // Where statement
    std::vector<aku_ParamId> ids;
    std::tie(status, ids, error) = parse_where_clause(ptree, gagg.metric, matcher);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    // Read timestamps
    aku_Timestamp ts_begin, ts_end;
    std::tie(status, ts_begin, ts_end, error) = parse_range_timestamp(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    // Parse filter query
    std::vector<std::string> funcnames;
    for (auto fn: gagg.func) {
        auto fnname = Aggregation::to_string(fn);
        funcnames.push_back(fnname);
    }
    std::tie(status, result.select.filters, result.select.filter_rule, error) = parse_filter(ptree, funcnames);
    // Functions are used instead of metrics because group-aggregate query can produce
    // more than one output, for instance:
    // `"group-aggregate": { "metric": "foo", "step": "1s", "func": ["min", "max"] }` query
    // will produce tuples with two components - min and max. User may want to filter by
    // first component or by the second. In this case the filter statement may look like this:
    // `"filter": { "max": { "gt": 100 } }` or `"filter": { "min": { "gt": 100 } }` or
    // combination of both.
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    // Initialize request
    result.agg.enabled = true;
    result.agg.func = gagg.func;
    result.agg.step = gagg.step;

    result.select.begin = ts_begin;
    result.select.end = ts_end;
    result.select.columns.push_back(Column{ids});

    std::tie(status, result.order_by, error) = parse_orderby(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    if (groupbytag) {
        result.group_by.enabled = true;
        std::tie(status, error) = init_matcher_in_group_aggregate(&result, groupbytag, gagg.func);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, result, error);
        }
        result.group_by.transient_map = groupbytag->get_mapping();
        if (result.group_by.transient_map.empty()) {
            return std::make_tuple(AKU_ENO_DATA, result, "Group-by statement doesn't match any series");
        }
    }
    else {
        std::tie(status, error) = init_matcher_in_group_aggregate(&result, matcher, gagg.func);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, result, error);
        }
    }

    return std::make_tuple(AKU_SUCCESS, result, ErrorMsg());

}

static std::tuple<aku_Status, ErrorMsg> init_matcher_in_join_query(
        ReshapeRequest*                 req,
        SeriesMatcher const&            global_matcher,
        std::vector<std::string> const& metric_names)
{
    if (req->select.columns.size() < 2) {
        Logger::msg(AKU_LOG_ERROR, "Can't initialize matcher. Query is not a `JOIN` query.");
        return std::make_tuple(AKU_EBAD_ARG, "Can't initialize matcher. Query is not a `JOIN` query.");
    }
    if (req->select.columns.size() != metric_names.size()) {
        Logger::msg(AKU_LOG_ERROR, "Can't initialize matcher. Invalid metric names.");
        return std::make_tuple(AKU_EBAD_ARG, "Can't initialize matcher. Invalid metric names.");
    }
    std::vector<aku_ParamId> ids = req->select.columns.at(0).ids;
    auto matcher = std::make_shared<PlainSeriesMatcher>();
    for (auto id: ids) {
        auto sname = global_matcher.id2str(id);
        std::string name(sname.first, sname.first + sname.second);
        if (!boost::algorithm::starts_with(name, metric_names.front())) {
            Logger::msg(AKU_LOG_ERROR, "Matcher initialization failed. Invalid metric names.");
            return std::make_tuple(AKU_EBAD_DATA, "Matcher initialization failed. Invalid metric names.");
        }
        auto tags = name.substr(metric_names.front().size());
        std::stringstream str;
        bool first = true;
        for (auto metric: metric_names) {
            if (first) {
                first = false;
            } else {
                str << '|';
            }
            str << metric;
        }
        str << tags;
        matcher->_add(str.str(), id);
    }
    req->select.matcher = matcher;
    return std::make_tuple(AKU_SUCCESS, ErrorMsg());
}

std::tuple<aku_Status, ReshapeRequest, ErrorMsg> QueryParser::parse_join_query(
        boost::property_tree::ptree const&  ptree,
        SeriesMatcher const&                matcher)
{
    ReshapeRequest result = {};
    ErrorMsg error;
    aku_Status status;
    std::tie(status, error) = validate_query(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    std::vector<std::string> metrics;
    std::tie(status, metrics, error) = parse_join_stmt(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    // Order-by statment
    OrderBy order;
    std::tie(status, order, error) = parse_orderby(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }
    result.order_by = order;

    // Where statement
    std::vector<aku_ParamId> ids;
    std::tie(status, ids, error) = parse_where_clause(ptree, metrics, matcher);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    // Read timestamps
    aku_Timestamp ts_begin, ts_end;
    std::tie(status, ts_begin, ts_end, error) = parse_range_timestamp(ptree);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    // TODO: implement group-by
    result.group_by.enabled = false;

    // Initialize request
    result.agg.enabled = false;
    result.select.begin = ts_begin;
    result.select.end = ts_end;

    std::tie(status, result.select.filters, result.select.filter_rule, error) = parse_filter(ptree, metrics);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    size_t ncolumns = metrics.size();
    size_t nentries = ids.size() / ncolumns;
    if (ids.size() % ncolumns != 0) {
        AKU_PANIC("Invalid `where` statement processing results");
    }
    u32 idix = 0;
    for (auto i = 0u; i < ncolumns; i++) {
        Column column;
        for (auto j = 0u; j < nentries; j++) {
            column.ids.push_back(ids.at(idix));
            idix++;
        }
        result.select.columns.push_back(column);
    }

    std::tie(status, error) = init_matcher_in_join_query(&result, matcher, metrics);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, result, error);
    }

    return std::make_tuple(AKU_SUCCESS, result, ErrorMsg());
}

struct TerminalNode : QP::Node {

    InternalCursor* cursor;

    TerminalNode(InternalCursor* cur)
        : cursor(cur)
    {
    }

    // Node interface

    void complete() {
        cursor->complete();
    }

    bool put(MutableSample& sample) {
        return cursor->put(sample.payload_.sample);
    }

    void set_error(aku_Status status) {
        cursor->set_error(status);
    }

    int get_requirements() const {
        return TERMINAL;
    }
};


std::tuple<aku_Status, std::shared_ptr<Node>, ErrorMsg>
    make_sampler(boost::property_tree::ptree const& ptree,
                 std::shared_ptr<Node> next,
                 const ReshapeRequest& req)
{
    try {
        std::string name;
        name = ptree.get<std::string>("name");
        return std::make_tuple(AKU_SUCCESS, QP::create_node(name, ptree, req, next), ErrorMsg());
    } catch (const boost::property_tree::ptree_error& e) {
        auto err = ErrorMsg("Query object has invalid `apply` field ") + e.what();
        Logger::msg(AKU_LOG_ERROR, err);
        return std::make_tuple(AKU_EQUERY_PARSING_ERROR, nullptr, err);
    } catch (const QueryParserError& e) {
        auto err = ErrorMsg("Query object has invalid `apply` field ") + e.what();
        Logger::msg(AKU_LOG_ERROR, err);
        return std::make_tuple(AKU_EQUERY_PARSING_ERROR, nullptr, err);
    } catch (...) {
        Logger::msg(AKU_LOG_ERROR, std::string("Unknown query parsing error: ") +
                    boost::current_exception_diagnostic_information());
        return std::make_tuple(AKU_EQUERY_PARSING_ERROR,
                               nullptr,
                               "Query object has invalid `apply` field");
    }
}

std::tuple<aku_Status, std::vector<std::shared_ptr<Node>>, ErrorMsg> QueryParser::parse_processing_topology(
    boost::property_tree::ptree const& ptree,
    InternalCursor* cursor,
    const ReshapeRequest& req)
{
    std::shared_ptr<Node> terminal = std::make_shared<TerminalNode>(cursor);
    auto prev = terminal;
    std::vector<std::shared_ptr<Node>> result;

    auto apply = ptree.get_child_optional("apply");
    if (apply) {
        for (auto it = apply->rbegin(); it != apply->rend(); it++) {
            aku_Status status;
            std::shared_ptr<Node> node;
            ErrorMsg err;
            std::tie(status, node, err) = make_sampler(it->second, prev, req);
            if (status == AKU_SUCCESS) {
                result.push_back(node);
                prev = node;
            } else {
                return std::make_tuple(status, result, err);
            }
        }
    }

    auto limoff = parse_limit_offset(ptree);
    if (limoff.first != 0 || limoff.second != 0) {
        auto node = std::make_shared<QP::Limiter>(limoff.first, limoff.second, prev);
        result.push_back(node);
        prev = node;
    }

    result.push_back(terminal);
    return std::make_tuple(AKU_SUCCESS, result, ErrorMsg());
}

}}  // namespace
