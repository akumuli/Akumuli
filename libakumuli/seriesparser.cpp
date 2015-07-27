/**
 * Copyright (c) 2015 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "queryprocessor.h"
#include "seriesparser.h"
#include "util.h"
#include "datetime.h"

#include <string>
#include <map>
#include <algorithm>
#include <regex>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

namespace Akumuli {

//                          //
//      Series Matcher      //
//                          //

static const SeriesMatcher::StringT EMPTY = std::make_pair(nullptr, 0);

SeriesMatcher::SeriesMatcher(uint64_t starting_id)
    : table(StringTools::create_table(0x1000))
    , series_id(starting_id)
{
    if (starting_id == 0u) {
        AKU_PANIC("Bad series ID");
    }
}

uint64_t SeriesMatcher::add(const char* begin, const char* end) {
    auto id = series_id++;
    StringT pstr = pool.add(begin, end, id);
    auto tup = std::make_tuple(std::get<0>(pstr), std::get<1>(pstr), id);
    table[pstr] = id;
    inv_table[id] = pstr;
    names.push_back(tup);
    return id;
}

void SeriesMatcher::_add(std::string series, uint64_t id) {
    if (series.empty()) {
        return;
    }
    const char* begin = &series[0];
    const char* end = begin + series.size();
    StringT pstr = pool.add(begin, end, id);
    table[pstr] = id;
    inv_table[id] = pstr;
}

uint64_t SeriesMatcher::match(const char* begin, const char* end) {

    int len = end - begin;
    StringT str = std::make_pair(begin, len);

    auto it = table.find(str);
    if (it == table.end()) {
        return 0ul;
    }
    return it->second;
}

SeriesMatcher::StringT SeriesMatcher::id2str(uint64_t tokenid) {
    auto it = inv_table.find(tokenid);
    if (it == inv_table.end()) {
        return EMPTY;
    }
    return it->second;
}

void SeriesMatcher::pull_new_names(std::vector<SeriesMatcher::SeriesNameT> *buffer) {
    std::swap(names, *buffer);
}

static boost::optional<std::string> parse_select_stmt(boost::property_tree::ptree const& ptree, aku_logger_cb_t logger) {
    auto select = ptree.get_child_optional("select");
    if (select && select->empty()) {
        // simple select query
        auto str = select->get_value<std::string>("");
        if (str == "names") {
            // the only supported select query for now
            return str;
        }
        (*logger)(AKU_LOG_ERROR, "Invalid `select` query");
        auto rte = std::runtime_error("Invalid `select` query");
        BOOST_THROW_EXCEPTION(rte);
    }
    return boost::optional<std::string>();
}

static boost::optional<const boost::property_tree::ptree&> parse_sampling_params(boost::property_tree::ptree const& ptree) {
    return ptree.get_child_optional("sample");
}

static std::vector<std::string> parse_metric(boost::property_tree::ptree const& ptree,
                                             aku_logger_cb_t logger) {
    std::vector<std::string> metrics;
    auto metric = ptree.get_child_optional("metric");
    if (metric) {
        auto single = metric->get_value<std::string>();
        if (single.empty()) {
            for(auto child: *metric) {
                auto metric_name = child.second.get_value<std::string>();
                metrics.push_back(metric_name);
            }
        } else {
            metrics.push_back(single);
        }
    }
    return metrics;
}

static aku_Timestamp parse_range_timestamp(boost::property_tree::ptree const& ptree,
                                           std::string const& name,
                                           aku_logger_cb_t logger) {
    auto range = ptree.get_child("range");
    for(auto child: range) {
        if (child.first == name) {
            auto iso_string = child.second.get_value<std::string>();
            auto ts = DateTimeUtil::from_iso_string(iso_string.c_str());
            return ts;
        }
    }
    std::stringstream fmt;
    fmt << "can't find `" << name << "` tag inside the query";
    QueryParserError error(fmt.str().c_str());
    BOOST_THROW_EXCEPTION(error);
}


static aku_ParamId extract_id_from_pool(StringPool::StringT res) {
    // Series name in string pool should be followed by \0 character and 64-bit series id.
    auto p = res.first + res.second;
    assert(p[0] == '\0');
    p += 1;  // zero terminator + sizeof(uint64_t)
    return *reinterpret_cast<uint64_t const*>(p);
}

static std::vector<aku_ParamId> parse_where_clause(boost::property_tree::ptree const& ptree,
                                                   std::string metric,
                                                   std::string pred,
                                                   StringPool const& pool,
                                                   aku_logger_cb_t logger)
{
    std::vector<aku_ParamId> ids;
    bool not_set = false;
    auto where = ptree.get_child_optional("where");
    if (where) {
        for (auto child: *where) {
            auto predicate = child.second;
            auto items = predicate.get_child_optional(pred);
            if (items) {
                for (auto item: *items) {
                    std::string tag = item.first;
                    auto idslist = item.second;
                    // Read idlist
                    for (auto idnode: idslist) {
                        std::string value = idnode.second.get_value<std::string>();
                        std::stringstream series_regexp;
                        series_regexp << "(" << metric << R"((?:\s\w+=\w+)*\s)"
                                      << tag << "=" << value << R"((?:\s\w+=\w+)*))";
                        std::string regex = series_regexp.str();
                        auto results = pool.regex_match(regex.c_str());
                        for(auto res: results) {
                            aku_ParamId id = extract_id_from_pool(res);
                            ids.push_back(id);
                        }
                    }
                }
            } else {
                not_set = true;
            }
        }
    } else {
        not_set = true;
    }
    if (not_set) {
        if (pred == "in") {
            // there is no "in" predicate so we need to include all
            // series from this metric
            std::stringstream series_regexp;
            series_regexp << "" << metric << R"((\s\w+=\w+)*)";
            std::string regex = series_regexp.str();
            auto results = pool.regex_match(regex.c_str());
            for(auto res: results) {
                aku_ParamId id = extract_id_from_pool(res);
                ids.push_back(id);
            }
        }
    }
    return ids;
}

static std::string to_json(boost::property_tree::ptree const& ptree, bool pretty_print = true) {
    std::stringstream ss;
    boost::property_tree::write_json(ss, ptree, pretty_print);
    return ss.str();
}

std::shared_ptr<QP::IQueryProcessor>
SeriesMatcher::build_query_processor(const char* query, std::shared_ptr<QP::Node> terminal, aku_logger_cb_t logger) {
    namespace pt = boost::property_tree;
    using namespace QP;

    const auto NOSAMPLE = std::make_pair<std::string, size_t>("", 0u);

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
        pt::json_parser::read_json(stream, ptree);
    } catch (pt::json_parser_error& e) {
        // Error, bad query
        (*logger)(AKU_LOG_ERROR, e.what());
        throw QueryParserError(e.what());
    }

    logger(AKU_LOG_INFO, "Parsing query:");
    logger(AKU_LOG_INFO, to_json(ptree, true).c_str());

    try {
        // Read metric(s) name
        auto metrics = parse_metric(ptree, logger);

        // Read select statment
        auto select = parse_select_stmt(ptree, logger);

        // Read sampling method
        auto sampling_params = parse_sampling_params(ptree);

        // Read where clause
        std::vector<aku_ParamId> ids_included;
        std::vector<aku_ParamId> ids_excluded;

        for(auto metric: metrics) {

            auto in = parse_where_clause(ptree, metric, "in", pool, logger);
            std::copy(in.begin(), in.end(), std::back_inserter(ids_included));

            auto notin = parse_where_clause(ptree, metric, "not_in", pool, logger);
            std::copy(notin.begin(), notin.end(), std::back_inserter(ids_excluded));
        }

        if (sampling_params && select) {
            (*logger)(AKU_LOG_ERROR, "Can't combine select and sample statements together");
            auto rte = std::runtime_error("`sample` and `select` can't be used together");
            BOOST_THROW_EXCEPTION(rte);
        }

        // Build topology
        std::shared_ptr<Node> next = terminal;
        if (!select) {
            // Read timestamps
            auto ts_begin = parse_range_timestamp(ptree, "from", logger);
            auto ts_end = parse_range_timestamp(ptree, "to", logger);

            if (!ids_included.empty()) {
                next = NodeBuilder::make_filter_by_id_list(ids_included, next, logger);
            }
            if (!ids_excluded.empty()) {
                next = NodeBuilder::make_filter_out_by_id_list(ids_excluded, next, logger);
            }
            if (sampling_params) {
                    next = NodeBuilder::make_sampler(*sampling_params,
                                                     next,
                                                     logger);
            }
            // Build query processor
            return std::make_shared<ScanQueryProcessor>(next, metrics, ts_begin, ts_end);
        }

        if (ids_included.empty() && metrics.empty()) {
            // list all
            for (auto val: table) {
                auto id = val.second;
                ids_included.push_back(id);
            }
        }
        if (!ids_excluded.empty()) {
            std::sort(ids_included.begin(), ids_included.end());
            std::sort(ids_excluded.begin(), ids_excluded.end());
            std::vector<aku_ParamId> tmp;
            std::set_difference(ids_included.begin(), ids_included.end(),
                                ids_excluded.begin(), ids_excluded.end(),
                                std::back_inserter(tmp));
            std::swap(tmp, ids_included);
        }
        return std::make_shared<MetadataQueryProcessor>(ids_included, next);

    } catch(std::exception const& e) {
        (*logger)(AKU_LOG_ERROR, e.what());
        throw QueryParserError(e.what());
    }
}

//                         //
//      Series Parser      //
//                         //

//! Move pointer to the of the whitespace, return this pointer or end on error
static const char* skip_space(const char* p, const char* end) {
    while(p < end && (*p == ' ' || *p == '\t')) {
        p++;
    }
    return p;
}

static const char* copy_until(const char* begin, const char* end, const char pattern, char** out) {
    char* it_out = *out;
    while(begin < end) {
        *it_out = *begin;
        it_out++;
        begin++;
        if (*begin == pattern) {
            break;
        }
    }
    *out = it_out;
    return begin;
}

//! Move pointer to the beginning of the next tag, return this pointer or end on error
static const char* skip_tag(const char* p, const char* end, bool *error) {
    // skip until '='
    while(p < end && *p != '=' && *p != ' ' && *p != '\t') {
        p++;
    }
    if (p == end || *p != '=') {
        *error = true;
        return end;
    }
    // skip until ' '
    const char* c = p;
    while(c < end && *c != ' ') {
        c++;
    }
    *error = c == p;
    return c;
}

int SeriesParser::to_normal_form(const char* begin, const char* end,
                                 char* out_begin, char* out_end,
                                 const char** keystr_begin,
                                 const char** keystr_end)
{
    // Verify args
    if (end < begin) {
        return AKU_EBAD_ARG;
    }
    if (out_end < out_begin) {
        return AKU_EBAD_ARG;
    }
    int series_name_len = end - begin;
    if (series_name_len > AKU_LIMITS_MAX_SNAME) {
        return AKU_EBAD_DATA;
    }
    if (series_name_len > (out_end - out_begin)) {
        return AKU_EBAD_ARG;
    }

    char* it_out = out_begin;
    const char* it = begin;
    // Get metric name
    it = skip_space(it, end);
    it = copy_until(it, end, ' ', &it_out);
    it = skip_space(it, end);

    if (it == end) {
        // At least one tag should be specified
        return AKU_EBAD_DATA;
    }

    *keystr_begin = it_out;

    // Get pointers to the keys
    const char* tags[AKU_LIMITS_MAX_TAGS];
    auto ix_tag = 0u;
    bool error = false;
    while(it < end && ix_tag < AKU_LIMITS_MAX_TAGS) {
        tags[ix_tag] = it;
        it = skip_tag(it, end, &error);
        it = skip_space(it, end);
        if (!error) {
            ix_tag++;
        } else {
            break;
        }
    }
    if (error) {
        // Bad string
        return AKU_EBAD_DATA;
    }
    if (ix_tag == 0) {
        // User should specify at least one tag
        return AKU_EBAD_DATA;
    }

    std::sort(tags, tags + ix_tag, [tags, end](const char* lhs, const char* rhs) {
        // lhs should be always less thenn rhs
        auto lenl = 0u;
        auto lenr = 0u;
        if (lhs < rhs) {
            lenl = rhs - lhs;
            lenr = end - rhs;
        } else {
            lenl = end - lhs;
            lenr = lhs - rhs;
        }
        auto it = 0u;
        while(true) {
            if (it >= lenl || it >= lenr) {
                return it < lenl;
            }
            if (lhs[it] == '=' || rhs[it] == '=') {
                return lhs[it] == '=';
            }
            if (lhs[it] < rhs[it]) {
                return true;
            } else if (lhs[it] > rhs[it]) {
                return false;
            }
            it++;
        }
        return true;
    });

    // Copy tags to output string
    for (auto i = 0u; i < ix_tag; i++) {
        // insert space
        *it_out++ = ' ';
        // insert tag
        const char* tag = tags[i];
        copy_until(tag, end, ' ', &it_out);
    }
    *keystr_begin = skip_space(*keystr_begin, out_end);
    *keystr_end = it_out;
    return AKU_SUCCESS;
}

}
