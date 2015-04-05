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

void SeriesMatcher::pull_new_names(std::vector<SeriesMatcher::SeriesNameT> *buffer) {
    std::swap(names, *buffer);
}

struct QueryParserError : std::runtime_error {
    QueryParserError(const char* parser_message) : std::runtime_error(parser_message) {}
};

static std::pair<std::string, size_t> parse_sampling_params(boost::property_tree::ptree const& ptree,
                                                            aku_logger_cb_t logger) {
    auto sample = ptree.get_child("sample");
    if (sample.empty()) {
        (*logger)(AKU_LOG_ERROR, "No `sample` tag");
        auto rte = std::runtime_error("`sample` expected");
        BOOST_THROW_EXCEPTION(rte);
    }
    std::string sample_type;
    size_t sampling_buffer_size;
    for (auto child: sample) {
        sample_type = child.first;
        sampling_buffer_size = child.second.get_value<size_t>();
        break;
    }
    return std::make_pair(sample_type, sampling_buffer_size);
}

static std::vector<std::string> parse_metric(boost::property_tree::ptree const& ptree,
                                             aku_logger_cb_t logger) {
    std::vector<std::string> metrics;
    auto metric = ptree.get_child("metric");
    auto single = metric.get_value<std::string>();
    if (single.empty()) {
        for(auto child: metric) {
            auto metric_name = child.second.get_value<std::string>();
            metrics.push_back(metric_name);
        }
    } else {
        metrics.push_back(single);
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

static std::vector<aku_ParamId> parse_where_clause(boost::property_tree::ptree const& ptree,
                                                   std::string metric,
                                                   std::string pred,
                                                   StringPool const& pool,
                                                   aku_logger_cb_t logger)
{
    std::vector<aku_ParamId> ids;
    auto where = ptree.get_child("where");
    for (auto child: where) {
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
                    series_regexp << "(" << metric << "\\s(\\w+=\\w+)*" << tag << "=" << value << "(\\s\\w+=\\w+)*)";
                    auto results = pool.regex_match(series_regexp.str().c_str());
                    // TODO: Extract all series IDs
                }
            }
        }
    }
    return ids;
}

std::shared_ptr<QP::QueryProcessor>
SeriesMatcher::build_query_processor(const char* query, aku_logger_cb_t logger) {
    static const std::shared_ptr<QP::QueryProcessor> NONE;
    /* Query format:
     * {
     *      "sample": "all", // { "step": "5sec" } or { "random": 1000 }
     *      "metric": "cpu",
     *      // or
     *      "metric": ["cpu", "mem"],
     *      "range": {
     *          "from": "20150101T000000",
     *          "to"  : "20150102T000000"
     *      },
     *      "where": [
     *          { "equals": { "key1": "val1" }},
     *          { "not_equals" : { "key2": "val2" }},
     *          { "in" : { "key3": [1, 2, 3, "foo"]}
     *      ],
     *      "group_by": [
     *          "key" : [ "key1", "key2" ]
     *      ]
     * }
     */
    namespace pt = boost::property_tree;
    using namespace QP;

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
        return NONE;
    }

    try {
        // Read metric(s) name
        auto metrics = parse_metric(ptree, logger);

        // Read sampling method
        auto sampling_params = parse_sampling_params(ptree, logger);

        // Read timestamps
        auto ts_begin = parse_range_timestamp(ptree, "from", logger);
        auto ts_end = parse_range_timestamp(ptree, "to", logger);

        // Read where clause
        std::vector<aku_ParamId> ids;
        for(auto metric: metrics) {
            auto tmp = parse_where_clause(ptree, "cpu", "in", pool, logger);
            std::copy(tmp.begin(), tmp.end(), std::back_inserter(ids));
        }

        // Build topology
        auto sampler = NodeBuilder::make_random_sampler(sampling_params.first,
                                                        sampling_params.second,
                                                        std::shared_ptr<Node>(), // TODO: Create nodes correct
                                                        logger);                 // order, pass correct value
        // Build query processor
        auto qproc = std::make_shared<QueryProcessor>(sampler, metrics, ts_begin, ts_end);
        return qproc;

    } catch(std::exception const& e) {
        (*logger)(AKU_LOG_ERROR, e.what());
        return NONE;
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
