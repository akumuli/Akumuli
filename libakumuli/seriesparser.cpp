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

#include <bits/unordered_set.h>

namespace Akumuli {

//                          //
//      Series Matcher      //
//                          //

static const SeriesMatcher::StringT EMPTY = std::make_pair(nullptr, 0);

SeriesMatcher::SeriesMatcher(u64 starting_id)
    : table(StringTools::create_table(0x1000))
    , series_id(starting_id)
{
    if (starting_id == 0u) {
        AKU_PANIC("Bad series ID");
    }
}

u64 SeriesMatcher::add(const char* begin, const char* end) {
    auto id = series_id++;
    StringT pstr = pool.add(begin, end);
    auto tup = std::make_tuple(std::get<0>(pstr), std::get<1>(pstr), id);
    table[pstr] = id;
    inv_table[id] = pstr;
    names.push_back(tup);
    return id;
}

void SeriesMatcher::_add(std::string series, u64 id) {
    if (series.empty()) {
        return;
    }
    const char* begin = &series[0];
    const char* end = begin + series.size();
    StringT pstr = pool.add(begin, end);
    table[pstr] = id;
    inv_table[id] = pstr;
}

void SeriesMatcher::_add(const char*  begin, const char* end, u64 id) {
    StringT pstr = pool.add(begin, end);
    table[pstr] = id;
    inv_table[id] = pstr;
}

u64 SeriesMatcher::match(const char* begin, const char* end) {

    int len = static_cast<int>(end - begin);
    StringT str = std::make_pair(begin, len);

    auto it = table.find(str);
    if (it == table.end()) {
        return 0ul;
    }
    return it->second;
}

SeriesMatcher::StringT SeriesMatcher::id2str(u64 tokenid) const {
    auto it = inv_table.find(tokenid);
    if (it == inv_table.end()) {
        return EMPTY;
    }
    return it->second;
}

void SeriesMatcher::pull_new_names(std::vector<SeriesMatcher::SeriesNameT> *buffer) {
    std::swap(names, *buffer);
}

std::vector<u64> SeriesMatcher::get_all_ids() const {
    std::vector<u64> result;
    for (auto const &tup: inv_table) {
        result.push_back(tup.first);
    }
    return result;
}

std::vector<SeriesMatcher::SeriesNameT> SeriesMatcher::regex_match(const char* rexp) {
    std::vector<SeriesNameT> series;
    StringPoolOffset offset = {};
    size_t size = 0;
    std::vector<StringPool::StringT> res = pool.regex_match(rexp, &offset, &size);
    std::transform(res.begin(), res.end(), std::back_inserter(series), [this](StringT s) {
        auto it = table.find(s);
        if (it == table.end()) {
            // We should always find id by string, otherwise - invariant is
            // broken (due to memory corruption most likely).
            AKU_PANIC("Invalid string-pool.");
        }
        return std::make_tuple(s.first, s.second, it->second);
    });
    return series;
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

static StringTools::StringT get_tag_name(const char* p, const char* end) {
    StringTools::StringT EMPTY = {nullptr, 0};
    auto begin = p;
    while(p < end && *p != '=' && *p != ' ' && *p != '\t') {
        p++;
    }
    if (p == end || *p != '=') {
        return EMPTY;
    }
    return {begin, p - begin};
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

aku_Status SeriesParser::to_normal_form(const char* begin, const char* end,
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

std::tuple<aku_Status, SeriesParser::StringT> SeriesParser::filter_tags(SeriesParser::StringT const& input, const StringTools::SetT &tags, char* out) {
    StringT NO_RESULT = {};
    char* out_begin = out;
    char* it_out = out;
    const char* it = input.first;
    const char* end = it + input.second;

    // Get metric name
    it = skip_space(it, end);
    it = copy_until(it, end, ' ', &it_out);
    it = skip_space(it, end);

    if (it == end) {
        // At least one tag should be specified
        return std::make_tuple(AKU_EBAD_DATA, NO_RESULT);
    }

    // Get pointers to the keys
    const char* last_tag;
    auto ix_tag = 0u;
    bool error = false;
    while(it < end && ix_tag < AKU_LIMITS_MAX_TAGS) {
        last_tag = it;
        it = skip_tag(it, end, &error);
        if (!error) {
            // Check tag
            StringT tag = get_tag_name(last_tag, it);
            if (tags.count(tag) != 0) {
                *it_out = ' ';
                it_out++;
                auto sz = it - last_tag;
                memcpy((void*)it_out, (const void*)last_tag, sz);
                it_out += sz;
                ix_tag++;
            }
        } else {
            break;
        }
        it = skip_space(it, end);
    }

    if (error) {
        // Bad string
        return std::make_tuple(AKU_EBAD_DATA, NO_RESULT);
    }

    if (ix_tag == 0) {
        // User should specify at least one tag
        return std::make_tuple(AKU_EBAD_DATA, NO_RESULT);
    }

    return std::make_tuple(AKU_SUCCESS, std::make_pair(out_begin, it_out - out_begin));
}

}
