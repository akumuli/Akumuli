#include "seriesparser.h"
#include "util.h"
#include <string>
#include <map>
#include <algorithm>

namespace Akumuli {

//                       //
//      String Pool      //
//                       //

StringPool::StringT StringPool::add(const char* begin, const char* end) {
    if (pool.empty()) {
        pool.emplace_back();
        pool.back().reserve(MAX_BIN_SIZE);
    }
    int size = end - begin;
    if (size == 0) {
        return std::make_pair("", 0);
    }
    size++;
    std::vector<char>* bin = &pool.back();
    if (static_cast<int>(bin->size()) + size > MAX_BIN_SIZE) {
        // New bin
        pool.emplace_back();
        bin = &pool.back();
        bin->reserve(MAX_BIN_SIZE);
    }
    for(auto i = begin; i < end; i++) {
        bin->push_back(*i);
    }
    bin->push_back('\0');
    const char* p = &bin->back();
    p -= size - 1;
    return std::make_pair(p, size);
}


//                          //
//      Series Matcher      //
//                          //

size_t SeriesMatcher::hash(SeriesMatcher::StringT str) {
    // implementation of Dan Bernstein's djb2
    const char* begin = str.first;
    int len = str.second;
    const char* end = begin + len;
    size_t hash = 5381;
    int c;
    while (begin < end) {
        c = *begin++;
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    return hash;
}

bool SeriesMatcher::equal(StringT lhs, StringT rhs) {
    if (lhs.second != rhs.second) {
        return false;
    }
    return std::equal(lhs.first, lhs.first + lhs.second, rhs.first);
}

SeriesMatcher::SeriesMatcher(uint64_t starting_id)
    : table(0x1000, &SeriesMatcher::hash, &SeriesMatcher::equal)
    , series_id(starting_id)
{
    if (starting_id == 0u) {
        AKU_PANIC("Bad series ID");
    }
}

uint64_t SeriesMatcher::add(const char* begin, const char* end) {
    StringT pstr = pool.add(begin, end);
    auto id = series_id++;
    table[pstr] = id;
    auto tup = std::make_tuple(std::get<0>(pstr), std::get<1>(pstr), id);
    names.push_back(tup);
    return id;
}

void SeriesMatcher::_add(std::string series, uint64_t id) {
    if (series.empty()) {
        return;
    }
    const char* begin = &series[0];
    const char* end = begin + series.size();
    StringT pstr = pool.add(begin, end);
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
