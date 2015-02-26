#include "seriesparser.h"
#include <string>
#include <map>

namespace Akumuli {


SeriesParser::SeriesParser()
{
}

static const char* skip_space(const char* p) {
    while(*p == ' ' || *p == '\t') {
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

static const char* skip_tag(const char* p) {
    throw "Not implemented";
}

int SeriesParser::to_normal_form(const char* begin, const char* end,
                                 char* out_begin, char* out_end,
                                 const char** keystr_begin)
{
    // Verify args
    if (end < begin) {
        return AKU_EBAD_ARG;
    }
    if (out_end < out_begin) {
        return AKU_EBAD_ARG;
    }
    if ((end - begin) > (out_end - out_begin)) {
        return AKU_EBAD_ARG;
    }

    char* it_out = out_begin;
    const char* it = begin;
    // Get metric name
    it = copy_until(it, end, ' ', &it_out);
    *keystr_begin = it_out;
    it = skip_space(it);

    // Get pointers to the keys
    const char* tags[100];  // TODO: move to config
    int ix_tag = 0;
    while(it < end) {
        tags[ix_tag] = it;
        it = skip_tag(it);
    }

    return AKU_ENOT_IMPLEMENTED;
}

}
