#pragma once
#include <stdint.h>
#include "akumuli_def.h"
#include <tuple>

namespace Akumuli {

struct SeriesParser
{
public:
    SeriesParser();

    /** Convert input string to normal form.
      * In normal form metric name is followed by the list of key
      * value pairs in alphabetical order. All keys should be unique and
      * separated from metric name and from each other by exactly one space.
      * @param begin points to the begining of the input string
      * @param end points to the to the end of the string
      * @param out_begin points to the begining of the output buffer (should be not less then input buffer)
      * @param out_end points to the end of the output buffer
      * @param keystr_begin points to the begining of the key string (string with key-value pairs)
      * @return AKU_SUCCESS if everything is OK, error code otherwise
      */
    static int to_normal_form(const char* begin, const char* end,
                              char* out_begin, char* out_end,
                              const char** keystr_begin);
};

}

