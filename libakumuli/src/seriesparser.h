#pragma once
#include <stdint.h>
#include <tuple>

namespace Akumuli {

struct SeriesParser
{
public:
    SeriesParser();

    /** Parse series key-string and return series Id and hash.
      * @param begin points to the begining of the string
      * @param end points to the next character afther the last string's character
      * @return param-id and hash
      */
    std::tuple<uint64_t, uint32_t> parse(const char* begin, const char* end);
};

}

