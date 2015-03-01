#pragma once
#include "akumuli_def.h"

#include <stdint.h>
#include <map>
#include <unordered_map>
#include <vector>


namespace Akumuli {

struct StringPool {
    typedef std::pair<const char*, int> StringT;
    const int MAX_BIN_SIZE = AKU_LIMITS_MAX_SNAME*0x1000;
    std::vector<std::vector<char>> pool;
    StringT add(const char* begin, const char *end);
};

/** Series matcher. Table that maps series names to series
  * ids. Should be initialized on startup from sqlite table.
  */
struct SeriesMatcher {
    // TODO: add LRU cache
    typedef std::pair<const char*, int> StringT;
    static size_t hash(StringT str);
    typedef std::unordered_map<StringT, uint64_t, decltype(&SeriesMatcher::hash)> TableT;

    // Variables
    StringPool pool;
    TableT table;
    uint64_t series_id;

    SeriesMatcher(uint64_t starting_id);

    /** Add new string to matcher.
      */
    void add(const char* begin, const char* end);

    /** Match string and return it's id. If string is new return 0.
      */
    uint64_t match(const char* begin, const char* end);
};

/** Namespace class to store all parsing related things.
  */
struct SeriesParser
{
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

