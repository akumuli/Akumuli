#pragma once
#include "akumuli_def.h"

#include <stdint.h>
#include <map>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <deque>

namespace Akumuli {

struct StringPool {
    typedef std::pair<const char*, int> StringT;
    const int MAX_BIN_SIZE = AKU_LIMITS_MAX_SNAME*0x1000;
    std::deque<std::vector<char>> pool;
    StringT add(const char* begin, const char *end);
};

/** Series matcher. Table that maps series names to series
  * ids. Should be initialized on startup from sqlite table.
  */
struct SeriesMatcher {
    // TODO: add LRU cache

    //! Pooled string
    typedef std::pair<const char*, int> StringT;
    //! Series name descriptor - pointer to string, length, series id.
    typedef std::tuple<const char*, int, uint64_t> SeriesNameT;

    static size_t hash(StringT str);
    static bool equal(StringT lhs, StringT rhs);
    typedef std::unordered_map<StringT, uint64_t,
                               decltype(&SeriesMatcher::hash),
                               decltype(&SeriesMatcher::equal)> TableT;

    // Variables
    StringPool               pool;       //! String pool that stores time-series
    TableT                   table;      //! Series table (name to id mapping)
    uint64_t                 series_id;  //! Series ID counter
    std::vector<SeriesNameT> names;      //! List of recently added names

    SeriesMatcher(uint64_t starting_id);

    /** Add new string to matcher.
      */
    void add(const char* begin, const char* end);

    /** Match string and return it's id. If string is new return 0.
      */
    uint64_t match(const char* begin, const char* end);

    /** Push all new elements to the buffer.
      * @param buffer is an output parameter that will receive new elements
      */
    void pull_new_names(std::vector<SeriesNameT> *buffer);
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

