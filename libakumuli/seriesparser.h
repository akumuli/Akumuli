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

#pragma once
#include "akumuli_def.h"
#include "queryprocessor.h"
#include "stringpool.h"

#include <stdint.h>
#include <map>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <deque>
#include <memory>
#include <mutex>

namespace Akumuli {

//! Exception triggered by query parser
struct QueryParserError;


/** Series matcher. Table that maps series names to series
  * ids. Should be initialized on startup from sqlite table.
  */
struct SeriesMatcher {
    // TODO: add LRU cache
    //! Pooled string
    typedef StringTools::StringT StringT;
    //! Series name descriptor - pointer to string, length, series id.
    typedef std::tuple<const char*, int, uint64_t> SeriesNameT;

    typedef StringTools::TableT TableT;
    typedef StringTools::InvT   InvT;

    // Variables
    StringPool               pool;       //! String pool that stores time-series
    TableT                   table;      //! Series table (name to id mapping)
    InvT                     inv_table;  //! Ids table (id to name mapping)
    uint64_t                 series_id;  //! Series ID counter
    std::vector<SeriesNameT> names;      //! List of recently added names
    std::mutex               mutex;      //! Mutex for shared data

    SeriesMatcher(uint64_t starting_id);

    /** Add new string to matcher.
      */
    uint64_t add(const char* begin, const char* end);

    /** Add value from DB to matcher. This function should be
      * used only to load data from database to matcher. Internal
      * `series_id` counter shouldn't be affected by this call.
      */
    void _add(std::string series, uint64_t id);

    /** Match string and return it's id. If string is new return 0.
      */
    uint64_t match(const char* begin, const char* end);

    //! Convert id to string
    StringT id2str(uint64_t tokenid);

    /** Push all new elements to the buffer.
      * @param buffer is an output parameter that will receive new elements
      */
    void pull_new_names(std::vector<SeriesNameT> *buffer);

    /** Create new query processor.
      * @param query should point to 0-terminated query string
      * @param terminal_node should contain valid pointer to terminal(final) node
      * @param logger should contain valid pointer to logging function
      */
    std::shared_ptr<QP::QueryProcessor> build_query_processor(const char* query,
                                                              std::shared_ptr<QP::Node> terminal_node,
                                                              aku_logger_cb_t logger);
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
                              const char** keystr_begin, const char **keystr_end);
};

}

