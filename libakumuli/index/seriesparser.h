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
#include "index/stringpool.h"
#include "index/invertedindex.h"

#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <stdint.h>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace Akumuli {

static const i64 AKU_STARTING_SERIES_ID = 1024;

struct SeriesMatcherBase {

    ~SeriesMatcherBase() = default;

    /** Add new string to matcher.
      */
    virtual i64 add(const char* begin, const char* end) = 0;

    /** Add value to matcher. This function should be
      * used only to load data to matcher. Internal
      * `series_id` counter wouldn't be affected by this call, so
      * it should be set up propertly in constructor.
      */
    virtual void _add(std::string series, i64 id) = 0;

    /** Add value to matcher. This function should be
      * used only to load data to matcher. Internal
      * `series_id` counter wouldn't be affected by this call, so
      * it should be set up propertly in constructor.
      */
    virtual void _add(const char* begin, const char* end, i64 id) = 0;

    /**
      * Match string and return it's id. If string is new return 0.
      */
    virtual i64 match(const char* begin, const char* end) const = 0;

    /**
      * Convert id to string
      */
    virtual StringT id2str(i64 tokenid) const = 0;
};

/** Series index. Can be used to retreive series names and ids by tags.
  * Implements inverted index with compression and other optimizations.
  * It's more efficient than PlainSeriesMatcher but it's costly to have
  * many instances in one application.
  */
struct SeriesMatcher : SeriesMatcherBase {
    //! Series name descriptor - pointer to string, length, series id.
    typedef std::tuple<const char*, int, i64> SeriesNameT;

    typedef StringTools::TableT TableT;
    typedef StringTools::InvT   InvT;

    Index                    index;      //! Series name index and storage
    TableT                   table;      //! Series table (name to id mapping)
    InvT                     inv_table;  //! Ids table (id to name mapping)
    i64                      series_id;  //! Series ID counter, positive values
                                         //! are resurved for metrics, negative are for events
    std::vector<SeriesNameT> names;      //! List of recently added names
    mutable std::mutex       mutex;      //! Mutex for shared data

    SeriesMatcher(i64 starting_id=AKU_STARTING_SERIES_ID);

    /** Add new string to matcher.
      */
    i64 add(const char* begin, const char* end);

    /** Add value to matcher. This function should be
      * used only to load data to matcher. Internal
      * `series_id` counter wouldn't be affected by this call, so
      * it should be set up propertly in constructor.
      */
    void _add(std::string series, i64 id);

    /** Add value to matcher. This function should be
      * used only to load data to matcher. Internal
      * `series_id` counter wouldn't be affected by this call, so
      * it should be set up propertly in constructor.
      */
    void _add(const char* begin, const char* end, i64 id);

    /**
      * Match string and return it's id. If string is new return 0.
      */
    i64 match(const char* begin, const char* end) const;

    /**
      * Convert id to string
      */
    StringT id2str(i64 tokenid) const;

    /** Push all new elements to the buffer.
      * @param buffer is an output parameter that will receive new elements
      */
    void pull_new_names(std::vector<SeriesNameT>* buffer);

    std::vector<i64> get_all_ids() const;

    std::vector<SeriesNameT> search(IndexQueryNodeBase const& query) const;

    std::vector<StringT> suggest_metric(std::string prefix) const;

    std::vector<StringT> suggest_tags(std::string metric, std::string tag_prefix) const;

    std::vector<StringT> suggest_tag_values(std::string metric, std::string tag, std::string value_prefix) const;

    size_t memory_use() const;

    size_t index_memory_use() const;

    size_t pool_memory_use() const;

};


/** Series matcher. Table that maps series names to series
  * ids.
  * Implements simple forward index. Can be used to map ids
  * to names and names to ids. Can search index using regular
  * expressions.
  */
struct PlainSeriesMatcher : SeriesMatcherBase {
    //! Series name descriptor - pointer to string, length, series id.
    typedef std::tuple<const char*, int, i64> SeriesNameT;

    typedef StringTools::TableT TableT;
    typedef StringTools::InvT   InvT;

    // Variables
    LegacyStringPool         pool;       //! String pool that stores time-series
    TableT                   table;      //! Series table (name to id mapping)
    InvT                     inv_table;  //! Ids table (id to name mapping)
    i64                      series_id;  //! Series ID counter
    std::vector<SeriesNameT> names;      //! List of recently added names
    mutable std::mutex       mutex;      //! Mutex for shared data

    PlainSeriesMatcher(i64 starting_id=AKU_STARTING_SERIES_ID);

    /** Add new string to matcher.
      */
    i64 add(const char* begin, const char* end);

    /** Add value to matcher. This function should be
      * used only to load data to matcher. Internal
      * `series_id` counter wouldn't be affected by this call, so
      * it should be set up propertly in constructor.
      */
    void _add(std::string series, i64 id);

    /** Add value to matcher. This function should be
      * used only to load data to matcher. Internal
      * `series_id` counter wouldn't be affected by this call, so
      * it should be set up propertly in constructor.
      */
    void _add(const char*  begin, const char* end, i64 id);

    /** Match string and return it's id. If string is new return 0.
      */
    i64 match(const char* begin, const char* end) const;

    //! Convert id to string
    StringT id2str(i64 tokenid) const;

    /** Push all new elements to the buffer.
      * @param buffer is an output parameter that will receive new elements
      */
    void pull_new_names(std::vector<SeriesNameT>* buffer);

    std::vector<i64> get_all_ids() const;

    std::vector<SeriesNameT> regex_match(const char* rexp) const;

    std::vector<SeriesNameT> regex_match(const char* rexp, StringPoolOffset* offset, size_t* prevsize) const;
};

/** Namespace class to store all parsing related things.
  */
struct SeriesParser {
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
    static aku_Status to_canonical_form(const char* begin, const char* end, char* out_begin,
                                     char* out_end, const char** keystr_begin,
                                     const char** keystr_end);

    typedef StringTools::StringT StringT;

    /** Remove redundant tags from input string. Leave only metric and tags from the list.
      * If 'inv' is set leave tags that are not in the set.
      */
    static std::tuple<aku_Status, StringT> filter_tags(StringT const& input,
                                                       StringTools::SetT const& tags,
                                                       char* out,
                                                       bool inv=false);
};


/** Group-by processor. Maps set of global series names to
  * some other set of local series ids.
  */
struct LegacyGroupByTag {
    std::string regex_;
    //! Mapping from global parameter ids to local parameter ids
    std::unordered_map<aku_ParamId, aku_ParamId> ids_;
    //! Shared series matcher
    PlainSeriesMatcher const& matcher_;
    //! Previous string pool offset
    StringPoolOffset offset_;
    //! Previous string pool size
    size_t prev_size_;
    //! List of tags of interest
    std::vector<std::string> tags_;
    //! Local string pool. All transient series names lives here.
    PlainSeriesMatcher local_matcher_;
    //! List of string already added string pool
    StringTools::SetT snames_;

    //! Main c-tor
    LegacyGroupByTag(const PlainSeriesMatcher &matcher, std::string metric, std::vector<std::string> const& tags);

    void refresh_();

    bool apply(aku_Sample* sample);

    std::unordered_map<aku_ParamId, aku_ParamId> get_mapping() const;
};


struct TagRenamer {
    virtual ~TagRenamer() = default;
    virtual PlainSeriesMatcher& get_series_matcher() = 0;
    virtual std::unordered_map<aku_ParamId, aku_ParamId> get_mapping() const = 0;
};


enum class GroupByOpType {
    PIVOT,
    GROUP,
};

/** Group-by processor. Maps set of global series names to
  * some other set of local series ids.
  */
struct GroupByTag {
    //! Mapping from global parameter ids to local parameter ids
    std::unordered_map<aku_ParamId, aku_ParamId> ids_;
    //! Shared series matcher
    SeriesMatcher const& matcher_;
    //! Previous string pool offset
    StringPoolOffset offset_;
    //! Previous string pool size
    size_t prev_size_;
    //! Metric names
    std::vector<std::string> metrics_;
    //! List of function names (for aggregate queries)
    std::vector<std::string> funcs_;
    //! List of tags of interest
    std::vector<std::string> tags_;
    //! Local string pool. All transient series names lives here.
    PlainSeriesMatcher local_matcher_;
    //! List of string already added string pool
    StringTools::SetT snames_;
    GroupByOpType type_;

    //! Main c-tor
    GroupByTag(const SeriesMatcher &matcher, std::string metric, std::vector<std::string> const& tags, GroupByOpType op);
    GroupByTag(const SeriesMatcher &matcher,
               const std::vector<std::string>& metrics,
               const std::vector<std::string>& func_names,
               std::vector<std::string> const& tags,
               GroupByOpType op);

    void refresh_();

    PlainSeriesMatcher& get_series_matcher();
    std::unordered_map<aku_ParamId, aku_ParamId> get_mapping() const;
};

}
