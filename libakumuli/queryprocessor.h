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
#include <chrono>
#include <memory>

#include "akumuli.h"
#include "queryprocessor_framework.h"
#include "seriesparser.h"
#include "stringpool.h"

#include <boost/property_tree/ptree_fwd.hpp>

namespace Akumuli {
namespace QP {


struct Builder {

    /** Create new query processor.
      * @param query should point to 0-terminated query string
      * @param terminal_node should contain valid pointer to terminal(final) node
      * @param logger should contain valid pointer to logging function
      */
    static std::shared_ptr<QP::IQueryProcessor>
    build_query_processor(const char* query, std::shared_ptr<QP::Node> terminal_node,
                          const SeriesMatcher& matcher, aku_logger_cb_t logger);
};

/** Group-by time statement processor */
struct GroupByTime {
    aku_Timestamp step_;
    bool          first_hit_;
    aku_Timestamp lowerbound_;
    aku_Timestamp upperbound_;

    GroupByTime();

    GroupByTime(aku_Timestamp step);

    GroupByTime(const GroupByTime& other);

    GroupByTime& operator=(const GroupByTime& other);

    bool put(aku_Sample const& sample, Node& next);

    bool empty() const;
};


/** Group-by tag statement processor */
struct GroupByTag {
    std::string regex_;
    //! Mapping from global parameter ids to local parameter ids
    std::unordered_map<aku_ParamId, aku_ParamId> ids_;
    //! Shared string pool
    StringPool const* spool_;
    //! Previous string pool offset
    StringPoolOffset offset_;
    //! Previous string pool size
    size_t prev_size_;
    //! List of tags of interest
    std::vector<std::string> tags_;
    //! Local string pool. All transient series names lives here.
    SeriesMatcher local_matcher_;
    //! List of string already added string pool
    StringTools::SetT snames_;

    //! Main c-tor
    GroupByTag(StringPool const* spool, std::string metric, std::vector<std::string> const& tags);

    void refresh_();

    bool apply(aku_Sample* sample);
};


/** Numeric data query processor. Can be used to return raw data
  * from HDD or derivatives (Depending on the list of processing nodes).
  */
struct ScanQueryProcessor : IQueryProcessor {

    typedef StringTools::StringT StringT;
    typedef StringTools::TableT  TableT;

    //! Query range clause data (upperbound, lowerbound, direction, query type)
    QueryRange range_;
    //! Name of the metrics of interest
    const std::string metric_;
    //! Name to id mapping
    TableT namesofinterest_;
    //! Group-by-time statement
    GroupByTime groupby_;
    //! Filter
    std::shared_ptr<IQueryFilter> filter_;
    //! Root of the processing topology
    std::shared_ptr<Node> root_node_;
    //! Final of the processing topology
    std::shared_ptr<Node> last_node_;
    //! Group-by-tag
    std::unique_ptr<GroupByTag> groupby_tag_;

    /** Create new query processor.
      * @param root is a root of the processing topology
      * @param metric is a name of the metric of interest
      * @param begin is a timestamp to begin from
      * @param end is a timestamp to end with
      *        (depending on a scan direction can be greater or smaller then lo)
      */
    ScanQueryProcessor(std::vector<std::shared_ptr<Node>> nodes, std::string metric,
                       aku_Timestamp begin, aku_Timestamp end, QueryRange::QueryRangeType type,
                       std::shared_ptr<IQueryFilter> filter, GroupByTime groupby,
                       std::unique_ptr<GroupByTag> groupbytag, OrderBy orderby);

    QueryRange range() const;

    IQueryFilter& filter();

    SeriesMatcher* matcher();

    bool start();

    //! Process value
    bool put(const aku_Sample& sample);

    //! Should be called when processing completed
    void stop();

    //! Set execution error
    void set_error(aku_Status error);
};


struct MetadataQueryProcessor : IQueryProcessor {

    std::shared_ptr<IQueryFilter> filter_;
    std::shared_ptr<Node>         root_;

    MetadataQueryProcessor(std::shared_ptr<IQueryFilter> flt, std::shared_ptr<Node> node);

    QueryRange     range() const;
    IQueryFilter&  filter();
    SeriesMatcher* matcher();
    bool           start();
    bool put(const aku_Sample& sample);
    void stop();
    void set_error(aku_Status error);
};
}
}  // namespaces
