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
#include "stringpool.h"
#include "queryprocessor_framework.h"
#include "seriesparser.h"

#include <boost/property_tree/ptree_fwd.hpp>

namespace Akumuli {
namespace QP {


struct Builder {

    /** Create new query processor.
      * @param query should point to 0-terminated query string
      * @param terminal_node should contain valid pointer to terminal(final) node
      * @param logger should contain valid pointer to logging function
      */
    static std::shared_ptr<QP::IQueryProcessor> build_query_processor(const char* query,
                                                                      std::shared_ptr<QP::Node> terminal_node,
                                                                      const SeriesMatcher& matcher,
                                                                      aku_logger_cb_t logger);
};


/** Group-by statement processor */
struct GroupByStatement {
    aku_Timestamp   step_;
    bool            first_hit_;
    aku_Timestamp   lowerbound_;
    aku_Timestamp   upperbound_;

    GroupByStatement();

    GroupByStatement(aku_Timestamp step);

    GroupByStatement(const GroupByStatement& other);

    GroupByStatement& operator = (const GroupByStatement& other);

    bool put(aku_Sample const& sample, Node& next);
};


/** Numeric data query processor. Can be used to return raw data
  * from HDD or derivatives (Depending on the list of processing nodes).
  */
struct ScanQueryProcessor : IQueryProcessor {

    typedef StringTools::StringT StringT;
    typedef StringTools::TableT TableT;

    //! Lowerbound
    const aku_Timestamp                lowerbound_;
    //! Upperbound
    const aku_Timestamp                upperbound_;
    //! Scan direction
    const int                          direction_;
    //! List of metrics of interest
    const std::vector<std::string>     metrics_;
    //! Name to id mapping
    TableT                             namesofinterest_;

    //! Group-by statement
    GroupByStatement                   groupby_;

    //! Root of the processing topology
    std::shared_ptr<Node>              root_node_;

    /** Create new query processor.
      * @param root is a root of the processing topology
      * @param metrics is a list of metrics of interest
      * @param begin is a timestamp to begin from
      * @param end is a timestamp to end with
      *        (depending on a scan direction can be greater or smaller then lo)
      */
    ScanQueryProcessor(std::shared_ptr<Node> root,
                       std::vector<std::string> metrics,
                       aku_Timestamp begin,
                       aku_Timestamp end,
                       GroupByStatement groupby = GroupByStatement());

    //! Lowerbound
    aku_Timestamp lowerbound() const;

    //! Upperbound
    aku_Timestamp upperbound() const;

    //! Scan direction (AKU_CURSOR_DIR_BACKWARD or AKU_CURSOR_DIR_FORWARD)
    int direction() const;

    bool start();

    //! Process value
    bool put(const aku_Sample& sample);

    //! Should be called when processing completed
    void stop();

    //! Set execution error
    void set_error(aku_Status error);
};


struct MetadataQueryProcessor : IQueryProcessor {

    std::vector<aku_ParamId> ids_;
    std::shared_ptr<Node>    root_;

    MetadataQueryProcessor(std::vector<aku_ParamId> ids, std::shared_ptr<Node> node);

    aku_Timestamp lowerbound() const;
    aku_Timestamp upperbound() const;
    int direction() const;
    bool start();
    bool put(const aku_Sample &sample);
    void stop();
    void set_error(aku_Status error);
};

}}  // namespaces
