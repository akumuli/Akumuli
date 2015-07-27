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
#include "queryprocessor_fwd.h"
#include "seriesparser.h"

#include <boost/property_tree/ptree_fwd.hpp>

namespace Akumuli {
namespace QP {

struct NodeException : std::runtime_error {
    Node::NodeType type_;
    NodeException(Node::NodeType type, const char* msg);
    Node::NodeType get_type() const;
};


struct NodeBuilder {
    //! Create random sampling node
    static std::shared_ptr<Node> make_sampler(const boost::property_tree::ptree &ptree,
                                                     std::shared_ptr<Node> next,
                                                     aku_logger_cb_t logger);

    //! Create filtering node
    static std::shared_ptr<Node> make_filter_by_id(aku_ParamId id, std::shared_ptr<Node> next,
                                                   aku_logger_cb_t logger);

    //! Create filtering node
    static std::shared_ptr<Node> make_filter_by_id_list(std::vector<aku_ParamId> ids, std::shared_ptr<Node> next,
                                                        aku_logger_cb_t logger);

    //! Create filtering node
    static std::shared_ptr<Node> make_filter_out_by_id_list(std::vector<aku_ParamId> ids, std::shared_ptr<Node> next,
                                                            aku_logger_cb_t logger);

    //! Create moving average
    static std::shared_ptr<Node> make_moving_average(std::shared_ptr<Node> next,
                                                     aku_Timestamp step,
                                                     aku_logger_cb_t logger);
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
                   aku_Timestamp end);

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
