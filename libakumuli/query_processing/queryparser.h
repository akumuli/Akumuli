#pragma once
#include "akumuli_def.h"
#include <string>

#include "queryprocessor_framework.h"
#include "seriesparser.h"
#include "stringpool.h"

namespace Akumuli {
namespace QP {

enum class QueryKind {
    SELECT,     //! Metadata query
    SCAN,       //! Scan query
    AGGREGATE,  //! Aggregation query
};

struct QueryParser {

    /** Determain type of query.
      */
    static QueryKind get_query_kind(boost::property_tree::ptree const& ptree);

    /** Parse query and produce reshape request.
      * @param ptree contains query
      * @returns status and ReshapeRequest
      */
    static std::tuple<aku_Status, ReshapeRequest> parse_scan_query(boost::property_tree::ptree const& ptree, SeriesMatcher const& matcher);


    /** Parse stream processing pipeline.
      * @param ptree contains query
      * @returns vector of Nodes in proper order
      */
    static std::vector<Node> parse_processing_topology(boost::property_tree::ptree const& ptree);
};

}}  // namespace
