#pragma once
#include "akumuli_def.h"
#include <string>

#include "queryprocessor_framework.h"
#include "seriesparser.h"
#include "stringpool.h"
#include "internal_cursor.h"

namespace Akumuli {
namespace QP {

enum class QueryKind {
    SELECT,     //! Metadata query
    SCAN,       //! Scan query
    AGGREGATE,  //! Aggregation query
};

struct QueryParser {

    static std::tuple<aku_Status, boost::property_tree::ptree> parse_json(const char* query);

    /** Determain type of query.
      */
    static std::tuple<aku_Status, QueryKind> get_query_kind(boost::property_tree::ptree const& ptree);

    /** Parse query and produce reshape request.
      * @param ptree contains query
      * @returns status and ReshapeRequest
      */
    static std::tuple<aku_Status, ReshapeRequest> parse_scan_query(boost::property_tree::ptree const& ptree, SeriesMatcher const& matcher);


    /** Parse stream processing pipeline.
      * @param ptree contains query
      * @returns vector of Nodes in proper order
      */
    static std::tuple<aku_Status, GroupByTime, std::vector<std::shared_ptr<Node>>> parse_processing_topology(
            boost::property_tree::ptree const& ptree,
            Caller& caller,
            InternalCursor* cursor);
};

}}  // namespace
