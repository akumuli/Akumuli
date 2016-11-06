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

class SeriesRetreiver {
    boost::optional<std::string> metric_;
    std::map<std::string, std::vector<std::string>> tags_;
public:
    //! Matches all series names
    SeriesRetreiver();

    //! Matches all series from one metric
    SeriesRetreiver(std::string metric);

    //! Add tag-name and tag-value pair
    aku_Status add_tag(std::string name, std::string value);

    //! Add tag name and set of possible values
    aku_Status add_tags(std::string name, std::vector<std::string> values);

    //! Get results
    std::tuple<aku_Status, std::vector<aku_ParamId>> extract_ids(SeriesMatcher const& matcher) const;
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

    /** Parse select query (metadata query).
      * @param ptree is a property tree generated from query json
      * @param matcher is a global matcher
      */
    static std::tuple<aku_Status, std::vector<aku_ParamId>> parse_select_query(boost::property_tree::ptree const& ptree, SeriesMatcher const& matcher);

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
