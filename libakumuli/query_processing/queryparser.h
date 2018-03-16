#pragma once
#include "akumuli_def.h"
#include <string>

#include "queryprocessor_framework.h"
#include "index/seriesparser.h"
#include "index/stringpool.h"
#include "internal_cursor.h"

namespace Akumuli {
namespace QP {

enum class QueryKind {
    SELECT,
    SELECT_META,
    JOIN,
    AGGREGATE,
    GROUP_AGGREGATE,
};

class SeriesRetreiver {
    std::vector<std::string> metric_;
    std::map<std::string, std::vector<std::string>> tags_;
    std::vector<std::string> series_;
public:
    //! Matches all series names
    SeriesRetreiver();

    //! Matches all series from one metric
    SeriesRetreiver(std::vector<std::string> const& metric);

    //! Add tag-name and tag-value pair
    aku_Status add_tag(std::string name, std::string value);

    //! Add tag name and set of possible values
    aku_Status add_tags(std::string name, std::vector<std::string> values);

    //! Add full series name
    aku_Status add_series_name(std::string name);

    //! Get results
    std::tuple<aku_Status, std::vector<aku_ParamId>> extract_ids(PlainSeriesMatcher const& matcher) const;

    std::tuple<aku_Status, std::vector<aku_ParamId>> extract_ids(SeriesMatcher const& matcher) const;

    std::tuple<aku_Status, std::vector<aku_ParamId>> fuzzy_match(PlainSeriesMatcher const& matcher) const;
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
    static std::tuple<aku_Status, ReshapeRequest> parse_select_query(boost::property_tree::ptree const& ptree, SeriesMatcher const& matcher);

    /** Parse select query (metadata query).
      * @param ptree is a property tree generated from query json
      * @param matcher is a global matcher
      */
    static std::tuple<aku_Status, std::vector<aku_ParamId>> parse_select_meta_query(boost::property_tree::ptree const& ptree, SeriesMatcher const& matcher);

    /** Parse search query.
      * @param ptree is a property tree generated from query json
      * @param matcher is a global matcher
      */
    static std::tuple<aku_Status, std::vector<aku_ParamId> > parse_search_query(boost::property_tree::ptree const& ptree, SeriesMatcher const& matcher);

    /**
     * @brief Parse suggest query
     * @param ptree is a property tree generated from query json
     * @param matcher is a series matcher object
     */
    static std::tuple<aku_Status, std::shared_ptr<PlainSeriesMatcher>, std::vector<aku_ParamId>>
        parse_suggest_query(boost::property_tree::ptree const& ptree, SeriesMatcher const& matcher);

    /** Parse aggregate query and produce reshape request.
     */
    static std::tuple<aku_Status, ReshapeRequest> parse_aggregate_query(
            boost::property_tree::ptree const& ptree,
            SeriesMatcher const& matcher);

    /** Parse join query and create `reshape` request for column-store.
     */
    static std::tuple<aku_Status, ReshapeRequest> parse_join_query(
            boost::property_tree::ptree const& ptree,
            SeriesMatcher const& matcher);

    /**
     * Parse group-aggregate query
     * @param ptree is a json query
     * @param matcher is a series matcher
     * @return status and request object
     */
    static std::tuple<aku_Status, ReshapeRequest> parse_group_aggregate_query(boost::property_tree::ptree const& ptree,
                                                                              SeriesMatcher const& matcher);

    /** Parse stream processing pipeline.
      * @param ptree contains query
      * @returns vector of Nodes in proper order
      */
    static std::tuple<aku_Status, std::vector<std::shared_ptr<Node>>> parse_processing_topology(
            boost::property_tree::ptree const& ptree,
            InternalCursor* cursor);
};

}}  // namespace
