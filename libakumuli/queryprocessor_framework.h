#pragma once
#include <memory>
#include <stdexcept>

#include "akumuli.h"
#include "util.h"
#include "seriesparser.h"

#include <boost/property_tree/ptree.hpp>


namespace Akumuli {
namespace QP {

/* ColumnStore + reshape functionality
 * selct cpu where host=XXXX group by tag order by time from 0 to 100;
 * TS  Series name Value
 *  0  cpu tag=Foo    10
 *  0  cpu tag=Bar    20
 *  1  cpu tag=Foo    10
 *  2  cpu tag=Foo    12
 *  2  cpu tag=Bar    30
 *  ...
 *
 * selct cpu where host=XXXX group by tag order by series from 0 to 100;
 * TS  Series name Value
 *  0  cpu tag=Foo    21
 *  1  cpu tag=Foo    20
 * ...
 * 99  cpu tag=Foo    19
 *  0  cpu tag=Bar    20
 *  1  cpu tag=Bar    11
 * ...
 * 99  cpu tag=Bar    14
 *  ...
 *
 * It is possible to add processing steps via IQueryProcessor.
 */

enum class AggregationFunction {
    MIN, MAX, SUM, CNT,
};

struct Aggregation {
    bool enabled;
    AggregationFunction func;

    static std::tuple<aku_Status, AggregationFunction> from_string(std::string str) {
        if (str == "min") {
            return std::make_tuple(AKU_SUCCESS, AggregationFunction::MIN);
        } else if (str == "max") {
            return std::make_tuple(AKU_SUCCESS, AggregationFunction::MAX);
        } else if (str == "sum") {
            return std::make_tuple(AKU_SUCCESS, AggregationFunction::SUM);
        } else if (str == "count") {
            return std::make_tuple(AKU_SUCCESS, AggregationFunction::CNT);
        }
        return std::make_tuple(AKU_EBAD_ARG, AggregationFunction::CNT);
    }
};

struct Column {
    std::vector<aku_ParamId> ids;
};

//! Set of ids returned by the query (defined by select and where clauses)
struct Selection {
    //! Set of columns returned by the query (1 columns - select statement, N columns - join statement)
    std::vector<Column> columns;
    aku_Timestamp begin;
    aku_Timestamp end;
};

//! Mapping from persistent series names to transient series names
struct GroupBy {
    bool enabled;
    std::unordered_map<aku_ParamId, aku_ParamId> transient_map;
    std::shared_ptr<SeriesMatcher> matcher;
};

//! Output order
enum class OrderBy {
    SERIES,
    TIME,
};

//! Reshape request defines what should be sent to query processor
struct ReshapeRequest {
    Aggregation  agg;
    Selection select;
    GroupBy group_by;
    OrderBy order_by;
};


//! Exception triggered by query parser
struct QueryParserError : std::runtime_error {
    QueryParserError(const char* parser_message)
        : std::runtime_error(parser_message) {}
};

static const aku_Sample NO_DATA = { 0u, 0u, { 0.0, sizeof(aku_Sample), aku_PData::EMPTY } };

static const aku_Sample SAMPLING_LO_MARGIN = { 0u,
                                               0u,
                                               { 0.0, sizeof(aku_Sample), aku_PData::LO_MARGIN } };

static const aku_Sample SAMPLING_HI_MARGIN = { 0u,
                                               0u,
                                               { 0.0, sizeof(aku_Sample), aku_PData::HI_MARGIN } };

struct Node {

    virtual ~Node() = default;

    //! Complete adding values
    virtual void complete() = 0;

    /** Process value, return false to interrupt process.
      * Empty sample can be sent to flush all updates.
      */
    virtual bool put(aku_Sample const& sample) = 0;

    virtual void set_error(aku_Status status) = 0;

    // Query validation

    enum QueryFlags {
        EMPTY             = 0,
        GROUP_BY_REQUIRED = 1,
        TERMINAL          = 2,
    };

    /** This method returns set of flags that describes its functioning.
      */
    virtual int get_requirements() const = 0;
};


struct NodeException : std::runtime_error {
    NodeException(const char* msg)
        : std::runtime_error(msg) {}
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

//! Stream processor interface
struct IStreamProcessor {

    virtual ~IStreamProcessor() = default;

    /** Will be called before query execution starts.
      * If result already obtained - return False.
      * In this case `stop` method shouldn't be called
      * at the end.
      */
    virtual bool start() = 0;

    //! Get new value
    virtual bool put(const aku_Sample& sample) = 0;

    //! Will be called when processing completed without errors
    virtual void stop() = 0;

    //! Will be called on error
    virtual void set_error(aku_Status error) = 0;
};


struct BaseQueryParserToken {
    virtual std::shared_ptr<Node> create(boost::property_tree::ptree const& ptree,
                                         std::shared_ptr<Node>              next) const = 0;
    virtual std::string get_tag() const                                                 = 0;
};

//! Register QueryParserToken
void add_queryparsertoken_to_registry(BaseQueryParserToken const* ptr);

//! Create new node using token registry
std::shared_ptr<Node> create_node(std::string tag, boost::property_tree::ptree const& ptree,
                                  std::shared_ptr<Node> next);

/** Register new query type
  * NOTE: Each template instantination should be used only once, to create query parser for type.
  */
template <class Target> struct QueryParserToken : BaseQueryParserToken {
    std::string tag;
    QueryParserToken(const char* tag)
        : tag(tag) {
        add_queryparsertoken_to_registry(this);
    }
    virtual std::string           get_tag() const { return tag; }
    virtual std::shared_ptr<Node> create(boost::property_tree::ptree const& ptree,
                                         std::shared_ptr<Node>              next) const {
        return std::make_shared<Target>(ptree, next);
    }
};
}
}  // namespaces
