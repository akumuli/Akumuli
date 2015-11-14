#pragma once
#include <stdexcept>
#include <memory>

#include "akumuli.h"
#include "seriesparser.h"

#include <boost/property_tree/ptree.hpp>

namespace Akumuli {
namespace QP {

//! Exception triggered by query parser
struct QueryParserError : std::runtime_error {
    QueryParserError(const char* parser_message) : std::runtime_error(parser_message) {}
};

static const aku_Sample NO_DATA = {0u, 0u, {0.0, sizeof(aku_Sample), aku_PData::EMPTY}};

static const aku_Sample SAMPLING_MARGIN = {0u, 0u, {0.0, sizeof(aku_Sample), aku_PData::MARGIN}};

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
        EMPTY = 0,
        GROUP_BY_REQUIRED = 1,
        TERMINAL = 2,
    };

    /** This method returns set of flags that describes its functioning.
      */
    virtual int get_requirements() const = 0;
};


struct NodeException : std::runtime_error {
    NodeException(const char* msg)
        : std::runtime_error(msg)
    {
    }
};


struct IQueryFilter {
    enum FilterResult {
        SKIP_THIS,
        SKIP_ALL,
        PROCESS,
    };
    virtual ~IQueryFilter() = default;
    virtual FilterResult apply(aku_ParamId id) = 0;
    virtual std::vector<aku_ParamId> get_ids() = 0;
};


//! Query filter that doesn't block anything (for testing purposes)
struct BypassFilter : QP::IQueryFilter {
    virtual FilterResult apply(aku_ParamId id) {
        return PROCESS;
    }
    std::vector<aku_ParamId> get_ids() {
        throw std::runtime_error("not implemented");
    }
};


//! Query processor interface
struct IQueryProcessor {

    // Query information
    virtual ~IQueryProcessor() = default;

    //! Lowerbound
    virtual aku_Timestamp lowerbound() const = 0;

    //! Upperbound
    virtual aku_Timestamp upperbound() const = 0;

    //! Scan direction (AKU_CURSOR_DIR_BACKWARD or AKU_CURSOR_DIR_FORWARD)
    virtual int direction() const = 0;

    //! Return query filter
    virtual IQueryFilter& filter() = 0;

    /** Returns series matcher to override global one for query execution.
      * If override is not needed - return nullptr.
      */
    virtual SeriesMatcher* matcher() = 0;

    // Execution control

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
    virtual std::shared_ptr<Node> create(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next) const = 0;
    virtual std::string get_tag() const = 0;
};

//! Register QueryParserToken
void add_queryparsertoken_to_registry(BaseQueryParserToken const* ptr);

//! Create new node using token registry
std::shared_ptr<Node> create_node(std::string tag, boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next);

/** Register new query type
  * NOTE: Each template instantination should be used only once, to create query parser for type.
  */
template<class Target>
struct QueryParserToken : BaseQueryParserToken {
    std::string tag;
    QueryParserToken(const char* tag) : tag(tag) {
        add_queryparsertoken_to_registry(this);
    }
    virtual std::string get_tag() const {
        return tag;
    }
    virtual std::shared_ptr<Node> create(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next) const {
        return std::make_shared<Target>(ptree, next);
    }
};


}}  // namespaces
