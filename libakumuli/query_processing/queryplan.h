#pragma once

#include <memory>
#include <vector>

#include "seriesparser.h"
#include "queryprocessor_framework.h"

namespace Akumuli {
namespace QP {

/*
 * Each query plan can contain up to three tiers:
 * - tier1 - storage level operators;
 * - tier2 - materialization step;
 * - tier3 - operators that works on materialized data;
 * Each tier of the query plan can contain several stages. Each
 * stage should contain all necesarry information to execute
 * a query.
 *
 */


enum class Tier1Operator {
    SCAN_RANGE,
    AGGREGATE_RANGE,
    GROUP_AGGREGATE_RANGE,
};

enum class Tier2Operator {
    CHAIN_SERIES,
    MERGE_TIME_ORDER,
    MERGE_SERIES_ORDER,
    AGGREGATE,
    AGGREGATE_COMBINE,
    MERGE_JOIN_SERIES_ORDER,
    MERGE_JOIN_TIME_ORDER,
    SERIES_ORDER_AGGREGATE_MATERIALIZER,
    TIME_ORDER_AGGREGATE_MATERIALIZER,
};

enum class Tier3Operator {
};

/**
 * @brief Stage of the query plan
 */
struct QueryPlanStage {
    typedef std::vector<aku_ParamId> IdListT;
    //! Query tier
    int tier_;
    union {
        Tier1Operator tier1;
        Tier2Operator tier2;
        Tier3Operator tier3;
    } op_;
    //! List of ids retreived by the query (optional)
    IdListT opt_ids_;
    //! Series matcher that can be used by materialization step (optional)
    std::shared_ptr<SeriesMatcher> opt_matcher_;
    //! Time-range covered by the query
    std::pair<aku_Timestamp, aku_Timestamp> time_range_;
    //! List of functions to calculate
    std::vector<QP::AggregationFunction> opt_func_;
    //! Optional JOIN cardinality
    int opt_join_cardinality_;
    //! Step for group-aggregate query
    aku_Timestamp opt_step_;
};

/**
 * @brief Object of this class represents a query plan.
 * Query execution process can take the following steps:
 * - Tier 1 extraction operators (this is a simple iterators or aggregators).
 * - Tier 1 operators (merge or concat).
 * - Tier 2 materialization step (can perform groupping or aggregation).
 * - Tier 2 operators on materialized data (joins).
 */
struct QueryPlan {
    std::vector<std::unique_ptr<QueryPlanStage>> stages_;

    QueryPlan(ReshapeRequest const& req);
};

}}  // namespaces
