#include "queryplan.h"

namespace Akumuli {
namespace QP {

typedef std::vector<std::unique_ptr<QueryPlanStage>> StagesT;

static StagesT create_scan(ReshapeRequest const& req) {
    // Hardwired query plan for scan query
    // Tier1
    // - List of range scan operators
    // Tier2
    // - If group-by is enabled:
    //   - Transform ids and matcher (generate new names)
    //   - Add merge materialization step (series or time order, depending on the
    //     order-by clause.
    // - Otherwise
    //   - If oreder-by is series add chain materialization step.
    //   - Otherwise add merge materializer.

    StagesT result;

    if (req.agg.enabled || req.select.columns.size() != 1) {
        AKU_PANIC("Invalid request");
    }

    std::unique_ptr<QueryPlanStage> t1stage;
    t1stage.reset(new QueryPlanStage());

    aku_Timestamp begin   = req.select.begin;
    aku_Timestamp end     = req.select.end;
    const auto &tier1ids  = req.select.columns.at(0).ids;
    t1stage->op_.tier1    = Tier1Operator::SCAN_RANGE;
    t1stage->tier_        = 1;
    t1stage->opt_ids_     = tier1ids;
    t1stage->opt_matcher_ = req.select.matcher;
    t1stage->time_range_  = std::make_pair(begin, end);

    result.push_back(std::move(t1stage));

    if (req.group_by.enabled) {
        std::vector<aku_ParamId> ids;
        for(auto id: req.select.columns.at(0).ids) {
            auto it = req.group_by.transient_map.find(id);
            if (it != req.group_by.transient_map.end()) {
                ids.push_back(it->second);
            }
        }
        std::unique_ptr<QueryPlanStage> t2stage;
        t2stage.reset(new QueryPlanStage());
        Tier2Operator op      = req.order_by == OrderBy::SERIES
                              ? Tier2Operator::MERGE_SERIES_ORDER
                              : Tier2Operator::MERGE_TIME_ORDER;
        t2stage->op_.tier2    = op;
        t2stage->tier_        = 2;
        t2stage->opt_matcher_ = req.group_by.matcher;
        t2stage->opt_ids_     = ids;
        t2stage->time_range_  = std::make_pair(begin, end);

        result.push_back(std::move(t2stage));
    } else {

        std::unique_ptr<QueryPlanStage> t2stage;
        t2stage.reset(new QueryPlanStage());

        Tier2Operator op      = req.order_by == OrderBy::SERIES
                              ? Tier2Operator::CHAIN_SERIES
                              : Tier2Operator::MERGE_TIME_ORDER;
        t2stage->op_.tier2    = op;
        t2stage->tier_        = 2;
        t2stage->opt_ids_     = req.select.columns.at(0).ids;
        t2stage->time_range_  = std::make_pair(begin, end);  // not needed here but anyway
        t2stage->opt_matcher_ = req.select.matcher;

        result.push_back(std::move(t2stage));
    }
    return result;
}

static StagesT create_aggregate(ReshapeRequest const& req) {
    // Hardwired query plan for aggregate query
    // Tier1
    // - List of aggregate operators
    // Tier2
    // - If group-by is enabled:
    //   - Transform ids and matcher (generate new names)
    //   - Add merge materialization step (series or time order, depending on the
    //     order-by clause.
    // - Otherwise
    //   - If oreder-by is series add chain materialization step.
    //   - Otherwise add merge materializer.

    StagesT result;

    if (req.order_by == OrderBy::TIME) {
        AKU_PANIC("Invalid request");
    }

    std::unique_ptr<QueryPlanStage> t1stage;
    t1stage.reset(new QueryPlanStage());

    aku_Timestamp begin   = req.select.begin;
    aku_Timestamp end     = req.select.end;
    const auto &tier1ids  = req.select.columns.at(0).ids;
    t1stage->op_.tier1    = Tier1Operator::AGGREGATE_RANGE;
    t1stage->tier_        = 1;
    t1stage->opt_ids_     = tier1ids;
    t1stage->opt_matcher_ = req.select.matcher;
    t1stage->time_range_  = std::make_pair(begin, end);

    result.push_back(std::move(t1stage));

    if (req.group_by.enabled) {
        // Stage2 - combine aggregate
        std::vector<aku_ParamId> ids;
        for(auto id: req.select.columns.at(0).ids) {
            auto it = req.group_by.transient_map.find(id);
            if (it != req.group_by.transient_map.end()) {
                ids.push_back(it->second);
            }
        }
        std::unique_ptr<QueryPlanStage> t2stage;
        t2stage.reset(new QueryPlanStage());
        Tier2Operator op2     = Tier2Operator::AGGREGATE_COMBINE;
        t2stage->op_.tier2    = op2;
        t2stage->tier_        = 2;
        t2stage->opt_matcher_ = req.group_by.matcher;
        t2stage->opt_ids_     = ids;
        t2stage->time_range_  = std::make_pair(begin, end);
        t2stage->opt_func_    = req.agg.func;

        result.push_back(std::move(t2stage));
    } else {
        // Stage2 - materialize aggregate
        std::unique_ptr<QueryPlanStage> t2stage;
        t2stage.reset(new QueryPlanStage());

        Tier2Operator op      = Tier2Operator::AGGREGATE;
        t2stage->op_.tier2    = op;
        t2stage->tier_        = 2;
        t2stage->opt_ids_     = req.select.columns.at(0).ids;
        t2stage->time_range_  = std::make_pair(begin, end);  // not needed here but anyway
        t2stage->opt_matcher_ = req.select.matcher;
        t2stage->opt_func_    = req.agg.func;

        result.push_back(std::move(t2stage));
    }

    return result;
}

static StagesT create_join(ReshapeRequest const& req) {
    StagesT result;

    // Group-by and aggregation is not supported currently
    if (req.agg.enabled || req.group_by.enabled || req.select.columns.size() < 2) {
        AKU_PANIC("Invalid request");
    }

    std::unique_ptr<QueryPlanStage> t1stage;
    t1stage.reset(new QueryPlanStage());
    std::vector<aku_ParamId> t1ids;

    int cardinality       = static_cast<int>(req.select.columns.size());
    for (size_t i = 0; i < req.select.columns.at(0).ids.size(); i++) {
        for (int c = 0; c < cardinality; c++) {
            t1ids.push_back(req.select.columns.at(static_cast<size_t>(c)).ids.at(i));
        }
    }
    aku_Timestamp begin   = req.select.begin;
    aku_Timestamp end     = req.select.end;
    t1stage->op_.tier1    = Tier1Operator::SCAN_RANGE;
    t1stage->tier_        = 1;
    t1stage->opt_ids_     = t1ids;
    t1stage->opt_matcher_ = req.select.matcher;
    t1stage->time_range_  = std::make_pair(begin, end);

    result.push_back(std::move(t1stage));
    if (req.group_by.enabled) {
        // Not supported
        AKU_PANIC("Group-by not supported");
    } else {
        // Stage2 - materialize aggregate
        std::unique_ptr<QueryPlanStage> t2stage;
        t2stage.reset(new QueryPlanStage());

        Tier2Operator op      = req.order_by == OrderBy::SERIES ?
                                Tier2Operator::MERGE_JOIN_SERIES_ORDER :
                                Tier2Operator::MERGE_JOIN_TIME_ORDER;
        t2stage->op_.tier2    = op;
        t2stage->tier_        = 2;
        t2stage->opt_join_cardinality_
                              = cardinality;
        t2stage->opt_ids_     = req.select.columns.at(0).ids;  // Join will use ids from the first row
        t2stage->time_range_  = std::make_pair(begin, end);  // not needed here but anyway
        t2stage->opt_matcher_ = req.select.matcher;

        result.push_back(std::move(t2stage));
    }
    return result;
}

static StagesT create_plan(ReshapeRequest const& req) {
    if (req.agg.enabled && req.agg.step == 0) {
        // Aggregate query
        return create_aggregate(req);
    } else if (req.agg.enabled == false && req.select.columns.size() > 1) {
        // Join query
        return create_join(req);
    }
    // Scan query
    return create_scan(req);
}


QueryPlan::QueryPlan(ReshapeRequest const& req)
    : stages_(create_plan(req))
{
}

}} // namespaces
