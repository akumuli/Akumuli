#include "queryplan.h"

namespace Akumuli {
namespace QP {

typedef std::vector<std::unique_ptr<QueryPlanStage>> StagesListT;

static StagesListT create_scan(ReshapeRequest const& req) {
    StagesListT result;
    if (req.agg.enabled || req.select.columns.size() != 1) {
        AKU_PANIC("Invalid request");
    }
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

    auto begin = req.select.begin;
    auto end = req.select.end;

    std::unique_ptr<QueryPlanStage> t1stage;
    t1stage.reset(new QueryPlanStage());

    const auto &tier1ids  = req.select.columns.at(0).ids;
    t1stage->op_.tier1    = Tier1Operator::RANGE_SCAN;
    t1stage->tier_        = 1;
    t1stage->opt_ids_     = tier1ids;
    t1stage->opt_matcher_ = req.select.matcher;
    t1stage->time_range_  = std::make_pair(begin, end);

    result.push_back(std::move(t1stage));

    if (req.group_by.enabled) {
        //
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
    return std::move(result);
}

static StagesListT create_plan(ReshapeRequest const& req) {
    return create_scan(req);
}


QueryPlan::QueryPlan(ReshapeRequest const& req)
    : stages(create_plan(req))
{
}

}} // namespaces
