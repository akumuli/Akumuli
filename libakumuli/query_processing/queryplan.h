#pragma once

#include <memory>
#include <vector>

#include "index/seriesparser.h"
#include "queryprocessor_framework.h"
#include "storage_engine/column_store.h"

namespace Akumuli {
namespace QP {

/**
 * Query plan interface
 */
struct IQueryPlan {
    virtual ~IQueryPlan() = default;

    /**
     * Execute query plan.
     * Data can be fetched after successful execute call.
     */
    virtual aku_Status execute(const StorageEngine::ColumnStore& cstore) = 0;

    /** Read samples in batch.
      * Samples can be of variable size.
      * @param dest is a pointer to buffer that will receive series of aku_Sample values
      * @param size is a size of the buffer in bytes
      * @return status of the operation (success or error code) and number of written bytes
      */
    virtual std::tuple<aku_Status, size_t> read(u8 *dest, size_t size) = 0;
};

struct QueryPlanBuilder {
    static std::tuple<aku_Status, std::unique_ptr<IQueryPlan> > create(const ReshapeRequest& req);
};

struct QueryPlanExecutor {

    void execute(const StorageEngine::ColumnStore& cstore, std::unique_ptr<QP::IQueryPlan>&& iter, QP::IStreamProcessor& qproc);
};

}}  // namespaces
