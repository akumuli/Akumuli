#include "ingestion_engine.h"

namespace Akumuli {
namespace DataIngestion {

/*  NBTree         TreeRegistry        StreamDispatcher       */
/*  Tree data      Id -> NBTree        Series name parsing    */
/*                 Global state        Connection local state */

// ///////////// //
// Tree registry //
// ///////////// //

TreeRegistry::TreeRegistry(std::unique_ptr<MetadataStorage>&& meta)
    : metadata_(std::move(meta))
{
}

aku_Status TreeRegistry::init_series_id(const char* begin, const char* end, aku_Sample *sample) {
    std::lock_guard<std::mutex> ml(this->metadata_lock_); AKU_UNUSED(ml);
    u64 id = global_matcher_.match(begin, end);
    if (id == 0) {
        // create new series
        id = global_matcher_.add(begin, end);
    }
    sample->paramid = id;
    return AKU_SUCCESS;
}

// //////////////// //
// StreamDispatcher //
// //////////////// //

aku_Status StreamDispatcher::init_series_id(const char* begin, const char* end, aku_Sample *sample) {
    // Series name normalization procedure. Most likeley a bottleneck but
    // can be easily parallelized.
    const char* ksbegin = nullptr;
    const char* ksend = nullptr;
    char buf[AKU_LIMITS_MAX_SNAME];
    char* ob = static_cast<char*>(buf);
    char* oe = static_cast<char*>(buf);
    aku_Status status = SeriesParser::to_normal_form(begin, end, ob, oe, &ksbegin, &ksend);
    if (status != AKU_SUCCESS) {
        return status;
    }
    // Match series name locally (on success use local information)
    // Otherwise - match using global registry. On success - add global information to
    //  the local matcher. On error - add series name to global registry and then to
    //  the local matcher.
    u64 id = local_matcher_.match(ob, ksend);
    if (!id) {
        // go to global registery
        auto reg = registry_.lock();
        if (reg) {
            status = reg->init_series_id(ob, ksend, sample);
        } else {
            // Global registery has been deleted. Connection should be closed.
            status = AKU_ECLOSED;
        }
    } else {
        // initialize using local info
        sample->paramid = id;
    }
    return status;
}

}}  // namespace
