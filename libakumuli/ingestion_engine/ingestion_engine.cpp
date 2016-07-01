#include "ingestion_engine.h"

namespace Akumuli {
namespace DataIngestion {

/*  NBTree         TreeRegistry        StreamDispatcher       */
/*  Tree data      Id -> NBTree        Series name parsing    */
/*                 Global state        Connection local state */

// ////////////// //
// Registry entry //
// ////////////// //

RegistryEntry::RegistryEntry(std::unique_ptr<StorageEngine::NBTreeExtentsList> &&nbtree)
    : roots_(std::move(nbtree))
{
}

void RegistryEntry::write(aku_Timestamp ts, double value) {
    bool should_flush = roots_->append(ts, value);
    AKU_UNUSED(should_flush);
    // FIXME: use `should_flush` variable (flush mechanism is not in its place yet)
}


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

std::shared_ptr<StreamDispatcher> TreeRegistry::create_dispatcher() {
    auto ptr = std::make_shared<StreamDispatcher>(shared_from_this());
    auto id = reinterpret_cast<size_t>(ptr.get());
    std::lock_guard<std::mutex> lg(metadata_lock_); AKU_UNUSED(lg);
    active_[id] = ptr;
    return ptr;
}

void TreeRegistry::remove_dispatcher(std::shared_ptr<StreamDispatcher> ptr) {
    auto id = reinterpret_cast<size_t>(ptr.get());
    std::lock_guard<std::mutex> lg(metadata_lock_); AKU_UNUSED(lg);
    auto it = active_.find(id);
    if (it != active_.end()) {
        active_.erase(it);
    }
}

void TreeRegistry::broadcast_sample(aku_Sample const* sample) {
    std::lock_guard<std::mutex> lg(metadata_lock_); AKU_UNUSED(lg);
    for (auto wdisp: active_) {
        auto disp = wdisp.second.lock();
        if (disp) {
            if (disp->_receive_broadcast(sample)) {
                // Sample processed so we don't need to hold the lock
                // anymore.
                break;
            }
        }
    }
}

// //////////////// //
// StreamDispatcher //
// //////////////// //

StreamDispatcher::StreamDispatcher(std::shared_ptr<TreeRegistry> registry)
    : registry_(registry)
{
    // At this point this `StreamDispatcher` should be already registered.
    // This should be done by `TreeRegistry::create_dispatcher` function
    // because we can't call `shared_from_this` in `StremDispatcher::c-tor`.
}

StreamDispatcher::~StreamDispatcher() {
    auto ptr = shared_from_this();
    auto reg = registry_.lock();
    if (reg) {
        reg->remove_dispatcher(ptr);
    }
}

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

aku_Status StreamDispatcher::write(aku_Sample const* sample) {
    if (AKU_UNLIKELY(sample->payload.type != AKU_PAYLOAD_FLOAT)) {
        return AKU_EBAD_ARG;
    }
    aku_ParamId id = sample->paramid;
    // Locate registery entry in cache, if no such entry - try to acquire
    // registery entry, if registery entry is already acquired by the other
    // `StreamDispatcher` - broadcast value to all other dispatchers.
    std::lock_guard<std::mutex> m(lock_); AKU_UNUSED(m);
    auto it = cache_.find(id);
    if (it == cache_.end()) {
        // try to acquire entry
    } else {
        it->second->write(sample->timestamp, sample->payload.float64);
    }
    return AKU_ENOT_IMPLEMENTED;
}

bool StreamDispatcher::_receive_broadcast(aku_Sample const* sample) {
    aku_ParamId id = sample->paramid;
    std::lock_guard<std::mutex> m(lock_); AKU_UNUSED(m);
    auto it = cache_.find(id);
    if (it != cache_.end()) {
        // perform write
        throw "Not implemented";
        return true;
    }
    return false;
}

}}  // namespace
