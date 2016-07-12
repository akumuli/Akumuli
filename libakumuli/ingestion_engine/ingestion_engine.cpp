#include "ingestion_engine.h"
#include "log_iface.h"

namespace Akumuli {
namespace Ingress {

using namespace StorageEngine;

/*  NBTree         TreeRegistry        StreamDispatcher       */
/*  Tree data      Id -> NBTree        Series name parsing    */
/*                 Global state        Connection local state */

// ////////////// //
// Registry entry //
// ////////////// //

RegistryEntry::RegistryEntry(std::unique_ptr<NBTreeExtentsList> &&nbtree)
    : roots_(std::move(nbtree))
{
}

bool RegistryEntry::is_available() const {
    std::lock_guard<std::mutex> m(lock_); AKU_UNUSED(m);
    return roots_.unique();
}

std::shared_ptr<NBTreeExtentsList> RegistryEntry::try_acquire() {
    std::lock_guard<std::mutex> m(lock_); AKU_UNUSED(m);
    if (roots_.unique()) {
        return roots_;
    }
    return std::shared_ptr<NBTreeExtentsList>();
}

// ///////////// //
// Tree registry //
// ///////////// //

IngestionContext::IngestionContext(std::shared_ptr<BlockStore> bstore, std::unique_ptr<MetadataStorage>&& meta)
    : blockstore_(bstore)
    , metadata_(std::move(meta))
{
}

aku_Status IngestionContext::init_series_id(const char* begin, const char* end, aku_Sample *sample, SeriesMatcher *local_matcher) {
    u64 id = 0;
    {
        std::lock_guard<std::mutex> ml(metadata_lock_); AKU_UNUSED(ml);
        id = global_matcher_.match(begin, end);
        if (id == 0) {
            // create new series
            id = global_matcher_.add(begin, end);
            // create new NBTreeExtentsList
            std::vector<LogicAddr> empty;
            auto ptr = new NBTreeExtentsList(id, empty, blockstore_);
            std::unique_ptr<NBTreeExtentsList> tree(ptr);
            auto entry = std::make_shared<RegistryEntry>(std::move(tree));
            std::lock_guard<std::mutex> tl(table_lock_); AKU_UNUSED(tl);
            table_[id] = entry;
        }
    }
    sample->paramid = id;
    local_matcher->_add(begin, end, id);
    return AKU_SUCCESS;
}

int IngestionContext::get_series_name(aku_ParamId id, char* buffer, size_t buffer_size, SeriesMatcher *local_matcher) {
    auto str = global_matcher_.id2str(id);
    if (str.first == nullptr) {
        return 0;
    }
    // copy value to local matcher
    local_matcher->_add(str.first, str.first + str.second, id);
    // copy the string to out buffer
    if (str.second > static_cast<int>(buffer_size)) {
        return -1*str.second;
    }
    memcpy(buffer, str.first, static_cast<size_t>(str.second));
    return str.second;
}

std::shared_ptr<IngestionSession> IngestionContext::create_dispatcher() {
    auto deleter = [](IngestionSession* p) {
        p->close();
        delete p;
    };
    auto ptr = new IngestionSession(shared_from_this());
    auto sptr = std::shared_ptr<IngestionSession>(ptr, deleter);
    auto id = reinterpret_cast<size_t>(ptr);
    std::lock_guard<std::mutex> lg(metadata_lock_); AKU_UNUSED(lg);
    active_[id] = sptr;
    return sptr;
}

void IngestionContext::remove_dispatcher(IngestionSession const& disp) {
    auto id = reinterpret_cast<size_t>(&disp);
    std::lock_guard<std::mutex> lg(metadata_lock_); AKU_UNUSED(lg);
    auto it = active_.find(id);
    if (it != active_.end()) {
        active_.erase(it);
    }
}

void IngestionContext::broadcast_sample(aku_Sample const& sample, IngestionSession const* source) {
    std::lock_guard<std::mutex> lg(metadata_lock_); AKU_UNUSED(lg);
    for (auto wdisp: active_) {
        auto disp = wdisp.second.lock();
        if (disp) {
            if (disp.get() != source) {
                if (disp->_receive_broadcast(sample)) {
                    // Sample processed so we don't need to hold the lock
                    // anymore.
                    break;
                }
            }
        }
    }
}

std::shared_ptr<NBTreeExtentsList> IngestionContext::try_acquire(aku_ParamId id) {
    std::lock_guard<std::mutex> lg(table_lock_); AKU_UNUSED(lg);
    auto it = table_.find(id);
    if (it != table_.end() && it->second->is_available()) {
        return it->second->try_acquire();
    }
    return std::shared_ptr<NBTreeExtentsList>();
}

// //////////////// //
// StreamDispatcher //
// //////////////// //

IngestionSession::IngestionSession(std::shared_ptr<IngestionContext> registry)
    : registry_(registry)
{
    // At this point this `StreamDispatcher` should be already registered.
    // This should be done by `TreeRegistry::create_dispatcher` function
    // because we can't call `shared_from_this` in `StremDispatcher::c-tor`.
}

void IngestionSession::close() {
    auto reg = registry_.lock();
    if (reg) {
        reg->remove_dispatcher(*this);
    }
}

aku_Status IngestionSession::init_series_id(const char* begin, const char* end, aku_Sample *sample) {
    // Series name normalization procedure. Most likeley a bottleneck but
    // can be easily parallelized.
    const char* ksbegin = nullptr;
    const char* ksend = nullptr;
    char buf[AKU_LIMITS_MAX_SNAME];
    char* ob = static_cast<char*>(buf);
    char* oe = static_cast<char*>(buf) + AKU_LIMITS_MAX_SNAME;
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
            status = reg->init_series_id(ob, ksend, sample, &local_matcher_);
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

int IngestionSession::get_series_name(aku_ParamId id, char* buffer, size_t buffer_size) {
    auto name = local_matcher_.id2str(id);
    if (name.first == nullptr) {
        // not yet cached!
        auto reg = registry_.lock();
        if (reg) {
            return reg->get_series_name(id, buffer, buffer_size, &local_matcher_);
        }
        Logger::msg(AKU_LOG_ERROR, "Attempt to get series name after close!");
        return 0;
    }
    memcpy(buffer, name.first, static_cast<size_t>(name.second));
    return name.second;
}

aku_Status IngestionSession::write(aku_Sample const& sample) {
    if (AKU_UNLIKELY(sample.payload.type != AKU_PAYLOAD_FLOAT)) {
        return AKU_EBAD_ARG;
    }
    aku_ParamId id = sample.paramid;
    // Locate registery entry in cache, if no such entry - try to acquire
    // registery entry, if registery entry is already acquired by the other
    // `StreamDispatcher` - broadcast value to all other dispatchers.
    std::lock_guard<std::mutex> m(lock_); AKU_UNUSED(m);
    auto it = cache_.find(id);
    if (it == cache_.end()) {
        // try to acquire entry
        auto reg = registry_.lock();
        if (reg) {
            auto entry = reg->try_acquire(id);
            if (entry) {
                cache_[id] = entry;
                auto flush = entry->append(sample.timestamp, sample.payload.float64);
                AKU_UNUSED(flush);
                // FIXME: perform flush if needed
            } else {
                reg->broadcast_sample(sample, this);
            }
        } else {
            return AKU_ECLOSED;
        }
    } else {
        auto flush = it->second->append(sample.timestamp, sample.payload.float64);
        AKU_UNUSED(flush);
        // FIXME: perform flush if needed
    }
    return AKU_SUCCESS;
}

bool IngestionSession::_receive_broadcast(const aku_Sample &sample) {
    aku_ParamId id = sample.paramid;
    std::lock_guard<std::mutex> m(lock_); AKU_UNUSED(m);
    auto it = cache_.find(id);
    if (it != cache_.end()) {
        // perform write
        auto should_flush = it->second->append(sample.timestamp, sample.payload.float64);
        AKU_UNUSED(should_flush);
        // FIXME: perform flush if needed
        return true;
    }
    return false;
}

}}  // namespace
