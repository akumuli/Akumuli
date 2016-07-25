#include "tree_registry.h"
#include "log_iface.h"

#include <boost/property_tree/ptree.hpp>

namespace Akumuli {
namespace StorageEngine {

using namespace StorageEngine;

/*  NBTree         TreeRegistry        StreamDispatcher       */
/*  Tree data      Id -> NBTree        Series name parsing    */
/*                 Global state        Connection local state */

static std::shared_ptr<NBTreeExtentsList> EMPTY_EXTL = std::shared_ptr<NBTreeExtentsList>();

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

std::tuple<aku_Status, std::shared_ptr<NBTreeExtentsList> > RegistryEntry::try_acquire() {
    std::lock_guard<std::mutex> m(lock_); AKU_UNUSED(m);
    if (roots_.unique()) {
        return std::make_tuple(AKU_SUCCESS, roots_);
    }
    return std::make_tuple(AKU_EBUSY, EMPTY_EXTL);
}

// ///////////// //
// Tree registry //
// ///////////// //

TreeRegistry::TreeRegistry(std::shared_ptr<BlockStore> bstore, std::unique_ptr<MetadataStorage>&& meta)
    : blockstore_(bstore)
    , metadata_(std::move(meta))
{
}

void TreeRegistry::update_rescue_points(aku_ParamId id, std::vector<StorageEngine::LogicAddr>&& addrlist) {
    // Lock metadata
    std::lock_guard<std::mutex> ml(metadata_lock_); AKU_UNUSED(ml);
    rescue_points_[id] = std::move(addrlist);
    cvar_.notify_one();
}

void TreeRegistry::sync_with_metadata_storage() {
    std::vector<SeriesMatcher::SeriesNameT> newnames;
    std::unordered_map<aku_ParamId, std::vector<LogicAddr>> rescue_points;
    {
        std::lock_guard<std::mutex> ml(metadata_lock_); AKU_UNUSED(ml);
        global_matcher_.pull_new_names(&newnames);
        std::swap(rescue_points, rescue_points_);
    }
    // Save new names
    metadata_->begin_transaction();
    metadata_->insert_new_names(newnames);
    // Save rescue points
    metadata_->upsert_rescue_points(std::move(rescue_points_));
    metadata_->end_transaction();
}

aku_Status TreeRegistry::wait_for_sync_request(int timeout_us) {
    std::unique_lock<std::mutex> lock(metadata_lock_);
    auto res = cvar_.wait_for(lock, std::chrono::microseconds(timeout_us));
    if (res == std::cv_status::timeout) {
        return AKU_ETIMEOUT;
    }
    return rescue_points_.empty() ? AKU_ERETRY : AKU_SUCCESS;
}

void TreeRegistry::wait_for_sessions() {
    std::unique_lock<std::mutex> lock(metadata_lock_);
    while(!active_.empty()) {
        cvar_.wait(lock);
    }
}

aku_Status TreeRegistry::init_series_id(const char* begin, const char* end, aku_Sample *sample, SeriesMatcher *local_matcher) {
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
            // add rescue points list (empty) for new entry
            rescue_points_[id] = std::vector<LogicAddr>();
            cvar_.notify_one();
        }
    }
    sample->paramid = id;
    local_matcher->_add(begin, end, id);
    return AKU_SUCCESS;
}

int TreeRegistry::get_series_name(aku_ParamId id, char* buffer, size_t buffer_size, SeriesMatcher *local_matcher) {
    std::lock_guard<std::mutex> ml(metadata_lock_); AKU_UNUSED(ml);
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

std::shared_ptr<Session> TreeRegistry::create_session() {
    auto deleter = [](Session* p) {
        p->close();
        delete p;
    };
    auto ptr = new Session(shared_from_this());
    auto sptr = std::shared_ptr<Session>(ptr, deleter);
    auto id = reinterpret_cast<size_t>(ptr);
    std::lock_guard<std::mutex> lg(metadata_lock_); AKU_UNUSED(lg);
    active_[id] = sptr;
    return sptr;
}

void TreeRegistry::remove_session(Session const& disp) {
    auto id = reinterpret_cast<size_t>(&disp);
    std::lock_guard<std::mutex> lg(metadata_lock_); AKU_UNUSED(lg);
    auto it = active_.find(id);
    if (it != active_.end()) {
        active_.erase(it);
    }
    cvar_.notify_one();
}

NBTreeAppendResult TreeRegistry::broadcast_sample(aku_Sample const& sample, Session const* source) {
    std::lock_guard<std::mutex> lg(metadata_lock_); AKU_UNUSED(lg);
    for (auto wdisp: active_) {
        auto disp = wdisp.second.lock();
        if (disp) {
            if (disp.get() != source) {
                NBTreeAppendResult result;
                bool stop = false;
                std::tie(stop, result) = disp->_receive_broadcast(sample);
                if (stop) {
                    // Sample processed so we don't need to hold the lock
                    // anymore.
                    return result;
                }
            }
        }
    }
    return NBTreeAppendResult::FAIL_BAD_ID;
}

std::tuple<aku_Status, std::shared_ptr<NBTreeExtentsList> > TreeRegistry::try_acquire(aku_ParamId id) {
    std::lock_guard<std::mutex> lg(table_lock_); AKU_UNUSED(lg);
    auto it = table_.find(id);
    if (it != table_.end()) {
        return it->second->try_acquire();
    }
    return std::make_tuple(AKU_ENOT_FOUND, EMPTY_EXTL);
}

std::vector<aku_ParamId> TreeRegistry::get_ids(std::string filter) {
    std::vector<aku_ParamId> ids;
    std::lock_guard<std::mutex> lg(metadata_lock_); AKU_UNUSED(lg);
    StringPoolOffset offset{};
    size_t size;
    auto result = global_matcher_.pool.regex_match(filter.c_str(), &offset, &size);
    for(StringPool::StringT str: result) {
        u64 id = StringTools::extract_id_from_pool(str);
        ids.push_back(static_cast<aku_ParamId>(id));
    }
    return ids;
}


// //////////// //
// ConcatCursor //
// //////////// //

ConcatCursor::ConcatCursor(std::vector<std::unique_ptr<NBTreeIterator>>&& it)
    : iters_(std::move(it))
    , pos_(0)
{
}

std::tuple<aku_Status, size_t> ConcatCursor::read(aku_Timestamp *destts, double *destval, size_t size) {
    if (pos_ == iters_.size()) {
        return std::make_tuple(AKU_ENO_DATA, 0);
    }
    return iters_[pos_]->read(destts, destval, size);
}

ConcatCursor::Direction ConcatCursor::get_direction() {
    return Direction::FORWARD;  // FIXME: use meaningful value
}

// //////////////// //
// StreamDispatcher //
// //////////////// //

Session::Session(std::shared_ptr<TreeRegistry> registry)
    : registry_(registry)
{
    // At this point this `StreamDispatcher` should be already registered.
    // This should be done by `TreeRegistry::create_dispatcher` function
    // because we can't call `shared_from_this` in `StremDispatcher::c-tor`.
}

void Session::close() {
    auto reg = registry_.lock();
    if (reg) {
        reg->remove_session(*this);
    }
}

aku_Status Session::init_series_id(const char* begin, const char* end, aku_Sample *sample) {
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

int Session::get_series_name(aku_ParamId id, char* buffer, size_t buffer_size) {
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

aku_Status Session::write(aku_Sample const& sample) {
    if (AKU_UNLIKELY(sample.payload.type != AKU_PAYLOAD_FLOAT)) {
        return AKU_EBAD_ARG;
    }
    auto status = AKU_SUCCESS;
    aku_ParamId id = sample.paramid;
    // Locate registery entry in cache, if no such entry - try to acquire
    // registery entry, if registery entry is already acquired by the other
    // `StreamDispatcher` - broadcast value to all other dispatchers.
    NBTreeAppendResult append_result = NBTreeAppendResult::OK;
    std::lock_guard<std::mutex> m(lock_); AKU_UNUSED(m);
    auto it = cache_.find(id);
    if (it == cache_.end()) {
        // try to acquire entry
        auto reg = registry_.lock();
        if (reg) {
            std::shared_ptr<NBTreeExtentsList> entry;
            std::tie(status, entry) = reg->try_acquire(id);
            if (status == AKU_SUCCESS) {
                cache_[id] = entry;
                append_result = entry->append(sample.timestamp, sample.payload.float64);
            } else if (status == AKU_EBUSY) {
                status = AKU_SUCCESS;
                append_result = reg->broadcast_sample(sample, this);
            }
        } else {
            status = AKU_ECLOSED;
        }
    } else {
        append_result = it->second->append(sample.timestamp, sample.payload.float64);
    }
    if (status == AKU_SUCCESS) {
        switch(append_result) {
        case NBTreeAppendResult::OK:
            break;
        case NBTreeAppendResult::OK_FLUSH_NEEDED: {
                std::vector<LogicAddr> rescue_points = it->second->get_roots();
                auto reg = registry_.lock();
                if (reg) {
                    reg->update_rescue_points(id, std::move(rescue_points));
                } else {
                    status = AKU_ECLOSED;
                }
                break;
            }
        case NBTreeAppendResult::FAIL_LATE_WRITE:
            status = AKU_ELATE_WRITE;
            break;
        case NBTreeAppendResult::FAIL_BAD_ID:
            status = AKU_ENOT_FOUND;
            break;
        }
    }
    return status;
}

std::tuple<bool, NBTreeAppendResult> Session::_receive_broadcast(const aku_Sample &sample) {
    aku_ParamId id = sample.paramid;
    std::lock_guard<std::mutex> m(lock_); AKU_UNUSED(m);
    auto it = cache_.find(id);
    if (it != cache_.end()) {
        // perform write
        auto result = it->second->append(sample.timestamp, sample.payload.float64);
        return std::make_tuple(true, result);
    }
    return std::make_tuple(false, NBTreeAppendResult::OK);
}

std::tuple<aku_Status, std::unique_ptr<ConcatCursor>> Session::query(const boost::property_tree::ptree &query)
{
    // NOTE: this is placeholder for query analyzer, at this point akumuli can
    // perform only simple reads.
    aku_Timestamp begin, end;
    std::string filter;
    try {
        begin = query.get<aku_Timestamp>("begin");
        end = query.get<aku_Timestamp>("end");
        filter = query.get<std::string>("filter");
    } catch (const boost::property_tree::ptree_error& err) {
        Logger::msg(AKU_LOG_ERROR, err.what());
        return std::make_tuple(AKU_EBAD_ARG, std::unique_ptr<ConcatCursor>());
    }
    auto reg = registry_.lock();
    if (!reg) {
        Logger::msg(AKU_LOG_ERROR, "Registry is closed");
        return std::make_tuple(AKU_ECLOSED, std::unique_ptr<ConcatCursor>());
    }
    auto ids = reg->get_ids(filter);
    std::vector<std::unique_ptr<NBTreeIterator>> iterators;
    std::lock_guard<std::mutex> m(lock_); AKU_UNUSED(m);
    for (auto id: ids) {
        auto it = cache_.find(id);
        if (it != cache_.end()) {
            auto nbtree_iter = it->second->search(begin, end);
            iterators.push_back(std::move(nbtree_iter));
        }
    }
    auto ptr = new ConcatCursor(std::move(iterators));
    return std::make_tuple(AKU_SUCCESS, std::unique_ptr<ConcatCursor>(ptr));
}

}}  // namespace
