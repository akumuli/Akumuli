#include "tree_registry.h"
#include "log_iface.h"
#include "query_processing/queryparser.h"

#include <boost/property_tree/ptree.hpp>

namespace Akumuli {
namespace StorageEngine {

using namespace QP;

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
            auto tree = std::make_shared<NBTreeExtentsList>(id, empty, blockstore_);
            std::unique_lock<std::mutex> tl(table_lock_);
            table_[id] = std::move(tree);
            tl.release();
            // add rescue points list (empty) for new entry
            std::unique_lock<std::mutex> ml(metadata_lock_);
            rescue_points_[id] = std::vector<LogicAddr>();
            ml.release();
            cvar_.notify_one();
        }
    }
    sample->paramid = id;
    local_matcher->_add(begin, end, id);
    return AKU_SUCCESS;
}

int TreeRegistry::get_series_name(aku_ParamId id, char* buffer, size_t buffer_size, SeriesMatcher *local_matcher) {
    std::lock_guard<std::mutex> ml(metadata_lock_);
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

void TreeRegistry::query(QP::IQueryProcessor& qproc) {
    auto &filter = qproc.filter();
    auto ids = filter.get_ids();
    auto range = qproc.range();
    std::vector<std::unique_ptr<NBTreeIterator>> iters;
    for (auto id: ids) {
        std::lock_guard<std::mutex> lg(table_lock_); AKU_UNUSED(lg);
        auto it = table_.find(id);
        if (it != table_.end()) {
            std::unique_ptr<NBTreeIterator> iter = it->second->search(range.begin(), range.end());
            iters.push_back(std::move(iter));
        } else {
            qproc.set_error(AKU_ENOT_FOUND);
        }
    }

    // TODO: Reshape iterators

    qproc.start();
    qproc.set_error(AKU_ENOT_IMPLEMENTED);
}

aku_Status TreeRegistry::write(aku_Sample const& sample,
                               std::unordered_map<aku_ParamId, std::shared_ptr<NBTreeExtentsList>>* cache_or_null)
{
    std::unique_lock<std::mutex> lock(table_lock_);
    aku_ParamId id = sample.paramid;
    auto it = table_.find(id);
    if (it != table_.end()) {
        auto tree = it->second;
        lock.release();
        auto res = tree->append(sample.timestamp, sample.payload.float64);
        switch (res) {
        case NBTreeAppendResult::OK:
            return AKU_SUCCESS;
        case NBTreeAppendResult::OK_FLUSH_NEEDED:
            update_rescue_points(id, tree->get_roots());
            return AKU_SUCCESS;
        case NBTreeAppendResult::FAIL_BAD_ID:
            AKU_PANIC("Invalid tree-registry, id = " + std::to_string(id));
        case NBTreeAppendResult::FAIL_LATE_WRITE:
            return AKU_ELATE_WRITE;
        };
        if (cache_or_null != nullptr) {
            cache_or_null->insert(std::make_pair(id, tree));
        }
    }
    return AKU_EBAD_ARG;
}


// ///////////////// //
//      Session      //
// ///////////////// //

Session::Session(std::shared_ptr<TreeRegistry> registry)
    : registry_(registry)
{
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
        status = registry_->init_series_id(ob, ksend, sample, &local_matcher_);
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
        return registry_->get_series_name(id, buffer, buffer_size, &local_matcher_);
    }
    memcpy(buffer, name.first, static_cast<size_t>(name.second));
    return name.second;
}

aku_Status Session::write(aku_Sample const& sample) {
    if (AKU_UNLIKELY(sample.payload.type != AKU_PAYLOAD_FLOAT)) {
        return AKU_EBAD_ARG;
    }
    // Cache lookup
    std::lock_guard<std::mutex> lock(lock_);
    auto it = cache_.find(sample.paramid);
    if (it != cache_.end()) {
        auto status = it->second->append(sample.timestamp, sample.payload.float64);
        switch (status) {
        case NBTreeAppendResult::OK:
            return AKU_SUCCESS;
        case NBTreeAppendResult::OK_FLUSH_NEEDED:
            registry_->update_rescue_points(sample.paramid, it->second->get_roots());
            return AKU_SUCCESS;
        case NBTreeAppendResult::FAIL_BAD_ID:
            AKU_PANIC("Invalid session cache, id = " + std::to_string(sample.paramid));
        case NBTreeAppendResult::FAIL_LATE_WRITE:
            return AKU_ELATE_WRITE;
        };
    }
    // Cache miss - access global registry
    return registry_->write(sample, &cache_);
}

void Session::query(QP::IQueryProcessor& proc) {
    registry_->query(proc);
}

}}  // namespace
