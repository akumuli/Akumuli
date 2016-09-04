#include "column_store.h"
#include "log_iface.h"
#include "status_util.h"
#include "query_processing/queryparser.h"

#include <boost/property_tree/ptree.hpp>

namespace Akumuli {
namespace StorageEngine {

using namespace QP;

static std::string to_string(ReshapeRequest const& req) {
    std::stringstream str;
    str << "ReshapeRequest(";
    switch (req.order_by) {
    case OrderBy::SERIES:
        str << "order-by: series, ";
        break;
    case OrderBy::TIME:
        str << "order-by: time, ";
        break;
    };
    if (req.group_by.enabled) {
        str << "group-by: enabled, ";
    } else {
        str << "group-by: disabled, ";
    }
    str << "range-begin: " << req.select.begin << ", range-end: " << req.select.end << ", ";
    str << "select: " << req.select.ids.size() << ")";
    return str.str();
}

/** This interface is used by column-store internally.
  * It allows to iterate through a bunch of columns row by row.
  */
struct RowIterator {

    virtual ~RowIterator() = default;
    /** Read samples in batch.
      * @param dest is an array that will receive values from cursor
      * @param size is an arrays size
      */
    virtual std::tuple<aku_Status, size_t> read(aku_Sample *dest, size_t size) = 0;
};


class ChainIterator : public RowIterator {
    std::vector<std::unique_ptr<NBTreeIterator>> iters_;
    std::vector<aku_ParamId> ids_;
    size_t pos_;
public:
    ChainIterator(std::vector<aku_ParamId>&& ids, std::vector<std::unique_ptr<NBTreeIterator>>&& it);
    virtual std::tuple<aku_Status, size_t> read(aku_Sample *dest, size_t size);
};


ChainIterator::ChainIterator(std::vector<aku_ParamId>&& ids, std::vector<std::unique_ptr<NBTreeIterator>>&& it)
    : iters_(std::move(it))
    , ids_(std::move(ids))
    , pos_(0)
{
}

std::tuple<aku_Status, size_t> ChainIterator::read(aku_Sample *dest, size_t size) {
    aku_Status status = AKU_ENO_DATA;
    size_t ressz = 0;  // current size
    size_t accsz = 0;  // accumulated size
    std::vector<aku_Timestamp> destts_vec(size, 0);
    std::vector<double> destval_vec(size, 0);
    std::vector<aku_ParamId> outids(size, 0);
    aku_Timestamp* destts = destts_vec.data();
    double* destval = destval_vec.data();
    while(pos_ < iters_.size()) {
        aku_ParamId curr = ids_[pos_];
        std::tie(status, ressz) = iters_[pos_]->read(destts, destval, size);
        for (size_t i = accsz; i < accsz+ressz; i++) {
            outids[i] = curr;
        }
        destts += ressz;
        destval += ressz;
        size -= ressz;
        accsz += ressz;
        if (size == 0) {
            break;
        }
        pos_++;
        if (status == AKU_ENO_DATA) {
            // this iterator is done, continue with next
            continue;
        }
        if (status != AKU_SUCCESS) {
            // Stop iteration on error!
            break;
        }
    }
    // Convert vectors to series of samples
    for (size_t i = 0; i < accsz; i++) {
        dest[i].payload.type = AKU_PAYLOAD_FLOAT;
        dest[i].paramid = outids[i];
        dest[i].timestamp = destts_vec[i];
        dest[i].payload.float64 = destval_vec[i];
    }
    return std::tie(status, accsz);
}

// ////////////// //
//  Column-store  //
// ////////////// //

ColumnStore::ColumnStore(std::shared_ptr<BlockStore> bstore, std::unique_ptr<MetadataStorage>&& meta)
    : blockstore_(bstore)
    , metadata_(std::move(meta))
{
}

void ColumnStore::update_rescue_points(aku_ParamId id, std::vector<StorageEngine::LogicAddr>&& addrlist) {
    // Lock metadata
    std::lock_guard<std::mutex> ml(metadata_lock_); AKU_UNUSED(ml);
    rescue_points_[id] = std::move(addrlist);
    cvar_.notify_one();
}

void ColumnStore::sync_with_metadata_storage() {
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

aku_Status ColumnStore::wait_for_sync_request(int timeout_us) {
    std::unique_lock<std::mutex> lock(metadata_lock_);
    auto res = cvar_.wait_for(lock, std::chrono::microseconds(timeout_us));
    if (res == std::cv_status::timeout) {
        return AKU_ETIMEOUT;
    }
    return rescue_points_.empty() ? AKU_ERETRY : AKU_SUCCESS;
}


aku_Status ColumnStore::init_series_id(const char* begin, const char* end, aku_Sample *sample, SeriesMatcher *local_matcher) {
    u64 id = 0;
    std::shared_ptr<NBTreeExtentsList> tree;
    {
        std::lock_guard<std::mutex> ml(metadata_lock_);
        id = global_matcher_.match(begin, end);
        if (id == 0) {
            // create new series
            id = global_matcher_.add(begin, end);
            // create new NBTreeExtentsList
            std::vector<LogicAddr> empty;
            tree = std::make_shared<NBTreeExtentsList>(id, empty, blockstore_);
            // add rescue points list (empty) for new entry
            rescue_points_[id] = std::vector<LogicAddr>();
            cvar_.notify_one();
        }
    }
    if (tree) {
        // New tree was created
        std::lock_guard<std::mutex> tl(table_lock_);
        columns_[id] = std::move(tree);
    }
    sample->paramid = id;
    local_matcher->_add(begin, end, id);
    return AKU_SUCCESS;
}

int ColumnStore::get_series_name(aku_ParamId id, char* buffer, size_t buffer_size, SeriesMatcher *local_matcher) {
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

void ColumnStore::query(const ReshapeRequest &req, QP::IQueryProcessor& qproc) {
    Logger::msg(AKU_LOG_TRACE, "ColumnStore query: " + to_string(req));
    std::vector<std::unique_ptr<NBTreeIterator>> iters;
    for (auto id: req.select.ids) {
        std::lock_guard<std::mutex> lg(table_lock_); AKU_UNUSED(lg);
        auto it = columns_.find(id);
        if (it != columns_.end()) {
            std::unique_ptr<NBTreeIterator> iter = it->second->search(req.select.begin, req.select.end);
            iters.push_back(std::move(iter));
        } else {
            qproc.set_error(AKU_ENOT_FOUND);
        }
    }

    std::unique_ptr<RowIterator> iter;
    if (req.order_by == OrderBy::SERIES) {
        auto ids = req.select.ids;
        iter.reset(new ChainIterator(std::move(ids), std::move(iters)));
    } else {
        Logger::msg(AKU_LOG_ERROR, "Order-by-time not implemented yet");
        qproc.set_error(AKU_ENOT_IMPLEMENTED);
    }

    const size_t dest_size = 0x1000;
    std::vector<aku_Sample> dest;
    dest.resize(dest_size);
    aku_Status status = AKU_SUCCESS;
    while(status == AKU_SUCCESS) {
        size_t size;
        std::tie(status, size) = iter->read(dest.data(), dest_size);
        if (status != AKU_SUCCESS && status != AKU_ENO_DATA) {
            Logger::msg(AKU_LOG_ERROR, "Iteration error " + StatusUtil::str(status));
            qproc.set_error(status);
            return;
        }
        if (req.group_by.enabled) {
            for (size_t ix = 0; ix < size; ix++) {
                auto it = req.group_by.transient_map.find(dest[ix].paramid);
                if (it == req.group_by.transient_map.end()) {
                    Logger::msg(AKU_LOG_ERROR, "Unexpected id " + std::to_string(dest[ix].paramid));
                    qproc.set_error(AKU_EBAD_DATA);
                    return;
                }
            }
        } else {
            for (size_t ix = 0; ix < size; ix++) {
                if (!qproc.put(dest[ix])) {
                    return;
                }
            }
        }
    }


}

aku_Status ColumnStore::write(aku_Sample const& sample,
                               std::unordered_map<aku_ParamId, std::shared_ptr<NBTreeExtentsList>>* cache_or_null)
{
    std::lock_guard<std::mutex> lock(table_lock_);
    aku_ParamId id = sample.paramid;
    auto it = columns_.find(id);
    if (it != columns_.end()) {
        auto tree = it->second;
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


// ////////////////////// //
//      WriteSession      //
// ////////////////////// //

WriteSession::WriteSession(std::shared_ptr<ColumnStore> registry)
    : registry_(registry)
{
}

aku_Status WriteSession::init_series_id(const char* begin, const char* end, aku_Sample *sample) {
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

int WriteSession::get_series_name(aku_ParamId id, char* buffer, size_t buffer_size) {
    auto name = local_matcher_.id2str(id);
    if (name.first == nullptr) {
        // not yet cached!
        return registry_->get_series_name(id, buffer, buffer_size, &local_matcher_);
    }
    memcpy(buffer, name.first, static_cast<size_t>(name.second));
    return name.second;
}

aku_Status WriteSession::write(aku_Sample const& sample) {
    if (AKU_UNLIKELY(sample.payload.type != AKU_PAYLOAD_FLOAT)) {
        return AKU_EBAD_ARG;
    }
    // Cache lookup
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

void WriteSession::query(const ReshapeRequest &req, QP::IQueryProcessor& proc) {
    registry_->query(req, proc);
}

}}  // namespace
