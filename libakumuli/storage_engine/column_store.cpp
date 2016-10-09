#include "column_store.h"
#include "log_iface.h"
#include "status_util.h"
#include "query_processing/queryparser.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/heap/skew_heap.hpp>
#include <boost/range.hpp>
#include <boost/range/iterator_range.hpp>

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
        dest[i].payload.size = sizeof(aku_Sample);
        dest[i].paramid = outids[i];
        dest[i].timestamp = destts_vec[i];
        dest[i].payload.float64 = destval_vec[i];
    }
    return std::tie(status, accsz);
}

static const size_t RANGE_SIZE = 1024;


template<int dir>  // 0 - forward, 1 - backward
struct TimeOrder {
    typedef std::tuple<aku_Timestamp, aku_ParamId> KeyType;
    typedef std::tuple<KeyType, double, u32> HeapItem;
    std::greater<KeyType> greater_;
    std::less<KeyType> less_;

    bool operator () (HeapItem const& lhs, HeapItem const& rhs) const {
        if (dir == 0) {
            return greater_(std::get<0>(lhs), std::get<0>(rhs));
        }
        return less_(std::get<0>(lhs), std::get<0>(rhs));
    }
};

template<int dir>  // 0 - forward, 1 - backward
struct SeriesOrder {
    typedef std::tuple<aku_Timestamp, aku_ParamId> KeyType;
    typedef std::tuple<KeyType, double, u32> HeapItem;
    typedef std::tuple<aku_ParamId, aku_Timestamp> InvKeyType;
    std::greater<InvKeyType> greater_;
    std::less<InvKeyType> less_;

    bool operator () (HeapItem const& lhs, HeapItem const& rhs) const {
        auto lkey = std::get<0>(lhs);
        InvKeyType ilhs = std::make_tuple(std::get<1>(lkey), std::get<0>(lkey));
        auto rkey = std::get<0>(rhs);
        InvKeyType irhs = std::make_tuple(std::get<1>(rkey), std::get<0>(rkey));
        if (dir == 0) {
            return greater_(ilhs, irhs);
        }
        return less_(ilhs, irhs);
    }
};


template<template <int dir> class CmpPred>
struct MergeIterator : RowIterator {
    std::vector<std::unique_ptr<NBTreeIterator>> iters_;
    std::vector<aku_ParamId> ids_;
    bool forward_;

    struct Range {
        std::vector<aku_Timestamp> ts;
        std::vector<double> xs;
        aku_ParamId id;
        size_t size;
        size_t pos;

        Range(aku_ParamId id)
            : id(id)
            , size(0)
            , pos(0)
        {
            ts.resize(RANGE_SIZE);
            xs.resize(RANGE_SIZE);
        }

        void advance() {
            pos++;
        }

        void retreat() {
            assert(pos);
            pos--;
        }

        bool empty() const {
            return !(pos < size);
        }

        std::tuple<aku_Timestamp, aku_ParamId> top_key() const {
            return std::make_tuple(ts.at(pos), id);
        }

        double top_value() const {
            return xs.at(pos);
        }
    };

    std::vector<Range> ranges_;

    MergeIterator(std::vector<aku_ParamId>&& ids, std::vector<std::unique_ptr<NBTreeIterator>>&& it)
        : iters_(std::move(it))
        , ids_(std::move(ids))
    {
        if (!iters_.empty()) {
            forward_ = iters_.front()->get_direction() == NBTreeIterator::Direction::FORWARD;
        }
        if (iters_.size() != ids_.size()) {
            AKU_PANIC("MergeIterator - broken invariant");
        }
    }

    virtual std::tuple<aku_Status, size_t> read(aku_Sample *dest, size_t size) override {
        if (forward_) {
            return kway_merge<0>(dest, size);
        }
        return kway_merge<1>(dest, size);
    }

    template<int dir>
    std::tuple<aku_Status, size_t> kway_merge(aku_Sample* dest, size_t size) {
        if (iters_.empty()) {
            return std::make_tuple(AKU_ENO_DATA, 0);
        }
        size_t outpos = 0;
        if (ranges_.empty()) {
            // `ranges_` array should be initialized on first call
            for (size_t i = 0; i < iters_.size(); i++) {
                Range range(ids_[i]);
                aku_Status status;
                size_t outsize;
                std::tie(status, outsize) = iters_[i]->read(range.ts.data(), range.xs.data(), RANGE_SIZE);
                if (status == AKU_SUCCESS || (status == AKU_ENO_DATA && outsize != 0)) {
                    range.size = outsize;
                    range.pos  = 0;
                    ranges_.push_back(std::move(range));
                }
                if (status != AKU_SUCCESS && status != AKU_ENO_DATA) {
                    return std::make_tuple(status, 0);
                }
            }
        }

        typedef CmpPred<dir> Comp;
        typedef typename Comp::HeapItem HeapItem;
        typedef typename Comp::KeyType KeyType;
        typedef boost::heap::skew_heap<HeapItem, boost::heap::compare<Comp>> Heap;
        Heap heap;

        int index = 0;
        for(auto& range: ranges_) {
            if (!range.empty()) {
                KeyType key = range.top_key();
                heap.push(std::make_tuple(key, range.top_value(), index));
            }
            index++;
        }

        enum {
            KEY = 0,
            VALUE = 1,
            INDEX = 2,
            TIME = 0,
            ID = 1,
        };

        while(!heap.empty()) {
            HeapItem item = heap.top();
            KeyType point = std::get<KEY>(item);
            u32 index = std::get<INDEX>(item);
            aku_Sample sample;
            sample.paramid = std::get<ID>(point);
            sample.timestamp = std::get<TIME>(point);
            sample.payload.type = AKU_PAYLOAD_FLOAT;
            sample.payload.size = sizeof(aku_Sample);
            sample.payload.float64 = std::get<VALUE>(item);

            if (outpos < size) {
                dest[outpos++] = sample;
            } else {
                // Output buffer is fully consumed
                return std::make_tuple(AKU_SUCCESS, size);
            }
            heap.pop();
            ranges_[index].advance();
            if (ranges_[index].empty()) {
                // Refill range if possible
                aku_Status status;
                size_t outsize;
                std::tie(status, outsize) = iters_[index]->read(ranges_[index].ts.data(), ranges_[index].xs.data(), RANGE_SIZE);
                if (status != AKU_SUCCESS && status != AKU_ENO_DATA) {
                    return std::make_tuple(status, 0);
                }
                ranges_[index].size = outsize;
                ranges_[index].pos  = 0;
            }
            if (!ranges_[index].empty()) {
                KeyType point = ranges_[index].top_key();
                heap.push(std::make_tuple(point, ranges_[index].top_value(), index));
            }
        }
        if (heap.empty()) {
            iters_.clear();
            ranges_.clear();
        }
        // All iterators are fully consumed
        return std::make_tuple(AKU_ENO_DATA, outpos);
    }

};

// ////////////// //
//  Column-store  //
// ////////////// //

ColumnStore::ColumnStore(std::shared_ptr<BlockStore> bstore)
    : blockstore_(bstore)
{
}

aku_Status ColumnStore::open_or_restore(std::unordered_map<aku_ParamId, std::vector<StorageEngine::LogicAddr>> const& mapping) {
    for (auto it: mapping) {
        aku_ParamId id = it.first;
        std::vector<LogicAddr> const& rescue_points = it.second;
        if (rescue_points.empty()) {
            AKU_PANIC("Invalid rescue points state");
        }
        auto status = NBTreeExtentsList::repair_status(rescue_points);
        if (status == NBTreeExtentsList::RepairStatus::REPAIR) {
            Logger::msg(AKU_LOG_ERROR, "Repair needed, id=" + std::to_string(id));
        }
        auto tree = std::make_shared<NBTreeExtentsList>(id, rescue_points, blockstore_);

        std::lock_guard<std::mutex> tl(table_lock_);
        if (columns_.count(id)) {
            Logger::msg(AKU_LOG_ERROR, "Can't open/repair " + std::to_string(id) + " (already exists)");
            return AKU_EBAD_ARG;
        } else {
            columns_[id] = std::move(tree);
        }
        columns_[id]->force_init();
    }
    return AKU_SUCCESS;
}

std::unordered_map<aku_ParamId, std::vector<StorageEngine::LogicAddr>> ColumnStore::close() {
    std::unordered_map<aku_ParamId, std::vector<StorageEngine::LogicAddr>> result;
    std::lock_guard<std::mutex> tl(table_lock_);
    Logger::msg(AKU_LOG_INFO, "Column-store commit called");
    for (auto it: columns_) {
        auto addrlist = it.second->close();
        result[it.first] = addrlist;
    }
    Logger::msg(AKU_LOG_INFO, "Column-store commit completed");
    return result;
}

aku_Status ColumnStore::create_new_column(aku_ParamId id) {
    std::vector<LogicAddr> empty;
    auto tree = std::make_shared<NBTreeExtentsList>(id, empty, blockstore_);
    {
        std::lock_guard<std::mutex> tl(table_lock_);
        if (columns_.count(id)) {
            return AKU_EBAD_ARG;
        } else {
            columns_[id] = std::move(tree);
        }
    }
    columns_[id]->force_init();
    return AKU_SUCCESS;
}


void ColumnStore::query(const ReshapeRequest &req, QP::IStreamProcessor& qproc) {
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
    auto ids = req.select.ids;
    if (req.group_by.enabled) {
        // Transform each id
        for (size_t i = 0; i < ids.size(); i++) {
            auto oldid = ids[i];
            auto it = req.group_by.transient_map.find(oldid);
            if (it != req.group_by.transient_map.end()) {
                ids[i] = it->second;
            } else {
                // Bad transient id mapping found!
                qproc.set_error(AKU_ENOT_FOUND);
                return;
            }
        }
        if (req.order_by == OrderBy::SERIES) {
            iter.reset(new MergeIterator<SeriesOrder>(std::move(ids), std::move(iters)));
        } else {
            iter.reset(new MergeIterator<TimeOrder>(std::move(ids), std::move(iters)));
        }
    } else {
        if (req.order_by == OrderBy::SERIES) {
            iter.reset(new ChainIterator(std::move(ids), std::move(iters)));
        } else {
            iter.reset(new MergeIterator<TimeOrder>(std::move(ids), std::move(iters)));
        }
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
        for (size_t ix = 0; ix < size; ix++) {
            if (!qproc.put(dest[ix])) {
                return;
            }
        }
    }
}

size_t ColumnStore::_get_uncommitted_memory() const {
    std::lock_guard<std::mutex> guard(table_lock_);
    size_t total_size = 0;
    for (auto const& p: columns_) {
        total_size += p.second->_get_uncommitted_size();
    }
    return total_size;
}

NBTreeAppendResult ColumnStore::write(aku_Sample const& sample, std::vector<LogicAddr>* rescue_points,
                               std::unordered_map<aku_ParamId, std::shared_ptr<NBTreeExtentsList>>* cache_or_null)
{
    std::lock_guard<std::mutex> lock(table_lock_);
    aku_ParamId id = sample.paramid;
    auto it = columns_.find(id);
    if (it != columns_.end()) {
        auto tree = it->second;
        auto res = tree->append(sample.timestamp, sample.payload.float64);
        if (res == NBTreeAppendResult::OK_FLUSH_NEEDED) {
            auto tmp = tree->get_roots();
            rescue_points->swap(tmp);
        }
        if (cache_or_null != nullptr) {
            cache_or_null->insert(std::make_pair(id, tree));
        }
        return res;
    }
    return NBTreeAppendResult::FAIL_BAD_ID;
}


// ////////////////////// //
//      WriteSession      //
// ////////////////////// //

CStoreSession::CStoreSession(std::shared_ptr<ColumnStore> registry)
    : cstore_(registry)
{
}

NBTreeAppendResult CStoreSession::write(aku_Sample const& sample, std::vector<LogicAddr> *rescue_points) {
    if (AKU_UNLIKELY(sample.payload.type != AKU_PAYLOAD_FLOAT)) {
        return NBTreeAppendResult::FAIL_BAD_VALUE;
    }
    // Cache lookup
    auto it = cache_.find(sample.paramid);
    if (it != cache_.end()) {
        return it->second->append(sample.timestamp, sample.payload.float64);
    }
    // Cache miss - access global registry
    return cstore_->write(sample, rescue_points, &cache_);
}

void CStoreSession::query(const ReshapeRequest &req, QP::IStreamProcessor& proc) {
    cstore_->query(req, proc);
}

}}  // namespace
