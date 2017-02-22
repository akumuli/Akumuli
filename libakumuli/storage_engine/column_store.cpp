#include "column_store.h"
#include "log_iface.h"
#include "status_util.h"
#include "query_processing/queryparser.h"
#include "operators/aggregate.h"
#include "operators/scan.h"
#include "operators/join.h"
#include "operators/merge.h"

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
    str << "select: " << req.select.columns.size() << ")";
    return str.str();
}

static const size_t RANGE_SIZE = 1024;



namespace GA {

    struct TupleOutputUtils {
        /** Get pointer to buffer and return pointer to sample and tuple data */
        static std::tuple<aku_Sample*, double*> cast(u8* dest) {
            aku_Sample* sample = reinterpret_cast<aku_Sample*>(dest);
            double* tuple      = reinterpret_cast<double*>(sample->payload.data);
            return std::make_tuple(sample, tuple);
        }

        static double get_flags(std::vector<AggregationFunction> const& tup) {
            // Shift will produce power of two (e.g. if tup.size() == 3 then
            // (1 << tup.size) will give us 8, 8-1 is 7 (exactly three lower
            // bits is set)).
            union {
                double d;
                u64 u;
            } bits;
            bits.u = (1ull << tup.size()) - 1;
            return bits.d;
        }

        static double get(AggregationResult const& res, AggregationFunction afunc) {
            double out = 0;
            switch (afunc) {
            case AggregationFunction::CNT:
                out = res.cnt;
                break;
            case AggregationFunction::SUM:
                out = res.sum;
                break;
            case AggregationFunction::MIN:
                out = res.min;
                break;
            case AggregationFunction::MIN_TIMESTAMP:
                out = static_cast<double>(res.mints);
                break;
            case AggregationFunction::MAX:
                out = res.max;
                break;
            case AggregationFunction::MAX_TIMESTAMP:
                out = res.maxts;
                break;
            case AggregationFunction::MEAN:
                out = res.sum / res.cnt;
                break;
            }
            return out;
        }

        static void set_tuple(double* tuple, std::vector<AggregationFunction> const& comp, AggregationResult const& res) {
            for (size_t i = 0; i < comp.size(); i++) {
                auto elem = comp[i];
                *tuple = get(res, elem);
                tuple++;
            }
        }

        static size_t get_tuple_size(const std::vector<AggregationFunction>& tup) {
            size_t payload = 0;
            assert(!tup.empty());
            payload = sizeof(double)*tup.size();
            return sizeof(aku_Sample) + payload;
        }
    };

    struct SeriesOrderIterator : TupleOutputUtils, TupleOperator {
        std::vector<std::unique_ptr<AggregateOperator>> iters_;
        std::vector<aku_ParamId> ids_;
        std::vector<AggregationFunction> tuple_;
        u32 pos_;

        SeriesOrderIterator(std::vector<aku_ParamId>&& ids,
                            std::vector<std::unique_ptr<AggregateOperator>>&& it,
                            const std::vector<AggregationFunction>& components)
            : iters_(std::move(it))
            , ids_(std::move(ids))
            , tuple_(std::move(components))
            , pos_(0)
        {
        }

        virtual std::tuple<aku_Status, size_t> read(u8 *dest, size_t size) override;
    };

    std::tuple<aku_Status, size_t> SeriesOrderIterator::read(u8 *dest, size_t dest_size) {
        aku_Status status = AKU_ENO_DATA;
        size_t ressz = 0;  // current size
        size_t accsz = 0;  // accumulated size
        size_t sample_size = get_tuple_size(tuple_);
        size_t size = dest_size / sample_size;
        std::vector<aku_Timestamp> destts_vec(size, 0);
        std::vector<AggregationResult> destval_vec(size, INIT_AGGRES);
        std::vector<aku_ParamId> outids(size, 0);
        aku_Timestamp* destts = destts_vec.data();
        AggregationResult* destval = destval_vec.data();
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
            double* tup;
            aku_Sample* sample;
            std::tie(sample, tup)   = cast(dest);
            dest                   += sample_size;
            sample->payload.type    = AKU_PAYLOAD_TUPLE;
            sample->payload.size    = static_cast<u16>(sample_size);
            sample->paramid         = outids[i];
            sample->timestamp       = destts_vec[i];
            sample->payload.float64 = get_flags(tuple_);
            set_tuple(tup, tuple_, destval_vec[i]);
        }
        return std::make_tuple(status, accsz*sample_size);

    }

    struct TimeOrderIterator : TupleOutputUtils, TupleOperator {
        std::unique_ptr<MergeJoinOperator> join_iter_;

        TimeOrderIterator(const std::vector<aku_ParamId>& ids,
                          std::vector<std::unique_ptr<AggregateOperator>> &it,
                          const std::vector<AggregationFunction>& components)
        {
            assert(it.size());
            bool forward = it.front()->get_direction() == AggregateOperator::Direction::FORWARD;
            std::vector<std::unique_ptr<TupleOperator>> iters;
            for (size_t i = 0; i < ids.size(); i++) {
                std::unique_ptr<TupleOperator> iter;
                auto agg = std::move(it.at(i));
                std::vector<std::unique_ptr<AggregateOperator>> agglist;
                agglist.push_back(std::move(agg));
                auto ptr = new SeriesOrderIterator({ ids[i] }, std::move(agglist), components);
                iter.reset(ptr);
                iters.push_back(std::move(iter));
            }
            join_iter_.reset(new MergeJoinOperator(std::move(iters), forward));
        }

        virtual std::tuple<aku_Status, size_t> read(u8 *dest, size_t size) override {
            return join_iter_->read(dest, size);
        }
    };
}


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
            columns_[id]->force_init();
            return AKU_SUCCESS;
        }
    }
}


void ColumnStore::query(const ReshapeRequest &req, QP::IStreamProcessor& qproc) {
    Logger::msg(AKU_LOG_TRACE, "ColumnStore `select` query: " + to_string(req));

    // Query validations
    if (req.select.columns.size() > 1) {
        Logger::msg(AKU_LOG_ERROR, "Bad column-store `select` request, too many columns");
        qproc.set_error(AKU_EBAD_ARG);
        return;
    } else if (req.select.columns.size() == 0) {
        Logger::msg(AKU_LOG_ERROR, "Bad column-store `select` request, no columns");
        qproc.set_error(AKU_EBAD_ARG);
        return;
    }
    if (req.agg.enabled) {
        if (req.agg.func.size() > 1) {
            Logger::msg(AKU_LOG_ERROR, "Bad column-store `aggregate` request, too many aggregation functions (not yet supported)");
            qproc.set_error(AKU_EBAD_ARG);
            return;
        } else if (req.agg.func.empty()) {
            Logger::msg(AKU_LOG_ERROR, "Bad column-store `aggregate` request, aggregation function is not set");
            qproc.set_error(AKU_EBAD_ARG);
            return;
        }
    }

    std::unique_ptr<TupleOperator> iter;
    auto ids = req.select.columns.at(0).ids;
    if (req.agg.enabled) {
        std::vector<std::unique_ptr<AggregateOperator>> agglist;
        for (auto id: req.select.columns.at(0).ids) {
            std::lock_guard<std::mutex> lg(table_lock_); AKU_UNUSED(lg);
            auto it = columns_.find(id);
            if (it != columns_.end()) {
                std::unique_ptr<AggregateOperator> agg = it->second->aggregate(req.select.begin, req.select.end);
                agglist.push_back(std::move(agg));
            } else {
                qproc.set_error(AKU_ENOT_FOUND);
                return;
            }
        }
        if (req.group_by.enabled) {
            // FIXME: Not yet supported
            Logger::msg(AKU_LOG_ERROR, "Group-by in `aggregate` query is not supported yet");
            qproc.set_error(AKU_ENOT_PERMITTED);
            return;
        } else {
            if (req.order_by == OrderBy::SERIES) {
                iter.reset(new Aggregator(std::move(ids), std::move(agglist), req.agg.func.front()));
            } else {
                // Error: invalid query
                Logger::msg(AKU_LOG_ERROR, "Bad `aggregate` query, order-by statement not supported");
                qproc.set_error(AKU_ENOT_PERMITTED);
                return;
            }
        }
    } else {
        std::vector<std::unique_ptr<RealValuedOperator>> iters;
        for (auto id: req.select.columns.at(0).ids) {
            std::lock_guard<std::mutex> lg(table_lock_); AKU_UNUSED(lg);
            auto it = columns_.find(id);
            if (it != columns_.end()) {
                std::unique_ptr<RealValuedOperator> iter = it->second->search(req.select.begin, req.select.end);
                iters.push_back(std::move(iter));
            } else {
                qproc.set_error(AKU_ENOT_FOUND);
                return;
            }
        }
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
                iter.reset(new MergeOperator<SeriesOrder>(std::move(ids), std::move(iters)));
            } else {
                iter.reset(new MergeOperator<TimeOrder>(std::move(ids), std::move(iters)));
            }
        } else {
            if (req.order_by == OrderBy::SERIES) {
                iter.reset(new ChainOperator(std::move(ids), std::move(iters)));
            } else {
                iter.reset(new MergeOperator<TimeOrder>(std::move(ids), std::move(iters)));
            }
        }
    }

    const size_t dest_size = 0x1000;
    std::vector<aku_Sample> dest;
    dest.resize(dest_size);
    aku_Status status = AKU_SUCCESS;
    while(status == AKU_SUCCESS) {
        size_t size;
        // This is OK because normal query (aggregate or select) will write fixed size samples with size = sizeof(aku_Sample).
        //
        std::tie(status, size) = iter->read(reinterpret_cast<u8*>(dest.data()), dest_size*sizeof(aku_Sample));
        if (status != AKU_SUCCESS && (status != AKU_ENO_DATA && status != AKU_EUNAVAILABLE)) {
            Logger::msg(AKU_LOG_ERROR, "Iteration error " + StatusUtil::str(status));
            qproc.set_error(status);
            return;
        }
        size_t ixsize = size / sizeof(aku_Sample);
        for (size_t ix = 0; ix < ixsize; ix++) {
            if (!qproc.put(dest[ix])) {
                Logger::msg(AKU_LOG_TRACE, "Iteration stopped by client");
                return;
            }
        }
    }
}

void ColumnStore::join_query(QP::ReshapeRequest const& req, QP::IStreamProcessor& qproc) {
    Logger::msg(AKU_LOG_TRACE, "ColumnStore `json` query: " + to_string(req));
    if (req.select.columns.size() < 2) {
        Logger::msg(AKU_LOG_ERROR, "Bad column-store `join` request, not enough columns");
        qproc.set_error(AKU_EBAD_ARG);
        return;
    }
    std::vector<std::unique_ptr<TupleOperator>> iters;
    for (u32 ix = 0; ix < req.select.columns.front().ids.size(); ix++) {
        std::vector<std::unique_ptr<RealValuedOperator>> row;
        std::vector<aku_ParamId> ids;
        for (u32 col = 0; col < req.select.columns.size(); col++) {
            auto id = req.select.columns[col].ids[ix];
            ids.push_back(id);
            std::lock_guard<std::mutex> lg(table_lock_); AKU_UNUSED(lg);
            auto it = columns_.find(id);
            if (it != columns_.end()) {
                std::unique_ptr<RealValuedOperator> iter = it->second->search(req.select.begin, req.select.end);
                row.push_back(std::move(iter));
            } else {
                qproc.set_error(AKU_ENOT_FOUND);
                return;
            }
        }
        auto it = new JoinOperator(std::move(row), ids.front());
        iters.push_back(std::unique_ptr<JoinOperator>(it));
    }

    if (req.order_by == OrderBy::SERIES) {
        for (auto& it: iters) {
            const size_t dest_size = 4096;
            std::vector<u8> dest;
            dest.resize(dest_size);
            aku_Status status = AKU_SUCCESS;
            while(status == AKU_SUCCESS) {
                size_t size;
                std::tie(status, size) = it->read(dest.data(), dest_size);
                if (status != AKU_SUCCESS && (status != AKU_ENO_DATA && status != AKU_EUNAVAILABLE)) {
                    Logger::msg(AKU_LOG_ERROR, "Iteration error " + StatusUtil::str(status));
                    qproc.set_error(status);
                    return;
                }
                // Parse `dest` buffer
                u8 const* ptr = dest.data();
                u8 const* end = ptr + size;
                while (ptr < end) {
                    aku_Sample const* sample = reinterpret_cast<aku_Sample const*>(ptr);
                    if (!qproc.put(*sample)) {
                        Logger::msg(AKU_LOG_TRACE, "Iteration stopped by client");
                        return;
                    }
                    ptr += sample->payload.size;
                }
            }
        }
    } else {
        std::unique_ptr<TupleOperator> iter;
        bool forward = req.select.begin < req.select.end;
        iter.reset(new MergeJoinOperator(std::move(iters), forward));

        const size_t dest_size = 0x1000;
        std::vector<u8> dest;
        dest.resize(dest_size);
        aku_Status status = AKU_SUCCESS;
        while(status == AKU_SUCCESS) {
            size_t size;
            std::tie(status, size) = iter->read(dest.data(), dest_size);
            if (status != AKU_SUCCESS && (status != AKU_ENO_DATA && status != AKU_EUNAVAILABLE)) {
                Logger::msg(AKU_LOG_ERROR, "Iteration error " + StatusUtil::str(status));
                qproc.set_error(status);
                return;
            }
            size_t pos = 0;
            while(pos < size) {
                aku_Sample const* sample = reinterpret_cast<aku_Sample const*>(dest.data() + pos);
                if (!qproc.put(*sample)) {
                    Logger::msg(AKU_LOG_TRACE, "Iteration stopped by client");
                    return;
                }
                pos += sample->payload.size;
            }
        }
    }
}

void ColumnStore::group_aggregate_query(QP::ReshapeRequest const& req, QP::IStreamProcessor& qproc) {
    Logger::msg(AKU_LOG_TRACE, "ColumnStore `json` query: " + to_string(req));
    if (req.select.columns.size() > 1) {
        Logger::msg(AKU_LOG_ERROR, "Bad column-store `group-aggregate` request, too many columns");
        qproc.set_error(AKU_EBAD_ARG);
        return;
    }
    if (!req.agg.enabled || req.agg.step == 0) {
        Logger::msg(AKU_LOG_ERROR, "Bad column-store `group-aggregate` request, aggregation disabled");
        qproc.set_error(AKU_EBAD_ARG);
        return;
    }
    std::vector<std::unique_ptr<AggregateOperator>> agglist;
    for (auto id: req.select.columns.at(0).ids) {
        std::lock_guard<std::mutex> lg(table_lock_); AKU_UNUSED(lg);
        auto it = columns_.find(id);
        if (it != columns_.end()) {
            std::unique_ptr<AggregateOperator> agg = it->second->group_aggregate(req.select.begin, req.select.end, req.agg.step);
            agglist.push_back(std::move(agg));
        } else {
            qproc.set_error(AKU_ENOT_FOUND);
            return;
        }
    }
    std::unique_ptr<TupleOperator> iter;
    auto ids = req.select.columns.at(0).ids;
    if (req.group_by.enabled) {
        // FIXME: Not yet supported
        Logger::msg(AKU_LOG_ERROR, "Group-by in `group-aggregate` query is not supported yet");
        qproc.set_error(AKU_ENOT_PERMITTED);
        return;
    } else {
        if (req.order_by == OrderBy::SERIES) {
            iter.reset(new GA::SeriesOrderIterator(std::move(ids), std::move(agglist), req.agg.func));
        } else {
            iter.reset(new GA::TimeOrderIterator(ids, agglist, req.agg.func));
        }
    }
    const size_t dest_size = 0x1000;
    std::vector<u8> dest;
    dest.resize(dest_size);
    aku_Status status = AKU_SUCCESS;
    while(status == AKU_SUCCESS) {
        size_t size;
        std::tie(status, size) = iter->read(dest.data(), dest_size);
        if (status != AKU_SUCCESS && (status != AKU_ENO_DATA && status != AKU_EUNAVAILABLE)) {
            Logger::msg(AKU_LOG_ERROR, "Iteration error " + StatusUtil::str(status));
            qproc.set_error(status);
            return;
        }
        size_t pos = 0;
        while(pos < size) {
            aku_Sample const* sample = reinterpret_cast<aku_Sample const*>(dest.data() + pos);
            if (!qproc.put(*sample)) {
                Logger::msg(AKU_LOG_TRACE, "Iteration stopped by client");
                return;
            }
            pos += sample->payload.size;
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
        auto res = it->second->append(sample.timestamp, sample.payload.float64);
        if (res == NBTreeAppendResult::OK_FLUSH_NEEDED) {
            auto tmp = it->second->get_roots();
            rescue_points->swap(tmp);
        }
        return res;
    }
    // Cache miss - access global registry
    return cstore_->write(sample, rescue_points, &cache_);
}

void CStoreSession::query(const ReshapeRequest &req, QP::IStreamProcessor& proc) {
    cstore_->query(req, proc);
}

}}  // namespace
