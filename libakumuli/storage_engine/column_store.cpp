#include "column_store.h"
#include "log_iface.h"
#include "status_util.h"
#include "query_processing/queryplan.h"
#include "operators/aggregate.h"
#include "operators/scan.h"
#include "operators/join.h"
#include "operators/merge.h"

#include <boost/property_tree/ptree.hpp>

namespace Akumuli {
namespace StorageEngine {

using namespace QP;


// ////////////// //
//  Column-store  //
// ////////////// //

ColumnStore::ColumnStore(std::shared_ptr<BlockStore> bstore)
    : blockstore_(bstore)
{
}

std::tuple<aku_Status, std::vector<aku_ParamId>> ColumnStore::open_or_restore(
        std::unordered_map<aku_ParamId,
        std::vector<StorageEngine::LogicAddr>> const& mapping,
        bool force_init)
{
    std::vector<aku_ParamId> ids2recover;
    for (auto it: mapping) {
        aku_ParamId id = it.first;
        std::vector<LogicAddr> const& rescue_points = it.second;
        if (rescue_points.empty()) {
            Logger::msg(AKU_LOG_ERROR, "Empty rescue points list found, leaf-node data was lost");
        }
        auto status = NBTreeExtentsList::repair_status(rescue_points);
        if (status == NBTreeExtentsList::RepairStatus::REPAIR) {
            Logger::msg(AKU_LOG_ERROR, "Repair needed, id=" + std::to_string(id));
        }
        auto tree = std::make_shared<NBTreeExtentsList>(id, rescue_points, blockstore_);

        std::lock_guard<std::mutex> tl(table_lock_);
        if (columns_.count(id)) {
            Logger::msg(AKU_LOG_ERROR, "Can't open/repair " + std::to_string(id) + " (already exists)");
            return std::make_tuple(AKU_EBAD_ARG, std::vector<aku_ParamId>());
        } else {
            columns_[id] = std::move(tree);
        }
        if (force_init || status == NBTreeExtentsList::RepairStatus::REPAIR) {
            // Repair is performed on initialization. We don't want to postprone this process
            // since it will introduce runtime penalties.
            columns_[id]->force_init();
            if (status == NBTreeExtentsList::RepairStatus::REPAIR) {
                ids2recover.push_back(id);
            }
            if (force_init == false) {
                // Close the tree until it will be acessed first
                auto rplist = columns_[id]->close();
                rescue_points_[id] = std::move(rplist);
            }
        }
    }
    return std::make_tuple(AKU_SUCCESS, ids2recover);
}

std::unordered_map<aku_ParamId, std::vector<StorageEngine::LogicAddr>> ColumnStore::close() {
    // TODO: remove
    size_t c1_mem = 0, c2_mem = 0;
    for (auto it: columns_) {
        if (it.second->is_initialized()) {
            size_t c1, c2;
            std::tie(c1, c2) = it.second->bytes_used();
            c1_mem += c1;
            c2_mem += c2;
        }
    }
    Logger::msg(AKU_LOG_INFO, "Total memory usage: " + std::to_string(c1_mem + c2_mem));
    Logger::msg(AKU_LOG_INFO, "Leaf node memory usage: " + std::to_string(c1_mem));
    Logger::msg(AKU_LOG_INFO, "SBlock memory usage: " + std::to_string(c2_mem));
    // end TODO remove
    std::unordered_map<aku_ParamId, std::vector<StorageEngine::LogicAddr>> result;
    std::lock_guard<std::mutex> tl(table_lock_);
    Logger::msg(AKU_LOG_INFO, "Column-store commit called");
    for (auto it: columns_) {
        if (it.second->is_initialized()) {
            auto addrlist = it.second->close();
            result[it.first] = addrlist;
        }
    }
    Logger::msg(AKU_LOG_INFO, "Column-store commit completed");
    return result;
}

std::unordered_map<aku_ParamId, std::vector<StorageEngine::LogicAddr>> ColumnStore::close(const std::vector<u64>& ids) {
    std::unordered_map<aku_ParamId, std::vector<StorageEngine::LogicAddr>> result;
    Logger::msg(AKU_LOG_INFO, "Column-store close specific columns");
    for (auto id: ids) {
        std::lock_guard<std::mutex> tl(table_lock_);
        auto it = columns_.find(id);
        if (it == columns_.end()) {
            continue;
        }
        if (it->second->is_initialized()) {
            auto addrlist = it->second->close();
            result[it->first] = addrlist;
        }
    }
    Logger::msg(AKU_LOG_INFO, "Column-store close specific columns, operation completed");
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

size_t ColumnStore::_get_uncommitted_memory() const {
    std::lock_guard<std::mutex> guard(table_lock_);
    size_t total_size = 0;
    for (auto const& p: columns_) {
        if (p.second->is_initialized()) {
            total_size += p.second->_get_uncommitted_size();
        }
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
        NBTreeAppendResult res = NBTreeAppendResult::OK;
        if (AKU_LIKELY(sample.payload.type == AKU_PAYLOAD_FLOAT)) {
            res = tree->append(sample.timestamp, sample.payload.float64);
        }
        else if (sample.payload.type == AKU_PAYLOAD_EVENT) {
            u32 sz = sample.payload.size - sizeof(aku_Sample);
            u8 const* pdata = reinterpret_cast<u8 const*>(sample.payload.data);
            res = tree->append(sample.timestamp, pdata, sz);
        }
        if (res == NBTreeAppendResult::OK_FLUSH_NEEDED) {
            auto tmp = tree->get_roots();
            rescue_points->swap(tmp);
        }
        if (cache_or_null != nullptr) {
            // Tree is guaranteed to be initialized here, so all values in the cache
            // don't need to be checked.
            cache_or_null->insert(std::make_pair(id, tree));
        }
        return res;
    }
    return NBTreeAppendResult::FAIL_BAD_ID;
}

NBTreeAppendResult ColumnStore::recovery_write(aku_Sample const& sample, bool allow_duplicates)
{
    std::lock_guard<std::mutex> lock(table_lock_);
    aku_ParamId id = sample.paramid;
    auto it = columns_.find(id);
    if (it != columns_.end()) {
        auto tree = it->second;
        return tree->append(sample.timestamp, sample.payload.float64, allow_duplicates);
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
    if (AKU_LIKELY(sample.payload.type == AKU_PAYLOAD_FLOAT)) {
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
    else if (sample.payload.type == AKU_PAYLOAD_EVENT) {
        // Unpack event fields
        u32 sz = sample.payload.size - sizeof(aku_Sample);
        u8 const* pdata = reinterpret_cast<u8 const*>(sample.payload.data);
        // Cache lookup
        auto it = cache_.find(sample.paramid);
        if (it != cache_.end()) {
            auto res = it->second->append(sample.timestamp, pdata, sz);
            if (res == NBTreeAppendResult::OK_FLUSH_NEEDED) {
                auto tmp = it->second->get_roots();
                rescue_points->swap(tmp);
            }
            return res;
        }
        return cstore_->write(sample, rescue_points, &cache_);
    }
    return NBTreeAppendResult::FAIL_BAD_VALUE;
}

void CStoreSession::close() {
    // This method can't be implemented yet, because it will waste space.
    // Leaf node recovery should be implemented first.
}

}}  // namespace
