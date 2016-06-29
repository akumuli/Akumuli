#pragma once

// Stdlib
#include <unordered_map>

// Project
#include "akumuli_def.h"
#include "storage_engine/nbtree.h"

namespace Akumuli {
namespace DataIngestion {

class RegistryEntry {
    std::unique_ptr<StorageEngine::NBTreeExtentsList> roots_;
};

class TreeRegistry {
    std::unordered_map<aku_ParamId, std::shared_ptr<RegistryEntry>> table_;
};

/** Dispatches incoming messages to corresponding NBTreeExtentsList instances.
  * Should be created per writer thread.
  */
class StreamDispatcher
{
    std::shared_ptr<TreeRegistry> registry_;
public:
    StreamDispatcher(std::shared_ptr<TreeRegistry> registry);

    aku_Status append(aku_ParamId id, aku_Timestamp ts, double value);

    std::tuple<aku_Status, aku_ParamId> decode_series_name(const char* begin, const char* end);
};

}}  // namespace
