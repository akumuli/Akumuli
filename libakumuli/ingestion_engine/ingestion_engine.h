/**
 * Copyright (c) 2016 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#pragma once

/* In general this is a tree-roots collection combined with series name parser
 * and series registery (backed by sqlite). One TreeRegistery should be created
 * per database. This registery can be used to create StreamDispatcher. The
 * StreamDispatcher instances should be created per-connection for each connection
 * to operate locally (without synchronization). This code assumes that each connection
 * ingests its own set of time-series. If this isn't the case - performance penalty
 * will be introduced.
 */

// Stdlib
#include <unordered_map>
#include <mutex>

// Project
#include "akumuli_def.h"
#include "metadatastorage.h"
#include "seriesparser.h"
// Project.storage_engine
#include "storage_engine/nbtree.h"

namespace Akumuli {
namespace DataIngestion {

class RegistryEntry {
    std::unique_ptr<StorageEngine::NBTreeExtentsList> roots_;
public:
};


// Fwd decl.
class StreamDispatcher;


class TreeRegistry {
    std::unique_ptr<MetadataStorage> metadata_;
    std::unordered_map<aku_ParamId, std::shared_ptr<RegistryEntry>> table_;
    SeriesMatcher global_matcher_;
    std::mutex metadata_lock_;
    std::mutex table_lock_;
public:
    TreeRegistry(std::unique_ptr<MetadataStorage>&& meta);

    //! Match series name. If series with such name doesn't exists - create it.
    aku_Status init_series_id(const char* begin, const char* end, aku_Sample *sample);
};


/** Dispatches incoming messages to corresponding NBTreeExtentsList instances.
  * Should be created per writer thread.
  */
class StreamDispatcher
{
    std::weak_ptr<TreeRegistry> registry_;
    SeriesMatcher local_matcher_;
public:
    StreamDispatcher(std::shared_ptr<TreeRegistry> registry);

    //! Match series name. If series with such name doesn't exists - create it.
    aku_Status init_series_id(const char* begin, const char* end, aku_Sample *sample);
};

}}  // namespace
