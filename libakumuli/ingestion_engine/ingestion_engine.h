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
namespace Ingress {

class RegistryEntry {
    mutable std::mutex lock_;
    std::shared_ptr<StorageEngine::NBTreeExtentsList> roots_;
public:

    RegistryEntry(std::unique_ptr<StorageEngine::NBTreeExtentsList>&& nbtree);

    //! Return true if entry is available for acquire.
    bool is_available() const;

    //! Acquire NBTreeExtentsList
    std::shared_ptr<StorageEngine::NBTreeExtentsList> try_acquire();
};


// Fwd decl.
class StreamDispatcher;


/** Global tree registery.
  * Serve as a central data repository for series metadata and NBTree roots.
  * Client code should create `StreamDispatcher` per connection, each dispatcher
  * should have link to `TreeRegistry`.
  * Instances of this class is thread-safe.
  */
class TreeRegistry : public std::enable_shared_from_this<TreeRegistry> {
    std::unique_ptr<MetadataStorage> metadata_;
    std::unordered_map<aku_ParamId, std::shared_ptr<RegistryEntry>> table_;
    SeriesMatcher global_matcher_;
    //! List of acitve dispatchers
    std::unordered_map<size_t, std::weak_ptr<StreamDispatcher>> active_;
    std::mutex metadata_lock_;
    std::mutex table_lock_;

public:
    TreeRegistry(std::unique_ptr<MetadataStorage>&& meta);

    // No value semantics allowed.
    TreeRegistry(TreeRegistry const&) = delete;
    TreeRegistry(TreeRegistry &&) = delete;
    TreeRegistry& operator = (TreeRegistry const&) = delete;

    //! Match series name. If series with such name doesn't exists - create it.
    aku_Status init_series_id(const char* begin, const char* end, aku_Sample *sample, SeriesMatcher *local_matcher);

    // Dispatchers handling

    //! Create and register new `StreamDispatcher`.
    std::shared_ptr<StreamDispatcher> create_dispatcher();

    //! Remove dispatcher from registry.
    void remove_dispatcher(StreamDispatcher const& disp);

    //! Broadcast sample to all active dispatchers.
    void broadcast_sample(aku_Sample const* sample);

    // Registry entry acquisition/release

    //! Acquire nbtree extents list (release should be automatic)
    std::shared_ptr<StorageEngine::NBTreeExtentsList> try_acquire(aku_ParamId id);
};


/** Dispatches incoming messages to corresponding NBTreeExtentsList instances.
  * Should be created per writer thread.
  */
class StreamDispatcher : public std::enable_shared_from_this<StreamDispatcher>
{
    //! Link to global registry.
    std::weak_ptr<TreeRegistry> registry_;
    //! Local registry cache.
    std::unordered_map<aku_ParamId, std::shared_ptr<StorageEngine::NBTreeExtentsList>> cache_;
    //! Local series matcher (with cached global data).
    SeriesMatcher local_matcher_;
    //! This mutex shouldn't be contended during normal operation.
    std::mutex lock_;
public:
    //! C-tor. Shouldn't be called directly.
    StreamDispatcher(std::shared_ptr<TreeRegistry> registry);

    StreamDispatcher(StreamDispatcher const&) = delete;
    StreamDispatcher(StreamDispatcher &&) = delete;
    StreamDispatcher& operator = (StreamDispatcher const&) = delete;

    /** Match series name. If series with such name doesn't exists - create it.
      * This method should be called for each sample to init its `paramid` field.
      */
    aku_Status init_series_id(const char* begin, const char* end, aku_Sample *sample);

    void close();

    //! Write sample
    aku_Status write(aku_Sample const* sample);

    /** Receive broadcast.
      * Should perform write only if registry entry sits in cache.
      * This method should only be called by `TreeRegistry` class.
      * @return true if sample processed, false otherwise.
      */
    bool _receive_broadcast(aku_Sample const* sample);
};

}}  // namespace
