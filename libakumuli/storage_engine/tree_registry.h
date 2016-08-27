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
 * per database. This registery can be used to create sessions. Session instances
 * should be created per-connection for each connection to operate locally (without
 * synchronization). This code assumes that each connection works with its own
 * set of time-series. If this isn't the case - performance penalty will be introduced.
 */

// Stdlib
#include <unordered_map>
#include <mutex>

// Boost libraries
#include <boost/property_tree/ptree_fwd.hpp>

// Project
#include "akumuli_def.h"
#include "external_cursor.h"
#include "metadatastorage.h"
#include "seriesparser.h"
#include "storage_engine/nbtree.h"

namespace Akumuli {
namespace StorageEngine {

class RegistryEntry {
    mutable std::mutex lock_;
    std::shared_ptr<StorageEngine::NBTreeExtentsList> roots_;
public:

    RegistryEntry(std::unique_ptr<StorageEngine::NBTreeExtentsList>&& nbtree);

    //! Return true if entry is available for acquire.
    bool is_available() const;

    //! Acquire NBTreeExtentsList
    std::tuple<aku_Status, std::shared_ptr<StorageEngine::NBTreeExtentsList>> try_acquire();
};


// Fwd decl.
class Session;


/** Global tree registery.
  * Serve as a central data repository for series metadata and NBTree roots.
  * Client code should create `StreamDispatcher` per connection, each dispatcher
  * should have link to `TreeRegistry`.
  * Instances of this class is thread-safe.
  */
class TreeRegistry : public std::enable_shared_from_this<TreeRegistry> {
    std::shared_ptr<StorageEngine::BlockStore> blockstore_;
    std::unique_ptr<MetadataStorage> metadata_;
    std::unordered_map<aku_ParamId, std::shared_ptr<RegistryEntry>> table_;
    SeriesMatcher global_matcher_;

    //! List of acitve dispatchers
    std::unordered_map<size_t, std::weak_ptr<Session>> active_;
    std::mutex metadata_lock_;
    std::mutex table_lock_;

    //! List of metadata to update
    std::unordered_map<aku_ParamId, std::vector<StorageEngine::LogicAddr>> rescue_points_;

    //! Syncronization for watcher thread
    std::condition_variable cvar_;

public:
    TreeRegistry(std::shared_ptr<StorageEngine::BlockStore> bstore, std::unique_ptr<MetadataStorage>&& meta);

    // No value semantics allowed.
    TreeRegistry(TreeRegistry const&) = delete;
    TreeRegistry(TreeRegistry &&) = delete;
    TreeRegistry& operator = (TreeRegistry const&) = delete;

    //! Match series name. If series with such name doesn't exists - create it.
    aku_Status init_series_id(const char* begin, const char* end, aku_Sample *sample, SeriesMatcher *local_matcher);

    int get_series_name(aku_ParamId id, char* buffer, size_t buffer_size, SeriesMatcher *local_matcher);

    //! Update rescue points list for `id`.
    void update_rescue_points(aku_ParamId id, std::vector<StorageEngine::LogicAddr>&& addrlist);

    //! Write rescue points to persistent storage synchronously.
    void sync_with_metadata_storage();

    //! Waint until some data will be available.
    aku_Status wait_for_sync_request(int timeout_us);

    //! Wait until all sessions will be closed.
    void wait_for_sessions();

    // Dispatchers handling

    //! Create and register new `StreamDispatcher`.
    std::shared_ptr<Session> create_session();

    //! Remove dispatcher from registry.
    void remove_session(Session const& disp);

    //! Broadcast sample to all active dispatchers.
    StorageEngine::NBTreeAppendResult broadcast_sample(const aku_Sample &sample, Session const* source);

    // Registry entry acquisition/release

    //! Acquire nbtree extents list (release should be automatic)
    std::tuple<aku_Status, std::shared_ptr<StorageEngine::NBTreeExtentsList>> try_acquire(aku_ParamId id);

    //! Temporary implementation
    std::vector<aku_ParamId> get_ids(std::string filter);
    std::tuple<aku_Status, std::unique_ptr<NBTreeIterator> > _search(aku_ParamId id, aku_Timestamp begin, aku_Timestamp end, const Session *src);
};


/** Dispatches incoming messages to corresponding NBTreeExtentsList instances.
  * Should be created per writer thread.
  */
class Session : public std::enable_shared_from_this<Session>
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
    Session(std::shared_ptr<TreeRegistry> registry);

    Session(Session const&) = delete;
    Session(Session &&) = delete;
    Session& operator = (Session const&) = delete;

    /** Match series name. If series with such name doesn't exists - create it.
      * This method should be called for each sample to init its `paramid` field.
      */
    aku_Status init_series_id(const char* begin, const char* end, aku_Sample *sample);

    int get_series_name(aku_ParamId id, char* buffer, size_t buffer_size);

    void close();

    //! Write sample
    aku_Status write(const aku_Sample &sample);

    /** Receive broadcast.
      * Should perform write only if registry entry sits in cache.
      * This method should only be called by `TreeRegistry` class.
      * @return true if sample processed, false otherwise.
      */
    std::tuple<bool, StorageEngine::NBTreeAppendResult>  _receive_broadcast(const aku_Sample &sample);

    std::tuple<aku_Status, std::unique_ptr<ExternalCursor>> query(std::string text_query);

    std::tuple<aku_Status, std::unique_ptr<NBTreeIterator>> _search(aku_ParamId id, aku_Timestamp begin, aku_Timestamp end);

    //! Return true if this session owns registry entry with such id.
    bool owns(aku_ParamId id);
};

}}  // namespace
