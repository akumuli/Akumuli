/**
 * Copyright (c) 2016 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at *
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
#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "akumuli_def.h"
#include "metadatastorage.h"
#include "seriesparser.h"
#include "util.h"

#include "storage_engine/blockstore.h"
#include "storage_engine/nbtree.h"
#include "storage_engine/column_store.h"

#include <boost/thread.hpp>

namespace Akumuli {

class Storage {
    std::shared_ptr<StorageEngine::BlockStore> bstore_;
    std::shared_ptr<StorageEngine::ColumnStore> reg_;
    std::atomic<int> done_;
    boost::barrier close_barrier_;
public:

    Storage(const char* path);

    //! Create new write session
    std::shared_ptr<StorageEngine::WriteSession> create_write_session();

    void debug_print() const;

    /** This method should be called before object destructor.
      * All ingestion sessions should be stopped first.
      */
    void close();

    /** Create empty database from scratch.
      * @param file_name is database name
      * @param metadata_path is a path to metadata storage
      * @param volumes_path is a path to volumes storage
      * @param num_volumes defines how many volumes should be crated
      * @param page_size is a size of the individual page in bytes
      * @return operation status
      */
    static aku_Status new_database( const char     *file_name
                                  , const char     *metadata_path
                                  , const char     *volumes_path
                                  , i32             num_volumes
                                  , u64             page_size);

    /** Remove existing database
      * @param file_name is a database name
      * @param force forces database deletion even if database is not empty
      * @return AKU_SUCCESS on success or AKU_ENOT_PERMITTED if database contains data and `force` is false or
      *         AKU_EACCESS if database there is not enough priveleges to delete the files
      */
    static aku_Status remove_storage(const char* file_name, bool force);
};

}
