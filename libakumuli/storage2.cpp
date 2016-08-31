/**
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
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


#include "storage2.h"
#include "util.h"
#include "queryprocessor.h"
#include "log_iface.h"

#include <algorithm>
#include <atomic>
#include <sstream>
#include <cassert>
#include <functional>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>

namespace Akumuli {

// Standalone functions //

/** This function creates metadata file - root of the storage system.
  * This page contains creation date and time, number of pages,
  * all the page file names and they order.
  * @return APR_EINIT on DB error.
  */
static apr_status_t create_metadata_page( const char* file_name
                                        , std::vector<std::string> const& page_file_names)
{
    using namespace std;
    try {
        auto storage = std::make_shared<MetadataStorage>(file_name);

        auto now = apr_time_now();
        char date_time[0x100];
        apr_rfc822_date(date_time, now);

        storage->init_config(date_time);

        std::vector<MetadataStorage::VolumeDesc> desc;
        int ix = 0;
        for(auto str: page_file_names) {
            desc.push_back(std::make_pair(ix++, str));
        }
        storage->init_volumes(desc);

    } catch (std::exception const& err) {
        std::stringstream fmt;
        fmt << "Can't create metadata file " << file_name << ", the error is: " << err.what();
        Logger::msg(AKU_LOG_ERROR, fmt.str().c_str());
        return APR_EGENERAL;
    }
    return APR_SUCCESS;
}


//----------- Storage ----------

Storage::Storage(const char* path)
    : done_{0}
    , close_barrier_(2)
{
    std::unique_ptr<MetadataStorage> meta;
    meta.reset(new MetadataStorage(path));

    std::string metapath;
    std::vector<std::string> volpaths;

    // first volume is a metavolume
    auto volumes = meta->get_volumes();
    for (auto vol: volumes) {
        std::string path;
        int index;
        std::tie(index, path) = vol;
        if (index == 0) {
            metapath = path;
        } else {
            volpaths.push_back(path);
        }
    }

    bstore_ = StorageEngine::FixedSizeFileStorage::open(metapath, volpaths);
    reg_ = std::make_shared<StorageEngine::TreeRegistry>(bstore_, std::move(meta));

    // This thread periodically checks state of the tree registry.
    // It calls `flush` method of the blockstore and then `sync_with_metadata_storage` method
    // if something needs to be synced.
    // This order guarantees that metadata storage always contains correct rescue points and
    // other metadata.
    auto sync_worker = [this]() {
        while(!done_.load()) {
            aku_Status status = reg_->wait_for_sync_request(10000);
            if (status == AKU_SUCCESS) {
                bstore_->flush();
                reg_->sync_with_metadata_storage();
            }
        }
        // Sync remaining data
        aku_Status status = reg_->wait_for_sync_request(0);
        if (status == AKU_SUCCESS) {
            bstore_->flush();
            reg_->sync_with_metadata_storage();
        }
        close_barrier_.wait();
    };
    std::thread sync_worker_thread(sync_worker);
    sync_worker_thread.detach();
}

void Storage::close() {
    // Wait for all ingestion sessions to stop
    reg_->wait_for_sessions();
    done_.store(1);
    close_barrier_.wait();
}

std::shared_ptr<StorageEngine::Session> Storage::create_dispatcher() {
    return reg_->create_session();
}

void Storage::debug_print() const {
    std::cout << "Storage::debug_print" << std::endl;
    std::cout << "...not implemented" << std::endl;
}

aku_Status Storage::new_database( const char     *file_name
                                  , const char     *metadata_path
                                  , const char     *volumes_path
                                  , i32             num_volumes
                                  , u64             page_size)
{
    // Create volumes and metapage
    u32 vol_size = static_cast<u32>(page_size / 4096);

    boost::filesystem::path volpath(volumes_path);
    boost::filesystem::path metpath(metadata_path);
    volpath = boost::filesystem::absolute(volpath);
    metpath = boost::filesystem::absolute(metpath);

    if (!boost::filesystem::exists(volpath)) {
        Logger::msg(AKU_LOG_INFO, std::string(volumes_path) + " doesn't exists, trying to create directory");
        boost::filesystem::create_directories(volpath);
    } else {
        if (!boost::filesystem::is_directory(volpath)) {
            Logger::msg(AKU_LOG_ERROR, std::string(volumes_path) + " is not a directory");
            return AKU_EBAD_ARG;
        }
    }

    if (!boost::filesystem::exists(metpath)) {
        Logger::msg(AKU_LOG_INFO, std::string(metadata_path) + " doesn't exists, trying to create directory");
        boost::filesystem::create_directories(metpath);
    } else {
        if (!boost::filesystem::is_directory(metpath)) {
            Logger::msg(AKU_LOG_ERROR, std::string(metadata_path) + " is not a directory");
            return AKU_EBAD_ARG;
        }
    }

    std::vector<std::tuple<u32, std::string>> paths;
    for (i32 i = 0; i < num_volumes; i++) {
        std::string basename = std::string(file_name) + "_" + std::to_string(i) + ".vol";
        boost::filesystem::path p = volpath / basename;
        paths.push_back(std::make_tuple(vol_size, p.string()));
    }
    // Volumes meta-page
    std::string basename = std::string(file_name) + ".metavol";
    boost::filesystem::path volmpage = volpath / basename;

    StorageEngine::FixedSizeFileStorage::create(volmpage.string(), paths);

    // Create sqlite database for metadata
    std::vector<std::string> mpaths;
    mpaths.push_back(volmpage.string());
    for (auto p: paths) {
        mpaths.push_back(std::get<1>(p));
    }
    std::string sqlitebname = std::string(file_name) + ".akumuli";
    boost::filesystem::path sqlitepath = metpath / sqlitebname;
    create_metadata_page(sqlitepath.c_str(), mpaths);
    return AKU_SUCCESS;
}

aku_Status Storage::remove_storage(const char* file_name, bool force) {
    auto meta = std::make_shared<MetadataStorage>(file_name);
    auto volumes = meta->get_volumes();
    std::vector<std::string> volume_names(volumes.size() - 1, "");
    // First volume is meta-page
    std::string meta_file;
    for(auto it: volumes) {
        if (it.first == 0) {
            meta_file = it.second;
        } else {
            volume_names.at(static_cast<size_t>(it.first)) = it.second;
        }
    }
    if (!force) {
        // Check whether or not database is empty
        auto fstore = StorageEngine::FixedSizeFileStorage::open(meta_file, volume_names);
        auto stats = fstore->get_stats();
        if (stats.nblocks != 0) {
            // DB is not empty
            return AKU_ENOT_PERMITTED;
        }
    }
    meta.reset();

    // Check access rights
    auto check_access = [](std::string const& p) {
        auto status = boost::filesystem::status(p);
        auto perms = status.permissions();
        if ((perms & boost::filesystem::owner_write) == 0) {
            return AKU_EACCESS;
        }
        return AKU_SUCCESS;
    };
    volume_names.push_back(meta_file);
    volume_names.push_back(file_name);
    std::vector<aku_Status> statuses;
    std::transform(volume_names.begin(), volume_names.end(), std::back_inserter(statuses), check_access);
    auto comb_status = [](aku_Status lhs, aku_Status rhs) {
        return lhs == AKU_SUCCESS ? rhs : lhs;
    };
    auto status = std::accumulate(statuses.begin(), statuses.end(), AKU_SUCCESS, comb_status);
    if (status != AKU_SUCCESS) {
        return AKU_EACCESS;
    }

    // Actual deletion starts here!
    auto delete_file = [](std::string const& fname) {
        if (!boost::filesystem::remove(fname)) {
            Logger::msg(AKU_LOG_ERROR, fname + " file is not deleted!");
        } else {
            Logger::msg(AKU_LOG_INFO, fname + " was deleted.");
        }
    };

    std::for_each(volume_names.begin(), volume_names.end(), delete_file);

    return AKU_SUCCESS;
}

}
