/*
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 */


#include "storage.h"
#include "util.h"
#include <stdexcept>
#include <algorithm>
#include <atomic>
#include <sstream>
#include <cassert>
#include <apr_general.h>
#include <apr_mmap.h>
#include <log4cxx/logger.h>
#include <log4cxx/logmanager.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

namespace Akumuli {

static log4cxx::LoggerPtr s_logger_ = log4cxx::LogManager::getLogger("Akumuli.Storage");

//----------------------------------Volume----------------------------------------------

Volume::Volume(const char* file_name)
    : mmap_(file_name)
{
    mmap_.throw_if_bad();  // panic if can't mmap volume
    page_ = reinterpret_cast<PageHeader*>(mmap_.get_pointer());
}

PageHeader* Volume::get_page() const noexcept {
    return page_;
}

//----------------------------------Storage---------------------------------------------

Storage::Storage(const char* file_name)
{
    // 1. Read json file
    boost::property_tree::ptree ptree;
    // NOTE: there is a known bug in boost 1.49 - https://svn.boost.org/trac/boost/ticket/6785
    // FIX: sed -i -e 's/std::make_pair(c.name, Str(b, e))/std::make_pair(c.name, Ptree(Str(b, e)))/' json_parser_read.hpp
    boost::property_tree::json_parser::read_json(file_name, ptree);

    // 2. Read volumes
}


// Reading

void Storage::find_entry(ParamId param, TimeStamp timestamp) {
    int64_t raw_time = timestamp.precise;
}

// Writing

//! commit changes
void Storage::commit() {
    // TODO: volume->flush()
}

//! write data
int Storage::write(Entry const& entry) {
    // FIXME: this code intentionaly single threaded
    while(true) {
        int status = active_page_->add_entry(entry);
        switch (status) {
        case AKU_WRITE_STATUS_OVERFLOW:
            // select next page in round robin order
            active_volume_index_++;
            active_volume_ = volumes_[active_volume_index_ % volumes_.size()];
            active_page_ = active_volume_->get_page();
            // TODO: reallocate space on disc for this volume
            active_page_->clear();
            break;
        default:
            return status;
        };
    }
}

int Storage::write(Entry2 const& entry) {
    // FIXME: this code intentionaly left single threaded
    while(true) {
        int status = active_page_->add_entry(entry);
        switch (status) {
        case AKU_WRITE_STATUS_OVERFLOW:
            // select next page in round robin order
            active_volume_index_++;
            active_volume_ = volumes_[active_volume_index_ % volumes_.size()];
            active_page_ = active_volume_->get_page();
            // TODO: reallocate space on disc for this volume
            active_page_->clear();
            break;
        default:
            return status;
        };
    }
}


/** This function creates file with specified size
  */
static apr_status_t create_file(const char* file_name, uint64_t size) {
    apr_status_t status;
    int success_count = 0;
    apr_pool_t* mem_pool = NULL;
    apr_file_t* file = NULL;

    status = apr_pool_create(&mem_pool, NULL);
    if (status == APR_SUCCESS) {
        success_count++;

        // Create new file
        status = apr_file_open(&file, file_name, APR_CREATE|APR_WRITE, APR_OS_DEFAULT, mem_pool);
        if (status == APR_SUCCESS) {
            success_count++;

            // Truncate file
            status = apr_file_trunc(file, size);
            if (status == APR_SUCCESS)
                success_count++;
        }
    }

    if (status != APR_SUCCESS) {
        char error_message[0x100];
        apr_strerror(status, error_message, 0x100);
        LOG4CXX_ERROR(s_logger_,  "Can't create file, error " << error_message << " on step " << success_count);
    }

    switch(success_count) {
    case 3:
    case 2:
        status = apr_file_close(file);
    case 1:
        apr_pool_destroy(mem_pool);
    case 0:
        // even apr pool is not created
        break;
    }
    return status;
}


/** This function creates one of the page files with specified
  * name and index.
  */
static apr_status_t create_page_file(const char* file_name, uint32_t page_index) {
    apr_status_t status;
    int64_t size = AKU_MAX_PAGE_SIZE;

    status = create_file(file_name, size);
    if (status != APR_SUCCESS) {
        LOG4CXX_ERROR(s_logger_, "Can't create page file " << file_name);
        return status;
    }

    MemoryMappedFile mfile(file_name);
    if (mfile.is_bad())
        return mfile.status_code();

    // Create index page
    auto index_ptr = mfile.get_pointer();
    auto index_page = new (index_ptr) PageHeader(PageType::Index, 0, AKU_MAX_PAGE_SIZE, page_index);

    // FIXME: revisit - is it actually needed?
    // Activate the first page
    if (page_index == 0) {
        index_page->clear();
    }
    return status;
}

/** Create page files, return list of statuses.
  */
static std::vector<apr_status_t> create_page_files(std::vector<std::string> const& targets) {
    std::vector<apr_status_t> results(targets.size(), APR_SUCCESS);
    for (size_t ix = 0; ix < targets.size(); ix++) {
        apr_status_t res = create_page_file(targets[ix].c_str(), ix);
        results[ix] = res;
    }
    return results;
}

static std::vector<apr_status_t> delete_files(const std::vector<std::string>& targets, const std::vector<apr_status_t>& statuses) {
    if (targets.size() != statuses.size()) {
        throw std::logic_error("Sizes of targets and statuses doesn't match");
    }
    apr_pool_t* mem_pool = NULL;
    int op_count = 0;
    apr_status_t status = apr_pool_create(&mem_pool, NULL);
    std::vector<apr_status_t> results;
    if (status == APR_SUCCESS) {
        op_count++;
        for(auto ix = 0; ix < targets.size(); ix++) {
            const std::string& target = targets[ix];
            if (statuses[ix] == APR_SUCCESS) {
                LOG4CXX_INFO(s_logger_, "Removing " << target);
                status = apr_file_remove(target.c_str(), mem_pool);
                results.push_back(status);
                if (status != APR_SUCCESS) {
                    char error_message[1024];
                    apr_strerror(status, error_message, 1024);
                    LOG4CXX_ERROR(s_logger_, "Error [" << error_message << "] while deleting a file " << target);
                }
            }
            else {
                LOG4CXX_INFO(s_logger_, "Target " << target << " doesn't need to be removed");
            }
        }
    }
    if (op_count) {
        apr_pool_destroy(mem_pool);
    }
    return results;
}


/** This function creates metadata file - root of the storage system.
  * This page contains creation date and time, number of pages,
  * all the page file names and they order.
  */
static apr_status_t create_metadata_page( const char* file_name
                                        , std::vector<std::string> const& page_file_names)
{
    try {
        boost::property_tree::ptree root;
        auto now = apr_time_now();
        char date_time[0x100];
        apr_rfc822_date(date_time, now);
        root.add("creation_time", date_time);
        root.add("num_pages", page_file_names.size());
        for(size_t i = 0; i < page_file_names.size(); i++) {
            boost::property_tree::ptree page_desc;
            page_desc.add("index", i);
            page_desc.add("name", page_file_names[i]);
            root.add_child("pages", page_desc);
        }
        boost::property_tree::json_parser::write_json(file_name, root);
    }
    catch(const std::exception& err) {
        LOG4CXX_ERROR(s_logger_, "Can't generate JSON file " << file_name <<
                                 ", the error is: " << err.what());
        return APR_EGENERAL;
    }
    return APR_SUCCESS;
}


apr_status_t Storage::new_storage( const char* 	file_name
                                 , const char* 	metadata_path
                                 , const char* 	volumes_path
                                 , int          num_pages
                                 )
{
    apr_pool_t* mempool;
    apr_status_t status = apr_pool_create(&mempool, NULL);
    if (status != APR_SUCCESS)
        return status;

    // calculate list of page-file names
    std::vector<std::string> page_names;
    for (int ix = 0; ix < num_pages; ix++) {
        std::stringstream stream;
        stream << file_name << "_" << ix << ".volume";
        char* path = nullptr;
        std::string volume_file_name = stream.str();
        status = apr_filepath_merge(&path, volumes_path, volume_file_name.c_str(), APR_FILEPATH_NATIVE, mempool);
        if (status != APR_SUCCESS) {
            auto error_message = apr_error_message(status);
            LOG4CXX_ERROR(s_logger_, "Invalid volumes path: " << error_message);
            apr_pool_destroy(mempool);
            throw AprException(status, error_message.c_str());
        }
        page_names.push_back(path);
    }

    apr_pool_clear(mempool);

    std::vector<apr_status_t> page_creation_statuses = create_page_files(page_names);
    for(auto creation_status: page_creation_statuses) {
        if (creation_status != APR_SUCCESS) {
            LOG4CXX_ERROR(s_logger_, "Not all pages successfullly created. Cleaning up.");
            apr_pool_destroy(mempool);
            delete_files(page_names, page_creation_statuses);
            return creation_status;
        }
    }

    std::stringstream stream;
    stream << file_name << ".akumuli";
    char* path = nullptr;
    std::string metadata_file_name = stream.str();
    status = apr_filepath_merge(&path, metadata_path, metadata_file_name.c_str(), APR_FILEPATH_NATIVE, mempool);
    if (status != APR_SUCCESS) {
        auto error_message = apr_error_message(status);
        LOG4CXX_ERROR(s_logger_, "Invalid metadata path: " << error_message);
        apr_pool_destroy(mempool);
        throw AprException(status, error_message.c_str());
    }
    status = create_metadata_page(path, page_names);
    apr_pool_destroy(mempool);
    return status;
}

}
