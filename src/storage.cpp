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
#include <strstream>
#include <cassert>
#include <apr_general.h>
#include <apr_mmap.h>
#include <log4cxx/logger.h>
#include <log4cxx/logmanager.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

namespace Akumuli {

static log4cxx::LoggerPtr s_logger_ = log4cxx::LogManager::getLogger("Akumuli.Storage");

//! Metadata physical page size in bytes
const size_t AKU_METADATA_PAGE_SIZE = 1024*1024;

//! Fixed indexes in metadata page
enum MetadataIndexes {
    CREATION_DATE = 0,
    NUM_PAGES = 1,
    FIRST_INDEX = 2
};

//! Returns specific metadata record from metadata page
static MetadataRecord* get_record(PageHeader* metadata, int index, MetadataRecord::TypeTag required_tag) {
    const Entry* entry = metadata->read_entry(index);
    aku_MemRange data = entry->get_storage();
    MetadataRecord* mdatarec = reinterpret_cast<MetadataRecord*>(data.address);
    if (mdatarec->tag != required_tag) {
        throw std::runtime_error("Storage is damaged");
    }
    return mdatarec;
}

Storage::Storage(const char* file_name)
    : mmap_(file_name)
{
    metadata_ = reinterpret_cast<PageHeader*>(mmap_.get_pointer());

    // Read creation date from metadata page
    auto mdatarec = get_record(metadata_, CREATION_DATE, MetadataRecord::DATE_TIME);
    creation_time_ = mdatarec->time.precise;

    // Read number of pages
    mdatarec = get_record(metadata_, NUM_PAGES, MetadataRecord::INTEGER);
    auto num_pages = static_cast<int>(mdatarec->integer);

    /* Read all pages offsets */
    for (int i = 0; i < num_pages; i++) {
        mdatarec = get_record(metadata_, FIRST_INDEX + i, MetadataRecord::INTEGER);
        auto offset = mdatarec->integer;
        auto index_page = reinterpret_cast<PageHeader*>((char*)mmap_.get_pointer() + offset);
        page_cache_.push_back(index_page);
    }

    // Search for active page
    active_page_ = page_cache_[0];
    uint32_t last_overwrite_count = active_page_->overwrites_count;
    for(PageHeader* page: page_cache_) {
        if (page->overwrites_count == (last_overwrite_count - 1)) {
            active_page_ = page;
            break;
        }
    }
    auto active_it = std::find(page_cache_.begin(), page_cache_.end(), active_page_);
    active_page_index_ = active_it - page_cache_.begin();
}

//! get page by index
PageHeader* Storage::get_index_page(int page_index) {
    return page_cache_[page_index];
}

// Reading

void Storage::find_entry(ParamId param, TimeStamp timestamp) {
    int64_t raw_time = timestamp.precise;
}

// Writing

//! commit changes
void Storage::commit() {
    mmap_.flush();
}

//! write data
int Storage::write(Entry const& entry) {
    // FIXME: this code intentionaly single threaded
    while(true) {
        int status = active_page_->add_entry(entry);
        switch (status) {
        case AKU_WRITE_STATUS_OVERFLOW:
            // select next page in round robin order
            active_page_index_++;
            active_page_ = page_cache_[active_page_index_ % page_cache_.size()];
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
            active_page_index_++;
            active_page_ = page_cache_[active_page_index_ % page_cache_.size()];
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
    std::vector<apr_status_t> results(APR_SUCCESS, targets.size());
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
        auto now = TimeStamp::utc_now();
        root.add_child("creation_time", now);
        root.add_child("num_pages", page_file_names.size());
        for(size_t i = 0; i < page_file_names.size(); i++) {
            boost::property_tree::ptree page_desc;
            page_desc.push_back(std::make_pair("index", i));
            page_desc.push_back(std::make_pair("name", page_file_names[i]));
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


apr_status_t Storage::init_storage(const char* file_name, int num_pages) {
    // calculate list of page-file names
    std::vector<std::string> page_names;
    for (int ix = 0; ix < num_pages; ix++) {
        std::strstream stream;
        stream << file_name << "_" << ix << ".page";
        page_names.push_back(stream.str());
    }

    std::vector<apr_status_t> page_creation_statuses = create_page_files(page_names);
    for(auto status: page_creation_statuses) {
        if (status != APR_SUCCESS) {
            LOG4CXX_ERROR(s_logger_, "Not all pages successfullly created. Cleaning up.");
            return delete_files(page_names, page_creation_statuses);
        }
    }

    std::strstream stream;
    stream << file_name << ".aku";
    return create_metadata_page(file_name, page_names);
}

}
