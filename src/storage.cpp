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
#include <cassert>
#include <apr_general.h>
#include <apr_mmap.h>
#include <log4cxx/logger.h>
#include <log4cxx/logmanager.h>

namespace Akumuli {

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
    const Entry* entry = metadata->find_entry(index);
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
            active_page_ = page_cache_[active_page_index_];
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
            active_page_ = page_cache_[active_page_index_];
            break;
        default:
            return status;
        };
    }
}

apr_status_t Storage::create_storage(const char* file_name, int num_pages) {
    apr_status_t status;
    int success_count = 0;
    apr_pool_t* mem_pool = NULL;
    apr_file_t* file = NULL;
    int64_t size = AKU_METADATA_PAGE_SIZE + num_pages*AKU_MAX_PAGE_SIZE;

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
        LOG4CXX_ERROR(s_logger_,  "Can't create storage, error " << error_message << " on step " << success_count);
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

apr_status_t Storage::init_storage(const char* file_name) {
    try {
        MemoryMappedFile mfile(file_name);
        const int64_t file_size = mfile.get_size();

        // Calculate number of segments
        const int full_pages = file_size / AKU_MAX_PAGE_SIZE;
        const int truncated = file_size % AKU_MAX_PAGE_SIZE;
        if (truncated != AKU_METADATA_PAGE_SIZE) {
            LOG4CXX_ERROR(s_logger_, "Invalid file");
            return APR_EGENERAL;
        }

        // Create meta page
        // TODO: make it variable length
        auto meta_ptr = mfile.get_pointer();
        auto meta_page = new (meta_ptr) PageHeader(PageType::Metadata, 0, AKU_METADATA_PAGE_SIZE, 0u);

        // Add creation date
        const int BUF_SIZE = 128;
        char buffer[BUF_SIZE];
        auto entry_size = Entry::get_size(sizeof(MetadataRecord));
        assert(BUF_SIZE >= entry_size);
        auto now = TimeStamp::utc_now();
        auto entry = new ((void*)buffer) Entry(0, now, entry_size);
        auto mem = entry->get_storage();
        auto mrec = new (mem.address) MetadataRecord(now);
        meta_page->add_entry(*entry);

        // Add number of pages
        mrec->tag = MetadataRecord::TypeTag::INTEGER;
        mrec->integer = full_pages;
        meta_page->add_entry(*entry);

        for (int i = 0; i < full_pages; i++)
        {
            int64_t page_offset = AKU_METADATA_PAGE_SIZE + AKU_MAX_PAGE_SIZE*i;
            // Add index pages offset to metadata
            mrec->tag = MetadataRecord::TypeTag::INTEGER;
            mrec->integer = page_offset;
            meta_page->add_entry(*entry);

            // Create index page
            auto index_ptr = (void*)((char*)meta_ptr + page_offset);
            auto index_page = new (index_ptr) PageHeader(PageType::Index, 0, AKU_MAX_PAGE_SIZE, (uint32_t)i);

            // Activate the first page
            if (i == 0) {
                index_page->clear();
            }
        }

        return mfile.flush();
    }
    catch(AprException const& err) {
        return err.status;
    }
}

log4cxx::LoggerPtr Storage::s_logger_ = log4cxx::LogManager::getLogger("Akumuli.Storage");

}
