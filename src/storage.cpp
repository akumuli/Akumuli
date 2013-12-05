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
#include <apr_general.h>
#include <apr_mmap.h>
#include <log4cxx/logger.h>
#include <log4cxx/logmanager.h>

namespace Akumuli {

Storage::Storage(const char* file_name)
    : mmap_(file_name)
{
    metadata_ = reinterpret_cast<PageHeader*>(mmap_.get_pointer());
    const Entry* entry = metadata_->find_entry(1);
    aku_MemRange data = entry->get_storage();
    MetadataRecord* mdatarec = reinterpret_cast<MetadataRecord*>(data.address);
    if (mdatarec->tag != MetadataRecord::INTEGER) {
        LOG4CXX_ERROR(s_logger_, "Error, metadata[1] must contain index offset, raising 'Storage is damaged' error!");
        throw std::runtime_error("Storage is damaged");
    }
    auto offset = mdatarec->integer;
    index_ = reinterpret_cast<PageHeader*>((char*)mmap_.get_pointer() + offset);
}

//! get page by index
PageHeader* Storage::get_index_page(int page_index) {
    return index_;
}

//! commit changes
void Storage::commit() {
    mmap_.flush();
}

//! write data
void Storage::write(Entry const& entry) {
    index_->add_entry(entry);
}

void Storage::write(Entry2 const& entry) {
    index_->add_entry(entry);
}

log4cxx::LoggerPtr Storage::s_logger_ = log4cxx::LogManager::getLogger("Akumuli.Storage");

}
