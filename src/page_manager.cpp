/*
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 */


#include "page_manager.h"
#include "util.h"
#include <stdexcept>
#include <apr_general.h>
#include <apr_mmap.h>
#include <log4cxx/logger.h>
#include <log4cxx/logmanager.h>

namespace Akumuli {

const int FREE_PAGES_AT_START = 10;


struct TransientPageManager : IPageManager
{
    size_t _page_size;

    TransientPageManager(size_t page_size)
        : _page_size(page_size) 
    {
    }

    PageHeader* _make_new(PageType type) const {
        auto page_data = malloc(_page_size); 
        auto page_header = new (page_data) PageHeader(type, 0, _page_size);
        return page_header;
    }

    void free_for_real(PageHeader* page) const noexcept {
        page->~PageHeader();
        free((void*)page);
    }

    // Public interface

    virtual PageHeader* make(PageType type) {
        return _make_new(type);
    }

    virtual void recycle(PageHeader* page) {
        free_for_real(page);
    }
};


struct PersistentPageManager : IPageManager
{
    MemoryMappedFile mmap_;
    static log4cxx::LoggerPtr s_logger_;

    PersistentPageManager(const char* file_name) : mmap_(file_name) {
    }

    // Public interface

    virtual int allocate_space(PageType type, size_t size) {
        // new regions appended to the end of file
    }

    //! get page by index
    virtual PageHeader* get_page(int page_index) = 0;
    //! commit changes
    virtual void commit() = 0;
};

log4cxx::LoggerPtr PersistentPageManager::s_logger_ = log4cxx::LogManager::getLogger("PersistentPageManager");


IPageManager* PageManagersFactory::create_new(PageManagersFactory::PageType type,
                                                 size_t page_size,
                                                 const char* param) {
    if (type == Persistent) {
        // FIXME: probably, PersistentPageManager is not page manager at all.
        // By the way - we need to create files from time to timie.
        //return new PersistentPageManager(param);
    }
    else if (type == Transient) {
        //return new TransientPageManager(page_size);
    }
    throw std::logic_error("BufferManagerFactory::create_new - Unknown buffer manager type");
}

}
