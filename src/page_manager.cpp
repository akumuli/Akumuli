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
    apr_pool_t* mem_pool_;  //< local memory pool
    apr_mmap_t *mmap_;
    apr_file_t *fp_;
    apr_finfo_t finfo_;
    static log4cxx::LoggerPtr s_logger_;

    PersistentPageManager(const char* file_name) {
        AprStatusChecker status;
        try {
            status = apr_pool_create(&mem_pool_, NULL);
            status = apr_file_open(&fp_, file_name, APR_WRITE|APR_READ, APR_OS_DEFAULT, mem_pool_);
            status = apr_file_info_get(&finfo_, APR_FINFO_SIZE, fp_);
            status = apr_mmap_create(&mmap_, fp_, 0, finfo_.size, APR_MMAP_WRITE|APR_MMAP_READ, mem_pool_);
        }
        catch(AprException const& err) {
            free_resources(status.count);
            LOG4CXX_ERROR(s_logger_, "Can't create PersistentPageManager, error " << err << " on step " << status.count);
            throw;
        }
    }

    /* Why not std::unique_ptr with custom deleter?
     * To make finalization order explicit and prevent null-reference errors in
     * APR finalizers. Resource finalizers in APR can't handle null pointers,
     * so we will need to wrap each `close` or `destroy` or whatsever
     * function to be able to pass it to unique_ptr c-tor as deleter.
     */

    void free_resources(int cnt)
    {
        switch(cnt)
        {
        default:
        case 4:
            apr_mmap_delete(mmap_);
        case 3:
        case 2:
            apr_file_close(fp_);
        case 1:
            apr_pool_destroy(mem_pool_);
        };
    }

    ~PersistentPageManager() {
        free_resources(4);
    }

    // Public interface

    virtual PageHeader* make(PageType type) {
        throw std::logic_error("not implemented");
    }

    virtual void recycle(PageHeader* page) {
        throw std::logic_error("not implemented");
    }
};

log4cxx::LoggerPtr PersistentPageManager::s_logger_ = log4cxx::LogManager::getLogger("PersistentPageManager");


IPageManager* PageManagersFactory::create_new(PageManagersFactory::PageType type,
                                                 size_t page_size, 
                                                 const char* param) {
    if (type == Persistent) {
        // FIXME: probably, PersistentPageManager is not page manager at all.
        // By the way - we need to create files from time to timie.
        return new PersistentPageManager(param);
    }
    else if (type == Transient) {
        return new TransientPageManager(page_size);
    }
    throw std::logic_error("BufferManagerFactory::create_new - Unknown buffer manager type");
}

}
