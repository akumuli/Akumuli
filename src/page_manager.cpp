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
#include <stdexcept>
#include <apr.h>

namespace Akumuli {

const int FREE_PAGES_AT_START = 10;


struct TransientBufferManager : IBufferManager
{
    size_t _page_size;       //< page size

    TransientBufferManager(size_t page_size) 
        : _page_size(page_size) 
    {
    }

    PageHeader* _make_new(PageType type) const {
        auto page_data = malloc(_page_size); 
        auto page_header = new (page_data) PageHeader(type, 0, _page_size);
        return page_header;
    }

    void _free_for_real(PageHeader* page) const noexcept {
        page->~PageHeader();
        free((void*)page);
    }

    // Public interface

    virtual PageHeader* make(PageType type) {
        return _make_new(type);
    }

    virtual void recycle(PageHeader* page) {
        _free_for_real(page);
    }
};


//struct PersistentBufferManager : IBuferManager
//{
//}


IBufferManager* BufferManagerFactory::create_new(BufferManagerFactory::BufferType type, 
                                                 size_t page_size, 
                                                 const char* param) {
    if (type == Persistent) {
        throw std::logic_error(
                "BufferManagerFactory::create_new - "
                "PersistentBufferManager not yet supported");
    }
    else if (type == Transient) {
        return new TransientBufferManager(page_size);
    }
    throw std::logic_error("BufferManagerFactory::create_new - Unknown buffer manager type");
}

}
