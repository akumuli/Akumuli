/*
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 */


#include "buffer_manager.h"
#include <stdexcept>

namespace IO {

struct TransientBufferManager : IBufferManager
{
    size_t _page_size;

    TransientBufferManager(size_t page_size) 
        : _page_size(page_size) {
    }

    virtual IOBuffer make() {
        return {
            malloc(_page_size), 
            _page_size
        };
    }

    virtual void recycle(IOBuffer buffer) {
        free(buffer.address);
    }
};

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
