/**
 * PRIVATE HEADER
 *
 * Buffer management API.
 *
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 */


#pragma once
#include <cstddef>

namespace IO
{

struct IOBuffer
{
    void* address;
    size_t length;
};


/** Interface to buffer manager
 */
struct IBufferManager
{
    //! Create new buffer
    virtual IOBuffer make() = 0;
    //! Return buffer back
    virtual void recycle(IOBuffer buffer) = 0;
};


/** Buffer manager factory.
 */
struct BufferManagerFactory
{
    enum BufferType {
        Persistent,  //< Memory mapped file
        Transient //< Memory allocation
    };

    /** Create new buffer manager of some type.
     * There is two types:
     * - MMF - memory mapped files, `param` must contain path to file.
     * - Memory - memory allocation from OS, `param` can be null. 
     */
    IBufferManager* create_new(BufferType type, size_t page_size, const char* param);
};

}
