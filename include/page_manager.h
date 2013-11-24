/**
 * PRIVATE HEADER
 *
 * Page management API.
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
#include "page.h"

namespace Akumuli {


/** Interface to page manager
 */
struct IPageManager
{
    //! Create new buffer
    virtual PageHeader* make(PageType type) = 0;
    //! Return buffer back
    virtual void recycle(PageHeader* buffer) = 0;
};


/** Buffer manager factory.
 */
struct PageManagersFactory
{
    enum PageType {
        Persistent,  //< Memory mapped file
        Transient //< Memory allocation
    };

    /** Create new page manager of some type.
     * There is two types:
     * - MMF - memory mapped files, `param` must contain path to file.
     * - Memory - memory allocation from OS, `param` can be null. 
     */
    IPageManager* create_new(PageType type, size_t page_size, const char* param);
};

}
