/**
 * PRIVATE HEADER
 *
 * Data structures for main memory storage.
 *
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 */

#pragma once

#define BOOST_COROUTINES_BIDIRECT
#include <boost/coroutine/all.hpp>

#include "akumuli.h"

namespace Akumuli {


struct InternalCursor;


typedef boost::coroutines::coroutine< void(InternalCursor*) > Coroutine;
typedef typename Coroutine::caller_type Caller;


/** Interface used by different search procedures
 *  in akumuli. Must be used only inside library.
 */
struct InternalCursor {
    //! Send offset to caller
    virtual void put(Caller&, EntryOffset offset) noexcept = 0;
    virtual void complete(Caller&) noexcept = 0;
    //! Set error and stop execution
    virtual void set_error(Caller&, int error_code) noexcept = 0;
};

}
