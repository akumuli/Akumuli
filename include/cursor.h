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
#include "page.h"

namespace Akumuli {

struct BasicCursor {
    //! Send offset to caller
    virtual void put(EntryOffset offset) noexcept = 0;
    virtual void complete() noexcept = 0;
};


/** Simple cursor implementation for testing.
  * Stores all values in std::vector.
  */
struct RecordingCursor : BasicCursor {
    std::vector<EntryOffset> offsets;
    bool completed = false;
    virtual void put(EntryOffset offset) noexcept;
    virtual void complete() noexcept;
};

}  // namespace
