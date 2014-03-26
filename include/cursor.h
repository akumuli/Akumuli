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
#include "akumuli.h"
#include <boost/coroutine/all.hpp>
#include <boost/bind.hpp>
#include <memory>

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


/** Simple cursor implementation for testing.
  * Stores all values in std::vector.
  */
struct RecordingCursor : InternalCursor {
    std::vector<EntryOffset> offsets;
    bool completed = false;
    enum ErrorCodes {
        NO_ERROR = -1
    };
    int error_code = NO_ERROR;

    virtual void put(Caller&, EntryOffset offset) noexcept;
    virtual void complete(Caller&) noexcept;
    virtual void set_error(Caller&, int error_code) noexcept;
};


//! Simple static buffer cursor
struct BufferedCursor : InternalCursor {
    EntryOffset* offsets_buffer;
    size_t buffer_size;
    size_t count;
    bool completed = false;
    int error_code = AKU_SUCCESS;
    //! C-tor
    BufferedCursor(EntryOffset* buf, size_t size) noexcept;
    virtual void put(Caller&, EntryOffset offset) noexcept;
    virtual void complete(Caller&) noexcept;
    virtual void set_error(Caller&, int error_code) noexcept;
};


/** Data retreival interface that can be used by
 *  code that reads data from akumuli.
 */
struct ExternalCursor {
    //! Read portion of the data to the buffer
    virtual int read(EntryOffset* buf, int buf_len) noexcept = 0;
    //! Check is everything done
    virtual bool is_done() const noexcept = 0;
    //! Check is error occured and (optionally) get the error code
    virtual bool is_error(int* out_error_code_or_null=nullptr) const noexcept = 0;
    //! Finalizer
    virtual void close() noexcept = 0;
};

struct CoroCursor : InternalCursor, ExternalCursor {
    boost::shared_ptr<Coroutine> coroutine_;
    // user owned data
    EntryOffset*    usr_buffer_;        //! User owned buffer for output
    int             usr_buffer_len_;    //! Size of the user owned buffer
    // library owned data
    int             write_index_;       //! Current write position in usr_buffer_
    bool            error_;             //! Error flag
    int             error_code_;        //! Error code
    bool            complete_;          //! Is complete

    CoroCursor() ;

    // External cursor implementation

    virtual int read(EntryOffset* buf, int buf_len) noexcept;

    virtual bool is_done() const noexcept;

    virtual bool is_error(int* out_error_code_or_null=nullptr) const noexcept;

    virtual void close() noexcept;

    // Internal cursor implementation

    void set_error(Caller& caller, int error_code) noexcept;

    void put(Caller& caller, EntryOffset off) noexcept;

    void complete(Caller& caller) noexcept;

    template<class Fn>
    void start(Fn const& fn) {
        coroutine_.reset(new Coroutine(fn));
    }
};
}  // namespace
