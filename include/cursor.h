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

#include <vector>
#include <memory>

#include "akumuli.h"          // for EntryOffset
#include "internal_cursor.h"  // for InternalCursor
#include "page.h"             // for PageHeader, TimeStamp and ParamId

namespace Akumuli {



/** Simple cursor implementation for testing.
  * Stores all values in std::vector.
  */
struct RecordingCursor : InternalCursor {
    std::vector<CursorResult> offsets;
    bool completed = false;
    enum ErrorCodes {
        NO_ERROR = -1
    };
    int error_code = NO_ERROR;

    virtual void put(Caller&, EntryOffset offset, const PageHeader* page) noexcept;
    virtual void complete(Caller&) noexcept;
    virtual void set_error(Caller&, int error_code) noexcept;
};


//! Simple static buffer cursor
struct BufferedCursor : InternalCursor {
    CursorResult* offsets_buffer;
    size_t buffer_size;
    size_t count;
    bool completed = false;
    int error_code = AKU_SUCCESS;
    //! C-tor
    BufferedCursor(CursorResult *buf, size_t size) noexcept;
    virtual void put(Caller&, EntryOffset offset, const PageHeader *page) noexcept;
    virtual void complete(Caller&) noexcept;
    virtual void set_error(Caller&, int error_code) noexcept;
};


/** Data retreival interface that can be used by
 *  code that reads data from akumuli.
 */
struct ExternalCursor {
    //! Read portion of the data to the buffer
    virtual int read(CursorResult* buf, int buf_len) noexcept = 0;
    //! Check is everything done
    virtual bool is_done() const noexcept = 0;
    //! Check is error occured and (optionally) get the error code
    virtual bool is_error(int* out_error_code_or_null=nullptr) const noexcept = 0;
    //! Finalizer
    virtual void close() noexcept = 0;
};


//! Combined cursor interface
struct Cursor : InternalCursor, ExternalCursor {};


struct CoroCursor : Cursor {
    boost::shared_ptr<Coroutine> coroutine_;
    // user owned data
    CursorResult*   usr_buffer_;        //! User owned buffer for output
    int             usr_buffer_len_;    //! Size of the user owned buffer
    // library owned data
    int             write_index_;       //! Current write position in usr_buffer_
    bool            error_;             //! Error flag
    int             error_code_;        //! Error code
    bool            complete_;          //! Is complete

    CoroCursor() ;

    // External cursor implementation

    virtual int read(CursorResult* buf, int buf_len) noexcept;

    virtual bool is_done() const noexcept;

    virtual bool is_error(int* out_error_code_or_null=nullptr) const noexcept;

    virtual void close() noexcept;

    // Internal cursor implementation

    void set_error(Caller& caller, int error_code) noexcept;

    void put(Caller& caller, EntryOffset off, const PageHeader *page) noexcept;

    void complete(Caller& caller) noexcept;

    template<class Fn_1arg_caller>
    void start(Fn_1arg_caller const& fn) {
        coroutine_.reset(new Coroutine(fn));
    }

    template<class Fn_1arg>
    static std::unique_ptr<ExternalCursor> make(Fn_1arg const& fn) {
         std::unique_ptr<CoroCursor> cursor(new CoroCursor());
         cursor->start(fn);
         return std::move(cursor);
    }

    template<class Fn_2arg, class Tobj, class T2nd>
    static std::unique_ptr<ExternalCursor> make(Fn_2arg const& fn, Tobj* obj, T2nd arg2) {
         std::unique_ptr<CoroCursor> cursor(new CoroCursor());
         cursor->start(std::bind(fn, obj, std::placeholders::_1/*caller*/, cursor.get(), arg2));
         return std::move(cursor);
    }

    template<class Fn_3arg, class Tobj, class T2nd, class T3rd>
    static std::unique_ptr<ExternalCursor> make(Fn_3arg const& fn, Tobj* obj, T2nd arg2, T3rd arg3) {
         std::unique_ptr<CoroCursor> cursor(new CoroCursor());
         cursor->start(std::bind(fn, obj, std::placeholders::_1/*caller*/, cursor.get(), arg2, arg3));
         return std::move(cursor);
    }

    template<class Fn_4arg, class Tobj, class T2nd, class T3rd, class T4th>
    static std::unique_ptr<ExternalCursor> make(Fn_4arg const& fn, Tobj* obj, T2nd arg2, T3rd arg3, T4th arg4) {
         std::unique_ptr<CoroCursor> cursor(new CoroCursor());
         cursor->start(std::bind(fn, obj, std::placeholders::_1/*caller*/, cursor.get(), arg2, arg3, arg4));
         return std::move(cursor);
    }
};


/**
 * @brief Fan in cursor.
 * Takes list of cursors and pages and merges
 * results from this cursors in one ordered
 * sequence of events.
 */
class FanInCursor {
    const std::vector<ExternalCursor*>  in_cursors_;
    const int                           direction_;
    InternalCursor*                     out_cursor_;

    void read_impl_(Caller& caller) noexcept;
public:
    /**
     * @brief C-tor
     * @param cursors array of pointer to cursors
     * @param size size of the cursors array
     * @param direction direction of the cursor (forward or backward)
     */
    FanInCursor( InternalCursor* out_cursor
               , ExternalCursor** in_cursors
               , int size
               , int direction) noexcept;

    /** Start new coroutine cursor. (shortcut)
     */
    static std::unique_ptr<CoroCursor>&& start( ExternalCursor** in_cursors
                                              , int size
                                              , int direction) noexcept
    {
        CoroCursor* c = new CoroCursor();
        std::unique_ptr<CoroCursor> cursor(c);
        auto fn = [=](Caller& caller) {
            FanInCursor fcur(c, in_cursors, size, direction);
        };
        cursor->start(fn);
        return std::move(cursor);
    }
};

}  // namespace
