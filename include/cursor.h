/**
 * PRIVATE HEADER
 *
 * Data structures for main memory storage.
 *
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


#pragma once

#include <vector>
#include <memory>

#include "akumuli.h"
#include "internal_cursor.h"
#include "page.h"

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

    virtual void put(Caller&, aku_EntryOffset offset, const PageHeader* page) noexcept;
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
    virtual void put(Caller&, aku_EntryOffset offset, const PageHeader *page) noexcept;
    virtual void complete(Caller&) noexcept;
    virtual void set_error(Caller&, int error_code) noexcept;
};


/** Simple page cursor that write incomming data to
 *  page index directly.
 */
struct DirectPageSyncCursor : InternalCursor {
    int error_code_;
    bool error_is_set_;
    bool completed_;
    //! C-tor
    DirectPageSyncCursor();
    virtual void put(Caller&, aku_EntryOffset offset, const PageHeader *page) noexcept;
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

    virtual ~ExternalCursor() {}
};


//! Combined cursor interface
struct Cursor : InternalCursor, ExternalCursor {};


struct CoroCursorStackAllocator {
    void allocate(boost::coroutines::stack_context& ctx, size_t size) const;
    void deallocate(boost::coroutines::stack_context& ctx) const;
};

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

    void put(Caller& caller, aku_EntryOffset off, const PageHeader *page) noexcept;

    void complete(Caller& caller) noexcept;

    template<class Fn_1arg_caller>
    void start(Fn_1arg_caller const& fn) {
        coroutine_.reset(
                    new Coroutine(
                        fn,
                        boost::coroutines::attributes(AKU_STACK_SIZE),
                        CoroCursorStackAllocator()));
    }

    template<class Fn_1arg>
    static std::unique_ptr<ExternalCursor> make(Fn_1arg const& fn) {
         std::unique_ptr<CoroCursor> cursor(new CoroCursor());
         cursor->start(fn);
         return std::move(cursor);
    }

    template<class Fn_2arg, class Tobj, class T2nd>
    static std::unique_ptr<ExternalCursor> make(Fn_2arg const& fn, Tobj* obj, T2nd const& arg2) {
         std::unique_ptr<CoroCursor> cursor(new CoroCursor());
         cursor->start(std::bind(fn, obj, std::placeholders::_1/*caller*/, cursor.get(), arg2));
         return std::move(cursor);
    }

    template<class Fn_3arg, class Tobj, class T2nd, class T3rd>
    static std::unique_ptr<ExternalCursor> make(Fn_3arg const& fn, Tobj* obj, T2nd const& arg2, T3rd const& arg3) {
         std::unique_ptr<CoroCursor> cursor(new CoroCursor());
         cursor->start(std::bind(fn, obj, std::placeholders::_1/*caller*/, cursor.get(), arg2, arg3));
         return std::move(cursor);
    }

    template<class Fn_4arg, class Tobj, class T2nd, class T3rd, class T4th>
    static std::unique_ptr<ExternalCursor> make(Fn_4arg const& fn, Tobj* obj, T2nd const& arg2, T3rd const& arg3, T4th const& arg4) {
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
class FanInCursorCombinator : ExternalCursor {
    const std::vector<ExternalCursor*>  in_cursors_;
    const int                           direction_;
    CoroCursor                          out_cursor_;

    void read_impl_(Caller& caller) noexcept;
public:
    /**
     * @brief C-tor
     * @param cursors array of pointer to cursors
     * @param size size of the cursors array
     * @param direction direction of the cursor (forward or backward)
     */
    FanInCursorCombinator( ExternalCursor** in_cursors
                         , int size
                         , int direction) noexcept;

    // ExternalCursor interface
public:
    virtual int read(CursorResult *buf, int buf_len) noexcept;
    virtual bool is_done() const noexcept;
    virtual bool is_error(int *out_error_code_or_null) const noexcept;
    virtual void close() noexcept;
};

}  // namespace
