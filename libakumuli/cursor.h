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

std::ostream& operator << (std::ostream& st, aku_Sample res);

class CursorFSM {
    // user data
    aku_Sample *usr_buffer_;        //! User owned buffer for output
    size_t            usr_buffer_len_;    //! Size of the user owned buffer
    // cursor state
    size_t            write_index_;       //! Current write position in usr_buffer_
    bool              error_;             //! Error flag
    int               error_code_;        //! Error code
    bool              complete_;          //! Is complete
    bool              closed_;            //! Used to check that close method was called
public:
    CursorFSM();
    ~CursorFSM();
    // modifiers
    void put(aku_Sample const& result);
    void complete();
    void set_error(int error_code);
    void update_buffer(aku_Sample* buf, size_t buf_len);
    void update_buffer(CursorFSM *other_fsm);
    bool close();
    // accessors
    bool can_put() const;
    bool is_done() const;
    bool get_error(int *error_code) const;
    size_t get_data_len() const;
};


/** Data retreival interface that can be used by
 *  code that reads data from akumuli.
 */
struct ExternalCursor {
    //! Read portion of the data to the buffer
    virtual size_t read(aku_Sample* buf, size_t buf_len) = 0;
    //! Check is everything done
    virtual bool is_done() const = 0;
    //! Check is error occured and (optionally) get the error code
    virtual bool is_error(int* out_error_code_or_null=nullptr) const = 0;
    //! Finalizer
    virtual void close() = 0;

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
    CursorFSM cursor_fsm_;

    // External cursor implementation

    virtual size_t read(aku_Sample* buf, size_t buf_len);

    virtual bool is_done() const;

    virtual bool is_error(int* out_error_code_or_null=nullptr) const;

    virtual void close();

    // Internal cursor implementation

    void set_error(Caller& caller, int error_code);

    bool put(Caller& caller, aku_Sample const& result);

    void complete(Caller& caller);

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
    static std::unique_ptr<ExternalCursor> make(Fn_2arg const& fn, Tobj obj, T2nd const& arg2) {
         std::unique_ptr<CoroCursor> cursor(new CoroCursor());
         cursor->start(std::bind(fn, obj, std::placeholders::_1/*caller*/, cursor.get(), arg2));
         return std::move(cursor);
    }

    template<class Fn_3arg, class Tobj, class T2nd, class T3rd>
    static std::unique_ptr<ExternalCursor> make(Fn_3arg const& fn, Tobj obj, T2nd const& arg2, T3rd const& arg3) {
         std::unique_ptr<CoroCursor> cursor(new CoroCursor());
         cursor->start(std::bind(fn, obj, std::placeholders::_1/*caller*/, cursor.get(), arg2, arg3));
         return std::move(cursor);
    }

    template<class Fn_4arg, class Tobj, class T2nd, class T3rd, class T4th>
    static std::unique_ptr<ExternalCursor> make(Fn_4arg const& fn, Tobj obj, T2nd const& arg2, T3rd const& arg3, T4th const& arg4) {
         std::unique_ptr<CoroCursor> cursor(new CoroCursor());
         cursor->start(std::bind(fn, obj, std::placeholders::_1/*caller*/, cursor.get(), arg2, arg3, arg4));
         return std::move(cursor);
    }
};

typedef std::tuple<aku_Sample, int, int> HeapItem;

struct HeapPred {
    int dir;
    bool operator () (HeapItem const& lhs, HeapItem const& rhs) const {
        bool result = false;
        const aku_Sample& lres = std::get<0>(lhs);
        const aku_Sample& rres = std::get<0>(rhs);
        const auto lkey = std::make_tuple(lres.timestamp, lres.paramid);
        const auto rkey = std::make_tuple(rres.timestamp, rres.paramid);
        if (dir == AKU_CURSOR_DIR_BACKWARD) {
            // Min heap is used
            result = lkey < rkey;
        } else if (dir == AKU_CURSOR_DIR_FORWARD) {
            result = lkey > rkey;
        } else {
            AKU_PANIC("bad direction of the fan-in cursor")
        }
        return result;
    }
};

/**
 * @brief Fan in cursor.
 * Takes list of cursors and pages and merges
 * results from this cursors in one ordered
 * sequence of events.
 */
class StacklessFanInCursorCombinator : ExternalCursor {
    typedef std::vector<HeapItem> Heap;
    const int                           direction_;
    const std::vector<ExternalCursor*>  in_cursors_;
    const HeapPred                      pred_;
    Heap                                heap_;
    CursorFSM                           cursor_fsm_;

    void read_impl_();
    void set_error(int error_code);
    bool put(aku_Sample const& result);
    void complete();
public:
    /**
     * @brief C-tor
     * @param cursors array of pointer to cursors
     * @param size size of the cursors array
     * @param direction direction of the cursor (forward or backward)
     */
    StacklessFanInCursorCombinator( ExternalCursor** in_cursors
                                  , int size
                                  , int direction);

    // ExternalCursor interface
public:
    virtual size_t read(aku_Sample *buf, size_t buf_len);
    virtual bool is_done() const;
    virtual bool is_error(int *out_error_code_or_null) const;
    virtual void close();
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

    void read_impl_(Caller& caller);
public:
    /**
     * @brief C-tor
     * @param cursors array of pointer to cursors
     * @param size size of the cursors array
     * @param direction direction of the cursor (forward or backward)
     */
    FanInCursorCombinator( ExternalCursor** in_cursors
                         , int size
                         , int direction);

    // ExternalCursor interface
public:
    virtual size_t read(aku_Sample *buf, size_t buf_len);
    virtual bool is_done() const;
    virtual bool is_error(int *out_error_code_or_null) const;
    virtual void close();
};

}  // namespace
