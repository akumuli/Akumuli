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

#include <memory>
#include <vector>

#include "akumuli.h"
#include "internal_cursor.h"
#include "external_cursor.h"
#include "page.h"

namespace Akumuli {

std::ostream& operator<<(std::ostream& st, aku_Sample res);

class CursorFSM {
    // user data
    void*  usr_buffer_;      //! User owned buffer for output
    size_t usr_buffer_len_;  //! Size of the user owned buffer
    // cursor state
    size_t     write_offset_;  //! Current write position in usr_buffer_
    bool       error_;         //! Error flag
    aku_Status error_code_;    //! Error code
    bool       complete_;      //! Is complete
    bool       closed_;        //! Used to check that close method was called
public:
    CursorFSM();
    ~CursorFSM();
    // modifiers
    void put(aku_Sample const& result);
    void complete();
    void set_error(aku_Status error_code);
    void update_buffer(void* buf, size_t buf_len);
    void update_buffer(CursorFSM* other_fsm);
    bool close();
    // accessors
    bool can_put(int size) const;
    bool is_done() const;
    bool get_error(aku_Status* error_code) const;
    size_t get_data_len() const;
};



//! Combined cursor interface
struct Cursor : InternalCursor, ExternalCursor {};


struct CoroCursorStackAllocator {
    void allocate(boost::coroutines::stack_context& ctx, size_t size) const;
    void deallocate(boost::coroutines::stack_context& ctx) const;
};

struct CoroCursor : Cursor {
    boost::shared_ptr<Coroutine> coroutine_;
    CursorFSM                    cursor_fsm_;

    // External cursor implementation

    virtual u32 read(void* buffer, u32 buffer_size);

    virtual bool is_done() const;

    virtual bool is_error(aku_Status* out_error_code_or_null = nullptr) const;

    virtual void close();

    // Internal cursor implementation

    void set_error(Caller& caller, aku_Status error_code);

    bool put(Caller& caller, aku_Sample const& result);

    void complete(Caller& caller);

    template <class Fn_1arg_caller> void start(Fn_1arg_caller const& fn) {
        coroutine_.reset(new Coroutine(fn, boost::coroutines::attributes(AKU_STACK_SIZE),
                                       CoroCursorStackAllocator()));
    }

    template <class Fn_1arg> static std::unique_ptr<ExternalCursor> make(Fn_1arg const& fn) {
        std::unique_ptr<CoroCursor> cursor(new CoroCursor());
        cursor->start(fn);
        return std::move(cursor);
    }

    template <class Fn_2arg, class Tobj, class T2nd>
    static std::unique_ptr<ExternalCursor> make(Fn_2arg const& fn, Tobj obj, T2nd const& arg2) {
        std::unique_ptr<CoroCursor> cursor(new CoroCursor());
        cursor->start(std::bind(fn, obj, std::placeholders::_1 /*caller*/, cursor.get(), arg2));
        return std::move(cursor);
    }

    template <class Fn_3arg, class Tobj, class T2nd, class T3rd>
    static std::unique_ptr<ExternalCursor> make(Fn_3arg const& fn, Tobj obj, T2nd const& arg2,
                                                T3rd const& arg3) {
        std::unique_ptr<CoroCursor> cursor(new CoroCursor());
        cursor->start(
            std::bind(fn, obj, std::placeholders::_1 /*caller*/, cursor.get(), arg2, arg3));
        return std::move(cursor);
    }

    template <class Fn_4arg, class Tobj, class T2nd, class T3rd, class T4th>
    static std::unique_ptr<ExternalCursor> make(Fn_4arg const& fn, Tobj obj, T2nd const& arg2,
                                                T3rd const& arg3, T4th const& arg4) {
        std::unique_ptr<CoroCursor> cursor(new CoroCursor());
        cursor->start(
            std::bind(fn, obj, std::placeholders::_1 /*caller*/, cursor.get(), arg2, arg3, arg4));
        return std::move(cursor);
    }
};

}  // namespace
