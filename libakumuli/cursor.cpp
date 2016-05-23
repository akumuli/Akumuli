/**
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

#include "cursor.h"
#include "search.h"

#include <iostream>

#include <algorithm>
#include <boost/crc.hpp>


namespace Akumuli {

// CursorFSM

CursorFSM::CursorFSM()
    : usr_buffer_(nullptr)
    , usr_buffer_len_(0)
    , write_offset_(0)
    , error_(false)
    , error_code_(AKU_SUCCESS)
    , complete_(false)
    , closed_(false)
{
}

CursorFSM::~CursorFSM() {
#ifdef DEBUG
    if (!closed_) {
        std::cout << "Warning in " << __FUNCTION__ << " - cursor isn't closed" << std::endl;
    }
#endif
}

void CursorFSM::update_buffer(void* buf, size_t buf_len) {
    usr_buffer_ = buf;
    usr_buffer_len_ = buf_len;
    write_offset_ = 0;
}

void CursorFSM::update_buffer(CursorFSM *other_fsm) {
    usr_buffer_ = other_fsm->usr_buffer_;
    usr_buffer_len_ = other_fsm->usr_buffer_len_;
    write_offset_ = other_fsm->write_offset_;
}

bool CursorFSM::can_put(int size) const {
    return usr_buffer_ != nullptr && (write_offset_ + size) < usr_buffer_len_;
}

void CursorFSM::put(const aku_Sample &result) {
    auto len = std::max(result.payload.size, (u16)sizeof(aku_Sample));
    auto ptr = (char*)usr_buffer_ + write_offset_;
    assert(len >= sizeof(aku_Sample));
    assert((result.payload.type|aku_PData::SAX_WORD) == 0 ?
           result.payload.size == sizeof(aku_Sample) :
           result.payload.size >= sizeof(aku_Sample));
    memcpy(ptr, &result, len);
    write_offset_ += len;
}

bool CursorFSM::close() {
    bool old = closed_;
    closed_ = true;
    return old != closed_;
}

void CursorFSM::complete() {
    complete_ = true;
}

void CursorFSM::set_error(aku_Status error_code) {
    error_code_ = error_code;
    error_ = true;
    complete_ = true;
}

bool CursorFSM::is_done() const {
    return complete_ || closed_;
}

bool CursorFSM::get_error(aku_Status *error_code) const {
    if (error_ && error_code) {
        *error_code = error_code_;
        return true;
    }
    return error_;
}

size_t CursorFSM::get_data_len() const {
    return write_offset_;
}

// CoroCursor

// coverity[+alloc]
void CoroCursorStackAllocator::allocate(boost::coroutines::stack_context& ctx, size_t size) const
{
    ctx.size = size;
    // TODO: this is not an error, add to coverity mapping
    ctx.sp = reinterpret_cast<char*>(malloc(size)) + size;
}

// coverity[+free : arg-1]
void CoroCursorStackAllocator::deallocate(boost::coroutines::stack_context& ctx) const {
    free(reinterpret_cast<char*>(ctx.sp) - ctx.size);
}

// External cursor implementation

size_t CoroCursor::read_ex(void* buffer, size_t buffer_size) {
    cursor_fsm_.update_buffer(buffer, buffer_size);
    coroutine_->operator()(this);
    return cursor_fsm_.get_data_len();
}

bool CoroCursor::is_done() const {
    return cursor_fsm_.is_done();
}

bool CoroCursor::is_error(aku_Status* out_error_code_or_null) const {
    return cursor_fsm_.get_error(out_error_code_or_null);
}

void CoroCursor::close() {
    if (cursor_fsm_.close()) {
        coroutine_->operator()(this);
        coroutine_.reset();
    }
}

// Internal cursor implementation

void CoroCursor::set_error(Caller& caller, aku_Status error_code) {
    cursor_fsm_.set_error(error_code);
    caller();
}

bool CoroCursor::put(Caller& caller, aku_Sample const& result) {
    if (cursor_fsm_.is_done()) {
        return false;
    }
    if (!cursor_fsm_.can_put(std::max(result.payload.size, (u16)sizeof(aku_Sample)))) {
        caller();
        if (cursor_fsm_.is_done()) {
            return false;
        }
    }
    cursor_fsm_.put(result);
    if (result.payload.type&aku_PData::URGENT) {
        // Important sample received (anomaly). Cursor should call consumer immediately.
        caller();
        if (cursor_fsm_.is_done()) {
            return false;
        }
    }
    return true;
}

void CoroCursor::complete(Caller& caller) {
    cursor_fsm_.complete();
    caller();
}

}
