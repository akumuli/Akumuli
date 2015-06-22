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
    , write_index_(0)
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

void CursorFSM::update_buffer(aku_CursorResult* buf, size_t buf_len) {
    usr_buffer_ = buf;
    usr_buffer_len_ = buf_len;
    write_index_ = 0;
}

void CursorFSM::update_buffer(CursorFSM *other_fsm) {
    usr_buffer_ = other_fsm->usr_buffer_;
    usr_buffer_len_ = other_fsm->usr_buffer_len_;
    write_index_ = other_fsm->write_index_;
}

bool CursorFSM::can_put() const {
    return usr_buffer_ != nullptr && write_index_ < usr_buffer_len_;
}

void CursorFSM::put(const aku_CursorResult &result) {
    usr_buffer_[write_index_++] = result;
}

bool CursorFSM::close() {
    bool old = closed_;
    closed_ = true;
    return old != closed_;
}

void CursorFSM::complete() {
    complete_ = true;
}

void CursorFSM::set_error(int error_code) {
    error_code_ = error_code;
    error_ = true;
    complete_ = true;
}

bool CursorFSM::is_done() const {
    return complete_ || closed_;
}

bool CursorFSM::get_error(int *error_code) const {
    if (error_ && error_code) {
        *error_code = error_code_;
        return true;
    }
    return error_;
}

size_t CursorFSM::get_data_len() const {
    return write_index_;
}

// CoroCursor

void CoroCursorStackAllocator::allocate(boost::coroutines::stack_context& ctx, size_t size) const
{
    ctx.size = size;
    ctx.sp = reinterpret_cast<char*>(malloc(size)) + size;
}

void CoroCursorStackAllocator::deallocate(boost::coroutines::stack_context& ctx) const {
    free(reinterpret_cast<char*>(ctx.sp) - ctx.size);
}

// External cursor implementation

size_t CoroCursor::read(aku_CursorResult *buf, size_t buf_len) {
    cursor_fsm_.update_buffer(buf, buf_len);
    coroutine_->operator()(this);
    return cursor_fsm_.get_data_len();
}

bool CoroCursor::is_done() const {
    return cursor_fsm_.is_done();
}

bool CoroCursor::is_error(int* out_error_code_or_null) const {
    return cursor_fsm_.get_error(out_error_code_or_null);
}

void CoroCursor::close() {
    if (cursor_fsm_.close()) {
        coroutine_->operator()(this);
        coroutine_.reset();
    }
}

// Internal cursor implementation

void CoroCursor::set_error(Caller& caller, int error_code) {
    cursor_fsm_.set_error(error_code);
    caller();
}

bool CoroCursor::put(Caller& caller, aku_CursorResult const& result) {
    if (cursor_fsm_.is_done()) {
        return false;
    }
    if (!cursor_fsm_.can_put()) {
        // yield control to client
        caller();
    }
    if (cursor_fsm_.is_done()) {
        return false;
    }
    cursor_fsm_.put(result);
    return true;
}

void CoroCursor::complete(Caller& caller) {
    cursor_fsm_.complete();
    caller();
}


// StacklessFanInCursorCombinator

StacklessFanInCursorCombinator::StacklessFanInCursorCombinator(
        ExternalCursor** in_cursors,
        int size,
        int direction)
    : direction_(direction)
    , in_cursors_(in_cursors, in_cursors + size)
    , pred_{direction}
{
    int error = AKU_SUCCESS;
    for (auto cursor: in_cursors_) {
        if (cursor->is_error(&error)) {
            set_error(error);
            return;
        }
    }

    const size_t BUF_LEN = 0x200;
    aku_CursorResult buffer[BUF_LEN];
    for(auto cur_index = 0u; cur_index < in_cursors_.size(); cur_index++) {
        if (!in_cursors_[cur_index]->is_done()) {
            ExternalCursor* cursor = in_cursors_[cur_index];
            size_t nwrites = cursor->read(buffer, BUF_LEN);
            if (cursor->is_error(&error)) {
                set_error(error);
                return;
            }
            for (size_t buf_ix = 0u; buf_ix < nwrites; buf_ix++) {
                auto cur_count = nwrites - buf_ix;
                auto key = std::make_tuple(buffer[buf_ix], cur_index, cur_count);
                heap_.push_back(key);
            }
        }
    }

    std::make_heap(heap_.begin(), heap_.end(), pred_);
}

void StacklessFanInCursorCombinator::read_impl_() {
    // Check preconditions
    int error = 0;
    bool proceed = true;
    const size_t BUF_LEN = 0x200;
    aku_CursorResult buffer[BUF_LEN];
    while(proceed && !heap_.empty()) {
        std::pop_heap(heap_.begin(), heap_.end(), pred_);
        auto item = heap_.back();
        const aku_CursorResult& cur_result = std::get<0>(item);
        int cur_index = std::get<1>(item);
        int cur_count = std::get<2>(item);
        proceed = put(cur_result);
        heap_.pop_back();
        if (cur_count == 1 && !in_cursors_[cur_index]->is_done()) {
            ExternalCursor* cursor = in_cursors_[cur_index];
            size_t nwrites = cursor->read(buffer, BUF_LEN);
            if (cursor->is_error(&error)) {
                set_error(error);
                return;
            }
            for (size_t buf_ix = 0u; buf_ix < nwrites; buf_ix++) {
                auto key = std::make_tuple(buffer[buf_ix], cur_index, nwrites - buf_ix);
                heap_.push_back(key);
                std::push_heap(heap_.begin(), heap_.end(), pred_);
            }
        }
    }
    if (heap_.empty()) {
        complete();
    }
}

size_t StacklessFanInCursorCombinator::read(aku_CursorResult *buf, size_t buf_len) {
    cursor_fsm_.update_buffer(buf, buf_len);
    read_impl_();
    return cursor_fsm_.get_data_len();
}

bool StacklessFanInCursorCombinator::is_done() const {
    return cursor_fsm_.is_done();
}

bool StacklessFanInCursorCombinator::is_error(int *out_error_code_or_null) const {
    return cursor_fsm_.get_error(out_error_code_or_null);
}

void StacklessFanInCursorCombinator::close() {
    for (auto cursor: in_cursors_) {
        cursor->close();
    }
    cursor_fsm_.close();
}

void StacklessFanInCursorCombinator::set_error(int error_code) {
    cursor_fsm_.set_error(error_code);
}

bool StacklessFanInCursorCombinator::put(aku_CursorResult const& result) {
    if (cursor_fsm_.can_put()) {
        cursor_fsm_.put(result);
    }
    return cursor_fsm_.is_done();
}

void StacklessFanInCursorCombinator::complete() {
    cursor_fsm_.complete();
}


// FanInCursor implementation

FanInCursorCombinator::FanInCursorCombinator(ExternalCursor **cursors, int size, int direction)
    : in_cursors_(cursors, cursors + size)
    , direction_(direction)
    , out_cursor_()
{
    out_cursor_.start(std::bind(&FanInCursorCombinator::read_impl_, this, std::placeholders::_1));
}

void FanInCursorCombinator::read_impl_(Caller& caller) {
#ifdef DEBUG
    aku_CursorResult dbg_prev_item;
    bool dbg_first_item = true;
    long dbg_counter = 0;
#endif
    // Check preconditions
    int error = 0;
    for (auto cursor: in_cursors_) {
        if (cursor->is_error(&error)) {
            out_cursor_.set_error(caller, error);
            return;
        }
    }

    typedef std::vector<HeapItem> Heap;
    Heap heap;
    HeapPred pred = { direction_ };

    const size_t BUF_LEN = 0x200;
    aku_CursorResult buffer[BUF_LEN];
    for(auto cur_index = 0u; cur_index < in_cursors_.size(); cur_index++) {
        if (!in_cursors_[cur_index]->is_done()) {
            ExternalCursor* cursor = in_cursors_[cur_index];
            size_t nwrites = cursor->read(buffer, BUF_LEN);
            if (cursor->is_error(&error)) {
                out_cursor_.set_error(caller, error);
                return;
            }
            for (size_t buf_ix = 0; buf_ix < nwrites; buf_ix++) {
                auto cur_count = nwrites - buf_ix;
                auto key = std::make_tuple(buffer[buf_ix], cur_index, cur_count);
                heap.push_back(key);
            }
        }
    }

    std::make_heap(heap.begin(), heap.end(), pred);

    while(!heap.empty()) {
        std::pop_heap(heap.begin(), heap.end(), pred);
        auto item = heap.back();
        const aku_CursorResult& cur_result = std::get<0>(item);
        int cur_index = std::get<1>(item);
        int cur_count = std::get<2>(item);
#ifdef DEBUG
        auto dbg_time_stamp = cur_result.timestamp;
        auto dbg_param_id = cur_result.paramid;
        AKU_UNUSED(dbg_time_stamp);
        AKU_UNUSED(dbg_param_id);
        if (!dbg_first_item) {
            bool cmp_res = false;
            if (direction_ == AKU_CURSOR_DIR_FORWARD) {
                cmp_res = dbg_prev_item.timestamp <= std::get<0>(item).timestamp;
            } else {
                cmp_res = dbg_prev_item.timestamp >= std::get<0>(item).timestamp;
            }
            assert(cmp_res);
        }
        dbg_prev_item = std::get<0>(item);
        dbg_first_item = false;
        dbg_counter++;
#endif
        out_cursor_.put(caller, cur_result);
        heap.pop_back();
        if (cur_count == 1 && !in_cursors_[cur_index]->is_done()) {
            ExternalCursor* cursor = in_cursors_[cur_index];
            size_t nwrites = cursor->read(buffer, BUF_LEN);
            if (cursor->is_error(&error)) {
                out_cursor_.set_error(caller, error);
                return;
            }
            for (size_t buf_ix = 0u; buf_ix < nwrites; buf_ix++) {
                auto key = std::make_tuple(buffer[buf_ix], cur_index, nwrites - buf_ix);
                heap.push_back(key);
                std::push_heap(heap.begin(), heap.end(), pred);
            }
        }
    }
    out_cursor_.complete(caller);
}

size_t FanInCursorCombinator::read(aku_CursorResult *buf, size_t buf_len)
{
    return out_cursor_.read(buf, buf_len);
}

bool FanInCursorCombinator::is_done() const
{
    return out_cursor_.is_done();
}

bool FanInCursorCombinator::is_error(int *out_error_code_or_null) const
{
    return out_cursor_.is_error(out_error_code_or_null);
}

void FanInCursorCombinator::close()
{
    for (auto cursor: in_cursors_) {
        cursor->close();
    }
    return out_cursor_.close();
}

}
