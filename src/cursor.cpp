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
#include <algorithm>


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
    assert(closed_);
}

void CursorFSM::update_buffer(CursorResult* buf, int buf_len) {
    usr_buffer_ = buf;
    usr_buffer_len_ = buf_len;
    write_index_ = 0;
}

bool CursorFSM::put(CursorResult const& result) {
    if (closed_ || complete_) {
        return false;
    }
    usr_buffer_[write_index_++] = result;
    return write_index_ >= usr_buffer_len_;
}

void CursorFSM::close() {
    closed_ = true;
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
    return false;
}

int CursorFSM::get_data_len() const {
    return write_index_;
}

// RecordingCursor

bool RecordingCursor::put(Caller &, const CursorResult &result) {
    results.push_back(result);
    return true;
}

void RecordingCursor::complete(Caller&) {
    completed = true;
}


void RecordingCursor::set_error(Caller&, int error_code) {
    this->error_code = error_code;
}


// Page cursor

DirectPageSyncCursor::DirectPageSyncCursor(Rand &rand)
    : error_code_()
    , error_is_set_()
    , completed_()
    , last_page_(nullptr)
    , rand_(rand)
{
}

bool DirectPageSyncCursor::put(Caller&, CursorResult const& result) {
    if (last_page_ != nullptr && last_page_ != result.page) {
        // Stop synchronizing page
        auto mutable_page = const_cast<PageHeader*>(last_page_);
        mutable_page->sync_next_index(0, 0, true);
    }
    auto mutable_page = const_cast<PageHeader*>(result.page);
    mutable_page->sync_next_index(result.data_offset, rand_(), false);
    last_page_ = result.page;
    return true;
}

void DirectPageSyncCursor::complete(Caller&) {
    completed_ = true;
    if (last_page_ != nullptr) {
        const_cast<PageHeader*>(last_page_)->sync_next_index(0, 0, true);
    }
}

void DirectPageSyncCursor::set_error(Caller&, int error_code) {
    error_code_ = error_code;
    error_is_set_ = true;
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

CoroCursor::CoroCursor()
    : usr_buffer_(nullptr)
    , usr_buffer_len_(0)
    , write_index_(0)
    , error_(false)
    , error_code_(AKU_SUCCESS)
    , complete_(false)
    , closed_(false)
{
}

// External cursor implementation

int CoroCursor::read(CursorResult* buf, int buf_len) {
    usr_buffer_ = buf;
    usr_buffer_len_ = buf_len;
    write_index_ = 0;
    coroutine_->operator()(this);
    return write_index_;
}

bool CoroCursor::is_done() const {
    return complete_;
}

bool CoroCursor::is_error(int* out_error_code_or_null) const
{
    if (out_error_code_or_null)
       *out_error_code_or_null = error_code_;
    return error_;
}

void CoroCursor::close() {
    if (!closed_) {
        closed_ = true;
        coroutine_->operator()(this);
        coroutine_.reset();
    }
}

// Internal cursor implementation

void CoroCursor::set_error(Caller& caller, int error_code) {
    closed_ = true;
    error_code_ = error_code;
    error_ = true;
    complete_ = true;
    caller();
}

bool CoroCursor::put(Caller& caller, CursorResult const& result) {
    if (closed_) {
        return false;
    }
    if (write_index_ >= usr_buffer_len_) {
        // yield control to client
        caller();
    }
    if (closed_) {
        return false;
    }
    usr_buffer_[write_index_++] = result;
    return true;
}

void CoroCursor::complete(Caller& caller) {
    complete_ = true;
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

    const int BUF_LEN = 0x200;
    CursorResult buffer[BUF_LEN];
    for(auto cur_index = 0u; cur_index < in_cursors_.size(); cur_index++) {
        if (!in_cursors_[cur_index]->is_done()) {
            ExternalCursor* cursor = in_cursors_[cur_index];
            int nwrites = cursor->read(buffer, BUF_LEN);
            if (cursor->is_error(&error)) {
                set_error(error);
                return;
            }
            for (int buf_ix = 0; buf_ix < nwrites; buf_ix++) {
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
    const int BUF_LEN = 0x200;
    CursorResult buffer[BUF_LEN];
    while(proceed && !heap_.empty()) {
        std::pop_heap(heap_.begin(), heap_.end(), pred_);
        auto item = heap_.back();
        const CursorResult& cur_result = std::get<0>(item);
        int cur_index = std::get<1>(item);
        int cur_count = std::get<2>(item);
        proceed = put(cur_result);
        heap_.pop_back();
        if (cur_count == 1 && !in_cursors_[cur_index]->is_done()) {
            ExternalCursor* cursor = in_cursors_[cur_index];
            int nwrites = cursor->read(buffer, BUF_LEN);
            if (cursor->is_error(&error)) {
                set_error(error);
                return;
            }
            for (int buf_ix = 0; buf_ix < nwrites; buf_ix++) {
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

int StacklessFanInCursorCombinator::read(CursorResult *buf, int buf_len) {
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
    cursor_fsm_.close();
}

void StacklessFanInCursorCombinator::set_error(int error_code) {
    cursor_fsm_.set_error(error_code);
}

bool StacklessFanInCursorCombinator::put(CursorResult const& result) {
    return cursor_fsm_.put(result);
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
    CursorResult dbg_prev_item;
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

    const int BUF_LEN = 0x200;
    CursorResult buffer[BUF_LEN];
    for(auto cur_index = 0u; cur_index < in_cursors_.size(); cur_index++) {
        if (!in_cursors_[cur_index]->is_done()) {
            ExternalCursor* cursor = in_cursors_[cur_index];
            int nwrites = cursor->read(buffer, BUF_LEN);
            if (cursor->is_error(&error)) {
                out_cursor_.set_error(caller, error);
                return;
            }
            for (int buf_ix = 0; buf_ix < nwrites; buf_ix++) {
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
        const CursorResult& cur_result = std::get<0>(item);
        int cur_index = std::get<1>(item);
        int cur_count = std::get<2>(item);
#ifdef DEBUG
        auto dbg_time_stamp = cur_result.timestamp;
        auto dbg_param_id = cur_result.param_id;
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
            int nwrites = cursor->read(buffer, BUF_LEN);
            if (cursor->is_error(&error)) {
                out_cursor_.set_error(caller, error);
                return;
            }
            for (int buf_ix = 0; buf_ix < nwrites; buf_ix++) {
                auto key = std::make_tuple(buffer[buf_ix], cur_index, nwrites - buf_ix);
                heap.push_back(key);
                std::push_heap(heap.begin(), heap.end(), pred);
            }
        }
    }
    out_cursor_.complete(caller);
}

int FanInCursorCombinator::read(CursorResult *buf, int buf_len)
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
