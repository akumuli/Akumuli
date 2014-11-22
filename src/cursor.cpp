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

void CursorFSM::update_buffer(CursorResult* buf, int buf_len) {
    usr_buffer_ = buf;
    usr_buffer_len_ = buf_len;
    write_index_ = 0;
}

bool CursorFSM::can_put() const {
    return usr_buffer_ != nullptr && write_index_ < usr_buffer_len_;
}

void CursorFSM::put(CursorResult const& result) {
#ifdef DEBUG
    if (result.length == 0) {
        std::cout << "put error, result.length == 0" << std::endl;
        assert(result.length);
    }
#endif
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

int CursorFSM::get_data_len() const {
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

int CoroCursor::read(CursorResult* buf, int buf_len) {
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

bool CoroCursor::put(Caller& caller, CursorResult const& result) {
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
    for (auto cursor: in_cursors_) {
        cursor->close();
    }
    cursor_fsm_.close();
}

void StacklessFanInCursorCombinator::set_error(int error_code) {
    cursor_fsm_.set_error(error_code);
}

bool StacklessFanInCursorCombinator::put(CursorResult const& result) {
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

// ChunkCursor

namespace {

struct ChunkHeaderSearcher : InterpolationSearch<ChunkHeaderSearcher> {
    ChunkHeader const& header;
    ChunkHeaderSearcher(ChunkHeader const& h) : header(h) {}

    // Interpolation search supporting functions
    bool read_at(aku_TimeStamp* out_timestamp, uint32_t ix) const {
        if (ix < header.timestamps.size()) {
            *out_timestamp = header.timestamps[ix];
            return true;
        }
        return false;
    }

    bool is_small(SearchRange range) const {
        return false;
    }

    SearchStats& get_search_stats() {
        return get_global_search_stats();
    }
};

}

ChunkCursor::ChunkCursor( PageHeader const* page
                        , aku_Entry const* entry
                        , aku_TimeStamp key
                        , SearchQuery query
                        , bool backward
                        , bool binary_search)
    : binary_search_(binary_search)
    , probe_entry_(entry)
    , page_(page)
    , key_(key)
    , query_(query)
    , IS_BACKWARD_(backward)
    , start_pos_(0u)
{
    auto pdesc = reinterpret_cast<ChunkDesc const*>(&probe_entry_->value[0]);
    auto pbegin = (const unsigned char*)(page_->cdata() + pdesc->begin_offset);
    auto pend = (const unsigned char*)(page_->cdata() + pdesc->end_offset);
    probe_length_ = pdesc->n_elements;

    // TODO:checksum!
    boost::crc_32_type checksum;
    checksum.process_block(pbegin, pend);
    if (checksum.checksum() != pdesc->checksum) {
        AKU_PANIC("File damaged!");
        return;
    }

    // read timestamps
    CompressionUtil::decode_chunk(&header_, &pbegin, pend, 0, 1, probe_length_);

    if (IS_BACKWARD_) {
        start_pos_ = static_cast<int>(probe_length_ - 1);
    }
    // test timestamp range
    if (binary_search_) {
        ChunkHeaderSearcher int_searcher(header_);
        SearchRange sr = { 0, static_cast<uint32_t>(header_.timestamps.size())};
        int_searcher.run(key_, &sr);
        auto begin = header_.timestamps.begin() + sr.begin;
        auto end = header_.timestamps.begin() + sr.end;
        auto it = std::lower_bound(begin, end, key_);
        if (it == end) {
            // key_ not in chunk
            cursor_fsm_.complete();
            return;
        }
        if (IS_BACKWARD_) {
            if (!header_.timestamps.empty()) {
                auto last = header_.timestamps.begin() + start_pos_;
                auto delta = last - it;
                start_pos_ -= delta;
            }
        } else {
            start_pos_ += it - header_.timestamps.begin();
        }
    }

    CompressionUtil::decode_chunk(&header_, &pbegin, pend, 1, 3, probe_length_);
}

void ChunkCursor::scan_compressed_entries()
{
    bool probe_in_time_range = true;

    if (IS_BACKWARD_) {
        for (int i = static_cast<int>(start_pos_); i >= 0; i--) {
            probe_in_time_range = query_.lowerbound <= header_.timestamps[i] &&
                                  query_.upperbound >= header_.timestamps[i];
            if (probe_in_time_range
               && query_.param_pred(header_.paramids[i])
               && !cursor_fsm_.is_done()) {
                CursorResult result = {
                    header_.lengths[i],
                    header_.timestamps[i],
                    header_.paramids[i],
                    page_->read_entry_data(header_.offsets[i]),
                };
                if (cursor_fsm_.can_put()) {
                    cursor_fsm_.put(result);
                } else {
                    start_pos_ = i;
                    return;
                }
            } else {
                probe_in_time_range = query_.lowerbound <= header_.timestamps[i];
                if (!probe_in_time_range || cursor_fsm_.is_done()) {
                    break;
                }
            }
        }
    } else {
        for (auto i = start_pos_; i != probe_length_; i++) {
            probe_in_time_range = query_.lowerbound <= header_.timestamps[i] &&
                                  query_.upperbound >= header_.timestamps[i];
            if (probe_in_time_range
               && query_.param_pred(header_.paramids[i])
               && !cursor_fsm_.is_done()) {
                CursorResult result = {
                    header_.lengths[i],
                    header_.timestamps[i],
                    header_.paramids[i],
                    page_->read_entry_data(header_.offsets[i]),
                };
                if (cursor_fsm_.can_put()) {
                    cursor_fsm_.put(result);
                } else {
                    start_pos_ = i;
                    return;
                }
            } else {
                probe_in_time_range = query_.upperbound >= header_.timestamps[i];
                if (!probe_in_time_range || cursor_fsm_.is_done()) {
                    break;
                }
            }
        }
    }
    cursor_fsm_.close();
}

int ChunkCursor::read(CursorResult *buf, int buf_len)
{
    cursor_fsm_.update_buffer(buf, buf_len);
    scan_compressed_entries();
    return cursor_fsm_.get_data_len();
}

bool ChunkCursor::is_done() const
{
    return cursor_fsm_.is_done();
}

bool ChunkCursor::is_error(int *out_error_code_or_null) const
{
    return cursor_fsm_.get_error(out_error_code_or_null);
}

void ChunkCursor::close()
{
    cursor_fsm_.close();
}

}
