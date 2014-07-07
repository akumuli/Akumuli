#include "cursor.h"
#include <log4cxx/logger.h>
#include <log4cxx/logmanager.h>
#include <algorithm>


namespace Akumuli {

static log4cxx::LoggerPtr s_logger_ = log4cxx::LogManager::getLogger("Akumuli.Cursor");

void RecordingCursor::put(Caller &, EntryOffset offset, const PageHeader *page) noexcept {
    offsets.push_back(std::make_pair(offset, page));
}

void RecordingCursor::complete(Caller&) noexcept {
    completed = true;
}


void RecordingCursor::set_error(Caller&, int error_code) noexcept {
    this->error_code = error_code;
}


BufferedCursor::BufferedCursor(CursorResult* buf, size_t size) noexcept
    : offsets_buffer(buf)
    , buffer_size(size)
    , count(0)
{
}

void BufferedCursor::put(Caller&, EntryOffset offset, const PageHeader* page) noexcept {
    if (count == buffer_size) {
        completed = true;
        error_code = AKU_EOVERFLOW;
        return;
    }
    offsets_buffer[count++] = std::make_pair(offset, page);
}

void BufferedCursor::complete(Caller&) noexcept {
    completed = true;
}

void BufferedCursor::set_error(Caller&, int code) noexcept {
    completed = true;
    error_code = code;
}

// Page cursor

DirectPageSyncCursor::DirectPageSyncCursor()
    : error_code_()
    , error_is_set_()
    , completed_()
{
}

void DirectPageSyncCursor::put(Caller&, EntryOffset offset, const PageHeader *page) noexcept {
    const_cast<PageHeader*>(page)->sync_next_index(offset);
}

void DirectPageSyncCursor::complete(Caller&) noexcept {
    completed_ = true;
}

void DirectPageSyncCursor::set_error(Caller&, int error_code) noexcept {
    error_code_ = error_code;
    error_is_set_ = true;
}


// CoroCursor

CoroCursor::CoroCursor()
    : usr_buffer_(nullptr)
    , usr_buffer_len_(0)
    , write_index_(0)
    , error_(false)
    , error_code_(AKU_SUCCESS)
    , complete_(false)
{
}

// External cursor implementation

int CoroCursor::read(CursorResult* buf, int buf_len) noexcept {
    usr_buffer_ = buf;
    usr_buffer_len_ = buf_len;
    write_index_ = 0;
    coroutine_->operator()(this);
    return write_index_;
}

bool CoroCursor::is_done() const noexcept {
    return complete_;
}

bool CoroCursor::is_error(int* out_error_code_or_null) const noexcept
{
    if (out_error_code_or_null)
       *out_error_code_or_null = error_code_;
    return error_;
}

void CoroCursor::close() noexcept {
    coroutine_->operator()(this);
    coroutine_.reset();
}

// Internal cursor implementation

void CoroCursor::set_error(Caller& caller, int error_code) noexcept {
    error_code_ = error_code;
    error_ = true;
    complete_ = true;
    caller();
}

void CoroCursor::put(Caller& caller, EntryOffset off, const PageHeader* page) noexcept {
    if (write_index_ == usr_buffer_len_) {
        // yield control to client
        caller();
    }
    usr_buffer_[write_index_++] = std::make_pair(off, page);
}

void CoroCursor::complete(Caller& caller) noexcept {
    complete_ = true;
    caller();
}


// FanInCursor implementation

typedef std::tuple<TimeStamp, ParamId, EntryOffset, int, int, const PageHeader*> HeapItem;

struct HeapPred {
    int dir;
    bool operator () (HeapItem const& lhs, HeapItem const& rhs) {
        bool result = false;
        if (dir == AKU_CURSOR_DIR_FORWARD) {
            // Min heap is used
            result = lhs > rhs;
        } else if (dir == AKU_CURSOR_DIR_BACKWARD) {
            // Max heap is used
            result = lhs < rhs;
        } else {
            AKU_PANIC("bad direction of the fan-in cursor");
        }
        return result;
    }
};

FanInCursorCombinator::FanInCursorCombinator(ExternalCursor **cursors, int size, int direction) noexcept
    : in_cursors_(cursors, cursors + size)
    , direction_(direction)
    , out_cursor_()
{
    out_cursor_.start(std::bind(&FanInCursorCombinator::read_impl_, this, std::placeholders::_1));
}

void FanInCursorCombinator::read_impl_(Caller& caller) noexcept {
#ifdef DEBUG
    HeapItem dbg_prev_item;
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
                auto offset = buffer[buf_ix].first;
                auto page = buffer[buf_ix].second;
                const Entry* entry = page->read_entry(offset);
                auto cur_count = nwrites - buf_ix;
                auto key = std::make_tuple(entry->time, entry->param_id, offset, cur_index, cur_count, page);
                heap.push_back(key);
            }
        }
    }

    std::make_heap(heap.begin(), heap.end(), pred);

    while(!heap.empty()) {
        std::pop_heap(heap.begin(), heap.end(), pred);
        auto item = heap.back();
        auto offset = std::get<2>(item);
        int cur_index = std::get<3>(item);
        int cur_count = std::get<4>(item);
        auto cur_page = std::get<5>(item);
#ifdef DEBUG
        auto dbg_time_stamp = std::get<0>(item);
        auto dbg_param_id = std::get<1>(item);
        if (!dbg_first_item) {
            bool cmp_res = false;
            if (direction_ == AKU_CURSOR_DIR_BACKWARD)  {
                cmp_res = dbg_prev_item >= item;
            } else {
                cmp_res = dbg_prev_item <= item;
            }
            assert(cmp_res);
        }
        dbg_prev_item = item;
        dbg_first_item = false;
        dbg_counter++;
#endif
        out_cursor_.put(caller, offset, cur_page);
        heap.pop_back();
        if (cur_count == 1 && !in_cursors_[cur_index]->is_done()) {
            ExternalCursor* cursor = in_cursors_[cur_index];
            int nwrites = cursor->read(buffer, BUF_LEN);
            if (cursor->is_error(&error)) {
                out_cursor_.set_error(caller, error);
                return;
            }
            for (int buf_ix = 0; buf_ix < nwrites; buf_ix++) {
                auto offset = buffer[buf_ix].first;
                auto page = buffer[buf_ix].second;
                const Entry* entry = page->read_entry(offset);
                auto key = std::make_tuple(entry->time, entry->param_id, offset, cur_index, nwrites - buf_ix, page);
                heap.push_back(key);
                std::push_heap(heap.begin(), heap.end(), pred);
            }
        }
    }
    out_cursor_.complete(caller);
}

int FanInCursorCombinator::read(CursorResult *buf, int buf_len) noexcept
{
    return out_cursor_.read(buf, buf_len);
}

bool FanInCursorCombinator::is_done() const noexcept
{
    return out_cursor_.is_done();
}

bool FanInCursorCombinator::is_error(int *out_error_code_or_null) const noexcept
{
    return out_cursor_.is_error(out_error_code_or_null);
}

void FanInCursorCombinator::close() noexcept
{
    for (auto cursor: in_cursors_) {
        cursor->close();
    }
    return out_cursor_.close();
}

}
