#include "cursor.h"
#include <log4cxx/logger.h>
#include <log4cxx/logmanager.h>
#include <algorithm>


namespace Akumuli {

static log4cxx::LoggerPtr s_logger_ = log4cxx::LogManager::getLogger("Akumuli.Cursor");

void RecordingCursor::put(Caller &, EntryOffset offset) noexcept {
    offsets.push_back(offset);
}

void RecordingCursor::complete(Caller&) noexcept {
    completed = true;
}


void RecordingCursor::set_error(Caller&, int error_code) noexcept {
    this->error_code = error_code;
}


BufferedCursor::BufferedCursor(EntryOffset* buf, size_t size) noexcept
    : offsets_buffer(buf)
    , buffer_size(size)
    , count(0u)
{
}

void BufferedCursor::put(Caller&, EntryOffset offset) noexcept {
    if (count == buffer_size) {
        completed = true;
        error_code = AKU_EOVERFLOW;
        return;
    }
    offsets_buffer[count++] = offset;
}

void BufferedCursor::complete(Caller&) noexcept {
    completed = true;
}

void BufferedCursor::set_error(Caller&, int code) noexcept {
    completed = true;
    error_code = code;
}


// CoroCursor
CoroCursor::CoroCursor()
    : usr_buffer_(nullptr)
    , usr_buffer_len_(0)
    , write_index_(0)
    , error_code_(AKU_SUCCESS)
    , error_(false)
    , complete_(false)
{
}

// External cursor implementation

int CoroCursor::read(EntryOffset* buf, int buf_len) noexcept {
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

void CoroCursor::put(Caller& caller, EntryOffset off) noexcept {
    if (write_index_ == usr_buffer_len_) {
        // yield control to client
        caller();
    }
    usr_buffer_[write_index_++] = off;
}

void CoroCursor::complete(Caller& caller) noexcept {
    complete_ = true;
    caller();
}


// FanInCursor implementation

typedef std::tuple<TimeStamp, ParamId, EntryOffset, int, int> HeapItem;

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
            // Panic!
            throw std::runtime_error("Bad direction of the fan-in cursor");
        }
        return result;
    }
};

FanInCursor::FanInCursor(ExternalCursor **cursors, PageHeader** pages, int size, int direction) noexcept
    : cursors_(cursors, cursors + size)
    , pages_(pages, pages + size)
    , direction_(direction)
    , is_done_(false)
    , error_(AKU_SUCCESS)
{
}

void FanInCursor::read_impl_(Caller& caller, InternalCursor* out_cursor) noexcept {
    // Check preconditions
    for (auto cursor: cursors_) {
       if (cursor->is_error(&error_)) {
           is_done_ = true;
           return;
       }
    }

    typedef std::vector<HeapItem> Heap;
    Heap heap;
    HeapPred pred = { direction_ };

    const int BUF_LEN = 0x200;
    EntryOffset buffer[BUF_LEN];
    for(int cur_index = 0; cur_index < cursors_.size(); cur_index++) {
        if (!cursors_[cur_index]->is_done()) {
            PageHeader* page = pages_[cur_index];
            ExternalCursor* cursor = cursors_[cur_index];
            int nwrites = cursor->read(buffer, BUF_LEN);
            if (cursor->is_error(&error_)) {
                is_done_ = true;
                return;
            }
            for (int buf_ix = 0; buf_ix < nwrites; buf_ix++) {
                EntryOffset offset = buffer[buf_ix];
                const Entry* entry = page->read_entry(offset);
                auto key = std::make_tuple(entry->time, entry->param_id, offset, cur_index, nwrites - buf_ix);
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
        out_cursor->put(caller, offset);
        heap.pop_back();
        if (cur_count == 0 && !cursors_[cur_index]->is_done()) {
            ExternalCursor* cursor = cursors_[cur_index];
            PageHeader* page = pages_[cur_index];
            int nwrites = cursor->read(buffer, BUF_LEN);
            if (cursor->is_error(&error_)) {
                is_done_ = true;
                return;
            }
            for (int buf_ix = 0; buf_ix < nwrites; buf_ix++) {
                EntryOffset offset = buffer[buf_ix];
                const Entry* entry = page->read_entry(offset);
                auto key = std::make_tuple(entry->time, entry->param_id, offset, cur_index, nwrites - buf_ix);
                heap.push_back(key);
                std::push_heap(heap.begin(), heap.end(), pred);
            }
        }
    }

    is_done_ = true;
}

int FanInCursor::read(EntryOffset* buf, int buf_len) noexcept {
    throw std::logic_error("Not implemented");
}

bool FanInCursor::is_done() const noexcept {
    return is_done_;
}

bool FanInCursor::is_error(int* out_error_code_or_null) const noexcept {
    if (out_error_code_or_null)
       *out_error_code_or_null = error_;
    return error_;
}

void FanInCursor::close() noexcept {
    for (auto cursor: cursors_) {
        cursor->close();
    }
}

}
