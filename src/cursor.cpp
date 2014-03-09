#include "cursor.h"
#include <log4cxx/logger.h>
#include <log4cxx/logmanager.h>


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

void CoroCursor::put(Caller& caller, int i) noexcept {
    if (write_index_ == usr_buffer_len_) {
        // yield control to client
        caller();
    }
    usr_buffer_[write_index_++] = i;
}

void CoroCursor::complete(Caller& caller) noexcept {
    complete_ = true;
    caller();
}

}
