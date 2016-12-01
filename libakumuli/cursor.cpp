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
#include <string.h>
#include <algorithm>


namespace Akumuli {

// CoroCursor //

namespace {
    enum {
        BUFFER_SIZE = 0x4000,
        QUEUE_MAX = 0x20,
    };
}

// External cursor implementation //

ConcurrentCursor::ConcurrentCursor()
    : done_{false}
    , error_code_{AKU_SUCCESS}
{}

u32 ConcurrentCursor::read(void* buffer, u32 buffer_size) {
    u32 nbytes = 0;
    u8* dest = static_cast<u8*>(buffer);
    std::unique_lock<std::mutex> lock(mutex_);
    while(true) {
        if (queue_.empty()) {
            if (done_) {
                return nbytes;
            }
            cond_.wait(lock);
            continue;
        }
        auto front = queue_.front();
        auto bytes2read = std::min(buffer_size, static_cast<u32>(front->wrpos - front->rdpos));
        memcpy(dest, front->buf.data() + front->rdpos, bytes2read);
        front->rdpos += bytes2read;
        nbytes += bytes2read;
        dest += bytes2read;
        buffer_size -= bytes2read;
        if (front->rdpos == front->wrpos) {
            queue_.pop_front();
            cond_.notify_all();
        }
        if (buffer_size < sizeof(aku_Sample)) {
            break;
        }
    }
    return nbytes;
}

bool ConcurrentCursor::is_done() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return done_ && queue_.empty();
}

bool ConcurrentCursor::is_error(aku_Status* out_error_code_or_null) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (out_error_code_or_null != nullptr) {
        *out_error_code_or_null = error_code_;
    }
    return done_ && error_code_ != AKU_SUCCESS;
}

void ConcurrentCursor::close() {
    done_ = true;
    cond_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
}

// Internal cursor implementation

void ConcurrentCursor::set_error(aku_Status error_code) {
    std::lock_guard<std::mutex> lock(mutex_);
    done_ = true;
    error_code_ = error_code;
    cond_.notify_all();
}

static std::shared_ptr<ConcurrentCursor::BufferT> make_empty() {
    auto buf = std::make_shared<ConcurrentCursor::BufferT>();
    buf->buf.resize(BUFFER_SIZE);
    buf->rdpos = 0;
    buf->wrpos = 0;
    return buf;
}

bool ConcurrentCursor::put(aku_Sample const& result) {
    if (done_) {
        return false;
    }
    u32 bytes = result.payload.size;
    std::unique_lock<std::mutex> lock(mutex_);
    std::shared_ptr<BufferT> top;
    while(true) {
        if(queue_.empty()) {
            top = make_empty();
            queue_.push_back(top);
        }
        top = queue_.back();
        if (top->wrpos + bytes > BUFFER_SIZE) {
            // Overflow
            if (queue_.size() < QUEUE_MAX) {
                top = make_empty();
                queue_.push_back(top);
            } else {
                cond_.wait(lock);
            }
            continue;
        } else {
            break;
        }
    }
    memcpy(top->buf.data() + top->wrpos, &result, bytes);
    top->wrpos += bytes;
    cond_.notify_all();
    return true;
}

void ConcurrentCursor::complete() {
    done_ = true;
    cond_.notify_all();
}

}
