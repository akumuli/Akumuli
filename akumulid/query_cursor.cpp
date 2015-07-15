#include "query_cursor.h"
#include <cstdio>

namespace Akumuli {

QueryCursor::QueryCursor(std::shared_ptr<DbConnection> con, int readbufsize)
    : connection_(con)
    , rdbuf_pos_(0)
    , rdbuf_top_(0)
{
    try {
        rdbuf_.resize(readbufsize);
    } catch (const std::bad_alloc&) {
        // readbufsize is too large (bad config probably), use default value
        rdbuf_.resize(DEFAULT_RDBUF_SIZE_);
    }
}

void QueryCursor::throw_if_started() const {
    if (cursor_) {
        BOOST_THROW_EXCEPTION(std::runtime_error("allready started"));
    }
}

void QueryCursor::throw_if_not_started() const {
    if (!cursor_) {
        BOOST_THROW_EXCEPTION(std::runtime_error("not started"));
    }
}

void QueryCursor::start() {
    throw_if_started();
    cursor_ = connection_->search(query_text_);
}

void QueryCursor::append(const char *data, size_t data_size) {
    throw_if_started();
    query_text_ += std::string(data, data + data_size);
}

aku_Status QueryCursor::get_error() {
    aku_Status err = AKU_SUCCESS;
    return cursor_->is_error(&err);
}

size_t QueryCursor::read_some(char *buf, size_t buf_size) {
    throw_if_not_started();
    if (rdbuf_pos_ == rdbuf_top_) {
        // read new data from DB
        rdbuf_top_ = cursor_->read(rdbuf_.data(), rdbuf_.size());
        rdbuf_pos_ = 0u;
    }

    // format output
    char* begin = buf;
    char* end = begin + buf_size;
    while(rdbuf_pos_ < rdbuf_top_) {
        char* next = format(begin, end, rdbuf_.at(rdbuf_pos_));
        if (next == nullptr) {
            // done
            break;
        }
        begin = next;
        rdbuf_pos_++;
    }
    return begin - buf;
}

//! Try to format sample
char* QueryCursor::format(char* begin, char* end, const aku_Sample& sample) {
    // TODO: output formatting in query
    // RESP formatted output: +series name\r\n+timestamp\r\n+value\r\n (for double or $value for blob)

    char* pskip = begin;  // return this pointer to skip sample

    if(begin >= end) {
        return nullptr;  // not enough space inside the buffer
    }
    int size = end - begin;

    begin[0] = '+';
    begin++;
    size--;

    // sz can't be zero here because of precondition

    // Series name
    int len = connection_->param_id_to_series(sample.paramid, begin, size);
    if (len == 0) { // Error, no such Id
        len = snprintf(begin, size, "id=%lu", sample.paramid);
        if (len < 0 || len == size) {
            // Not enough space inside the buffer
            return nullptr;
        }
    } else if (len < 0) {
        // Not enough space
        return nullptr;
    }
    begin += len;
    size  -= len;
    // Add trailing \r\n to the end
    if (size < 2) {
        return nullptr;
    }
    begin[0] = '\r';
    begin[1] = '\n';
    begin += 2;
    size  -= 2;

    // Timestamp
    begin[0] = '+';
    begin++;
    size--;
    if (size < 0) {
        return nullptr;
    }
    len = aku_timestamp_to_string(sample.timestamp, begin, size) - 1;  // -1 is for '\0' character
    if (len == -1) {
        // Invalid timestamp, format as number
        len = snprintf(begin, size, "ts=%lu", sample.timestamp);
        if (len < 0 || len == size) {
            // Not enough space inside the buffer
            return nullptr;
        }
    } else if (len < -1) {
        return nullptr;
    }
    begin += len;
    size  -= len;
    // Add trailing \r\n to the end
    if (size < 2) {
        return nullptr;
    }
    begin[0] = '\r';
    begin[1] = '\n';
    begin += 2;
    size  -= 2;

    // Payload
    if (size < 0) {
        return nullptr;
    }
    if (sample.payload.type == aku_PData::FLOAT) {
        // Floating-point
        len = snprintf(begin, size, "+%G\r\n", sample.payload.value.float64);
        if (len == size || len < 0) {
            return nullptr;
        }
        begin += len;
        size  -= len;
    } else if (sample.payload.type == aku_PData::BLOB) {
        // BLOB
        int blobsize = (int)sample.payload.value.blob.size;
        if (blobsize < size) {
            // write length prefix - "$X\r\n"
            len = snprintf(begin, size, "$%d\r\n", blobsize);
            if (len < 0 || len == size) {
                return nullptr;
            }
            begin += len;
            size  -= len;
            if (blobsize > size) {
                return nullptr;
            }
            memcpy(begin, sample.payload.value.blob.begin, blobsize);
            begin += blobsize;
            size  -= blobsize;
            if (size < 2) {
                return nullptr;
            }
            begin[0] = '\r';
            begin[1] = '\n';
            begin += 2;
            size  -= 2;
        }
    } else {
        // Something went wrong
        return pskip;
    }
    return begin;
}

void QueryCursor::close() {
    throw_if_not_started();
    cursor_->close();
}

QueryProcessor::QueryProcessor(std::shared_ptr<DbConnection> con, int rdbuf)
    : con_(con)
    , rdbufsize_(rdbuf)
{
}

Http::QueryCursor* QueryProcessor::create() {
    return new QueryCursor(con_, rdbufsize_);
}

}  // namespace

