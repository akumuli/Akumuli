/**
 * Copyright (c) 2014 Eugene Lazin <4lazin@gmail.com>
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
 */

#pragma once
#include "akumuli_def.h"
#include <cstddef>
#include <memory>
#include <queue>
#include <stdexcept>
#include <string>
#include <vector>

namespace Akumuli {

typedef char Byte;

class StreamError : public std::exception {
    std::string line_;
    size_t      pos_;

public:
    enum {
        MAX_LENGTH = 64,
    };
    StreamError(std::string line, size_t pos);

    virtual const char* what() const noexcept;
    std::string         get_bottom_line() const;
};

/** Stream reader that operates on byte level. */
struct ByteStreamReader {

    virtual ~ByteStreamReader();

    /** Read one byte from stream (if any).
      * If stream is empty StreamError exception is generated.
      * User should check if there any data in the stream using function `is_eof`.
      */
    virtual Byte get() = 0;

    /** Read top element of the stream.
      * If stream is closed or empty StreamError exception is generated.
      * This method doesn't changes the state of the stream.
      */
    virtual Byte pick() const = 0;

    /** Check whether or not stream end is reached.
      * @returns true if stream end is reached false otherwise.
      */
    virtual bool is_eof() = 0;

    /** Read chunk of bytes from the stream. If `buffer_len` bytes
      * was read to the `buffer` output parameter - method should
      * return number of bytes read (it should be equal to `buffer_len`.
      * If stream doesn't contains enough bytes, method should read all thats
      * left to the `buffer` array and return number of bytes read (it should be
      * less then `buffer_len`). If EOF was reached - method
      * should return zero. If error occured (stream already closed) - negative value
      * should be returned.
      */
    virtual int read(Byte* buffer, size_t buffer_len) = 0;

    /** Read line from the stream. Returns number of bytes read.
      * Line terminator is an \r\n or \n series of symbols.
      */
    virtual int read_line(Byte* buffer, size_t quota) = 0;

    /** Close stream.
     **/
    virtual void close() = 0;

    /** Method returns error context in form:
     * Error message at "abcdefgh"
     *                       ^
     */
    virtual std::tuple<std::string, size_t> get_error_context(const char* error_message) const = 0;

    /** This method should be called when full message have benn successfuly readen from
      * the stream.
      */
    void consume();

    /** This method should be called if message can be extracted from stream only partially
      * and receiver should wait for more data.
      */
    void discard();
};


class MemStreamReader : public ByteStreamReader {
    const Byte*  buf_;   //< Source bytes
    const size_t size_;  //< Source size
    size_t       cons_;  //< Size of the consumed part of the stream
    size_t       pos_;   //< Position in the stream
public:
    MemStreamReader(const Byte* buffer, size_t buffer_len);

    // ByteStreamReader interface
public:
    virtual Byte get();
    virtual Byte pick() const;
    virtual bool is_eof();
    virtual int read(Byte* buffer, size_t buffer_len);
    virtual void close();
    virtual std::tuple<std::string, size_t> get_error_context(const char* error_message) const;
    virtual void consume();
    virtual void discard();
    virtual int read_line(Byte* buffer, size_t quota) override;
};

}  // namespace
