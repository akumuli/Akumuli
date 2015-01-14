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

#include "stream.h"
#include <vector>

namespace Akumuli {

struct RESPError : StreamError {
    RESPError(std::string msg, int pos);
};

/**
  * REdis Serialization Protocol implementation.
  */
struct RESPStream
{
    enum {
        KB = 1024,
        MB = 1024*KB,
        STRING_LENGTH_MAX = 1*KB,  //< Longest possible string
        BULK_LENGTH_MAX   = 1*MB,  //< Longest possible bulk string
    };

    enum Type {
        STRING,
        INTEGER,
        ARRAY,
        BULK_STR,
        ERROR,
        BAD,
    };

    ByteStreamReader *stream_;  //< Stream

    RESPStream(ByteStreamReader *stream);

    /** Read next element's type.
      */
    Type next_type() const;

    /** Read integer.
      * Result is undefined unless next element in a stream is an integer.
      * @return value
      * @throw on error
      */
    uint64_t read_int();

    /** Read integer implementation
      * @throw on error
      * @return parsed integer
      */
    uint64_t _read_int_body();

    /** Read string element.
      * Result is undefined unless next element in a stream is a string.
      * @param buffer user suplied buffer
      * @param buffer_size size of the buffer
      * @return number of bytes copied or negative value on error
      */
    int read_string(Byte *buffer, size_t buffer_size);

    /** Read string implementation */
    int _read_string_body(Byte *buffer, size_t buffer_size);

    /** Read bulk string element.
      * Result is undefined unless next element in a stream is a bulk string.
      * @param buffer user suplied buffer
      * @param buffer_size size of the buffer
      * @return number of bytes copied or negative value on error
      */
    int read_bulkstr(Byte *buffer, size_t buffer_size);

    /** Read size of the array element.
      * Exception is thrown unless next element in a stream is an array.
      * Funtion reads size of the array and puts stream cursor to the
      * first array element.
      */
    uint64_t read_array_size();
};

}

