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

/**
  * REdis Serialization Protocol implementation.
  */
struct RESPStream
{
    enum {
        MB = 1024*1024,
        STRING_LENGTH_MAX = 1*MB,  //< Longest possible string
        BULK_LENGTH_MAX   = 8*MB,  //< Longest possible bulk string
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

    bool is_error();

    /** Read next element's type.
      */
    Type next_type() const;

    /** Read integer.
      * Result is undefined unless next element in a stream is an integer.
      * @param output resulting integer
      * @return true on success false on error
      */
    bool read_int(uint64_t *output);

    /** Read string element.
      * Result is undefined unless next element in a stream is a string.
      * @param buffer user suplied buffer
      * @param buffer_size size of the buffer
      * @return number of bytes copied
      */
    size_t read_string(Byte *buffer, size_t buffer_size);

    /** Read bulk string element.
      * Result is undefined unless next element in a stream is a bulk string.
      * @param buffer user suplied buffer
      * @param buffer_size size of the buffer
      * @return number of bytes copied
      */
    size_t read_bulkstr(Byte *bufer, size_t buffer_size);

    /** Read size of the array element.
      * Result is undefined unless next element in a stream is an array.
      */
    int read_array_size();
};

}

