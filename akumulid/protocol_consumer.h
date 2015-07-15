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
#include "akumuli.h"

namespace Akumuli {

typedef char Byte;

/** Protocol consumer. All decoded data goes here.
  * Abstract class.
  */
struct ProtocolConsumer {

    ~ProtocolConsumer() {}

    virtual void write(const aku_Sample&) = 0;

    // TODO: remove this function, bulk string decoding should be done inside ProtocolParser
    virtual void add_bulk_string(const Byte *buffer, size_t n) = 0;

    //! Convert series name to param id
    virtual aku_Status series_to_param_id(const char* str, size_t strlen, aku_Sample* sample) = 0;
};

}
