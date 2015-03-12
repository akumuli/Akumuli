/**
 * Copyright (c) 2015 Eugene Lazin <4lazin@gmail.com>
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
#include "stringpool.h"

namespace Akumuli {

/** Query processor.
  * Should be built from textual representation (json at first).
  * Should be used by both sequencer and page to match parameters
  * and group them together.
  */
struct QueryProcessor {

    typedef StringTools::StringT StringT;
    typedef StringTools::TableT TableT;

    aku_TimeStamp                     lowerbound;
    aku_TimeStamp                     upperbound;
    int                                direction;
    std::unordered_map<uint64_t, int>  idmapping;
    TableT                       namesofinterest;

    QueryProcessor();

    /** Match param_id. Return group id on success or
      * negative value on error.
      */
    int match(uint64_t param_id);
};

}
