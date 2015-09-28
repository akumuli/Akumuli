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
 *
 */
#include "invertedindex.h"

namespace Akumuli {

static const int NUM_HASHES = 5;

InvertedIndex::InvertedIndex(const size_t table_size)
    : table_size_(table_size)
    , hashes_(NUM_HASHES, table_size)
{
    table_.resize(table_size);
}

}  // namespace
