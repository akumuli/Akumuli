/**
 * PRIVATE HEADER
 *
 * Sort alrgorithms for akumuli.
 *
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

#pragma once
#include <algorithm>

namespace Akumuli {


/** Insertion sort implementation.
  * @param TIter random access iterator type
  * @param TComp compare predicate (work on values, not on iterators)
  */
template< typename TIter
        , typename TComp>
void insertion_sort( TIter        begin
                   , TIter        end
                   , TComp const& cmp)
{
    if (begin == end) return;

    for (auto i = begin + 1; i != end; ++i) {
        if (cmp(*i, *begin)) {
            auto value = *i;
            std::copy_backward(begin, i, i + 1);
            *begin = value;
        } else {
            auto val = *i;
            auto next = i - 1;
            while (cmp(val, *next)) {
                *i = *next;
                i = next;
                --next;
            }
            *i = val;
        }
    }
}

}
