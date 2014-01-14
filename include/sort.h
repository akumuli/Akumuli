/**
 * PRIVATE HEADER
 *
 * Sort alrgorithms for akumuli.
 *
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
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
