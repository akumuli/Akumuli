/**
 * PRIVATE HEADER
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

#define BOOST_COROUTINES_BIDIRECT
#include <boost/coroutine/all.hpp>

#include "akumuli.h"

namespace Akumuli {


struct InternalCursor;


// NOTE: obsolete
typedef boost::coroutines::coroutine< void(InternalCursor*) > Coroutine;
// NOTE: obsolete
typedef typename Coroutine::caller_type Caller;

//! Cursor result
struct CursorResult {
    uint32_t          length;         //< entry data length
    aku_TimeStamp     timestamp;      //< entry timestamp
    aku_ParamId       param_id;       //< entry param id
    aku_PData         data;           //< pointer to data
};

/** Interface used by different search procedures
 *  in akumuli. Must be used only inside library.
 */
struct InternalCursor {
    //! Send offset to caller
    virtual bool put(Caller&, CursorResult const& offset) = 0;
    virtual void complete(Caller&) = 0;
    //! Set error and stop execution
    virtual void set_error(Caller&, int error_code) = 0;
};

}
