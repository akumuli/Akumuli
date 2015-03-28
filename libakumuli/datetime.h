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
#include "akumuli_def.h"

#define BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG
#include <boost/date_time/posix_time/posix_time.hpp>

#include <chrono>

namespace Akumuli {

/** aku_TimeStamp is a main datatype to represent date-time values.
  * It stores number of nanoseconds since epoch so it can fit uint64_t and doesn't prone to year 2038
  * problem.
  */

//! Static utility class for date-time utility functions
struct DateTimeUtil {

    static aku_Timestamp from_std_chrono(std::chrono::system_clock::time_point timestamp);

    static aku_Timestamp from_boost_ptime(boost::posix_time::ptime timestamp);

    //! Convert ISO string to akumuli's timestamp.
    static aku_Timestamp from_iso_string(const char* iso_str);

};

}
