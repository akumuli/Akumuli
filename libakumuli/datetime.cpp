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

#include "datetime.h"

namespace Akumuli {

//! 1ns interval
typedef std::chrono::nanoseconds DurationT;

static const boost::posix_time::ptime EPOCH = boost::posix_time::from_time_t(0);

aku_Timestamp DateTimeUtil::from_std_chrono(std::chrono::system_clock::time_point timestamp) {
    auto duration = timestamp.time_since_epoch();
    DurationT result = std::chrono::duration_cast<DurationT>(duration);
    return result.count();
}

aku_Timestamp DateTimeUtil::from_boost_ptime(boost::posix_time::ptime timestamp) {
    boost::posix_time::time_duration duration = timestamp - EPOCH;
    uint64_t ns = duration.total_nanoseconds();
    return ns;
}

aku_Timestamp DateTimeUtil::from_iso_string(const char* iso_str) {
    auto pt = boost::posix_time::from_iso_string(iso_str);
    return DateTimeUtil::from_boost_ptime(pt);
}

}

