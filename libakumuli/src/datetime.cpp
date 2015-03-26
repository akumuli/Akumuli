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

//! 10ns interval
typedef std::ratio<1, 100000000> TenNanoseconds;

typedef std::chrono::duration<aku_TimeStamp, TenNanoseconds> DurationT;

static const boost::posix_time::ptime EPOCH = boost::posix_time::from_time_t(0);

aku_TimeStamp DateTimeUtil::from_std_chrono(std::chrono::system_clock::time_point timestamp) {
    auto duration = timestamp.time_since_epoch();
    DurationT result = std::chrono::duration_cast<DurationT>(duration);
    return result.count();
}

aku_TimeStamp DateTimeUtil::from_boost_ptime(boost::posix_time::ptime timestamp) {
    boost::posix_time::time_duration duration = timestamp - EPOCH;
    uint64_t ns = duration.total_nanoseconds() / 10;
    return ns;
}

}

