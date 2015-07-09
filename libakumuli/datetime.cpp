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

struct BadDateTimeFormat : std::runtime_error {
    BadDateTimeFormat(const char* str) : std::runtime_error(str) {}
};

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

boost::posix_time::ptime DateTimeUtil::to_boost_ptime(aku_Timestamp timestamp) {
    boost::posix_time::ptime ptime = EPOCH + boost::posix_time::nanoseconds(timestamp);
    return ptime;
}

// parse N digits from string
static int parse_n_digits(const char* p, int n, const char* error_message = "can't parse digit") {
    int value = 0;
    for(int i = 0; i < n; i++) {
        char c = *p++;
        // c must be in [0x30:0x39] range
        if (c > 0x39 || c < 0x30) {
            BadDateTimeFormat err(error_message);
            BOOST_THROW_EXCEPTION(err);
        }
        value = value*10 + static_cast<int>(c & 0x0F);
    }
    return value;
}

aku_Timestamp DateTimeUtil::from_iso_string(const char* iso_str) {
    size_t len = std::strlen(iso_str);
    if (len < 15) {
        BadDateTimeFormat error("bad timestamp format (less then 15 digits)");
        BOOST_THROW_EXCEPTION(error);
    }
    const char* pend = iso_str + len; // should point to zero-terminator
    // first four digits - year
    const char* p = iso_str;
    int year = parse_n_digits(p, 4, "can't parse year from timestamp");
    p += 4;
    // then 2 month digits
    int month = parse_n_digits(p, 2, "can't parse month from timestamp");
    p += 2;
    // then 2 date digits
    int date = parse_n_digits(p, 2, "can't parse date from timestamp");
    p += 2;
    // then 'T'
    if (*p != 'T') {
        BadDateTimeFormat error("bad timestamp format, 'T' was expected");
        BOOST_THROW_EXCEPTION(error);
    }
    p++;
    // read two hour digits
    int hour = parse_n_digits(p, 2, "can't parse hours from timestamp");
    p += 2;
    // read two minute digits
    int minute = parse_n_digits(p, 2, "can't parse minutes from timestamp");
    p += 2;
    // read seconds
    int second = parse_n_digits(p, 2, "can't parse seconds from timestamp");
    p += 2;

    // optional
    int nanoseconds = 0;
    if (p != pend) {
        // here should go '.' or ',' according to ISO 8601
        if (*p != '.' && *p != ',') {
            BadDateTimeFormat error("bad timestamp format, ',' or '.' was expected");
            BOOST_THROW_EXCEPTION(error);
        }
        p++;

        // we should have at most 9 digits of nanosecond precision representation
        int n = pend - p;
        nanoseconds = parse_n_digits(p, n, "can't parse fractional part");
        for(int i = 9; i --> n;) {
            nanoseconds *= 10;
        }
    }

    auto gregorian_date = boost::gregorian::date(year, month, date);
    auto time = boost::posix_time::time_duration(hour, minute, second, nanoseconds);
    auto pt = boost::posix_time::ptime(gregorian_date, time);
    return DateTimeUtil::from_boost_ptime(pt);
}

aku_Status DateTimeUtil::to_iso_string(aku_Timestamp ts, char* buffer, size_t buffer_size) {
    // TODO: can be optimized
    boost::posix_time::ptime ptime = to_boost_ptime(ts);
    std::string str = boost::posix_time::to_iso_string(ptime);
    if (str.size() < buffer_size) {
        // OK
        strcpy(buffer, str.c_str());
        return 1 + (int)str.size();
    }
    return -1*static_cast<int>(str.size());
}

}

