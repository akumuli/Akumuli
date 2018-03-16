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
#include <cstdio>
#include <boost/regex.hpp>

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
    u64 ns = duration.total_nanoseconds();
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
    u32 len = static_cast<u32>(std::strlen(iso_str));
    if (len == 0) {
        BadDateTimeFormat error("empty timestamp value");
        BOOST_THROW_EXCEPTION(error);
    }
    // Trim left
    while(!isdigit(*iso_str)) {
        iso_str++;
        len--;
        if (len == 0) {
            break;
        }
    }
    if (len < 15 || iso_str[8] != 'T') {
        // Raw timestamp
        aku_Timestamp ts;
        char* end;
        ts = strtoull(iso_str, &end, 10);
        if (errno == ERANGE) {
            BadDateTimeFormat error("can't parse unix-timestamp from string");
            BOOST_THROW_EXCEPTION(error);
        }
        long parsed_len = end - iso_str;
        if (parsed_len < len) {
            BadDateTimeFormat error("unknown timestamp format");
            BOOST_THROW_EXCEPTION(error);
        }
        return ts;
    }
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
    try {
        auto gregorian_date = boost::gregorian::date(year, month, date);
        auto time = boost::posix_time::time_duration(hour, minute, second, nanoseconds);
        auto pt = boost::posix_time::ptime(gregorian_date, time);
        return DateTimeUtil::from_boost_ptime(pt);
    } catch (std::out_of_range const& range_error) {
        // Invalid date parameter
        BadDateTimeFormat error(range_error.what());
        BOOST_THROW_EXCEPTION(error);
    }
}

int DateTimeUtil::to_iso_string(aku_Timestamp ts, char* buffer, size_t buffer_size) {
    using namespace boost::gregorian;
    using namespace boost::posix_time;
    ptime ptime = to_boost_ptime(ts);
    date date = ptime.date();
    time_duration time = ptime.time_of_day();
    gregorian_calendar::ymd_type ymd = gregorian_calendar::from_day_number(date.day_number());

    auto fracsec = time.fractional_seconds();

    int len = snprintf(buffer, buffer_size, "%04d%02d%02dT%02d%02d%02d.%09d",
             // date part
             (int)ymd.year, (int)ymd.month, (int)ymd.day,
             // time part
             (int)time.hours(), (int)time.minutes(), (int)time.seconds(), (int)fracsec
             );

    if (len < 0 || len == (int)buffer_size) {
        return -26;
    }
    return len + 1;
}

aku_Duration DateTimeUtil::parse_duration(const char* str, size_t size) {
    static const char* exp = R"(^(\d+)(n|us|s|min|ms|m|h)?$)";
    static boost::regex regex(exp, boost::regex_constants::optimize);
    boost::cmatch m;
    if (!boost::regex_match(str, m, regex)) {
        BadDateTimeFormat bad_duration("bad duration");
        BOOST_THROW_EXCEPTION(bad_duration);
    }
    auto num = m[1];
    auto unit = m[2].first;
    auto unitlen = m[2].second - m[2].first;
    auto K = 0ul;
    if (unitlen > 0) {
        switch(unit[0]) {
        case 'n':  // nanosecond
            K = 1ul;
            break;
        case 'u':  // microsecond
            K = 1000ul;
            break;
        case 's':  // second
            K = 1000000000ul;
            break;
        case 'm':
            switch(unitlen) {
            case 1:
            case 3:  // minute
                K = 60*1000000000ul;
                break;
            case 2:  // milisecond
                K = 1000000ul;
                break;
            }
            break;
        case 'h':  // hour
            K = 60*60*1000000000ul;
            break;
        }
        if (K == 0ul) {
            BadDateTimeFormat err("unknown time duration unit");
            BOOST_THROW_EXCEPTION(err);
        }
    } else {
        K = 1ul;
    }
    return K*atoll(num.first);
}


}

