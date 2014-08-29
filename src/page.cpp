/**
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

#include <cstring>
#include <cassert>
#include <algorithm>
#include <mutex>
#include <apr_time.h>
#include "timsort.hpp"
#include "page.h"
#include "akumuli_def.h"

#include <random>
#include <iostream>


namespace Akumuli {

std::ostream& operator << (std::ostream& st, CursorResult res) {
    st << "CursorResult" << boost::to_string(res);
    return st;
}

//------------------------

static SearchQuery::ParamMatch single_param_matcher(aku_ParamId a, aku_ParamId b) {
    if (a == b) {
        return SearchQuery::MATCH;
    }
    return SearchQuery::NO_MATCH;
}

SearchQuery::SearchQuery( aku_ParamId   param_id
                        , aku_TimeStamp low
                        , aku_TimeStamp upp
                        , int           scan_dir)
    : lowerbound(low)
    , upperbound(upp)
    , param_pred(std::bind(&single_param_matcher, param_id, std::placeholders::_1))
    , direction(scan_dir)
{
}

SearchQuery::SearchQuery(MatcherFn matcher
                        , aku_TimeStamp low
                        , aku_TimeStamp upp
                        , int scan_dir)
    : lowerbound(low)
    , upperbound(upp)
    , param_pred(matcher)
    , direction(scan_dir)
{
}

// Page
// ----

PageBoundingBox::PageBoundingBox()
    : max_id(0)
    , min_id(std::numeric_limits<uint32_t>::max())
{
    max_timestamp = AKU_MIN_TIMESTAMP;
    min_timestamp = AKU_MAX_TIMESTAMP;
}


const char* PageHeader::cdata() const {
    return reinterpret_cast<const char*>(this);
}

char* PageHeader::data() {
    return reinterpret_cast<char*>(this);
}

PageHeader::PageHeader(uint32_t count, uint64_t length, uint32_t page_id)
    : version(0)
    , count(count)
    , last_offset(length - 1)
    , sync_count(0)
    , length(length)
    , open_count(0)
    , close_count(0)
    , page_id(page_id)
    , compression(1)  // TODO: get actual value from configuration
    , bbox()
{
    // zero out histogram
    memset(&histogram, 0, sizeof(histogram));
}

std::pair<aku_EntryOffset, int> PageHeader::index_to_offset(uint32_t index) const {
    if (index < 0 || index > count) {
        return std::make_pair(0u, AKU_EBAD_ARG);
    }
    return std::make_pair(page_index[index], AKU_SUCCESS);
}

int PageHeader::get_entries_count() const {
    return (int)this->count;
}

size_t PageHeader::get_free_space() const {
    auto begin = reinterpret_cast<const char*>(page_index + count);
    const char* end = cdata();
    end += last_offset;
    assert(end >= begin);
    return end - begin;
}

void PageHeader::update_bounding_box(aku_ParamId param, aku_TimeStamp time) {
    if (param > bbox.max_id) {
        bbox.max_id = param;
    }
    if (param < bbox.min_id) {
        bbox.min_id = param;
    }
    if (time > bbox.max_timestamp) {
        bbox.max_timestamp = time;
    }
    if (time < bbox.min_timestamp) {
        bbox.min_timestamp = time;
    }
}

bool PageHeader::inside_bbox(aku_ParamId param, aku_TimeStamp time) const {
    return time  <= bbox.max_timestamp
        && time  >= bbox.min_timestamp
        && param <= bbox.max_id
        && param >= bbox.min_id;
}

void PageHeader::reuse() {
    count = 0;
    open_count++;
    last_offset = length - 1;
    bbox = PageBoundingBox();
}

void PageHeader::close() {
    close_count++;
}

int PageHeader::add_entry( const aku_ParamId param
                          , const aku_TimeStamp timestamp
                          , const aku_MemRange range ) 
{

    const auto SPACE_REQUIRED = sizeof(aku_Entry)         // entry header
                              + range.length              // data size (in bytes)
                              + sizeof(aku_EntryOffset);  // offset inside page_index

    const auto ENTRY_SIZE = sizeof(aku_Entry) + range.length;

    if (!range.length) {
        return AKU_WRITE_STATUS_BAD_DATA;
    }
    if (SPACE_REQUIRED > get_free_space()) {
        return AKU_WRITE_STATUS_OVERFLOW;
    }
    char* free_slot = data() + last_offset;
    free_slot -= ENTRY_SIZE;
    aku_Entry* entry = reinterpret_cast<aku_Entry*>(free_slot);
    entry->param_id = param;
    entry->time = timestamp;
    entry->length = range.length;
    memcpy((void*)entry->value, range.address, range.length);
    last_offset = free_slot - cdata();
    page_index[count] = last_offset;
    count++;
    update_bounding_box(param, timestamp);
    return AKU_WRITE_STATUS_SUCCESS;
}

int PageHeader::add_chunk(const aku_MemRange range, const uint32_t free_space_required) {
    const auto
        SPACE_REQUIRED = range.length + free_space_required,
        SPACE_NEEDED = range.length;
    if (get_free_space() < SPACE_REQUIRED) {
        return AKU_EOVERFLOW;
    }
    char* free_slot = data() + last_offset;
    free_slot -= SPACE_NEEDED;
    memcpy((void*)free_slot, range.address, SPACE_NEEDED);
    last_offset = free_slot - cdata();
    return AKU_SUCCESS;
}

const aku_Entry *PageHeader::read_entry_at(uint32_t index) const {
    if (index >= 0 && index < count) {
        auto offset = page_index[index];
        return read_entry(offset);
    }
    return 0;
}

const aku_Entry *PageHeader::read_entry(aku_EntryOffset offset) const {
    auto ptr = cdata() + offset;
    auto entry_ptr = reinterpret_cast<const aku_Entry*>(ptr);
    return entry_ptr;
}

int PageHeader::get_entry_length_at(int entry_index) const {
    auto entry_ptr = read_entry_at(entry_index);
    if (entry_ptr) {
        return entry_ptr->length;
    }
    return 0;
}

int PageHeader::get_entry_length(aku_EntryOffset offset) const {
    auto entry_ptr = read_entry(offset);
    if (entry_ptr) {
        return entry_ptr->length;
    }
    return 0;
}

int PageHeader::copy_entry_at(int index, aku_Entry *receiver) const {
    auto entry_ptr = read_entry_at(index);
    if (entry_ptr) {
        size_t size = entry_ptr->length + sizeof(aku_Entry);
        if (entry_ptr->length > receiver->length) {
            return -1*entry_ptr->length;
        }
        memcpy((void*)receiver, (void*)entry_ptr, size);
        return entry_ptr->length;
    }
    return 0;
}

int PageHeader::copy_entry(aku_EntryOffset offset, aku_Entry *receiver) const {
    auto entry_ptr = read_entry(offset);
    if (entry_ptr) {
        if (entry_ptr->length > receiver->length) {
            return -1*entry_ptr->length;
        }
        memcpy((void*)receiver, (void*)entry_ptr, entry_ptr->length);
        return entry_ptr->length;
    }
    return 0;
}


/** Return false if query is ill-formed.
  * Status and error code fields will be changed accordignly.
  */
static bool validate_query(SearchQuery const& query) {
    // Cursor validation
    if ((query.direction != AKU_CURSOR_DIR_BACKWARD && query.direction != AKU_CURSOR_DIR_FORWARD) ||
         query.upperbound < query.lowerbound)
    {
        return false;
    }
    return true;
}

struct SearchStats {
    aku_SearchStats stats;
    std::mutex mutex;

    SearchStats() {
        memset(&stats, 0, sizeof(stats));
    }
};

static SearchStats& get_global_search_stats() {
    static SearchStats stats;
    return stats;
}

struct SearchRange {
    uint32_t begin;
    uint32_t end;

    bool is_small(PageHeader const* page) {
        auto ps = get_page_size();
        auto b = align_to_page(reinterpret_cast<void const*>(page->read_entry_at(begin)), ps);
        auto e = align_to_page(reinterpret_cast<void const*>(page->read_entry_at(end)), ps);
        return b == e;
    }
};

struct SearchAlgorithm {
    PageHeader const* page_;
    Caller& caller_;
    InternalCursor* cursor_;
    SearchQuery query_;

    const uint32_t MAX_INDEX_;
    const bool IS_BACKWARD_;
    const aku_TimeStamp key_;

    SearchRange range_;

    //! Interpolation search state
    enum I10nState {
        NONE,
        UNDERSHOOT,
        OVERSHOOT
    };

    SearchAlgorithm(PageHeader const* page, Caller& caller, InternalCursor* cursor, SearchQuery query)
        : page_(page)
        , caller_(caller)
        , cursor_(cursor)
        , query_(query)
        , MAX_INDEX_(page->sync_count)
        , IS_BACKWARD_(query.direction == AKU_CURSOR_DIR_BACKWARD)
        , key_(IS_BACKWARD_ ? query.upperbound : query.lowerbound)
    {
        if (MAX_INDEX_) {
            range_.begin = 0u;
            range_.end = MAX_INDEX_ - 1;
        } else {
            range_.begin = 0u;
            range_.end = 0u;
        }
    }

    bool fast_path() {
        if (!MAX_INDEX_) {
            cursor_->complete(caller_);
            return true;
        }

        if (!validate_query(query_)) {
            cursor_->set_error(caller_, AKU_SEARCH_EBAD_ARG);
            return true;
        }

        if (key_ > page_->bbox.max_timestamp || key_ < page_->bbox.min_timestamp) {
            // Shortcut for corner cases
            if (key_ > page_->bbox.max_timestamp) {
                if (IS_BACKWARD_) {
                    range_.begin = range_.end;
                    return false;
                } else {
                    // return empty result
                    cursor_->complete(caller_);
                    return true;
                }
            }
            else if (key_ < page_->bbox.min_timestamp) {
                if (!IS_BACKWARD_) {
                    range_.end = range_.begin;
                    return false;
                } else {
                    // return empty result
                    cursor_->complete(caller_);
                    return true;
                }
            }
        }
        return false;
    }

    void histogram() {
        auto const& h = page_->histogram;
        auto pred = [](PageHistogramEntry const& a, PageHistogramEntry const& b) {
            return a.timestamp < b.timestamp;
        };
        PageHistogramEntry hkey = { key_, 0 };
        auto begin = h.entries;
        auto end = begin + h.size;
        auto upper = std::upper_bound(begin, end, hkey, pred);
        auto lower = std::lower_bound(begin, end, hkey, pred);
        if (lower != end) {
            if (lower != begin && lower->timestamp > hkey.timestamp) {
                lower--;
            }
            range_.begin = lower->index;
        }
        if (upper != end) {
            range_.end = upper->index;
        }
    }

    void interpolation() {
        if (range_.begin == range_.end) {
            return;
        }
        aku_TimeStamp search_lower_bound = page_->read_entry_at(range_.begin)->time;
        aku_TimeStamp search_upper_bound = page_->read_entry_at(range_.end - 1)->time;
        uint32_t probe_index = 0u;
        int interpolation_search_quota = 4;  // TODO: move to configuration
        int steps_count = 0;
        int small_range_finish = 0;
        int page_scan_steps_num = 0;
        int page_scan_errors = 0;
        int page_scan_success = 0;
        int page_miss = 0;

        uint64_t overshoot = 0u;
        uint64_t undershoot = 0u;
        uint64_t exact_match = 0u;
        aku_TimeStamp prev_step_err = 0u;
        I10nState state = NONE;

        while(steps_count++ < interpolation_search_quota)  {
            // On small distances - fallback to binary search
            if (range_.is_small(page_)) {
                small_range_finish = 1;
                break;
            }

            uint64_t numerator = 0u;

            switch(state) {
            case UNDERSHOOT:
                numerator = key_ - search_lower_bound + (prev_step_err >> steps_count);
                break;
            case OVERSHOOT:
                numerator = key_ - search_lower_bound - (prev_step_err >> steps_count);
                break;
            default:
                numerator = key_ - search_lower_bound;
            }

            probe_index = range_.begin + ((numerator * (range_.end - range_.begin)) /
                                          (search_upper_bound - search_lower_bound));

            if (probe_index > range_.begin && probe_index < range_.end) {

                auto probe_offset = page_->page_index[probe_index];
                auto probe_entry = page_->read_entry(probe_offset);
                // TODO: count page faults
                auto probe = probe_entry->time;

                if (probe < key_) {
                    undershoot++;
                    state = UNDERSHOOT;
                    prev_step_err = key_ - probe;
                    range_.begin = probe_index;
                    probe_offset = page_->page_index[range_.begin];
                    probe_entry = page_->read_entry(probe_offset);
                    search_lower_bound = probe_entry->time;
                } else if (probe > key_) {
                    overshoot++;
                    state = OVERSHOOT;
                    prev_step_err = probe - key_;
                    range_.end = probe_index;
                    probe_offset = page_->page_index[range_.end];
                    probe_entry = page_->read_entry(probe_offset);
                    search_upper_bound = probe_entry->time;
                } else {
                    // probe == key_
                    exact_match = 1;
                    range_.begin = probe_index;
                    range_.end = probe_index;
                    break;
                }
            }
            else {
                break;
                // Continue with binary search
            }
        }
        auto& stats = get_global_search_stats();
        std::lock_guard<std::mutex> lock(stats.mutex);
        stats.stats.istats.n_matches += exact_match;
        stats.stats.istats.n_overshoots += overshoot;
        stats.stats.istats.n_undershoots += undershoot;
        stats.stats.istats.n_times += 1;
        stats.stats.istats.n_steps += steps_count;
        stats.stats.istats.n_reduced_to_one_page += small_range_finish;
        stats.stats.istats.n_page_in_core_checks += page_scan_steps_num;
        stats.stats.istats.n_page_in_core_errors += page_scan_errors;
        stats.stats.istats.n_pages_in_core_found += page_scan_success;
        stats.stats.istats.n_pages_in_core_miss += page_miss;
    }

    void binary_search() {
        uint64_t steps = 0ul;
        if (range_.begin == range_.end) {
            return;
        }
        uint32_t probe_index = 0u;
        while (range_.end >= range_.begin) {
            steps++;
            probe_index = range_.begin + ((range_.end - range_.begin) / 2u);
	    if (probe_index >= MAX_INDEX_) {
	        cursor_->set_error(caller_, AKU_EOVERFLOW);
	        range_.begin = range_.end = MAX_INDEX_;
	        return;
            }
            auto probe_offset = page_->page_index[probe_index];
            auto probe_entry = page_->read_entry(probe_offset);
            auto probe = probe_entry->time;

            if (probe == key_) {                         // found
                break;
            } else if (probe < key_) {
                range_.begin = probe_index + 1u;         // change min index to search upper subarray
                if (range_.begin >= MAX_INDEX_) {        // we hit the upper bound of the array
                    break;
                }
            } else {
                range_.end = probe_index - 1u;           // change max index to search lower subarray
                if (range_.end == ~0u) {                 // we hit the lower bound of the array
                    break;
                }
            }
        }
        range_.begin = probe_index;
        range_.end = probe_index;

        auto& stats = get_global_search_stats();
        std::lock_guard<std::mutex> guard(stats.mutex);
        auto& bst = stats.stats.bstats;
        bst.n_times += 1;
        bst.n_steps += steps;
    }

    void scan() {
        if (range_.begin != range_.end) {
            cursor_->set_error(caller_, AKU_EGENERAL);
            return;
        }
	if (range_.begin >= MAX_INDEX_) {
	    cursor_->set_error(caller_, AKU_EOVERFLOW);
	    return;
	}
        if (range_.begin < MAX_INDEX_) {
            uint64_t start_offset = 0ul,
                     stop_offset = 0ul;
#ifdef DEBUG
            // Debug variables
            aku_TimeStamp dbg_prev_ts;
            long dbg_count = 0;
#endif
            auto probe_index = range_.begin;
            start_offset = page_->page_index[probe_index];
            if (IS_BACKWARD_) {
                while (true) {
                    auto current_index = probe_index--;
                    auto probe_offset = page_->page_index[current_index];
                    auto probe_entry = page_->read_entry(probe_offset);
                    auto probe = probe_entry->param_id;
                    bool probe_in_time_range = query_.lowerbound <= probe_entry->time &&
                                               query_.upperbound >= probe_entry->time;
                    if (query_.param_pred(probe) == SearchQuery::MATCH && probe_in_time_range) {
#ifdef DEBUG
                        if (dbg_count) {
                            // check for backward direction
                            auto is_ok = dbg_prev_ts >= probe_entry->time;
                            assert(is_ok);
                        }
                        dbg_prev_ts = probe_entry->time;
                        dbg_count++;
#endif
                        if (!cursor_->put(caller_, probe_offset, page_)) {
                            break;
                        }
                    }
                    if (probe_entry->time < query_.lowerbound || current_index == 0u) {
                        stop_offset = probe_offset;
                        break;
                    }
                }
            } else {
                while (true) {
                    auto current_index = probe_index++;
                    if (current_index >= MAX_INDEX_) {
                        break;
                    }
                    auto probe_offset = page_->page_index[current_index];
                    auto probe_entry = page_->read_entry(probe_offset);
                    auto probe = probe_entry->param_id;
                    bool probe_in_time_range = query_.lowerbound <= probe_entry->time &&
                                               query_.upperbound >= probe_entry->time;
                    if (query_.param_pred(probe) == SearchQuery::MATCH  && probe_in_time_range) {
#ifdef DEBUG
                        if (dbg_count) {
                            // check for forward direction
                            auto is_ok = dbg_prev_ts <= probe_entry->time;
                            assert(is_ok);
                        }
                        dbg_prev_ts = probe_entry->time;
                        dbg_count++;
#endif
                        if (!cursor_->put(caller_, probe_offset, page_)) {
                            break;
                        }
                        stop_offset = probe_offset;
                    }
                    if (probe_entry->time > query_.upperbound) {
                        break;
                    }
                }
            }
            auto& stats = get_global_search_stats();
            {
                std::lock_guard<std::mutex> guard(stats.mutex);
                auto& scan_stats = stats.stats.scan;
                uint64_t sum;
                if (stop_offset < start_offset) {
                    sum = start_offset - stop_offset;
                } else {
                    sum = stop_offset - start_offset;
                }
                if (IS_BACKWARD_) {
                    scan_stats.bwd_bytes += sum;
                } else {
                    scan_stats.fwd_bytes += sum;
                }
            }
        }
        cursor_->complete(caller_);
    }
};

void PageHeader::search(Caller& caller, InternalCursor* cursor, SearchQuery query) const
{
    SearchAlgorithm search_alg(this, caller, cursor, query);
    if (search_alg.fast_path() == false) {
        search_alg.histogram();
        search_alg.interpolation();
        search_alg.binary_search();
        search_alg.scan();
    }
}

void PageHeader::_sort() {
    // This method is only for testing purposes.
    // Page invariants can break here.
    auto begin = page_index + sync_count;
    auto end = page_index + count;
    std::sort(begin, end, [&](aku_EntryOffset a, aku_EntryOffset b) {
        auto ea = read_entry(a);
        auto eb = read_entry(b);
        auto ta = std::tuple<aku_TimeStamp, aku_ParamId>(ea->time, ea->param_id);
        auto tb = std::tuple<aku_TimeStamp, aku_ParamId>(eb->time, eb->param_id);
        return ta < tb;
    });
    sync_count = count;
}

void PageHeader::sync_next_index(aku_EntryOffset offset, uint32_t rand_val, bool sort_histogram) {
    // sync_count updated only here! 
    if (!sort_histogram) {
        if (sync_count >= count) {
            AKU_PANIC("sync_index out of range");
        }
        auto index = sync_count++;
        page_index[index] = offset;

        if (histogram.size < AKU_HISTOGRAM_SIZE) {
            // first AKU_HISTOGRAM_SIZE samples
            auto& h = histogram.entries[histogram.size++];
            h.index = index;
            h.timestamp = read_entry(offset)->time;
        } else {
            // reservoir sampling
            auto rindex = static_cast<uint32_t>(rand_val % sync_count);
            if (rindex < histogram.size) {
                auto& h = histogram.entries[rindex];
                h.index = index;
                h.timestamp = read_entry(offset)->time;
            }
        }
    } else {
        gfx::timsort(histogram.entries, histogram.entries + histogram.size,
                  [](PageHistogramEntry const& a, PageHistogramEntry const& b) {
                        return a.timestamp < b.timestamp;
                  }
        );
    }
}

void PageHeader::get_search_stats(aku_SearchStats* stats, bool reset) {
    auto& gstats = get_global_search_stats();
    std::lock_guard<std::mutex> guard(gstats.mutex);

    memcpy( reinterpret_cast<void*>(stats)
          , reinterpret_cast<void*>(&gstats.stats)
          , sizeof(aku_SearchStats));

    if (reset) {
        memset(reinterpret_cast<void*>(&gstats.stats), 0, sizeof(aku_SearchStats));
    }
}

}  // namepsace
