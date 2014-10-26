#pragma once
#include "util.h"

struct SearchRange {
    uint32_t begin;  //< Begin index
    uint32_t end;    //< End index
};

/** This is a searcher base class. It is supposed to be used in
  * page search algorithm and inside chunk search algorithm.
  * It can calculate search statistics.
  */
template<class Derived, int SEARCH_QUOTA=4>
struct InterpolationSearch {
    // Derived class must implement:
    // - bool read_at(aku_TimeStamp* out_timestamp, aku_ParamId* out_paramid, uint32_t ix);
    // - bool is_small(SearchRange range);
    // - get_search_stats();

    //! Interpolation search state
    enum I10nState {
        NONE,
        UNDERSHOOT,
        OVERSHOOT
    };

    /** Run search algorithm. Returns true on success, false otherwise.
     *  @param key key to search.
     *  @param prange pointer to search index range
     *  @return true on success.
     */
    bool run(aku_TimeStamp key, SearchRange* prange) {
        SearchRange& range = *prange;
        if (range.begin == range.end) {
            return true;
        }
        const Derived* cderived = static_cast<const Derived*>(this);
        Derived* derived = static_cast<Derived*>(this);
        aku_TimeStamp search_lower_bound;
        aku_TimeStamp search_upper_bound;
        bool success =
            cderived->read_at(range.begin, &search_lower_bound, nullptr) &&
            cderived->read_at(range.end-1, &search_upper_bound, nullptr);
        if (!success) {
            return false;
        }
        uint32_t probe_index = 0u;
        int interpolation_search_quota = SEARCH_QUOTA;
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
            bool range_is_small = cderived->is_small(range);
            if (range_is_small || search_lower_bound == search_upper_bound) {
                small_range_finish = 1;
                break;
            }

            uint64_t numerator = 0u;

            switch(state) {
            case UNDERSHOOT:
                numerator = key - search_lower_bound + (prev_step_err >> steps_count);
                break;
            case OVERSHOOT:
                numerator = key - search_lower_bound - (prev_step_err >> steps_count);
                break;
            default:
                numerator = key - search_lower_bound;
            }

            probe_index = range.begin + ((numerator * (range.end - range.begin)) /
                                          (search_upper_bound - search_lower_bound));

            if (probe_index > range.begin && probe_index < range.end) {

                aku_TimeStamp probe;
                success = cderived->read_at(probe_index, &probe, nullptr);
                if (!success) {
                    return false;
                }

                if (probe < key) {
                    undershoot++;
                    state = UNDERSHOOT;
                    prev_step_err = key - probe;
                    range.begin = probe_index;
                    success = cderived->read_at(range.begin, &search_lower_bound, nullptr);
                    if (!success) {
                        return false;
                    }
                } else if (probe > key) {
                    overshoot++;
                    state = OVERSHOOT;
                    prev_step_err = probe - key;
                    range.end = probe_index;
                    success = cderived->read_at(range.end, &search_upper_bound, nullptr);
                    if (!success) {
                        return false;
                    }
                } else {
                    // probe == key
                    exact_match = 1;
                    range.begin = probe_index;
                    range.end = probe_index;
                    break;
                }
            }
            else {
                break;
            }
        }
        auto& stats = derived->get_search_stats();
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
};
