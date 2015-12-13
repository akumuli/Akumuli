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
#include "compression.h"
#include "akumuli_def.h"
#include "search.h"
#include "buffer_cache.h"

#include <random>
#include <iostream>
#include <boost/crc.hpp>


namespace Akumuli {


// Page
// ----


PageHeader::PageHeader(uint32_t count, uint64_t length, uint32_t page_id, uint32_t numpages)
    : version(0)
    , count(0)
    , next_offset(0)
    , open_count(0)
    , close_count(0)
    , page_id(page_id)
    , numpages(numpages)
    , length(length - sizeof(PageHeader))
{
}

uint32_t PageHeader::get_page_id() const {
    return page_id;
}

uint32_t PageHeader::get_numpages() const {
    return numpages;
}

uint32_t PageHeader::get_open_count() const {
    return open_count;
}

uint32_t PageHeader::get_close_count() const {
    return close_count;
}

void PageHeader::set_open_count(uint32_t cnt) {
    open_count = cnt;
}

void PageHeader::set_close_count(uint32_t cnt) {
    close_count = cnt;
}

void PageHeader::create_checkpoint() {
    checkpoint = count;
}

bool PageHeader::restore() {
    if (count != checkpoint) {
        count = checkpoint;
        return true;
    }
    return false;
}

aku_EntryIndexRecord* PageHeader::page_index(int index) {
    char* ptr = payload + length - sizeof(aku_EntryIndexRecord);
    aku_EntryIndexRecord* entry = reinterpret_cast<aku_EntryIndexRecord*>(ptr);
    entry -= index;
    return entry;
}

const aku_EntryIndexRecord* PageHeader::page_index(int index) const {
    const char* ptr = payload + length - sizeof(aku_EntryIndexRecord);
    const aku_EntryIndexRecord* entry = reinterpret_cast<const aku_EntryIndexRecord*>(ptr);
    entry -= index;
    return entry;
}

std::pair<aku_EntryIndexRecord, int> PageHeader::index_to_offset(uint32_t index) const {
    if (index > count) {
        return std::make_pair(aku_EntryIndexRecord(), AKU_EBAD_ARG);
    }
    return std::make_pair(*page_index(index), AKU_SUCCESS);
}

uint32_t PageHeader::get_entries_count() const {
    return count;
}

size_t PageHeader::get_free_space() const {
    auto begin = payload + next_offset;
    auto end = (payload + length) - count*sizeof(aku_EntryIndexRecord);
    assert(end >= payload);
    return end - begin;
}

void PageHeader::reuse() {
    count = 0;
    checkpoint = 0;
    count = 0;
    open_count++;
    next_offset = 0;
}

void PageHeader::close() {
    close_count++;
}

aku_Status PageHeader::add_entry( const aku_ParamId param
                                , const aku_Timestamp timestamp
                                , const aku_MemRange &range )
{
    if (count != 0) {
        // Require >= timestamp
        if (timestamp < page_index(count - 1)->timestamp) {
            return AKU_EBAD_ARG;
        }
    }

    const auto SPACE_REQUIRED = sizeof(aku_Entry)              // entry header
                              + range.length                   // data size (in bytes)
                              + sizeof(aku_EntryIndexRecord);  // offset inside page_index

    const auto ENTRY_SIZE = sizeof(aku_Entry) + range.length;

    if (!range.length) {
        return AKU_EBAD_DATA;
    }
    if (SPACE_REQUIRED > get_free_space()) {
        return AKU_EOVERFLOW;
    }
    char* free_slot = payload + next_offset;
    aku_Entry* entry = reinterpret_cast<aku_Entry*>(free_slot);
    entry->param_id = param;
    entry->length = range.length;
    memcpy((void*)&entry->value, range.address, range.length);
    page_index(count)->offset = next_offset;
    page_index(count)->timestamp = timestamp;
    next_offset += ENTRY_SIZE;
    count++;
    return AKU_SUCCESS;
}

aku_Status PageHeader::add_chunk(const aku_MemRange range, const uint32_t free_space_required, uint32_t* out_offset) {
    const auto
        SPACE_REQUIRED = range.length + free_space_required,
        SPACE_NEEDED = range.length;
    if (get_free_space() < SPACE_REQUIRED) {
        return AKU_EOVERFLOW;
    }
    *out_offset = next_offset;
    char* free_slot = payload + next_offset;
    memcpy((void*)free_slot, range.address, SPACE_NEEDED);
    next_offset += SPACE_NEEDED;
    return AKU_SUCCESS;
}

aku_Status PageHeader::complete_chunk(const UncompressedChunk& data) {
    CompressedChunkDesc desc;
    Rand rand;
    aku_Timestamp first_ts;
    aku_Timestamp last_ts;

    struct Writer : ChunkWriter {
        PageHeader *header;
        char* begin;
        char* end;

        Writer(PageHeader *h) : header(h) {}

        virtual aku_MemRange allocate() {
            size_t bytes_free = header->get_free_space();
            char* data = header->payload + header->next_offset;
            begin = data;
            return {(void*)data, (uint32_t)bytes_free};
        }

        virtual aku_Status commit(size_t bytes_written) {
            if (bytes_written < header->get_free_space()) {
                header->next_offset += bytes_written;
                end = begin + bytes_written;
                return AKU_SUCCESS;
            }
            return AKU_EOVERFLOW;
        }
    };
    Writer writer(this);

    // Write compressed data
    aku_Status status = CompressionUtil::encode_chunk(&desc.n_elements, &first_ts, &last_ts, &writer, data);
    if (status != AKU_SUCCESS) {
        return status;
    }

    // Calculate checksum of the new compressed data
    boost::crc_32_type checksum;
    checksum.process_block(writer.begin, writer.end);
    desc.checksum = checksum.checksum();
    desc.begin_offset = writer.begin - payload;
    desc.end_offset = writer.end - payload;

    aku_MemRange head = {&desc, sizeof(desc)};
    status = add_entry(AKU_CHUNK_BWD_ID, first_ts, head);
    if (status != AKU_SUCCESS) {
        return status;
    }
    status = add_entry(AKU_CHUNK_FWD_ID, last_ts, head);
    if (status != AKU_SUCCESS) {
        return status;
    }
    return status;
}

const aku_Timestamp PageHeader::read_timestamp_at(uint32_t index) const {
    return page_index(index)->timestamp;
}

const aku_Entry *PageHeader::read_entry_at(uint32_t index) const {
    if (index < count) {
        auto offset = page_index(index)->offset;
        return read_entry(offset);
    }
    return 0;
}

const aku_Entry *PageHeader::read_entry(uint32_t offset) const {
    auto ptr = payload + offset;
    auto entry_ptr = reinterpret_cast<const aku_Entry*>(ptr);
    return entry_ptr;
}

const void* PageHeader::read_entry_data(uint32_t offset) const {
    return payload + offset;
}

int PageHeader::get_entry_length_at(int entry_index) const {
    auto entry_ptr = read_entry_at(entry_index);
    if (entry_ptr) {
        return entry_ptr->length;
    }
    return 0;
}

int PageHeader::get_entry_length(uint32_t offset) const {
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

int PageHeader::copy_entry(uint32_t offset, aku_Entry *receiver) const {
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

SearchStats& get_global_search_stats() {
    static SearchStats stats;
    return stats;
}

namespace {
    struct ChunkHeaderSearcher : InterpolationSearch<ChunkHeaderSearcher> {
        UncompressedChunk const& header;
        ChunkHeaderSearcher(UncompressedChunk const& h) : header(h) {}

        // Interpolation search supporting functions
        bool read_at(aku_Timestamp* out_timestamp, uint32_t ix) const {
            if (ix < header.timestamps.size()) {
                *out_timestamp = header.timestamps[ix];
                return true;
            }
            return false;
        }

        bool is_small(SearchRange range) const {
            return false;
        }

        SearchStats& get_search_stats() {
            return get_global_search_stats();
        }
    };
}

struct SearchAlgorithm : InterpolationSearch<SearchAlgorithm>
{
    const PageHeader *page_;
    std::shared_ptr<QP::IQueryProcessor> query_;
    std::shared_ptr<ChunkCache> cache_;

    const bool          IS_BACKWARD_;
    const aku_Timestamp key_;
    const aku_Timestamp lowerbound_;
    const aku_Timestamp upperbound_;

    SearchRange range_;

    SearchAlgorithm(PageHeader const* page, std::shared_ptr<QP::IQueryProcessor> query, std::shared_ptr<ChunkCache> cache)
        : page_(page)
        , query_(query)
        , cache_(cache)
        , IS_BACKWARD_(query->direction() == AKU_CURSOR_DIR_BACKWARD)
        , key_(IS_BACKWARD_ ? query->upperbound() : query->lowerbound())
        , lowerbound_(query->lowerbound())
        , upperbound_(query->upperbound())
    {
        if (max_index()) {
            range_.begin = 0u;
            range_.end = max_index() - 1;
        } else {
            range_.begin = 0u;
            range_.end = 0u;
        }
    }

    uint32_t max_index() const {
        return page_->get_entries_count();
    }

    bool fast_path() {
        while (!max_index()) {
            if (page_->get_page_id() == 0 && page_->get_close_count() == 0) {
                // Special case. Database is new and there is no data yet.
                std::cout << "PAGE - search fast path - new database detected" << std::endl;
                if (query_->put(QP::NO_DATA)) {
                    continue;
                }
                break;
            }
            return true;
        }

        if (key_ > page_->page_index(range_.end)->timestamp ||
            key_ < page_->page_index(range_.begin)->timestamp)
        {
            // Shortcut for corner cases
            if (key_ > page_->page_index(range_.end)->timestamp) {
                if (IS_BACKWARD_) {
                    range_.begin = range_.end;
                    return false;
                } else {
                    // return empty result
                    return true;
                }
            }
            else if (key_ < page_->page_index(range_.begin)->timestamp) {
                if (!IS_BACKWARD_) {
                    range_.end = range_.begin;
                    return false;
                } else {
                    // return empty result
                    return true;
                }
            }
        }
        return false;
    }

    // Interpolation search supporting functions
    bool read_at(aku_Timestamp* out_timestamp, uint32_t ix) const {
        if (ix < page_->get_entries_count()) {
            *out_timestamp = page_->page_index(ix)->timestamp;
            return true;
        }
        return false;
    }

    bool is_small(SearchRange range) const {
        auto ps = get_page_size();
        auto b = align_to_page(reinterpret_cast<void const*>(page_->read_entry_at(range.begin)), ps);
        auto e = align_to_page(reinterpret_cast<void const*>(page_->read_entry_at(range.end)), ps);
        return b == e;
    }

    SearchStats& get_search_stats() {
        return get_global_search_stats();
    }

    bool interpolation() {
        if (!run(key_, &range_)) {
            query_->set_error(AKU_ENOT_FOUND);
            return false;
        }
        return true;
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
            if (probe_index >= max_index()) {
                query_->set_error(AKU_EOVERFLOW);
                range_.begin = range_.end = max_index();
                return;
            }

            auto probe = page_->page_index(probe_index)->timestamp;

            if (probe == key_) {                         // found
                break;
            } else if (probe < key_) {
                range_.begin = probe_index + 1u;         // change min index to search upper subarray
                if (range_.begin >= max_index()) {        // we hit the upper bound of the array
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

    enum ScanResultT {
        OVERSHOOT,
        UNDERSHOOT,
        IN_RANGE,
        INTERRUPTED,
    };

    ScanResultT scan_compressed_entries(uint32_t current_index,
                                        aku_Entry const* probe_entry,
                                        bool binary_search=false)
    {
        aku_Status status = AKU_SUCCESS;
        ScanResultT result = UNDERSHOOT;
        std::shared_ptr<UncompressedChunk> chunk_header, header;

        auto npages = page_->get_numpages();    // This needed to prevent key collision
        auto nopens = page_->get_open_count();  // between old and new page data, when
        auto pageid = page_->get_page_id();     // page is reallocated.

        auto key = std::make_tuple(npages*nopens + pageid, current_index);

        if (cache_ && cache_->contains(key)) {
            // Fast path
            header = cache_->get(key);
        } else {
            chunk_header.reset(new UncompressedChunk());
            header.reset(new UncompressedChunk());
            auto pdesc  = reinterpret_cast<CompressedChunkDesc const*>(&probe_entry->value[0]);
            auto pbegin = (const unsigned char*)page_->read_entry_data(pdesc->begin_offset);
            auto pend   = (const unsigned char*)page_->read_entry_data(pdesc->end_offset);
            auto probe_length = pdesc->n_elements;

            boost::crc_32_type checksum;
            checksum.process_block(pbegin, pend);
            if (checksum.checksum() != pdesc->checksum) {
                AKU_PANIC("File damaged!");
            }

            status = CompressionUtil::decode_chunk(chunk_header.get(), pbegin, pend, probe_length);
            if (status != AKU_SUCCESS) {
                AKU_PANIC("Can't decode chunk");
            }

            // TODO: depending on a query type we can use chunk order or convert back to time-order.
            // If we extract evertyhing it is better to convert to time order. If we picking some
            // parameter ids it is better to check if this ids present in a chunk and extract values
            // in chunk order and only after that - convert results to time-order.

            // Convert from chunk order to time order
            if (!CompressionUtil::convert_from_chunk_order(*chunk_header, header.get())) {
                AKU_PANIC("Bad chunk");
            }

            if (cache_) {
                cache_->put(key, header);
            }
        }

        int start_pos = 0;
        if (IS_BACKWARD_) {
            start_pos = static_cast<int>(header->timestamps.size() - 1);
        }

        auto queryproc = query_;
        auto page = page_;

        auto put_entry = [&header, queryproc, page] (uint32_t i) {
            auto id = header->paramids.at(i);
            if (queryproc->filter().apply(id) == QP::IQueryFilter::PROCESS) {
                aku_PData pdata;
                pdata.type = AKU_PAYLOAD_FLOAT;
                pdata.float64 = header->values.at(i);
                pdata.size = sizeof(aku_Sample);
                aku_Sample result = {
                    header->timestamps.at(i),
                    header->paramids.at(i),
                    pdata,
                };
                return queryproc->put(result);
            }
            return true;
        };

        if (IS_BACKWARD_) {
            for (int i = static_cast<int>(start_pos); i >= 0; i--) {
                result = check_timestamp(header->timestamps[i]);
                if (result == OVERSHOOT) {
                    break;
                }
                if (result == IN_RANGE) {
                    if (!put_entry(i)) {
                        // Scaning process interrupted by the user (connection closed)
                        result = INTERRUPTED;
                        break;
                    }
                }
            }
        } else {
            auto end_pos = (int)header->timestamps.size();
            for (auto i = start_pos; i != end_pos; i++) {
                result = check_timestamp(header->timestamps[i]);
                if (result == OVERSHOOT) {
                    break;
                }
                if (result == IN_RANGE) {
                    if (!put_entry(i)) {
                        result = INTERRUPTED;
                        break;
                    }
                }
            }
        }
        return result;
    }

    ScanResultT check_timestamp(aku_Timestamp probe_time)
    {
        ScanResultT proceed;
        if (IS_BACKWARD_) {
            if (probe_time > upperbound_) {
                proceed = UNDERSHOOT;
            } else if (probe_time < lowerbound_) {
                proceed = OVERSHOOT;
            } else {
                proceed = IN_RANGE;
            }
        } else {
            if (probe_time > upperbound_) {
                proceed = OVERSHOOT;
            } else if (probe_time < lowerbound_) {
                proceed = UNDERSHOOT;
            } else {
                proceed = IN_RANGE;
            }
        }
        return proceed;
    }

    /**
     * @brief scan_impl is a scan procedure impelementation
     * @param probe_index is an index to start with
     * @return tuple{fwd-bytes, bwd-bytes}
     */
    std::tuple<uint64_t, uint64_t> scan_impl(uint32_t probe_index) {
        int index_increment = IS_BACKWARD_ ? -1 : 1;
        ScanResultT proceed = IN_RANGE;
        while (proceed != INTERRUPTED) {
            auto current_index = probe_index;
            probe_index += index_increment;
            auto probe_offset = page_->page_index(current_index)->offset;
            auto probe_time = page_->page_index(current_index)->timestamp;
            auto probe_entry = page_->read_entry(probe_offset);
            auto probe = probe_entry->param_id;


            if (probe == AKU_CHUNK_FWD_ID && IS_BACKWARD_ == false) {
                proceed = scan_compressed_entries(current_index, probe_entry, false);
            } else if (probe == AKU_CHUNK_BWD_ID && IS_BACKWARD_ == true) {
                proceed = scan_compressed_entries(current_index, probe_entry, false);
            } else {
                proceed = check_timestamp(probe_time);
            }

            if (probe_index >= max_index()) {
                if (IS_BACKWARD_) {
                    proceed = INTERRUPTED;
                } else {
                    switch(proceed) {
                    case IN_RANGE:
                    case UNDERSHOOT:
                        if (query_->put(QP::NO_DATA)) {
                            // We should wait for consumer!
                            break;
                        }
                    case OVERSHOOT:
                    case INTERRUPTED:
                        proceed = INTERRUPTED;
                    };
                }
            }
        }
        // TODO: use relevant numbers here!
        return std::make_tuple(0ul, 0ul);
    }

    void scan() {
        if (range_.begin != range_.end) {
            query_->set_error(AKU_EGENERAL);
            return;
        }
        if (range_.begin >= max_index()) {
            query_->set_error(AKU_EOVERFLOW);
            return;
        }

        auto sums = scan_impl(range_.begin);

        auto& stats = get_global_search_stats();
        {
            std::lock_guard<std::mutex> guard(stats.mutex);
            stats.stats.scan.fwd_bytes += std::get<0>(sums);
            stats.stats.scan.bwd_bytes += std::get<1>(sums);
        }
    }
};


void PageHeader::search(std::shared_ptr<QP::IQueryProcessor> query, std::shared_ptr<ChunkCache> cache) const {
    SearchAlgorithm search_alg(this, query, cache);
    if (search_alg.fast_path() == false) {
        if (search_alg.interpolation()) {
            search_alg.binary_search();
            search_alg.scan();
        }
    }
}

void PageHeader::get_stats(aku_StorageStats* rcv_stats) {
    uint64_t used_space = 0,
             free_space = 0,
              n_entries = 0;

    auto all = length;
    auto free = get_free_space();
    used_space = all - free;
    free_space = free;
    n_entries = count;

    rcv_stats->free_space += free_space;
    rcv_stats->used_space += used_space;
    rcv_stats->n_entries += n_entries;
    rcv_stats->n_volumes += 1;
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
