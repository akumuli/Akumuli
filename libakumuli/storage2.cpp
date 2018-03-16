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


#include "storage2.h"
#include "util.h"
#include "queryprocessor.h"
#include "query_processing/queryparser.h"
#include "query_processing/queryplan.h"
#include "log_iface.h"
#include "status_util.h"
#include "datetime.h"
#include "akumuli_version.h"

#include <algorithm>
#include <atomic>
#include <sstream>
#include <cassert>
#include <functional>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>

#include <fcntl.h>
#include <cstdlib>

namespace Akumuli {

// Utility functions & classes //


// Standalone functions //

/** This function creates metadata file - root of the storage system.
  * This page contains creation date and time, number of pages,
  * all the page file names and they order.
  * @return APR_EINIT on DB error.
  */
static apr_status_t create_metadata_page(const char* db_name
                                        , const char* file_name
                                        , std::vector<std::string> const& page_file_names
                                        , std::vector<u32> const& capacities
                                        , const char* bstore_type )
{
    using namespace std;
    try {
        auto storage = std::make_shared<MetadataStorage>(file_name);

        auto now = apr_time_now();
        char date_time[0x100];
        apr_rfc822_date(date_time, now);

        storage->init_config(db_name, date_time, bstore_type);

        std::vector<MetadataStorage::VolumeDesc> desc;
        u32 ix = 0;
        for(auto str: page_file_names) {
            MetadataStorage::VolumeDesc volume;
            volume.path = str;
            volume.generation = ix;
            volume.capacity = capacities[ix];
            volume.id = ix;
            volume.nblocks = 0;
            volume.version = AKUMULI_VERSION;
            desc.push_back(volume);
            ix++;
        }
        storage->init_volumes(desc);

    } catch (std::exception const& err) {
        std::stringstream fmt;
        fmt << "Can't create metadata file " << file_name << ", the error is: " << err.what();
        Logger::msg(AKU_LOG_ERROR, fmt.str().c_str());
        return APR_EGENERAL;
    }
    return APR_SUCCESS;
}

//--------- StorageSession ----------

StorageSession::StorageSession(std::shared_ptr<Storage> storage, std::shared_ptr<StorageEngine::CStoreSession> session)
    : storage_(storage)
    , session_(session)
    , matcher_substitute_(nullptr)
{
}

aku_Status StorageSession::write(aku_Sample const& sample) {
    using namespace StorageEngine;
    std::vector<u64> rpoints;
    auto status = session_->write(sample, &rpoints);
    switch (status) {
    case NBTreeAppendResult::OK:
        return AKU_SUCCESS;
    case NBTreeAppendResult::OK_FLUSH_NEEDED:
        storage_-> _update_rescue_points(sample.paramid, std::move(rpoints));
        return AKU_SUCCESS;
    case NBTreeAppendResult::FAIL_BAD_ID:
        AKU_PANIC("Invalid session cache, id = " + std::to_string(sample.paramid));
    case NBTreeAppendResult::FAIL_LATE_WRITE:
        return AKU_ELATE_WRITE;
    case NBTreeAppendResult::FAIL_BAD_VALUE:
        return AKU_EBAD_ARG;
    };
    return AKU_SUCCESS;
}

aku_Status StorageSession::init_series_id(const char* begin, const char* end, aku_Sample *sample) {
    // Series name normalization procedure. Most likeley a bottleneck but
    // can be easily parallelized.
    const char* ksbegin = nullptr;
    const char* ksend = nullptr;
    char buf[AKU_LIMITS_MAX_SNAME];
    char* ob = static_cast<char*>(buf);
    char* oe = static_cast<char*>(buf) + AKU_LIMITS_MAX_SNAME;
    aku_Status status = SeriesParser::to_canonical_form(begin, end, ob, oe, &ksbegin, &ksend);
    if (status != AKU_SUCCESS) {
        return status;
    }
    // Match series name locally (on success use local information)
    // Otherwise - match using global registry. On success - add global information to
    //  the local matcher. On error - add series name to global registry and then to
    //  the local matcher.
    u64 id = local_matcher_.match(ob, ksend);
    if (!id) {
        // go to global registery
        status = storage_->init_series_id(ob, ksend, sample, &local_matcher_);
    } else {
        // initialize using local info
        sample->paramid = id;
    }
    return status;
}

int StorageSession::get_series_ids(const char* begin, const char* end, aku_ParamId* ids, size_t ids_size) {
    // Series name normalization procedure. Most likeley a bottleneck but
    // can be easily parallelized.
    const char* ksbegin = nullptr;
    const char* ksend = nullptr;
    char buf[AKU_LIMITS_MAX_SNAME];
    char* ob = static_cast<char*>(buf);
    char* oe = static_cast<char*>(buf) + AKU_LIMITS_MAX_SNAME;
    aku_Status status = SeriesParser::to_canonical_form(begin, end, ob, oe, &ksbegin, &ksend);
    if (status != AKU_SUCCESS) {
        return -1*status;
    }

    // String in buf should contain normal metric "cpu.user" or compound metric like
    // "cpu.user|cpu.system". The later means that we should find ids for two series:
    // "cpu.user ..." and "cpu.system ..." (tags should be the same in both cases).
    // At first we should determain numer of metrics.

    long nmetric = std::count(const_cast<const char*>(ob), ksbegin, '|') + 1;
    if (nmetric > static_cast<int>(ids_size)) {
        return -1*AKU_EBAD_ARG;
    }

    if (nmetric == 1) {
        // Fast path
        // Match series name locally (on success use local information)
        // Otherwise - match using global registry. On success - add global information to
        //  the local matcher. On error - add series name to global registry and then to
        //  the local matcher.
        u64 id = local_matcher_.match(ob, ksend);
        if (!id) {
            // go to global registery
            aku_Sample sample;
            status = storage_->init_series_id(ob, ksend, &sample, &local_matcher_);
            ids[0] = sample.paramid;
        } else {
            // initialize using local info
            ids[0] = id;
        }
    } else {
        char series[AKU_LIMITS_MAX_SNAME];
        // Copy tags without metrics to the end of the `series` array
        int tagline_len = static_cast<int>(ksend - ksbegin + 1);  // +1 for space, cast is safe because ksend-ksbegin < AKU_LIMITS_MAX_SNAME
        const char* metric_end = ksbegin - 1;  // -1 for space
        char* tagline = series + AKU_LIMITS_MAX_SNAME - tagline_len;
        memcpy(tagline, metric_end, static_cast<size_t>(tagline_len));
        char* send = series + AKU_LIMITS_MAX_SNAME;

        const char* it_begin = ob;
        const char* it_end = ob;
        for (int i = 0; i < nmetric; i++) {
            // copy i'th metric to the `series` array
            while(*it_end != '|' && it_end < metric_end) {
                it_end++;
            }
            // Copy metric name to `series` array
            auto metric_len = it_end - it_begin;
            char* sbegin = tagline - metric_len;
            memcpy(sbegin, it_begin, metric_len);
            // Move to next metric (if any)
            it_end++;
            it_begin = it_end;

            // Match series name locally (on success use local information)
            // Otherwise - match using global registry. On success - add global information to
            //  the local matcher. On error - add series name to global registry and then to
            //  the local matcher.
            u64 id = local_matcher_.match(sbegin, send);
            if (!id) {
                // go to global registery
                aku_Sample tmp;
                status = storage_->init_series_id(sbegin, send, &tmp, &local_matcher_);
                ids[i] = tmp.paramid;
            } else {
                // initialize using local info
                ids[i] = id;
            }
        }
    }
    return static_cast<int>(nmetric);
}

int StorageSession::get_series_name(aku_ParamId id, char* buffer, size_t buffer_size) {
    StringT name;
    if (matcher_substitute_) {
        // Use temporary matcher
        name = matcher_substitute_->id2str(id);
        if (name.first == nullptr) {
            // no such id, user error
            return 0;
        }
    } else {
        name = local_matcher_.id2str(id);
        if (name.first == nullptr) {
            // not yet cached!
            return storage_->get_series_name(id, buffer, buffer_size, &local_matcher_);
        }
    }
    memcpy(buffer, name.first, static_cast<size_t>(name.second));
    return name.second;
}

void StorageSession::query(InternalCursor* cur, const char* query) const {
    storage_->query(this, cur, query);
}

void StorageSession::suggest(InternalCursor* cur, const char* query) const {
    storage_->suggest(this, cur, query);
}

void StorageSession::search(InternalCursor* cur, const char* query) const {
    storage_->search(this, cur, query);
}

void StorageSession::set_series_matcher(std::shared_ptr<PlainSeriesMatcher> matcher) const {
    matcher_substitute_ = matcher;
}

void StorageSession::clear_series_matcher() const {
    matcher_substitute_ = nullptr;
}

//----------- Storage ----------

Storage::Storage()
    : done_{0}
    , close_barrier_(2)
{
    //! In-memory SQLite database
    metadata_.reset(new MetadataStorage(":memory:"));

    bstore_ = StorageEngine::BlockStoreBuilder::create_memstore();
    cstore_ = std::make_shared<StorageEngine::ColumnStore>(bstore_);

    start_sync_worker();
}

Storage::Storage(const char* path)
    : done_{0}
    , close_barrier_(2)
{
    metadata_.reset(new MetadataStorage(path));

    std::string metapath;
    std::vector<std::string> volpaths;

    // first volume is a metavolume
    auto volumes = metadata_->get_volumes();
    for (auto vol: volumes) {
        volpaths.push_back(vol.path);
    }
    std::string bstore_type = "FixedSizeFileStorage";
    std::string db_name = "db";
    metadata_->get_config_param("blockstore_type", &bstore_type);
    metadata_->get_config_param("db_name", &db_name);
    if (bstore_type == "FixedSizeFileStorage") {
        Logger::msg(AKU_LOG_INFO, "Open as fxied size storage");
        bstore_ = StorageEngine::FixedSizeFileStorage::open(metadata_);
    } else if (bstore_type == "ExpandableFileStorage") {
        Logger::msg(AKU_LOG_INFO, "Open as expandable storage");
        bstore_ = StorageEngine::ExpandableFileStorage::open(metadata_);
    } else {
        Logger::msg(AKU_LOG_ERROR, "Unknown blockstore type (" + bstore_type + ")");
        AKU_PANIC("Unknown blockstore type (" + bstore_type + ")");
    }
    cstore_ = std::make_shared<StorageEngine::ColumnStore>(bstore_);
    // Update series matcher
    boost::optional<u64> baseline = metadata_->get_prev_largest_id();
    if (baseline) {
        global_matcher_.series_id = baseline.get() + 1;
    }
    auto status = metadata_->load_matcher_data(global_matcher_);
    if (status != AKU_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, "Can't read series names");
        AKU_PANIC("Can't read series names");
    }
    // Update column store
    std::unordered_map<aku_ParamId, std::vector<StorageEngine::LogicAddr>> mapping;
    status = metadata_->load_rescue_points(mapping);
    if (status != AKU_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, "Can't read rescue points");
        AKU_PANIC("Can't read rescue points");
    }
    cstore_->open_or_restore(mapping, true);
    start_sync_worker();
}

static std::string to_isostring(aku_Timestamp ts) {
    char buffer[0x100];
    int len = DateTimeUtil::to_iso_string(ts, buffer, 0x100);
    if (len < 1) {
        AKU_PANIC("Can't convert timestamp to ISO string");
    }
    return std::string(buffer, buffer + len - 1);
}

// Run through mappings and dump contents
void dump_tree(std::ostream &stream,
               std::shared_ptr<StorageEngine::BlockStore> bstore,
               PlainSeriesMatcher const& matcher,
               aku_ParamId id,
               std::vector<StorageEngine::LogicAddr> rescue_points)
{
    auto namekv = matcher.id2str(id);
    std::string name(namekv.first, namekv.first + namekv.second);
    stream << "\t<id>" << id << "</id>" << std::endl;
    stream << "\t<name>" << name << "</name>" << std::endl;
    stream << "\t<rescue_points>" << std::endl;
    int tagix = 0;
    for(auto rp: rescue_points) {
        std::string tag = "addr_" + std::to_string(tagix++);
        stream << "\t\t<" << tag << ">" << rp << "</" << tag << ">" << std::endl;
    }
    stream << "\t</rescue_points>" << std::endl;

    // Repair status

    using namespace StorageEngine;

    auto treestate = NBTreeExtentsList::repair_status(rescue_points);
    switch(treestate) {
    case NBTreeExtentsList::RepairStatus::OK:
        stream << "\t<repair_status>OK</repair_status>" << std::endl;
    break;
    case NBTreeExtentsList::RepairStatus::REPAIR:
        stream << "\t<repair_status>Repair needed</repair_status>" << std::endl;
    break;
    case NBTreeExtentsList::RepairStatus::SKIP:
        stream << "\t<repair_status>Skip</repair_status>" << std::endl;
    break;
    }

    // Iterate tree in depth first order
    enum class StackItemType {
        // Flow control, this items should contain node addresses
        NORMAL,
        RECOVERY,
        // Formatting control (should be used to maintain XML structure)
        CLOSE_NODE,
        OPEN_NODE,
        CLOSE_CHILDREN,
        OPEN_CHILDREN,
        CLOSE_FANOUT,
        OPEN_FANOUT,
    };

    typedef std::tuple<LogicAddr, int, StackItemType> StackItem;  // (addr, indent, close)
    std::stack<StackItem> stack;
    for(auto it = rescue_points.rbegin(); it != rescue_points.rend(); it++) {
        stack.push(std::make_tuple(EMPTY_ADDR, 1, StackItemType::CLOSE_NODE));
        stack.push(std::make_tuple(*it, 2, treestate == NBTreeExtentsList::RepairStatus::OK
                                                      ? StackItemType::NORMAL
                                                      : StackItemType::RECOVERY ));
        stack.push(std::make_tuple(EMPTY_ADDR, 1, StackItemType::OPEN_NODE));
    }

    while(!stack.empty()) {
        int indent;
        LogicAddr curr;
        StackItemType type;
        std::tie(curr, indent, type) = stack.top();
        stack.pop();

        auto tag = [](int idnt, const char* tag_name, const char* token) {
            std::stringstream str;
            for (int i = 0; i < idnt; i++) {
                str << '\t';
            }
            str << token << tag_name << '>';
            return str.str();
        };

        auto _tag = [indent, tag](const char* tag_name) {
            return tag(indent, tag_name, "<");
        };

        auto tag_ = [indent, tag](const char* tag_name) {
            return tag(indent, tag_name, "</");
        };

        auto afmt = [](LogicAddr a) {
            if (a == EMPTY_ADDR) {
                return std::string("");
            }
            return std::to_string(a);
        };

        if (type == StackItemType::NORMAL || type == StackItemType::RECOVERY) {
            std::shared_ptr<Block> block;
            aku_Status status;
            std::tie(status, block) = bstore->read_block(curr);
            if (status != AKU_SUCCESS) {
                stream << _tag("addr") << afmt(curr) << "</addr>" << std::endl;
                stream << _tag("fail") << StatusUtil::c_str(status) << "</fail>" << std::endl;
                continue;
            }
            auto subtreeref = reinterpret_cast<SubtreeRef*>(block->get_data());
            if (subtreeref->type == NBTreeBlockType::LEAF) {
                // Dump leaf node's content
                NBTreeLeaf leaf(block);
                SubtreeRef const* ref = leaf.get_leafmeta();
                stream << _tag("type")         << "Leaf"                       << "</type>\n";
                stream << _tag("addr")         << afmt(curr)                   << "</addr>\n";
                stream << _tag("prev_addr")    << afmt(leaf.get_prev_addr())   << "</prev_addr>\n";
                stream << _tag("begin")        << to_isostring(ref->begin)     << "</begin>\n";
                stream << _tag("end")          << to_isostring(ref->end)       << "</end>\n";
                stream << _tag("count")        << ref->count                   << "</count>\n";
                stream << _tag("min")          << ref->min                     << "</min>\n";
                stream << _tag("min_time")     << to_isostring(ref->min_time)  << "</min_time>\n";
                stream << _tag("max")          << ref->max                     << "</max>\n";
                stream << _tag("max_time")     << to_isostring(ref->max_time)  << "</max_time>\n";
                stream << _tag("sum")          << ref->sum                     << "</sum>\n";
                stream << _tag("first")        << ref->first                   << "</first>\n";
                stream << _tag("last")         << ref->last                    << "</last>\n";
                stream << _tag("version")      << ref->version                 << "</version>\n";
                stream << _tag("level")        << ref->level                   << "</level>\n";
                stream << _tag("payload_size") << ref->payload_size            << "</payload_size>\n";
                stream << _tag("fanout_index") << ref->fanout_index            << "</fanout_index>\n";
                stream << _tag("checksum")     << ref->checksum                << "</checksum>\n";

                if (type == StackItemType::RECOVERY) {
                    // type is RECOVERY, open fanout tag and dump all connected nodes
                    stack.push(std::make_tuple(EMPTY_ADDR, indent, StackItemType::CLOSE_FANOUT));
                    LogicAddr prev = leaf.get_prev_addr();
                    while(prev != EMPTY_ADDR) {
                        stack.push(std::make_tuple(EMPTY_ADDR, indent + 1, StackItemType::CLOSE_NODE));
                        stack.push(std::make_tuple(prev, indent + 2, StackItemType::NORMAL));
                        stack.push(std::make_tuple(EMPTY_ADDR, indent + 1, StackItemType::OPEN_NODE));
                        std::tie(status, block) = bstore->read_block(prev);
                        if (status != AKU_SUCCESS) {
                            // Block was deleted but it should be on the stack anyway
                            break;
                        }
                        NBTreeLeaf lnext(block);
                        prev = lnext.get_prev_addr();
                    }
                    stack.push(std::make_tuple(EMPTY_ADDR, indent, StackItemType::OPEN_FANOUT));
                }
            } else {
                // Dump inner node's content and children
                NBTreeSuperblock sblock(block);
                SubtreeRef const* ref = sblock.get_sblockmeta();
                stream << _tag("addr")         << afmt(curr)                   << "</addr>\n";
                stream << _tag("type")         << "Superblock"                 << "</type>\n";
                stream << _tag("prev_addr")    << afmt(sblock.get_prev_addr()) << "</prev_addr>\n";
                stream << _tag("begin")        << to_isostring(ref->begin)     << "</begin>\n";
                stream << _tag("end")          << to_isostring(ref->end)       << "</end>\n";
                stream << _tag("count")        << ref->count                   << "</count>\n";
                stream << _tag("min")          << ref->min                     << "</min>\n";
                stream << _tag("min_time")     << to_isostring(ref->min_time)  << "</min_time>\n";
                stream << _tag("max")          << ref->max                     << "</max>\n";
                stream << _tag("max_time")     << to_isostring(ref->max_time)  << "</max_time>\n";
                stream << _tag("sum")          << ref->sum                     << "</sum>\n";
                stream << _tag("first")        << ref->first                   << "</first>\n";
                stream << _tag("last")         << ref->last                    << "</last>\n";
                stream << _tag("version")      << ref->version                 << "</version>\n";
                stream << _tag("level")        << ref->level                   << "</level>\n";
                stream << _tag("payload_size") << ref->payload_size            << "</payload_size>\n";
                stream << _tag("fanout_index") << ref->fanout_index            << "</fanout_index>\n";
                stream << _tag("checksum")     << ref->checksum                << "</checksum>\n";

                if (type == StackItemType::RECOVERY) {
                    // type is RECOVERY, open fanout tag and dump all connected nodes (if any)
                    stack.push(std::make_tuple(EMPTY_ADDR, indent, StackItemType::CLOSE_FANOUT));
                    LogicAddr prev = sblock.get_prev_addr();
                    while(prev != EMPTY_ADDR) {
                        stack.push(std::make_tuple(EMPTY_ADDR, indent + 1, StackItemType::CLOSE_NODE));
                        stack.push(std::make_tuple(prev, indent + 2, StackItemType::NORMAL));
                        stack.push(std::make_tuple(EMPTY_ADDR, indent + 1, StackItemType::OPEN_NODE));
                        std::tie(status, block) = bstore->read_block(prev);
                        if (status != AKU_SUCCESS) {
                            // Block was deleted but it should be on the stack anyway
                            break;
                        }
                        NBTreeSuperblock sbnext(block);
                        prev = sbnext.get_prev_addr();
                    }
                    stack.push(std::make_tuple(EMPTY_ADDR, indent, StackItemType::OPEN_FANOUT));
                }

                std::vector<SubtreeRef> children;
                status = sblock.read_all(&children);
                stack.push(std::make_tuple(EMPTY_ADDR, indent , StackItemType::CLOSE_CHILDREN));
                for(auto sref: children) {
                    LogicAddr addr = sref.addr;
                    stack.push(std::make_tuple(EMPTY_ADDR, indent + 1, StackItemType::CLOSE_NODE));
                    stack.push(std::make_tuple(addr, indent + 2, StackItemType::NORMAL));
                    stack.push(std::make_tuple(EMPTY_ADDR, indent + 1, StackItemType::OPEN_NODE));
                }
                stack.push(std::make_tuple(EMPTY_ADDR, indent, StackItemType::OPEN_CHILDREN));
            }
        } else if (type == StackItemType::CLOSE_CHILDREN) {
            stream << tag_("children") << std::endl;
        } else if (type == StackItemType::CLOSE_FANOUT) {
            stream << tag_("fanout") << std::endl;
        } else if (type == StackItemType::CLOSE_NODE) {
            stream << tag_("node") << std::endl;
        } else if (type == StackItemType::OPEN_CHILDREN) {
            stream << _tag("children") << std::endl;
        } else if (type == StackItemType::OPEN_FANOUT) {
            stream << _tag("fanout") << std::endl;
        } else if (type == StackItemType::OPEN_NODE) {
            stream << _tag("node") << std::endl;
        }
    }
};

aku_Status Storage::generate_report(const char* path, const char *output) {
    /* NOTE: this method generates XML report based on database structure.
     * Because database can be huge, this tool shouldn't consume memory
     * proportional to it's size. Instead of that it looks at one node at
     * a time and keeps in memory small amount of node addresses (2 * fanout size).
     * I decided to roll my own XML writer instead of using existing one to
     * minimize external dependencies.
     */
    auto metadata = std::make_shared<MetadataStorage>(path);

    std::string metapath;
    std::vector<std::string> volpaths;

    // first volume is a metavolume
    auto volumes = metadata->get_volumes();
    for (auto vol: volumes) {
        volpaths.push_back(vol.path);
    }

    auto bstore = StorageEngine::FixedSizeFileStorage::open(metadata);

    // Load series matcher data
    PlainSeriesMatcher matcher;
    auto status = metadata->load_matcher_data(matcher);
    if (status != AKU_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, "Can't read series names");
        return status;
    }

    // Load column-store mapping
    std::unordered_map<aku_ParamId, std::vector<StorageEngine::LogicAddr>> mapping;
    status = metadata->load_rescue_points(mapping);
    if (status != AKU_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, "Can't read rescue points");
        return status;
    }
    // Do not restore column-store!

    std::fstream outfile;
    if (output) {
        outfile.open(output, std::fstream::out);
    }
    std::ostream& stream = output == nullptr ? std::cout : outfile;

    stream << "<report>" << std::endl;
    stream << "<file_name>" << path << "</file_name>" << std::endl;
    stream << "<num_volumes>" << volpaths.size() << "</num_volumes>" << std::endl;
    stream << "<volumes>" << std::endl;
    for(auto volpath: volpaths) {
        stream << "\t<volume_path>" << volpath << "</volume_path>" << std::endl;
    }
    stream << "</volumes>" << std::endl;

    stream << "<database>" << std::endl;
    for(auto kv: mapping) {
        aku_ParamId id = kv.first;
        std::vector<StorageEngine::LogicAddr> rescue_points = kv.second;
        stream << "<tree>" << std::endl;
        dump_tree(stream, bstore, matcher, id, rescue_points);
        stream << "</tree>" << std::endl;
    }
    stream << "</database>" << std::endl;
    stream << "</report>" << std::endl;
    return AKU_SUCCESS;
}

aku_Status Storage::generate_recovery_report(const char* path, const char *output) {
    auto metadata = std::make_shared<MetadataStorage>(path);

    std::string metapath;
    std::vector<std::string> volpaths;

    // first volume is a metavolume
    auto volumes = metadata->get_volumes();
    for (auto vol: volumes) {
        volpaths.push_back(vol.path);
    }

    auto bstore = StorageEngine::FixedSizeFileStorage::open(metadata);
    auto cstore = std::make_shared<StorageEngine::ColumnStore>(bstore);

    // Load series matcher data
    PlainSeriesMatcher matcher;
    auto status = metadata->load_matcher_data(matcher);
    if (status != AKU_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, "Can't read series names");
        return status;
    }

    // Load column-store mapping
    std::unordered_map<aku_ParamId, std::vector<StorageEngine::LogicAddr>> mapping;
    status = metadata->load_rescue_points(mapping);
    if (status != AKU_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, "Can't read rescue points");
        return status;
    }

    // Restore column store
    cstore->open_or_restore(mapping);

    std::fstream outfile;
    if (output) {
        outfile.open(output, std::fstream::out);
    }
    std::ostream& stream = output == nullptr ? std::cout : outfile;

    stream << "<report>" << std::endl;
    stream << "<file_name>" << path << "</file_name>" << std::endl;
    stream << "<num_volumes>" << volpaths.size() << "</num_volumes>" << std::endl;
    stream << "<volumes>" << std::endl;
    for(auto volpath: volpaths) {
        stream << "\t<volume_path>" << volpath << "</volume_path>" << std::endl;
    }
    stream << "</volumes>" << std::endl;

    stream << "<column_store>" << std::endl;

    auto columns = cstore->_get_columns();
    for(auto kv: columns) {
        aku_ParamId id = kv.first;
        std::shared_ptr<StorageEngine::NBTreeExtentsList> column = kv.second;
        stream << "\t<column>" << std::endl;
        auto namekv = matcher.id2str(id);
        std::string name(namekv.first, namekv.first + namekv.second);
        stream << "\t\t<id>" << id << "</id>\n";
        stream << "\t\t<name>" << name << "</name>\n";
        stream << "\t\t<extents>" << std::endl;
        for(auto ext: column->get_extents()) {
            stream << "\t\t\t<extent>" << std::endl;
            ext->debug_dump(stream, 4, to_isostring);
            stream << "\t\t\t</extent>" << std::endl;
        }
        stream << "\t\t</extents>" << std::endl;
        stream << "\t</column>" << std::endl;
    }
    stream << "</column_store>" << std::endl;
    stream << "</report>" << std::endl;
    return AKU_SUCCESS;
}


Storage::Storage(std::shared_ptr<MetadataStorage>                  meta,
                 std::shared_ptr<StorageEngine::BlockStore>        bstore,
                 std::shared_ptr<StorageEngine::ColumnStore>       cstore,
                 bool                                              start_worker)
    : bstore_(bstore)
    , cstore_(cstore)
    , done_{0}
    , close_barrier_(2)
    , metadata_(meta)
{
    if (start_worker) {
        start_sync_worker();
    }
}

void Storage::start_sync_worker() {
    // This thread periodically sync rescue points and series names.
    // It calls `flush` method of the blockstore and then `sync_with_metadata_storage` method
    // if something needs to be synced.
    // This order guarantees that metadata storage always contains correct rescue points and
    // other metadata.
    enum {
        SYNC_REQUEST_TIMEOUT = 10000,
    };
    auto sync_worker = [this]() {
        auto get_names = [this](std::vector<PlainSeriesMatcher::SeriesNameT>* names) {
            std::lock_guard<std::mutex> guard(lock_);
            global_matcher_.pull_new_names(names);
        };

        while(done_.load() == 0) {
            auto status = metadata_->wait_for_sync_request(SYNC_REQUEST_TIMEOUT);
            if (status == AKU_SUCCESS) {
                bstore_->flush();
                metadata_->sync_with_metadata_storage(get_names);
            }
        }

        close_barrier_.wait();
    };
    std::thread sync_worker_thread(sync_worker);
    sync_worker_thread.detach();
}

void Storage::close() {
    // Wait for all ingestion sessions to stop
    done_.store(1);
    metadata_->force_sync();
    close_barrier_.wait();
    // Close column store
    auto mapping = cstore_->close();
    if (!mapping.empty()) {
        for (auto kv: mapping) {
            u64 id;
            std::vector<u64> vals;
            std::tie(id, vals) = kv;
            metadata_->add_rescue_point(id, std::move(vals));
        }
        // Save finall mapping (should contain all affected columns)
        metadata_->sync_with_metadata_storage(boost::bind(&SeriesMatcher::pull_new_names, &global_matcher_, _1));
    }
    bstore_->flush();
}


void Storage::_update_rescue_points(aku_ParamId id, std::vector<StorageEngine::LogicAddr>&& rpoints) {
    metadata_->add_rescue_point(id, std::move(rpoints));
}

std::shared_ptr<StorageSession> Storage::create_write_session() {
    std::shared_ptr<StorageEngine::CStoreSession> session = std::make_shared<StorageEngine::CStoreSession>(cstore_);
    return std::make_shared<StorageSession>(shared_from_this(), session);
}

aku_Status Storage::init_series_id(const char* begin, const char* end, aku_Sample *sample, PlainSeriesMatcher *local_matcher) {
    u64 id = 0;
    bool create_new = false;
    {
        std::lock_guard<std::mutex> guard(lock_);
        id = global_matcher_.match(begin, end);
        if (id == 0) {
            // create new series
            id = global_matcher_.add(begin, end);
            metadata_->add_rescue_point(id, std::vector<u64>());
            create_new = true;
        }
    }
    if (create_new) {
        // id guaranteed to be unique
        cstore_->create_new_column(id);
    }
    sample->paramid = id;
    local_matcher->_add(begin, end, id);
    return AKU_SUCCESS;
}

int Storage::get_series_name(aku_ParamId id, char* buffer, size_t buffer_size, PlainSeriesMatcher *local_matcher) {
    auto str = global_matcher_.id2str(id);
    if (str.first == nullptr) {
        return 0;
    }
    // copy value to local matcher
    local_matcher->_add(str.first, str.first + str.second, id);
    // copy the string to out buffer
    if (str.second > buffer_size) {
        return -1*str.second;
    }
    memcpy(buffer, str.first, static_cast<size_t>(str.second));
    return static_cast<int>(str.second);
}

aku_Status Storage::parse_query(boost::property_tree::ptree const& ptree, QP::ReshapeRequest* req) const {
    using namespace QP;
    QueryKind kind;
    aku_Status status;

    std::tie(status, kind) = QueryParser::get_query_kind(ptree);
    if (status != AKU_SUCCESS) {
        return status;
    }
    switch (kind) {
    case QueryKind::SELECT_META:
        Logger::msg(AKU_LOG_ERROR, "Metadata query is not supported");
        return AKU_EBAD_ARG;
    case QueryKind::AGGREGATE:
        std::tie(status, *req) = QueryParser::parse_aggregate_query(ptree, global_matcher_);
        if (status != AKU_SUCCESS) {
            return status;
        }
        break;
    case QueryKind::GROUP_AGGREGATE:
        std::tie(status, *req) = QueryParser::parse_group_aggregate_query(ptree, global_matcher_);
        if (status != AKU_SUCCESS) {
            return status;
        }
        break;
    case QueryKind::SELECT:
        std::tie(status, *req) = QueryParser::parse_select_query(ptree, global_matcher_);
        if (status != AKU_SUCCESS) {
            return status;
        }
        break;
    case QueryKind::JOIN:
        std::tie(status, *req) = QueryParser::parse_join_query(ptree, global_matcher_);
        if (status != AKU_SUCCESS) {
            return status;
        }
        break;
    };
    return AKU_SUCCESS;
}

void Storage::query(StorageSession const* session, InternalCursor* cur, const char* query) const {
    using namespace QP;
    boost::property_tree::ptree ptree;
    aku_Status status;
    session->clear_series_matcher();
    std::tie(status, ptree) = QueryParser::parse_json(query);
    if (status != AKU_SUCCESS) {
        cur->set_error(status);
        return;
    }
    QueryKind kind;
    std::tie(status, kind) = QueryParser::get_query_kind(ptree);
    if (status != AKU_SUCCESS) {
        cur->set_error(status);
        return;
    }
    std::shared_ptr<IStreamProcessor> proc;
    ReshapeRequest req;

    if (kind == QueryKind::SELECT_META) {
        std::vector<aku_ParamId> ids;
        std::tie(status, ids) = QueryParser::parse_select_meta_query(ptree, global_matcher_);
        if (status != AKU_SUCCESS) {
            cur->set_error(status);
            return;
        }
        std::vector<std::shared_ptr<Node>> nodes;
        std::tie(status, nodes) = QueryParser::parse_processing_topology(ptree, cur);
        if (status != AKU_SUCCESS) {
            cur->set_error(status);
            return;
        }
        proc = std::make_shared<MetadataQueryProcessor>(nodes.front(), std::move(ids));
        if (proc->start()) {
            proc->stop();
        }
        return;
    } else {
        status = parse_query(ptree, &req);
        if (status != AKU_SUCCESS) {
            cur->set_error(status);
            return;
        }
        std::vector<std::shared_ptr<Node>> nodes;
        std::tie(status, nodes) = QueryParser::parse_processing_topology(ptree, cur);
        if (status != AKU_SUCCESS) {
            cur->set_error(status);
            return;
        }
        bool groupbytime = kind == QueryKind::GROUP_AGGREGATE;
        proc = std::make_shared<ScanQueryProcessor>(nodes, groupbytime);
        if (req.select.matcher) {
            session->set_series_matcher(req.select.matcher);
        } else {
            session->clear_series_matcher();
        }
        // Return error if no series was found
        if (req.select.columns.empty()) {
            cur->set_error(AKU_EQUERY_PARSING_ERROR);
            return;
        }
        if (req.select.columns.at(0).ids.empty()) {
            cur->set_error(AKU_ENOT_FOUND);
            return;
        }
        std::unique_ptr<QP::IQueryPlan> query_plan;
        std::tie(status, query_plan) = QP::QueryPlanBuilder::create(req);
        if (status != AKU_SUCCESS) {
            cur->set_error(status);
            return;
        }
        // TODO: log query plan if required
        if (proc->start()) {
            QueryPlanExecutor executor;
            executor.execute(*cstore_, std::move(query_plan), *proc);
            proc->stop();
        }
    }
}

void Storage::suggest(StorageSession const* session, InternalCursor* cur, const char* query) const {
    using namespace QP;
    boost::property_tree::ptree ptree;
    aku_Status status;
    session->clear_series_matcher();
    std::tie(status, ptree) = QueryParser::parse_json(query);
    if (status != AKU_SUCCESS) {
        cur->set_error(status);
        return;
    }
    std::vector<aku_ParamId> ids;
    std::shared_ptr<PlainSeriesMatcher> substitute;
    std::tie(status, substitute, ids) = QueryParser::parse_suggest_query(ptree, global_matcher_);
    if (status != AKU_SUCCESS) {
        cur->set_error(status);
        return;
    }
    std::vector<std::shared_ptr<Node>> nodes;
    std::tie(status, nodes) = QueryParser::parse_processing_topology(ptree, cur);
    if (status != AKU_SUCCESS) {
        cur->set_error(status);
        return;
    }
    session->set_series_matcher(substitute);
    std::shared_ptr<IStreamProcessor> proc =
            std::make_shared<MetadataQueryProcessor>(nodes.front(), std::move(ids));
    if (proc->start()) {
        proc->stop();
    }
}

void Storage::search(StorageSession const* session, InternalCursor* cur, const char* query) const {
    using namespace QP;
    boost::property_tree::ptree ptree;
    aku_Status status;
    session->clear_series_matcher();
    std::tie(status, ptree) = QueryParser::parse_json(query);
    if (status != AKU_SUCCESS) {
        cur->set_error(status);
        return;
    }
    std::vector<aku_ParamId> ids;
    std::tie(status, ids) = QueryParser::parse_search_query(ptree, global_matcher_);
    if (status != AKU_SUCCESS) {
        cur->set_error(status);
        return;
    }
    std::vector<std::shared_ptr<Node>> nodes;
    std::tie(status, nodes) = QueryParser::parse_processing_topology(ptree, cur);
    if (status != AKU_SUCCESS) {
        cur->set_error(status);
        return;
    }
    std::shared_ptr<IStreamProcessor> proc =
            std::make_shared<MetadataQueryProcessor>(nodes.front(), std::move(ids));
    if (proc->start()) {
        proc->stop();
    }
}

void Storage::debug_print() const {
    std::cout << "Storage::debug_print" << std::endl;
    std::cout << "...not implemented" << std::endl;
}

aku_Status Storage::new_database( const char     *base_file_name
                                , const char     *metadata_path
                                , const char     *volumes_path
                                , i32             num_volumes
                                , u64             volume_size
                                , bool            allocate)
{
    // Check for max volume size
    const u64 MAX_SIZE = 0x100000000 * 4096 - 1;  // 15TB
    const u64 MIN_SIZE = 0x100000;  // 1MB
    if (volume_size > MAX_SIZE) {
        Logger::msg(AKU_LOG_ERROR, "Volume size is too big: " + std::to_string(volume_size) + ", it can't be greater than 15TB");
        return AKU_EBAD_ARG;
    } else if (volume_size < MIN_SIZE) {
        Logger::msg(AKU_LOG_ERROR, "Volume size is too small: " + std::to_string(volume_size) + ", it can't be less than 1MB");
        return AKU_EBAD_ARG;
    }
    // Create volumes and metapage
    u32 volsize = static_cast<u32>(volume_size / 4096);

    boost::filesystem::path volpath(volumes_path);
    boost::filesystem::path metpath(metadata_path);
    volpath = boost::filesystem::absolute(volpath);
    metpath = boost::filesystem::absolute(metpath);
    std::string sqlitebname = std::string(base_file_name) + ".akumuli";
    boost::filesystem::path sqlitepath = metpath / sqlitebname;

    if (!boost::filesystem::exists(volpath)) {
        Logger::msg(AKU_LOG_INFO, std::string(volumes_path) + " doesn't exists, trying to create directory");
        boost::filesystem::create_directories(volpath);
    } else {
        if (!boost::filesystem::is_directory(volpath)) {
            Logger::msg(AKU_LOG_ERROR, std::string(volumes_path) + " is not a directory");
            return AKU_EBAD_ARG;
        }
    }

    if (!boost::filesystem::exists(metpath)) {
        Logger::msg(AKU_LOG_INFO, std::string(metadata_path) + " doesn't exists, trying to create directory");
        boost::filesystem::create_directories(metpath);
    } else {
        if (!boost::filesystem::is_directory(metpath)) {
            Logger::msg(AKU_LOG_ERROR, std::string(metadata_path) + " is not a directory");
            return AKU_EBAD_ARG;
        }
    }

    if (boost::filesystem::exists(sqlitepath)) {
        Logger::msg(AKU_LOG_ERROR, "Database is already exists");
        return AKU_EBAD_ARG;
    }

    i32 actual_nvols = (num_volumes == 0) ? 1 : num_volumes;
    std::vector<std::tuple<u32, std::string>> paths;
    for (i32 i = 0; i < actual_nvols; i++) {
        std::string basename = std::string(base_file_name) + "_" + std::to_string(i) + ".vol";
        boost::filesystem::path p = volpath / basename;
        paths.push_back(std::make_tuple(volsize, p.string()));
    }

    StorageEngine::FileStorage::create(paths);

    if (allocate) {
        for (const auto& path: paths) {
            const auto& p = std::get<1>(path);
            int fd = open(p.c_str(), O_WRONLY);
            if (fd < 0) {
                boost::system::error_code error(errno, boost::system::system_category());
                Logger::msg(AKU_LOG_ERROR, "Can't open file '" + p + "' reason: " + error.message() + ". Skip.");
                break;
            }
            int ret = posix_fallocate(fd, 0, std::get<0>(path));
            ::close(fd);
            if (ret == 0) {
                Logger::msg(AKU_LOG_INFO, "Disk space for " + p + " preallocated");
            } else {
                boost::system::error_code error(ret, boost::system::system_category());
                Logger::msg(AKU_LOG_ERROR, "posix_fallocate fail: " + error.message());
            }
        }
    }

    // Create sqlite database for metadata
    std::vector<std::string> mpaths;
    std::vector<u32> msizes;
    for (auto p: paths) {
        msizes.push_back(std::get<0>(p));
        mpaths.push_back(std::get<1>(p));
    }
    if (num_volumes == 0) {
        Logger::msg(AKU_LOG_INFO, "Creating expandable file storage");
        create_metadata_page(base_file_name, sqlitepath.c_str(), mpaths, msizes, "ExpandableFileStorage");
    } else {
        Logger::msg(AKU_LOG_INFO, "Creating fixed file storage");
        create_metadata_page(base_file_name, sqlitepath.c_str(), mpaths, msizes, "FixedSizeFileStorage");
    }
    return AKU_SUCCESS;
}

aku_Status Storage::remove_storage(const char* file_name, bool force) {
    if (!boost::filesystem::exists(file_name)) {
        return AKU_ENOT_FOUND;
    }
    auto meta = std::make_shared<MetadataStorage>(file_name);
    auto volumes = meta->get_volumes();
    if (volumes.empty()) {
        // Bad database
        return AKU_EBAD_ARG;
    }
    std::vector<std::string> volume_names(volumes.size(), "");

    for(auto it: volumes) {
        volume_names.at(it.id) = it.path;
    }
    if (!force) {
        // Check whether or not database is empty
        auto fstore = StorageEngine::FixedSizeFileStorage::open(meta);
        auto stats = fstore->get_stats();
        if (stats.nblocks != 0) {
            // DB is not empty
            return AKU_ENOT_PERMITTED;
        }
    }
    meta.reset();

    // Check access rights
    auto check_access = [](std::string const& p) {
        auto status = boost::filesystem::status(p);
        auto perms = status.permissions();
        if ((perms & boost::filesystem::owner_write) == 0) {
            return AKU_EACCESS;
        }
        return AKU_SUCCESS;
    };
    volume_names.push_back(file_name);
    std::vector<aku_Status> statuses;
    std::transform(volume_names.begin(), volume_names.end(), std::back_inserter(statuses), check_access);
    auto comb_status = [](aku_Status lhs, aku_Status rhs) {
        return lhs == AKU_SUCCESS ? rhs : lhs;
    };
    auto status = std::accumulate(statuses.begin(), statuses.end(), AKU_SUCCESS, comb_status);
    if (status != AKU_SUCCESS) {
        return AKU_EACCESS;
    }

    // Actual deletion starts here!
    auto delete_file = [](std::string const& fname) {
        if (!boost::filesystem::remove(fname)) {
            Logger::msg(AKU_LOG_ERROR, fname + " file is not deleted!");
        } else {
            Logger::msg(AKU_LOG_INFO, fname + " was deleted.");
        }
    };

    std::for_each(volume_names.begin(), volume_names.end(), delete_file);

    return AKU_SUCCESS;
}

boost::property_tree::ptree Storage::get_stats() {
    boost::property_tree::ptree result;
    auto volstats = bstore_->get_volume_stats();
    int ix = 0;
    for (auto kv: volstats) {
        auto name = kv.first;
        auto stats = kv.second;
        auto capacity = stats.capacity * stats.block_size;
        auto free_vol = capacity - stats.nblocks * stats.block_size;
        std::string path = "volume_" + std::to_string(ix++);
        result.put(path + ".free_space", free_vol);
        result.put(path + ".file_name", name);
    }
    return result;
}

}
