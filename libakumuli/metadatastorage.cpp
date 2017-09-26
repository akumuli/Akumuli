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

#include "metadatastorage.h"
#include "util.h"
#include "log_iface.h"

#include <sstream>

#include <boost/lexical_cast.hpp>
#include <boost/exception/diagnostic_information.hpp>

#include <sqlite3.h>  // to set trace callback

namespace Akumuli {

void delete_apr_pool(apr_pool_t *p) {
    if (p) {
        apr_pool_destroy(p);
    }
}

AprHandleDeleter::AprHandleDeleter(const apr_dbd_driver_t *driver)
    : driver(driver)
{
}

void AprHandleDeleter::operator()(apr_dbd_t* handle) {
    if (driver != nullptr && handle != nullptr) {
        apr_dbd_close(driver, handle);
    }
}


//-------------------------------MetadataStorage----------------------------------------

static void callback_adapter(void*, const char* msg) {
    Logger::msg(AKU_LOG_TRACE, msg);
}

MetadataStorage::MetadataStorage(const char* db)
    : pool_(nullptr, &delete_apr_pool)
    , driver_(nullptr)
    , handle_(nullptr, AprHandleDeleter(nullptr))
{
    apr_pool_t *pool = nullptr;
    auto status = apr_pool_create(&pool, NULL);
    if (status != APR_SUCCESS) {
        // report error (can't return error from c-tor)
        AKU_PANIC("Can't create memory pool");
    }
    pool_.reset(pool);

    status = apr_dbd_get_driver(pool, "sqlite3", &driver_);
    if (status != APR_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, "Can't load driver, maybe libaprutil1-dbd-sqlite3 isn't installed");
        AKU_PANIC("Can't load sqlite3 driver");
    }

    apr_dbd_t *handle = nullptr;
    status = apr_dbd_open(driver_, pool, db, &handle);
    if (status != APR_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, "Can't open database, check file path");
        AKU_PANIC("Can't open database");
    }
    handle_ = HandleT(handle, AprHandleDeleter(driver_));

    auto sqlite_handle = apr_dbd_native_handle(driver_, handle);
    sqlite3_trace(static_cast<sqlite3*>(sqlite_handle), callback_adapter, nullptr);

    create_tables();

    // Create prepared statement
    const char* query = "INSERT INTO akumuli_series (series_id, keyslist, storage_id) VALUES (%s, %s, %d)";
    status = apr_dbd_prepare(driver_, pool_.get(), handle_.get(), query, "INSERT_SERIES_NAME", &insert_);
    if (status != 0) {
        Logger::msg(AKU_LOG_ERROR, "Error creating prepared statement");
        AKU_PANIC(apr_dbd_error(driver_, handle_.get(), status));
    }
}

void MetadataStorage::sync_with_metadata_storage(std::function<void(std::vector<SeriesT>*)> pull_new_names) {
    // Make temporary copies under the lock
    std::vector<PlainSeriesMatcher::SeriesNameT>           newnames;
    std::unordered_map<aku_ParamId, std::vector<u64>> rescue_points;
    std::unordered_map<u32, VolumeDesc>               volume_records;
    {
        std::lock_guard<std::mutex> guard(sync_lock_);
        std::swap(rescue_points, pending_rescue_points_);
        std::swap(volume_records, pending_volumes_);
    }
    pull_new_names(&newnames);

    // Save new names
    begin_transaction();
    insert_new_names(std::move(newnames));

    // Save rescue points
    upsert_rescue_points(std::move(rescue_points));

    // Save volume records
    upsert_volume_records(std::move(volume_records));

    end_transaction();
}

void MetadataStorage::force_sync() {
    sync_cvar_.notify_one();
}

int MetadataStorage::execute_query(std::string query) {
    int nrows = -1;
    int status = apr_dbd_query(driver_, handle_.get(), &nrows, query.c_str());
    if (status != 0 && status != 21) {
        // generate error and throw
        Logger::msg(AKU_LOG_ERROR, "Error executing query");
        AKU_PANIC(apr_dbd_error(driver_, handle_.get(), status));
    }
    return nrows;
}

void MetadataStorage::create_tables() {
    const char* query = nullptr;

    // Create volumes table
    query =
            "CREATE TABLE IF NOT EXISTS akumuli_volumes("
            "id INTEGER UNIQUE,"
            "path TEXT UNIQUE,"
            // Content of the metadata volume moved to sqlite
            "version INTEGER,"
            "nblocks INTEGER,"
            "capacity INTEGER,"
            "generation INTEGER"
            ");";
    execute_query(query);

    // Create configuration table (key-value-commmentary)
    query =
            "CREATE TABLE IF NOT EXISTS akumuli_configuration("
            "name TEXT UNIQUE,"
            "value TEXT,"
            "comment TEXT"
            ");";
    execute_query(query);

    query =
            "CREATE TABLE IF NOT EXISTS akumuli_series("
            "id INTEGER PRIMARY KEY UNIQUE,"
            "series_id TEXT,"
            "keyslist TEXT,"
            "storage_id INTEGER UNIQUE"
            ");";
    execute_query(query);

    query =
            "CREATE TABLE IF NOT EXISTS akumuli_rescue_points("
            "storage_id INTEGER PRIMARY KEY UNIQUE,"
            "addr0 INTEGER,"
            "addr1 INTEGER,"
            "addr2 INTEGER,"
            "addr3 INTEGER,"
            "addr4 INTEGER,"
            "addr5 INTEGER,"
            "addr6 INTEGER,"
            "addr7 INTEGER"
            ");";
    execute_query(query);
}

void MetadataStorage::init_config(const char* db_name,
                                  const char* creation_datetime,
                                  const char* bstore_type)
{
    // Create table and insert data into it

    std::stringstream insert;
    insert << "INSERT INTO akumuli_configuration (name, value, comment)" << std::endl;
    insert << "\tVALUES ('creation_datetime', '" << creation_datetime << "', " << "'DB creation time.'), "
           << "('blockstore_type', '" << bstore_type << "', " << "'Type of block storage used.'),"
          #ifdef AKU_VERSION
           << "('storage_version', '" << AKU_VERSION << "', " << "'Akumuli version used to create the database.'),"
          #endif
           << "('db_name', '" << db_name << "', " << "'Name of DB instance.');"
           << std::endl;
    std::string insert_query = insert.str();
    execute_query(insert_query);
}

bool MetadataStorage::get_config_param(const std::string name, std::string* result)
{
    // Read requested config
    std::stringstream query;
    query << "SELECT value FROM akumuli_configuration WHERE name='" << name << "'";
    auto results = select_query(query.str().c_str());
    if (results.size() != 1) {
        Logger::msg(AKU_LOG_TRACE, "Can't find configuration parameter `" + name + "`");
        return false;
    }
    auto tuple = results.at(0);
    if (tuple.size() != 1) {
        AKU_PANIC("Invalid configuration query (" + name + ")");
    }
    // This value can be encoded as dobule by the sqlite engine
    *result = tuple.at(0);
    return true;
}

void MetadataStorage::init_volumes(std::vector<VolumeDesc> volumes) {
    std::stringstream query;
    query << "INSERT INTO akumuli_volumes (id, path, version, nblocks, capacity, generation)" << std::endl;
    bool first = true;
    for (auto desc: volumes) {
        if (first) {
            query << "\tSELECT "
                  << desc.id << " as id, '"
                  << desc.path << "' as path, '"
                  << desc.version << "' as version, "
                  << desc.nblocks << " as nblocks, "
                  << desc.capacity << " as capacity, "
                  << desc.generation << " as generation"
                  << std::endl;
            first = false;
        } else {
            query << "\tUNION SELECT "
                  << desc.id << ", '"
                  << desc.path << "', '"
                  << desc.version << "', "
                  << desc.nblocks << ", "
                  << desc.capacity << ", "
                  << desc.generation
                  << std::endl;
        }
    }
    std::string full_query = query.str();
    execute_query(full_query);
}


std::vector<MetadataStorage::UntypedTuple> MetadataStorage::select_query(const char* query) const {
    std::vector<UntypedTuple> tuples;
    apr_dbd_results_t *results = nullptr;
    int status = apr_dbd_select(driver_, pool_.get(), handle_.get(), &results, query, 0);
    if (status != 0) {
        Logger::msg(AKU_LOG_ERROR, "Error executing query");
        AKU_PANIC(apr_dbd_error(driver_, handle_.get(), status));
    }
    // get rows
    int ntuples = apr_dbd_num_tuples(driver_, results);
    int ncolumns = apr_dbd_num_cols(driver_, results);
    for (int i = ntuples; i --> 0;) {
        apr_dbd_row_t *row = nullptr;
        status = apr_dbd_get_row(driver_, pool_.get(), results, &row, -1);
        if (status != 0) {
            Logger::msg(AKU_LOG_ERROR, "Error getting row from resultset");
            AKU_PANIC(apr_dbd_error(driver_, handle_.get(), status));
        }
        UntypedTuple tup;
        for (int col = 0; col < ncolumns; col++) {
            const char* entry = apr_dbd_get_entry(driver_, row, col);
            if (entry) {
                tup.emplace_back(entry);
            } else {
                tup.emplace_back();
            }
        }
        tuples.push_back(std::move(tup));
    }
    return tuples;
}

std::vector<MetadataStorage::VolumeDesc> MetadataStorage::get_volumes() const {
    const char* query =
            "SELECT id, path, version, nblocks, capacity, generation FROM akumuli_volumes;";
    std::vector<VolumeDesc> tuples;
    std::vector<UntypedTuple> untyped = select_query(query);
    // get rows
    auto ntuples = untyped.size();
    for (size_t i = 0; i < ntuples; i++) {
        VolumeDesc desc;
        desc.id = boost::lexical_cast<u32>(untyped.at(i).at(0));
        desc.path = untyped.at(i).at(1);
        desc.version = boost::lexical_cast<u32>(untyped.at(i).at(2));
        desc.nblocks = boost::lexical_cast<u32>(untyped.at(i).at(3));
        desc.capacity = boost::lexical_cast<u32>(untyped.at(i).at(4));
        desc.generation = boost::lexical_cast<u32>(untyped.at(i).at(5));
        tuples.push_back(desc);
    }
    return tuples;
}

void MetadataStorage::add_volume(const VolumeDesc &vol) {
    std::string query =
             "INSERT INTO akumuli_volumes (id, path, version, nblocks, capacity, generation) VALUES ";
    query += "(" + std::to_string(vol.id) + ", \"" + vol.path + "\", "
                 + std::to_string(vol.version) + ", "
                 + std::to_string(vol.nblocks) + ", "
                 + std::to_string(vol.capacity) + ", "
                 + std::to_string(vol.generation) + ");";
    Logger::msg(AKU_LOG_TRACE, "Execute query: " + query);
    int rows = execute_query(query);
    if (rows == 0) {
        Logger::msg(AKU_LOG_ERROR, "Insert query failed: " + query + " - can't save the volume.");
    }
}

struct LightweightString {
    const char* str;
    int len;
};

std::ostream& operator << (std::ostream& s, LightweightString val) {
    s.write(val.str, val.len);
    return s;
}

static bool split_series(const char* str, int n, LightweightString* outname, LightweightString* outkeys) {
    int len = 0;
    while(len < n && str[len] != ' ' && str[len] != '\t') {
        len++;
    }
    if (len >= n) {
        // Error
        return false;
    }
    outname->str = str;
    outname->len = len;
    // Skip space
    auto end = str + n;
    str = str + len;
    while(str < end && (*str == ' ' || *str == '\t')) {
        str++;
    }
    if (str == end) {
        // Error (no keys present)
        return false;
    }
    outkeys->str = str;
    outkeys->len = end - str;
    return true;
}

aku_Status MetadataStorage::wait_for_sync_request(int timeout_us) {
    std::unique_lock<std::mutex> lock(sync_lock_);
    auto res = sync_cvar_.wait_for(lock, std::chrono::microseconds(timeout_us));
    if (res == std::cv_status::timeout) {
        return AKU_ETIMEOUT;
    }
    return (pending_rescue_points_.empty() && pending_volumes_.empty()) ? AKU_ERETRY : AKU_SUCCESS;
}

void MetadataStorage::add_rescue_point(aku_ParamId id, std::vector<u64>&& val) {
    std::lock_guard<std::mutex> guard(sync_lock_);
    pending_rescue_points_[id] = val;
    sync_cvar_.notify_one();
}

void MetadataStorage::update_volume(const VolumeDesc& vol) {
    std::lock_guard<std::mutex> guard(sync_lock_);
    pending_volumes_[vol.id] = vol;
    sync_cvar_.notify_one();
}

std::string MetadataStorage::get_dbname() {
    std::string dbname;
    bool success = get_config_param("db_name", &dbname);
    if (!success) {
        AKU_PANIC("Configuration parameter 'db_name' is missing");
    }
    return dbname;
}

void MetadataStorage::begin_transaction() {
    execute_query("BEGIN TRANSACTION;");
}

void MetadataStorage::end_transaction() {
    execute_query("END TRANSACTION;");}

void MetadataStorage::upsert_volume_records(std::unordered_map<u32, VolumeDesc>&& input) {
    if (input.empty()) {
        return;
    }
    std::stringstream query;
    std::vector<VolumeDesc> items;
    for (const auto& kv: input) {
        items.push_back(kv.second);
    }
    while(!items.empty()) {
        const size_t batchsize = 500;  // This limit is defined by SQLITE_MAX_COMPOUND_SELECT
        const size_t newsize = items.size() > batchsize ? items.size() - batchsize : 0;
        std::vector<VolumeDesc> batch(items.begin() + static_cast<ssize_t>(newsize), items.end());
        items.resize(newsize);
        query << "INSERT OR REPLACE INTO akumuli_volumes (id, path, version, nblocks, capacity, generation) VALUES ";
        size_t ix = 0;
        for (auto const& vol: batch) {
            query << "(" << std::to_string(vol.id)         << ", '"
                         << vol.path                       << "', "
                         << std::to_string(vol.version)    << ", "
                         << std::to_string(vol.nblocks)    << ", "
                         << std::to_string(vol.capacity)   << ", "
                         << std::to_string(vol.generation) << ")";
            ix++;
            if (ix == batch.size()) {
                query << ";\n";
            } else {
                query << ",";
            }
        }
    }
    execute_query(query.str());
}

void MetadataStorage::upsert_rescue_points(std::unordered_map<aku_ParamId, std::vector<u64>>&& input) {
    if (input.empty()) {
        return;
    }
    std::stringstream query;
    typedef std::pair<aku_ParamId, std::vector<u64>> ValueT;
    std::vector<ValueT> items(input.begin(), input.end());
    while(!items.empty()) {
        const size_t batchsize = 500;  // This limit is defined by SQLITE_MAX_COMPOUND_SELECT
        const size_t newsize = items.size() > batchsize ? items.size() - batchsize : 0;
        std::vector<ValueT> batch(items.begin() + static_cast<ssize_t>(newsize), items.end());
        items.resize(newsize);
        query <<
            "INSERT OR REPLACE INTO akumuli_rescue_points (storage_id, addr0, addr1, addr2, addr3, addr4, addr5, addr6, addr7) VALUES ";
        size_t ix = 0;
        for (auto const& kv: batch) {
            query << "( " << kv.first;
            for (auto id: kv.second) {
                if (id == ~0ull) {
                    // Values that big can't be represented in SQLite, -1 value should be interpreted as EMPTY_ADDR,
                    query << ", -1";
                } else {
                    query << ", " << id;
                }
            }
            for(auto i = kv.second.size(); i < 8; i++) {
                query << ", null";
            }
            query << ")";
            ix++;
            if (ix == batch.size()) {
                query << ";\n";
            } else {
                query << ",";
            }
        }
    }
    execute_query(query.str());

}

void MetadataStorage::insert_new_names(std::vector<SeriesT> &&items) {
    if (items.size() == 0) {
        return;
    }
    // Write all data
    std::stringstream query;
    while(!items.empty()) {
        const size_t batchsize = 500; // This limit is defined by SQLITE_MAX_COMPOUND_SELECT
        const size_t newsize = items.size() > batchsize ? items.size() - batchsize : 0;
        std::vector<MetadataStorage::SeriesT> batch(items.begin() + static_cast<ssize_t>(newsize), items.end());
        items.resize(newsize);
        query << "INSERT INTO akumuli_series (series_id, keyslist, storage_id)" << std::endl;
        bool first = true;
        for (auto item: batch) {
            LightweightString name, keys;
            auto stid = std::get<2>(item);
            if (split_series(std::get<0>(item), std::get<1>(item), &name, &keys)) {
                if (first) {
                    query << "\tSELECT '" << name << "' as series_id, '"
                                          << keys << "' as keyslist, "
                                          << stid << "  as storage_id"
                                          << std::endl;
                    first = false;
                } else {
                    query << "\tUNION "
                          <<   "SELECT '" << name << "', '"
                                          << keys << "', "
                                          << stid
                                          << std::endl;
                }
            }
        }
        query << ";\n";
    }
    std::string full_query = query.str();
    execute_query(full_query);
}

boost::optional<u64> MetadataStorage::get_prev_largest_id() {
    auto query = "SELECT max(storage_id) FROM akumuli_series;";
    try {
        auto results = select_query(query);
        auto row = results.at(0);
        if (row.empty()) {
            AKU_PANIC("Can't get max storage id");
        }
        auto id = row.at(0);
        if (id == "") {
            // Table is empty
            return boost::optional<u64>();
        }
        return boost::lexical_cast<u64>(id);
    } catch(...) {
        Logger::msg(AKU_LOG_ERROR, boost::current_exception_diagnostic_information().c_str());
        AKU_PANIC("Can't get max storage id");
    }
}

aku_Status MetadataStorage::load_matcher_data(SeriesMatcherBase& matcher) {
    auto query = "SELECT series_id || ' ' || keyslist, storage_id FROM akumuli_series;";
    try {
        auto results = select_query(query);
        for(auto row: results) {
            if (row.size() != 2) {
                continue;
            }
            auto series = row.at(0);
            auto id = boost::lexical_cast<u64>(row.at(1));
            matcher._add(series, id);
        }
    } catch(...) {
        Logger::msg(AKU_LOG_ERROR, boost::current_exception_diagnostic_information().c_str());
        return AKU_EGENERAL;
    }
    return AKU_SUCCESS;
}

aku_Status MetadataStorage::load_rescue_points(std::unordered_map<u64, std::vector<u64>>& mapping) {
    auto query =
        "SELECT storage_id, addr0, addr1, addr2, addr3,"
                          " addr4, addr5, addr6, addr7 "
        "FROM akumuli_rescue_points;";
    try {
        auto results = select_query(query);
        for(auto row: results) {
            if (row.size() != 9) {
                continue;
            }
            auto series_id = boost::lexical_cast<u64>(row.at(0));
            if (errno == ERANGE) {
                Logger::msg(AKU_LOG_ERROR, "Can't parse series id, database corrupted");
                return AKU_EBAD_DATA;
            }
            std::vector<u64> addrlist;
            for (size_t i = 0; i < 8; i++) {
                auto addr = row.at(1 + i);
                if (addr.empty()) {
                    break;
                } else {
                    u64 uaddr;
                    i64 iaddr = boost::lexical_cast<i64>(addr);
                    if (iaddr < 0) {
                        uaddr = ~0ull;
                    } else {
                        uaddr = static_cast<u64>(iaddr);
                    }
                    addrlist.push_back(uaddr);
                }
            }
            mapping[series_id] = std::move(addrlist);
        }
    } catch(...) {
        Logger::msg(AKU_LOG_ERROR, boost::current_exception_diagnostic_information().c_str());
        return AKU_EGENERAL;
    }
    return AKU_SUCCESS;
}

}
