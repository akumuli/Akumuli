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

#include <sstream>

#include <boost/lexical_cast.hpp>
#include <boost/exception/diagnostic_information.hpp>

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

MetadataStorage::MetadataStorage(const char* db, aku_logger_cb_t logger)
    : pool_(nullptr, &delete_apr_pool)
    , driver_(nullptr)
    , handle_(nullptr, AprHandleDeleter(nullptr))
    , logger_(logger)
{
    apr_pool_t *pool = nullptr;
    auto status = apr_pool_create(&pool, NULL);
    if (status != APR_SUCCESS) {
        // report error (can't return error from c-tor)
        throw std::runtime_error("Can't create memory pool");
    }
    pool_.reset(pool);

    status = apr_dbd_get_driver(pool, "sqlite3", &driver_);
    if (status != APR_SUCCESS) {
        (*logger_)(AKU_LOG_ERROR, "Can't load driver, maybe libaprutil1-dbd-sqlite3 isn't installed");
        throw std::runtime_error("Can't load sqlite3 dirver");
    }

    apr_dbd_t *handle = nullptr;
    status = apr_dbd_open(driver_, pool, db, &handle);
    if (status != APR_SUCCESS) {
        (*logger_)(AKU_LOG_ERROR, "Can't open database, check file path");
        throw std::runtime_error("Can't open database");
    }
    handle_ = HandleT(handle, AprHandleDeleter(driver_));

    create_tables();

    // Create prepared statement
    const char* query = "INSERT INTO akumuli_series (series_id, keyslist, storage_id) VALUES (%s, %s, %d)";
    status = apr_dbd_prepare(driver_, pool_.get(), handle_.get(), query, "INSERT_SERIES_NAME", &insert_);
    if (status != 0) {
        (*logger_)(AKU_LOG_ERROR, "Error creating prepared statement");
        throw std::runtime_error(apr_dbd_error(driver_, handle_.get(), status));
    }
}

int MetadataStorage::execute_query(std::string query) {
    if (logger_ != &aku_console_logger) {
        (*logger_)(AKU_LOG_TRACE, query.c_str());
    }
    int nrows = -1;
    int status = apr_dbd_query(driver_, handle_.get(), &nrows, query.c_str());
    if (status != 0 && status != 21) {
        // generate error and throw
        (*logger_)(AKU_LOG_ERROR, "Error executing query");
        throw std::runtime_error(apr_dbd_error(driver_, handle_.get(), status));
    }
    return nrows;
}

void MetadataStorage::create_tables() {
    const char* query = nullptr;

    // Create volumes table
    query =
            "CREATE TABLE IF NOT EXISTS akumuli_volumes("
            "id INTEGER UNIQUE,"
            "path TEXT UNIQUE"
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
}

void MetadataStorage::init_config(uint32_t compression_threshold,
                                  uint32_t max_cache_size,
                                  uint64_t window_size,
                                  const char* creation_datetime)
{
    // Create table and insert data into it

    std::stringstream insert;
    insert << "INSERT INTO akumuli_configuration (name, value, comment)" << std::endl;
    insert << "\tSELECT 'compression_threshold' as name, '" << compression_threshold << "' as value, "
           << "'Compression threshold value' as comment" << std::endl;
    insert << "\tUNION SELECT 'max_cache_size', '" << max_cache_size
           << "', 'Maximal cache size'" << std::endl;
    insert << "\tUNION SELECT 'window_size', '" << window_size << "', 'Write window size'" << std::endl;
    insert << "\tUNION SELECT 'creation_time', '" << creation_datetime
           << "', 'Database creation time'" << std::endl;
    std::string insert_query = insert.str();
    execute_query(insert_query);
}

void MetadataStorage::get_configs(uint32_t *compression_threshold,
                                  uint32_t *max_cache_size,
                                  uint64_t *window_size,
                                  std::string *creation_datetime)
{
    {   // Read compression_threshold
        std::string query = "SELECT value FROM akumuli_configuration "
                            "WHERE name='compression_threshold'";
        auto results = select_query(query.c_str());
        if (results.size() != 1) {
            throw std::runtime_error("Invalid configuration (compression_threshold)");
        }
        auto tuple = results.at(0);
        if (tuple.size() != 1) {
            throw std::runtime_error("Invalid configuration query (compression_threshold)");
        }
        *compression_threshold = boost::lexical_cast<uint32_t>(tuple.at(0));
    }
    {   // Read max_cache_size
        std::string query = "SELECT value FROM akumuli_configuration WHERE name='max_cache_size'";
        auto results = select_query(query.c_str());
        if (results.size() != 1) {
            throw std::runtime_error("Invalid configuration (max_cache_size)");
        }
        auto tuple = results.at(0);
        if (tuple.size() != 1) {
            throw std::runtime_error("Invalid configuration query (max_cache_size)");
        }
        *max_cache_size = boost::lexical_cast<uint32_t>(tuple.at(0));
    }
    {   // Read window_size
        std::string query = "SELECT value FROM akumuli_configuration WHERE name='window_size'";
        auto results = select_query(query.c_str());
        if (results.size() != 1) {
            throw std::runtime_error("Invalid configuration (window_size)");
        }
        auto tuple = results.at(0);
        if (tuple.size() != 1) {
            throw std::runtime_error("Invalid configuration query (window_size)");
        }
        // This value can be encoded as dobule by the sqlite engine
        *window_size = boost::lexical_cast<uint64_t>(tuple.at(0));
    }
    {   // Read creation time
        std::string query = "SELECT value FROM akumuli_configuration WHERE name='creation_time'";
        auto results = select_query(query.c_str());
        if (results.size() != 1) {
            throw std::runtime_error("Invalid configuration (creation_time)");
        }
        auto tuple = results.at(0);
        if (tuple.size() != 1) {
            throw std::runtime_error("Invalid configuration query (creation_time)");
        }
        // This value can be encoded as dobule by the sqlite engine
        *creation_datetime = tuple.at(0);
    }
}

void MetadataStorage::init_volumes(std::vector<VolumeDesc> volumes) {
    std::stringstream query;
    query << "INSERT INTO akumuli_volumes (id, path)" << std::endl;
    bool first = true;
    for (auto desc: volumes) {
        if (first) {
            query << "\tSELECT " << desc.first << " as id, '" << desc.second << "' as path" << std::endl;
            first = false;
        } else {
            query << "\tUNION SELECT " << desc.first << ", '" << desc.second << "'" << std::endl;
        }
    }
    std::string full_query = query.str();
    execute_query(full_query);
}


std::vector<MetadataStorage::UntypedTuple> MetadataStorage::select_query(const char* query) const {
    (*logger_)(AKU_LOG_TRACE, query);
    std::vector<UntypedTuple> tuples;
    apr_dbd_results_t *results = nullptr;
    int status = apr_dbd_select(driver_, pool_.get(), handle_.get(), &results, query, 0);
    if (status != 0) {
        (*logger_)(AKU_LOG_ERROR, "Error executing query");
        throw std::runtime_error(apr_dbd_error(driver_, handle_.get(), status));
    }
    // get rows
    int ntuples = apr_dbd_num_tuples(driver_, results);
    int ncolumns = apr_dbd_num_cols(driver_, results);
    for (int i = ntuples; i --> 0;) {
        apr_dbd_row_t *row = nullptr;
        status = apr_dbd_get_row(driver_, pool_.get(), results, &row, -1);
        if (status != 0) {
            (*logger_)(AKU_LOG_ERROR, "Error getting row from resultset");
            throw std::runtime_error(apr_dbd_error(driver_, handle_.get(), status));
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
            "SELECT id, path FROM akumuli_volumes;";
    std::vector<VolumeDesc> tuples;
    std::vector<UntypedTuple> untyped = select_query(query);
    // get rows
    auto ntuples = untyped.size();
    for (size_t i = 0; i < ntuples; i++) {
        // get id
        std::string idrepr = untyped.at(i).at(0);
        int id = boost::lexical_cast<int>(idrepr);
        // get path
        std::string path = untyped.at(i).at(1);
        tuples.push_back(std::make_pair(id, path));
    }
    return tuples;
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

void MetadataStorage::insert_new_names(std::vector<MetadataStorage::SeriesT> items) {
    if (items.size() == 0) {
        return;
    }

    execute_query("BEGIN TRANSACTION;");

    // Write all data
    while(!items.empty()) {
        const size_t batchsize = 100;
        const size_t newsize = items.size() > batchsize ? items.size() - batchsize : 0;
        std::vector<MetadataStorage::SeriesT> batch(items.begin() + newsize, items.end());
        items.resize(newsize);
        std::stringstream query;
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
        std::string full_query = query.str();
        execute_query(full_query);
    }

    execute_query("END TRANSACTION;");
}

uint64_t MetadataStorage::get_prev_largest_id() {
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
            return 1ul;
        }
        return boost::lexical_cast<uint64_t>(id);
    } catch(...) {
        (*logger_)(AKU_LOG_ERROR, boost::current_exception_diagnostic_information().c_str());
        AKU_PANIC("Can't get max storage id");
    }
}


aku_Status MetadataStorage::load_matcher_data(SeriesMatcher& matcher) {
    auto query = "SELECT series_id || ' ' || keyslist, storage_id FROM akumuli_series;";
    try {
        auto results = select_query(query);
        for(auto row: results) {
            if (row.size() != 2) {
                continue;
            }
            auto series = row.at(0);
            auto id = boost::lexical_cast<uint64_t>(row.at(1));
            matcher._add(series, id);
        }
    } catch(...) {
        (*logger_)(AKU_LOG_ERROR, boost::current_exception_diagnostic_information().c_str());
        return AKU_EGENERAL;
    }
    return AKU_SUCCESS;
}

}
