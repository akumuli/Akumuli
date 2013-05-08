#include "recorder.h"
#include <cstdio>
#include <cstring>
#include <cstdlib>


// Fwd decs
void flush_callback(Database* db);
void add_sample_callback(Database* db, int32_t param_id, int32_t unix_timestamp, pvalue_t value);

struct DatabaseImpl : public Database
{
    Config _config;

    // private fields
    DatabaseImpl(const Config& config)
        : _config(config) {
        auto len = strlen(config.path_to_file);
        _config.path_to_file = (char*)malloc(len + 1);
        strcpy(_config.path_to_file, config.path_to_file);
        this->flush = &flush_callback;
    }

    ~DatabaseImpl() {
        free(_config.path_to_file);
    }
};

void flush_callback(Database* db) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    printf(dbi->_config.path_to_file);
}

void add_sample_callback(Database* db, int32_t param_id, int32_t unix_timestamp, pvalue_t value) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
}

Database* open_database(const Config& config)
{
    Database* ptr = new DatabaseImpl(config);
    return static_cast<Database*>(ptr);
}

/** Close database.
 */
void close_database(Database* db)
{
    delete db;
}
