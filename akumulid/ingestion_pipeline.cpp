#include "ingestion_pipeline.h"
#include "logger.h"
#include "utility.h"

#include <thread>

#include <boost/exception/all.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

namespace Akumuli
{

static Logger db_logger_ = Logger("akumuli-storage");

//! Abstraction layer above aku_Cursor
struct AkumuliCursor : DbCursor {
    aku_Cursor* cursor_;

    AkumuliCursor(aku_Cursor* cur) : cursor_(cur) { }

    virtual size_t read(void *dest, size_t dest_size) {
        return aku_cursor_read(cursor_, dest, dest_size);
    }

    virtual int is_done() {
        return aku_cursor_is_done(cursor_);
    }

    virtual bool is_error(aku_Status *out_error_code_or_null) {
        return aku_cursor_is_error(cursor_, out_error_code_or_null);
    }

    virtual void close() {
        aku_cursor_close(cursor_);
    }
};


// Session //

AkumuliSession::AkumuliSession(aku_Session* session)
    : session_(session)
{
}

AkumuliSession::~AkumuliSession() {
    aku_destroy_session(session_);
}

aku_Status AkumuliSession::write(const aku_Sample &sample) {
    return aku_write(session_, &sample);
}

std::shared_ptr<DbCursor> AkumuliSession::query(std::string query) {
    aku_Cursor* cursor = aku_query(session_, query.c_str());
    return std::make_shared<AkumuliCursor>(cursor);
}

std::shared_ptr<DbCursor> AkumuliSession::suggest(std::string query) {
    aku_Cursor* cursor = aku_suggest(session_, query.c_str());
    return std::make_shared<AkumuliCursor>(cursor);
}

std::shared_ptr<DbCursor> AkumuliSession::search(std::string query) {
    aku_Cursor* cursor = aku_search(session_, query.c_str());
    return std::make_shared<AkumuliCursor>(cursor);
}

int AkumuliSession::param_id_to_series(aku_ParamId id, char *buffer, size_t buffer_size) {
    return aku_param_id_to_series(session_, id, buffer, buffer_size);
}

aku_Status AkumuliSession::series_to_param_id(const char *name, size_t size, aku_Sample *sample) {
    return aku_series_to_param_id(session_, name, name + size, sample);
}

int AkumuliSession::name_to_param_id_list(const char* begin, const char* end, aku_ParamId* ids, u32 cap) {
    return aku_name_to_param_id_list(session_, begin, end, ids, cap);
}

// Connection //

AkumuliConnection::AkumuliConnection(const char *path)
    : dbpath_(path)
{
    db_logger_.info() << "Open database at: " << path;
    aku_FineTuneParams params = {};
    db_ = aku_open_database(dbpath_.c_str(), params);
}

AkumuliConnection::~AkumuliConnection() {
    db_logger_.info() << "Close database at: " << dbpath_;
    try {
        aku_close_database(db_);
    } catch (...) {
        db_logger_.error() << boost::current_exception_diagnostic_information(true);
        std::terminate();
    }
}

std::string AkumuliConnection::get_all_stats() {
    std::vector<char> buffer;
    buffer.resize(0x1000);
    int nbytes = aku_json_stats(db_, buffer.data(), buffer.size());
    if (nbytes > 0) {
        return std::string(buffer.data(), buffer.data() + nbytes);
    }
    return "Can't generate stats, buffer is too small";
}

std::shared_ptr<DbSession> AkumuliConnection::create_session() {
    auto session = aku_create_session(db_);
    std::shared_ptr<DbSession> result;
    result.reset(new AkumuliSession(session));
    return result;
}

}
