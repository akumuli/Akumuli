#pragma once
#include "httpserver.h"
#include "ingestion_pipeline.h"

namespace Akumuli {

struct QueryCursor : Http::QueryCursor {

    std::string query_text_;
    std::shared_ptr<DbConnection> connection_;
    std::shared_ptr<DbCursor> cursor_;
    std::vector<aku_Sample> rdbuf_;
    size_t rdbuf_pos_;
    size_t rdbuf_top_;
    static const size_t DEFAULT_RDBUF_SIZE_ = 1000u;

    QueryCursor(std::shared_ptr<DbConnection> con, int readbufsize);

    void throw_if_started() const;

    void throw_if_not_started() const;

    virtual void start();

    virtual void append(const char *data, size_t data_size);

    virtual aku_Status get_error();

    virtual size_t read_some(char *buf, size_t buf_size);

    //! Try to format sample
    char* format(char* begin, char* end, const aku_Sample& sample);

    virtual void close();
};

struct QueryProcessor : Http::QueryProcessor
{
    std::shared_ptr<DbConnection> con_;
    int rdbufsize_;

    QueryProcessor(std::shared_ptr<DbConnection> con, int rdbuf);

    virtual Http::QueryCursor *create();
};

}  // namespace
