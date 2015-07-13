#pragma once
#include "httpserver.h"
#include "ingestion_pipeline.h"

namespace Akumuli {

struct QueryCursor : Http::QueryCursor {

    std::string query_text_;
    std::shared_ptr<AkumuliConnection> connection_;

    QueryCursor(std::shared_ptr<AkumuliConnection> con) : connection_(con)
    {
    }

    virtual void start() {
        //connection_->read???
    }

    virtual void append(const char *data, size_t data_size)
    {
        query_text_ += std::string(data, data + data_size);
    }

    virtual aku_Status get_error()
    {
    }

    virtual size_t read_some(char *buf, size_t buf_size)
    {
    }

    virtual void close()
    {
    }
};

struct QueryProcessor : Http::QueryProcessor
{
    virtual Http::QueryCursor *create()
    {
    }
};

}  // namespace
