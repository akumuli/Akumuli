#include "httpserver.h"
#include "util.h"

namespace Akumuli {
namespace Http {

//! Microhttpd callback functions
namespace MHD {
static ssize_t read_callback(void *data, uint64_t pos, char *buf, size_t max) {
    AKU_UNUSED(pos);
    QueryCursor* cur = (QueryCursor*)data;
    size_t sz = cur->read_some(buf, max);
    if (sz == 0) {
        return MHD_CONTENT_READER_END_OF_STREAM;
    }
    return sz;
}

static void free_callback(void *data) {
    QueryCursor* cur = (QueryCursor*)data;
    cur->close();
    delete cur;
}


struct PostProcessor {
    MHD_PostProcessor* proc_;

    static int post_data_iter (void *cls,
                               enum MHD_ValueKind kind,
                               const char *key,
                               const char *filename,
                               const char *content_type,
                               const char *transfer_encoding,
                               const char *data,
                               uint64_t off,
                               size_t size)
    {
        PostProcessor* self = static_cast<PostProcessor*>(cls);
        // TODO: process data using `self`
    }

    PostProcessor(MHD_Connection* con) {
        proc_ = MHD_create_post_processor(con, 2048, &post_data_iter, this);
        if (proc_ == nullptr) {
            BOOST_THROW_EXCEPTION(std::runtime_error("can't create POST processor"));
        }
    }

    ~PostProcessor() {
        MHD_destroy_post_processor(proc_);
    }

    int process(const char* upload_data, size_t upload_data_size) {
        return MHD_post_process(proc_, upload_data, upload_data_size);
    }
};

static int accept_connection(void           *cls,
                             MHD_Connection *connection,
                             const char     *url,
                             const char     *method,
                             const char     *version,
                             const char     *upload_data,
                             size_t         *upload_data_size,
                             void          **con_cls)
{
    PostProcessor* postproc = static_cast<PostProcessor>(*con_cls);

    if (postproc == nullptr) {
        postproc = new PostProcessor(con);
        *con_cls = postproc;
        return MHD_YES;
    }
    if (*upload_data_size) {
        auto ret = postproc->process(upload_data, *upload_data_size);
        *upload_data_size = 0;
        return ret;
    }
    QueryProcessor *queryproc = static_cast<QueryProcessor*>(cls);
    QueryCursor *cursor = proc->process(nullptr, 0);
    auto response = MHD_create_response_from_callback(MHD_SIZE_UNKNOWN, 64*1024, &read_callback, cursor, &free_callback);
    int ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
    MHD_destroy_response(response);
    delete postproc;
    return ret;
}
}

HttpServer::HttpServer(std::shared_ptr<QueryProcessor> qproc, AccessControlList const& acl)
    : acl_(acl)
{
}

HttpServer::HttpServer(std::shared_ptr<QueryProcessor> qproc)
    : HttpServer(AccessControlList())
{
}

void HttpServer::start() {
    daemon_ = MHD_start_daemon(MHD_USE_THREAD_PER_CONNECTION,
                               port_,
                               NULL,
                               NULL,
                               &MHD::accept_connection,
                               page_buffer.data(),
                               MHD_OPTION_END);
    if (daemon_ == nullptr) {
        BOOST_THROW_EXCEPTION(std::runtime_error("can't start daemon"));
    }
}

}  // namespace Http
}  // namespace Akumuli

