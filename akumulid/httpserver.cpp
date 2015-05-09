#include "httpserver.h"
#include "utility.h"

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

static int accept_connection(void           *cls,
                             MHD_Connection *connection,
                             const char     *url,
                             const char     *method,
                             const char     *version,
                             const char     *upload_data,
                             size_t         *upload_data_size,
                             void          **con_cls)
{
    if (strcmp(method, "POST") == 0) {
        QueryProcessor *queryproc = static_cast<QueryProcessor*>(cls);
        QueryCursor* cursor = static_cast<QueryCursor*>(*con_cls);

        if (cursor == nullptr) {
            cursor = queryproc->create();
            *con_cls = cursor;
            return MHD_YES;
        }
        if (*upload_data_size) {
            cursor->append(upload_data, *upload_data_size);
            *upload_data_size = 0;
            return MHD_YES;
        }
        auto response = MHD_create_response_from_callback(MHD_SIZE_UNKNOWN, 64*1024, &read_callback, cursor, &free_callback);
        int ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
        MHD_destroy_response(response);
        return ret;
    } else {
        // Unsupported method
        // TODO: implement GET handler for simple queries (self diagnostics)
        return MHD_NO;
    }
}
}

HttpServer::HttpServer(unsigned short port, std::shared_ptr<QueryProcessor> qproc, AccessControlList const& acl)
    : acl_(acl)
    , proc_(qproc)
    , port_(port)
{
}

HttpServer::HttpServer(unsigned short port, std::shared_ptr<QueryProcessor> qproc)
    : HttpServer(port, qproc, AccessControlList())
{
}

void HttpServer::start() {
    daemon_ = MHD_start_daemon(MHD_USE_THREAD_PER_CONNECTION,
                               port_,
                               NULL,
                               NULL,
                               &MHD::accept_connection,
                               proc_.get(),
                               MHD_OPTION_END);
    if (daemon_ == nullptr) {
        BOOST_THROW_EXCEPTION(std::runtime_error("can't start daemon"));
    }
}

void HttpServer::stop() {
    MHD_stop_daemon(daemon_);
}

}  // namespace Http
}  // namespace Akumuli

