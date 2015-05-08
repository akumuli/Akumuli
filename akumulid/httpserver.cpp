#include "httpserver.h"
#include "util.h"

namespace Akumuli {
namespace Http {

//! Microhttpd callback functions
namespace MHD {
static ssize_t read_callback(void *data, uint64_t pos, char *buf, size_t max)
{
    AKU_UNUSED(pos);
    QueryCursor* cur = (QueryCursor*)data;
    size_t sz = cur->read_some(buf, max);
    if (sz == 0) {
        return MHD_CONTENT_READER_END_OF_STREAM;
    }
    return sz;
}

static void free_callback(void *data)
{
    QueryCursor* cur = (QueryCursor*)data;
    cur->close();
    delete cur;
}

static int ahc_echo(void * cls,
                    struct MHD_Connection * connection,
                    const char * url,
                    const char * method,
                    const char * version,
                    const char * upload_data,
                    size_t * upload_data_size,
                    void ** ptr) {
    static int dummy;
    QueryProcessor *proc = (QueryProcessor*)cls;
    struct MHD_Response *response;
    int ret;

    if (0 != strcmp(method, "GET")) {
        return MHD_NO; // unexpected method
    }
    if (&dummy != *ptr) {
        /* The first time only the headers are valid,
         do not respond in the first round... */
        *ptr = &dummy;
        return MHD_YES;
    }
    *ptr = NULL; /* clear context pointer */
    QueryCursor *cursor = proc->process(nullptr, 0);  // TODO: get query data
    response = MHD_create_response_from_callback(MHD_SIZE_UNKNOWN, 64*1024, &read_callback, cursor, &free_callback);
    ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
    MHD_destroy_response(response);
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

}  // namespace Http
}  // namespace Akumuli

