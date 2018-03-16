#include "httpserver.h"
#include "utility.h"
#include <cstring>
#include <thread>

#include <boost/bind.hpp>

namespace Akumuli {
namespace Http {

static Logger logger("http");

//! Microhttpd callback functions
namespace MHD {
static ssize_t read_callback(void *data, u64 pos, char *buf, size_t max) {
    AKU_UNUSED(pos);
    ReadOperation* cur = (ReadOperation*)data;
    size_t sz;
    bool is_done;
    std::tie(sz, is_done) = cur->read_some(buf, max);
    if (is_done) {
        logger.info() << "Cursor " << reinterpret_cast<u64>(cur) << " done";
        return MHD_CONTENT_READER_END_OF_STREAM;
    } else {
        if (sz == 0u) {
            // Not at the end of the stream but data is not ready yet.
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    return sz;
}

static void free_callback(void *data) {
    ReadOperation* cur = (ReadOperation*)data;
    cur->close();
    logger.info() << "Cursor " << reinterpret_cast<u64>(cur) << " destroyed";
    delete cur;
}

static ApiEndpoint get_endpoint(const std::string& path) {
    if (path == "/api/query") {
        return ApiEndpoint::QUERY;
    } else if (path == "/api/suggest") {
        return ApiEndpoint::SUGGEST;
    } else if (path == "/api/search") {
        return ApiEndpoint::SEARCH;
    }
    return ApiEndpoint::UNKNOWN;
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
    std::string path = url;
    auto error_response = [&](const char* msg, unsigned int error_code) {
        char buffer[0x200];
        int len = snprintf(buffer, 0x200, "-%s\r\n", msg);
        auto response = MHD_create_response_from_buffer(len, buffer, MHD_RESPMEM_MUST_COPY);
        int ret = MHD_queue_response(connection, error_code, response);
        MHD_destroy_response(response);
        return ret;
    };
    if (strcmp(method, "POST") == 0) {
        ApiEndpoint endpoint = get_endpoint(path);
        if (endpoint != ApiEndpoint::UNKNOWN) {
            ReadOperationBuilder *queryproc = static_cast<ReadOperationBuilder*>(cls);
            ReadOperation* cursor = static_cast<ReadOperation*>(*con_cls);
            if (cursor == nullptr) {
                cursor = queryproc->create(endpoint);
                *con_cls = cursor;
                logger.info() << "Cursor " << reinterpret_cast<u64>(con_cls) << " created";
                return MHD_YES;
            }
            if (*upload_data_size) {
                cursor->append(upload_data, *upload_data_size);
                *upload_data_size = 0;
                return MHD_YES;
            }

            // Should be called once
            try {
                logger.info() << "Cursor " << reinterpret_cast<u64>(con_cls) << " started";
                cursor->start();
            } catch (const std::exception& err) {
                logger.error() << "Cursor " << reinterpret_cast<u64>(con_cls) << " start error: " << err.what();
                return error_response(err.what(), MHD_HTTP_BAD_REQUEST);
            }

            // Check for error
            auto err = cursor->get_error();
            if (err != AKU_SUCCESS) {
                const char* error_msg = aku_error_message(err);
                logger.error() << "Cursor " << reinterpret_cast<u64>(con_cls) << " error: " << error_msg;
                return error_response(error_msg, MHD_HTTP_BAD_REQUEST);
            }

            auto response = MHD_create_response_from_callback(MHD_SIZE_UNKNOWN, 64*1024, &read_callback, cursor, &free_callback);
            int ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
            MHD_destroy_response(response);
            return ret;
        } else {
            std::string error_msg = "Invalid url " + path;
            logger.error() << error_msg << " (POST)";
            return error_response(error_msg.c_str(), MHD_HTTP_NOT_FOUND);
        }
    } else if (strcmp(method, "GET") == 0) {
        static const char* SIGIL = "";
        auto queryproc = static_cast<ReadOperationBuilder*>(cls);
        auto cursor = static_cast<const char*>(*con_cls);
        if (cursor == nullptr) {
            *con_cls = const_cast<char*>(SIGIL);
            return MHD_YES;
        }
        if (path == "/api/stats") {
            std::string stats = queryproc->get_all_stats();
            auto response = MHD_create_response_from_buffer(stats.size(), const_cast<char*>(stats.data()), MHD_RESPMEM_MUST_COPY);
            int ret = MHD_add_response_header(response, "content-type", "application/json");
            if (ret == MHD_NO) {
                return ret;
            }
            ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
            MHD_destroy_response(response);
            return ret;
        } else if (path == "/api/function-names") {
            std::string stats = queryproc->get_resource("function-names");
            auto response = MHD_create_response_from_buffer(stats.size(), const_cast<char*>(stats.data()), MHD_RESPMEM_MUST_COPY);
            int ret = MHD_add_response_header(response, "content-type", "application/json");
            if (ret == MHD_NO) {
                return ret;
            }
            ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
            MHD_destroy_response(response);
            return ret;
        } else {
            std::string error_msg = "Invalid url " + path;
            logger.error() << error_msg << " (GET)";
            return error_response(error_msg.c_str(), MHD_HTTP_NOT_FOUND);
        }
    }
    logger.error() << "Invalid HTTP request, method: " << method << ", path: " << path;
    std::string error_msg = "Invalid request";
    return error_response(error_msg.c_str(), MHD_HTTP_NOT_FOUND);
}
}

HttpServer::HttpServer(unsigned short port, std::shared_ptr<ReadOperationBuilder> qproc, AccessControlList const& acl)
    : acl_(acl)
    , proc_(qproc)
    , port_(port)
    , daemon_(nullptr)  // `start` should be called to initialize daemon_ correctly
{
}

HttpServer::HttpServer(unsigned short port, std::shared_ptr<ReadOperationBuilder> qproc)
    : HttpServer(port, qproc, AccessControlList())
{
}

void HttpServer::start(SignalHandler* sig, int id) {
    logger.info() << "Start MHD daemon";
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

    auto self = shared_from_this();
    sig->add_handler(boost::bind(&HttpServer::stop, std::move(self)), id);
}

void HttpServer::stop() {
    logger.info() << "Stop MHD daemon";
    MHD_stop_daemon(daemon_);
}

static Logger s_logger_("http-server");

struct HttpServerBuilder {

    HttpServerBuilder() {
        ServerFactory::instance().register_type("HTTP", *this);
    }

    std::shared_ptr<Server> operator () (std::shared_ptr<DbConnection>,
                                         std::shared_ptr<ReadOperationBuilder> qproc,
                                         const ServerSettings& settings) {
        if (settings.protocols.size() != 1) {
            s_logger_.error() << "Can't initialize HTTP server, more than one protocol specified";
            BOOST_THROW_EXCEPTION(std::runtime_error("invalid http-server settings"));
        }
        return std::make_shared<HttpServer>(settings.protocols.front().port, qproc);
    }
};

static HttpServerBuilder reg_type;

}  // namespace Http
}  // namespace Akumuli

