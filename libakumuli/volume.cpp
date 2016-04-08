#include "volume.h"
#include <apr.h>
#include <apr_general.h>
#include <apr_file_io.h>

#include <boost/exception/all.hpp>

#include "logger.h"

namespace Akumuli {
namespace V2 {

static void throw_on_error(apr_status_t status) {
    if (status != APR_SUCCESS) {
        char error_message[0x100];
        apr_strerror(status, error_message, 0x100);
        Logger::msg(AKU_LOG_ERROR, error_message);
        std::runtime_error error(error_message);
        BOOST_THROW_EXCEPTION(error);
    }
}

static void _close_apr_file(apr_file_t* file) {
    apr_file_close(file);
}

static AprPoolPtr _make_apr_pool() {
    apr_pool_t* mem_pool = NULL;
    apr_status_t status = apr_pool_create(&mem_pool, NULL);
    throw_on_error(status);
    AprPoolPtr pool(mem_pool, &apr_pool_destroy);
    return std::move(pool);
}

static AprFilePtr _create_empty_file(const* file_name) {
    Logger::msg(AKU_LOG_INFO, "Create empty " + std::string(file_name));
    AprPoolPtr pool = _make_apr_pool();
    apr_file_t* pfile = nullptr;
    apr_status_t status = apr_file_open(&pfile, file_name, APR_CREATE|APR_WRITE, APR_OS_DEFAULT, pool);
    throw_on_error(status);
    AprFilePtr file(pfile, &_close_apr_file);
    return std::move(file);
}

static AprFilePtr _open_file(const* file_name, apr_pool_t* pool) {
    apr_file_t* pfile = nullptr;
    apr_status_t status = apr_file_open(&pfile, file_name, APR_READ|APR_WRITE, APR_OS_DEFAULT, pool);
    throw_on_error(status);
    AprFilePtr file(pfile, &_close_apr_file);
    return std::move(file);
}

static size_t _get_file_size(apr_file_t* file) {
    apr_finfo_t info;
    auto status = apr_file_info_get(&info, APR_FINFO_SIZE, file);
    throw_on_error(status);
    return info.size;
}

/** This function creates file with specified size
  */
static void _create_file(const char* file_name, uint64_t size) {
    AprFilePtr file = _create_empty_file(file_name);
    apr_status_t status = apr_file_trunc(file.get(), size);
    throw_on_error(status);
}

Volume::Volume(const char* path)
    : apr_pool_(_make_apr_pool())
    , apr_file_handle_(_open_file(path, apr_pool_.get()))
    , file_size_(_get_file_size(apr_file_handle_.get()))
{
}

void Volume::create_new(const char* path) {
    _create_empty_file(path);
}

std::unique_ptr<Volume> Volume::open_existing(const char* path) {
    std::unique_ptr<Volume> result;
    result.reset(new Volume(path));
    return std::move(result);
}

}}  // namespace
