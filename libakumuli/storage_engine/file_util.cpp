#include "file_util.h"

namespace Akumuli {
namespace StorageEngine {

void FileUtil::panic_on_error(apr_status_t status, const char* msg) {
    if (status != APR_SUCCESS) {
        char error_message[0x100];
        apr_strerror(status, error_message, 0x100);
        Logger::msg(AKU_LOG_ERROR, std::string(msg) + " " + error_message);
        AKU_PANIC(msg);
    }
}

void FileUtil::close_apr_file(apr_file_t* file) {
    apr_file_close(file);
}

AprPoolPtr FileUtil::make_apr_pool() {
    apr_pool_t* mem_pool = NULL;
    apr_status_t status = apr_pool_create(&mem_pool, NULL);
    panic_on_error(status, "Can't create APR pool");
    AprPoolPtr pool(mem_pool, &apr_pool_destroy);
    return std::move(pool);
}

AprFilePtr FileUtil::open_file(const char* file_name, apr_pool_t* pool) {
    apr_file_t* pfile = nullptr;
    apr_status_t status = apr_file_open(&pfile, file_name, APR_READ|APR_WRITE, APR_OS_DEFAULT, pool);
    panic_on_error(status, "Can't open file");
    AprFilePtr file(pfile, &close_apr_file);
    return std::move(file);
}

size_t FileUtil::get_file_size(apr_file_t* file) {
    apr_finfo_t info;
    auto status = apr_file_info_get(&info, APR_FINFO_SIZE, file);
    panic_on_error(status, "Can't get file info");
    return static_cast<size_t>(info.size);
}

void FileUtil::create_file(const char* file_name, u64 size) {
    Logger::msg(AKU_LOG_INFO, "Create " + std::string(file_name) + " size: " + std::to_string(size));
    AprPoolPtr pool = make_apr_pool();
    apr_file_t* pfile = nullptr;
    apr_status_t status = apr_file_open(&pfile, file_name, APR_TRUNCATE|APR_CREATE|APR_WRITE, APR_OS_DEFAULT, pool.get());
    panic_on_error(status, "Can't create file");
    AprFilePtr file(pfile, &close_apr_file);
    status = apr_file_trunc(file.get(), static_cast<apr_off_t>(size));
    panic_on_error(status, "Can't truncate file");
}

}
}
