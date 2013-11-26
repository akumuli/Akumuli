#include "storage_manager.h"
#include "page.h"
#include "util.h"
#include <apr_general.h>
#include <log4cxx/logmanager.h>

namespace Akumuli {

apr_status_t StorageManager::create_database(const char* file_name, size_t size) {
    AprStatusChecker status;
    apr_pool_t* mem_pool = NULL;
    apr_file_t* file = NULL;

    try {
        status = apr_pool_create(&mem_pool, NULL);
        // Create new file
        status = apr_file_open(&file, file_name, APR_CREATE, APR_OS_DEFAULT, mem_pool);
        // Truncate file
        status = apr_file_trunc(file, size);
        // Done
    } catch(AprException const& error) {
        LOG4CXX_ERROR(s_logger_, "Can't create database, error " << error << " on step " << status.count);
    }
    switch(status.count) {
    case 3:
    case 2:
        status = apr_file_close(file);
    case 1:
        apr_pool_destroy(mem_pool);
    case 0:
        // even apr pool is not created
        break;
    }
    return status.status;
}

log4cxx::LoggerPtr StorageManager::s_logger_ = log4cxx::LogManager::getLogger("Akumuli.StorageManager");

}
