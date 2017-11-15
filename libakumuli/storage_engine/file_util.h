#pragma once

#include <apr.h>
#include <apr_general.h>
#include <apr_file_io.h>

#include <boost/exception/all.hpp>

#include "util.h"
#include "log_iface.h"
#include "akumuli_version.h"
#include "akumuli_def.h"

namespace Akumuli {
namespace StorageEngine {

typedef std::unique_ptr<apr_pool_t, void (*)(apr_pool_t*)> AprPoolPtr;
typedef std::unique_ptr<apr_file_t, void (*)(apr_file_t*)> AprFilePtr;

/**
 * @brief Provides a namespace for file utility function
 */
struct FileUtil {

    static void panic_on_error(apr_status_t status, const char* msg);

    static void close_apr_file(apr_file_t* file);

    static AprPoolPtr make_apr_pool();

    static AprFilePtr open_file(const char* file_name, apr_pool_t* pool);

    static size_t get_file_size(apr_file_t* file);

    /** This function creates file with specified size
      */
    static void create_file(const char* file_name, u64 size);
};

}
}
