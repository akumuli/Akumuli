#include "util.h"
#include <log4cxx/logmanager.h>

namespace Akumuli
{

AprException::AprException(apr_status_t status, const char* message)
    : std::runtime_error(message)
    , status(status)
{
}

std::ostream& operator << (std::ostream& str, AprException const& e) {
    str << "Error: " << e.what() << ", status: " << e.status << ".";
    return str;
}

void apr_throw_if_error(apr_status_t status) {
    if (status != APR_SUCCESS) {
        char buf[0x100];
        const char* err = apr_strerror(status, buf, 1024);
        throw AprException(status, err);
    }
}

AprStatusChecker::AprStatusChecker()
    : count(0)
    , status(APR_SUCCESS)
{
}

AprStatusChecker AprStatusChecker::operator = (apr_status_t st) {
    status = st;
    apr_throw_if_error(st);
    count++;
    return *this;
}


MemoryMappedFile::MemoryMappedFile(const char* file_name) {
    AprStatusChecker status;
    try {
        status = apr_pool_create(&mem_pool_, NULL);
        status = apr_file_open(&fp_, file_name, APR_WRITE|APR_READ, APR_OS_DEFAULT, mem_pool_);
        status = apr_file_info_get(&finfo_, APR_FINFO_SIZE, fp_);
        status = apr_mmap_create(&mmap_, fp_, 0, finfo_.size, APR_MMAP_WRITE|APR_MMAP_READ, mem_pool_);
    }
    catch(AprException const& err) {
        free_resources(status.count);
        LOG4CXX_ERROR(s_logger_, "Can't mmap file, error " << err << " on step " << status.count);
        throw;
    }
}

/* Why not std::unique_ptr with custom deleter?
 * To make finalization order explicit and prevent null-reference errors in
 * APR finalizers. Resource finalizers in APR can't handle null pointers,
 * so we will need to wrap each `close` or `destroy` or whatsever
 * function to be able to pass it to unique_ptr c-tor as deleter.
 */

void MemoryMappedFile::free_resources(int cnt)
{
    switch(cnt)
    {
    default:
    case 4:
        apr_mmap_delete(mmap_);
    case 3:
    case 2:
        apr_file_close(fp_);
    case 1:
        apr_pool_destroy(mem_pool_);
    };
}

MemoryMappedFile::~MemoryMappedFile() {
    // there is no exception in construction
    free_resources(4);
}

void* MemoryMappedFile::get_pointer() const noexcept {
    return mmap_->mm;
}

size_t MemoryMappedFile::get_size() const noexcept {
    return mmap_->size;
}

apr_status_t MemoryMappedFile::flush() noexcept {
    return apr_file_flush(fp_);
}

log4cxx::LoggerPtr MemoryMappedFile::s_logger_ = log4cxx::LogManager::getLogger("Akumuli.MemoryMappedFile");


}
