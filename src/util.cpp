#include "util.h"
#include <log4cxx/logmanager.h>

namespace Akumuli
{

std::string apr_error_message(apr_status_t status) noexcept {
    char error_message[0x100];
    apr_strerror(status, error_message, 0x100);
    return std::string(error_message);
}

AprException::AprException(apr_status_t status, const char* message)
    : std::runtime_error(message)
    , status(status)
{
}

std::ostream& operator << (std::ostream& str, AprException const& e) {
    str << "Error: " << e.what() << ", status: " << e.status << ".";
    return str;
}

MemoryMappedFile::MemoryMappedFile(const char* file_name) {
    int success_count = 0;

    status_ = apr_pool_create(&mem_pool_, NULL);
    if (status_ == APR_SUCCESS) {
        success_count++;
        status_ = apr_file_open(&fp_, file_name, APR_WRITE|APR_READ, APR_OS_DEFAULT, mem_pool_);
        if (status_ == APR_SUCCESS) {
            success_count++;
            status_ = apr_file_info_get(&finfo_, APR_FINFO_SIZE, fp_);
            if (status_ == APR_SUCCESS) {
                success_count++;
                status_ = apr_mmap_create(&mmap_, fp_, 0, finfo_.size, APR_MMAP_WRITE|APR_MMAP_READ, mem_pool_);
                if (status_ == APR_SUCCESS)
                    success_count++; }}}

    if (status_ != APR_SUCCESS) {
        free_resources(success_count);
        LOG4CXX_ERROR(s_logger_, "Can't mmap file, error " << error_message() << " on step " << success_count);
    }
}

bool MemoryMappedFile::is_bad() const noexcept {
    return status_ != APR_SUCCESS;
}

std::string MemoryMappedFile::error_message() const noexcept {
    char error_message[0x100];
    apr_strerror(status_, error_message, 0x100);
    return std::string(error_message);
}

apr_status_t MemoryMappedFile::status_code() const noexcept {
    return status_;
}

void MemoryMappedFile::throw_if_bad() {
    if (status_ != APR_SUCCESS) {
        char error_message[0x100];
        apr_strerror(status_, error_message, 0x100);
        throw AprException(status_, error_message);
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
    if (!is_bad())
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
