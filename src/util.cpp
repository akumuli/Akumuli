/**
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "util.h"
#include <log4cxx/logmanager.h>
#include <stdio.h>

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

MemoryMappedFile::MemoryMappedFile(const char* file_name) noexcept
    : path_(file_name)
{
    map_file();
}

apr_status_t MemoryMappedFile::map_file() noexcept {
    int success_count = 0;
    status_ = apr_pool_create(&mem_pool_, NULL);
    if (status_ == APR_SUCCESS) {
        success_count++;
        status_ = apr_file_open(&fp_, path_.c_str(), APR_WRITE|APR_READ, APR_OS_DEFAULT, mem_pool_);
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
    return status_;
}

void MemoryMappedFile::remap_file_destructive() {
    apr_off_t file_size = finfo_.size;
    free_resources(4);
    apr_pool_t* pool;
    apr_file_t* file_ptr;
    apr_status_t status;
    int success_counter = 0;
    status = apr_pool_create(&pool, NULL);
    if (status == APR_SUCCESS) {
        success_counter++;
        status = apr_file_open(&file_ptr, path_.c_str(), APR_WRITE, APR_OS_DEFAULT, pool);
        if (status == APR_SUCCESS) {
            success_counter++;
            status = apr_file_trunc(file_ptr, 0);
            if (status == APR_SUCCESS) {
                success_counter++;
                status = apr_file_trunc(file_ptr, file_size);
                if (status == APR_SUCCESS) {
                    success_counter++;
                }
            }
        }
    }
    switch(success_counter) {
        case 4:
        case 3:
        case 2:
            apr_file_close(file_ptr);
        case 1:
            apr_pool_destroy(pool);
    };
    if (status != APR_SUCCESS) {
        LOG4CXX_ERROR(s_logger_, "Can't remap file, error " << apr_error_message(status) << " on step " << success_counter);
        AKU_PANIC("can't remap file");
    }
    status = map_file();
    if (status != APR_SUCCESS) {
        LOG4CXX_ERROR(s_logger_, "Can't remap file, error " << apr_error_message(status));
        AKU_PANIC("can't remap file");
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

void MemoryMappedFile::panic_if_bad() {
    if (status_ != APR_SUCCESS) {
        char error_message[0x100];
        apr_strerror(status_, error_message, 0x100);
        AKU_APR_PANIC(status_, error_message);
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


int64_t log2(int64_t value) noexcept {
    // TODO: visual studio version needed
    return static_cast<int64_t>(8*sizeof(uint64_t) - __builtin_clzll((uint64_t)value) - 1);
}

}
