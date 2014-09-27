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
#include <stdio.h>
#include <cassert>
#include <sys/mman.h>
#include "akumuli_def.h"

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

MemoryMappedFile::MemoryMappedFile(const char* file_name, int tag, aku_printf_t logger) noexcept
    : path_(file_name)
    , tag_(tag)
    , logger_(logger)
{
    map_file();
}

apr_status_t MemoryMappedFile::move_file(const char* new_name) {
    return apr_file_rename(path_.c_str(), new_name, this->mem_pool_);
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
        (*logger_)(tag_, "Can't mmap file, error %s on step %d", error_message().c_str(), success_count);
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
        (*logger_)(tag_, "Can't remap file, error %s on step %d", apr_error_message(status).c_str(), success_counter);
        AKU_PANIC("can't remap file");
    }
    status = map_file();
    if (status != APR_SUCCESS) {
        (*logger_)(tag_, "Can't remap file, error %s", apr_error_message(status).c_str());
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

int64_t log2(int64_t value) noexcept {
    // TODO: visual studio version needed
    return static_cast<int64_t>(8*sizeof(uint64_t) - __builtin_clzll((uint64_t)value) - 1);
}

const void* align_to_page(const void* ptr, size_t page_size) {
    return reinterpret_cast<const void*>(
        reinterpret_cast<unsigned long long>(ptr) & ~(page_size - 1));
}

void prefetch_mem(const void* ptr, size_t mem_size) {
    auto aptr = align_to_page(ptr, get_page_size());
    int err = madvise(const_cast<void*>(aptr), mem_size, MADV_WILLNEED);
    switch(err) {
    case EBADF:
        AKU_PANIC("(madvise) the map exists, but the area maps something that isn't a file");
        break;
    case EINVAL:
        // Severe error - panic!
        AKU_PANIC("(madvise) the value is negative | addr is not page-aligned | advice is not a valid value |...");
        break;

    case EAGAIN: //  A kernel resource was temporarily unavailable.
    case EIO:    // Paging  in  this  area  would  exceed  the process's maximum resident set size.
    case ENOMEM: // Not enough memory: paging in failed.
    default:
        break;
    };
    auto begin = static_cast<const char*>(aptr);
    auto end = begin + mem_size;
    auto step = get_page_size();
    volatile char acc = 0;
    while(begin < end) {
        acc += *begin;
        begin += step;
    }
}

static const unsigned char MINCORE_MASK = 1;

PageInfo::PageInfo(const void* start_addr, size_t len_bytes)
    : page_size_(sysconf(_SC_PAGESIZE))
    , base_addr_(align_to_page(start_addr, page_size_))
    , len_bytes_(len_bytes)
{
    assert(len_bytes <= 4UL*1024UL*1024UL*1024UL);
    auto len = (len_bytes_ + page_size_ - 1) / page_size_;
    data_.resize(len);
}

bool PageInfo::swapped() {
    fill_mem();
    refresh(base_addr_);
    auto res = std::accumulate(data_.begin(), data_.end(), MINCORE_MASK,
                               [](unsigned char a, unsigned char b) { return a & b;});
    return !static_cast<bool>(res & MINCORE_MASK);
}

aku_Status PageInfo::refresh(const void *addr) {
    base_addr_ = align_to_page(addr, page_size_);
    int error = mincore(const_cast<void*>(base_addr_), len_bytes_, data_.data());
    aku_Status status = AKU_SUCCESS;
    switch(error) {
    case EFAULT:
        AKU_PANIC("mincore returns EFAULT - vec points to an invalid address");
    case EAGAIN:
        status = AKU_EBUSY;
        break;
    case EINVAL:
    case ENOMEM:
        status = AKU_EBAD_ARG;
        break;
    }
    if (status != AKU_SUCCESS) {
        fill_mem();
    }
    return status;
}

bool PageInfo::in_core(const void* addr) {
    auto req = reinterpret_cast<const unsigned char*>(addr);
    auto base = reinterpret_cast<const unsigned char*>(base_addr_);
    if (req < base) {
        return false;
    }
    auto len = req - base;
    size_t ix = len / page_size_;
    if (ix < data_.size()) {
        return data_[ix] & MINCORE_MASK;
    }
    return false;
}

void PageInfo::fill_mem() {
    std::fill(data_.begin(), data_.end(), MINCORE_MASK);
}

size_t get_page_size() {
    auto page_size = sysconf(_SC_PAGESIZE);
    return page_size;
}

std::tuple<bool, aku_Status> page_in_core(const void* addr) {
    auto page_size = sysconf(_SC_PAGESIZE);
    auto base_addr = align_to_page(addr, page_size);
    unsigned char val;
    int error = mincore(const_cast<void*>(base_addr), 1, &val);
    aku_Status status = AKU_SUCCESS;
    switch(error) {
    case EFAULT:
        AKU_PANIC("mincore returns EFAULT - vec points to an invalid address");
    case EAGAIN:
        status = AKU_EBUSY;
        val = MINCORE_MASK;
        break;
    case EINVAL:
    case ENOMEM:
        status = AKU_EBAD_ARG;
        val = MINCORE_MASK;
        break;
    }
    return std::make_tuple(val&MINCORE_MASK, status);
}

Rand::Rand()
    : rand_()  // TODO: add seed
{
}

uint32_t Rand::operator () () {
    return (uint32_t)rand_();
}


RWLock::RWLock()
    : rwlock_ PTHREAD_RWLOCK_INITIALIZER
{
    int error = pthread_rwlock_init(&rwlock_, nullptr);
    if (error) {
        AKU_PANIC("pthread_rwlock_init error");
    }
}

RWLock::~RWLock() {
    pthread_rwlock_destroy(&rwlock_);
}

void RWLock::rdlock() {
    int err = pthread_rwlock_rdlock(&rwlock_);
    if (err) {
        AKU_PANIC("pthread_rwlock_rdlock error");
    }
}

bool RWLock::try_rdlock() {
    int err = pthread_rwlock_tryrdlock(&rwlock_);
    switch(err) {
    case 0:
        return true;
    case EBUSY:
    case EDEADLK:
        return false;
    default:
        break;
    }
    AKU_PANIC("pthread_rwlock_tryrdlock error");
}

void RWLock::wrlock() {
    int err = pthread_rwlock_wrlock(&rwlock_);
    if (err) {
        AKU_PANIC("pthread_rwlock_wrlock error");
    }
}

bool RWLock::try_wrlock() {
    int err = pthread_rwlock_trywrlock(&rwlock_);
    switch(err) {
    case 0:
        return true;
    case EBUSY:
    case EDEADLK:
        return false;
    default:
        break;
    }
    AKU_PANIC("pthread_rwlock_trywrlock error");
}

void RWLock::unlock() {
    int err = pthread_rwlock_unlock(&rwlock_);
    if (err) {
        AKU_PANIC("pthread_rwlock_unlock error");
    }
}

}

