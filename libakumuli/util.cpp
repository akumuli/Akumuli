/**
 * Copyright (c) 2016 Eugene Lazin <4lazin@gmail.com>, Mark Adler <madler@alumni.caltech.edu>
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
#include <chrono>
#include <thread>
#include <sstream>
#include <iostream>

#include <sys/mman.h>

#include "log_iface.h"

namespace Akumuli
{

std::string apr_error_message(apr_status_t status) {
    char error_message[0x100];
    apr_strerror(status, error_message, 0x100);
    return std::string(error_message);
}

static void aku_empty_panic_handler(const char* msg) {
    // This is default panic handler, it does nothing.
    // After panic handler returns - akumuli will
    // throw exception.
}

static aku_panic_handler_t g_panic_handler = &aku_empty_panic_handler;

void set_panic_handler(aku_panic_handler_t new_panic_handler) {
    g_panic_handler = new_panic_handler;
}

// coverity[+kill]
void invoke_panic_handler(const char* message) {
    (*g_panic_handler)(message);
    Logger::msg(AKU_LOG_ERROR, message);
    std::terminate();
}

// coverity[+kill]
void invoke_panic_handler(std::string const& message) {
    invoke_panic_handler(message.c_str());
}

MemoryMappedFile::MemoryMappedFile(const char* file_name, bool enable_huge_tlb)
    : mem_pool_()
    , mmap_()
    , fp_()
    , finfo_()
    , status_(APR_EINIT)
    , path_(file_name)
    , enable_huge_tlb_(enable_huge_tlb)
{
    map_file();
}

void MemoryMappedFile::move_file(const char* new_name) {
    status_ = apr_file_rename(path_.c_str(), new_name, mem_pool_);
    if (status_ == APR_SUCCESS) {
        path_ = new_name;
    }
}

void MemoryMappedFile::delete_file() {
    using namespace std;
    status_ = apr_file_remove(path_.c_str(), mem_pool_);
    if (status_ != APR_SUCCESS) {
        stringstream fmt;
        fmt << "Can't remove file " << path_ << " error " << error_message();
        Logger::msg(AKU_LOG_ERROR, fmt.str().c_str());
    }
}

apr_status_t MemoryMappedFile::map_file() {
    using namespace std;
    int success_count = 0;
    status_ = apr_pool_create(&mem_pool_, NULL);
    if (status_ == APR_SUCCESS) {
        success_count++;
        status_ = apr_file_open(&fp_, path_.c_str(), APR_WRITE|APR_READ, APR_OS_DEFAULT, mem_pool_);
        if (status_ == APR_SUCCESS) {
            success_count++;
            status_ = apr_file_lock(fp_, APR_FLOCK_EXCLUSIVE);
            if (status_ == APR_SUCCESS) {
                // No need to increment success_count, no cleanup needed for apr_file_lock
                status_ = apr_file_info_get(&finfo_, APR_FINFO_SIZE, fp_);
                if (status_ == APR_SUCCESS) {
                    success_count++;
                    apr_int32_t flags = APR_MMAP_WRITE | APR_MMAP_READ;
                    if (enable_huge_tlb_) {
#if defined MAP_HUGETLB
						flags |= MAP_HUGETLB;
#endif
                    }
                    status_ = apr_mmap_create(&mmap_, fp_, 0, finfo_.size, flags, mem_pool_);
                    if (status_ == APR_SUCCESS)
                        success_count++; }}}}

    if (status_ != APR_SUCCESS) {
        free_resources(success_count);
        stringstream err;
        err << "Can't mmap file " << path_ << ", error " << error_message() << " on step " << success_count;
        Logger::msg(AKU_LOG_ERROR, err.str().c_str());
    }
    return status_;
}

void MemoryMappedFile::remap_file_destructive() {
    using namespace std;
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
        stringstream err;
        err << "Can't remap file " << path_ << " error " << apr_error_message(status) << " on step " << success_counter;
        Logger::msg(AKU_LOG_ERROR, err.str().c_str());
        AKU_PANIC("can't remap file");
    }
    status = map_file();
    if (status != APR_SUCCESS) {
        stringstream err;
        err << "Can't remap file " << path_ << " error " << apr_error_message(status) << " on step " << success_counter;
        Logger::msg(AKU_LOG_ERROR, err.str().c_str());
        AKU_PANIC("can't remap file");
    }
}

bool MemoryMappedFile::is_bad() const {
    return status_ != APR_SUCCESS;
}

std::string MemoryMappedFile::error_message() const {
    char error_message[0x100];
    apr_strerror(status_, error_message, 0x100);
    return std::string(error_message);
}

apr_status_t MemoryMappedFile::status_code() const {
    return status_;
}

void MemoryMappedFile::panic_if_bad() {
    if (status_ != APR_SUCCESS) {
        char error_message[0x100];
        apr_strerror(status_, error_message, 0x100);
        AKU_PANIC(error_message);
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

void* MemoryMappedFile::get_pointer() const {
    return mmap_->mm;
}

size_t MemoryMappedFile::get_size() const {
    return mmap_->size;
}

apr_status_t MemoryMappedFile::flush() {
    return flush(0, mmap_->size);
}

aku_Status MemoryMappedFile::protect_all() {
    if (!mprotect(mmap_->mm, mmap_->size, PROT_READ)) {
        return AKU_SUCCESS;
    }
    int err = errno;
    aku_Status ret = AKU_EGENERAL;
    switch(err) {
    case ENOMEM:
        ret = AKU_ENO_MEM;
        break;
    case EACCES:
        ret = AKU_EBAD_DATA;
        break;
    };
    return ret;
}

aku_Status MemoryMappedFile::unprotect_all() {
    if (!mprotect(mmap_->mm, mmap_->size, PROT_WRITE)) {
        return AKU_SUCCESS;
    }
    int err = errno;
    aku_Status ret = AKU_EGENERAL;
    switch(err) {
    case ENOMEM:
        ret = AKU_ENO_MEM;
        break;
    case EACCES:
        ret = AKU_EBAD_DATA;
        break;
    };
    return ret;
}

apr_status_t MemoryMappedFile::flush(size_t from, size_t to) {
    void* p = align_to_page(static_cast<char*>(mmap_->mm) + from, get_page_size());
    size_t len = to - from;
    if (msync(p, len, MS_SYNC) == 0) {
        return AKU_SUCCESS;
    }
    int e = errno;
    switch(e) {
    case EBUSY:
        Logger::msg(AKU_LOG_ERROR, "Can't msync, busy");
        return AKU_EBUSY;
    case EINVAL:
    case ENOMEM:
        Logger::msg(AKU_LOG_ERROR, "Invalid args passed to msync");
        return AKU_EBAD_ARG;
    default:
        Logger::msg(AKU_LOG_ERROR, "Unknown msync error");
    };
    return AKU_EGENERAL;
}

i64 log2(i64 value) {
    return static_cast<i64>(8*sizeof(u64) - __builtin_clzll((u64)value) - 1);
}


const void* align_to_page(const void* ptr, size_t page_size) {
    return reinterpret_cast<const void*>(
        reinterpret_cast<unsigned long long>(ptr) & ~(page_size - 1));
}

void* align_to_page(void* ptr, size_t page_size) {
    return reinterpret_cast<void*>(
        reinterpret_cast<unsigned long long>(ptr) & ~(page_size - 1));
}

void prefetch_mem(const void* ptr, size_t mem_size) {
    auto aptr = align_to_page(ptr, get_page_size());
    int err = madvise(const_cast<void*>(aptr), mem_size, MADV_WILLNEED);
    switch(err) {
    case EBADF:
        AKU_PANIC("(madvise) the map exists, but the area maps something that isn't a file");
    case EINVAL:
        // Severe error - panic!
        AKU_PANIC("(madvise) the value is negative | addr is not page-aligned | advice is not a valid value |...");
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

size_t get_page_size() {
    auto page_size = sysconf(_SC_PAGESIZE);
    if (AKU_UNLIKELY(page_size < 0)) {
        AKU_PANIC("sysconf error, can't get _SC_PAGESIZE");
    }
    return static_cast<size_t>(page_size);
}

Rand::Rand()
    : rand_()
{
    typename std::chrono::system_clock seed_clock;
    auto init_seed = static_cast<unsigned>(seed_clock.now().time_since_epoch().count());
    rand_.seed(init_seed);
}

u32 Rand::operator () () {
    return (u32)rand_();
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

bool same_value(double a, double b) {
    union Bits {
        double d;
        u64 u;
    };
    Bits ba = {};
    ba.d = a;
    Bits bb = {};
    bb.d = b;
    return ba.u == bb.u;
}


}

