/**
 * PRIVATE HEADER
 *
 * Utils
 *
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


#pragma once

#include <apr_general.h>
#include <apr_mmap.h>
#include <stdexcept>
#include <ostream>
#include <atomic>
#include <vector>
#include <tuple>
#include <random>
#include <boost/throw_exception.hpp>
#include "akumuli.h"

namespace Akumuli
{
    /** APR error converter */
    std::string apr_error_message(apr_status_t status) noexcept;

    /** APR error wrapper.
     *  Code must deal with APR error codes and only if
     *  it can't handle some error - it must throw AprException
     *  to panic!
     */
    struct AprException : public std::runtime_error
    {
        apr_status_t status;
        AprException(apr_status_t status, const char* message);
    };

    std::ostream& operator << (std::ostream& str, AprException const& except);


    /** Memory mapped file
      * maps all file on construction
      */
    class MemoryMappedFile
    {
        apr_pool_t* mem_pool_;  //< local memory pool
        apr_mmap_t *mmap_;
        apr_file_t *fp_;
        apr_finfo_t finfo_;
        apr_status_t status_;
        std::string path_;
        int tag_;
        aku_printf_t logger_;
    public:
        MemoryMappedFile(const char* file_name, int tag, aku_printf_t logger) noexcept;
        ~MemoryMappedFile();
        void* get_pointer() const noexcept;
        size_t get_size() const noexcept;
        apr_status_t flush() noexcept;
        bool is_bad() const noexcept;
        std::string error_message() const noexcept;
        void panic_if_bad();
        apr_status_t status_code() const noexcept;
        //! Remap file in a destructive way (all file content is lost)
        void remap_file_destructive();
    private:
        //! Map file into virtual address space
        apr_status_t map_file() noexcept;
        //! Free OS resources associated with object
        void free_resources(int cnt);
    };

    //! Fast integer logarithm
    int64_t log2(int64_t value) noexcept;

    std::tuple<bool, aku_Status> page_in_core(const void* addr);

    size_t get_page_size();

    const void* align_to_page(const void* ptr, size_t get_page_size);

    void prefetch_mem(const void* ptr, size_t mem_size);

    /** Wrapper for mincore syscall.
     * If everything is OK works as simple wrapper
     * (memory needed for mincore syscall managed by wrapper itself).
     * If non-fatal error occured - acts as in case when all memory is
     * in core (optimistically).
     */
    class PageInfo {
        std::vector<unsigned char> data_;
        size_t page_size_;
        const void* base_addr_;
        size_t len_bytes_;

        void fill_mem();
    public:

        /** C-tor.
         * @param start_addr start address of the monitored memory region
         * @param len_bytes length (in bytes) of the monitored region
         */
        PageInfo(const void* addr, size_t len_bytes);

        //! Query data from OS
        aku_Status refresh(const void* addr);

        //! Check if memory address is in core
        bool in_core(const void* addr);

        //! Check if underlying memory is swapped to disk
        bool swapped();
    };

    class Rand {
        std::ranlux48_base rand_;
    public:
        Rand();
        uint32_t operator () ();
    };

    /** Reader writer lock
     *  mutex.
     */
    class RWLock {
        // TODO: specializations for different platforms
        pthread_rwlock_t rwlock_;
    public:
        RWLock();

        ~RWLock();

        void rdlock();

        bool try_rdlock();

        void wrlock();

        bool try_wrlock();

        void unlock();
    };
}

#define AKU_PANIC(msg) BOOST_THROW_EXCEPTION(std::runtime_error(msg));
#define AKU_APR_PANIC(status, msg) BOOST_THROW_EXCEPTION(Akumuli::AprException(status, msg));

#define AKU_UNUSED(x) (void)(x)
