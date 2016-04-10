#include "volume.h"
#include <apr.h>
#include <apr_general.h>
#include <apr_file_io.h>

#include <boost/exception/all.hpp>

#include "logger.h"
#include "akumuli_version.h"

namespace Akumuli {
namespace V2 {

static void panic_on_error(apr_status_t status, const char* msg) {
    if (status != APR_SUCCESS) {
        char error_message[0x100];
        apr_strerror(status, error_message, 0x100);
        Logger::msg(AKU_LOG_ERROR, std::string(msg) + " " + error_message);
        AKU_APR_PANIC(status, msg);
    }
}

static void _close_apr_file(apr_file_t* file) {
    apr_file_close(file);
}

static AprPoolPtr _make_apr_pool() {
    apr_pool_t* mem_pool = NULL;
    apr_status_t status = apr_pool_create(&mem_pool, NULL);
    panic_on_error(status, "Can't create APR pool");
    AprPoolPtr pool(mem_pool, &apr_pool_destroy);
    return std::move(pool);
}

static AprFilePtr _create_empty_file(const char* file_name) {
    Logger::msg(AKU_LOG_INFO, "Create empty " + std::string(file_name));
    AprPoolPtr pool = _make_apr_pool();
    apr_file_t* pfile = nullptr;
    apr_status_t status = apr_file_open(&pfile, file_name, APR_CREATE|APR_WRITE, APR_OS_DEFAULT, pool.get());
    panic_on_error(status, "Can't create file");
    AprFilePtr file(pfile, &_close_apr_file);
    return std::move(file);
}

static AprFilePtr _open_file(const char* file_name, apr_pool_t* pool) {
    apr_file_t* pfile = nullptr;
    apr_status_t status = apr_file_open(&pfile, file_name, APR_READ|APR_WRITE, APR_OS_DEFAULT, pool);
    panic_on_error(status, "Can't open file");
    AprFilePtr file(pfile, &_close_apr_file);
    return std::move(file);
}

static size_t _get_file_size(apr_file_t* file) {
    apr_finfo_t info;
    auto status = apr_file_info_get(&info, APR_FINFO_SIZE, file);
    panic_on_error(status, "Can't get file info");
    return info.size;
}

/** This function creates file with specified size
  */
static void _create_file(const char* file_name, uint64_t size) {
    AprFilePtr file = _create_empty_file(file_name);
    apr_status_t status = apr_file_trunc(file.get(), size);
    panic_on_error(status, "Can't truncate file");
}

//------------------------- MetaVolume ---------------------------------//

struct VolumeRef {
    uint32_t version;
    uint32_t id;
    uint32_t nblocks;
    uint32_t capacity;
    uint32_t generation;
} __attribute__((packed));

MetaVolume::MetaVolume(const char *path)
    : mmap_(path, false)
    , file_size_(mmap_.get_size())
    , mmap_ptr_((uint8_t*)mmap_.get_pointer())
{
}

void MetaVolume::create_new(const char* path, size_t capacity, const uint32_t *vol_capacities) {
    size_t size = capacity * BLOCK_SIZE;
    _create_file(path, size);
    MemoryMappedFile mmap(path);
    uint8_t* it = (uint8_t*)mmap.get_pointer();
    uint8_t* end = it + mmap.get_size();
    uint32_t id = 0;
    // Initialization
    while(it < end) {
        VolumeRef* pvolume = (VolumeRef*)it;
        pvolume->capacity = vol_capacities[id];
        pvolume->generation = 0;
        pvolume->id = id;
        pvolume->nblocks = 0;
        pvolume->version = AKUMULI_VERSION;
        it += BLOCK_SIZE;
        id++;
    }
    auto status = mmap.flush();
    panic_on_error(status, "Flush error");
}

std::unique_ptr<MetaVolume> MetaVolume::open_existing(const char* path) {
    std::unique_ptr<MetaVolume> result;
    result.reset(new MetaVolume(path));
    return std::move(result);
}

//! Helper function
static VolumeRef* get_volref(uint8_t* p, uint32_t id) {
    uint8_t* it = p + id * MetaVolume::BLOCK_SIZE;
    VolumeRef* vol = (VolumeRef*)it;
    return vol;
}

std::tuple<aku_Status, uint32_t> MetaVolume::get_nblocks(uint32_t id) const {
    if (id < file_size_/BLOCK_SIZE) {
        auto pvol = get_volref(mmap_ptr_, id);
        return std::make_tuple(AKU_SUCCESS, pvol->nblocks);
    }
    return std::make_tuple(AKU_EBAD_ARG, 0u);
}

std::tuple<aku_Status, uint32_t> MetaVolume::get_capacity(uint32_t id) const {
    if (id < file_size_/BLOCK_SIZE) {
        auto pvol = get_volref(mmap_ptr_, id);
        return std::make_tuple(AKU_SUCCESS, pvol->capacity);
    }
    return std::make_tuple(AKU_EBAD_ARG, 0u);
}

std::tuple<aku_Status, uint32_t> MetaVolume::get_generation(uint32_t id) const {
    if (id < file_size_/BLOCK_SIZE) {
        auto pvol = get_volref(mmap_ptr_, id);
        return std::make_tuple(AKU_SUCCESS, pvol->generation);
    }
    return std::make_tuple(AKU_EBAD_ARG, 0u);
}

aku_Status MetaVolume::update(uint32_t id, uint32_t nblocks, uint32_t capacity, uint32_t gen) {
    if (id < file_size_/BLOCK_SIZE) {
        auto pvol = get_volref(mmap_ptr_, id);
        pvol->nblocks = nblocks;
        pvol->capacity = capacity;
        pvol->generation = gen;
    }
    return AKU_EBAD_ARG;  // id out of range
}

aku_Status MetaVolume::set_nblocks(uint32_t id, uint32_t nblocks) {
    if (id < file_size_/BLOCK_SIZE) {
        auto pvol = get_volref(mmap_ptr_, id);
        pvol->nblocks = nblocks;
    }
    return AKU_EBAD_ARG;  // id out of range
}

aku_Status MetaVolume::set_capacity(uint32_t id, uint32_t cap) {
    if (id < file_size_/BLOCK_SIZE) {
        auto pvol = get_volref(mmap_ptr_, id);
        pvol->capacity = cap;
    }
    return AKU_EBAD_ARG;  // id out of range
}

aku_Status MetaVolume::set_generation(uint32_t id, uint32_t gen) {
    if (id < file_size_/BLOCK_SIZE) {
        auto pvol = get_volref(mmap_ptr_, id);
        pvol->generation = gen;
    }
    return AKU_EBAD_ARG;  // id out of range
}

void MetaVolume::flush() {
    auto status = mmap_.flush();
    panic_on_error(status, "Flush error");
}

aku_Status MetaVolume::flush(uint32_t id) {
    if (id < file_size_/BLOCK_SIZE) {
        size_t from = id * BLOCK_SIZE;
        size_t to = from + BLOCK_SIZE;
        auto status = mmap_.flush(from, to);
        panic_on_error(status, "Flush (range) error");
        return AKU_SUCCESS;
    }
    return AKU_EBAD_ARG;
}

//--------------------------- Volume -----------------------------------//

Volume::Volume(const char* path, size_t write_pos)
    : apr_pool_(_make_apr_pool())
    , apr_file_handle_(_open_file(path, apr_pool_.get()))
    , file_size_(_get_file_size(apr_file_handle_.get()))
    , write_pos_(write_pos)
{
}

void Volume::create_new(const char* path, size_t capacity) {
    auto size = capacity * BLOCK_SIZE;
    _create_file(path, size);
}

std::unique_ptr<Volume> Volume::open_existing(const char* path, size_t pos) {
    std::unique_ptr<Volume> result;
    result.reset(new Volume(path, pos));
    return std::move(result);
}

//! Append block to file (source size should be 4 at least BLOCK_SIZE)
aku_Status Volume::append_block(const uint8_t* source) {
    if (write_pos_ >= file_offset_) {
        return AKU_EOVERFLOW;
    }
    apr_off_t seek_off = write_pos_*BLOCK_SIZE;
    apr_status_t status = apr_file_seek(apr_file_handle_.get(), APR_SET, &seek_off);
    panic_on_error(status, "Volume seek error");
    apr_size_t bytes_written = 0;
    status = apr_file_write_full(apr_file_handle_.get(), source, BLOCK_SIZE, &bytes_written);
    panic_on_error(status, "Volume write error");
    write_pos_++;
    return AKU_SUCCESS;
}

//! Read filxed size block from file
aku_Status Volume::read_block(uint32_t ix, uint8_t* dest) const {
    apr_off_t offset = ix * BLOCK_SIZE;
    if (ix >= file_size_) {
        return AKU_EBAD_ARG;
    }
    apr_status_t status = apr_file_seek(apr_file_handle_.get(), APR_SET, &offset);
    panic_on_error(status, "Volume seek error");
    apr_size_t outsize = 0;
    status = apr_file_read_full(apr_file_handle_.get(), dest, BLOCK_SIZE, &outsize);
    panic_on_error(status, "Volume read error");
    return AKU_SUCCESS;
}

void Volume::flush() {
    apr_status_t status = apr_file_flush(apr_file_handle_.get());
    panic_on_error(status, "Volume flush error");
}

}}  // namespace
