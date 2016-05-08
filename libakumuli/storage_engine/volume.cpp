#include "volume.h"
#include <apr.h>
#include <apr_general.h>
#include <apr_file_io.h>

#include <boost/exception/all.hpp>

#include "log_iface.h"
#include "akumuli_version.h"

namespace Akumuli {
namespace StorageEngine {

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
static void _create_file(const char* file_name, u64 size) {
    Logger::msg(AKU_LOG_INFO, "Create " + std::string(file_name) + " size: " + std::to_string(size));
    AprPoolPtr pool = _make_apr_pool();
    apr_file_t* pfile = nullptr;
    apr_status_t status = apr_file_open(&pfile, file_name, APR_TRUNCATE|APR_CREATE|APR_WRITE, APR_OS_DEFAULT, pool.get());
    panic_on_error(status, "Can't create file");
    AprFilePtr file(pfile, &_close_apr_file);
    status = apr_file_trunc(file.get(), size);
    panic_on_error(status, "Can't truncate file");
}

//------------------------- MetaVolume ---------------------------------//

struct VolumeRef {
    u32 version;
    u32 id;
    u32 nblocks;
    u32 capacity;
    u32 generation;
} __attribute__((packed));

MetaVolume::MetaVolume(const char *path)
    : mmap_(path, false)
    , file_size_(mmap_.get_size())
    , mmap_ptr_((u8*)mmap_.get_pointer())
    , double_write_buffer_(mmap_.get_size(), 0)
{
    memcpy(double_write_buffer_.data(), mmap_ptr_, mmap_.get_size());
}

size_t MetaVolume::get_nvolumes() const {
    return file_size_/AKU_BLOCK_SIZE;
}

void MetaVolume::create_new(const char* path, size_t capacity, const u32 *vol_capacities) {
    size_t size = capacity * AKU_BLOCK_SIZE;
    _create_file(path, size);
    MemoryMappedFile mmap(path, false);
    u8* it = (u8*)mmap.get_pointer();
    u8* end = it + mmap.get_size();
    u32 id = 0;
    // Initialization
    while(it < end) {
        VolumeRef* pvolume = (VolumeRef*)it;
        pvolume->capacity = vol_capacities[id];
        pvolume->generation = id;
        pvolume->id = id;
        pvolume->nblocks = 0;
        pvolume->version = AKUMULI_VERSION;
        it += AKU_BLOCK_SIZE;
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
static VolumeRef* get_volref(u8* p, u32 id) {
    u8* it = p + id * AKU_BLOCK_SIZE;
    VolumeRef* vol = (VolumeRef*)it;
    return vol;
}

std::tuple<aku_Status, u32> MetaVolume::get_nblocks(u32 id) const {
    if (id < file_size_/AKU_BLOCK_SIZE) {
        auto pvol = get_volref(double_write_buffer_.data(), id);
        u32 nblocks = pvol->nblocks;
        return std::make_tuple(AKU_SUCCESS, nblocks);
    }
    return std::make_tuple(AKU_EBAD_ARG, 0u);
}

std::tuple<aku_Status, u32> MetaVolume::get_capacity(u32 id) const {
    if (id < file_size_/AKU_BLOCK_SIZE) {
        auto pvol = get_volref(double_write_buffer_.data(), id);
        u32 cap = pvol->capacity;
        return std::make_tuple(AKU_SUCCESS, cap);
    }
    return std::make_tuple(AKU_EBAD_ARG, 0u);
}

std::tuple<aku_Status, u32> MetaVolume::get_generation(u32 id) const {
    if (id < file_size_/AKU_BLOCK_SIZE) {
        auto pvol = get_volref(double_write_buffer_.data(), id);
        u32 gen = pvol->generation;
        return std::make_tuple(AKU_SUCCESS, gen);
    }
    return std::make_tuple(AKU_EBAD_ARG, 0u);
}

aku_Status MetaVolume::update(u32 id, u32 nblocks, u32 capacity, u32 gen) {
    if (id < file_size_/AKU_BLOCK_SIZE) {
        auto pvol = get_volref(double_write_buffer_.data(), id);
        pvol->nblocks = nblocks;
        pvol->capacity = capacity;
        pvol->generation = gen;
        return AKU_SUCCESS;
    }
    return AKU_EBAD_ARG;  // id out of range
}

aku_Status MetaVolume::set_nblocks(u32 id, u32 nblocks) {
    if (id < file_size_/AKU_BLOCK_SIZE) {
        auto pvol = get_volref(double_write_buffer_.data(), id);
        pvol->nblocks = nblocks;
        return AKU_SUCCESS;
    }
    return AKU_EBAD_ARG;  // id out of range
}

aku_Status MetaVolume::set_capacity(u32 id, u32 cap) {
    if (id < file_size_/AKU_BLOCK_SIZE) {
        auto pvol = get_volref(double_write_buffer_.data(), id);
        pvol->capacity = cap;
        return AKU_SUCCESS;
    }
    return AKU_EBAD_ARG;  // id out of range
}

aku_Status MetaVolume::set_generation(u32 id, u32 gen) {
    if (id < file_size_/AKU_BLOCK_SIZE) {
        auto pvol = get_volref(double_write_buffer_.data(), id);
        pvol->generation = gen;
        return AKU_SUCCESS;
    }
    return AKU_EBAD_ARG;  // id out of range
}

void MetaVolume::flush() {
    memcpy(mmap_ptr_, double_write_buffer_.data(), mmap_.get_size());
    auto status = mmap_.flush();
    panic_on_error(status, "Flush error");
}

aku_Status MetaVolume::flush(u32 id) {
    if (id < file_size_/AKU_BLOCK_SIZE) {
        memcpy(mmap_ptr_, double_write_buffer_.data(), mmap_.get_size());
        size_t from = id * AKU_BLOCK_SIZE;
        size_t to = from + AKU_BLOCK_SIZE;
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
    , file_size_(_get_file_size(apr_file_handle_.get())/AKU_BLOCK_SIZE)
    , write_pos_(write_pos)
{
}

void Volume::reset() {
    write_pos_ = 0;
}

void Volume::create_new(const char* path, size_t capacity) {
    auto size = capacity * AKU_BLOCK_SIZE;
    _create_file(path, size);
}

std::unique_ptr<Volume> Volume::open_existing(const char* path, size_t pos) {
    std::unique_ptr<Volume> result;
    result.reset(new Volume(path, pos));
    return std::move(result);
}

//! Append block to file (source size should be 4 at least BLOCK_SIZE)
std::tuple<aku_Status, BlockAddr> Volume::append_block(const u8* source) {
    if (write_pos_ >= file_size_) {
        return std::make_tuple(AKU_EOVERFLOW, 0u);
    }
    apr_off_t seek_off = write_pos_ * AKU_BLOCK_SIZE;
    apr_status_t status = apr_file_seek(apr_file_handle_.get(), APR_SET, &seek_off);
    panic_on_error(status, "Volume seek error");
    apr_size_t bytes_written = 0;
    status = apr_file_write_full(apr_file_handle_.get(), source, AKU_BLOCK_SIZE, &bytes_written);
    panic_on_error(status, "Volume write error");
    auto result = write_pos_++;
    return std::make_tuple(AKU_SUCCESS, result);
}

//! Read filxed size block from file
aku_Status Volume::read_block(u32 ix, u8* dest) const {
    if (ix >= write_pos_) {
        return AKU_EBAD_ARG;
    }
    apr_off_t offset = ix * AKU_BLOCK_SIZE;
    apr_status_t status = apr_file_seek(apr_file_handle_.get(), APR_SET, &offset);
    panic_on_error(status, "Volume seek error");
    apr_size_t outsize = 0;
    status = apr_file_read_full(apr_file_handle_.get(), dest, AKU_BLOCK_SIZE, &outsize);
    panic_on_error(status, "Volume read error");
    return AKU_SUCCESS;
}

void Volume::flush() {
    apr_status_t status = apr_file_flush(apr_file_handle_.get());
    panic_on_error(status, "Volume flush error");
}

u32 Volume::get_size() const {
    return file_size_;
}

}}  // namespace
