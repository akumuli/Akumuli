/**
 * Copyright (c) 2016 Eugene Lazin <4lazin@gmail.com>
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

#include "volume.h"
#include <apr.h>
#include <apr_general.h>
#include <apr_file_io.h>
#include <set>

#include <boost/exception/all.hpp>

#include "log_iface.h"
#include "akumuli_version.h"

namespace Akumuli {
namespace StorageEngine {

ShreddedBlock::ShreddedBlock()
    : pos_(0)
{
    data_[0].resize(COMPONENT_SIZE);
}

bool ShreddedBlock::is_readonly() const {
    return false;
}

int ShreddedBlock::add() {
    for (int i = 0; i < NCOMPONENTS; i++) {
        if (data_[i].size() == 0) {
            data_[i].resize(COMPONENT_SIZE);
            return i;
        }
    }
    return -1;
}

int ShreddedBlock::space_left() const {
    return AKU_BLOCK_SIZE - pos_;
}

int ShreddedBlock::size() const {
    return pos_;
}

void ShreddedBlock::put(u8 val) {
    int c = pos_ / COMPONENT_SIZE;
    int i = pos_ % COMPONENT_SIZE;
    if (data_[c].empty()) {
        data_[c].resize(COMPONENT_SIZE);
    }
    data_[c][static_cast<size_t>(i)] = val;
    pos_++;
}

bool ShreddedBlock::safe_put(u8 val) {
    int c = pos_ / COMPONENT_SIZE;
    int i = pos_ % COMPONENT_SIZE;
    if (c >= NCOMPONENTS) {
        return false;
    }
    if (data_[c].empty()) {
        data_[c].resize(COMPONENT_SIZE);
    }
    data_[c][static_cast<size_t>(i)] = val;
    pos_++;
    return true;
}

int ShreddedBlock::get_write_pos() const {
    return pos_;
}

void ShreddedBlock::set_write_pos(int pos) {
    int c = pos / COMPONENT_SIZE;
    if (c >= NCOMPONENTS) {
        AKU_PANIC("Invalid shredded block write-position");
    }
    pos_ = pos;
}

//--

const u8* ShreddedBlock::get_data(int component) const {
    return data_[component].data();
}

const u8* ShreddedBlock::get_cdata(int component) const {
    return data_[component].data();
}

u8* ShreddedBlock::get_data(int component) {
    return data_[component].data();
}

size_t ShreddedBlock::get_size(int component) const {
    return data_[component].size();
}

static void panic_on_error(apr_status_t status, const char* msg) {
    if (status != APR_SUCCESS) {
        char error_message[0x100];
        apr_strerror(status, error_message, 0x100);
        Logger::msg(AKU_LOG_ERROR, std::string(msg) + " " + error_message);
        AKU_PANIC(msg);
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
    return pool;
}

static AprFilePtr _open_file(const char* file_name, apr_pool_t* pool) {
    apr_file_t* pfile = nullptr;
    apr_status_t status = apr_file_open(&pfile, file_name, APR_READ|APR_WRITE, APR_OS_DEFAULT, pool);
    panic_on_error(status, "Can't open file");
    AprFilePtr file(pfile, &_close_apr_file);
    return file;
}


static size_t _get_file_size(apr_file_t* file) {
    apr_finfo_t info;
    auto status = apr_file_info_get(&info, APR_FINFO_SIZE, file);
    panic_on_error(status, "Can't get file info");
    return static_cast<size_t>(info.size);
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
    status = apr_file_trunc(file.get(), static_cast<apr_off_t>(size));
    panic_on_error(status, "Can't truncate file");
}

//------------------------- MetaVolume ---------------------------------//

struct VolumeRef {
    u32 version;
    u32 id;
    u32 nblocks;
    u32 capacity;
    u32 generation;
    char path[];
};

static void volcpy(u8* block, const VolumeRegistry::VolumeDesc* desc) {
    VolumeRef* pvolume  = reinterpret_cast<VolumeRef*>(block);
    pvolume->capacity   = desc->capacity;
    pvolume->generation = desc->generation;
    pvolume->id         = desc->id;
    pvolume->nblocks    = desc->nblocks;
    pvolume->version    = desc->version;
    memcpy(pvolume->path, desc->path.data(), desc->path.size());
    pvolume->path[desc->path.size()] = '\0';
}

MetaVolume::MetaVolume(std::shared_ptr<VolumeRegistry> meta)
    : meta_(meta)
{
    auto volumes = meta_->get_volumes();
    file_size_ = volumes.size() * AKU_BLOCK_SIZE;
    double_write_buffer_.resize(file_size_);
    std::set<u32> init_list;
    for (const auto& vol: volumes) {
        if (init_list.count(vol.id) != 0) {
            AKU_PANIC("Duplicate volume record");
        }
        init_list.insert(vol.id);
        auto block = double_write_buffer_.data() + vol.id * AKU_BLOCK_SIZE;
        volcpy(block, &vol);
    }
}

size_t MetaVolume::get_nvolumes() const {
    return file_size_ / AKU_BLOCK_SIZE;
}

std::unique_ptr<MetaVolume> MetaVolume::open_existing(std::shared_ptr<VolumeRegistry> meta) {
    std::unique_ptr<MetaVolume> result;
    result.reset(new MetaVolume(meta));
    return result;
}

//! Helper function
static VolumeRef* get_volref(u8* p, u32 id) {
    u8* it = p + id * AKU_BLOCK_SIZE;
    VolumeRef* vol = reinterpret_cast<VolumeRef*>(it);
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

aku_Status MetaVolume::add_volume(u32 id, u32 capacity, const std::string& path) {
    if (path.size() > AKU_BLOCK_SIZE - sizeof(VolumeRef)) {
        return AKU_EBAD_ARG;
    }

    size_t old_size = double_write_buffer_.size();
    double_write_buffer_.resize(old_size + AKU_BLOCK_SIZE);
    file_size_ += AKU_BLOCK_SIZE;
    u8* block = double_write_buffer_.data() + old_size;
    VolumeRef* pvolume  = reinterpret_cast<VolumeRef*>(block);
    pvolume->capacity   = capacity;
    pvolume->generation = id;
    pvolume->id         = id;
    pvolume->nblocks    = 0;
    pvolume->version    = AKUMULI_VERSION;
    memcpy(pvolume->path, path.data(), path.size());
    pvolume->path[path.size()] = '\0';

    // Update metadata storage
    VolumeRegistry::VolumeDesc vol;
    vol.nblocks         = pvolume->nblocks;
    vol.generation      = pvolume->generation;
    vol.capacity        = pvolume->capacity;
    vol.version         = AKUMULI_VERSION;
    vol.id              = pvolume->id;
    vol.path            = path;

    meta_->add_volume(vol);

    return AKU_SUCCESS;
}

aku_Status MetaVolume::update(u32 id, u32 nblocks, u32 capacity, u32 gen) {
    if (id < file_size_/AKU_BLOCK_SIZE) {
        auto pvol        = get_volref(double_write_buffer_.data(), id);
        pvol->nblocks    = nblocks;
        pvol->capacity   = capacity;
        pvol->generation = gen;
        pvol->version    = AKUMULI_VERSION;

        // Update metadata storage (this update will be written into the sqlite
        // database eventually in the asynchronous manner.
        VolumeRegistry::VolumeDesc vol;
        vol.nblocks      = pvol->nblocks;
        vol.generation   = pvol->generation;
        vol.capacity     = pvol->capacity;
        vol.id           = pvol->id;
        vol.version      = AKUMULI_VERSION;
        vol.path.assign(static_cast<const char*>(pvol->path));
        meta_->update_volume(vol);

        return AKU_SUCCESS;
    }
    return AKU_EBAD_ARG;  // id out of range
}

aku_Status MetaVolume::set_nblocks(u32 id, u32 nblocks) {
    if (id < file_size_/AKU_BLOCK_SIZE) {
        auto pvol = get_volref(double_write_buffer_.data(), id);
        pvol->nblocks = nblocks;

        VolumeRegistry::VolumeDesc vol;
        vol.nblocks      = pvol->nblocks;
        vol.generation   = pvol->generation;
        vol.capacity     = pvol->capacity;
        vol.id           = pvol->id;
        vol.version      = pvol->version;
        vol.path.assign(static_cast<const char*>(pvol->path));
        meta_->update_volume(vol);

        return AKU_SUCCESS;
    }
    return AKU_EBAD_ARG;  // id out of range
}

aku_Status MetaVolume::set_capacity(u32 id, u32 cap) {
    if (id < file_size_/AKU_BLOCK_SIZE) {
        auto pvol = get_volref(double_write_buffer_.data(), id);
        pvol->capacity = cap;

        VolumeRegistry::VolumeDesc vol;
        vol.nblocks      = pvol->nblocks;
        vol.generation   = pvol->generation;
        vol.capacity     = pvol->capacity;
        vol.id           = pvol->id;
        vol.version      = pvol->version;
        vol.path.assign(static_cast<const char*>(pvol->path));
        meta_->update_volume(vol);

        return AKU_SUCCESS;
    }
    return AKU_EBAD_ARG;  // id out of range
}

aku_Status MetaVolume::set_generation(u32 id, u32 gen) {
    if (id < file_size_/AKU_BLOCK_SIZE) {
        auto pvol = get_volref(double_write_buffer_.data(), id);
        pvol->generation = gen;

        VolumeRegistry::VolumeDesc vol;
        vol.nblocks      = pvol->nblocks;
        vol.generation   = pvol->generation;
        vol.capacity     = pvol->capacity;
        vol.id           = pvol->id;
        vol.version      = pvol->version;
        vol.path.assign(static_cast<const char*>(pvol->path));
        meta_->update_volume(vol);

        return AKU_SUCCESS;
    }
    return AKU_EBAD_ARG;  // id out of range
}

void MetaVolume::flush() {
}

aku_Status MetaVolume::flush(u32 id) {
    return AKU_SUCCESS;
}

//--------------------------- Volume -----------------------------------//

Volume::Volume(const char* path, size_t write_pos)
    : apr_pool_(_make_apr_pool())
    , apr_file_handle_(_open_file(path, apr_pool_.get()))
    , file_size_(static_cast<u32>(_get_file_size(apr_file_handle_.get())/AKU_BLOCK_SIZE))
    , write_pos_(static_cast<u32>(write_pos))
    , path_(path)
    , mmap_ptr_(nullptr)
{
#if UINTPTR_MAX == 0xFFFFFFFFFFFFFFF0
    // 64-bit architecture, we can use mmap for speed
    mmap_.reset(new MemoryMappedFile(path, false));
    if (mmap_->is_bad()) {
        // Fallback on error
        Logger::msg(AKU_LOG_ERROR, path_ + " memory mapping error: '" + mmap_->error_message() + "', fallback to `fopen`");
        mmap_.reset();
        mmap_ptr_ = nullptr;
        return;
    }
    mmap_->protect_all();
    mmap_ptr_ = static_cast<const u8*>(mmap_->get_pointer());
    if (mmap_->get_size() != file_size_*AKU_BLOCK_SIZE) {
        Logger::msg(AKU_LOG_ERROR, path_ + " memory mapping error, fallback to `fopen`");
        mmap_ptr_ = nullptr;
        mmap_.reset();
    }
#endif
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
    return result;
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

std::tuple<aku_Status, BlockAddr> Volume::append_block(const ShreddedBlock *source) {
    if (write_pos_ >= file_size_) {
        return std::make_tuple(AKU_EOVERFLOW, 0u);
    }
    apr_off_t seek_off = write_pos_ * AKU_BLOCK_SIZE;
    apr_status_t status = apr_file_seek(apr_file_handle_.get(), APR_SET, &seek_off);
    panic_on_error(status, "Volume seek error");
    apr_size_t bytes_written = 0;
    auto csize = ShreddedBlock::COMPONENT_SIZE;
    const struct iovec vec[] = {
        { const_cast<u8*>(source->get_data(0)) + 0*csize, csize },
        { const_cast<u8*>(source->get_data(1)) + 1*csize, csize },
        { const_cast<u8*>(source->get_data(2)) + 2*csize, csize },
        { const_cast<u8*>(source->get_data(3)) + 3*csize, csize }
    };
    status = apr_file_writev_full(apr_file_handle_.get(), vec, 4, &bytes_written);
    panic_on_error(status, "Volume write error");
    auto result = write_pos_++;
    return std::make_tuple(AKU_SUCCESS, result);
}

//! Read filxed size block from file
aku_Status Volume::read_block(u32 ix, u8* dest) const {
    if (ix >= write_pos_) {
        return AKU_EBAD_ARG;
    }
    if (mmap_ptr_) {
        // Fast path
        size_t offset = ix * AKU_BLOCK_SIZE;
        memcpy(dest, mmap_ptr_ + offset, AKU_BLOCK_SIZE);
        return AKU_SUCCESS;
    }
    apr_off_t offset = ix * AKU_BLOCK_SIZE;
    apr_status_t status = apr_file_seek(apr_file_handle_.get(), APR_SET, &offset);
    panic_on_error(status, "Volume seek error");
    apr_size_t outsize = 0;
    status = apr_file_read_full(apr_file_handle_.get(), dest, AKU_BLOCK_SIZE, &outsize);
    panic_on_error(status, "Volume read error");
    return AKU_SUCCESS;
}

std::tuple<aku_Status, const u8*> Volume::read_block_zero_copy(u32 ix) const {
    if (ix >= write_pos_) {
        return std::make_tuple(AKU_EBAD_ARG, nullptr);
    }
    if (mmap_ptr_) {
        // Fast path
        size_t offset = ix * AKU_BLOCK_SIZE;
        auto ptr = mmap_ptr_ + offset;
        return std::make_tuple(AKU_SUCCESS, ptr);
    }
    return std::make_tuple(AKU_EUNAVAILABLE, nullptr);
}

void Volume::flush() {
    apr_status_t status = apr_file_flush(apr_file_handle_.get());
    panic_on_error(status, "Volume flush error");
}

u32 Volume::get_size() const {
    return file_size_;
}

std::string Volume::get_path() const {
  return path_;
}

}}  // namespace
