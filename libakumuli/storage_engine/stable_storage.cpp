#include "stable_storage.h"
#include <boost/filesystem.hpp>

namespace Akumuli {
namespace StorageEngine {

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
    return std::move(pool);
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

//  StableStorageVolume  //

StableStorageVolume::StableStorageVolume(const char* path)
    : path_(path)
{
}

bool StableStorageVolume::exists() const {
    return boost::filesystem::exists(path_);
}

aku_Status StableStorageVolume::open_existing() {
    mmap_.reset(new MemoryMappedFile(path_.c_str(), false));
    if(mmap_->is_bad()) {
        return AKU_EACCESS;
    }
    return AKU_SUCCESS;
}

aku_Status StableStorageVolume::create() {
    _create_file(path_.c_str(), VOLUME_SIZE);
}

char* StableStorageVolume::get_writable_mem() {
    return mmap_->get_pointer();
}

//  StableStorage  //

StableStorage::StableStorage(const char* location)
    : location_(location)
{
    if (!boost::filesystem::exists(location)) {
        AKU_PANIC(std::string("Location ") + location + " doesn't exists");
    }
}

static std::tuple<VolumeId, Offset> split(PageId id) {
    auto res = std::make_tuple(static_cast<VolumeId>(id >> 16), static_cast<Offset>(0xFFFF&id));
    return res;
}

std::shared_ptr<Block> StableStorage::get_block(PageId id) {
    std::shared_ptr<Block> res;
    VolumeId vol;
    Offset   off;
    std::tie(vol, off) = id;
    auto it = volumes_.find(vol);
    if (it == volumes_.end()) {
        return res;
    }
    auto volume = it->second;
    char* ptr   = volume->get_writable_mem();
    ptr        += BLOCK_SIZE;
    res.reset(new Block(0, ptr));  // TODO: fix
    return res;
}

}
}
