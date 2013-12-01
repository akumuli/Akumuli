#include <cassert>
#include "storage_manager.h"
#include "page.h"
#include "util.h"
#include <apr_general.h>
#include <log4cxx/logmanager.h>

namespace Akumuli {

const size_t AKU_METADATA_PAGE_SIZE = 1024*1024;
const size_t AKU_MIN_FILE_SIZE = 64*1024*1024;

apr_status_t StorageManager::create_storage(const char* file_name, size_t size) {
    AprStatusChecker status;
    apr_pool_t* mem_pool = NULL;
    apr_file_t* file = NULL;

    try {
        status = apr_pool_create(&mem_pool, NULL);
        // Create new file
        status = apr_file_open(&file, file_name, APR_CREATE|APR_WRITE, APR_OS_DEFAULT, mem_pool);
        // Truncate file
        status = apr_file_trunc(file, size);
        // Done
    } catch(AprException const& error) {
        LOG4CXX_ERROR(s_logger_, "Can't create database, error " << error << " on step " << status.count);
    }
    switch(status.count) {
    case 3:
    case 2:
        status = apr_file_close(file);
    case 1:
        apr_pool_destroy(mem_pool);
    case 0:
        // even apr pool is not created
        break;
    }
    return status.status;
}

apr_status_t StorageManager::init_storage(const char* file_name) {
    try {
        MemoryMappedFile mfile(file_name);
        size_t file_size = mfile.get_size();
        if (file_size < MIN_FILE_SIZE) {
            return APR_EGENERAL;
        }

        // Create meta page
        auto meta_ptr = mfile.get_pointer();
        auto meta_page = new (meta_ptr) PageHeader(PageType::Metadata, 0, AKU_METADATA_PAGE_SIZE);

        // Add creation date
        const int BUF_SIZE = 128;
        char buffer[BUF_SIZE];
        auto entry_size = Entry::get_size(sizeof(MetadataRecord));
        assert(BUF_SIZE >= entry_size);
        auto now = TimeStamp::utc_now();
        auto entry = new ((void*)buffer) Entry(0, now, entry_size);
        auto mem = entry->get_storage();
        auto mrec = new (mem.address) MetadataRecord(now);
        meta_page->add_entry(*entry);

        // Add index page offset
        mrec->tag = MetadataRecord::TypeTag::INTEGER;
        mrec->integer = AKU_METADATA_PAGE_SIZE;
        meta_page->add_entry(*entry);

        // Create index page
        auto index_ptr = (void*)((char*)meta_ptr + AKU_METADATA_PAGE_SIZE);
        auto index_page = new (index_ptr) PageHeader(PageType::Index, 0, file_size - AKU_METADATA_PAGE_SIZE);
        return mfile.flush();
    }
    catch(AprException const& err) {
        return err.status;
    }
}

log4cxx::LoggerPtr StorageManager::s_logger_ = log4cxx::LogManager::getLogger("Akumuli.StorageManager");

}
