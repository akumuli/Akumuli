#include <iostream>
#include <string>

#include <sys/stat.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>

#include "akumuli.h"

const std::string DEFAULT_DIR = "/tmp";

void print_help() {
    std::cout << "Storage engine functional tests" << std::endl;
    std::cout << "-------------------------------" << std::endl;
    std::cout << "param: working directory (default: /tmp)" << std::endl;
    std::cout << "example: ./storage_test ./home/work" << std::endl;
}

bool check_path_exists(std::string path) {
    struct stat s = {0};
    if (!stat(path.c_str(), &s)) {
        return S_ISDIR(s.st_mode);
    }
    return false;
}

void delete_storage(std::string work_dir) {
    std::string path = work_dir + "/test.akumuli";
    aku_remove_database(path.c_str(), &aku_console_logger);
}


/** Storage wrapper class. Allows to test seamlessly local libakumuli instance and akumulid daemon
  * deployed on AWS instance.
  */
struct Storage {
    virtual void create_new() = 0;
    virtual void open() = 0;
    virtual void delete_all() = 0;
    virtual void add(aku_Timestamp ts, aku_ParamId id, double value) = 0;
    virtual void add(aku_Timestamp ts, aku_ParamId id, std::vector<char> const& blob) = 0;
};


struct LibAkumuli : Storage {
    const std::string work_dir_;
    const uint32_t compression_threshold_;
    const uint64_t sliding_window_size_;
    const int32_t n_volumes_;
    const uint32_t durability_;
    bool enable_huge_tlb_;
    const char* DBNAME_;
    aku_Database *db_;

    LibAkumuli(
            std::string work_dir,
            // Creation parameters, used only to create database
            uint32_t compression_threshold,
            uint64_t sliding_window_size,
            int32_t n_volumes,
            // Open parameters, used unly to open database
            uint32_t durability = 1u,
            bool huge_tlb = false
            )
        : work_dir_(work_dir)
        , compression_threshold_(compression_threshold)
        , sliding_window_size_(sliding_window_size)
        , n_volumes_(n_volumes)
        , durability_(durability)
        , enable_huge_tlb_(huge_tlb)
        , DBNAME_("test")
        , db_(nullptr)
    {
    }

    void throw_on_error(aku_Status status) const {
        if (status != AKU_SUCCESS) {
            throw std::runtime_error(aku_error_message(status));
        }
    }

    std::string get_db_file_path() const {
        std::string path = work_dir_ + "/" + DBNAME_ + ".akumuli";
        return path;
    }

    // Storage interface
    virtual void create_new()
    {
        apr_status_t result = aku_create_database(DBNAME_, work_dir_.c_str(), work_dir_.c_str(), n_volumes_,
                                                  compression_threshold_, sliding_window_size_, 0, nullptr);
        throw_on_error(result);
    }

    virtual void open()
    {
        if (db_ != nullptr) {
            throw std::logic_error("Database allready opened");
        }
        aku_FineTuneParams params;
        params.durability = durability_;
        params.enable_huge_tlb = enable_huge_tlb_ ? 1 : 0;
        params.logger = &aku_console_logger;
        std::string path = get_db_file_path();
        db_ = aku_open_database(path.c_str(), params);
        auto status = aku_open_status(db_);
        throw_on_error(status);
    }

    virtual void delete_all()
    {
        std::string path = get_db_file_path();
        auto status = aku_remove_database(path.c_str(), &aku_console_logger);
        throw_on_error(status);
    }

    virtual void add(aku_Timestamp ts, aku_ParamId id, double value) {
        aku_Status status = AKU_EBUSY;
        while(status == AKU_EBUSY) {
            status = aku_write_double_raw(db_, id, ts, value);
        }
        throw_on_error(status);
    }

    virtual void add(aku_Timestamp ts, aku_ParamId id, std::vector<char> const& blob) {
        aku_Status status = AKU_EBUSY;
        aku_MemRange range = { (void*)blob.data(), (uint32_t)blob.size() };
        while(status == AKU_EBUSY) {
            status = aku_write_blob(db_, id, ts, range);
        }
        throw_on_error(status);
    }
};


int main(int argc, const char** argv) {
    std::string dir;
    if (argc == 1) {
        dir = DEFAULT_DIR;
        return 1;
    } else if (argc == 2) {
        dir = argv[1];
        if (dir == "--help") {
            print_help();
            return 0;
        }
        if (!check_path_exists(dir)) {
            std::cout << "Invalid path" << std::endl;
            return 2;
        }
    }
    if (boost::ends_with(dir, "/")) {
        dir.pop_back();
    }
    std::cout << "Working directory: " << dir << std::endl;
    aku_initialize(nullptr);

    uint32_t compression_threshold = 5;
    uint64_t windowsize = 10;
    LibAkumuli storage(dir, compression_threshold, windowsize, 2);

    // Try to delete old data if any
    try {
        storage.delete_all();
    } catch(std::runtime_error const&) {
        // No old data
    }

    storage.create_new();

    storage.open();
}
