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

bool create_storage(std::string work_dir) {
    uint32_t threshold = 10;
    uint64_t windowsize = 100;
    apr_status_t result = aku_create_database("test", work_dir.c_str(), work_dir.c_str(), 2,
                                              threshold, windowsize, 0, nullptr);
    if (result != APR_SUCCESS) {
        return false;
    }

    return true;
}

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

    // Try to delete old storage
    delete_storage(dir);

    if (!create_storage(dir)) {
        std::cout << "Can't create new storage" << std::endl;
        return 3;
    }
}
