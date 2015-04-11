#include <iostream>
#include <string>

#include <sys/stat.h>
#include <unistd.h>

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
}
