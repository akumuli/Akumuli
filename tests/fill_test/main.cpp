#include <iostream>
#include <cassert>
#include <random>

#include <boost/timer.hpp>
#include <apr_mmap.h>

#include "akumuli.h"
#include "page.h"
#include "storage.h"

using namespace Akumuli;
using namespace std;

const int DB_SIZE = 1;

enum Target {
    NOTHING,
    CREATE,
    WRITE
};

int main(int cnt, const char** args)
{
    Target target = NOTHING;
    if (cnt == 2) {
        string param = args[1];
        if (param == "--create") {
            target = CREATE;
        }
        else if (param == "--write") {
            target = WRITE;
        }
    }
    if (target == NOTHING) {
        std::cout << "Nothing to do" << std::endl;
        return 0;
    }
    if (target == CREATE) {
        // TODO: create interface for storage manager in akumuli.h
        auto result = Storage::create_storage("./test.db", DB_SIZE);
        if (result != APR_SUCCESS) {
            std::cout << "Error in create_storage" << std::endl;
            return 1;
        }
        result = Storage::init_storage("./test.db");
        if (result != APR_SUCCESS) {
            std::cout << "Error in init_storage" << std::endl;
            return 1;
        }
        return 0;
    }
    if (target == WRITE) {
        // TODO: open database
        char* path = "./test.db";
        aku_Config config;
        config.debug_mode = 0;
        config.page_size = 0;
        config.path_to_file = path;
        auto db = aku_open_database(config);
        // TODO: write some data
        for(unsigned long i = 0; i < 1000000; i++) {
            aku_MemRange memr;
            memr.address = (void*)&i;
            memr.length = sizeof(i);
            aku_add_sample(db, i % 1000, i >> 4, memr);
        }
        aku_close_database(db);
    }
    return 0;
}
