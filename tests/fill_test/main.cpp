#include <iostream>
#include <cassert>
#include <random>

#include <boost/timer.hpp>
#include <apr_mmap.h>
#include <apr_general.h>

#include "akumuli.h"
#include "page.h"
#include "storage.h"

using namespace Akumuli;
using namespace std;

const int DB_SIZE = 2;

enum Target {
    NOTHING,
    CREATE,
    WRITE,
    READ
};

int main(int cnt, const char** args)
{
    Target target = NOTHING;
    if (cnt == 2) {
        string param = args[1];
        if (param == "--create") {
            target = CREATE;
        } else
        if (param == "--write") {
            target = WRITE;
        } else
        if (param == "--read") {
            target = READ;
        }
    }
    if (target == NOTHING) {
        std::cout << "Nothing to do" << std::endl;
        return 0;
    }
    if (target == CREATE) {
        // TODO: create interface for storage manager in akumuli.h
        apr_status_t result = Storage::new_storage("test", "./", "./", 2);
        if (result != APR_SUCCESS) {
            std::cout << "Error in new_storage" << std::endl;
            return 1;
        }
        return 0;
    }
    if (target == READ) {
        char* path = "./test.db";
        aku_Config config;
        config.debug_mode = 0;
        config.page_size = 0;
        config.path_to_file = path;
        auto db = aku_open_database(config);
        // TODO:...
        aku_close_database(db);
    }
    if (target == WRITE) {
        char* path = "./test.db";
        aku_Config config;
        config.debug_mode = 0;
        config.page_size = 0;
        config.path_to_file = path;
        auto db = aku_open_database(config);
        boost::timer timer;
        for(int64_t i = 0; i < 100000000; i++) {
            apr_time_t now = i*2L;
            aku_MemRange memr;
            memr.address = (void*)&i;
            memr.length = sizeof(i);
            aku_add_sample(db, 1, now, memr);
            if (i % 1000000 == 0) {
                std::cout << i << " " << timer.elapsed() << "s" << std::endl;
                timer.restart();
            }
        }
        aku_close_database(db);
    }
    return 0;
}
