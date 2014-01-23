#include <iostream>
#include <cassert>
#include <random>
#include <tuple>
#include <map>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <memory>

#include <boost/unordered_map.hpp>
#include <cpp-btree/btree_map.h>

#include <boost/timer.hpp>
#include <boost/pool/pool.hpp>
#include <boost/pool/pool_alloc.hpp>
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
    READ,
    MEM
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
        } else
        if (param == "--mem") {
            target = MEM;
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
    if (target == MEM) {

        typedef std::tuple<TimeStamp, ParamId> KeyType;

        struct KeyHasher {
            size_t operator () (KeyType const& kt) const noexcept {
                return (size_t) std::get<0>(kt).value;
            }
        };

        typedef btree::btree_multimap<KeyType, EntryOffset
                , std::less<KeyType>
                , std::allocator<std::pair<const KeyType, EntryOffset> >
                >
            Generation;

        std::vector< std::unique_ptr<Generation> > gen7s;
        const int num_gen7s = 4;
        const int gen7_size_max = 10000;
        for(int i = 0; i < num_gen7s; i++) {
            gen7s.push_back(std::unique_ptr<Generation>(new Generation()));
        }
        boost::timer timer;
        for(int64_t i = 0; i < 100000000; i++) {
            TimeStamp ts;
            ts.value = i*2L;
            ParamId id = 1;

            gen7s[0]->insert(std::make_pair(std::make_tuple(ts, id), 1024));
            if (gen7s[0]->size() > gen7_size_max) {
                gen7s[0]->clear();
            }

            if (i % 1000000 == 0) {
                std::cout << i << " " << timer.elapsed() << "s" << std::endl;
                timer.restart();
            }
        }
    }
    if (target == READ) {
        char* path = "test";
        aku_Config config;
        config.debug_mode = 0;
        config.path_to_file = path;
        auto db = aku_open_database(config);
        // TODO:...
        aku_close_database(db);
    }
    if (target == WRITE) {
        char* path = "test.akumuli";
        aku_Config config;
        config.debug_mode = 0;
        config.path_to_file = path;
        //TODO: set proper ttl
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
