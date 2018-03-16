#include <fstream>
#include <iostream>
#include "akumuli.h"
#include "index/seriesparser.h"

using namespace Akumuli;

int main(int argc, char** argv) {
    if (argc == 1) {
        return 1;
    }
    std::string file_name(argv[1]);
    std::fstream input(file_name, std::ios::binary|std::ios::in|std::ios::out);

    for (std::string line; std::getline(input, line);) {
        const char* keystr_begin;
        const char* keystr_end;
        const size_t size = AKU_LIMITS_MAX_SNAME + 1;
        char out[size];
        SeriesParser::to_canonical_form(line.data(), line.data() + line.size(), out, out + size, &keystr_begin, &keystr_end);
    }
}
