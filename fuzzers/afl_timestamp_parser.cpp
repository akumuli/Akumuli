#include <fstream>
#include <iostream>
#include "datetime.h"

using namespace Akumuli;

int main(int argc, char** argv) {
    if (argc == 1) {
        return 1;
    }
    std::string file_name(argv[1]);
    std::fstream input(file_name, std::ios::binary|std::ios::in|std::ios::out);

    for (std::string line; std::getline(input, line);) {
        try {
            DateTimeUtil::from_iso_string(line.c_str());
        } catch(BadDateTimeFormat const&) {
            continue;
        }
    }
}
