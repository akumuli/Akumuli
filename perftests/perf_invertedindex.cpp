#include "invertedindex.h"
#include <iostream>
#include <cstring>

using namespace Akumuli;

int main() {

    InvertedIndex index(128);

    for (int i = 0; i < 1000; i++) {
        char buffer[128];
        int n = snprintf(buffer, 128, "%d", i);
        index.append(i, buffer, buffer + n);
    }

    const char* query = "981";
    auto results = index.get_count(query, query + strlen(query));
    for (auto kv: results) {
        std::cout << "kv: " << kv.first << ", " << kv.second << std::endl;
    }

    return 0;
}
