#include "invertedindex.h"

using namespace Akumuli;

int main() {

    InvertedIndex index(128, 1024);

    for (int i = 0; i < 1000; i++) {
        char buffer[128];
        int n = snprintf(buffer, 128, "%d", i);
        index.append(i, buffer, buffer + n);
    }

    return 0;
}
