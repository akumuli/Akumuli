#include "compression.h"
#include "perftest_tools.h"

#include <iostream>
#include <cstdlib>

using namespace Akumuli;

int main() {

    std::cout << "Testing timestamp sequence" << std::endl;
    std::vector<uint64_t> tslist;
    int c = 0;
    for(int i = 0; i < 1000; i++) {
        int k = rand() % 2;
        if (k) {
            c++;
        } else {
            c--;
        }
        tslist.push_back(i + c);
    }

    ByteVector tsout;

    DeltaRLETSWriter tswriter(tsout);
    for(auto val: tslist) {
        tswriter.put(val);
    }
    tswriter.close();

    std::cout << "Original size: " << (tslist.size()*8) << std::endl;
    std::cout << "Compressed size: " << tsout.size() << std::endl;

    std::cout << "Testing doubles" << std::endl;
    std::vector<double> vallist;
    std::vector<aku_ParamId> pidlist;
    double xval = 0;
    for(int i = 0; i < 1000; i++) {
        xval += 1.1;
        vallist.push_back(xval);
        pidlist.push_back(0);
    }
    ByteVector valout;
    CompressionUtil::compress_doubles(vallist, pidlist, &valout);

    std::cout << "Original size: " << (vallist.size()*8) << std::endl;
    std::cout << "Compressed size: " << valout.size() << std::endl;
}
