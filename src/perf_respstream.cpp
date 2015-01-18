#include "resp.h"
#include "graphite_client.h"
#include <boost/timer.hpp>
#include <iostream>
#include <algorithm>

const int TEST_ITERATIONS = 1000000;
const int N_TESTS = 100;

using namespace Akumuli;

bool push_to_graphite = false;

int main(int argc, char *argv[]) {
    if (argc == 2) {
        push_to_graphite = std::string(argv[1]) == "graphite";
    }
    const char* pattern = ":1234567\r\n+3.14159\r\n";
    std::string input;
    for (int i = 0; i < TEST_ITERATIONS/2; i++) {
        input += pattern;
    }
    std::vector<double> timedeltas;
    uint64_t intvalue;
    Byte buffer[RESPStream::STRING_LENGTH_MAX];
    for (int i = N_TESTS; i --> 0;) {
        boost::timer tm;
        MemStreamReader stream(input.data(), input.size());
        RESPStream protocol(&stream);
        for (int j = TEST_ITERATIONS; j --> 0;) {
            auto type = protocol.next_type();
            switch(type) {
            case RESPStream::INTEGER:
                intvalue = protocol.read_int();
                if (intvalue != 1234567) {
                    std::cerr << "Bad int value at " << j << std::endl;
                    return -1;
                }
                break;
            case RESPStream::STRING: {
                    int len = protocol.read_string(buffer, sizeof(buffer));
                    if (len != 7) {
                        std::cerr << "Bad string value at " << j << std::endl;
                        return -1;
                    }
                    char *p = buffer;
                    double res = strtod(buffer, &p);
                    if (abs(res - 3.14159) > 0.0001) {
                        std::cerr << "Can't parse float at " << j << std::endl;
                        return -1;
                    }
                }
                break;
            case RESPStream::ARRAY:
            case RESPStream::BAD:
            case RESPStream::BULK_STR:
            case RESPStream::ERROR:
            default:
                std::cerr << "Error at " << j << std::endl;
                return -1;
            };
        }
        timedeltas.push_back(tm.elapsed());
    }
    double min = std::numeric_limits<double>::max();
    for (auto t: timedeltas) {
        min = std::min(min, t);
    }
    if (push_to_graphite) {
        push_metric_to_graphite("respstream", 1000.0*min);
    }
    std::cout << "Parsing " << TEST_ITERATIONS << " messages in " << min << " sec." << std::endl;
    return 0;
}
