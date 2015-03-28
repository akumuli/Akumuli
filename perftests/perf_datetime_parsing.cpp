#include "datetime.h"
#include "perftest_tools.h"

using namespace Akumuli;

int main() {

    const char* test_strings[] = {
        "20060102T100405.999999999",
        "20060202T110406.888888888",
        "20060302T120407.777777777",
        "20060402T130408.666666666",
        "20060502T140409.555555555",
        "20060602T150400.444444444",
        "20060702T160401.333333333",
        "20060802T170402.222222222",
        "20060902T180403.111111111",
        "20061002T190404.000000000"
    };
    aku_Timestamp tsacc = 0;
    PerfTimer timer;
    for(int k = 100000; k --> 0;) {
        for(int i = 10; i --> 0;) {
            tsacc += DateTimeUtil::from_iso_string(test_strings[i]);
        }
    }
    double elapsed = timer.elapsed();
    std::cout << "Summ: " << tsacc << std::endl;
    std::cout << "Elapsed: " << elapsed << std::endl;
    return 0;
}
