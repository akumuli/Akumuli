#include "saxencoder.h"

namespace Akumuli {
namespace SAX {

static std::tuple<double, double> mean_and_stddev(double* array, size_t size) {
    if (size == 0) {
        return std::make_tuple(NAN, NAN);
    }
    double sqrsum = 0;
    double sum = 0;
    int count = 0;
    for (size_t i = 0; i < size; i++) {
        sqrsum += array[i] * array[i];
        sum += array[i];
        count += 1;
    }
    double stddev;
    if (size > 1) {
        stddev = sqrt((size * sqrsum - sum * sum) / (size * (size - 1)));
    } else {
        stddev = NAN;
    }
    double mean = sum / size;
    return std::make_tuple(mean, stddev);
}

//! Z-norm series in-place
void znorm(double* array, size_t size, double threshold) {
    double mean, stddev;
    std::tie(mean, stddev) = mean_and_stddev(array, size);
    if (stddev < threshold) {
        for (size_t i = 0; i < size; i++) {
            array[i] -= mean;
        }
    } else {
        for (size_t i = 0; i < size; i++) {
            array[i] = (array[i] - mean) / stddev;
        }
    }
}

int leading_zeroes(int value) {
    return value == 0 ? sizeof(value)*8 : __builtin_clz(value);
}


SAXEncoder::SAXEncoder(int alphabet, int window_width)
    : alphabet_(alphabet)
    , window_width_(window_width)
    , input_samples_(window_width)
    , output_samples_(window_width)
{
}

void SAXEncoder::append(double sample) {
    input_samples_.push_back(sample);
    // scan samples
    //SAXWord word(input_samples_.begin(), input_samples_.end());
}

}}  // namespace
