/**
 * Copyright (c) 2015 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "saxencoder.h"
#include <map>
#include <cmath>
#include <vector>

namespace Akumuli {
namespace SAX {

#define AKU_ZNORM_THRESHOLD 1e-10

template<class It>
std::tuple<double, double> mean_and_stddev(It begin, It end) {
    size_t size = std::distance(begin, end);
    if (size == 0) {
        return std::make_tuple(NAN, NAN);
    }
    double sqrsum = 0;
    double sum = 0;
    int count = 0;
    for (auto it = begin; it < end; it++) {
        auto val = *it;
        sqrsum += val * val;
        sum += val;
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


// source: https://github.com/jMotif/SAX/blob/master/src/main/java/net/seninp/jmotif/sax/alphabet/NormalAlphabet.java
static const std::map<int, std::vector<double>> CUTPOINTS = {
    {  2, {  0.0                 }},

    {  3, { -0.430727300000000,  0.430727300000000   }},

    {  4, { -0.674489800000000,  0.0,                0.674489800000000   }},

    {  5, { -0.841621233572914, -0.253347103135800,  0.253347103135800,  0.841621233572914   }},

    {  6, { -0.967421566101701, -0.430727299295457,  0.0,                0.430727299295457,
             0.967421566101701   }},

    {  7, { -1.067570523878140, -0.565948821932863, -0.180012369792705,  0.180012369792705,
             0.565948821932863,  1.067570523878140   }},

    {  8, { -1.150349380376010, -0.674489750196082, -0.318639363964375,  0.0,
             0.318639363964375,  0.674489750196082,  1.150349380376010   }},

    {  9, { -1.220640348847350, -0.764709673786387, -0.430727299295457, -0.139710298881862,
             0.139710298881862,  0.430727299295457,  0.764709673786387,  1.220640348847350   }},

    { 10, { -1.281551565544600, -0.841621233572914, -0.524400512708041, -0.253347103135800,
             0.0,                0.253347103135800,  0.524400512708041,  0.841621233572914,
             1.281551565544600   }},

    { 11, { -1.335177736118940, -0.908457868537385, -0.604585346583237, -0.348755695517045,
            -0.114185294321428,  0.114185294321428,  0.348755695517045,  0.604585346583237,
             0.908457868537385,  1.335177736118940   }},

    { 12, { -1.382994127100640, -0.967421566101701, -0.674489750196082, -0.430727299295457,
            -0.210428394247925,  0.0,                0.210428394247925,  0.430727299295457,
             0.674489750196082,  0.967421566101701,  1.382994127100640   }},

    { 13, { -1.426076872272850, -1.020076232786200, -0.736315917376129, -0.502402223373355,
            -0.293381232121193, -0.096558615289639,  0.096558615289639,  0.293381232121194,
             0.502402223373355,  0.736315917376130,  1.020076232786200,  1.426076872272850   }},

    { 14, { -1.465233792685520, -1.067570523878140, -0.791638607743375, -0.565948821932863,
            -0.366106356800570, -0.180012369792705,  0.0,                0.180012369792705,
             0.366106356800570,  0.565948821932863,  0.791638607743375,  1.067570523878140,
             1.465233792685520   }},

    { 15, { -1.501085946044020, -1.110771616636790, -0.841621233572914, -0.622925723210088,
            -0.430727299295457, -0.253347103135800, -0.083651733907129,  0.083651733907129,
             0.253347103135800,  0.430727299295457,  0.622925723210088,  0.841621233572914,
             1.110771616636790,  1.501085946044020   }},

    { 16, { -1.534120544352550, -1.150349380376010, -0.887146559018876, -0.674489750196082,
            -0.488776411114669, -0.318639363964375, -0.157310684610171,  0.0,
             0.157310684610171,  0.318639363964375,  0.488776411114669,  0.674489750196082,
             0.887146559018876,  1.150349380376010,  1.534120544352550   }},

    { 17, { -1.564726471361800, -1.186831432755820, -0.928899491647271, -0.721522283982343,
            -0.541395085129088, -0.377391943828554, -0.223007830940367, -0.073791273808273,
             0.073791273808273,  0.223007830940367,  0.377391943828554,  0.541395085129088,
             0.721522283982343,  0.928899491647271,  1.186831432755820,  1.564726471361800   }},

    { 18, { -1.593218818023050, -1.220640348847350, -0.967421566101701, -0.764709673786387,
            -0.589455797849779, -0.430727299295457, -0.282216147062508, -0.139710298881862,
             0.0,                0.139710298881862,  0.282216147062508,  0.430727299295457,
             0.589455797849779,  0.764709673786387,  0.967421566101701,  1.220640348847350,
             1.593218818023050   }},

    { 19, { -1.619856258638270, -1.252119520265220, -1.003147967662530, -0.804596380360300,
            -0.633640000779701, -0.479505653330950, -0.336038140371823, -0.199201324789267,
            -0.066011812375841,  0.066011812375841,  0.199201324789267,  0.336038140371823,
             0.479505653330950,  0.633640000779701,  0.804596380360300,  1.003147967662530,
             1.252119520265220,  1.619856258638270   }},

    { 20, { -1.644853626951470, -1.281551565544600, -1.036433389493790, -0.841621233572914,
            -0.674489750196082, -0.524400512708041, -0.385320466407568, -0.253347103135800,
            -0.125661346855074,  0.0,                0.125661346855074,  0.253347103135800,
             0.385320466407568,  0.524400512708041,  0.674489750196082,  0.841621233572914,
             1.036433389493790,  1.281551565544600,  1.644853626951470   }},
};

static const char ALPHABET[] = {
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u' // 21 symbols
};

static char to_char(double value, const std::vector<double>& cuts) {
    for (auto i = 0u; i < cuts.size(); i++) {
        if (value < cuts[i]) {
            return ALPHABET[i];
        }
    }
    return ALPHABET[cuts.size()];
}

//! Convert array of doubles to characters (both array modified in-place)
static void saxify(const boost::circular_buffer<double>& input, std::string& output, double threshold, int alphabet_size) {
    auto it = CUTPOINTS.find(alphabet_size);
    if (it == CUTPOINTS.end()) {
        std::runtime_error error("invalid alphabet size");
        BOOST_THROW_EXCEPTION(error);
    }
    const std::vector<double>& cuts = it->second;

    double mean, stddev;
    std::tie(mean, stddev) = mean_and_stddev(input.begin(), input.end());
    auto size = input.size();
    output.resize(size);
    if (stddev < threshold) {
        for (size_t i = 0; i < size; i++) {
            double val = input[i] - mean;
            output[i] = to_char(val, cuts);
        }
    } else {
        for (size_t i = 0; i < size; i++) {
            double val = (input[i] - mean) / stddev;
            output[i] = to_char(val, cuts);
        }
    }
}

int leading_zeroes(int value) {
    return value == 0 ? sizeof(value)*8 : __builtin_clz(value);
}


SAXEncoder::SAXEncoder()
    : alphabet_(0)
    , window_width_(0)
    , input_samples_(1)
{
}

SAXEncoder::SAXEncoder(int alphabet, int window_width)
    : alphabet_(alphabet)
    , window_width_(window_width)
    , input_samples_(window_width)
{
}

bool SAXEncoder::encode(double sample, char *outword, size_t outword_size) {
    input_samples_.push_back(sample);
    if (input_samples_.full()) {
        // scan samples
        saxify(input_samples_, buffer_, AKU_ZNORM_THRESHOLD, alphabet_);
        if (buffer_ != last_) {
            // Simple numerocity reduction
            last_ = buffer_;
            memcpy(outword, buffer_.data(), buffer_.size());
            buffer_.clear();
            return true;
        }
    }
    return false;
}

}}  // namespace
