#include "paa.h"
#include "../util.h"
#include <algorithm>

namespace Akumuli {
namespace QP {

void MeanCounter::reset() {
    acc = 0;
    num = 0;
}

double MeanCounter::value() const {
    return acc/num;
}

bool MeanCounter::ready() const {
    return num != 0;
}

void MeanCounter::add(aku_Sample const& value) {
    acc += value.payload.float64;
    num++;
}

MeanPAA::MeanPAA(std::shared_ptr<Node> next)
    : PAA<MeanCounter>(next)
{
}

MeanPAA::MeanPAA(const boost::property_tree::ptree &, std::shared_ptr<Node> next)
    : PAA<MeanCounter>(next)
{
}

void MedianCounter::reset() {
    std::vector<double> tmp;
    std::swap(tmp, acc);
}

double MedianCounter::value() const {
    if (acc.empty()) {
        AKU_PANIC("`ready` should be called first");
    }
    if (acc.size() < 2) {
        return acc.at(0);
    } else if (acc.size() == 2) {
        return (acc[0] + acc[1])/2;
    }
    auto middle = acc.begin();
    std::advance(middle, acc.size() / 2);
    std::partial_sort(acc.begin(), middle + 1, acc.end());
    return *middle;
}

bool MedianCounter::ready() const {
    return !acc.empty();
}

void MedianCounter::add(aku_Sample const& value) {
    acc.push_back(value.payload.float64);
}

MedianPAA::MedianPAA(std::shared_ptr<Node> next)
    : PAA<MedianCounter>(next)
{
}

MedianPAA::MedianPAA(const boost::property_tree::ptree &, std::shared_ptr<Node> next)
    : PAA<MedianCounter>(next)
{
}

struct SelectMin {
   double operator () (double lhs, double rhs) {
       if (lhs < rhs) {
           return lhs;
       }
       return rhs;
   }
};

struct SelectMax {
   double operator () (double lhs, double rhs) {
       if (lhs > rhs) {
           return lhs;
       }
       return rhs;
   }
};

struct SelectFirst {
   double operator () (double lhs, double) {
       return lhs;
   }
};

struct SelectLast {
   double operator () (double, double rhs) {
       return rhs;
   }
};

typedef GenericPAA<SelectMax> MaxPAA;
typedef GenericPAA<SelectMin> MinPAA;
typedef GenericPAA<SelectFirst> FirstPAA;
typedef GenericPAA<SelectLast> LastPAA;

//static QueryParserToken<MeanPAA> mean_paa_token("paa");
//static QueryParserToken<MedianPAA> median_paa_token("median-paa");
//static QueryParserToken<MaxPAA> max_paa_token("max-paa");
//static QueryParserToken<MinPAA> min_paa_token("min-paa");
//static QueryParserToken<FirstPAA> first_paa_token("first-paa");
//static QueryParserToken<LastPAA> last_paa_token("last-paa");

}}  // namespace
