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

Node::NodeType MeanPAA::get_type() const {
    return Node::MovingAverage;
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
    }
    auto middle = acc.begin();
    std::advance(middle, acc.size() / 2);
    std::partial_sort(acc.begin(), middle, acc.end());
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

Node::NodeType MedianPAA::get_type() const {
    return Node::MovingMedian;
}

static QueryParserToken<MeanPAA> mean_paa_token("paa");
static QueryParserToken<MedianPAA> median_paa_token("median-paa");

}}  // namespace
