#include "top.h"

namespace Akumuli {
namespace QP {

TopN::TopN(size_t N, std::shared_ptr<Node> next)
    : next_(next)
    , N_(N)
{
}

TopN::TopN(const boost::property_tree::ptree& ptree, std::shared_ptr<Node> next)
    : next_(next)
{
    N_ = ptree.get<size_t>("N");
}

void TopN::complete() {
    std::vector<const Context*> ctx;
    for (const auto& p: table_) {
        ctx.push_back(&p.second);
    }
    std::sort(ctx.begin(), ctx.end(), [](const Context* lhs, const Context* rhs) {
        return lhs->sum > rhs->sum;
    });
    for (size_t i = 0; i < N_; i++) {
        if (i >= ctx.size()) {
            break;
        }
        aku_Sample sample;
        sample.paramid = ctx[i]->id;
        sample.timestamp = ctx[i]->last_ts;
        sample.payload.size    = sizeof(aku_Sample);
        sample.payload.float64 = ctx[i]->sum;
        sample.payload.type = AKU_PAYLOAD_FLOAT;
        MutableSample mut(&sample);
        if (!next_->put(mut)) {
            break;
        }
    }
    next_->complete();
}

bool TopN::put(MutableSample& sample) {
    static const double nanosinsec = 1000000000.0;
    // Require scalar
    if ((sample.payload_.sample.payload.type & AKU_PAYLOAD_FLOAT) != AKU_PAYLOAD_FLOAT) {
        // Query doesn't work with tuples
        set_error(AKU_EHIGH_CARDINALITY);
        return false;
    }
    aku_Timestamp ts = sample.get_timestamp();
    auto key = sample.get_paramid();
    auto it = table_.find(key);
    if (it == table_.end()) {
        bool inserted;
        std::tie(it, inserted) = table_.insert(std::make_pair(key, Context{}));
        assert(inserted);
        it->second.id = key;
    } else {
        Context& ctx    = it->second;
        double* pval    = sample[0];
        if (pval) {
            auto delta  = (ts - ctx.last_ts) / nanosinsec;
            auto value  = delta * ctx.last_xs;
            ctx.sum    += value;
            ctx.last_ts = ts;
            ctx.last_xs = *pval;
        }
    }
    return true;
}

void TopN::set_error(aku_Status status) {
    next_->set_error(status);
}

int TopN::get_requirements() const {
    return TERMINAL;
}


static QueryParserToken<TopN>  cusum_token("top");

}
}
