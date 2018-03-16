#pragma once

#include <memory>

#include "../queryprocessor_framework.h"

namespace Akumuli {
namespace QP {

struct TopN : Node {

    struct Context {
        double last_xs;
        aku_Timestamp last_ts;
        double sum;
        aku_ParamId id;
    };

    std::unordered_map< aku_ParamId
                      , Context
                      > table_;

    std::shared_ptr<Node> next_;

    size_t N_;

    TopN(size_t N, std::shared_ptr<Node> next);

    TopN(const boost::property_tree::ptree&, std::shared_ptr<Node> next);

    virtual void complete();

    virtual bool put(MutableSample& sample);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};

}
}
