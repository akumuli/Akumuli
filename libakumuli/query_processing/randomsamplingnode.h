#pragma once

#include <memory>
#include <vector>

#include "../queryprocessor_fwd.h"
#include "../util.h"

namespace Akumuli {
namespace QP {

struct RandomSamplingNode : std::enable_shared_from_this<RandomSamplingNode>, Node {
    const uint32_t                      buffer_size_;
    std::vector<aku_Sample>             samples_;
    Rand                                random_;
    std::shared_ptr<Node>               next_;

    RandomSamplingNode(uint32_t buffer_size, std::shared_ptr<Node> next);

    virtual NodeType get_type() const;

    bool flush();

    virtual void complete();

    virtual bool put(const aku_Sample& sample);

    void set_error(aku_Status status);
};

}}  // namespace
