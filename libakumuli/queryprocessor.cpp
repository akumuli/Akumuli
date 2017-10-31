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
 */

#include "queryprocessor.h"
#include "util.h"

#include <boost/exception/diagnostic_information.hpp>


namespace Akumuli {
namespace QP {

//  ScanQueryProcessor  //

ScanQueryProcessor::ScanQueryProcessor(std::vector<std::shared_ptr<Node>> nodes, bool group_by_time)
{
    if (nodes.empty()) {
        AKU_PANIC("`nodes` shouldn't be empty")
    }

    root_node_ = nodes.front();
    last_node_ = nodes.back();

    // validate query processor data
    if (group_by_time) {
        for (auto ptr: nodes) {
            if ((ptr->get_requirements() & Node::GROUP_BY_REQUIRED) != 0) {
                NodeException err("`group_by` required");  // TODO: more detailed error message
                BOOST_THROW_EXCEPTION(err);
            }
        }
    }

    int nnormal = 0;
    for (auto it = nodes.rbegin(); it != nodes.rend(); it++) {
        if (((*it)->get_requirements() & Node::TERMINAL) != 0) {
            if (nnormal != 0) {
                NodeException err("invalid sampling order");  // TODO: more detailed error message
                BOOST_THROW_EXCEPTION(err);
            }
        } else {
            nnormal++;
        }
    }
}

bool ScanQueryProcessor::start() {
    return true;
}

bool ScanQueryProcessor::put(const aku_Sample &sample) {
    MutableSample mut(&sample);
    return root_node_->put(mut);
}

void ScanQueryProcessor::stop() {
    root_node_->complete();
}

void ScanQueryProcessor::set_error(aku_Status error) {
    root_node_->set_error(error);
}


MetadataQueryProcessor::MetadataQueryProcessor(std::shared_ptr<Node> node, std::vector<aku_ParamId> &&ids)
    : root_(node)
    , ids_(std::move(ids))
{
}

bool MetadataQueryProcessor::start() {
    for (auto id: ids_) {
        aku_Sample s;
        s.paramid = id;
        s.timestamp = 0;
        s.payload.type = aku_PData::PARAMID_BIT;
        s.payload.size = sizeof(aku_Sample);
        MutableSample mut(&s);
        if (!root_->put(mut)) {
            root_->complete();
            return false;
        }
    }
    return true;
}

bool MetadataQueryProcessor::put(const aku_Sample &sample) {
    // no-op
    return false;
}

void MetadataQueryProcessor::stop() {
    root_->complete();
}

void MetadataQueryProcessor::set_error(aku_Status error) {
    root_->set_error(error);
}


}} // namespace
