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

#pragma once
#include <chrono>
#include <memory>

#include "akumuli.h"
#include "stringpool.h"

namespace Akumuli {

struct Bolt {

    enum BoltType {
        // Samplers
        RandomSampler,
        Resampler,
        // Joins
        JoinByTimestamp,
        // Testing
        Mock,
    };

    virtual ~Bolt() = default;
    //! Complete adding values
    virtual void complete(std::shared_ptr<Bolt> caller) = 0;
    //! Process value
    virtual void put(aku_Timestamp ts, aku_ParamId id, double value) = 0;

    // Connections management
    //! Add output
    virtual void add_output(std::shared_ptr<Bolt> next) = 0;
    virtual void add_input(std::weak_ptr<Bolt> input) = 0;

    // Introspections

    //! Get bolt type
    virtual BoltType get_bolt_type() const = 0;

    //! Get all inputs
    virtual std::vector<std::shared_ptr<Bolt>> get_bolt_inputs() const = 0;

    //! Get all outputs
    virtual std::vector<std::shared_ptr<Bolt>> get_bolt_outputs() const = 0;
};

struct BoltException : std::runtime_error {
    Bolt::BoltType type_;
    BoltException(Bolt::BoltType type, const char* msg);

    Bolt::BoltType get_type() const;
};



struct BoltsBuilder {
    static std::shared_ptr<Bolt> make_random_sampler(std::string type,
                                                     size_t buffer_size,
                                                     aku_logger_cb_t logger);
};

/** Query processor.
  * Should be built from textual representation (json at first).
  * Should be used by both sequencer and page to match parameters
  * and group them together.
  */
struct QueryProcessor {

    typedef StringTools::StringT StringT;
    typedef StringTools::TableT TableT;

    //! Lowerbound
    const aku_Timestamp                lowerbound;
    //! Upperbound
    const aku_Timestamp                upperbound;
    //! Scan direction
    const int                          direction;
    //! List of metrics of interest
    const std::vector<std::string>     metrics;
    //! Name to id mapping
    TableT                             namesofinterest;

    //! Root of the processing topology
    std::shared_ptr<Bolt>              root_bolt;

    /** Create new query processor.
      * @param root is a root of the processing topology
      * @param metrics is a list of metrics of interest
      * @param begin is a timestamp to begin from
      * @param end is a timestamp to end with
      *        (depending on a scan direction can be greater or smaller then lo)
      */
    QueryProcessor(std::shared_ptr<Bolt> root,
                   std::vector<std::string> metrics,
                   aku_Timestamp begin,
                   aku_Timestamp end);

    /** Match param_id. Return group id on success or
      * negative value on error.
      */
    int match(uint64_t param_id);
};

}
