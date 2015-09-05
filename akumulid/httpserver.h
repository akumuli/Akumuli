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
#include <string>
#include <memory>
#include <tuple>

#include <microhttpd.h>

#include "logger.h"
#include "akumuli.h"

namespace Akumuli {
namespace Http {

struct AccessControlList {};  // TODO: implement ACL

// Fwd decl
struct QueryResultsPooler;

//! Query processor interface for HTTP server
struct QueryProcessor {
    virtual ~QueryProcessor() = default;
    virtual QueryResultsPooler* create() = 0;
};

struct QueryResultsPooler {
    virtual ~QueryResultsPooler() = default;

    /** Start query execution
      */
    virtual void start() = 0;

    /** Append query data to cursor
      */
    virtual void append(const char* data, size_t data_size) = 0;

    /** Return error code or AKU_SUCCESS.
      * This error code represent result of the query parsing and initial processing. It can indicate
      * error in the query. Result of the call to this function shouldn't change while reading data.
      * If error occurs during reading `read_some` method should throw an error.
      */
    virtual aku_Status get_error() = 0;

    /** Read some data from cursor. This method should be called only if `get_error` have returned
      * AKU_SUCCESS. If some error occured during read operation this method should throw.
      * Method returns tuple (num_elements, is_done). If there is no more results, method returns
      * (any, true) otherwise it returns (any, false). Number of elements can be 0, in this case
      * if second tuple element is false client should call this method again.
      */
    virtual std::tuple<size_t, bool> read_some(char* buf, size_t buf_size) = 0;

    /** Close cursor.
      * Should be called after read operation was completed or interrupted.
      */
    virtual void close() = 0;
};

struct HttpServer
{
    AccessControlList               acl_;
    std::shared_ptr<QueryProcessor> proc_;
    unsigned short                  port_;
    MHD_Daemon                     *daemon_;

    HttpServer(unsigned short port, std::shared_ptr<QueryProcessor> qproc);
    HttpServer(unsigned short port, std::shared_ptr<QueryProcessor> qproc, AccessControlList const& acl);
    void start();
    void stop();
};

}
}

