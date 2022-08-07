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
#include <memory>
#include <string>
#include <tuple>

#include <microhttpd.h>
#include <boost/asio.hpp>

#include "akumuli.h"
#include "logger.h"
#include "server.h"

// https://github.com/macports/macports-ports/pull/8941/files
// Beginning with v0.9.71, libmicrohttpd changed the return type
// of most functions from int to enum MHD_Result
// https://git.gnunet.org/gnunet.git/tree/src/include/gnunet_mhd_compat.h
// proposes to define a constant for the return type so it works well
// with all versions of libmicrohttpd
#if MHD_VERSION >= 0x00097002
#define MHD_RESULT enum MHD_Result
#else
#define MHD_RESULT int
#endif

namespace Akumuli {
namespace Http {

struct AccessControlList {};  // TODO: implement ACL

struct HttpServer : std::enable_shared_from_this<HttpServer>, Server {
    AccessControlList                     acl_;
    std::shared_ptr<ReadOperationBuilder> proc_;
    boost::asio::ip::tcp::endpoint        endpoint_;
    MHD_Daemon*                           daemon_;

    HttpServer(const boost::asio::ip::tcp::endpoint &endpoint, std::shared_ptr<ReadOperationBuilder> qproc);
    HttpServer(const boost::asio::ip::tcp::endpoint &endpoint, std::shared_ptr<ReadOperationBuilder> qproc,
               AccessControlList const& acl);

    virtual void start(SignalHandler* handler, int id);
    void stop();
};
}
}
