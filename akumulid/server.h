#pragma once
#include "akumuli.h"
#include "ingestion_pipeline.h"
#include "signal_handler.h"

#include <map>
#include <string>
#include <tuple>
#include <tuple>

namespace Akumuli {

struct ProtocolSettings {
    std::string name;
    int         port;
};

struct ServerSettings {
    std::string                   name;
    std::vector<ProtocolSettings> protocols;
    int                           nworkers;
};


/** Interface to query data.
  */
struct ReadOperation {
    virtual ~ReadOperation() = default;

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

/**
 * API endpoint from which the query originated from.
 */
enum class ApiEndpoint {
    QUERY,
    SUGGEST,
    SEARCH,
    UNKNOWN,
};

//! Interface that can be used to create read operations
struct ReadOperationBuilder {
    virtual ~ReadOperationBuilder()                        = default;
    virtual ReadOperation* create(ApiEndpoint ep)          = 0;
    virtual std::string    get_all_stats()                 = 0;
    virtual std::string    get_resource(std::string name)  = 0;
};

//! Server interface
struct Server {
    virtual ~Server()                                      = default;
    virtual void start(SignalHandler* sig_handler, int id) = 0;
};

struct ServerFactory {

    typedef std::function<std::shared_ptr<Server>(std::shared_ptr<AkumuliConnection>,
                                                  std::shared_ptr<ReadOperationBuilder>,
                                                  const ServerSettings&)>
        Generator;

    std::map<std::string, Generator> gen_;

    std::shared_ptr<Server> create(std::shared_ptr<AkumuliConnection>    connection,
                                   std::shared_ptr<ReadOperationBuilder> qproc,
                                   const ServerSettings&                 settings) {
        auto it = gen_.find(settings.name);
        if (it == gen_.end()) {
            return std::shared_ptr<Server>();
        }
        return it->second(connection, qproc, settings);
    }

    void register_type(std::string name, Generator gen) { gen_[name] = gen; }

    static ServerFactory& instance() {
        static ServerFactory factory;
        return factory;
    }
};

}  // namespace
