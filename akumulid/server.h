#pragma once
#include "signal_handler.h"
#include "ingestion_pipeline.h"
#include <map>
#include <string>

namespace Akumuli {

    struct ServerSettings {
        std::string name;
        int         port;
        int         nworkers;
    };

    //! Server interface
    struct Server {
        virtual ~Server() = default;
        virtual void start(SignalHandler* sig_handler, int id) = 0;
    };

    struct ServerFactory {
        typedef std::function<std::shared_ptr<Server>(std::shared_ptr<IngestionPipeline>, const ServerSettings&)> Generator;
        std::map<std::string, Generator> gen_;

        std::shared_ptr<Server> create(std::shared_ptr<IngestionPipeline> pipeline, const ServerSettings& settings) {
            auto it = gen_.find(settings.name);
            if (it == gen_.end()) {
                return std::shared_ptr<Server>();
            }
            return it->second(pipeline, settings);
        }

        void register_type(std::string name, Generator gen) {
            gen_[name] = gen;
        }

        static ServerFactory& instance() {
            static ServerFactory factory;
            return factory;
        }
    };
}
