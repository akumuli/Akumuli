/**
 * This file includes different performance tests of the Akumuli's
 * ingestion pipeline component:
 *
 * - PipelineSpout speed compared to the baseline.
 *
 *
 * Copyright (c) 2014 Eugene Lazin <4lazin@gmail.com>
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

#include "ingestion_pipeline.h"
#include "utility.h"
#include "perftest_tools.h"

#include <boost/lockfree/queue.hpp>

#include <thread>
#include <iostream>

using namespace Akumuli;

namespace detail {
    bool err_shown = false;
    const static int TAG = 111222333;
    struct ConnectionMock : Akumuli::DbConnection {
        int cnt;
        aku_Status write(const aku_Sample &sample) {
            if (AKU_LIKELY(sample.paramid == TAG)) {
                cnt++;
            } else {
                if (!err_shown) {
                    err_shown = true;
                    std::cout << "Error in ConnectionMock, unexpected value" << std::endl;
                }
                return AKU_EBAD_ARG;
            }
            return AKU_SUCCESS;
        }
        std::shared_ptr<DbCursor> search(std::string query) {
            throw "not implemented";
        }
        int param_id_to_series(aku_ParamId id, char *buffer, size_t buffer_size) {
            throw "not implemented";
        }
        aku_Status series_to_param_id(const char *name, size_t size, aku_Sample *sample) {
            throw "not implemented";
        }
    };
};

struct SpoutTest {
    // Baseline - transfer N messages between threads without allocation

    enum {
        N_ITERS = 10000000,
    };

    static double run_baseline() {

        boost::lockfree::queue<int, boost::lockfree::capacity<0x1000>, boost::lockfree::fixed_sized<true>> queue;

        // load generator
        auto worker = [&]() {
            for (int i = N_ITERS/2; i > 0;) {
                if (queue.push(1)) {
                    i--;
                } else {
                    std::this_thread::yield();
                }
            }
        };

        std::thread workerA(worker);
        std::thread workerB(worker);

        PerfTimer timer;
        int cnt = 0;
        while(true) {
            int val;
            if (queue.pop(val)) {
                cnt++;
                if (cnt == N_ITERS) {
                    break;
                }
            }
        }
        double e = timer.elapsed();

        workerA.join();
        workerB.join();
        return e;
    }

    static double run_pipeline() {
        using namespace detail;
        std::shared_ptr<ConnectionMock> con = std::make_shared<ConnectionMock>();
        con->cnt = 0;
        auto pipeline = std::make_shared<IngestionPipeline>(con, AKU_LINEAR_BACKOFF);
        auto worker = [&]() {
            auto spout = pipeline->make_spout();
            for (int i = N_ITERS/2; i --> 0;) {
                spout->write({(aku_Timestamp)i, (aku_ParamId)detail::TAG});
            }
        };
        PerfTimer tm;
        pipeline->start();
        std::thread workerA(worker);
        std::thread workerB(worker);
        workerA.join();
        workerB.join();
        pipeline->stop();
        double e = tm.elapsed();
        if (con->cnt != N_ITERS) {
            std::cout << "Error in pipeline " << con->cnt << std::endl;
        }
        return e;
    }
};


int main(int argc, char* argv[]) {
    std::cout << "Spout test" << std::endl;
    double b = SpoutTest::run_baseline();
    std::cout << "- baseline " << b << "s" << std::endl;
    double e = SpoutTest::run_pipeline();
    std::cout << "- pipeline " << e << "s" << std::endl;
    double rel = b/e;
    std::cout << "relative speedup " << rel << std::endl;
    bool push_to_graphite = false;
    if (argc == 2) {
        push_to_graphite = std::string(argv[1]) == "graphite";
    }
    if (push_to_graphite) {
        push_metric_to_graphite("pipeline", rel);
    }
}
