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

#include <boost/lockfree/queue.hpp>
#include <boost/timer.hpp>

#include <thread>
#include <iostream>

namespace detail {
    bool err_shown = false;
    const static int TAG = 111222333;
    struct ConnectionMock : Akumuli::DbConnection {
        int cnt;
        void write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
            if (AKU_LIKELY(param == TAG)) {
                cnt++;
            } else {
                if (!err_shown) {
                    err_shown = true;
                    std::cout << "Error in ConnectionMock, unexpected value" << std::endl;
                }
            }
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

        boost::timer timer;
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
        using namespace Akumuli;
        using namespace detail;
        std::shared_ptr<ConnectionMock> con = std::make_shared<ConnectionMock>();
        con->cnt = 0;
        auto pipeline = std::make_shared<IngestionPipeline>(con);
        auto worker = [&]() {
            auto spout = pipeline->make_spout();
            for (int i = N_ITERS/2; i --> 0;) {
                spout->write_double(detail::TAG, i, 0.0);
            }
        };
        boost::timer tm;
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


int main() {
    std::cout << "Spout test" << std::endl;
    double e = SpoutTest::run_baseline();
    std::cout << "- baseline " << e << "s" << std::endl;
    e = SpoutTest::run_pipeline();
    std::cout << "- pipeline " << e << "s" << std::endl;
}
