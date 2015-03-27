#include "ingestion_pipeline.h"
#include "logger.h"
#include "utility.h"

#include <thread>

#include <boost/exception/all.hpp>

namespace Akumuli
{

static Logger db_logger_ = Logger("akumuli-storage", 32);

static void db_logger(int tag, const char *msg) {
    db_logger_.error() << "(" << tag << ") " << msg;
}

AkumuliConnection::AkumuliConnection(const char *path, bool hugetlb, Durability durability)
    : dbpath_(path)
{
    aku_FineTuneParams params = {
        // Debug mode
        0,
        // Pointer to logging function
        &db_logger,
        // huge tlbs
        (hugetlb ? 1u : 0u),
        // durability
        (uint32_t)durability
    };
    db_ = aku_open_database(dbpath_.c_str(), params);
}

aku_Status AkumuliConnection::write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
    return aku_write_double_raw(db_, param, ts, data);
}

// Pipeline spout
PipelineSpout::PipelineSpout(std::shared_ptr<Queue> q, BackoffPolicy bp)
    : created_{0}
    , deleted_{0}
    , pool_()
    , queue_(q)
    , backoff_(bp)
    , logger_("pipeline-spout", 32)
{
    pool_.resize(POOL_SIZE);
    for(int ix = POOL_SIZE; ix --> 0;) {
        pool_.at(ix).reset(new TVal());
    }
}

PipelineSpout::~PipelineSpout() {
}

void PipelineSpout::set_error_cb(PipelineErrorCb cb) {
    on_error_ = cb;
}

void PipelineSpout::write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
    int ix = get_index_of_empty_slot();
    while (AKU_UNLIKELY(ix < 0)) {
        ix = get_index_of_empty_slot();
        if (ix < 0 && backoff_ == AKU_LINEAR_BACKOFF) {
            std::this_thread::yield();
            continue;
        } else if ( ix < 0 && backoff_ == AKU_THROTTLE) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            return;
        }
    }

    auto pvalue = pool_.at(ix).get();

    pvalue->id       =      param;
    pvalue->ts       =         ts;
    pvalue->value    =       data;
    pvalue->cnt      =  &deleted_;
    pvalue->on_error = &on_error_;

    while (!queue_->push(pvalue)) {
        std::this_thread::yield();
    }
}

void PipelineSpout::add_bulk_string(const Byte *buffer, size_t n) {
    // Shouldn't be implemented
}

int PipelineSpout::get_index_of_empty_slot() {
    if (created_ - deleted_ < POOL_SIZE) {
        // There is some space in the pool
        auto result = created_  % POOL_SIZE;
        created_++;
        return result;
    }
    return -1;
}

// Ingestion pipeline

IngestionPipeline::IngestionPipeline(std::shared_ptr<DbConnection> con, BackoffPolicy bp)
    : con_(con)
    , ixmake_{0}
    , stopbar_(2)
    , startbar_(2)
    , backoff_(bp)
    , logger_("ingestion-pipeline", 32)

{
    for (int i = N_QUEUES; i --> 0;) {
        queues_.push_back(std::make_shared<PipelineSpout::Queue>(PipelineSpout::QCAP));
    }
}

void IngestionPipeline::start() {
    auto self = shared_from_this();
    auto worker = [self]() {
        try {
            self->logger_.info() << "Starting pipeline worker";
            self->startbar_.wait();
            self->logger_.info() << "Pipeline worker started";

            // Write loop (should be unique)
            PipelineSpout::TVal *val;
            int poison_cnt = 0;
            std::vector<PipelineSpout::PQueue> queues = self->queues_;
            const int IDLE_THRESHOLD = 0x10000;
            int idle_count = 0;
            for (int ix = 0; true; ix++) {
                auto& qref = queues.at(ix % N_QUEUES);
                if (qref->pop(val)) {
                    idle_count = 0;
                    // New write
                    if (AKU_UNLIKELY(val->cnt == nullptr)) {  //poisoned
                        poison_cnt++;
                        if (poison_cnt == N_QUEUES) {
                            // Check
                            for (auto& x: self->queues_) {
                                if (!x->empty()) {
                                    self->logger_.error() << "Queue not empty, some data will be lost.";
                                }
                            }
                            // Stop
                            self->logger_.info() << "Stopping pipeline worker";
                            self->stopbar_.wait();
                            self->logger_.info() << "Pipeline worker stopped";
                            return;
                        }
                    } else {
                        auto error = self->con_->write_double(val->id, val->ts, val->value);
                        (*val->cnt)++;
                        if (AKU_UNLIKELY(error != AKU_SUCCESS)) {
                            (*val->on_error)(error, *val->cnt);
                        }
                    }
                } else {
                    idle_count++;
                    if (idle_count > IDLE_THRESHOLD) {
                        if (idle_count % N_QUEUES == 0) {
                            // in idle state
                            // check all queues and go idle again
                            std::this_thread::sleep_for(std::chrono::milliseconds(1));
                        }
                    }
                }
            }
        } catch (...) {
            // Fatal error. Report. Die!
            self->logger_.error() << "Fatal error in ingestion pipeline worker thread!";
            self->logger_.error() << boost::current_exception_diagnostic_information();
            throw;
        }
    };

    std::thread th(worker);
    th.detach();

    logger_.info() << "Starting pipeline";
    startbar_.wait();
    logger_.info() << "Pipeline started";
}

std::shared_ptr<PipelineSpout> IngestionPipeline::make_spout() {
    ixmake_++;
    return std::make_shared<PipelineSpout>(queues_.at(ixmake_ % N_QUEUES), backoff_);
}

PipelineSpout::TVal* IngestionPipeline::POISON = new PipelineSpout::TVal{0, 0, 0, nullptr};
int IngestionPipeline::TIMEOUT = 15000;  // 15 seconds

void IngestionPipeline::stop() {
    logger_.info() << "Trying to stop pipeline, pushing poison to nodes";
    for (auto& q: queues_) {
        while(!q->push(POISON)) {
            std::this_thread::yield();
        }
    }
    logger_.info() << "Trying to stop pipeline, waiting for worker to stop";
    stopbar_.wait();
    logger_.info() << "Pipeline stopped (IngestionPipeline::stop)";
}

}
