#include "ingestion_pipeline.h"
#include "logger.h"
#include "utility.h"

#include <thread>

#include <boost/exception/all.hpp>

namespace Akumuli
{

static Logger logger_ = Logger("IP", 32);

static void db_logger(int tag, const char *msg) {
    logger_.error() << "(" << tag << ") " << msg;
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

void AkumuliConnection::write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
    aku_write_double(db_, param, ts, data);
}

// Pipeline spout
PipelineSpout::PipelineSpout(std::shared_ptr<Queue> q, BackoffPolicy bp)
    : counter_{0}
    , created_(0)
    , deleted_(0)
    , pool_()
    , queue_(q)
    , backoff_(bp)
{
    pool_.resize(POOL_SIZE);
}

void PipelineSpout::write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
    int ix = get_index_of_empty_slot();
    while (AKU_UNLIKELY(ix < 0)) {
        // Try to delete old items from the pool
        gc();
        ix = get_index_of_empty_slot();
        if ( ix < 0 && AKU_LIKELY(backoff_ == AKU_THROTTLE)     ) { // this setting will be used in production
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            return;
        } else if (ix < 0 && backoff_      == AKU_LINEAR_BACKOFF) {
            // Linear backoff if needed and stop when hitting backoff threshold
            std::this_thread::yield();
            continue;
        }
    }
    pool_.at(ix).reset(new TVal{param, ts, data, &counter_});
    auto pvalue = pool_.at(ix).get();
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

void PipelineSpout::gc() {
    uint64_t processed = counter_;
    while(deleted_ < processed) {
        pool_.at(deleted_ % POOL_SIZE).reset();
        deleted_++;
    }
}

// Ingestion pipeline

IngestionPipeline::IngestionPipeline(std::shared_ptr<DbConnection> con, BackoffPolicy bp)
    : con_(con)
    , ixmake_{0}
    , mutex_(std::make_shared<Mtx>())
    , stopped_(false)
    , backoff_(bp)

{
    for (int i = N_QUEUES; i --> 0;) {
        queues_.push_back(std::make_shared<PipelineSpout::Queue>(PipelineSpout::QCAP));
    }
}

void IngestionPipeline::start() {
    auto self = shared_from_this();
    auto worker = [self]() {
        try {
            // Write loop (should be unique)
            PipelineSpout::TVal *val;
            int poison_cnt = 0;
            for (int ix = 0; true; ix++) {
                auto& qref = self->queues_.at(ix % N_QUEUES);
                for (int i = 0; i < 16; i++) {
                    if (qref->pop(val)) {
                        // New write
                        if (val->cnt == nullptr) {  //poisoned
                            poison_cnt++;
                            if (poison_cnt == N_QUEUES) {
                                // Check
                                for (auto& x: self->queues_) {
                                    if (!x->empty()) {
                                        logger_.error() << "Queue not empty, some data will be lost.";
                                    }
                                }
                                // Stop
                                {
                                    std::lock_guard<Mtx> m(*self->mutex_);
                                    self->stopped_ = true;
                                }
                                self->cvar_.notify_one();
                                return;
                            }
                        } else {
                            self->con_->write_double(val->id, val->ts, val->value);
                            (*val->cnt)++;
                        }
                    }
                }
            }
        } catch (...) {
            // Fatal error. Report. Die!
            logger_.error() << "Fatal error in ingestion pipeline worker thread!";
            logger_.error() << boost::current_exception_diagnostic_information();
            throw;
        }
    };

    std::thread th(worker);
    th.detach();
}

std::shared_ptr<PipelineSpout> IngestionPipeline::make_spout() {
    ixmake_++;
    return std::make_shared<PipelineSpout>(queues_.at(ixmake_ % N_QUEUES), backoff_);
}

PipelineSpout::TVal* IngestionPipeline::POISON = new PipelineSpout::TVal{0, 0, 0, nullptr};
int IngestionPipeline::TIMEOUT = 15000;  // 15 seconds

void IngestionPipeline::stop() {
    for (auto& q: queues_) {
        q->push(POISON);
    }
    std::unique_lock<Mtx> lock(*mutex_);
    if (cvar_.wait_for(lock, std::chrono::milliseconds(TIMEOUT)) == std::cv_status::no_timeout) {
        if (!stopped_) {
            logger_.error() << "Can't stop pipeline";
        }
    } else {
        logger_.error() << "Pipeline stop timeout";
    }
}

}
