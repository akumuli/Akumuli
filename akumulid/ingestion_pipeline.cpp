#include "ingestion_pipeline.h"
#include "logger.h"
#include "utility.h"

#include <thread>

#include <boost/exception/all.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

namespace Akumuli
{

static Logger db_logger_ = Logger("akumuli-storage", 32);

static void db_logger(aku_LogLevel tag, const char *msg) {
    db_logger_.error() << "(" << tag << ") " << msg;
}

//! Abstraction layer above aku_Cursor
struct AkumuliCursor : DbCursor {
    aku_Cursor* cursor_;

    AkumuliCursor(aku_Cursor* cur) : cursor_(cur) { }

    virtual size_t read(void *dest, size_t dest_size) {
        return aku_cursor_read(cursor_, dest, dest_size);
    }

    virtual int is_done() {
        return aku_cursor_is_done(cursor_);
    }

    virtual bool is_error(aku_Status *out_error_code_or_null) {
        return aku_cursor_is_error(cursor_, out_error_code_or_null);
    }

    virtual void close() {
        aku_cursor_close(cursor_);
    }
};

AkumuliConnection::AkumuliConnection(const char *path,
                                     bool hugetlb,
                                     Durability durability,
                                     u32 compression_threshold,
                                     u64 window_width,
                                     u64 cache_size)
    : dbpath_(path)
{
    aku_FineTuneParams params = {};
    params.debug_mode = 0;
    params.durability = (u32)durability;
    params.enable_huge_tlb = hugetlb ? 1u : 0u;
    params.logger = &db_logger;
    params.compression_threshold = compression_threshold;
    params.window_size = window_width;
    params.max_cache_size = cache_size;
    db_ = aku_open_database(dbpath_.c_str(), params);
}

void AkumuliConnection::close() {
    aku_close_database(db_);
}

aku_Status AkumuliConnection::write(aku_Sample const& sample) {
    // FIXME: api was changed
    //return aku_write(db_, &sample);
    throw "not implemented";
}

std::shared_ptr<DbCursor> AkumuliConnection::search(std::string query) {
    aku_Cursor* cursor = aku_query(db_, query.c_str());
    return std::make_shared<AkumuliCursor>(cursor);
}

int AkumuliConnection::param_id_to_series(aku_ParamId id, char* buffer, size_t buffer_size) {
    //return aku_param_id_to_series(db_, id, buffer, buffer_size);
    // FIXME: api was changed
    throw "not implemented";
}

aku_Status AkumuliConnection::series_to_param_id(const char *name, size_t size, aku_Sample *sample) {
    // FIXME: api changed
    //return aku_series_to_param_id(db_, name, name + size, sample);
    throw "not implemented";
}

std::string AkumuliConnection::get_all_stats() {
    //std::vector<char> buffer;
    //buffer.resize(0x1000);
    //int nbytes = aku_json_stats(db_, buffer.data(), buffer.size());
    //if (nbytes > 0) {
    //    return std::string(buffer.data(), buffer.data() + nbytes);
    //}
    // FIXME: aku_json_stats fn
    return "nope!";
}

// Pipeline spout
PipelineSpout::PipelineSpout(std::shared_ptr<Queue> q, BackoffPolicy bp, std::shared_ptr<DbConnection> con)
    : created_{0}
    , deleted_{0}
    , pool_()
    , queue_(q)
    , backoff_(bp)
    , logger_("pipeline-spout", 32)
    , db_(con)
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

void PipelineSpout::write(const aku_Sample& sample) {
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

    pvalue->sample   =     sample;
    pvalue->cnt      =  &deleted_;
    pvalue->on_error = &on_error_;

    while (!queue_->push(pvalue)) {
        std::this_thread::yield();
    }
}

aku_Status PipelineSpout::series_to_param_id(const char *str, size_t strlen, aku_Sample *sample) {
    return db_->series_to_param_id(str, strlen, sample);
}

void PipelineSpout::add_bulk_string(const Byte *buffer, size_t n) {
    // Shouldn't be implemented
    throw std::runtime_error("not implemented");
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

bool PipelineSpout::is_empty() const {
    return created_ == deleted_;
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
                            self->logger_.info() << "Closing akumuli database";
                            self->con_->close();
                            // Stop
                            self->logger_.info() << "Stopping pipeline worker";
                            self->stopbar_.wait();
                            self->logger_.info() << "Pipeline worker stopped";
                            return;
                        }
                    } else {
                        auto error = self->con_->write(val->sample);
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
    return std::make_shared<PipelineSpout>(queues_.at(ixmake_ % N_QUEUES), backoff_, con_);
}

PipelineSpout::TVal* IngestionPipeline::POISON = new PipelineSpout::TVal{{}, nullptr, nullptr};

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
