#include "ingestion_pipeline.h"
#include "logger.h"
#include <thread>

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
PipelineSpout::PipelineSpout(std::shared_ptr<Queue> q)
    : counter_{0}
    , created_(0)
    , deleted_(0)
    , pool_()
    , queue_(q)
{
    pool_.resize(POOL_SIZE);
}

void PipelineSpout::write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
    int ix = get_index_of_empty_slot();
    if (ix < 0) {
        // Try to delete old items from the pool
        gc();
        ix = get_index_of_empty_slot();
        if (ix < 0) {
            // Impossible to free some space
            // TODO: register data loss
            return;
        }
    }
    pool_.at(ix).reset(new TVal{param, ts, data, &counter_});
    auto pvalue = pool_.at(ix).get();
    queue_->push(pvalue);
}

void PipelineSpout::add_bulk_string(const Byte *buffer, size_t n) {
    // Shouldn't be implemented
}

int PipelineSpout::get_index_of_empty_slot() {
    if (created_ - deleted_ < POOL_SIZE) {
        // There is some space in the pool
        return created_++;
    }
    return POOL_SIZE + deleted_ - created_;
}

void PipelineSpout::gc() {
    uint64_t processed = counter_;
    while(deleted_ < processed) {
        pool_.at(deleted_ % POOL_SIZE).reset();
        deleted_++;
    }
}

// Ingestion pipeline

IngestionPipeline::IngestionPipeline(std::shared_ptr<DbConnection> con)
    : con_(con)
{
}

void IngestionPipeline::run() {
    auto qref = queue_;
    auto con = con_;
    auto worker = [con, qref]() {
        // Write loop (should be unique)
        while(true) {
            PipelineSpout::TVal *val;
            if (qref->pop(val)) {
                // New write
                if (val->cnt == nullptr) {  //poisoned
                    break;
                }
                con->write_double(val->id, val->ts, val->value);
                val->cnt++;
            }
        }
    };

    std::thread th(worker);
    th.detach();
}

std::shared_ptr<PipelineSpout> IngestionPipeline::make_spout() {
    return std::make_shared<PipelineSpout>(queue_);
}

}
