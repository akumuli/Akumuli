#pragma once
#include "akumuli.h"

namespace Akumuli {
namespace QP {

struct Node {

    enum NodeType {
        // Samplers
        RandomSampler,
        Resampler,
        // Filtering
        FilterById,
        // Group by
        GroupBy,
        // Testing
        Mock,
        // Cursor node
        Cursor,
    };

    virtual ~Node() = default;

    //! Complete adding values
    virtual void complete() = 0;

    //! Process value, return false to interrupt process
    virtual bool put(aku_Sample const& sample) = 0;

    virtual void set_error(aku_Status status) = 0;

    // Introspections

    //! Get node type
    virtual NodeType get_type() const = 0;
};

//! Query processor interface
struct IQueryProcessor {

    //! Lowerbound
    virtual aku_Timestamp lowerbound() const = 0;

    //! Upperbound
    virtual aku_Timestamp upperbound() const = 0;

    //! Scan direction (AKU_CURSOR_DIR_BACKWARD or AKU_CURSOR_DIR_FORWARD)
    virtual int direction() const = 0;

    //! Should be called before processing begins
    virtual void start() = 0;

    //! Process value
    virtual bool put(const aku_Sample& sample) = 0;

    //! Should be called when processing completed
    virtual void stop() = 0;

    //! Set execution error
    virtual void set_error(aku_Status error) = 0;
};


}}  // namespaces
