#pragma once
#include "akumuli.h"

namespace Akumuli {
namespace QP {

struct Node {

    enum NodeType {
        // Samplers
        RandomSampler,
        MovingAverage,
        MovingMedian,
        AnomalyDetector,
        Resampler,
        // Tok-K elements
        SpaceSaver,
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

    /** Process value, return false to interrupt process.
      * Empty sample can be sent to flush all updates.
      */
    virtual bool put(aku_Sample const& sample) = 0;

    virtual void set_error(aku_Status status) = 0;

    // Introspections

    //! Get node type
    virtual NodeType get_type() const = 0;
};

//! Query processor interface
struct IQueryProcessor {

    // Query information

    //! Lowerbound
    virtual aku_Timestamp lowerbound() const = 0;

    //! Upperbound
    virtual aku_Timestamp upperbound() const = 0;

    //! Scan direction (AKU_CURSOR_DIR_BACKWARD or AKU_CURSOR_DIR_FORWARD)
    virtual int direction() const = 0;

    // Execution control

    /** Will be called before query execution starts.
      * If result already obtained - return False.
      * In this case `stop` method shouldn't be called
      * at the end.
      */
    virtual bool start() = 0;

    //! Get new value
    virtual bool put(const aku_Sample& sample) = 0;

    //! Will be called when processing completed without errors
    virtual void stop() = 0;

    //! Will be called on error
    virtual void set_error(aku_Status error) = 0;
};


}}  // namespaces
