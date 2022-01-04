/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators header
 */
#ifndef AGGREGATOR_H
#define AGGREGATOR_H

#include <memory>

#include "operator/aggregation/definitions.h"
#include "vector/vector_type.h"
#include "vector/vector.h"
#include "vector/vector_common.h"
#include "operator/execution_context.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

using ColumnIndex = struct ColumnIndex {
    uint32_t idx;
    VecType input;
    VecType output;
};

using PrepareContext = struct PrepareContext {
    uint32_t *context;
    size_t len;
};

using AggregateType = enum AggregateType {
    OMNI_AGGREGATION_TYPE_SUM = 0,
    OMNI_AGGREGATION_TYPE_COUNT,
    OMNI_AGGREGATION_TYPE_AVG,
    OMNI_AGGREGATION_TYPE_MAX,
    OMNI_AGGREGATION_TYPE_MIN,
    OMNI_AGGREGATION_TYPE_DNV,
    OMNI_AGGREGATION_TYPE_INVALIDE
};

using AggregateState = union AggregateState {
    // For sum() and basic type min()/max()
    void *val;
    // For count()
    int64_t count;
    // For basic type avg()
    struct {
        void *avgVal;
        int64_t avgCnt;
    };
    // For string min()/max()
    struct {
        uint8_t *strVal;
        int32_t strLen;
    };
};

template <typename T> int32_t ALWAYS_INLINE Compare(const T &leftVal, const T &rightVal)
{
    return (leftVal > rightVal ? 1 : (leftVal < rightVal ? -1 : 0));
}

class Aggregator {
public:
    /* Initiate this aggregator, such as setting default values for states.
     * @param aggregateType indicates which aggregate function this aggregator stands for
     * @param outputType indicates this aggregator's output data type. It's used to create Vector
     *  */
    Aggregator(AggregateType aggregateType, int32_t inputType, int32_t outputType, bool inputRaw = true,
        bool outputParitial = false)
        : type(aggregateType),
          inputType(inputType),
          outputType(outputType),
          initiated(false),
          inputRaw(inputRaw),
          outputPartial(outputParitial),
          nonGroupState({ nullptr }),
          executionContext(std::make_unique<ExecutionContext>()) {}
    virtual ~Aggregator() {}
    // process input data row by row, e.g. for 'sum' aggregation function, add each input to the intermediate state.
    // TODO seperate data process from hashing in 'inloop'. Change this function to process a input batch instead of
    // only a row.
    virtual void ProcessGroup(AggregateState &state, Vector *vector, uint32_t offset) = 0;
    virtual void ProcessNonGroup(Vector *vector, uint32_t offset) = 0;
    virtual void InitiateGroup(AggregateState &state, Vector *vector, uint32_t offset) = 0;
    virtual void InitiateNonGroup(Vector *vector, uint32_t offset) = 0;
    // return nullptr if error occurs
    virtual void* Evaluate(const AggregateState &state) = 0;
    // set result to output vector
    virtual void ExtractValue(Vector *vector, AggregateState &state, int32_t rowIndex) = 0;

    bool IsInputRaw() const
    {
        return this->inputRaw;
    }

    bool IsOutputPartial() const
    {
        return this->outputPartial;
    }

    AggregateType GetType() const
    {
        return type;
    }

    const AggregateState &GetNonGroupState()
    {
        return nonGroupState;
    }

    int32_t GetInputType() const
    {
        return inputType;
    }

    int32_t GetOutputType() const
    {
        return outputType;
    }

protected:
    AggregateType type;
    int32_t inputType;
    int32_t outputType;
    // state for non-grouping aggregate
    AggregateState nonGroupState;
    bool initiated;
    bool inputRaw;
    bool outputPartial;
    std::unique_ptr<ExecutionContext> executionContext;
};
} // end of namespace op
} // end of namespace omniruntime
#endif