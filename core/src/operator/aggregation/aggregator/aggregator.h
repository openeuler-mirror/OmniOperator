/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators header
 */
#ifndef AGGREGATOR_H
#define AGGREGATOR_H

#include <memory>

#include "operator/aggregation/definitions.h"
#include "type/data_type.h"
#include "vector/vector.h"
#include "vector/vector_common.h"
#include "operator/execution_context.h"
#include "operator/util/function_type.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

using ColumnIndex = struct ColumnIndex {
    int32_t idx;
    DataType input;
    DataType output;
};

using PrepareContext = struct PrepareContext {
    uint32_t *context;
    size_t len;
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
     *          */
    Aggregator(FunctionType aggregateType, const DataType &inputType, const DataType &outputType, int32_t channel,
        bool inputRaw = true, bool outputPartial = false)
        : type(aggregateType),
          inputType(inputType),
          outputType(outputType),
          initiated(false),
          inputRaw(inputRaw),
          outputPartial(outputPartial),
          channel(channel),
          executionContext(std::make_unique<ExecutionContext>())
    {}

    virtual ~Aggregator() {}

    // process input data row by row, e.g. for 'sum' aggregation function, add each input to the intermediate state.
    // TODO seperate data process from hashing in 'inloop'. Change this function to process a input batch instead of
    // only a row.
    virtual void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) = 0;

    virtual void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) = 0;

    // set result to output vector
    virtual void ExtractValue(AggregateState &state, Vector *vector, int32_t rowIndex) = 0;

    bool IsInputRaw() const
    {
        return this->inputRaw;
    }

    bool IsOutputPartial() const
    {
        return this->outputPartial;
    }

    FunctionType GetType() const
    {
        return type;
    }

    const DataType &GetInputType() const
    {
        return inputType;
    }

    const DataType &GetOutputType() const
    {
        return outputType;
    }

    int32_t GetInputChannel() const
    {
        return channel;
    }

public:
    static const int32_t INVALID_MASK_COL = -1;
    static const int32_t INVALID_INPUT_COL = -1;

protected:
    FunctionType type;
    DataType inputType;
    DataType outputType;
    bool initiated;
    bool inputRaw;
    bool outputPartial;
    int32_t channel;
    std::unique_ptr<ExecutionContext> executionContext;
};
} // end of namespace op
} // end of namespace omniruntime
#endif