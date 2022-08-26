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
    DataTypePtr input;
    DataTypePtr output;
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

using DecimalAverageState = struct DecimalAverageState {
    int64_t count;
    int64_t overflow;
    uint64_t lowBits;
    int64_t highBits;
};

using DecimalSumState = struct DecimalSumState {
    int64_t overflow;
    uint64_t lowBits;
    int64_t highBits;
};

static constexpr int32_t PARTIAL_SUM_OUTPUT_LENGTH = sizeof(DecimalSumState);
static constexpr int32_t PARTIAL_AVG_OUTPUT_LENGTH = sizeof(DecimalAverageState);

template <typename T> int32_t ALWAYS_INLINE Compare(const T &leftVal, const T &rightVal)
{
    return (leftVal > rightVal ? 1 : (leftVal < rightVal ? -1 : 0));
}

class Aggregator {
public:
    /* Initiate this aggregator, such as setting default values for states.
     * @param aggregateType indicates which aggregate function this aggregator stands for
     * @param outputType indicates this aggregator's output data type. It's used to create Vector
     *                 */
    Aggregator(FunctionType aggregateType, DataTypePtr inputType, DataTypePtr outputType, int32_t channel,
        bool inputRaw = true, bool outputPartial = false)
        : type(aggregateType),
          inputType(std::move(inputType)),
          outputType(std::move(outputType)),
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

    virtual bool IsInputRaw() const
    {
        return this->inputRaw;
    }

    virtual bool IsOutputPartial() const
    {
        return this->outputPartial;
    }

    virtual FunctionType GetType() const
    {
        return type;
    }

    virtual const DataTypePtr &GetInputType() const
    {
        return inputType;
    }

    virtual const DataTypePtr &GetOutputType() const
    {
        return outputType;
    }

    virtual int32_t GetInputChannel() const
    {
        return channel;
    }

    void SetExecutionContextAllocator(BaseAllocator *allocator)
    {
        executionContext->GetArena()->SetAllocator(allocator);
    }

public:
    static const int32_t INVALID_MASK_COL = -1;
    static const int32_t INVALID_INPUT_COL = -1;

protected:
    FunctionType type;
    DataTypePtr inputType;
    DataTypePtr outputType;
    bool inputRaw;
    bool outputPartial;
    int32_t channel;
    std::unique_ptr<ExecutionContext> executionContext;
};
} // end of namespace op
} // end of namespace omniruntime
#endif