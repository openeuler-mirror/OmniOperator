/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators header
 */
#ifndef AGGREGATOR_H
#define AGGREGATOR_H

#include <memory>

#include "operator/aggregation/definitions.h"
#include "type/data_types.h"
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

using FirstState = struct FirstState {
    void *val;
    bool valIsNull;
    bool valueSet;
};

static constexpr int32_t PARTIAL_SUM_OUTPUT_LENGTH = sizeof(DecimalSumState);
static constexpr int32_t PARTIAL_AVG_OUTPUT_LENGTH = sizeof(DecimalAverageState);
static constexpr int32_t PARTIAL_FIRST_OUTPUT_LENGTH = sizeof(FirstState);

template <typename T> int32_t ALWAYS_INLINE Compare(const T &leftVal, const T &rightVal)
{
    return (leftVal > rightVal ? 1 : (leftVal < rightVal ? -1 : 0));
}

class Aggregator {
public:
    /* Initiate this aggregator, such as setting default values for states.
     * @param aggregateType indicates which aggregate function this aggregator stands for
     * @param inputTypes indicates this aggregator's input data types(support multi-input)
     * it can use normal vector or container vector
     * @param outputTypes indicates this aggregator's output data types(support multi-input)
     * it can use normal vector or container vector
     * @param channels indicates this aggregator's input channels for VectorBatch.
     * @param inputRaw indicates this aggregator's input data type
     * true for raw input, false for intermeidate input, default value as true.
     * @param outputPartial indicates this aggregator's output data type.
     * true for intermeidate output, false for final output, default value as false.
     * @param isOverflowAsNull indicates aggregator handle overflow calculation result
     * true overflow as null value, false throw exception, default value as false.
     *
     */
    Aggregator(FunctionType aggregateType, DataTypesPtr inputTypes, DataTypesPtr outputTypes,
        const std::vector<int32_t> &channels, bool inputRaw = true, bool outputPartial = false,
        bool isOverflowAsNull = false)
        : type(aggregateType),
          inputTypes(std::move(inputTypes)),
          outputTypes(std::move(outputTypes)),
          inputRaw(inputRaw),
          outputPartial(outputPartial),
          isOverflowAsNull(isOverflowAsNull),
          channels(channels),
          executionContext(std::make_unique<ExecutionContext>())
    {}

    virtual ~Aggregator() {}

#ifdef ENABLE_HMPP
    virtual void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch)
    {
        throw OmniException("NOT SUPPORT", "this aggregator is not supported by hmpp");
    }
    // HMPP handle flag for input handle framework, push down choice to aggregator
    virtual bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch)
    {
        return false;
    }
#endif

    // process input data row by row, e.g. for 'sum' aggregation function, add each input to the intermediate state.
    // TODO seperate data process from hashing in 'inloop'. Change this function to process a input batch instead of
    // only a row.
    virtual void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) = 0;

    virtual void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) = 0;

    // set result to output vector
    virtual void ExtractValues(AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) = 0;

    virtual bool IsInputRaw() const
    {
        return this->inputRaw;
    }

    virtual bool IsOutputPartial() const
    {
        return this->outputPartial;
    }

    virtual bool IsOverflowAsNull() const
    {
        return this->isOverflowAsNull;
    }

    virtual FunctionType GetType() const
    {
        return type;
    }

    virtual const DataTypesPtr &GetInputTypes() const
    {
        return inputTypes;
    }

    virtual const DataTypesPtr &GetOutputTypes() const
    {
        return outputTypes;
    }

    virtual const std::vector<int32_t> &GetInputChannels() const
    {
        return channels;
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
    DataTypesPtr inputTypes;
    DataTypesPtr outputTypes;
    bool inputRaw;
    bool outputPartial;
    bool isOverflowAsNull;
    std::vector<int32_t> channels;
    std::unique_ptr<ExecutionContext> executionContext;
};
} // end of namespace op
} // end of namespace omniruntime
#endif