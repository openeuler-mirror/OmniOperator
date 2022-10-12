/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators header
 */
#ifndef AGGREGATOR_H
#define AGGREGATOR_H

#include "operator/aggregation/definitions.h"
#include "type/data_types.h"
#include "type/data_type.h"
#include "type/decimal128.h"
#include "type/base_operations.h"
#include "vector/vector.h"
#include "vector/vector_common.h"
#include "operator/execution_context.h"
#include "operator/util/function_type.h"
#include "util/type_util.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

using ColumnIndex = struct ColumnIndex {
    int32_t idx;
    DataTypePtr input;
    DataTypePtr output;
};

using AggregateState = struct AggregateState {
    // for sum/avg/min/max holds latest aggregatred value
    // for stringMin/stringMax holds pointer to latestes aggregated value
    void *val = nullptr;

    // for sum/avg holds number of rows aggregated so far (not including null rows), or -1 if overflow happened.
    // for min/max it is 1 when at leats there is one not-null row in aggregation, otherwise 0.
    // for count holds number of counted rows
    int64_t count = 0;

    void Reset()
    {
        val = nullptr;
        count = 0;
    }
};

using DecimalAverageState = struct DecimalAverageState {
    int64_t count;
    int64_t overflow;
    int128 val;
};

using DecimalSumState = struct DecimalSumState {
    int64_t overflow;
    int128 val;
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
    Aggregator(const FunctionType aggregateType, const DataTypes &inputTypes, const DataTypes &outputTypes,
        const std::vector<int32_t> &channels, const bool inputRaw = true, const bool outputPartial = false,
        const bool isOverflowAsNull = false)
        : type(aggregateType),
          inputTypes(inputTypes),
          outputTypes(outputTypes),
          inputRaw(inputRaw),
          outputPartial(outputPartial),
          isOverflowAsNull(isOverflowAsNull),
          channels(channels),
          executionContext(std::make_unique<ExecutionContext>())
    {}

    virtual ~Aggregator() = default;

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

    virtual void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex)
    {
        throw OmniException("Not implemented",
            "InitiateGroup(AggregateState &, VectorBatch *, int32_t) not implemented for "
            + std::to_string(as_integer(type)));
    }

    virtual void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex)
    {
        throw OmniException("Not implemented",
            "ProcessGroup(AggregateState &, VectorBatch *, int32_t) not implemented for "
            + std::to_string(as_integer(type)));
    }

    // for no groupby aggregation
    virtual void ProcessGroup(
        AggregateState &state, VectorBatch *vectorBatch, const int32_t rowOffset, const int32_t rowCount)
    {
#ifdef DEBUG
        LogWarn("Using not-optimized aggregator api for aggregator %d", as_integer(type));
#endif
        int32_t end = rowOffset + rowCount;
        for (int32_t i = rowOffset; i < end; ++i) {
            ProcessGroup(state, vectorBatch, i);
        }
    }

    // for groupby hash aggregation
    virtual void ProcessGroup(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
        VectorBatch *vectorBatch, const int32_t rowOffset)
    {
#ifdef DEBUG
        LogWarn("Using not-optimized aggregator api for aggregator %d", as_integer(type));
#endif
        int32_t rowIndex = rowOffset;
        size_t rowCount = rowStates.size();
        for (size_t i = 0; i < rowCount; ++i) {
            ProcessGroup(rowStates[i][aggIdx], vectorBatch, rowIndex++);
        }
    }

    virtual void InitState(AggregateState &state)
    {
        state.val = nullptr;
        state.count = 0;
    }

    // set result to output vector
    virtual void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, const int32_t rowIndex) = 0;

    virtual bool IsTypedAggregator()
    {
        return false;
    }

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

    virtual const DataTypes &GetInputTypes() const
    {
        return inputTypes;
    }

    virtual const DataTypes &GetOutputTypes() const
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
    const FunctionType type;
    const DataTypes inputTypes;
    const DataTypes outputTypes;
    const bool inputRaw;
    const bool outputPartial;
    const bool isOverflowAsNull;
    const std::vector<int32_t> channels;
    std::unique_ptr<ExecutionContext> executionContext;
};
} // end of namespace op
} // end of namespace omniruntime
#endif
