/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
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
#include "src/util/config_util.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::exception;
using namespace omniruntime::vec;

using ColumnIndex = struct ColumnIndex {
    int32_t idx;
    type::DataTypePtr input;
    type::DataTypePtr output;
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
    type::int128_t val;
};

using DecimalSumState = struct DecimalSumState {
    int64_t overflow;
    type::int128_t val;
};

using FirstState = struct FirstState {
    void *val = nullptr;
    bool valIsNull = true;
    bool valueSet = false;
};

// Avg decimal and overflow is decode/encode in continuous memory
static inline void DecodeAvgDecimal(op::DecimalAverageState *statePtr, type::int128_t &val, int64_t &overflow,
    int64_t &count)
{
    count = statePtr->count;
    overflow = statePtr->overflow;
    val = statePtr->val;
}

static inline void EncodeAvgDecimal(op::DecimalAverageState *statePtr, const type::int128_t &val,
    const int64_t &overflow, const int64_t &count)
{
    statePtr->count = count;
    statePtr->overflow = overflow;
    statePtr->val = val;
}

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
    Aggregator(const FunctionType aggregateType, const type::DataTypes &inputTypes, const type::DataTypes &outputTypes,
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

    virtual void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex)
    {
        throw OmniException("Not implemented",
            "InitiateGroup(AggregateState &, VectorBatch *, int32_t) not implemented for " +
            std::to_string(as_integer(type)));
    }

    virtual void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex)
    {
        throw OmniException("Not implemented",
            "ProcessGroup(AggregateState &, VectorBatch *, int32_t) not implemented for " +
            std::to_string(as_integer(type)));
    }

    virtual void GetSpillType(std::vector<DataTypeId> &spillTypes)
    {
        throw OmniException("UNSUPPORTED_ERROR",
            "GetSpillType not implemented for " + std::to_string(as_integer(type)));
    }

    // for no groupby aggregation
    virtual void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, const int32_t rowOffset,
        const int32_t rowCount)
    {
#ifdef DEBUG
        LogWarn("Using not-optimized aggregator api for aggregator %d", as_integer(type));
#endif
        int32_t end = rowOffset + rowCount;
        for (int32_t i = rowOffset; i < end; ++i) {
            ProcessGroup(state, vectorBatch, i);
        }
    }

    // for no groupby aggregation  with filter
    virtual void ProcessGroupFilter(AggregateState &state, VectorBatch *vectorBatch, const int32_t rowOffset,
        const int32_t filterIndex)
    {
#ifdef DEBUG
        LogWarn("Using not-optimized aggregator api for aggregator %d", as_integer(type));
#endif

        int32_t rowEnd = rowOffset + vectorBatch->GetRowCount();
        auto booleanVector = static_cast<Vector<bool> *>(vectorBatch->Get(filterIndex));
        bool needFilterJude = false;
        for (int32_t start = 0, end = rowEnd - 1; start <= end; ++start, --end) {
            if (!booleanVector->GetValue(start) || !booleanVector->GetValue(end)) {
                needFilterJude = true;
                break;
            }
        }
        if (needFilterJude) {
            for (int32_t i = rowOffset; i < rowEnd; ++i) {
                if (booleanVector->GetValue(i)) {
                    ProcessGroup(state, vectorBatch, i);
                }
            }
        } else {
            for (int32_t i = rowOffset; i < rowEnd; ++i) {
                ProcessGroup(state, vectorBatch, i);
            }
        }
    }

    // for groupby hash aggregation
    virtual void ProcessGroup(std::vector<AggregateState *> &rowStates, const size_t aggIdx, VectorBatch *vectorBatch,
        const int32_t rowOffset)
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

    virtual void ProcessGroupAfterSpill(AggregateState &state, VectorBatch *vectorBatch, int32_t &vectorIndex,
        int32_t rowIdx)
    {
        throw OmniException("UNSUPPORTED_ERROR",
            "ProcessGroupAfterSpill not implemented for " + std::to_string(as_integer(type)));
    }

    // for groupby hash aggregation
    virtual void ProcessGroupFilter(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
        VectorBatch *vectorBatch, const int32_t filterStart, const int32_t rowOffset)
    {
#ifdef DEBUG
        LogWarn("Using not-optimized aggregator api for aggregator %d", as_integer(type));
#endif
        int32_t rowIndex = rowOffset;
        auto rowCount = static_cast<int32_t>(rowStates.size());
        bool needFilterJude = false;

        auto booleanVector = static_cast<Vector<bool> *>(vectorBatch->Get(filterStart + aggIdx));
        for (int32_t start = 0, end = rowCount - 1; start <= end; ++start, --end) {
            if (!booleanVector->GetValue(start) || !booleanVector->GetValue(end)) {
                needFilterJude = true;
                break;
            }
        }

        if (needFilterJude) {
            for (int32_t i = 0; i < rowCount; ++i) {
                if (booleanVector->GetValue(i)) {
                    ProcessGroup(rowStates[i][aggIdx], vectorBatch, rowIndex++);
                    continue;
                }
                rowIndex++;
            }
        } else {
            for (int32_t i = 0; i < rowCount; ++i) {
                ProcessGroup(rowStates[i][aggIdx], vectorBatch, rowIndex++);
            }
        }
    }

    virtual void InitState(AggregateState &state)
    {
        state.val = nullptr;
        state.count = 0;
    }

    // set result to output vector
    virtual void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors,
        const int32_t rowIndex) = 0;
    virtual void ExtractSpillValues(const AggregateState &state, std::vector<BaseVector *> &vectors,
        const int32_t rowIndex) = 0;

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

    virtual const type::DataTypes &GetInputTypes() const
    {
        return inputTypes;
    }

    virtual const type::DataTypes &GetOutputTypes() const
    {
        return outputTypes;
    }

    virtual const std::vector<int32_t> &GetInputChannels() const
    {
        return channels;
    }

public:
    static const int32_t INVALID_MASK_COL = -1;
    static const int32_t INVALID_INPUT_COL = -1;

protected:
    const FunctionType type;
    type::DataTypes inputTypes;
    type::DataTypes outputTypes;
    const bool inputRaw;
    const bool outputPartial;
    const bool isOverflowAsNull;
    const std::vector<int32_t> channels;
    std::unique_ptr<ExecutionContext> executionContext;
};
} // end of namespace op
} // end of namespace omniruntime
#endif
