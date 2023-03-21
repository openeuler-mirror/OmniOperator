/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Returns the first value of `child` for a group of rows.
 * Caution: The function is non-deterministic.
 * Returns the first value of `child` for a group of rows.
 * If the first value of `child` is `null`, it returns `null`.
 * Even if [[First]] is used on an already sorted column, if we do partial aggregation and final aggregation
 * its result will not be deterministic(unless the input table is sorted and has a single partition,
 * and we use a single reducer to do the aggregation.)
 *
 * If `isIgnoreNull` is true, returns only non-null values.
 */
#ifndef OMNI_RUNTIME_FIRST_AGGREGATOR_H
#define OMNI_RUNTIME_FIRST_AGGREGATOR_H

#include "aggregator.h"
#include "type/decimal_operations.h"

namespace omniruntime {
namespace op {
// First Aggregator date type information:
// input: InputType
// intermediate: InputType + bool(wrap with ContainerDataType and ContainerVector)
// final: InputType
template <typename InputVecType, typename InputType> class FirstAggregator : public Aggregator {
public:
    FirstAggregator(FunctionType aggregateType, const DataTypes &in, const DataTypes &out, std::vector<int32_t> &channels)
        : Aggregator(aggregateType, in, out, channels),
          isIgnoreNull(aggregateType == OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL)
    {}

    FirstAggregator(FunctionType aggregateType, const DataTypes &in, const DataTypes &out, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : Aggregator(aggregateType, in, out, channels, inputRaw, outputPartial, isOverflowAsNull),
          isIgnoreNull(aggregateType == OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL)
    {}

    ~FirstAggregator() override {}

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        if (state.val == nullptr) {
            InitState(state);
        }
        ProcessGroup(state, vectorBatch, rowIndex);
    }

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        if (state.val == nullptr) {
            InitiateGroup(state, vectorBatch, rowIndex);
            return;
        }
        int32_t offset;
        auto firstState = static_cast<FirstState *>(state.val);
        if (inputRaw) {
            Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
            if (isIgnoreNull) {
                if (!firstState->valueSet && !vector->IsValueNull(offset)) {
                    *reinterpret_cast<InputType *>(firstState->val) =
                        static_cast<InputVecType *>(vector)->GetValue(offset);
                    firstState->valIsNull = vector->IsValueNull(offset);
                }
                firstState->valueSet = firstState->valueSet || !vector->IsValueNull(offset);
            } else {
                if (!firstState->valueSet) {
                    *reinterpret_cast<InputType *>(firstState->val) =
                        static_cast<InputVecType *>(vector)->GetValue(offset);
                    firstState->valIsNull = vector->IsValueNull(offset);
                }
                firstState->valueSet = true;
            }
        } else {
            auto firstVector = reinterpret_cast<InputVecType *>(
                VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset));
            auto valueSetVector = reinterpret_cast<BooleanVector *>(
                VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[1]), rowIndex, offset));

            if (!firstState->valueSet) {
                *reinterpret_cast<InputType *>(firstState->val) =
                    static_cast<InputVecType *>(firstVector)->GetValue(offset);
                firstState->valIsNull = firstVector->IsValueNull(offset);
            }
            firstState->valueSet = firstState->valueSet || valueSetVector->GetValue(offset);
        }
    }

    void ExtractValues(AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        int32_t offset;
        auto firstVector =
                reinterpret_cast<InputVecType *>(VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset));
        if (state.val == nullptr) {
            firstVector->SetValueNull(rowIndex);
            return;
        }
        auto firstState = static_cast<FirstState *>(state.val);
        if (outputPartial) {
             firstVector =
                reinterpret_cast<InputVecType *>(VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset));

            if (firstState->valIsNull) {
                firstVector->SetValueNull(rowIndex);
            } else {
                firstVector->SetValueNotNull(rowIndex);
                firstVector->SetValue(rowIndex, *static_cast<InputType *>(firstState->val));
            }
            auto valueSetVector =
                reinterpret_cast<BooleanVector *>(VectorHelper::ExpandVectorAndIndex(vectors[1], rowIndex, offset));
            valueSetVector->SetValue(rowIndex, firstState->valueSet);
        } else {
            Vector *vector = VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset);
            auto toSetVector = static_cast<InputVecType *>(vector);
            if (firstState->valIsNull) {
                toSetVector->SetValueNull(rowIndex);
            } else {
                toSetVector->SetValueNotNull(rowIndex);
                toSetVector->SetValue(rowIndex, *static_cast<InputType *>(firstState->val));
            }
        }
    }

private:
    void InitState(AggregateState &state)
    {
        state.val = executionContext->GetArena()->Allocate(PARTIAL_FIRST_OUTPUT_LENGTH);
        auto ptr = executionContext->GetArena()->Allocate(sizeof(InputType));
        auto firstState = static_cast<FirstState *>(state.val);
        firstState->val = ptr;
        firstState->valueSet = false;
        firstState->valIsNull = true;
    }
    bool isIgnoreNull;
};
}
}
#endif // OMNI_RUNTIME_FIRST_AGGREGATOR_H
