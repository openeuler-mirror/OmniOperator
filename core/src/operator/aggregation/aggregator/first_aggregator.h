/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
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
static constexpr int32_t PARTIAL_FIRST_OUTPUT_LENGTH = sizeof(FirstState);

// First Aggregator date type information:
// input: InputType
// intermediate: InputType + bool(wrap with ContainerDataType and ContainerVector)
// final: InputType
template <bool INPUT_RAW, bool OUT_PARTIAL, typename InputType>
class FirstAggregator : public Aggregator {
public:
    FirstAggregator(FunctionType aggregateType, const DataTypes &in, const DataTypes &out,
        std::vector<int32_t> &channels)
        : Aggregator(aggregateType, in, out, channels),
          isIgnoreNull(aggregateType == OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL)
    {}

    FirstAggregator(FunctionType aggregateType, const DataTypes &in, const DataTypes &out,
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull)
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

    void UpdateFirstState(int32_t rowIndex, FirstState *firstState, BaseVector *vector) const
    {
        if (vector->GetEncoding() == OMNI_DICTIONARY) {
            *reinterpret_cast<InputType *>(firstState->val) =
                static_cast<Vector<DictionaryContainer<InputType>> *>(vector)->GetValue(rowIndex);
        } else {
            *reinterpret_cast<InputType *>(firstState->val) =
                static_cast<Vector<InputType> *>(vector)->GetValue(rowIndex);
        }
        firstState->valIsNull = vector->IsNull(rowIndex);
    }

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        if (state.val == nullptr) {
            InitiateGroup(state, vectorBatch, rowIndex);
            return;
        }
        auto firstState = static_cast<FirstState *>(state.val);
        if constexpr (INPUT_RAW) {
            BaseVector *vector = vectorBatch->Get(channels[0]);
            if (isIgnoreNull) {
                if (!firstState->valueSet && !vector->IsNull(rowIndex)) {
                    UpdateFirstState(rowIndex, firstState, vector);
                }
                firstState->valueSet = firstState->valueSet || !vector->IsNull(rowIndex);
            } else {
                if (!firstState->valueSet) {
                    UpdateFirstState(rowIndex, firstState, vector);
                }
                firstState->valueSet = true;
            }
        } else {
            BaseVector *firstVector = vectorBatch->Get(channels[0]);
            BaseVector *valueSetVector = vectorBatch->Get(channels[1]);

            if (!firstState->valueSet) {
                UpdateFirstState(rowIndex, firstState, firstVector);
            }
            bool IntermediateState;
            if (valueSetVector->GetEncoding() == OMNI_DICTIONARY) {
                IntermediateState = static_cast<Vector<DictionaryContainer<bool>>*>(valueSetVector)->GetValue(rowIndex);
            } else {
                IntermediateState = reinterpret_cast<Vector<bool> *>(valueSetVector)->GetValue(rowIndex);
            }
            firstState->valueSet = firstState->valueSet || IntermediateState;
        }
    }

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        auto firstVector = reinterpret_cast<Vector<InputType> *>(vectors[0]);
        if (state.val == nullptr) {
            firstVector->SetNull(rowIndex);
            return;
        }
        auto firstState = static_cast<FirstState *>(state.val);
        if constexpr (OUT_PARTIAL) {
            if (firstState->valIsNull) {
                firstVector->SetNull(rowIndex);
            } else {
                firstVector->SetNotNull(rowIndex);
                firstVector->SetValue(rowIndex, *static_cast<InputType *>(firstState->val));
            }
            auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
            valueSetVector->SetValue(rowIndex, firstState->valueSet);
        } else {
            if (firstState->valIsNull) {
                firstVector->SetNull(rowIndex);
            } else {
                firstVector->SetNotNull(rowIndex);
                firstVector->SetValue(rowIndex, *static_cast<InputType *>(firstState->val));
            }
        }
    }

    void InitState(AggregateState &state) override
    {
        state.val = executionContext->GetArena()->Allocate(PARTIAL_FIRST_OUTPUT_LENGTH);
        auto ptr = executionContext->GetArena()->Allocate(sizeof(InputType));
        auto firstState = static_cast<FirstState *>(state.val);
        firstState->val = ptr;
        firstState->valueSet = false;
        firstState->valIsNull = true;
    }

private:
    bool isIgnoreNull;
};
}
}
#endif // OMNI_RUNTIME_FIRST_AGGREGATOR_H
