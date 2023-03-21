/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Aggregate factories
 */

#ifndef OMNI_RUNTIME_SUM_SPARK_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_SPARK_DECIMAL_AGGREGATOR_H

#include "aggregator.h"
#include "type/decimal_operations.h"


namespace omniruntime {
namespace op {
// decimal sum state, sum's initial val is 0.
using SparkDecimalSumState = struct SparkDecimalSumState {
    int128 val;
    bool isOverflow; // isOverflow is true when it has had an overflow
    bool isEmpty;    // isEmpty is true when all row in a vector are NULL
    bool isUnprocessed;
};

static constexpr int32_t SPARK_DECIMAL_SUM_STATE_LENGTH = sizeof(SparkDecimalSumState);

/**
 * SUM agg data type
 * input: decimal
 * middle: decimal+boolean(isEmpty)
 * final: decimal
 */
template <bool INPUT_RAW, bool OUTPUT_PARTIAL> class SumSparkDecimalAggregator : public Aggregator {
public:
    SumSparkDecimalAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels)
    {}

    SumSparkDecimalAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels, inputRaw, outputPartial,
        isOverflowAsNull)
    {}

    ~SumSparkDecimalAggregator() override = default;

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
        SparkDecimalSumState *stateVal = static_cast<SparkDecimalSumState *>(state.val);
        if (vector->IsValueNull(offset)) {
            static_cast<SparkDecimalSumState *>(state.val)->isEmpty = true;
            return;
        }

        // The inputType is either OMNI_DECIMAL64 or OMNI_DECIMAL128
        int32_t inputType = inputTypes.GetIds()[0];
        if constexpr (INPUT_RAW) {
            // 1. get a new value
            int128 curVal;
            GetValFromVector(vector, offset, inputType, curVal);

            // 2. decode current state
            int128 decodedDec = stateVal->val;
            bool isOverflow = stateVal->isOverflow;
            bool isEmpty = stateVal->isEmpty || stateVal->isUnprocessed;
            // 3. if overflowed, no need to do calculation
            if (isOverflow) {
                return;
            }
            // 4. do calculation
            isOverflow = isOverflow || AddCheckedOverflow(decodedDec, curVal, decodedDec);
            // 5. encode to state, the isEmpty is always false because the row is not NULL
            EncodeSumState(static_cast<SparkDecimalSumState *>(state.val), decodedDec, isOverflow, false);
        } else {
            // 1. get partial sum and isEmptyInVec
            int128 curVal;
            GetValFromVector(vector, offset, inputType, curVal);
            int32_t emptyOffset;
            Vector *emptyVector =
                VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[1]), rowIndex, emptyOffset);
            bool isEmptyInVec = reinterpret_cast<BooleanVector *>(emptyVector)->GetValue(emptyOffset);

            // 2. decode current state and intermediate state
            int128 decodedDec = stateVal->val;
            bool isOverflow = stateVal->isOverflow;
            bool isEmptyInState = stateVal->isEmpty || stateVal->isUnprocessed;

            // 3. if overflowed, no need to do calculation
            if (isOverflow) {
                return;
            }

            // 4. do calculation
            isOverflow = isOverflow || AddCheckedOverflow(decodedDec, curVal, decodedDec);
            // 5. encode to state.
            // isEmptyInVec will Set to false if either one of the left or right is set to false.
            // This means we have seen at least a value that was not null.
            EncodeSumState(static_cast<SparkDecimalSumState *>(state.val), decodedDec, isOverflow,
                isEmptyInState && isEmptyInVec);
        }
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);

        // The inputType is either OMNI_DECIMAL64 or OMNI_DECIMAL128
        int32_t inputType = inputTypes.GetIds()[0];
        if constexpr (INPUT_RAW) {
            if (vector->IsValueNull(offset)) {
                state.val = executionContext->GetArena()->Allocate(SPARK_DECIMAL_SUM_STATE_LENGTH);
                EncodeSumState(static_cast<SparkDecimalSumState *>(state.val), 0, false, true);
                return;
            }
            int128 initState;
            GetValFromVector(vector, offset, inputType, initState);

            int64_t oldOverflow = 0;
            state.val = executionContext->GetArena()->Allocate(SPARK_DECIMAL_SUM_STATE_LENGTH);
            EncodeSumState(static_cast<SparkDecimalSumState *>(state.val), initState, oldOverflow, false, true);
        } else {
            // in final mode, input vector is partial sum. if partial sum is null, it means we have had an overflow.
            if (vector->IsValueNull(offset)) {
                state.val = executionContext->GetArena()->Allocate(SPARK_DECIMAL_SUM_STATE_LENGTH);
                EncodeSumState(static_cast<SparkDecimalSumState *>(state.val), 0, true, false);
                return;
            }
            // get value from containerVector
            int128 curVal;
            GetValFromVector(vector, offset, inputType, curVal);

            int32_t emptyOffset;
            Vector *emptyVector =
                VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[1]), rowIndex, emptyOffset);
            bool isEmpty = reinterpret_cast<BooleanVector *>(emptyVector)->GetValue(emptyOffset);

            state.val = executionContext->GetArena()->Allocate(SPARK_DECIMAL_SUM_STATE_LENGTH);
            EncodeSumState(static_cast<SparkDecimalSumState *>(state.val), curVal, false, isEmpty);
        }
    }

    void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {

        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset);
        SparkDecimalSumState *stateVal = static_cast<SparkDecimalSumState *>(state.val);

        int128 decodedDec = stateVal->val;
        bool isOverflow = stateVal->isOverflow;
        bool isEmpty = stateVal->isEmpty;
        bool isUnprocessed = stateVal->isUnprocessed;

        int128 resultDec;
        // only support output scale >= input scale
        // for spark, input type is always decimal. for olk, input type is varbinary and the precision
        // and scale are zero.
        int32_t scaleDiff = static_cast<DecimalDataType *>(outputTypes.GetType(0).get())->GetScale() -
            static_cast<DecimalDataType *>(inputTypes.GetType(0).get())->GetScale();
        // rescale dividend and divisor to output scale
        isOverflow = isOverflow || MulCheckedOverflow(decodedDec, TenOfInt128[scaleDiff], resultDec);

        // The outputType is either OMNI_DECIMAL64 or OMNI_DECIMAL128
        int32_t outputType = outputTypes.GetIds()[0];
        if constexpr (OUTPUT_PARTIAL) {
            if (isOverflow) {
                // partial output vector is sum, it will be set to NULL if overflowed.
                vector->SetValueNull(rowIndex);
            } else {
                SetValToVector(vector, rowIndex, outputType, resultDec);
            }

            int32_t emptyOffset;
            Vector *emptyVector = VectorHelper::ExpandVectorAndIndex(vectors[1], rowIndex, emptyOffset);
            reinterpret_cast<BooleanVector *>(emptyVector)->SetValue(rowIndex,
                isEmpty || isUnprocessed);
        } else {
            if (isOverflow) {
                SetNullOrThrowException(vector, rowIndex);
                return;
            }
            if (isEmpty) {
                // isEmpty is true means that all row is NULL, so we set the result to NULL.
                vector->SetValueNull(rowIndex);
                return;
            }
            SetValToVector(vector, rowIndex, outputType, resultDec);
        }
    }

    void InitState(AggregateState &state)
    {
        state.val = executionContext->GetArena()->Allocate(SPARK_DECIMAL_SUM_STATE_LENGTH);
        EncodeSumState(static_cast<SparkDecimalSumState *>(state.val), 0, false, false, true);
    }

private:
    // set vector value null or throw exception when overflow
    void SetNullOrThrowException(Vector *vector, int index)
    {
        if (!IsOverflowAsNull()) {
            throw OmniException("OPERATOR_RUNTIME_ERROR", "Overflow in sum of decimals");
        }
        vector->SetValueNull(index);
    }

    void EncodeSumState(SparkDecimalSumState *statePtr, const int128 &val, const bool isOverflow, const bool isEmpty,
        const bool isUnprocessed = false)
    {
        statePtr->val = val;
        statePtr->isOverflow = isOverflow;
        statePtr->isEmpty = isEmpty;
        statePtr->isUnprocessed = isUnprocessed;
    }

    // Set decimal val to output vector in Extract function. The outputType is either OMNI_DECIMAL64 or OMNI_DECIMAL128.
    void SetValToVector(Vector *vector, int32_t rowIndex, int32_t outputType, int128 &deciVal)
    {
        if (outputType == OMNI_DECIMAL64) {
            int64_t longVal = static_cast<int64_t>(deciVal);
            static_cast<LongVector *>(vector)->SetValue(rowIndex, longVal);
        } else {
            static_cast<Decimal128Vector *>(vector)->SetValue(rowIndex, Decimal128(deciVal));
        }
    }

    // Get decimal val from input vector. The inputType is either OMNI_DECIMAL64 or OMNI_DECIMAL128. The deciVal is the
    // result.
    void GetValFromVector(Vector *vector, int32_t rowIndex, int32_t inputType, int128 &deciVal)
    {
        if (inputType == OMNI_DECIMAL64) {
            deciVal = reinterpret_cast<LongVector *>(vector)->GetValue(rowIndex);
        } else {
            deciVal = reinterpret_cast<Decimal128Vector *>(vector)->GetValue(rowIndex).ToInt128();
        }
    }
};
}
}

#endif // OMNI_RUNTIME_SUM_SPARK_DECIMAL_AGGREGATOR_H
