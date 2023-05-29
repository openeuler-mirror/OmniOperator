/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Aggregate factories
 */

#ifndef OMNI_RUNTIME_SUM_SPARK_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_SPARK_DECIMAL_AGGREGATOR_H

#include "aggregator.h"
#include "type/decimal_operations.h"
#include "type/decimal128.h"

namespace omniruntime {
namespace op {
// decimal sum state, sum's initial val is 0.
using SparkDecimalSumState = struct SparkDecimalSumState {
    int128_t val;
    bool isOverflow; // isOverflow is true when it has had an overflow
    bool isEmpty;    // isEmpty is true when all row in a vector are NULL
    bool isUnprocessed;
};

template<typename InDecimalType, typename OutDecimalType, bool HasNullFlag>
VECTORIZE_LOOP NO_INLINE void AddDecimalUseRowIndex(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
                                             const InDecimalType *__restrict dataPtr, const bool *__restrict emptyPtr,
                                                   const uint8_t *__restrict nullMap = nullptr){
    bool isOverflow = false;
    auto rowCount = rowStates.size();
    using ResultIntType = std::conditional_t<std::is_same_v<OutDecimalType,Decimal128>,int128_t ,int64_t>;

    for (size_t i = 0; i < rowCount; ++i) {
        AggregateState &state = rowStates[i][aggIdx];
        if (state.count < 0) {
            // cur state overflow, so no need to aggregate
            continue;
        }
        auto res = reinterpret_cast<OutDecimalType*>(state.val);
        ResultIntType tmpResult = 0;
        if constexpr(HasNullFlag) {
            if (nullMap[i]) {
                // partial stage overflow , so no need to do aggregation in final
                state.count = -1;
                continue;
            }
        }

        if (not emptyPtr[i]) {
            if constexpr (std::is_same_v<OutDecimalType,Decimal128>) {
                tmpResult = res->ToInt128();
                if constexpr(std::is_same_v<InDecimalType,Decimal128>) {
                    // decimal128 + decimal128 = decimal128
                    isOverflow = AddCheckedOverflow(tmpResult, dataPtr[i].ToInt128(), tmpResult);
                } else {
                    // decimal64 + decimal64 = decimal128
                    isOverflow = AddCheckedOverflow(tmpResult, int128_t(dataPtr[i]), tmpResult);
                }
            } else {
                // decimal64 + decimal64 = decimal64
                tmpResult = *res;
                isOverflow = __builtin_add_overflow(tmpResult, dataPtr[i], &tmpResult);
            }
            state.count += 1;
            *res = OutDecimalType(tmpResult);
        }

        if (isOverflow) {
            state.count = -1;
        }
    }
}

static constexpr int32_t SPARK_DECIMAL_SUM_STATE_LENGTH = sizeof(SparkDecimalSumState);

/**
 * SUM agg data type
 * input: decimal
 * middle: decimal+boolean(isEmpty)
 * final: decimal
 */
template <DataTypeId InDecimalId, DataTypeId OutDecimalId>
class SumSparkDecimalAggregator : public TypedAggregator {
public:
    using ResultType = typename AggNativeAndVectorType<OutDecimalId>::type;
    using ResultIntType = std::conditional_t<std::is_same_v<ResultType,Decimal128>,int128_t,int64_t>;
    using ResultVectorType = typename AggNativeAndVectorType<OutDecimalId>::vector;
    using InRawType = typename AggNativeAndVectorType<InDecimalId>::type;
    using InVectorType = typename AggNativeAndVectorType<InDecimalId>::vector;
    SumSparkDecimalAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : TypedAggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels, inputRaw, outputPartial,
        isOverflowAsNull)
    {}

    SumSparkDecimalAggregator(FunctionType aggregateType, const DataTypes &inputTypes,
                                                const DataTypes &outputTypes, std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial,
                                                const bool isOverflowAsNull)
            : TypedAggregator(aggregateType, inputTypes, outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull)
    {}

    ~SumSparkDecimalAggregator() override = default;

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        BaseVector *vector = vectorBatch->Get(channels[0]);
        SparkDecimalSumState *stateVal = static_cast<SparkDecimalSumState *>(state.val);
        if (vector->IsNull(rowIndex)) {
            static_cast<SparkDecimalSumState *>(state.val)->isUnprocessed = false;
            return;
        }

        // The inputType is either OMNI_DECIMAL64 or OMNI_DECIMAL128
        int32_t inputType = inputTypes.GetIds()[0];
        if (inputRaw) {
            // 1. get a new value
            int128_t curVal;
            GetDecimalValue(vector, inputType, rowIndex, curVal);

            // 2. decode current state
            int128_t decodedDec = stateVal->val;
            bool isOverflow = stateVal->isOverflow;

            // 3. if overflowed, no need to do calculation
            if (isOverflow) {
                return;
            }
            // 4. do calculation
            isOverflow = AddCheckedOverflow(decodedDec, curVal, decodedDec);
            // 5. encode to state, the isEmpty is always false because the row is not NULL
            EncodeSumState(static_cast<SparkDecimalSumState *>(state.val), decodedDec, isOverflow, false);
        } else {
            // 1. get partial sum and isEmptyInVec
            int128_t curVal;
            GetDecimalValue(vector, inputType, rowIndex, curVal);
            BaseVector *emptyVector = vectorBatch->Get(channels[1]);
            bool isEmptyInVec = reinterpret_cast<Vector<bool> *>(emptyVector)->GetValue(rowIndex);

            // 2. decode current state and intermediate state
            int128_t decodedDec = stateVal->val;
            bool isOverflow = stateVal->isOverflow;
            bool isEmptyInState = stateVal->isEmpty || stateVal->isUnprocessed;

            // 3. if overflowed, no need to do calculation
            if (isOverflow) {
                return;
            }

            // 4. do calculation
            isOverflow = AddCheckedOverflow(decodedDec, curVal, decodedDec);
            // 5. encode to state.
            // isEmptyInVec will Set to false if either one of the left or right is set to false.
            // This means we have seen at least a value that was not null.
            EncodeSumState(static_cast<SparkDecimalSumState *>(state.val), decodedDec, isOverflow,
                isEmptyInState && isEmptyInVec);
        }
    }

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        BaseVector *vector = vectors[0];

        int128_t decodedDec = 0;
        if constexpr(std::is_same_v<ResultType,Decimal128>) {
            decodedDec = static_cast<Decimal128 *>(state.val)->ToInt128();
        } else {
            decodedDec = *(static_cast<int64_t *>(state.val));
        }
        bool isOverflow = (state.count < 0);
        bool isEmpty = (state.count == 0);

        int128_t resultDec;
        // only support output scale >= input scale
        // for spark, input type is always decimal. for olk, input type is varbinary and the precision
        // and scale are zero.
        int32_t scaleDiff = static_cast<DecimalDataType *>(outputTypes.GetType(0).get())->GetScale() -
            static_cast<DecimalDataType *>(inputTypes.GetType(0).get())->GetScale();
        // rescale dividend and divisor to output scale
        isOverflow = isOverflow || MulCheckedOverflow(decodedDec, TenOfInt128[scaleDiff], resultDec);

        // The outputType is either OMNI_DECIMAL64 or OMNI_DECIMAL128
        int32_t outputType = outputTypes.GetIds()[0];
        if (outputPartial) {
            if (isOverflow) {
                // partial output vector is sum, it will be set to NULL if overflowed.
                vector->SetNull(rowIndex);
            } else {
                SetValToVector(vector, rowIndex, outputType, resultDec);
            }

            BaseVector *emptyVector = vectors[1];
            reinterpret_cast<Vector<bool> *>(emptyVector)->SetValue(rowIndex, isEmpty);
        } else {
            if (isOverflow) {
                SetNullOrThrowException(vector, rowIndex);
                return;
            }
            if (isEmpty) {
                // isEmpty is true means that all row is NULL, so we set the result to NULL.
                vector->SetNull(rowIndex);
                return;
            }
            SetValToVector(vector, rowIndex, outputType, resultDec);
        }
    }

    void InitState(AggregateState &state) override
    {
        if constexpr(std::is_same_v<ResultType,Decimal128>) {
            state.val = executionContext->GetArena()->Allocate(sizeof(Decimal128));
            new (state.val) Decimal128(0,0);
        } else {
            state.val = executionContext->GetArena()->Allocate(sizeof(int64_t));
            *((int64_t*)(state.val)) = 0;
        }
        state.count = 0;
    }

    void ProcessGroupInternalFinal(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
        BaseVector *dataVector, const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap)
    {
        // final stage : input vector will be Vector<Decimal128> or Vector<Decimal64>
        auto *dataPtr = reinterpret_cast<InRawType *>(GetValuesFromVector<InDecimalId>(dataVector));
        dataPtr += rowOffset;
        auto *emptyVector = curVectorBatch->Get(channels[1]);
        auto *emptyPtr = reinterpret_cast<const bool *>(GetValuesFromVector<type::OMNI_BOOLEAN>(emptyVector));
        emptyPtr += rowOffset;
        if (nullMap == nullptr) {
            AddDecimalUseRowIndex<InRawType, ResultType, false>(rowStates, aggIdx, dataPtr, emptyPtr);
        } else {
            AddDecimalUseRowIndex<InRawType, ResultType, true>(rowStates, aggIdx, dataPtr, emptyPtr, nullMap);
        }
    }

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap)
    {
        if (inputRaw) {
            if (indexMap == nullptr) {
                auto *ptr = reinterpret_cast<InRawType *>(GetValuesFromVector<InDecimalId>(vector));
                ptr += rowOffset;
                if (nullMap == nullptr) {
                    AddUseRowIndex<InRawType, ResultType, SumOp<InRawType, ResultType>>(rowStates, aggIdx, ptr);
                } else {
                    // Reza: can we use customize float operation similar to sumConditionalFloat
                    AddConditionalUseRowIndex<InRawType, ResultType, SumConditionalOp<InRawType, ResultType, false>>(
                        rowStates, aggIdx, ptr, nullMap);
                }
            } else {
                auto *ptr = reinterpret_cast<InRawType *>(GetValuesFromDict<InDecimalId>(vector));
                if (nullMap == nullptr) {
                    AddDictUseRowIndex<InRawType, ResultType, SumOp<InRawType, ResultType>>(rowStates, aggIdx, ptr, indexMap);
                } else {
                    AddDictConditionalUseRowIndex<InRawType, ResultType, SumConditionalOp<InRawType, ResultType, false>>(
                        rowStates, aggIdx, ptr, nullMap, indexMap);
                }
            }
        } else {
            ProcessGroupInternalFinal(rowStates, aggIdx, vector, rowOffset, nullMap, indexMap);
        }
    }

    void ProcessSingleInternalFinal(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const uint8_t *conditionMap, const int32_t *indexMap)
    {
        auto *res = reinterpret_cast<ResultType *>(state.val);
        auto *ptr = reinterpret_cast<InRawType *>(GetValuesFromVector<InDecimalId>(vector));

        auto *emptyVector = curVectorBatch->Get(channels[1]);
        auto *emptyPtr = reinterpret_cast<bool *>(GetValuesFromVector<type::OMNI_BOOLEAN>(emptyVector));
        ptr += rowOffset;
        emptyPtr += rowOffset;

        if (conditionMap == nullptr) {
            for (size_t i = 0; i < rowCount; ++i) {
                if (state.count < 0) {
                    // means overflow in final stage, no need to calculate remaining data
                    break;
                }
                if (not emptyPtr[i]) {
                    // the emptyPtr here means partial result is null
                    SumOp<InRawType, ResultType>(res, state.count, ptr[i], 1LL);
                }
            }
        } else {
            for (size_t i = 0; i < rowCount; ++i) {
                if (state.count < 0) {
                    // means overflow in final stage, no need to calculate remaining data
                    break;
                }
                if (not conditionMap[i]) {
                    if (not emptyPtr[i]) {
                        // the emptyPtr here means partial result is null
                        SumOp<InRawType, ResultType>(res, state.count, ptr[i], 1LL);
                    }
                } else {
                    // means partial overflow , no need to calculate remaining data
                    state.count = -1;
                    break;
                }
            }
        }
    }

    void ProcessSingleInternal(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap)
    {
        auto *res = reinterpret_cast<ResultType *>(state.val);
        if (inputRaw) {
            if (indexMap == nullptr) {
                auto *ptr = reinterpret_cast<InRawType *>(GetValuesFromVector<InDecimalId>(vector));
                ptr += rowOffset;
                if (nullMap == nullptr) {
                    Add<InRawType, ResultType, SumOp<InRawType, ResultType>>(res, state.count, ptr, rowCount);
                } else {
                    AddConditional<InRawType, ResultType, SumConditionalOp<InRawType, ResultType, false>>(res, state.count,
                        ptr, rowCount, nullMap);
                }
            } else {
                auto *ptr = reinterpret_cast<InRawType *>(GetValuesFromDict<InDecimalId>(vector));
                if (nullMap == nullptr) {
                    AddDict<InRawType, ResultType, SumOp<InRawType, ResultType>>(res, state.count, ptr, rowCount, indexMap);
                } else {
                    AddDictConditional<InRawType, ResultType, SumConditionalOp<InRawType, ResultType, false>>(res,
                        state.count, ptr, rowCount, nullMap, indexMap);
                }
            }
        } else {
            ProcessSingleInternalFinal(state, vector, rowOffset, rowCount, nullMap, indexMap);
        }
    }

private:
    // set vector value null or throw exception when overflow
    void SetNullOrThrowException(BaseVector *vector, int index)
    {
        if (!IsOverflowAsNull()) {
            throw OmniException("OPERATOR_RUNTIME_ERROR", "Overflow in sum of decimals");
        }
        vector->SetNull(index);
    }

    void EncodeSumState(SparkDecimalSumState *statePtr, const int128_t &val, const bool isOverflow, const bool isEmpty,
        const bool isUnprocessed = false)
    {
        statePtr->val = val;
        statePtr->isOverflow = isOverflow;
        statePtr->isEmpty = isEmpty;
        statePtr->isUnprocessed = isUnprocessed;
    }

    // Set decimal val to output vector in Extract function. The outputType is either OMNI_DECIMAL64 or OMNI_DECIMAL128.
    void SetValToVector(BaseVector *vector, int32_t rowIndex, int32_t outputType, int128_t &deciVal)
    {
        if constexpr(std::is_same_v<ResultType ,Decimal128>) {
            Decimal128 decimal128Val(deciVal);
            static_cast<Vector<Decimal128> *>(vector)->SetValue(rowIndex, decimal128Val);
        } else {
            int64_t longVal = static_cast<int64_t>(deciVal);
            static_cast<Vector<int64_t> *>(vector)->SetValue(rowIndex, longVal);
        }
    }
};
}
}

#endif // OMNI_RUNTIME_SUM_SPARK_DECIMAL_AGGREGATOR_H
