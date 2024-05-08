/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Aggregate factories
 */

#ifndef OMNI_RUNTIME_SUM_SPARK_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_SPARK_DECIMAL_AGGREGATOR_H

#include "aggregator.h"
#include "type/decimal_operations.h"
#include "type/decimal128.h"

namespace omniruntime {
namespace op {
template <typename InDecimalType, typename OutDecimalType, bool HasNullFlag>
VECTORIZE_LOOP NO_INLINE void AddDecimalUseRowIndex(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    const InDecimalType *__restrict dataPtr, const bool *__restrict emptyPtr,
    const uint8_t *__restrict nullMap = nullptr)
{
    bool isOverflow = false;
    auto rowCount = rowStates.size();
    using ResultIntType = std::conditional_t<std::is_same_v<OutDecimalType, Decimal128>, int128_t, int64_t>;

    for (size_t i = 0; i < rowCount; ++i) {
        AggregateState &state = rowStates[i][aggIdx];
        if (state.count < 0) {
            // cur state overflow, so no need to aggregate
            continue;
        }

        ResultIntType tmpResult = 0;
        if constexpr (HasNullFlag) {
            if (nullMap[i]) {
                // partial stage overflow , so no need to do aggregation in final
                state.count = -1;
                continue;
            }
        }

        if (not emptyPtr[i]) {
            if constexpr (std::is_same_v<OutDecimalType, Decimal128>) {
                auto res = reinterpret_cast<OutDecimalType *>(state.val);
                tmpResult = res->ToInt128();
                if constexpr (std::is_same_v<InDecimalType, Decimal128>) {
                    // decimal128 + decimal128 = decimal128
                    isOverflow = AddCheckedOverflow(tmpResult, dataPtr[i].ToInt128(), tmpResult);
                } else {
                    // decimal64 + decimal64 = decimal128
                    isOverflow = AddCheckedOverflow(tmpResult, int128_t(dataPtr[i]), tmpResult);
                }
                *res = OutDecimalType(tmpResult);
            } else {
                // decimal64 + decimal64 = decimal64
                tmpResult = state.val;
                isOverflow = __builtin_add_overflow(tmpResult, dataPtr[i], &tmpResult);
                state.val = OutDecimalType(tmpResult);
            }
            state.count += 1;
        }

        if (isOverflow) {
            state.count = -1;
        }
    }
}

template <typename InDecimalType, typename OutDecimalType, typename ResultIntType>
VECTORIZE_LOOP NO_INLINE void AddDecimalRowIndex(AggregateState &state, const InDecimalType *__restrict dataPtr,
    int64_t cnt, int32_t rowIdx)
{
    bool isOverflow = false;
    if (state.count < 0) {
        // cur state overflow, so no need to aggregate
        return;
    }

    ResultIntType tmpResult = 0;
    if constexpr (std::is_same_v<OutDecimalType, Decimal128>) {
        auto res = reinterpret_cast<OutDecimalType *>(state.val);
        tmpResult = res->ToInt128();
        if constexpr (std::is_same_v<InDecimalType, Decimal128>) {
            // decimal128 + decimal128 = decimal128
            isOverflow = AddCheckedOverflow(tmpResult, dataPtr[rowIdx].ToInt128(), tmpResult);
        } else {
            // decimal64 + decimal64 = decimal128
            isOverflow = AddCheckedOverflow(tmpResult, int128_t(dataPtr[rowIdx]), tmpResult);
        }
        *res = OutDecimalType(tmpResult);
    } else {
        // decimal64 + decimal64 = decimal64
        tmpResult = state.val;
        isOverflow = __builtin_add_overflow(tmpResult, dataPtr[rowIdx], &tmpResult);
        state.val = OutDecimalType(tmpResult);
    }
    state.count += cnt;

    if (isOverflow) {
        state.count = -1;
    }
}

/**
 * SUM agg data type
 * input: decimal
 * middle: decimal+boolean(isEmpty)
 * final: decimal
 */
template <DataTypeId InDecimalId, DataTypeId OutDecimalId> class SumSparkDecimalAggregator : public TypedAggregator {
public:
    using ResultType = typename AggNativeAndVectorType<OutDecimalId>::type;
    using ResultIntType = std::conditional_t<std::is_same_v<ResultType, Decimal128>, int128_t, int64_t>;
    using InRawType = typename AggNativeAndVectorType<InDecimalId>::type;
    SumSparkDecimalAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : TypedAggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels, inputRaw, outputPartial,
        isOverflowAsNull)
    {}

    SumSparkDecimalAggregator(FunctionType aggregateType, const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
        : TypedAggregator(aggregateType, inputTypes, outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull)
    {}

    ~SumSparkDecimalAggregator() override = default;
    void GetSpillType(std::vector<DataTypePtr> &spillTypes) override
    {
        if constexpr (InDecimalId == OMNI_DECIMAL64) {
            spillTypes.push_back(std::make_shared<DataType>(OutDecimalId));
        } else {
            spillTypes.push_back(std::make_shared<DataType>(OMNI_DECIMAL128));
        }
        spillTypes.push_back(std::make_shared<DataType>(OMNI_LONG));
    }

    void ExtractSpillValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        auto spillValueVec = static_cast<Vector<ResultType> *>(vectors[0]);
        auto spillCountVec = static_cast<Vector<long> *>(vectors[1]);
        if (state.count == 0) {
            spillValueVec->SetNull(rowIndex);
            spillCountVec->SetValue(rowIndex, state.count);
            return;
        }

        if constexpr (std::is_same_v<ResultType, Decimal128>) {
            spillValueVec->SetValue(rowIndex, *reinterpret_cast<Decimal128 *>(state.val));
        } else {
            spillValueVec->SetValue(rowIndex, state.val);
        }

        spillCountVec->SetValue(rowIndex, state.count);
    }

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        BaseVector *vector = vectors[0];

        int128_t decodedDec = 0;
        if constexpr (std::is_same_v<ResultType, Decimal128>) {
            decodedDec = reinterpret_cast<Decimal128 *>(state.val)->ToInt128();
        } else {
            decodedDec = state.val;
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
        if constexpr (std::is_same_v<ResultType, Decimal128>) {
            auto val = arenaAllocator->Allocate(sizeof(Decimal128));
            new (val)Decimal128(0, 0);
            state.val = reinterpret_cast<int64_t>(val);
        } else {
            state.val = 0;
        }
        state.count = 0;
    }

    void ProcessGroupInternalFinal(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
        BaseVector *dataVector, const int32_t rowOffset, const uint8_t *nullMap)
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

    void ProcessGroupAfterSpill(AggregateState &state, VectorBatch *vectorBatch, int32_t &vectorIndex,
        int32_t rowIdx) override
    {
        auto sumVector = vectorBatch->Get(vectorIndex++);
        auto sum = reinterpret_cast<ResultType *>(GetValuesFromVector<OutDecimalId>(sumVector));
        auto countVector = vectorBatch->Get(vectorIndex++);
        auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(countVector));

        int64_t cnt = cntPtr[rowIdx];
        if (cnt == 0 || sumVector->IsNull(rowIdx)) {
            return;
        } else if (inputRaw) {
            if constexpr (std::is_same_v<ResultType, Decimal128>) {
                SumOp<ResultType, ResultType, false>(reinterpret_cast<ResultType *>(state.val), state.count,
                    sum[rowIdx], cnt);
            } else {
                SumOp<ResultType, ResultType, false>(reinterpret_cast<ResultType *>(&state.val), state.count,
                    sum[rowIdx], cnt);
            }
        } else {
            AddDecimalRowIndex<ResultType, ResultType, ResultIntType>(state, sum, cnt, rowIdx);
        }
    }

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *vector,
        const int32_t rowOffset, const uint8_t *nullMap)
    {
        if (inputRaw) {
            if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
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
                auto *indexMap = GetIdsFromDict<InDecimalId>(vector) + rowOffset;
                if (nullMap == nullptr) {
                    AddDictUseRowIndex<InRawType, ResultType, SumOp<InRawType, ResultType>>(rowStates, aggIdx, ptr,
                        indexMap);
                } else {
                    AddDictConditionalUseRowIndex<InRawType, ResultType,
                        SumConditionalOp<InRawType, ResultType, false>>(rowStates, aggIdx, ptr, nullMap, indexMap);
                }
            }
        } else {
            ProcessGroupInternalFinal(rowStates, aggIdx, vector, rowOffset, nullMap);
        }
    }

    void ProcessSingleInternalFinal(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const uint8_t *conditionMap)
    {
        auto *ptr = reinterpret_cast<InRawType *>(GetValuesFromVector<InDecimalId>(vector));

        auto *emptyVector = curVectorBatch->Get(channels[1]);
        auto *emptyPtr = reinterpret_cast<bool *>(GetValuesFromVector<type::OMNI_BOOLEAN>(emptyVector));
        ptr += rowOffset;
        emptyPtr += rowOffset;

        if (conditionMap == nullptr) {
            for (int32_t i = 0; i < rowCount; ++i) {
                if (state.count < 0) {
                    // means overflow in final stage, no need to calculate remaining data
                    break;
                }
                if (not emptyPtr[i]) {
                    // the emptyPtr here means partial result is null
                    if constexpr (std::is_same_v<ResultType, Decimal128>) {
                        SumOp<InRawType, ResultType>(reinterpret_cast<ResultType *>(state.val), state.count, ptr[i],
                            1LL);
                    } else {
                        SumOp<InRawType, ResultType>(reinterpret_cast<ResultType *>(&state.val), state.count, ptr[i],
                            1LL);
                    }
                }
            }
        } else {
            for (int32_t i = 0; i < rowCount; ++i) {
                if (state.count < 0) {
                    // means overflow in final stage, no need to calculate remaining data
                    break;
                }
                if (not conditionMap[i]) {
                    if (not emptyPtr[i]) {
                        // the emptyPtr here means partial result is null
                        if constexpr (std::is_same_v<ResultType, Decimal128>) {
                            SumOp<InRawType, ResultType>(reinterpret_cast<ResultType *>(state.val), state.count, ptr[i],
                                1LL);
                        } else {
                            SumOp<InRawType, ResultType>(reinterpret_cast<ResultType *>(&state.val), state.count,
                                ptr[i], 1LL);
                        }
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
        const int32_t rowCount, const uint8_t *nullMap)
    {
        if (inputRaw) {
            if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
                auto *ptr = reinterpret_cast<InRawType *>(GetValuesFromVector<InDecimalId>(vector));
                ptr += rowOffset;
                if (nullMap == nullptr) {
                    if constexpr (std::is_same_v<ResultType, Decimal128>) {
                        Add<InRawType, ResultType, SumOp<InRawType, ResultType>>(
                            reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount);
                    } else {
                        Add<InRawType, ResultType, SumOp<InRawType, ResultType>>(
                            reinterpret_cast<ResultType *>(&state.val), state.count, ptr, rowCount);
                    }
                } else {
                    if constexpr (std::is_same_v<ResultType, Decimal128>) {
                        AddConditional<InRawType, ResultType, SumConditionalOp<InRawType, ResultType, false>>(
                            reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, nullMap);
                    } else {
                        AddConditional<InRawType, ResultType, SumConditionalOp<InRawType, ResultType, false>>(
                            reinterpret_cast<ResultType *>(&state.val), state.count, ptr, rowCount, nullMap);
                    }
                }
            } else {
                auto *ptr = reinterpret_cast<InRawType *>(GetValuesFromDict<InDecimalId>(vector));
                auto *indexMap = GetIdsFromDict<InDecimalId>(vector) + rowOffset;
                if (nullMap == nullptr) {
                    if constexpr (std::is_same_v<ResultType, Decimal128>) {
                        AddDict<InRawType, ResultType, SumOp<InRawType, ResultType>>(
                            reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, indexMap);
                    } else {
                        AddDict<InRawType, ResultType, SumOp<InRawType, ResultType>>(
                            reinterpret_cast<ResultType *>(&state.val), state.count, ptr, rowCount, indexMap);
                    }
                } else {
                    if constexpr (std::is_same_v<ResultType, Decimal128>) {
                        AddDictConditional<InRawType, ResultType, SumConditionalOp<InRawType, ResultType, false>>(
                            reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, nullMap, indexMap);
                    } else {
                        AddDictConditional<InRawType, ResultType, SumConditionalOp<InRawType, ResultType, false>>(
                            reinterpret_cast<ResultType *>(&state.val), state.count, ptr, rowCount, nullMap, indexMap);
                    }
                }
            }
        } else {
            ProcessSingleInternalFinal(state, vector, rowOffset, rowCount, nullMap);
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

    // Set decimal val to output vector in Extract function. The outputType is either OMNI_DECIMAL64 or OMNI_DECIMAL128.
    void SetValToVector(BaseVector *vector, int32_t rowIndex, int32_t outputType, int128_t &deciVal)
    {
        if constexpr (std::is_same_v<ResultType, Decimal128>) {
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
