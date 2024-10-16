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

            if (isOverflow) {
                state.count = -1;
            } else {
                state.count += 1;
            }
        }
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

    std::vector<DataTypePtr> GetSpillType() override
    {
        std::vector<DataTypePtr> spillTypes;
        spillTypes.emplace_back(outputTypes.GetType(0));
        return spillTypes;
    }

    template <bool isOutputPartial>
    void ExtractValuesForSpillInternal(std::vector<AggregateState *> &groupStates, const size_t aggIdx,
        std::vector<BaseVector *> &vectors)
    {
        auto spillVec = static_cast<Vector<ResultType> *>(vectors[0]);
        auto rowCount = static_cast<int32_t>(groupStates.size());
        for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            auto &state = groupStates[rowIndex][aggIdx];
            bool isOverflow = (state.count < 0);
            bool isEmpty = (state.count == 0);

            // use null and value to distinguish empty group, overflow and other normal case
            if (isEmpty) {
                // set null for empty group(all rows are NULL) when spill to ensure skip empty group when unspill
                spillVec->SetNull(rowIndex);
                int128_t emptyValue = SPILL_EMPTY_VALUE;
                SetValToVector(spillVec, rowIndex, emptyValue);
            } else if (isOverflow) {
                int128_t overflowValue = SPILL_OVERFLOW_VALUE;
                SetValToVector(spillVec, rowIndex, overflowValue);
                if constexpr (isOutputPartial) {
                    spillVec->SetNull(rowIndex);
                } else {
                    SetNullOrThrowException(spillVec, rowIndex);
                }
            } else {
                int128_t decodedDec;
                if constexpr (std::is_same_v<ResultType, Decimal128>) {
                    decodedDec = reinterpret_cast<Decimal128 *>(state.val)->ToInt128();
                } else {
                    decodedDec = state.val;
                }
                SetValToVector(spillVec, rowIndex, decodedDec);
            }
        }
    }

    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, const size_t aggIdx,
        std::vector<BaseVector *> &vectors) override
    {
        if (outputPartial) {
            ExtractValuesForSpillInternal<true>(groupStates, aggIdx, vectors);
        } else {
            ExtractValuesForSpillInternal<false>(groupStates, aggIdx, vectors);
        }
    }

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        BaseVector *vector = vectors[0];
        int128_t resultDec = 0;
        bool isOverflow = (state.count < 0);
        bool isEmpty = (state.count == 0);

        if (outputPartial) {
            if (isOverflow) {
                vector->SetNull(rowIndex);
            } else if (isEmpty) {
                SetValToVector(vector, rowIndex, resultDec);
            } else {
                int128_t decodedDec = 0;
                if constexpr (std::is_same_v<ResultType, Decimal128>) {
                    decodedDec = reinterpret_cast<Decimal128 *>(state.val)->ToInt128();
                } else {
                    decodedDec = state.val;
                }

                // only support output scale >= input scale
                // for spark, input type is always decimal. for olk, input type is varbinary and the precision
                // and scale are zero.
                int32_t scaleDiff = static_cast<DecimalDataType *>(outputTypes.GetType(0).get())->GetScale() -
                    static_cast<DecimalDataType *>(inputTypes.GetType(0).get())->GetScale();
                // rescale dividend and divisor to output scale
                isOverflow = MulCheckedOverflow(decodedDec, TenOfInt128[scaleDiff], resultDec);
                if (isOverflow) {
                    vector->SetNull(rowIndex);
                } else {
                    SetValToVector(vector, rowIndex, resultDec);
                }
            }
            BaseVector *emptyVector = vectors[1];
            reinterpret_cast<Vector<bool> *>(emptyVector)->SetValue(rowIndex, isEmpty);
        } else {
            if (isOverflow) {
                SetNullOrThrowException(vector, rowIndex);
            } else if (isEmpty) {
                vector->SetNull(rowIndex);
            } else {
                int128_t decodedDec = 0;
                if constexpr (std::is_same_v<ResultType, Decimal128>) {
                    decodedDec = reinterpret_cast<Decimal128 *>(state.val)->ToInt128();
                } else {
                    decodedDec = state.val;
                }

                // only support output scale >= input scale
                // for spark, input type is always decimal. for olk, input type is varbinary and the precision
                // and scale are zero.
                int32_t scaleDiff = static_cast<DecimalDataType *>(outputTypes.GetType(0).get())->GetScale() -
                    static_cast<DecimalDataType *>(inputTypes.GetType(0).get())->GetScale();
                // rescale dividend and divisor to output scale
                isOverflow = MulCheckedOverflow(decodedDec, TenOfInt128[scaleDiff], resultDec);
                if (isOverflow) {
                    SetNullOrThrowException(vector, rowIndex);
                } else {
                    SetValToVector(vector, rowIndex, resultDec);
                }
            }
        }
    }

    template <bool OUTPUT_PARTIAL>
    void ExtractValuesBatchInternal(std::vector<AggregateState *> &groupStates, const size_t aggIdx,
        std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount)
    {
        // only support output scale >= input scale
        // for spark, input type is always decimal. for olk, input type is varbinary and the precision
        // and scale are zero.
        int32_t scaleDiff = static_cast<DecimalDataType *>(outputTypes.GetType(0).get())->GetScale() -
            static_cast<DecimalDataType *>(inputTypes.GetType(0).get())->GetScale();
        auto scaleNum = TenOfInt128[scaleDiff];
        BaseVector *vector = vectors[0];
        for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            auto &state = groupStates[rowIndex][aggIdx];
            bool isOverflow = (state.count < 0);
            bool isEmpty = (state.count == 0);
            if constexpr (OUTPUT_PARTIAL) {
                reinterpret_cast<Vector<bool> *>(vectors[1])->SetValue(rowIndex, isEmpty);
            }

            int128_t decodedDec = 0;
            if (isOverflow) {
                if constexpr (OUTPUT_PARTIAL) {
                    // partial output vector is sum, it will be set to NULL if overflowed.
                    vector->SetNull(rowIndex);
                } else {
                    SetNullOrThrowException(vector, rowIndex);
                }
            } else if (isEmpty) {
                if constexpr (OUTPUT_PARTIAL) {
                    SetValToVector(vector, rowIndex, decodedDec);
                } else {
                    vector->SetNull(rowIndex);
                }
            } else {
                if constexpr (std::is_same_v<ResultType, Decimal128>) {
                    decodedDec = reinterpret_cast<Decimal128 *>(state.val)->ToInt128();
                } else {
                    decodedDec = state.val;
                }
                int128_t resultDec;
                // rescale dividend and divisor to output scale
                isOverflow = MulCheckedOverflow(decodedDec, scaleNum, resultDec);
                if (isOverflow) {
                    if constexpr (OUTPUT_PARTIAL) {
                        // partial output vector is sum, it will be set to NULL if overflowed.
                        vector->SetNull(rowIndex);
                    } else {
                        SetNullOrThrowException(vector, rowIndex);
                    }
                } else {
                    SetValToVector(vector, rowIndex, resultDec);
                }
            }
        }
    }

    // The outputType is either OMNI_DECIMAL64 or OMNI_DECIMAL128
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, const size_t aggIdx,
        std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount) override
    {
        if (outputPartial) {
            ExtractValuesBatchInternal<true>(groupStates, aggIdx, vectors, rowOffset, rowCount);
        } else {
            ExtractValuesBatchInternal<false>(groupStates, aggIdx, vectors, rowOffset, rowCount);
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

    void InitStates(std::vector<AggregateState *> groupStates, const size_t aggIdx) override
    {
        for (auto groupState : groupStates) {
            auto &state = groupState[aggIdx];
            InitState(state);
        }
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

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, const size_t aggIdx,
        int32_t &vectorIndex) override
    {
        auto firstVecIdx = vectorIndex++;
        for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            auto &row = unspillRows[rowIdx];
            auto batch = row.batch;
            auto index = row.rowIdx;
            auto sumVector = static_cast<Vector<ResultType> *>(batch->Get(firstVecIdx));
            auto &state = row.state[aggIdx];
            auto value = sumVector->GetValue(index);
            if (!sumVector->IsNull(index)) {
                // normal case
                if constexpr (std::is_same_v<ResultType, Decimal128>) {
                    SumOp<ResultType, ResultType, false>(reinterpret_cast<ResultType *>(state.val), state.count, value,
                        1LL);
                } else {
                    SumOp<ResultType, ResultType, false>(reinterpret_cast<ResultType *>(&state.val), state.count, value,
                        1LL);
                }
            } else {
                // empty group or overflow case
                // if it is overflow we set -1
                // if it is empty group we skipped
                int128_t resultIntValue;
                if constexpr (std::is_same_v<ResultType, Decimal128>) {
                    resultIntValue = value.ToInt128();
                } else {
                    resultIntValue = value;
                }
                if (resultIntValue == SPILL_OVERFLOW_VALUE) {
                    state.count = -1;
                }
            }
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

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector, const uint8_t *nullMap,
        const bool aggFilter) override
    {
        int rowCount = originVector->GetSize();
        // opt: if InRawType and ResultType are same type, directly setValue
        if constexpr (std::is_same_v<InRawType, ResultType>) {
            auto sumVector = VectorHelper::SliceVector(originVector, 0, rowCount);
            auto emptyVector = reinterpret_cast<Vector<bool> *>(VectorHelper::CreateFlatVector(OMNI_BOOLEAN, rowCount));
            if (nullMap == nullptr) {
                bool *valueAddr = reinterpret_cast<bool *>(GetValuesFromVector<OMNI_BOOLEAN>(emptyVector));
                std::fill_n(valueAddr, rowCount, false);
            } else {
                for (int index = 0; index < rowCount; ++index) {
                    if (nullMap[index]) {
                        emptyVector->SetValue(index, true);
                    } else {
                        emptyVector->SetValue(index, false);
                    }
                }
            }
            result->Append(sumVector);
            result->Append(emptyVector);
            return;
        }

        if (originVector->GetEncoding() == OMNI_DICTIONARY) {
            ProcessAlignAggSchemaInternal<Vector<DictionaryContainer<InRawType>>>(result, originVector, nullMap);
        } else {
            ProcessAlignAggSchemaInternal<Vector<InRawType>>(result, originVector, nullMap);
        }
    }

    // logic: Template-based vector encoding type, to avoid long functions and high depth.
    template<typename T>
    void ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector, const uint8_t *nullMap)
    {
        int rowCount = originVector->GetSize();
        auto sumVector = reinterpret_cast<Vector<ResultType> *>(VectorHelper::CreateFlatVector(OutDecimalId, rowCount));
        auto emptyVector = reinterpret_cast<Vector<bool> *>(VectorHelper::CreateFlatVector(OMNI_BOOLEAN, rowCount));
        auto vector = reinterpret_cast<T *>(originVector);
        if (nullMap != nullptr) {
            for (int index = 0; index < rowCount; ++index) {
                if (nullMap[index]) {
                    sumVector->SetValue(index, (ResultType)(0));
                    emptyVector->SetValue(index, true);
                } else {
                    if constexpr (std::is_same_v<ResultType, Decimal128>) {
                        Decimal128 d = Decimal128(vector->GetValue(index));
                        sumVector->SetValue(index, d);
                    } else {
                        sumVector->SetValue(index, (ResultType)vector->GetValue(index));
                    }
                    emptyVector->SetValue(index, false);
                }
            }
        } else {
            for (int index = 0; index < rowCount; ++index) {
                if constexpr (std::is_same_v<ResultType, Decimal128>) {
                    Decimal128 d = Decimal128(vector->GetValue(index));
                    sumVector->SetValue(index, d);
                } else {
                    sumVector->SetValue(index, (ResultType)vector->GetValue(index));
                }
            }
            bool *valueAddr = reinterpret_cast<bool *>(GetValuesFromVector<OMNI_BOOLEAN>(emptyVector));
            std::fill_n(valueAddr, rowCount, false);
        }
        result->Append(sumVector);
        result->Append(emptyVector);
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
    void SetValToVector(BaseVector *vector, int32_t rowIndex, int128_t &deciVal)
    {
        if constexpr (std::is_same_v<ResultType, Decimal128>) {
            Decimal128 decimal128Val(deciVal);
            static_cast<Vector<Decimal128> *>(vector)->SetValue(rowIndex, decimal128Val);
        } else {
            int64_t longVal = static_cast<int64_t>(deciVal);
            static_cast<Vector<int64_t> *>(vector)->SetValue(rowIndex, longVal);
        }
    }

    static constexpr int128_t SPILL_EMPTY_VALUE = 0;
    static constexpr int128_t SPILL_OVERFLOW_VALUE = -1;
};
}
}

#endif // OMNI_RUNTIME_SUM_SPARK_DECIMAL_AGGREGATOR_H
