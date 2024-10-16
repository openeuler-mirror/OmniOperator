/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * Description: Average aggregate
 */

#include "average_aggregator.h"
#include "operator/aggregation/vector_getter.h"

namespace omniruntime {
namespace op {
template <DataTypeId IN_ID, DataTypeId OUT_ID>
template <bool PARTIAL_OUT, bool DECIMAL_PRECISION_IMPROVEMENT>
void AverageAggregator<IN_ID, OUT_ID>::ExtractValuesFunction(const AggregateState &state,
    std::vector<BaseVector *> &vectors, int32_t rowIndex)
{
    if constexpr (PARTIAL_OUT) {
        if constexpr (OUT_ID == OMNI_VARCHAR) {
            SumAggregator<IN_ID, OUT_ID>::ExtractValues(state, vectors, rowIndex);
        } else if constexpr (OUT_ID == OMNI_CONTAINER) {
            OutType result {};
            auto *vector = static_cast<ContainerVector *>(vectors[0]);
            auto *doubleVector = reinterpret_cast<OutVector *>(vector->GetValue(0));
            auto *longVector = reinterpret_cast<Vector<int64_t> *>(vector->GetValue(1));

            bool overflow = state.count < 0;
            if (state.count > 0) {
                result = this->template CastWithOverflowEntry<ResultType, OutType>(state.val, overflow);
            }

            doubleVector->SetValue(rowIndex, result);
            longVector->SetValue(rowIndex, overflow ? 0 : state.count);

            if (overflow && !this->IsOverflowAsNull()) {
                throw OmniException("OPERATOR_RUNTIME_ERROR", "average_aggregator overflow.");
            }
        } else {
            throw OmniException("Unreachable code", "Reached unreachable code in average aggregator extract partial");
        }
    } else {
        OutType result {};
        auto v = static_cast<OutVector *>(vectors[0]);
        bool overflow = state.count < 0;
        if (state.count > 0) {
            if constexpr (std::is_same_v<ResultType, Decimal128>) {
                DivideWithOverflow<DECIMAL_PRECISION_IMPROVEMENT>(state, result, overflow);
            } else if constexpr (std::is_same_v<ResultType, double>) {
                // Result type is double, we generate double avgResult
                double avgResult = *reinterpret_cast<double *>(state.val) / static_cast<double>(state.count);
                result = this->template CastWithOverflow<double, OutType>(avgResult, overflow);
            } else {
                // Result type is int64, we generate double avgResult
                double avgResult =
                    static_cast<double>(static_cast<ResultType>(state.val)) / static_cast<double>(state.count);
                result = this->template CastWithOverflow<double, OutType>(avgResult, overflow);
            }
        }

        v->SetValue(rowIndex, result);
        if (overflow) {
            this->SetNullOrThrowException(v, rowIndex, "average_aggregator overflow.");
        } else if (state.count == 0) {
            v->SetNull(rowIndex);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
template <bool PARTIAL_OUT, bool DECIMAL_PRECISION_IMPROVEMENT, bool IS_OVERFLOW_AS_NULL>
void AverageAggregator<IN_ID, OUT_ID>::ExtractValuesBatchInternal(std::vector<AggregateState *> &groupStates,
    const size_t aggIdx, std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount)
{
    if constexpr (PARTIAL_OUT) {
        if constexpr (OUT_ID == OMNI_VARCHAR) {
            SumAggregator<IN_ID, OUT_ID>::ExtractValuesBatch(groupStates, aggIdx, vectors, rowOffset, rowCount);
        } else if constexpr (OUT_ID == OMNI_CONTAINER) {
            auto *vector = static_cast<ContainerVector *>(vectors[0]);
            auto *doubleVector = reinterpret_cast<OutVector *>(vector->GetValue(0));
            auto *longVector = reinterpret_cast<Vector<int64_t> *>(vector->GetValue(1));

            for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                auto &state = groupStates[rowIndex][aggIdx];
                bool overflow = state.count < 0;
                OutType result {};
                if (state.count > 0) {
                    result = this->template CastWithOverflowEntry<ResultType, OutType>(state.val, overflow);
                }
                doubleVector->SetValue(rowIndex, result);
                longVector->SetValue(rowIndex, overflow ? 0 : state.count);

                if (overflow && !this->IsOverflowAsNull()) {
                    throw OmniException("OPERATOR_RUNTIME_ERROR", "average_aggregator overflow.");
                }
            }
        } else {
            throw OmniException("Unreachable code", "Reached unreachable code in average aggregator extract partial");
        }
    } else {
        auto v = static_cast<OutVector *>(vectors[0]);
        for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            auto &state = groupStates[rowIndex][aggIdx];
            bool isOverflow = state.count < 0;
            bool isEmpty = state.count == 0;
            if (isOverflow) {
                this->SetNullOrThrowException(v, rowIndex, "average_aggregator overflow.");
            } else if (isEmpty) {
                v->SetNull(rowIndex);
            } else {
                OutType result {};
                if constexpr (std::is_same_v<ResultType, Decimal128>) {
                    DivideWithOverflow<DECIMAL_PRECISION_IMPROVEMENT>(state, result, isOverflow);
                } else if constexpr (std::is_same_v<ResultType, double>) {
                    // Result type is double, we generate double avgResult
                    double avgResult = *reinterpret_cast<double *>(state.val) / static_cast<double>(state.count);
                    result = this->template CastWithOverflow<double, OutType>(avgResult, isOverflow);
                } else {
                    // Result type is int64, we generate double avgResult
                    auto avgResult =
                        static_cast<double>(static_cast<ResultType>(state.val)) / static_cast<double>(state.count);
                    result = this->template CastWithOverflow<double, OutType>(avgResult, isOverflow);
                }
                if (isOverflow) {
                    this->SetNullOrThrowException(v, rowIndex, "average_aggregator overflow.");
                } else {
                    v->SetValue(rowIndex, result);
                }
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
template <bool DECIMAL_PRECISION_IMPROVEMENT>
void AverageAggregator<IN_ID, OUT_ID>::DivideWithOverflow(const AggregateState &state, OutType &result, bool &overflow)
{
    int128_t result128 = reinterpret_cast<Decimal128 *>(state.val)->ToInt128();
    if constexpr (DECIMAL_PRECISION_IMPROVEMENT && std::is_same_v<OutType, Decimal128>) {
        auto dividend = this->template CastWithOverflow<Decimal128, OutType>(Decimal128(result128), overflow);
        DivideRoundUp(dividend.ToInt128(), static_cast<int128_t>(state.count), result128);
        result = Decimal128(result128);
    } else {
        DivideRoundUp(result128, static_cast<int128_t>(state.count), result128);
        result = this->template CastWithOverflow<Decimal128, OutType>(Decimal128(result128), overflow);
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void AverageAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex)
{
    (this->*extractValuesFuncPointer)(state, vectors, rowIndex);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void AverageAggregator<IN_ID, OUT_ID>::ExtractValuesBatch(std::vector<AggregateState *> &groupStates,
    const size_t aggIdx, std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount)
{
    // varchar only in partial stage
    if constexpr (OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CONTAINER) {
        if (this->IsOverflowAsNull()) {
            ExtractValuesBatchInternal<true, false, true>(groupStates, aggIdx, vectors, rowOffset, rowCount);
        } else {
            ExtractValuesBatchInternal<true, false, false>(groupStates, aggIdx, vectors, rowOffset, rowCount);
        }
    } else {
        if (ConfigUtil::GetSupportDecimalPrecisionImprovementRule() ==
            SupportDecimalPrecisionImprovementRule::IS_SUPPORT) {
            if (this->IsOverflowAsNull()) {
                ExtractValuesBatchInternal<false, true, true>(groupStates, aggIdx, vectors, rowOffset, rowCount);
            } else {
                ExtractValuesBatchInternal<false, true, false>(groupStates, aggIdx, vectors, rowOffset, rowCount);
            }
        } else {
            if (this->IsOverflowAsNull()) {
                ExtractValuesBatchInternal<false, false, true>(groupStates, aggIdx, vectors, rowOffset, rowCount);
            } else {
                ExtractValuesBatchInternal<false, false, false>(groupStates, aggIdx, vectors, rowOffset, rowCount);
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> std::vector<DataTypePtr> AverageAggregator<IN_ID, OUT_ID>::GetSpillType()
{
    std::vector<DataTypePtr> spillTypes;
    if constexpr (IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG) {
        spillTypes.emplace_back(std::make_shared<DataType>(OMNI_LONG));
    } else if constexpr (IN_ID == OMNI_DOUBLE || IN_ID == OMNI_CONTAINER) {
        spillTypes.emplace_back(std::make_shared<DataType>(OMNI_DOUBLE));
    } else {
        spillTypes.emplace_back(std::make_shared<DataType>(OMNI_DECIMAL128));
    }
    spillTypes.emplace_back(std::make_shared<DataType>(OMNI_LONG));
    return spillTypes;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void AverageAggregator<IN_ID, OUT_ID>::ExtractValuesForSpill(std::vector<AggregateState *> &groupStates,
    const size_t aggIdx, std::vector<BaseVector *> &vectors)
{
    auto spillValueVec = static_cast<Vector<ResultType> *>(vectors[0]);
    auto spillCountVec = reinterpret_cast<Vector<int64_t> *>(vectors[1]);

    auto rowCount = static_cast<int32_t>(groupStates.size());
    for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        auto &state = groupStates[rowIndex][aggIdx];
        auto count = state.count;
        bool isOverflow = count < 0;
        bool isEmpty = count == 0;

        if (isOverflow) {
            this->SetNullOrThrowException(spillValueVec, rowIndex, "average_aggregator overflow.");
            // set -1 to count vector if it is overflow
            spillCountVec->SetValue(rowIndex, -1);
        } else if (isEmpty) {
            spillValueVec->SetNull(rowIndex);
            // set null for empty group(all rows are NULL) when spill to ensure skip empty group when unspill
            spillCountVec->SetValue(rowIndex, 0);
        } else {
            ResultType result;
            if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType>) {
                result = *reinterpret_cast<ResultType *>(state.val);
            } else {
                result = static_cast<ResultType>(state.val);
            }
            spillValueVec->SetValue(rowIndex, result);
            spillCountVec->SetValue(rowIndex, count);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void AverageAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState &state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap)
{
    if constexpr (IN_ID == OMNI_CONTAINER) {
        // when input is not raw, vector is container with <double, long> columns for <sum, count>
        auto v = static_cast<ContainerVector *>(vector);
        auto *sumVector = reinterpret_cast<InVector *>(v->GetValue(0));

        auto *cntVector = reinterpret_cast<Vector<int64_t> *>(v->GetValue(1));

        if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
            auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<OMNI_DOUBLE>(sumVector));
            auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(cntVector));
            ptr += rowOffset;
            cntPtr += rowOffset;
            if (nullMap == nullptr) {
                if constexpr (std::is_floating_point_v<ResultType> || std::is_same_v<ResultType, Decimal128>) {
                    AddAvg<InType, ResultType, SumOp<InType, ResultType>>(reinterpret_cast<ResultType *>(state.val),
                        state.count, ptr, cntPtr, rowCount);
                } else {
                    AddAvg<InType, ResultType, SumOp<InType, ResultType>>(reinterpret_cast<ResultType *>(&state.val),
                        state.count, ptr, cntPtr, rowCount);
                }
            } else {
                if constexpr (std::is_floating_point_v<InType>) {
                    if constexpr (std::is_floating_point_v<ResultType> || std::is_same_v<ResultType, Decimal128>) {
                        AvgConditionalFloat<InType, ResultType, false>(reinterpret_cast<ResultType *>(state.val),
                            state.count, ptr, cntPtr, rowCount, nullMap);
                    } else {
                        AvgConditionalFloat<InType, ResultType, false>(reinterpret_cast<ResultType *>(&state.val),
                            state.count, ptr, cntPtr, rowCount, nullMap);
                    }
                } else {
                    if constexpr (std::is_floating_point_v<ResultType> || std::is_same_v<ResultType, Decimal128>) {
                        AddConditionalAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(
                            reinterpret_cast<ResultType *>(state.val), state.count, ptr, cntPtr, rowCount, nullMap);
                    } else {
                        AddConditionalAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(
                            reinterpret_cast<ResultType *>(&state.val), state.count, ptr, cntPtr, rowCount, nullMap);
                    }
                }
            }
        } else {
            auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<OMNI_DOUBLE>(sumVector));
            auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromDict<OMNI_LONG>(cntVector));
            auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
            if (nullMap == nullptr) {
                if constexpr (std::is_floating_point_v<ResultType> || std::is_same_v<ResultType, Decimal128>) {
                    AddDictAvg<InType, ResultType, SumOp<InType, ResultType>>(reinterpret_cast<ResultType *>(state.val),
                        state.count, ptr, cntPtr, rowCount, indexMap);
                } else {
                    AddDictAvg<InType, ResultType, SumOp<InType, ResultType>>(
                        reinterpret_cast<ResultType *>(&state.val), state.count, ptr, cntPtr, rowCount, indexMap);
                }
            } else {
                if constexpr (std::is_floating_point_v<ResultType> || std::is_same_v<ResultType, Decimal128>) {
                    AddDictConditionalAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(
                        reinterpret_cast<ResultType *>(state.val), state.count, ptr, cntPtr, rowCount, nullMap,
                        indexMap);
                } else {
                    AddDictConditionalAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(
                        reinterpret_cast<ResultType *>(&state.val), state.count, ptr, cntPtr, rowCount, nullMap,
                        indexMap);
                }
            }
        }
    } else {
        SumAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(state, vector, rowOffset, rowCount, nullMap);
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void AverageAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates,
    const size_t aggIdx, BaseVector *vector, const int32_t rowOffset, const uint8_t *nullMap)
{
    if constexpr (IN_ID == OMNI_CONTAINER) {
        // when input is not raw, vector is container with <double, long> columns for <sum, count>
        auto *v = static_cast<ContainerVector *>(vector);
        auto *sumVector = reinterpret_cast<InVector *>(v->GetValue(0));
        auto *cntVector = reinterpret_cast<Vector<int64_t> *>(v->GetValue(1));

        if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
            auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<OMNI_DOUBLE>(sumVector));
            auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(cntVector));
            ptr += rowOffset;
            cntPtr += rowOffset;
            if (nullMap == nullptr) {
                AddUseRowIndexAvg<InType, ResultType, SumOp<InType, ResultType>>(rowStates, aggIdx, ptr, cntPtr);
            } else {
                // Reza: can we use customize float operation similar to sumConditionalFloat
                AddConditionalUseRowIndexAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(rowStates,
                    aggIdx, ptr, cntPtr, nullMap);
            }
        } else {
            auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<OMNI_DOUBLE>(sumVector));
            auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromDict<OMNI_LONG>(cntVector));
            auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
            if (nullMap == nullptr) {
                AddDictUseRowIndexAvg<InType, ResultType, SumOp<InType, ResultType>>(rowStates, aggIdx, ptr, cntPtr,
                    indexMap);
            } else {
                AddDictConditionalUseRowIndexAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(
                    rowStates, aggIdx, ptr, cntPtr, nullMap, indexMap);
            }
        }
    } else {
        SumAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(rowStates, aggIdx, vector, rowOffset, nullMap);
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void AverageAggregator<IN_ID, OUT_ID>::ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount,
    const size_t aggIdx, int32_t &vectorIndex)
{
    auto firstVecIdx = vectorIndex++;
    auto secondVecIdx = vectorIndex++;
    for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
        auto &row = unspillRows[rowIdx];
        auto batch = row.batch;
        auto index = row.rowIdx;
        auto &state = row.state[aggIdx];
        auto countVector = static_cast<Vector<int64_t> *>(batch->Get(secondVecIdx));
        auto count = countVector->GetValue(index);
        if (count < 0) {
            // we set -1 in overflow case
            state.count = -1;
        } else if (count == 0) {
            // we skipped in empty group case
            continue;
        } else {
            auto sumVector = static_cast<Vector<ResultType> *>(batch->Get(firstVecIdx));
            auto value = sumVector->GetValue(index);
            if constexpr (std::is_floating_point_v<ResultType> || std::is_same_v<ResultType, Decimal128>) {
                SumOp<ResultType, ResultType>(reinterpret_cast<ResultType *>(state.val), state.count, value, count);
            } else {
                SumOp<ResultType, ResultType>(reinterpret_cast<ResultType *>(&state.val), state.count, value, count);
            }
        }
    }
}

// olk interface
template <DataTypeId IN_ID, DataTypeId OUT_ID>
void AverageAggregator<IN_ID, OUT_ID>::ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
    const uint8_t *nullMap, const bool aggFilter)
{
    if constexpr (OUT_ID == OMNI_VARCHAR) {
        SumAggregator<IN_ID, OUT_ID>::ProcessAlignAggSchema(result, originVector, nullMap, aggFilter);
    } else if constexpr (OUT_ID == OMNI_CONTAINER) {
        if (originVector->GetEncoding() == OMNI_DICTIONARY) {
            ProcessAlignAggSchemaInternal<Vector<DictionaryContainer<InType>>>(result, originVector, nullMap);
        } else {
            ProcessAlignAggSchemaInternal<Vector<InType>>(result, originVector, nullMap);
        }
    } else {
        throw OmniException("Unreachable code", "Reached unreachable code in average aggregator extract partial");
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
template <typename T>
void AverageAggregator<IN_ID, OUT_ID>::ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector,
    const uint8_t *nullMap)
{
    int rowCount = originVector->GetSize();
    auto containerVector = reinterpret_cast<Vector<double> *>(VectorHelper::CreateFlatVector(OMNI_DOUBLE, 2));
    auto sumVector = reinterpret_cast<Vector<double> *>(VectorHelper::CreateFlatVector(OMNI_DOUBLE, rowCount));
    auto countVector = reinterpret_cast<Vector<int64_t> *>(VectorHelper::CreateFlatVector(OMNI_LONG, rowCount));

    auto vector = reinterpret_cast<T *>(originVector);
    if (nullMap != nullptr) {
        for (int index = 0; index < rowCount; ++index) {
            if (nullMap[index]) {
                sumVector->SetValue(index, 0);
                countVector->SetValue(index, 0);
            } else {
                InType val = vector->GetValue(index);
                bool overflow = false;
                OutType out = this->template CastWithOverflow<InType, OutType>(static_cast<InType>(val), overflow);
                sumVector->SetValue(index, out);
                countVector->SetValue(index, 1);
            }
        }
    } else {
        for (int index = 0; index < rowCount; ++index) {
            InType val = vector->GetValue(index);
            bool overflow = false;
            OutType out = this->template CastWithOverflow<InType, OutType>(static_cast<InType>(val), overflow);
            sumVector->SetValue(index, out);
            countVector->SetValue(index, 1);
        }
    }

    containerVector->SetValue(0, reinterpret_cast<uintptr_t>(sumVector));
    containerVector->SetValue(1, reinterpret_cast<uintptr_t>(countVector));
    result->Append(containerVector);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
AverageAggregator<IN_ID, OUT_ID>::AverageAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
    : SumAggregator<IN_ID, OUT_ID>(OMNI_AGGREGATION_TYPE_AVG, inputTypes, outputTypes, channels, inputRaw,
    outputPartial, isOverflowAsNull)
{
    // varchar only in partial stage
    if constexpr (OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CONTAINER) {
        extractValuesFuncPointer = &AverageAggregator<IN_ID, OUT_ID>::ExtractValuesFunction<true, false>;
    } else {
        if (ConfigUtil::GetSupportDecimalPrecisionImprovementRule() ==
            SupportDecimalPrecisionImprovementRule::IS_SUPPORT) {
            extractValuesFuncPointer = &AverageAggregator<IN_ID, OUT_ID>::ExtractValuesFunction<false, true>;
        } else {
            extractValuesFuncPointer = &AverageAggregator<IN_ID, OUT_ID>::ExtractValuesFunction<false, false>;
        }
    }
}

// Explicit template instantiation
// Defining templated aggregators in header file consume a lot of memory during compilation
// since, compiler needs to generate each individual template instance wherever aggregator header is include
// to reduce time and memory usage during compilation moved templated aggregator implementation into .cpp files
// and used explicit template instantiation to generate template instances
template class AverageAggregator<OMNI_SHORT, OMNI_DOUBLE>;

template class AverageAggregator<OMNI_SHORT, OMNI_DECIMAL128>;

template class AverageAggregator<OMNI_SHORT, OMNI_DECIMAL64>;

template class AverageAggregator<OMNI_SHORT, OMNI_CONTAINER>;

template class AverageAggregator<OMNI_INT, OMNI_DOUBLE>;

template class AverageAggregator<OMNI_INT, OMNI_DECIMAL128>;

template class AverageAggregator<OMNI_INT, OMNI_DECIMAL64>;

template class AverageAggregator<OMNI_INT, OMNI_CONTAINER>;

template class AverageAggregator<OMNI_LONG, OMNI_DOUBLE>;

template class AverageAggregator<OMNI_LONG, OMNI_DECIMAL128>;

template class AverageAggregator<OMNI_LONG, OMNI_DECIMAL64>;

template class AverageAggregator<OMNI_LONG, OMNI_CONTAINER>;

template class AverageAggregator<OMNI_DOUBLE, OMNI_DOUBLE>;

template class AverageAggregator<OMNI_DOUBLE, OMNI_DECIMAL128>;

template class AverageAggregator<OMNI_DOUBLE, OMNI_DECIMAL64>;

template class AverageAggregator<OMNI_DOUBLE, OMNI_CONTAINER>;

template class AverageAggregator<OMNI_DECIMAL128, OMNI_DOUBLE>;

template class AverageAggregator<OMNI_DECIMAL128, OMNI_DECIMAL128>;

template class AverageAggregator<OMNI_DECIMAL128, OMNI_DECIMAL64>;

template class AverageAggregator<OMNI_DECIMAL128, OMNI_VARCHAR>;

template class AverageAggregator<OMNI_DECIMAL64, OMNI_DOUBLE>;

template class AverageAggregator<OMNI_DECIMAL64, OMNI_DECIMAL128>;

template class AverageAggregator<OMNI_DECIMAL64, OMNI_DECIMAL64>;

template class AverageAggregator<OMNI_DECIMAL64, OMNI_VARCHAR>;

template class AverageAggregator<OMNI_VARCHAR, OMNI_DOUBLE>;

template class AverageAggregator<OMNI_VARCHAR, OMNI_DECIMAL128>;

template class AverageAggregator<OMNI_VARCHAR, OMNI_DECIMAL64>;

template class AverageAggregator<OMNI_VARCHAR, OMNI_VARCHAR>;

template class AverageAggregator<OMNI_CONTAINER, OMNI_DOUBLE>;

template class AverageAggregator<OMNI_CONTAINER, OMNI_DECIMAL128>;

template class AverageAggregator<OMNI_CONTAINER, OMNI_DECIMAL64>;

template class AverageAggregator<OMNI_CONTAINER, OMNI_CONTAINER>;
}
}
