/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: Average aggregate
 */

#include "average_aggregator.h"
#include "operator/aggregation/vector_getter.h"

namespace omniruntime {
namespace op {
template <DataTypeId IN_ID, DataTypeId OUT_ID>
template <bool PARTIAL_OUT>
void AverageAggregator<IN_ID, OUT_ID>::ExtractValuesFunction(const AggregateState &state,
    std::vector<BaseVector *> &vectors, int32_t rowIndex)
{
    if constexpr (PARTIAL_OUT) {
        if constexpr (OUT_ID == OMNI_VARCHAR) {
            SumAggregator<IN_ID, OUT_ID>::ExtractValues(state, vectors, rowIndex);
        } else if constexpr (OUT_ID == OMNI_CONTAINER) {
            OutType result{};
            auto *vector = static_cast<ContainerVector *>(vectors[0]);
            auto *doubleVector = reinterpret_cast<OutVector *>(vector->GetValue(0));
            auto *longVector = reinterpret_cast<Vector<int64_t> *>(vector->GetValue(1));

            bool overflow = state.count < 0;
            if (state.count > 0 && state.val != nullptr) {
                result = this->template CastWithOverflow<ResultType, OutType>(
                    *reinterpret_cast<ResultType *>(state.val), overflow);
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
        OutType result{};
        auto v = static_cast<OutVector *>(vectors[0]);
        bool overflow = state.count < 0;
        if (state.count > 0 && state.val != nullptr) {
            if constexpr (std::is_same_v<ResultType, Decimal128>) {
                Decimal128Wrapper result128 = Decimal128Wrapper(*reinterpret_cast<Decimal128 *>(state.val))
                                                  .Divide(Decimal128Wrapper(state.count), 0);
                result = this->template CastWithOverflow<Decimal128, OutType>(result128.ToDecimal128(), overflow);
            } else {
                // Result type is either double or int64, which for both cases we generate double avgResult;
                double avgResult =
                    static_cast<double>(*reinterpret_cast<ResultType *>(state.val)) / static_cast<double>(state.count);
                result = this->template CastWithOverflow<double, OutType>(avgResult, overflow);
            }
        }

        v->SetValue(rowIndex, result);
        if (overflow) {
            this->SetNullOrThrowException(v, rowIndex, "average_aggregator overflow.");
        } else if (state.count == 0 || state.val == nullptr) {
            v->SetNull(rowIndex);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void AverageAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex)
{
    (this->*extractValuesFuncPointer)(state, vectors, rowIndex);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void AverageAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState &state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap)
{
    if constexpr (IN_ID == OMNI_CONTAINER) {
        if (state.val == nullptr) {
            this->InitState(state);
        }
        auto *res = reinterpret_cast<ResultType *>(state.val);

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
                AddAvg<InType, ResultType, SumOp<InType, ResultType>>(res, state.count, ptr, cntPtr, rowCount);
            } else {
                if constexpr (std::is_floating_point_v<InType>) {
                    AvgConditionalFloat<InType, ResultType, false>(res, state.count, ptr, cntPtr, rowCount, nullMap);
                } else {
                    AddConditionalAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(res, state.count,
                        ptr, cntPtr, rowCount, nullMap);
                }
            }
        } else {
            auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<OMNI_DOUBLE>(sumVector));
            auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromDict<OMNI_LONG>(cntVector));
            auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
            if (nullMap == nullptr) {
                AddDictAvg<InType, ResultType, SumOp<InType, ResultType>>(res, state.count, ptr, cntPtr, rowCount,
                    indexMap);
            } else {
                AddDictConditionalAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(res, state.count,
                    ptr, cntPtr, rowCount, nullMap, indexMap);
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
AverageAggregator<IN_ID, OUT_ID>::AverageAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
    : SumAggregator<IN_ID, OUT_ID>(OMNI_AGGREGATION_TYPE_AVG, inputTypes, outputTypes, channels, inputRaw,
    outputPartial, isOverflowAsNull)
{
    // varchar only in partial stage
    if constexpr (OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CONTAINER) {
        extractValuesFuncPointer = &AverageAggregator<IN_ID, OUT_ID>::ExtractValuesFunction<true>;
    } else {
        extractValuesFuncPointer = &AverageAggregator<IN_ID, OUT_ID>::ExtractValuesFunction<false>;
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
