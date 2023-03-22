/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: Average aggregate
 */

#include "average_aggregator.h"

#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
#ifdef ENABLE_HMPP
template <DataTypeId IN_ID, DataTypeId OUT_ID>
void AverageAggregator<IN_ID, OUT_ID>::ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch)
{
    if constexpr (IN_ID != OMNI_LONG && IN_ID != OMNI_DECIMAL128) {
        throw OmniException("NOT SUPPORT", "Unsupported input type for avg aggregate");
    } else {
        auto vector = vectorBatch->GetVector(this->channels[0]);

        auto vectorValues = vector->GetValues();
        auto positionOffset = vector->GetPositionOffset();
        auto rowCount = vector->GetSize();
        auto nullAddr = vector->GetValueNulls();
        bool overflow = false;
        int32_t count = 0;

        HmppResult result = HMPP_STS_NO_ERR;

        if constexpr (IN_ID == OMNI_LONG) {
            LogDebug("HMPP-Agg-avg");
            double sumVal = 0;
            result =
                HMPPS_Mean_64s(static_cast<int64_t *>(static_cast<int64_t *>(vectorValues) + positionOffset), rowCount,
                static_cast<int8_t *>(static_cast<int8_t *>(nullAddr) + positionOffset), &overflow, &sumVal, &count);
            if (result != HMPP_STS_NO_ERR) {
                throw OmniException("HMPP ERROR", "avg failed for hmpp error");
            }

            if (state.val == nullptr) {
                auto valPtr = this->executionContext->GetArena()->Allocate(sizeof(ResultType));
                *reinterpret_cast<ResultType *>(valPtr) = static_cast<ResultType>(sumVal);
                state.val = valPtr;
                state.count = static_cast<int64_t>(count);
            } else {
                *(reinterpret_cast<ResultType *>(state.val)) += static_cast<ResultType>(sumVal);
                state.count += static_cast<int64_t>(count);
            }
        } else {
            // IN_ID == OMNI_DECIMAL128
            LogDebug("HMPP-Agg-avg");
            HmppDecimal128 sumVal {};
            result = HMPPS_Mean_decimal128(
                static_cast<HmppDecimal128 *>(static_cast<HmppDecimal128 *>(vectorValues) + positionOffset), rowCount,
                static_cast<int8_t *>(static_cast<int8_t *>(nullAddr) + positionOffset), &overflow, &sumVal, &count);
            if (result != HMPP_STS_NO_ERR) {
                throw OmniException("HMPP ERROR", "avg failed for hmpp error");
            }

            if (state.val == nullptr) {
                state.val = this->executionContext->GetArena()->Allocate(sizeof(Decimal128));
                *reinterpret_cast<Decimal128 *>(state.val) = Decimal128(sumVal.high, sumVal.low);
            } else {
                Decimal128Wrapper preSumVal(*(reinterpret_cast<Decimal128 *>(state.val)));
                preSumVal = preSumVal.Add(Decimal128Wrapper(sumVal.high, sumVal.low));
                *reinterpret_cast<Decimal128 *>(state.val) = preSumVal.ToDecimal128();
                overflow |= (preSumVal.IsOverflow() != OpStatus::SUCCESS);
            }
            if (overflow) {
                state.count = -1;
            } else if (state.count >= 0) {
                state.count += static_cast<int64_t>(count);
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
bool AverageAggregator<IN_ID, OUT_ID>::CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch)
{
    // just support raw input data
    if (!AverageAggregator<IN_ID, OUT_ID>::inputRaw) {
        return false;
    } else {
        // not accept dictionnary vector
        if (vectorBatch->GetVector(this->channels[0])->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            return false;
        }
        // only long or decimal128 type input support
        return this->inputTypes.GetType(0)->GetId() == OMNI_LONG ||
            this->inputTypes.GetType(0)->GetId() == OMNI_DECIMAL128;
    }
}
#endif

template <DataTypeId IN_ID, DataTypeId OUT_ID>
template <bool PARTIAL_OUT>
void AverageAggregator<IN_ID, OUT_ID>::ExtractValuesFunction(const AggregateState &state,
    std::vector<Vector *> &vectors, int32_t rowIndex)
{
    if constexpr (PARTIAL_OUT) {
        if constexpr (OUT_ID == OMNI_VARCHAR) {
            SumAggregator<IN_ID, OUT_ID>::ExtractValues(state, vectors, rowIndex);
        } else if constexpr (OUT_ID == OMNI_CONTAINER) {
            int32_t offset;
            OutType result {};
            ContainerVector *vector =
                static_cast<ContainerVector *>(VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset));
            OutVector *doubleVector = reinterpret_cast<OutVector *>(vector->GetValue(0));
            LongVector *longVector = reinterpret_cast<LongVector *>(vector->GetValue(1));

            bool overflow = state.count < 0;
            if (state.count > 0 && state.val != nullptr) {
                result = this->template CastWithOverflow<ResultType, OutType>(
                    *reinterpret_cast<ResultType *>(state.val), overflow);
            }

            doubleVector->SetValue(offset, result);
            longVector->SetValue(offset, overflow ? 0 : state.count);

            if (overflow && !this->IsOverflowAsNull()) {
                throw OmniException("OPERATOR_RUNTIME_ERROR", "average_aggregator overflow.");
            }
        } else {
            throw OmniException("Unreachable code", "Reached unreachable code in average aggregator extract partial");
        }
    } else {
        int32_t offset;
        OutType result {};
        auto v = static_cast<OutVector *>(VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset));
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

        v->SetValue(offset, result);
        if (overflow) {
            this->SetNullOrThrowException(v, offset, "average_aggregator overflow.");
        } else if (state.count == 0 || state.val == nullptr) {
            v->SetValueNull(offset);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void AverageAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors,
    int32_t rowIndex)
{
    (this->*extractValuesFuncPointer)(state, vectors, rowIndex);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void AverageAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState &state, Vector *vector,
    const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap)
{
    if constexpr (IN_ID == OMNI_CONTAINER) {
        if (state.val == nullptr) {
            this->InitState(state);
        }
        ResultType *res = reinterpret_cast<ResultType *>(state.val);

        // when input is not raw, vector is container with <double, long> columns for <sum, count>
        auto v = static_cast<ContainerVector *>(vector);
        InVector *sumVector = reinterpret_cast<InVector *>(v->GetValue(0));
        InType *ptr = reinterpret_cast<InType *>(sumVector->GetValues());
        ptr += sumVector->GetPositionOffset();

        LongVector *cntVector = reinterpret_cast<LongVector *>(v->GetValue(1));
        int64_t *cntPtr = reinterpret_cast<int64_t *>(cntVector->GetValues());
        cntPtr += cntVector->GetPositionOffset();

        if (indexMap == nullptr) {
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
            if (nullMap == nullptr) {
                AddDictAvg<InType, ResultType, SumOp<InType, ResultType>>(res, state.count, ptr, cntPtr, rowCount,
                    indexMap);
            } else {
                AddDictConditionalAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(res, state.count,
                    ptr, cntPtr, rowCount, nullMap, indexMap);
            }
        }
    } else {
        SumAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(state, vector, rowOffset, rowCount, nullMap, indexMap);
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void AverageAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates,
    const size_t aggIdx, Vector *vector, const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap)
{
    if constexpr (IN_ID == OMNI_CONTAINER) {
        // when input is not raw, vector is container with <double, long> columns for <sum, count>
        ContainerVector *v = static_cast<ContainerVector *>(vector);
        InVector *sumVector = reinterpret_cast<InVector *>(v->GetValue(0));
        InType *ptr = reinterpret_cast<InType *>(sumVector->GetValues());
        ptr += sumVector->GetPositionOffset();

        LongVector *cntVector = reinterpret_cast<LongVector *>(v->GetValue(1));
        int64_t *cntPtr = reinterpret_cast<int64_t *>(cntVector->GetValues());
        cntPtr += cntVector->GetPositionOffset();

        if (indexMap == nullptr) {
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
            if (nullMap == nullptr) {
                AddDictUseRowIndexAvg<InType, ResultType, SumOp<InType, ResultType>>(rowStates, aggIdx, ptr, cntPtr,
                    indexMap);
            } else {
                AddDictConditionalUseRowIndexAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(
                    rowStates, aggIdx, ptr, cntPtr, nullMap, indexMap);
            }
        }
    } else {
        SumAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(rowStates, aggIdx, vector, rowOffset, nullMap, indexMap);
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
