/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Sum aggregator
 */

#include "sum_aggregator.h"
#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
#ifdef ENABLE_HMPP
template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::ProcessGroupWithHMPP(AggregateState &state,
    VectorBatch *vectorBatch)
{
    if constexpr (IN_ID != OMNI_LONG && IN_ID != OMNI_DECIMAL128) {
        throw OmniException("NOT SUPPORT", "Unsupported input type for sum aggregate");
    } else {
        auto vector = vectorBatch->GetVector(this->channels[0]);

        auto vectorValues = vector->GetValues();
        auto positionOffset = vector->GetPositionOffset();
        auto rowCount = vector->GetSize();
        auto nullAddr = vector->GetValueNulls();
        bool overflow = false;

        HmppResult result = HMPP_STS_NO_ERR;

        if constexpr (IN_ID == OMNI_LONG) {
            LogDebug("HMPP-Agg-sum");
            long sumVal = 0;
            result = HMPPS_Sum_64s(static_cast<int64_t *>(static_cast<int64_t *>(vectorValues) + positionOffset),
                rowCount, static_cast<int8_t *>(static_cast<int8_t *>(nullAddr) + positionOffset), &overflow, &sumVal);

            if (result != HMPP_STS_NO_ERR) {
                throw OmniException("HMPP ERROR", "sum failed for hmpp error");
            }

            ResultType res = static_cast<ResultType>(sumVal);
            if (state.val == nullptr) {
                auto valPtr = this->executionContext->GetArena()->Allocate(sizeof(ResultType));
                *reinterpret_cast<ResultType *>(valPtr) = res;
                state.val = valPtr;
            } else {
                *(reinterpret_cast<ResultType *>(state.val)) += res;
            }
        } else {
            // IN_ID == OMNI_DECIMAL128
            LogDebug("HMPP-Agg-sum");
            HmppDecimal128 sumVal{};
            result = HMPPS_Sum_decimal128(
                static_cast<HmppDecimal128 *>(static_cast<HmppDecimal128 *>(vectorValues) + positionOffset), rowCount,
                static_cast<int8_t *>(static_cast<int8_t *>(nullAddr) + positionOffset), &overflow, &sumVal);

            if (result != HMPP_STS_NO_ERR) {
                throw OmniException("HMPP ERROR", "sum failed for hmpp error");
            }

            if (state.val == nullptr) {
                state.val = this->executionContext->GetArena()->Allocate(sizeof(Decimal128));
                *reinterpret_cast<Decimal128 *>(state.val) = Decimal128(sumVal.high, sumVal.low);
            } else {
                Decimal128Wrapper preSumVal(*(reinterpret_cast<Decimal128 *>(state.val)));
                preSumVal = preSumVal.Add(Decimal128Wrapper(sumVal.high, sumVal.low));
                overflow |= (preSumVal.IsOverflow() != OpStatus::SUCCESS);
            }
        }

        if (overflow) {
            state.count = -1;
        } else if (state.count >= 0) {
            state.count++;
        }
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
bool SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::CanProcessWithHMPP(AggregateState &state,
    VectorBatch *vectorBatch)
{
    // not accept dictionnary vector
    if (vectorBatch->GetVector(this->channels[0])->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
        return false;
    }

    // only OMNI_LONG or OMNI_DECIMAL128 type input support
    if (this->inputTypes.GetType(0)->GetId() == OMNI_DECIMAL128) {
        // just support row Raw data for decimal128
        return RAW_IN;
    } else if (this->inputTypes.GetType(0)->GetId() == OMNI_LONG) {
        return true;
    }
    return false;
}
#endif

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::ExtractValues(const AggregateState &state,
    std::vector<Vector *> &vectors, int32_t rowIndex)
{
    int32_t offset;
    auto v = static_cast<OutVector *>(VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset));

    OutType result{};
    bool overflow = state.count < 0;

    if constexpr (OUT_ID == OMNI_VARCHAR) {
        if (state.count > 0 && state.val != nullptr) {
            result.sum = this->template CastWithOverflow<Decimal128, Decimal128>(
                *reinterpret_cast<Decimal128 *>(state.val), overflow);
        }
        result.count = overflow ? 0 : state.count;
        v->SetValue(offset, reinterpret_cast<uint8_t *>(&result), sizeof(OutType));
        if (overflow && !this->IsOverflowAsNull()) {
            throw OmniException("OPERATOR_RUNTIME_ERROR", "sum_aggregator overflow.");
        }
    } else {
        if (state.count > 0 && state.val != nullptr) {
            result = this->template CastWithOverflow<ResultType, OutType>(*reinterpret_cast<ResultType *>(state.val),
                overflow);
        }

        v->SetValue(offset, result);
        if (overflow) {
            this->SetNullOrThrowException(v, offset, "sum_aggregator overflow.");
        } else if (state.count == 0 || state.val == nullptr) {
            v->SetValueNull(offset);
        }
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::InitState(AggregateState &state)
{
    state.val = this->executionContext->GetArena()->Allocate(sizeof(ResultType));
    *reinterpret_cast<ResultType *>(state.val) = ResultType{};
    state.count = 0;
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState &state,
    Vector *vector, const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap)
{
    if (state.val == nullptr) {
        this->InitState(state);
    }
    ResultType *res = reinterpret_cast<ResultType *>(state.val);

    InType *ptr = reinterpret_cast<InType *>(static_cast<InVector *>(vector)->GetValues());
    ptr += vector->GetPositionOffset();

    if (indexMap == nullptr) {
        ptr += rowOffset;
        if (nullMap == nullptr) {
            add<InType, ResultType, sumOp<InType, ResultType>>(res, state.count, ptr, rowCount);
        } else {
            if constexpr (std::is_floating_point_v<InType>) {
                sumConditionalFloat<InType, ResultType, false>(res, state.count, ptr, rowCount, nullMap);
            } else {
                addConditional<InType, ResultType, sumConditionalOp<InType, ResultType, false>>(res, state.count, ptr,
                    rowCount, nullMap);
            }
        }
    } else {
        if (nullMap == nullptr) {
            addDict<InType, ResultType, sumOp<InType, ResultType>>(res, state.count, ptr, rowCount, indexMap);
        } else {
            addDictConditional<InType, ResultType, sumConditionalOp<InType, ResultType, false>>(res, state.count, ptr,
                rowCount, nullMap, indexMap);
        }
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::ProcessGroupInternal(
    std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector, const int32_t rowOffset,
    const uint8_t *nullMap, const int32_t *indexMap)
{
    InType *ptr = reinterpret_cast<InType *>(static_cast<InVector *>(vector)->GetValues());
    ptr += vector->GetPositionOffset();

    if (indexMap == nullptr) {
        ptr += rowOffset;
        if (nullMap == nullptr) {
            addUseRowIndex<InType, ResultType, sumOp<InType, ResultType>>(rowStates, aggIdx, ptr);
        } else {
            // Reza: can we use customize float operation similar to sumConditionalFloat
            addConditionalUseRowIndex<InType, ResultType, sumConditionalOp<InType, ResultType, false>>(rowStates,
                aggIdx, ptr, nullMap);
        }
    } else {
        if (nullMap == nullptr) {
            addDictUseRowIndex<InType, ResultType, sumOp<InType, ResultType>>(rowStates, aggIdx, ptr, indexMap);
        } else {
            addDictConditionalUseRowIndex<InType, ResultType, sumConditionalOp<InType, ResultType, false>>(rowStates,
                aggIdx, ptr, nullMap, indexMap);
        }
    }
}

// Explicit template instantiation
// Defining templated aggregators in header file consume a lot of memory during compilation
// since, compiler needs to generate each individual template instance wherever aggregator header is include
// to reduce time and memory usage during compilation moved templated aggregator implementation into .cpp files
// and used explicit template instantiation to generate template instances
template class SumAggregator<false, false, false, OMNI_SHORT, OMNI_SHORT>;
template class SumAggregator<false, false, true, OMNI_SHORT, OMNI_SHORT>;
template class SumAggregator<false, true, false, OMNI_SHORT, OMNI_SHORT>;
template class SumAggregator<false, true, true, OMNI_SHORT, OMNI_SHORT>;
template class SumAggregator<true, false, false, OMNI_SHORT, OMNI_SHORT>;
template class SumAggregator<true, false, true, OMNI_SHORT, OMNI_SHORT>;
template class SumAggregator<true, true, false, OMNI_SHORT, OMNI_SHORT>;
template class SumAggregator<true, true, true, OMNI_SHORT, OMNI_SHORT>;

template class SumAggregator<false, false, false, OMNI_SHORT, OMNI_INT>;
template class SumAggregator<false, false, true, OMNI_SHORT, OMNI_INT>;
template class SumAggregator<false, true, false, OMNI_SHORT, OMNI_INT>;
template class SumAggregator<false, true, true, OMNI_SHORT, OMNI_INT>;
template class SumAggregator<true, false, false, OMNI_SHORT, OMNI_INT>;
template class SumAggregator<true, false, true, OMNI_SHORT, OMNI_INT>;
template class SumAggregator<true, true, false, OMNI_SHORT, OMNI_INT>;
template class SumAggregator<true, true, true, OMNI_SHORT, OMNI_INT>;

template class SumAggregator<false, false, false, OMNI_SHORT, OMNI_LONG>;
template class SumAggregator<false, false, true, OMNI_SHORT, OMNI_LONG>;
template class SumAggregator<false, true, false, OMNI_SHORT, OMNI_LONG>;
template class SumAggregator<false, true, true, OMNI_SHORT, OMNI_LONG>;
template class SumAggregator<true, false, false, OMNI_SHORT, OMNI_LONG>;
template class SumAggregator<true, false, true, OMNI_SHORT, OMNI_LONG>;
template class SumAggregator<true, true, false, OMNI_SHORT, OMNI_LONG>;
template class SumAggregator<true, true, true, OMNI_SHORT, OMNI_LONG>;

template class SumAggregator<false, false, false, OMNI_SHORT, OMNI_DOUBLE>;
template class SumAggregator<false, false, true, OMNI_SHORT, OMNI_DOUBLE>;
template class SumAggregator<false, true, false, OMNI_SHORT, OMNI_DOUBLE>;
template class SumAggregator<false, true, true, OMNI_SHORT, OMNI_DOUBLE>;
template class SumAggregator<true, false, false, OMNI_SHORT, OMNI_DOUBLE>;
template class SumAggregator<true, false, true, OMNI_SHORT, OMNI_DOUBLE>;
template class SumAggregator<true, true, false, OMNI_SHORT, OMNI_DOUBLE>;
template class SumAggregator<true, true, true, OMNI_SHORT, OMNI_DOUBLE>;

template class SumAggregator<false, false, false, OMNI_SHORT, OMNI_DECIMAL128>;
template class SumAggregator<false, false, true, OMNI_SHORT, OMNI_DECIMAL128>;
template class SumAggregator<false, true, false, OMNI_SHORT, OMNI_DECIMAL128>;
template class SumAggregator<false, true, true, OMNI_SHORT, OMNI_DECIMAL128>;
template class SumAggregator<true, false, false, OMNI_SHORT, OMNI_DECIMAL128>;
template class SumAggregator<true, false, true, OMNI_SHORT, OMNI_DECIMAL128>;
template class SumAggregator<true, true, false, OMNI_SHORT, OMNI_DECIMAL128>;
template class SumAggregator<true, true, true, OMNI_SHORT, OMNI_DECIMAL128>;

template class SumAggregator<false, false, false, OMNI_SHORT, OMNI_DECIMAL64>;
template class SumAggregator<false, false, true, OMNI_SHORT, OMNI_DECIMAL64>;
template class SumAggregator<false, true, false, OMNI_SHORT, OMNI_DECIMAL64>;
template class SumAggregator<false, true, true, OMNI_SHORT, OMNI_DECIMAL64>;
template class SumAggregator<true, false, false, OMNI_SHORT, OMNI_DECIMAL64>;
template class SumAggregator<true, false, true, OMNI_SHORT, OMNI_DECIMAL64>;
template class SumAggregator<true, true, false, OMNI_SHORT, OMNI_DECIMAL64>;
template class SumAggregator<true, true, true, OMNI_SHORT, OMNI_DECIMAL64>;

template class SumAggregator<false, false, false, OMNI_SHORT, OMNI_VARCHAR>;
template class SumAggregator<false, false, true, OMNI_SHORT, OMNI_VARCHAR>;
template class SumAggregator<false, true, false, OMNI_SHORT, OMNI_VARCHAR>;
template class SumAggregator<false, true, true, OMNI_SHORT, OMNI_VARCHAR>;
template class SumAggregator<true, false, false, OMNI_SHORT, OMNI_VARCHAR>;
template class SumAggregator<true, false, true, OMNI_SHORT, OMNI_VARCHAR>;
template class SumAggregator<true, true, false, OMNI_SHORT, OMNI_VARCHAR>;
template class SumAggregator<true, true, true, OMNI_SHORT, OMNI_VARCHAR>;

template class SumAggregator<false, false, false, OMNI_SHORT, OMNI_CONTAINER>;
template class SumAggregator<false, false, true, OMNI_SHORT, OMNI_CONTAINER>;
template class SumAggregator<false, true, false, OMNI_SHORT, OMNI_CONTAINER>;
template class SumAggregator<false, true, true, OMNI_SHORT, OMNI_CONTAINER>;
template class SumAggregator<true, false, false, OMNI_SHORT, OMNI_CONTAINER>;
template class SumAggregator<true, false, true, OMNI_SHORT, OMNI_CONTAINER>;
template class SumAggregator<true, true, false, OMNI_SHORT, OMNI_CONTAINER>;
template class SumAggregator<true, true, true, OMNI_SHORT, OMNI_CONTAINER>;


template class SumAggregator<false, false, false, OMNI_INT, OMNI_SHORT>;
template class SumAggregator<false, false, true, OMNI_INT, OMNI_SHORT>;
template class SumAggregator<false, true, false, OMNI_INT, OMNI_SHORT>;
template class SumAggregator<false, true, true, OMNI_INT, OMNI_SHORT>;
template class SumAggregator<true, false, false, OMNI_INT, OMNI_SHORT>;
template class SumAggregator<true, false, true, OMNI_INT, OMNI_SHORT>;
template class SumAggregator<true, true, false, OMNI_INT, OMNI_SHORT>;
template class SumAggregator<true, true, true, OMNI_INT, OMNI_SHORT>;

template class SumAggregator<false, false, false, OMNI_INT, OMNI_INT>;
template class SumAggregator<false, false, true, OMNI_INT, OMNI_INT>;
template class SumAggregator<false, true, false, OMNI_INT, OMNI_INT>;
template class SumAggregator<false, true, true, OMNI_INT, OMNI_INT>;
template class SumAggregator<true, false, false, OMNI_INT, OMNI_INT>;
template class SumAggregator<true, false, true, OMNI_INT, OMNI_INT>;
template class SumAggregator<true, true, false, OMNI_INT, OMNI_INT>;
template class SumAggregator<true, true, true, OMNI_INT, OMNI_INT>;

template class SumAggregator<false, false, false, OMNI_INT, OMNI_LONG>;
template class SumAggregator<false, false, true, OMNI_INT, OMNI_LONG>;
template class SumAggregator<false, true, false, OMNI_INT, OMNI_LONG>;
template class SumAggregator<false, true, true, OMNI_INT, OMNI_LONG>;
template class SumAggregator<true, false, false, OMNI_INT, OMNI_LONG>;
template class SumAggregator<true, false, true, OMNI_INT, OMNI_LONG>;
template class SumAggregator<true, true, false, OMNI_INT, OMNI_LONG>;
template class SumAggregator<true, true, true, OMNI_INT, OMNI_LONG>;

template class SumAggregator<false, false, false, OMNI_INT, OMNI_DOUBLE>;
template class SumAggregator<false, false, true, OMNI_INT, OMNI_DOUBLE>;
template class SumAggregator<false, true, false, OMNI_INT, OMNI_DOUBLE>;
template class SumAggregator<false, true, true, OMNI_INT, OMNI_DOUBLE>;
template class SumAggregator<true, false, false, OMNI_INT, OMNI_DOUBLE>;
template class SumAggregator<true, false, true, OMNI_INT, OMNI_DOUBLE>;
template class SumAggregator<true, true, false, OMNI_INT, OMNI_DOUBLE>;
template class SumAggregator<true, true, true, OMNI_INT, OMNI_DOUBLE>;

template class SumAggregator<false, false, false, OMNI_INT, OMNI_DECIMAL128>;
template class SumAggregator<false, false, true, OMNI_INT, OMNI_DECIMAL128>;
template class SumAggregator<false, true, false, OMNI_INT, OMNI_DECIMAL128>;
template class SumAggregator<false, true, true, OMNI_INT, OMNI_DECIMAL128>;
template class SumAggregator<true, false, false, OMNI_INT, OMNI_DECIMAL128>;
template class SumAggregator<true, false, true, OMNI_INT, OMNI_DECIMAL128>;
template class SumAggregator<true, true, false, OMNI_INT, OMNI_DECIMAL128>;
template class SumAggregator<true, true, true, OMNI_INT, OMNI_DECIMAL128>;

template class SumAggregator<false, false, false, OMNI_INT, OMNI_DECIMAL64>;
template class SumAggregator<false, false, true, OMNI_INT, OMNI_DECIMAL64>;
template class SumAggregator<false, true, false, OMNI_INT, OMNI_DECIMAL64>;
template class SumAggregator<false, true, true, OMNI_INT, OMNI_DECIMAL64>;
template class SumAggregator<true, false, false, OMNI_INT, OMNI_DECIMAL64>;
template class SumAggregator<true, false, true, OMNI_INT, OMNI_DECIMAL64>;
template class SumAggregator<true, true, false, OMNI_INT, OMNI_DECIMAL64>;
template class SumAggregator<true, true, true, OMNI_INT, OMNI_DECIMAL64>;

template class SumAggregator<false, false, false, OMNI_INT, OMNI_VARCHAR>;
template class SumAggregator<false, false, true, OMNI_INT, OMNI_VARCHAR>;
template class SumAggregator<false, true, false, OMNI_INT, OMNI_VARCHAR>;
template class SumAggregator<false, true, true, OMNI_INT, OMNI_VARCHAR>;
template class SumAggregator<true, false, false, OMNI_INT, OMNI_VARCHAR>;
template class SumAggregator<true, false, true, OMNI_INT, OMNI_VARCHAR>;
template class SumAggregator<true, true, false, OMNI_INT, OMNI_VARCHAR>;
template class SumAggregator<true, true, true, OMNI_INT, OMNI_VARCHAR>;

template class SumAggregator<false, false, false, OMNI_INT, OMNI_CONTAINER>;
template class SumAggregator<false, false, true, OMNI_INT, OMNI_CONTAINER>;
template class SumAggregator<false, true, false, OMNI_INT, OMNI_CONTAINER>;
template class SumAggregator<false, true, true, OMNI_INT, OMNI_CONTAINER>;
template class SumAggregator<true, false, false, OMNI_INT, OMNI_CONTAINER>;
template class SumAggregator<true, false, true, OMNI_INT, OMNI_CONTAINER>;
template class SumAggregator<true, true, false, OMNI_INT, OMNI_CONTAINER>;
template class SumAggregator<true, true, true, OMNI_INT, OMNI_CONTAINER>;


template class SumAggregator<false, false, false, OMNI_LONG, OMNI_SHORT>;
template class SumAggregator<false, false, true, OMNI_LONG, OMNI_SHORT>;
template class SumAggregator<false, true, false, OMNI_LONG, OMNI_SHORT>;
template class SumAggregator<false, true, true, OMNI_LONG, OMNI_SHORT>;
template class SumAggregator<true, false, false, OMNI_LONG, OMNI_SHORT>;
template class SumAggregator<true, false, true, OMNI_LONG, OMNI_SHORT>;
template class SumAggregator<true, true, false, OMNI_LONG, OMNI_SHORT>;
template class SumAggregator<true, true, true, OMNI_LONG, OMNI_SHORT>;

template class SumAggregator<false, false, false, OMNI_LONG, OMNI_INT>;
template class SumAggregator<false, false, true, OMNI_LONG, OMNI_INT>;
template class SumAggregator<false, true, false, OMNI_LONG, OMNI_INT>;
template class SumAggregator<false, true, true, OMNI_LONG, OMNI_INT>;
template class SumAggregator<true, false, false, OMNI_LONG, OMNI_INT>;
template class SumAggregator<true, false, true, OMNI_LONG, OMNI_INT>;
template class SumAggregator<true, true, false, OMNI_LONG, OMNI_INT>;
template class SumAggregator<true, true, true, OMNI_LONG, OMNI_INT>;

template class SumAggregator<false, false, false, OMNI_LONG, OMNI_LONG>;
template class SumAggregator<false, false, true, OMNI_LONG, OMNI_LONG>;
template class SumAggregator<false, true, false, OMNI_LONG, OMNI_LONG>;
template class SumAggregator<false, true, true, OMNI_LONG, OMNI_LONG>;
template class SumAggregator<true, false, false, OMNI_LONG, OMNI_LONG>;
template class SumAggregator<true, false, true, OMNI_LONG, OMNI_LONG>;
template class SumAggregator<true, true, false, OMNI_LONG, OMNI_LONG>;
template class SumAggregator<true, true, true, OMNI_LONG, OMNI_LONG>;

template class SumAggregator<false, false, false, OMNI_LONG, OMNI_DOUBLE>;
template class SumAggregator<false, false, true, OMNI_LONG, OMNI_DOUBLE>;
template class SumAggregator<false, true, false, OMNI_LONG, OMNI_DOUBLE>;
template class SumAggregator<false, true, true, OMNI_LONG, OMNI_DOUBLE>;
template class SumAggregator<true, false, false, OMNI_LONG, OMNI_DOUBLE>;
template class SumAggregator<true, false, true, OMNI_LONG, OMNI_DOUBLE>;
template class SumAggregator<true, true, false, OMNI_LONG, OMNI_DOUBLE>;
template class SumAggregator<true, true, true, OMNI_LONG, OMNI_DOUBLE>;

template class SumAggregator<false, false, false, OMNI_LONG, OMNI_DECIMAL128>;
template class SumAggregator<false, false, true, OMNI_LONG, OMNI_DECIMAL128>;
template class SumAggregator<false, true, false, OMNI_LONG, OMNI_DECIMAL128>;
template class SumAggregator<false, true, true, OMNI_LONG, OMNI_DECIMAL128>;
template class SumAggregator<true, false, false, OMNI_LONG, OMNI_DECIMAL128>;
template class SumAggregator<true, false, true, OMNI_LONG, OMNI_DECIMAL128>;
template class SumAggregator<true, true, false, OMNI_LONG, OMNI_DECIMAL128>;
template class SumAggregator<true, true, true, OMNI_LONG, OMNI_DECIMAL128>;

template class SumAggregator<false, false, false, OMNI_LONG, OMNI_DECIMAL64>;
template class SumAggregator<false, false, true, OMNI_LONG, OMNI_DECIMAL64>;
template class SumAggregator<false, true, false, OMNI_LONG, OMNI_DECIMAL64>;
template class SumAggregator<false, true, true, OMNI_LONG, OMNI_DECIMAL64>;
template class SumAggregator<true, false, false, OMNI_LONG, OMNI_DECIMAL64>;
template class SumAggregator<true, false, true, OMNI_LONG, OMNI_DECIMAL64>;
template class SumAggregator<true, true, false, OMNI_LONG, OMNI_DECIMAL64>;
template class SumAggregator<true, true, true, OMNI_LONG, OMNI_DECIMAL64>;

template class SumAggregator<false, false, false, OMNI_LONG, OMNI_VARCHAR>;
template class SumAggregator<false, false, true, OMNI_LONG, OMNI_VARCHAR>;
template class SumAggregator<false, true, false, OMNI_LONG, OMNI_VARCHAR>;
template class SumAggregator<false, true, true, OMNI_LONG, OMNI_VARCHAR>;
template class SumAggregator<true, false, false, OMNI_LONG, OMNI_VARCHAR>;
template class SumAggregator<true, false, true, OMNI_LONG, OMNI_VARCHAR>;
template class SumAggregator<true, true, false, OMNI_LONG, OMNI_VARCHAR>;
template class SumAggregator<true, true, true, OMNI_LONG, OMNI_VARCHAR>;

template class SumAggregator<false, false, false, OMNI_LONG, OMNI_CONTAINER>;
template class SumAggregator<false, false, true, OMNI_LONG, OMNI_CONTAINER>;
template class SumAggregator<false, true, false, OMNI_LONG, OMNI_CONTAINER>;
template class SumAggregator<false, true, true, OMNI_LONG, OMNI_CONTAINER>;
template class SumAggregator<true, false, false, OMNI_LONG, OMNI_CONTAINER>;
template class SumAggregator<true, false, true, OMNI_LONG, OMNI_CONTAINER>;
template class SumAggregator<true, true, false, OMNI_LONG, OMNI_CONTAINER>;
template class SumAggregator<true, true, true, OMNI_LONG, OMNI_CONTAINER>;


template class SumAggregator<false, false, false, OMNI_DOUBLE, OMNI_SHORT>;
template class SumAggregator<false, false, true, OMNI_DOUBLE, OMNI_SHORT>;
template class SumAggregator<false, true, false, OMNI_DOUBLE, OMNI_SHORT>;
template class SumAggregator<false, true, true, OMNI_DOUBLE, OMNI_SHORT>;
template class SumAggregator<true, false, false, OMNI_DOUBLE, OMNI_SHORT>;
template class SumAggregator<true, false, true, OMNI_DOUBLE, OMNI_SHORT>;
template class SumAggregator<true, true, false, OMNI_DOUBLE, OMNI_SHORT>;
template class SumAggregator<true, true, true, OMNI_DOUBLE, OMNI_SHORT>;

template class SumAggregator<false, false, false, OMNI_DOUBLE, OMNI_INT>;
template class SumAggregator<false, false, true, OMNI_DOUBLE, OMNI_INT>;
template class SumAggregator<false, true, false, OMNI_DOUBLE, OMNI_INT>;
template class SumAggregator<false, true, true, OMNI_DOUBLE, OMNI_INT>;
template class SumAggregator<true, false, false, OMNI_DOUBLE, OMNI_INT>;
template class SumAggregator<true, false, true, OMNI_DOUBLE, OMNI_INT>;
template class SumAggregator<true, true, false, OMNI_DOUBLE, OMNI_INT>;
template class SumAggregator<true, true, true, OMNI_DOUBLE, OMNI_INT>;

template class SumAggregator<false, false, false, OMNI_DOUBLE, OMNI_LONG>;
template class SumAggregator<false, false, true, OMNI_DOUBLE, OMNI_LONG>;
template class SumAggregator<false, true, false, OMNI_DOUBLE, OMNI_LONG>;
template class SumAggregator<false, true, true, OMNI_DOUBLE, OMNI_LONG>;
template class SumAggregator<true, false, false, OMNI_DOUBLE, OMNI_LONG>;
template class SumAggregator<true, false, true, OMNI_DOUBLE, OMNI_LONG>;
template class SumAggregator<true, true, false, OMNI_DOUBLE, OMNI_LONG>;
template class SumAggregator<true, true, true, OMNI_DOUBLE, OMNI_LONG>;

template class SumAggregator<false, false, false, OMNI_DOUBLE, OMNI_DOUBLE>;
template class SumAggregator<false, false, true, OMNI_DOUBLE, OMNI_DOUBLE>;
template class SumAggregator<false, true, false, OMNI_DOUBLE, OMNI_DOUBLE>;
template class SumAggregator<false, true, true, OMNI_DOUBLE, OMNI_DOUBLE>;
template class SumAggregator<true, false, false, OMNI_DOUBLE, OMNI_DOUBLE>;
template class SumAggregator<true, false, true, OMNI_DOUBLE, OMNI_DOUBLE>;
template class SumAggregator<true, true, false, OMNI_DOUBLE, OMNI_DOUBLE>;
template class SumAggregator<true, true, true, OMNI_DOUBLE, OMNI_DOUBLE>;

template class SumAggregator<false, false, false, OMNI_DOUBLE, OMNI_DECIMAL128>;
template class SumAggregator<false, false, true, OMNI_DOUBLE, OMNI_DECIMAL128>;
template class SumAggregator<false, true, false, OMNI_DOUBLE, OMNI_DECIMAL128>;
template class SumAggregator<false, true, true, OMNI_DOUBLE, OMNI_DECIMAL128>;
template class SumAggregator<true, false, false, OMNI_DOUBLE, OMNI_DECIMAL128>;
template class SumAggregator<true, false, true, OMNI_DOUBLE, OMNI_DECIMAL128>;
template class SumAggregator<true, true, false, OMNI_DOUBLE, OMNI_DECIMAL128>;
template class SumAggregator<true, true, true, OMNI_DOUBLE, OMNI_DECIMAL128>;

template class SumAggregator<false, false, false, OMNI_DOUBLE, OMNI_DECIMAL64>;
template class SumAggregator<false, false, true, OMNI_DOUBLE, OMNI_DECIMAL64>;
template class SumAggregator<false, true, false, OMNI_DOUBLE, OMNI_DECIMAL64>;
template class SumAggregator<false, true, true, OMNI_DOUBLE, OMNI_DECIMAL64>;
template class SumAggregator<true, false, false, OMNI_DOUBLE, OMNI_DECIMAL64>;
template class SumAggregator<true, false, true, OMNI_DOUBLE, OMNI_DECIMAL64>;
template class SumAggregator<true, true, false, OMNI_DOUBLE, OMNI_DECIMAL64>;
template class SumAggregator<true, true, true, OMNI_DOUBLE, OMNI_DECIMAL64>;

template class SumAggregator<false, false, false, OMNI_DOUBLE, OMNI_VARCHAR>;
template class SumAggregator<false, false, true, OMNI_DOUBLE, OMNI_VARCHAR>;
template class SumAggregator<false, true, false, OMNI_DOUBLE, OMNI_VARCHAR>;
template class SumAggregator<false, true, true, OMNI_DOUBLE, OMNI_VARCHAR>;
template class SumAggregator<true, false, false, OMNI_DOUBLE, OMNI_VARCHAR>;
template class SumAggregator<true, false, true, OMNI_DOUBLE, OMNI_VARCHAR>;
template class SumAggregator<true, true, false, OMNI_DOUBLE, OMNI_VARCHAR>;
template class SumAggregator<true, true, true, OMNI_DOUBLE, OMNI_VARCHAR>;

template class SumAggregator<false, false, false, OMNI_DOUBLE, OMNI_CONTAINER>;
template class SumAggregator<false, false, true, OMNI_DOUBLE, OMNI_CONTAINER>;
template class SumAggregator<false, true, false, OMNI_DOUBLE, OMNI_CONTAINER>;
template class SumAggregator<false, true, true, OMNI_DOUBLE, OMNI_CONTAINER>;
template class SumAggregator<true, false, false, OMNI_DOUBLE, OMNI_CONTAINER>;
template class SumAggregator<true, false, true, OMNI_DOUBLE, OMNI_CONTAINER>;
template class SumAggregator<true, true, false, OMNI_DOUBLE, OMNI_CONTAINER>;
template class SumAggregator<true, true, true, OMNI_DOUBLE, OMNI_CONTAINER>;


template class SumAggregator<false, false, false, OMNI_DECIMAL128, OMNI_SHORT>;
template class SumAggregator<false, false, true, OMNI_DECIMAL128, OMNI_SHORT>;
template class SumAggregator<false, true, false, OMNI_DECIMAL128, OMNI_SHORT>;
template class SumAggregator<false, true, true, OMNI_DECIMAL128, OMNI_SHORT>;
template class SumAggregator<true, false, false, OMNI_DECIMAL128, OMNI_SHORT>;
template class SumAggregator<true, false, true, OMNI_DECIMAL128, OMNI_SHORT>;
template class SumAggregator<true, true, false, OMNI_DECIMAL128, OMNI_SHORT>;
template class SumAggregator<true, true, true, OMNI_DECIMAL128, OMNI_SHORT>;

template class SumAggregator<false, false, false, OMNI_DECIMAL128, OMNI_INT>;
template class SumAggregator<false, false, true, OMNI_DECIMAL128, OMNI_INT>;
template class SumAggregator<false, true, false, OMNI_DECIMAL128, OMNI_INT>;
template class SumAggregator<false, true, true, OMNI_DECIMAL128, OMNI_INT>;
template class SumAggregator<true, false, false, OMNI_DECIMAL128, OMNI_INT>;
template class SumAggregator<true, false, true, OMNI_DECIMAL128, OMNI_INT>;
template class SumAggregator<true, true, false, OMNI_DECIMAL128, OMNI_INT>;
template class SumAggregator<true, true, true, OMNI_DECIMAL128, OMNI_INT>;

template class SumAggregator<false, false, false, OMNI_DECIMAL128, OMNI_LONG>;
template class SumAggregator<false, false, true, OMNI_DECIMAL128, OMNI_LONG>;
template class SumAggregator<false, true, false, OMNI_DECIMAL128, OMNI_LONG>;
template class SumAggregator<false, true, true, OMNI_DECIMAL128, OMNI_LONG>;
template class SumAggregator<true, false, false, OMNI_DECIMAL128, OMNI_LONG>;
template class SumAggregator<true, false, true, OMNI_DECIMAL128, OMNI_LONG>;
template class SumAggregator<true, true, false, OMNI_DECIMAL128, OMNI_LONG>;
template class SumAggregator<true, true, true, OMNI_DECIMAL128, OMNI_LONG>;

template class SumAggregator<false, false, false, OMNI_DECIMAL128, OMNI_DOUBLE>;
template class SumAggregator<false, false, true, OMNI_DECIMAL128, OMNI_DOUBLE>;
template class SumAggregator<false, true, false, OMNI_DECIMAL128, OMNI_DOUBLE>;
template class SumAggregator<false, true, true, OMNI_DECIMAL128, OMNI_DOUBLE>;
template class SumAggregator<true, false, false, OMNI_DECIMAL128, OMNI_DOUBLE>;
template class SumAggregator<true, false, true, OMNI_DECIMAL128, OMNI_DOUBLE>;
template class SumAggregator<true, true, false, OMNI_DECIMAL128, OMNI_DOUBLE>;
template class SumAggregator<true, true, true, OMNI_DECIMAL128, OMNI_DOUBLE>;

template class SumAggregator<false, false, false, OMNI_DECIMAL128, OMNI_DECIMAL128>;
template class SumAggregator<false, false, true, OMNI_DECIMAL128, OMNI_DECIMAL128>;
template class SumAggregator<false, true, false, OMNI_DECIMAL128, OMNI_DECIMAL128>;
template class SumAggregator<false, true, true, OMNI_DECIMAL128, OMNI_DECIMAL128>;
template class SumAggregator<true, false, false, OMNI_DECIMAL128, OMNI_DECIMAL128>;
template class SumAggregator<true, false, true, OMNI_DECIMAL128, OMNI_DECIMAL128>;
template class SumAggregator<true, true, false, OMNI_DECIMAL128, OMNI_DECIMAL128>;
template class SumAggregator<true, true, true, OMNI_DECIMAL128, OMNI_DECIMAL128>;

template class SumAggregator<false, false, false, OMNI_DECIMAL128, OMNI_DECIMAL64>;
template class SumAggregator<false, false, true, OMNI_DECIMAL128, OMNI_DECIMAL64>;
template class SumAggregator<false, true, false, OMNI_DECIMAL128, OMNI_DECIMAL64>;
template class SumAggregator<false, true, true, OMNI_DECIMAL128, OMNI_DECIMAL64>;
template class SumAggregator<true, false, false, OMNI_DECIMAL128, OMNI_DECIMAL64>;
template class SumAggregator<true, false, true, OMNI_DECIMAL128, OMNI_DECIMAL64>;
template class SumAggregator<true, true, false, OMNI_DECIMAL128, OMNI_DECIMAL64>;
template class SumAggregator<true, true, true, OMNI_DECIMAL128, OMNI_DECIMAL64>;

template class SumAggregator<false, false, false, OMNI_DECIMAL128, OMNI_VARCHAR>;
template class SumAggregator<false, false, true, OMNI_DECIMAL128, OMNI_VARCHAR>;
template class SumAggregator<false, true, false, OMNI_DECIMAL128, OMNI_VARCHAR>;
template class SumAggregator<false, true, true, OMNI_DECIMAL128, OMNI_VARCHAR>;
template class SumAggregator<true, false, false, OMNI_DECIMAL128, OMNI_VARCHAR>;
template class SumAggregator<true, false, true, OMNI_DECIMAL128, OMNI_VARCHAR>;
template class SumAggregator<true, true, false, OMNI_DECIMAL128, OMNI_VARCHAR>;
template class SumAggregator<true, true, true, OMNI_DECIMAL128, OMNI_VARCHAR>;

template class SumAggregator<false, false, false, OMNI_DECIMAL128, OMNI_CONTAINER>;
template class SumAggregator<false, false, true, OMNI_DECIMAL128, OMNI_CONTAINER>;
template class SumAggregator<false, true, false, OMNI_DECIMAL128, OMNI_CONTAINER>;
template class SumAggregator<false, true, true, OMNI_DECIMAL128, OMNI_CONTAINER>;
template class SumAggregator<true, false, false, OMNI_DECIMAL128, OMNI_CONTAINER>;
template class SumAggregator<true, false, true, OMNI_DECIMAL128, OMNI_CONTAINER>;
template class SumAggregator<true, true, false, OMNI_DECIMAL128, OMNI_CONTAINER>;
template class SumAggregator<true, true, true, OMNI_DECIMAL128, OMNI_CONTAINER>;


template class SumAggregator<false, false, false, OMNI_DECIMAL64, OMNI_SHORT>;
template class SumAggregator<false, false, true, OMNI_DECIMAL64, OMNI_SHORT>;
template class SumAggregator<false, true, false, OMNI_DECIMAL64, OMNI_SHORT>;
template class SumAggregator<false, true, true, OMNI_DECIMAL64, OMNI_SHORT>;
template class SumAggregator<true, false, false, OMNI_DECIMAL64, OMNI_SHORT>;
template class SumAggregator<true, false, true, OMNI_DECIMAL64, OMNI_SHORT>;
template class SumAggregator<true, true, false, OMNI_DECIMAL64, OMNI_SHORT>;
template class SumAggregator<true, true, true, OMNI_DECIMAL64, OMNI_SHORT>;

template class SumAggregator<false, false, false, OMNI_DECIMAL64, OMNI_INT>;
template class SumAggregator<false, false, true, OMNI_DECIMAL64, OMNI_INT>;
template class SumAggregator<false, true, false, OMNI_DECIMAL64, OMNI_INT>;
template class SumAggregator<false, true, true, OMNI_DECIMAL64, OMNI_INT>;
template class SumAggregator<true, false, false, OMNI_DECIMAL64, OMNI_INT>;
template class SumAggregator<true, false, true, OMNI_DECIMAL64, OMNI_INT>;
template class SumAggregator<true, true, false, OMNI_DECIMAL64, OMNI_INT>;
template class SumAggregator<true, true, true, OMNI_DECIMAL64, OMNI_INT>;

template class SumAggregator<false, false, false, OMNI_DECIMAL64, OMNI_LONG>;
template class SumAggregator<false, false, true, OMNI_DECIMAL64, OMNI_LONG>;
template class SumAggregator<false, true, false, OMNI_DECIMAL64, OMNI_LONG>;
template class SumAggregator<false, true, true, OMNI_DECIMAL64, OMNI_LONG>;
template class SumAggregator<true, false, false, OMNI_DECIMAL64, OMNI_LONG>;
template class SumAggregator<true, false, true, OMNI_DECIMAL64, OMNI_LONG>;
template class SumAggregator<true, true, false, OMNI_DECIMAL64, OMNI_LONG>;
template class SumAggregator<true, true, true, OMNI_DECIMAL64, OMNI_LONG>;

template class SumAggregator<false, false, false, OMNI_DECIMAL64, OMNI_DOUBLE>;
template class SumAggregator<false, false, true, OMNI_DECIMAL64, OMNI_DOUBLE>;
template class SumAggregator<false, true, false, OMNI_DECIMAL64, OMNI_DOUBLE>;
template class SumAggregator<false, true, true, OMNI_DECIMAL64, OMNI_DOUBLE>;
template class SumAggregator<true, false, false, OMNI_DECIMAL64, OMNI_DOUBLE>;
template class SumAggregator<true, false, true, OMNI_DECIMAL64, OMNI_DOUBLE>;
template class SumAggregator<true, true, false, OMNI_DECIMAL64, OMNI_DOUBLE>;
template class SumAggregator<true, true, true, OMNI_DECIMAL64, OMNI_DOUBLE>;

template class SumAggregator<false, false, false, OMNI_DECIMAL64, OMNI_DECIMAL128>;
template class SumAggregator<false, false, true, OMNI_DECIMAL64, OMNI_DECIMAL128>;
template class SumAggregator<false, true, false, OMNI_DECIMAL64, OMNI_DECIMAL128>;
template class SumAggregator<false, true, true, OMNI_DECIMAL64, OMNI_DECIMAL128>;
template class SumAggregator<true, false, false, OMNI_DECIMAL64, OMNI_DECIMAL128>;
template class SumAggregator<true, false, true, OMNI_DECIMAL64, OMNI_DECIMAL128>;
template class SumAggregator<true, true, false, OMNI_DECIMAL64, OMNI_DECIMAL128>;
template class SumAggregator<true, true, true, OMNI_DECIMAL64, OMNI_DECIMAL128>;

template class SumAggregator<false, false, false, OMNI_DECIMAL64, OMNI_DECIMAL64>;
template class SumAggregator<false, false, true, OMNI_DECIMAL64, OMNI_DECIMAL64>;
template class SumAggregator<false, true, false, OMNI_DECIMAL64, OMNI_DECIMAL64>;
template class SumAggregator<false, true, true, OMNI_DECIMAL64, OMNI_DECIMAL64>;
template class SumAggregator<true, false, false, OMNI_DECIMAL64, OMNI_DECIMAL64>;
template class SumAggregator<true, false, true, OMNI_DECIMAL64, OMNI_DECIMAL64>;
template class SumAggregator<true, true, false, OMNI_DECIMAL64, OMNI_DECIMAL64>;
template class SumAggregator<true, true, true, OMNI_DECIMAL64, OMNI_DECIMAL64>;

template class SumAggregator<false, false, false, OMNI_DECIMAL64, OMNI_VARCHAR>;
template class SumAggregator<false, false, true, OMNI_DECIMAL64, OMNI_VARCHAR>;
template class SumAggregator<false, true, false, OMNI_DECIMAL64, OMNI_VARCHAR>;
template class SumAggregator<false, true, true, OMNI_DECIMAL64, OMNI_VARCHAR>;
template class SumAggregator<true, false, false, OMNI_DECIMAL64, OMNI_VARCHAR>;
template class SumAggregator<true, false, true, OMNI_DECIMAL64, OMNI_VARCHAR>;
template class SumAggregator<true, true, false, OMNI_DECIMAL64, OMNI_VARCHAR>;
template class SumAggregator<true, true, true, OMNI_DECIMAL64, OMNI_VARCHAR>;

template class SumAggregator<false, false, false, OMNI_DECIMAL64, OMNI_CONTAINER>;
template class SumAggregator<false, false, true, OMNI_DECIMAL64, OMNI_CONTAINER>;
template class SumAggregator<false, true, false, OMNI_DECIMAL64, OMNI_CONTAINER>;
template class SumAggregator<false, true, true, OMNI_DECIMAL64, OMNI_CONTAINER>;
template class SumAggregator<true, false, false, OMNI_DECIMAL64, OMNI_CONTAINER>;
template class SumAggregator<true, false, true, OMNI_DECIMAL64, OMNI_CONTAINER>;
template class SumAggregator<true, true, false, OMNI_DECIMAL64, OMNI_CONTAINER>;
template class SumAggregator<true, true, true, OMNI_DECIMAL64, OMNI_CONTAINER>;


template class SumAggregator<false, false, false, OMNI_VARCHAR, OMNI_SHORT>;
template class SumAggregator<false, false, true, OMNI_VARCHAR, OMNI_SHORT>;
template class SumAggregator<false, true, false, OMNI_VARCHAR, OMNI_SHORT>;
template class SumAggregator<false, true, true, OMNI_VARCHAR, OMNI_SHORT>;
template class SumAggregator<true, false, false, OMNI_VARCHAR, OMNI_SHORT>;
template class SumAggregator<true, false, true, OMNI_VARCHAR, OMNI_SHORT>;
template class SumAggregator<true, true, false, OMNI_VARCHAR, OMNI_SHORT>;
template class SumAggregator<true, true, true, OMNI_VARCHAR, OMNI_SHORT>;

template class SumAggregator<false, false, false, OMNI_VARCHAR, OMNI_INT>;
template class SumAggregator<false, false, true, OMNI_VARCHAR, OMNI_INT>;
template class SumAggregator<false, true, false, OMNI_VARCHAR, OMNI_INT>;
template class SumAggregator<false, true, true, OMNI_VARCHAR, OMNI_INT>;
template class SumAggregator<true, false, false, OMNI_VARCHAR, OMNI_INT>;
template class SumAggregator<true, false, true, OMNI_VARCHAR, OMNI_INT>;
template class SumAggregator<true, true, false, OMNI_VARCHAR, OMNI_INT>;
template class SumAggregator<true, true, true, OMNI_VARCHAR, OMNI_INT>;

template class SumAggregator<false, false, false, OMNI_VARCHAR, OMNI_LONG>;
template class SumAggregator<false, false, true, OMNI_VARCHAR, OMNI_LONG>;
template class SumAggregator<false, true, false, OMNI_VARCHAR, OMNI_LONG>;
template class SumAggregator<false, true, true, OMNI_VARCHAR, OMNI_LONG>;
template class SumAggregator<true, false, false, OMNI_VARCHAR, OMNI_LONG>;
template class SumAggregator<true, false, true, OMNI_VARCHAR, OMNI_LONG>;
template class SumAggregator<true, true, false, OMNI_VARCHAR, OMNI_LONG>;
template class SumAggregator<true, true, true, OMNI_VARCHAR, OMNI_LONG>;

template class SumAggregator<false, false, false, OMNI_VARCHAR, OMNI_DOUBLE>;
template class SumAggregator<false, false, true, OMNI_VARCHAR, OMNI_DOUBLE>;
template class SumAggregator<false, true, false, OMNI_VARCHAR, OMNI_DOUBLE>;
template class SumAggregator<false, true, true, OMNI_VARCHAR, OMNI_DOUBLE>;
template class SumAggregator<true, false, false, OMNI_VARCHAR, OMNI_DOUBLE>;
template class SumAggregator<true, false, true, OMNI_VARCHAR, OMNI_DOUBLE>;
template class SumAggregator<true, true, false, OMNI_VARCHAR, OMNI_DOUBLE>;
template class SumAggregator<true, true, true, OMNI_VARCHAR, OMNI_DOUBLE>;

template class SumAggregator<false, false, false, OMNI_VARCHAR, OMNI_DECIMAL128>;
template class SumAggregator<false, false, true, OMNI_VARCHAR, OMNI_DECIMAL128>;
template class SumAggregator<false, true, false, OMNI_VARCHAR, OMNI_DECIMAL128>;
template class SumAggregator<false, true, true, OMNI_VARCHAR, OMNI_DECIMAL128>;
template class SumAggregator<true, false, false, OMNI_VARCHAR, OMNI_DECIMAL128>;
template class SumAggregator<true, false, true, OMNI_VARCHAR, OMNI_DECIMAL128>;
template class SumAggregator<true, true, false, OMNI_VARCHAR, OMNI_DECIMAL128>;
template class SumAggregator<true, true, true, OMNI_VARCHAR, OMNI_DECIMAL128>;

template class SumAggregator<false, false, false, OMNI_VARCHAR, OMNI_DECIMAL64>;
template class SumAggregator<false, false, true, OMNI_VARCHAR, OMNI_DECIMAL64>;
template class SumAggregator<false, true, false, OMNI_VARCHAR, OMNI_DECIMAL64>;
template class SumAggregator<false, true, true, OMNI_VARCHAR, OMNI_DECIMAL64>;
template class SumAggregator<true, false, false, OMNI_VARCHAR, OMNI_DECIMAL64>;
template class SumAggregator<true, false, true, OMNI_VARCHAR, OMNI_DECIMAL64>;
template class SumAggregator<true, true, false, OMNI_VARCHAR, OMNI_DECIMAL64>;
template class SumAggregator<true, true, true, OMNI_VARCHAR, OMNI_DECIMAL64>;

template class SumAggregator<false, false, false, OMNI_VARCHAR, OMNI_VARCHAR>;
template class SumAggregator<false, false, true, OMNI_VARCHAR, OMNI_VARCHAR>;
template class SumAggregator<false, true, false, OMNI_VARCHAR, OMNI_VARCHAR>;
template class SumAggregator<false, true, true, OMNI_VARCHAR, OMNI_VARCHAR>;
template class SumAggregator<true, false, false, OMNI_VARCHAR, OMNI_VARCHAR>;
template class SumAggregator<true, false, true, OMNI_VARCHAR, OMNI_VARCHAR>;
template class SumAggregator<true, true, false, OMNI_VARCHAR, OMNI_VARCHAR>;
template class SumAggregator<true, true, true, OMNI_VARCHAR, OMNI_VARCHAR>;

template class SumAggregator<false, false, false, OMNI_VARCHAR, OMNI_CONTAINER>;
template class SumAggregator<false, false, true, OMNI_VARCHAR, OMNI_CONTAINER>;
template class SumAggregator<false, true, false, OMNI_VARCHAR, OMNI_CONTAINER>;
template class SumAggregator<false, true, true, OMNI_VARCHAR, OMNI_CONTAINER>;
template class SumAggregator<true, false, false, OMNI_VARCHAR, OMNI_CONTAINER>;
template class SumAggregator<true, false, true, OMNI_VARCHAR, OMNI_CONTAINER>;
template class SumAggregator<true, true, false, OMNI_VARCHAR, OMNI_CONTAINER>;
template class SumAggregator<true, true, true, OMNI_VARCHAR, OMNI_CONTAINER>;


template class SumAggregator<false, false, false, OMNI_CONTAINER, OMNI_SHORT>;
template class SumAggregator<false, false, true, OMNI_CONTAINER, OMNI_SHORT>;
template class SumAggregator<false, true, false, OMNI_CONTAINER, OMNI_SHORT>;
template class SumAggregator<false, true, true, OMNI_CONTAINER, OMNI_SHORT>;
template class SumAggregator<true, false, false, OMNI_CONTAINER, OMNI_SHORT>;
template class SumAggregator<true, false, true, OMNI_CONTAINER, OMNI_SHORT>;
template class SumAggregator<true, true, false, OMNI_CONTAINER, OMNI_SHORT>;
template class SumAggregator<true, true, true, OMNI_CONTAINER, OMNI_SHORT>;

template class SumAggregator<false, false, false, OMNI_CONTAINER, OMNI_INT>;
template class SumAggregator<false, false, true, OMNI_CONTAINER, OMNI_INT>;
template class SumAggregator<false, true, false, OMNI_CONTAINER, OMNI_INT>;
template class SumAggregator<false, true, true, OMNI_CONTAINER, OMNI_INT>;
template class SumAggregator<true, false, false, OMNI_CONTAINER, OMNI_INT>;
template class SumAggregator<true, false, true, OMNI_CONTAINER, OMNI_INT>;
template class SumAggregator<true, true, false, OMNI_CONTAINER, OMNI_INT>;
template class SumAggregator<true, true, true, OMNI_CONTAINER, OMNI_INT>;

template class SumAggregator<false, false, false, OMNI_CONTAINER, OMNI_LONG>;
template class SumAggregator<false, false, true, OMNI_CONTAINER, OMNI_LONG>;
template class SumAggregator<false, true, false, OMNI_CONTAINER, OMNI_LONG>;
template class SumAggregator<false, true, true, OMNI_CONTAINER, OMNI_LONG>;
template class SumAggregator<true, false, false, OMNI_CONTAINER, OMNI_LONG>;
template class SumAggregator<true, false, true, OMNI_CONTAINER, OMNI_LONG>;
template class SumAggregator<true, true, false, OMNI_CONTAINER, OMNI_LONG>;
template class SumAggregator<true, true, true, OMNI_CONTAINER, OMNI_LONG>;

template class SumAggregator<false, false, false, OMNI_CONTAINER, OMNI_DOUBLE>;
template class SumAggregator<false, false, true, OMNI_CONTAINER, OMNI_DOUBLE>;
template class SumAggregator<false, true, false, OMNI_CONTAINER, OMNI_DOUBLE>;
template class SumAggregator<false, true, true, OMNI_CONTAINER, OMNI_DOUBLE>;
template class SumAggregator<true, false, false, OMNI_CONTAINER, OMNI_DOUBLE>;
template class SumAggregator<true, false, true, OMNI_CONTAINER, OMNI_DOUBLE>;
template class SumAggregator<true, true, false, OMNI_CONTAINER, OMNI_DOUBLE>;
template class SumAggregator<true, true, true, OMNI_CONTAINER, OMNI_DOUBLE>;

template class SumAggregator<false, false, false, OMNI_CONTAINER, OMNI_DECIMAL128>;
template class SumAggregator<false, false, true, OMNI_CONTAINER, OMNI_DECIMAL128>;
template class SumAggregator<false, true, false, OMNI_CONTAINER, OMNI_DECIMAL128>;
template class SumAggregator<false, true, true, OMNI_CONTAINER, OMNI_DECIMAL128>;
template class SumAggregator<true, false, false, OMNI_CONTAINER, OMNI_DECIMAL128>;
template class SumAggregator<true, false, true, OMNI_CONTAINER, OMNI_DECIMAL128>;
template class SumAggregator<true, true, false, OMNI_CONTAINER, OMNI_DECIMAL128>;
template class SumAggregator<true, true, true, OMNI_CONTAINER, OMNI_DECIMAL128>;

template class SumAggregator<false, false, false, OMNI_CONTAINER, OMNI_DECIMAL64>;
template class SumAggregator<false, false, true, OMNI_CONTAINER, OMNI_DECIMAL64>;
template class SumAggregator<false, true, false, OMNI_CONTAINER, OMNI_DECIMAL64>;
template class SumAggregator<false, true, true, OMNI_CONTAINER, OMNI_DECIMAL64>;
template class SumAggregator<true, false, false, OMNI_CONTAINER, OMNI_DECIMAL64>;
template class SumAggregator<true, false, true, OMNI_CONTAINER, OMNI_DECIMAL64>;
template class SumAggregator<true, true, false, OMNI_CONTAINER, OMNI_DECIMAL64>;
template class SumAggregator<true, true, true, OMNI_CONTAINER, OMNI_DECIMAL64>;

template class SumAggregator<false, false, false, OMNI_CONTAINER, OMNI_VARCHAR>;
template class SumAggregator<false, false, true, OMNI_CONTAINER, OMNI_VARCHAR>;
template class SumAggregator<false, true, false, OMNI_CONTAINER, OMNI_VARCHAR>;
template class SumAggregator<false, true, true, OMNI_CONTAINER, OMNI_VARCHAR>;
template class SumAggregator<true, false, false, OMNI_CONTAINER, OMNI_VARCHAR>;
template class SumAggregator<true, false, true, OMNI_CONTAINER, OMNI_VARCHAR>;
template class SumAggregator<true, true, false, OMNI_CONTAINER, OMNI_VARCHAR>;
template class SumAggregator<true, true, true, OMNI_CONTAINER, OMNI_VARCHAR>;

template class SumAggregator<false, false, false, OMNI_CONTAINER, OMNI_CONTAINER>;
template class SumAggregator<false, false, true, OMNI_CONTAINER, OMNI_CONTAINER>;
template class SumAggregator<false, true, false, OMNI_CONTAINER, OMNI_CONTAINER>;
template class SumAggregator<false, true, true, OMNI_CONTAINER, OMNI_CONTAINER>;
template class SumAggregator<true, false, false, OMNI_CONTAINER, OMNI_CONTAINER>;
template class SumAggregator<true, false, true, OMNI_CONTAINER, OMNI_CONTAINER>;
template class SumAggregator<true, true, false, OMNI_CONTAINER, OMNI_CONTAINER>;
template class SumAggregator<true, true, true, OMNI_CONTAINER, OMNI_CONTAINER>;
}
}
