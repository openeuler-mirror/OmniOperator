/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Min aggregate
 */

#include "min_aggregator.h"
#ifdef ENABLE_HMPP
#include "aggregator_util.h"
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
#ifdef ENABLE_HMPP
template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
void MinAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::ProcessGroupWithHMPP(AggregateState &state,
    VectorBatch *vectorBatch)
{
    auto vector = vectorBatch->GetVector(this->channels[0]);

    auto vectorValues = vector->GetValues();
    auto positionOffset = vector->GetPositionOffset();
    auto rowCount = vector->GetSize();
    auto inputTypeId = this->inputTypes.GetType(0)->GetId();
    auto outputTypeId = this->outputTypes.GetType(0)->GetId();

    HmppResult result = HMPP_STS_NO_ERR;
    auto minVal = reinterpret_cast<InType *>(this->executionContext->GetArena()->Allocate(sizeof(InType)));
    switch (inputTypeId) {
        case OMNI_SHORT: {
            LogDebug("HMPP-Agg-min");
            result = HMPPS_Min_16s(static_cast<int16_t *>(static_cast<int16_t *>(vectorValues) + positionOffset),
                rowCount, reinterpret_cast<int16_t *>(minVal));
            if (outputTypeId == OMNI_LONG) {
                *minVal = *reinterpret_cast<int16_t *>(minVal);
            }
            break;
        }
        case OMNI_INT:
        case OMNI_DATE32: {
            LogDebug("HMPP-Agg-min");
            result = HMPPS_Min_32s(static_cast<int32_t *>(static_cast<int32_t *>(vectorValues) + positionOffset),
                rowCount, reinterpret_cast<int32_t *>(minVal));
            if (outputTypeId == OMNI_LONG) {
                *minVal = *reinterpret_cast<int32_t *>(minVal);
            }
            break;
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            LogDebug("HMPP-Agg-min");
            result = HMPPS_Min_64s(static_cast<int64_t *>(static_cast<int64_t *>(vectorValues) + positionOffset),
                rowCount, reinterpret_cast<int64_t *>(minVal));
            break;
        }
        case OMNI_DOUBLE: {
            LogDebug("HMPP-Agg-min");
            result = HMPPS_Min_64f(static_cast<double *>(static_cast<double *>(vectorValues) + positionOffset),
                rowCount, reinterpret_cast<double *>(minVal));
            break;
        }
        case OMNI_DECIMAL128: {
            LogDebug("HMPP-Agg-min");
            result = HMPPS_Min_decimal(
                static_cast<HmppDecimal128 *>(static_cast<HmppDecimal128 *>(vectorValues) + positionOffset), rowCount,
                reinterpret_cast<HmppDecimal128 *>(minVal));
            break;
        }
        default: {
            throw OmniException("NOT SUPPORT", "Unsupported input type for min aggregate");
            break;
        }
    }

    if (result != HMPP_STS_NO_ERR) {
        throw OmniException("HMPP ERROR", "min failed for hmpp error");
    }
    if (state.val == nullptr) {
        state.val = minVal;
    } else {
        auto preMinVal = static_cast<InType *>(state.val);
        *static_cast<InType *>(state.val) = (Compare(*preMinVal, *minVal) == -1) ? *preMinVal : *minVal;
    }
    // hmpp only works on not nullable columns, so it always find min
    state.count = 1;
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
bool MinAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::CanProcessWithHMPP(AggregateState &state,
    VectorBatch *vectorBatch)
{
    // just support raw input data and must no null inpout
    if constexpr (!RAW_IN) {
        return false;
    } else {
        if (vectorBatch->GetVector(this->channels[0])->MayHaveNull()) {
            return false;
        }
        // not accept dictionnary vector
        if (vectorBatch->GetVector(this->channels[0])->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            return false;
        }
        // type check with whitelist for min
        auto inputTypeId = this->inputTypes.GetType(0)->GetId();
        return AggregatorUtil::IsHMPPMaxMinSupportDataTypeId(inputTypeId);
    }
}
#endif

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
void MinAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::ExtractValues(const AggregateState &state,
    std::vector<Vector *> &vectors, int32_t rowIndex)
{
    int32_t offset;
    auto v = static_cast<OutVector *>(VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset));
    if (state.count == 0 || (state.count > 0 && state.val == nullptr)) {
        v->SetValueNull(offset);
        return;
    }

    bool overflow = state.count < 0;
    OutType result =
        this->template CastWithOverflow<ResultType, OutType>(*reinterpret_cast<ResultType *>(state.val), overflow);
    v->SetValue(offset, result);
    if (overflow) {
        this->SetNullOrThrowException(v, offset, "min_aggregator overflow.");
    } else if (state.count == 0 || state.val == nullptr) {
        v->SetValueNull(offset);
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
void MinAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::InitState(AggregateState &state)
{
    state.val = this->executionContext->GetArena()->Allocate(sizeof(ResultType));
    *reinterpret_cast<ResultType *>(state.val) = GetMax<ResultType>();
    state.count = 0;
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
void MinAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState &state,
    Vector *vector, const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap)
{
    if (state.val == nullptr) {
        InitState(state);
    }
    ResultType *res = reinterpret_cast<ResultType *>(state.val);

    InType *ptr = reinterpret_cast<InType *>(static_cast<InVector *>(vector)->GetValues());
    ptr += vector->GetPositionOffset();

    if (indexMap == nullptr) {
        ptr += rowOffset;
        if (nullMap == nullptr) {
            Add<InType, ResultType, MinOp<InType, ResultType>>(res, state.count, ptr, rowCount);
        } else {
            AddConditional<InType, ResultType, MinConditionalOp<InType, ResultType, false>>(res, state.count, ptr,
                rowCount, nullMap);
        }
    } else {
        if (nullMap == nullptr) {
            AddDict<InType, ResultType, MinOp<InType, ResultType>>(res, state.count, ptr, rowCount, indexMap);
        } else {
            AddDictConditional<InType, ResultType, MinConditionalOp<InType, ResultType, false>>(res, state.count, ptr,
                rowCount, nullMap, indexMap);
        }
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
void MinAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::ProcessGroupInternal(
    std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector, const int32_t rowOffset,
    const uint8_t *nullMap, const int32_t *indexMap)
{
    InType *ptr = reinterpret_cast<InType *>(static_cast<InVector *>(vector)->GetValues());
    ptr += vector->GetPositionOffset();

    if (indexMap == nullptr) {
        ptr += rowOffset;
        if (nullMap == nullptr) {
            AddUseRowIndex<InType, ResultType, MinOp<InType, ResultType>>(rowStates, aggIdx, ptr);
        } else {
            AddConditionalUseRowIndex<InType, ResultType, MinConditionalOp<InType, ResultType, false>>(rowStates,
                aggIdx, ptr, nullMap);
        }
    } else {
        if (nullMap == nullptr) {
            AddDictUseRowIndex<InType, ResultType, MinOp<InType, ResultType>>(rowStates, aggIdx, ptr, indexMap);
        } else {
            AddDictConditionalUseRowIndex<InType, ResultType, MinConditionalOp<InType, ResultType, false>>(rowStates,
                aggIdx, ptr, nullMap, indexMap);
        }
    }
}

// Explicit template instantiation
// Defining templated aggregators in header file consume a lot of memory during compilation
// since, compiler needs to generate each individual template instance wherever aggregator header is include
// to reduce time and memory usage during compilation moved templated aggregator implementation into .cpp files
// and used explicit template instantiation to generate template instances
template class MinAggregator<false, false, false, OMNI_BOOLEAN, OMNI_BOOLEAN>;
template class MinAggregator<false, false, true, OMNI_BOOLEAN, OMNI_BOOLEAN>;
template class MinAggregator<false, true, false, OMNI_BOOLEAN, OMNI_BOOLEAN>;
template class MinAggregator<false, true, true, OMNI_BOOLEAN, OMNI_BOOLEAN>;
template class MinAggregator<true, false, false, OMNI_BOOLEAN, OMNI_BOOLEAN>;
template class MinAggregator<true, false, true, OMNI_BOOLEAN, OMNI_BOOLEAN>;
template class MinAggregator<true, true, false, OMNI_BOOLEAN, OMNI_BOOLEAN>;
template class MinAggregator<true, true, true, OMNI_BOOLEAN, OMNI_BOOLEAN>;

template class MinAggregator<false, false, false, OMNI_BOOLEAN, OMNI_SHORT>;
template class MinAggregator<false, false, true, OMNI_BOOLEAN, OMNI_SHORT>;
template class MinAggregator<false, true, false, OMNI_BOOLEAN, OMNI_SHORT>;
template class MinAggregator<false, true, true, OMNI_BOOLEAN, OMNI_SHORT>;
template class MinAggregator<true, false, false, OMNI_BOOLEAN, OMNI_SHORT>;
template class MinAggregator<true, false, true, OMNI_BOOLEAN, OMNI_SHORT>;
template class MinAggregator<true, true, false, OMNI_BOOLEAN, OMNI_SHORT>;
template class MinAggregator<true, true, true, OMNI_BOOLEAN, OMNI_SHORT>;

template class MinAggregator<false, false, false, OMNI_BOOLEAN, OMNI_INT>;
template class MinAggregator<false, false, true, OMNI_BOOLEAN, OMNI_INT>;
template class MinAggregator<false, true, false, OMNI_BOOLEAN, OMNI_INT>;
template class MinAggregator<false, true, true, OMNI_BOOLEAN, OMNI_INT>;
template class MinAggregator<true, false, false, OMNI_BOOLEAN, OMNI_INT>;
template class MinAggregator<true, false, true, OMNI_BOOLEAN, OMNI_INT>;
template class MinAggregator<true, true, false, OMNI_BOOLEAN, OMNI_INT>;
template class MinAggregator<true, true, true, OMNI_BOOLEAN, OMNI_INT>;

template class MinAggregator<false, false, false, OMNI_BOOLEAN, OMNI_LONG>;
template class MinAggregator<false, false, true, OMNI_BOOLEAN, OMNI_LONG>;
template class MinAggregator<false, true, false, OMNI_BOOLEAN, OMNI_LONG>;
template class MinAggregator<false, true, true, OMNI_BOOLEAN, OMNI_LONG>;
template class MinAggregator<true, false, false, OMNI_BOOLEAN, OMNI_LONG>;
template class MinAggregator<true, false, true, OMNI_BOOLEAN, OMNI_LONG>;
template class MinAggregator<true, true, false, OMNI_BOOLEAN, OMNI_LONG>;
template class MinAggregator<true, true, true, OMNI_BOOLEAN, OMNI_LONG>;

template class MinAggregator<false, false, false, OMNI_BOOLEAN, OMNI_DOUBLE>;
template class MinAggregator<false, false, true, OMNI_BOOLEAN, OMNI_DOUBLE>;
template class MinAggregator<false, true, false, OMNI_BOOLEAN, OMNI_DOUBLE>;
template class MinAggregator<false, true, true, OMNI_BOOLEAN, OMNI_DOUBLE>;
template class MinAggregator<true, false, false, OMNI_BOOLEAN, OMNI_DOUBLE>;
template class MinAggregator<true, false, true, OMNI_BOOLEAN, OMNI_DOUBLE>;
template class MinAggregator<true, true, false, OMNI_BOOLEAN, OMNI_DOUBLE>;
template class MinAggregator<true, true, true, OMNI_BOOLEAN, OMNI_DOUBLE>;

template class MinAggregator<false, false, false, OMNI_BOOLEAN, OMNI_DECIMAL128>;
template class MinAggregator<false, false, true, OMNI_BOOLEAN, OMNI_DECIMAL128>;
template class MinAggregator<false, true, false, OMNI_BOOLEAN, OMNI_DECIMAL128>;
template class MinAggregator<false, true, true, OMNI_BOOLEAN, OMNI_DECIMAL128>;
template class MinAggregator<true, false, false, OMNI_BOOLEAN, OMNI_DECIMAL128>;
template class MinAggregator<true, false, true, OMNI_BOOLEAN, OMNI_DECIMAL128>;
template class MinAggregator<true, true, false, OMNI_BOOLEAN, OMNI_DECIMAL128>;
template class MinAggregator<true, true, true, OMNI_BOOLEAN, OMNI_DECIMAL128>;

template class MinAggregator<false, false, false, OMNI_BOOLEAN, OMNI_DECIMAL64>;
template class MinAggregator<false, false, true, OMNI_BOOLEAN, OMNI_DECIMAL64>;
template class MinAggregator<false, true, false, OMNI_BOOLEAN, OMNI_DECIMAL64>;
template class MinAggregator<false, true, true, OMNI_BOOLEAN, OMNI_DECIMAL64>;
template class MinAggregator<true, false, false, OMNI_BOOLEAN, OMNI_DECIMAL64>;
template class MinAggregator<true, false, true, OMNI_BOOLEAN, OMNI_DECIMAL64>;
template class MinAggregator<true, true, false, OMNI_BOOLEAN, OMNI_DECIMAL64>;
template class MinAggregator<true, true, true, OMNI_BOOLEAN, OMNI_DECIMAL64>;


template class MinAggregator<false, false, false, OMNI_SHORT, OMNI_BOOLEAN>;
template class MinAggregator<false, false, true, OMNI_SHORT, OMNI_BOOLEAN>;
template class MinAggregator<false, true, false, OMNI_SHORT, OMNI_BOOLEAN>;
template class MinAggregator<false, true, true, OMNI_SHORT, OMNI_BOOLEAN>;
template class MinAggregator<true, false, false, OMNI_SHORT, OMNI_BOOLEAN>;
template class MinAggregator<true, false, true, OMNI_SHORT, OMNI_BOOLEAN>;
template class MinAggregator<true, true, false, OMNI_SHORT, OMNI_BOOLEAN>;
template class MinAggregator<true, true, true, OMNI_SHORT, OMNI_BOOLEAN>;

template class MinAggregator<false, false, false, OMNI_SHORT, OMNI_SHORT>;
template class MinAggregator<false, false, true, OMNI_SHORT, OMNI_SHORT>;
template class MinAggregator<false, true, false, OMNI_SHORT, OMNI_SHORT>;
template class MinAggregator<false, true, true, OMNI_SHORT, OMNI_SHORT>;
template class MinAggregator<true, false, false, OMNI_SHORT, OMNI_SHORT>;
template class MinAggregator<true, false, true, OMNI_SHORT, OMNI_SHORT>;
template class MinAggregator<true, true, false, OMNI_SHORT, OMNI_SHORT>;
template class MinAggregator<true, true, true, OMNI_SHORT, OMNI_SHORT>;

template class MinAggregator<false, false, false, OMNI_SHORT, OMNI_INT>;
template class MinAggregator<false, false, true, OMNI_SHORT, OMNI_INT>;
template class MinAggregator<false, true, false, OMNI_SHORT, OMNI_INT>;
template class MinAggregator<false, true, true, OMNI_SHORT, OMNI_INT>;
template class MinAggregator<true, false, false, OMNI_SHORT, OMNI_INT>;
template class MinAggregator<true, false, true, OMNI_SHORT, OMNI_INT>;
template class MinAggregator<true, true, false, OMNI_SHORT, OMNI_INT>;
template class MinAggregator<true, true, true, OMNI_SHORT, OMNI_INT>;

template class MinAggregator<false, false, false, OMNI_SHORT, OMNI_LONG>;
template class MinAggregator<false, false, true, OMNI_SHORT, OMNI_LONG>;
template class MinAggregator<false, true, false, OMNI_SHORT, OMNI_LONG>;
template class MinAggregator<false, true, true, OMNI_SHORT, OMNI_LONG>;
template class MinAggregator<true, false, false, OMNI_SHORT, OMNI_LONG>;
template class MinAggregator<true, false, true, OMNI_SHORT, OMNI_LONG>;
template class MinAggregator<true, true, false, OMNI_SHORT, OMNI_LONG>;
template class MinAggregator<true, true, true, OMNI_SHORT, OMNI_LONG>;

template class MinAggregator<false, false, false, OMNI_SHORT, OMNI_DOUBLE>;
template class MinAggregator<false, false, true, OMNI_SHORT, OMNI_DOUBLE>;
template class MinAggregator<false, true, false, OMNI_SHORT, OMNI_DOUBLE>;
template class MinAggregator<false, true, true, OMNI_SHORT, OMNI_DOUBLE>;
template class MinAggregator<true, false, false, OMNI_SHORT, OMNI_DOUBLE>;
template class MinAggregator<true, false, true, OMNI_SHORT, OMNI_DOUBLE>;
template class MinAggregator<true, true, false, OMNI_SHORT, OMNI_DOUBLE>;
template class MinAggregator<true, true, true, OMNI_SHORT, OMNI_DOUBLE>;

template class MinAggregator<false, false, false, OMNI_SHORT, OMNI_DECIMAL128>;
template class MinAggregator<false, false, true, OMNI_SHORT, OMNI_DECIMAL128>;
template class MinAggregator<false, true, false, OMNI_SHORT, OMNI_DECIMAL128>;
template class MinAggregator<false, true, true, OMNI_SHORT, OMNI_DECIMAL128>;
template class MinAggregator<true, false, false, OMNI_SHORT, OMNI_DECIMAL128>;
template class MinAggregator<true, false, true, OMNI_SHORT, OMNI_DECIMAL128>;
template class MinAggregator<true, true, false, OMNI_SHORT, OMNI_DECIMAL128>;
template class MinAggregator<true, true, true, OMNI_SHORT, OMNI_DECIMAL128>;

template class MinAggregator<false, false, false, OMNI_SHORT, OMNI_DECIMAL64>;
template class MinAggregator<false, false, true, OMNI_SHORT, OMNI_DECIMAL64>;
template class MinAggregator<false, true, false, OMNI_SHORT, OMNI_DECIMAL64>;
template class MinAggregator<false, true, true, OMNI_SHORT, OMNI_DECIMAL64>;
template class MinAggregator<true, false, false, OMNI_SHORT, OMNI_DECIMAL64>;
template class MinAggregator<true, false, true, OMNI_SHORT, OMNI_DECIMAL64>;
template class MinAggregator<true, true, false, OMNI_SHORT, OMNI_DECIMAL64>;
template class MinAggregator<true, true, true, OMNI_SHORT, OMNI_DECIMAL64>;


template class MinAggregator<false, false, false, OMNI_INT, OMNI_BOOLEAN>;
template class MinAggregator<false, false, true, OMNI_INT, OMNI_BOOLEAN>;
template class MinAggregator<false, true, false, OMNI_INT, OMNI_BOOLEAN>;
template class MinAggregator<false, true, true, OMNI_INT, OMNI_BOOLEAN>;
template class MinAggregator<true, false, false, OMNI_INT, OMNI_BOOLEAN>;
template class MinAggregator<true, false, true, OMNI_INT, OMNI_BOOLEAN>;
template class MinAggregator<true, true, false, OMNI_INT, OMNI_BOOLEAN>;
template class MinAggregator<true, true, true, OMNI_INT, OMNI_BOOLEAN>;

template class MinAggregator<false, false, false, OMNI_INT, OMNI_SHORT>;
template class MinAggregator<false, false, true, OMNI_INT, OMNI_SHORT>;
template class MinAggregator<false, true, false, OMNI_INT, OMNI_SHORT>;
template class MinAggregator<false, true, true, OMNI_INT, OMNI_SHORT>;
template class MinAggregator<true, false, false, OMNI_INT, OMNI_SHORT>;
template class MinAggregator<true, false, true, OMNI_INT, OMNI_SHORT>;
template class MinAggregator<true, true, false, OMNI_INT, OMNI_SHORT>;
template class MinAggregator<true, true, true, OMNI_INT, OMNI_SHORT>;

template class MinAggregator<false, false, false, OMNI_INT, OMNI_INT>;
template class MinAggregator<false, false, true, OMNI_INT, OMNI_INT>;
template class MinAggregator<false, true, false, OMNI_INT, OMNI_INT>;
template class MinAggregator<false, true, true, OMNI_INT, OMNI_INT>;
template class MinAggregator<true, false, false, OMNI_INT, OMNI_INT>;
template class MinAggregator<true, false, true, OMNI_INT, OMNI_INT>;
template class MinAggregator<true, true, false, OMNI_INT, OMNI_INT>;
template class MinAggregator<true, true, true, OMNI_INT, OMNI_INT>;

template class MinAggregator<false, false, false, OMNI_INT, OMNI_LONG>;
template class MinAggregator<false, false, true, OMNI_INT, OMNI_LONG>;
template class MinAggregator<false, true, false, OMNI_INT, OMNI_LONG>;
template class MinAggregator<false, true, true, OMNI_INT, OMNI_LONG>;
template class MinAggregator<true, false, false, OMNI_INT, OMNI_LONG>;
template class MinAggregator<true, false, true, OMNI_INT, OMNI_LONG>;
template class MinAggregator<true, true, false, OMNI_INT, OMNI_LONG>;
template class MinAggregator<true, true, true, OMNI_INT, OMNI_LONG>;

template class MinAggregator<false, false, false, OMNI_INT, OMNI_DOUBLE>;
template class MinAggregator<false, false, true, OMNI_INT, OMNI_DOUBLE>;
template class MinAggregator<false, true, false, OMNI_INT, OMNI_DOUBLE>;
template class MinAggregator<false, true, true, OMNI_INT, OMNI_DOUBLE>;
template class MinAggregator<true, false, false, OMNI_INT, OMNI_DOUBLE>;
template class MinAggregator<true, false, true, OMNI_INT, OMNI_DOUBLE>;
template class MinAggregator<true, true, false, OMNI_INT, OMNI_DOUBLE>;
template class MinAggregator<true, true, true, OMNI_INT, OMNI_DOUBLE>;

template class MinAggregator<false, false, false, OMNI_INT, OMNI_DECIMAL128>;
template class MinAggregator<false, false, true, OMNI_INT, OMNI_DECIMAL128>;
template class MinAggregator<false, true, false, OMNI_INT, OMNI_DECIMAL128>;
template class MinAggregator<false, true, true, OMNI_INT, OMNI_DECIMAL128>;
template class MinAggregator<true, false, false, OMNI_INT, OMNI_DECIMAL128>;
template class MinAggregator<true, false, true, OMNI_INT, OMNI_DECIMAL128>;
template class MinAggregator<true, true, false, OMNI_INT, OMNI_DECIMAL128>;
template class MinAggregator<true, true, true, OMNI_INT, OMNI_DECIMAL128>;

template class MinAggregator<false, false, false, OMNI_INT, OMNI_DECIMAL64>;
template class MinAggregator<false, false, true, OMNI_INT, OMNI_DECIMAL64>;
template class MinAggregator<false, true, false, OMNI_INT, OMNI_DECIMAL64>;
template class MinAggregator<false, true, true, OMNI_INT, OMNI_DECIMAL64>;
template class MinAggregator<true, false, false, OMNI_INT, OMNI_DECIMAL64>;
template class MinAggregator<true, false, true, OMNI_INT, OMNI_DECIMAL64>;
template class MinAggregator<true, true, false, OMNI_INT, OMNI_DECIMAL64>;
template class MinAggregator<true, true, true, OMNI_INT, OMNI_DECIMAL64>;


template class MinAggregator<false, false, false, OMNI_LONG, OMNI_BOOLEAN>;
template class MinAggregator<false, false, true, OMNI_LONG, OMNI_BOOLEAN>;
template class MinAggregator<false, true, false, OMNI_LONG, OMNI_BOOLEAN>;
template class MinAggregator<false, true, true, OMNI_LONG, OMNI_BOOLEAN>;
template class MinAggregator<true, false, false, OMNI_LONG, OMNI_BOOLEAN>;
template class MinAggregator<true, false, true, OMNI_LONG, OMNI_BOOLEAN>;
template class MinAggregator<true, true, false, OMNI_LONG, OMNI_BOOLEAN>;
template class MinAggregator<true, true, true, OMNI_LONG, OMNI_BOOLEAN>;

template class MinAggregator<false, false, false, OMNI_LONG, OMNI_SHORT>;
template class MinAggregator<false, false, true, OMNI_LONG, OMNI_SHORT>;
template class MinAggregator<false, true, false, OMNI_LONG, OMNI_SHORT>;
template class MinAggregator<false, true, true, OMNI_LONG, OMNI_SHORT>;
template class MinAggregator<true, false, false, OMNI_LONG, OMNI_SHORT>;
template class MinAggregator<true, false, true, OMNI_LONG, OMNI_SHORT>;
template class MinAggregator<true, true, false, OMNI_LONG, OMNI_SHORT>;
template class MinAggregator<true, true, true, OMNI_LONG, OMNI_SHORT>;

template class MinAggregator<false, false, false, OMNI_LONG, OMNI_INT>;
template class MinAggregator<false, false, true, OMNI_LONG, OMNI_INT>;
template class MinAggregator<false, true, false, OMNI_LONG, OMNI_INT>;
template class MinAggregator<false, true, true, OMNI_LONG, OMNI_INT>;
template class MinAggregator<true, false, false, OMNI_LONG, OMNI_INT>;
template class MinAggregator<true, false, true, OMNI_LONG, OMNI_INT>;
template class MinAggregator<true, true, false, OMNI_LONG, OMNI_INT>;
template class MinAggregator<true, true, true, OMNI_LONG, OMNI_INT>;

template class MinAggregator<false, false, false, OMNI_LONG, OMNI_LONG>;
template class MinAggregator<false, false, true, OMNI_LONG, OMNI_LONG>;
template class MinAggregator<false, true, false, OMNI_LONG, OMNI_LONG>;
template class MinAggregator<false, true, true, OMNI_LONG, OMNI_LONG>;
template class MinAggregator<true, false, false, OMNI_LONG, OMNI_LONG>;
template class MinAggregator<true, false, true, OMNI_LONG, OMNI_LONG>;
template class MinAggregator<true, true, false, OMNI_LONG, OMNI_LONG>;
template class MinAggregator<true, true, true, OMNI_LONG, OMNI_LONG>;

template class MinAggregator<false, false, false, OMNI_LONG, OMNI_DOUBLE>;
template class MinAggregator<false, false, true, OMNI_LONG, OMNI_DOUBLE>;
template class MinAggregator<false, true, false, OMNI_LONG, OMNI_DOUBLE>;
template class MinAggregator<false, true, true, OMNI_LONG, OMNI_DOUBLE>;
template class MinAggregator<true, false, false, OMNI_LONG, OMNI_DOUBLE>;
template class MinAggregator<true, false, true, OMNI_LONG, OMNI_DOUBLE>;
template class MinAggregator<true, true, false, OMNI_LONG, OMNI_DOUBLE>;
template class MinAggregator<true, true, true, OMNI_LONG, OMNI_DOUBLE>;

template class MinAggregator<false, false, false, OMNI_LONG, OMNI_DECIMAL128>;
template class MinAggregator<false, false, true, OMNI_LONG, OMNI_DECIMAL128>;
template class MinAggregator<false, true, false, OMNI_LONG, OMNI_DECIMAL128>;
template class MinAggregator<false, true, true, OMNI_LONG, OMNI_DECIMAL128>;
template class MinAggregator<true, false, false, OMNI_LONG, OMNI_DECIMAL128>;
template class MinAggregator<true, false, true, OMNI_LONG, OMNI_DECIMAL128>;
template class MinAggregator<true, true, false, OMNI_LONG, OMNI_DECIMAL128>;
template class MinAggregator<true, true, true, OMNI_LONG, OMNI_DECIMAL128>;

template class MinAggregator<false, false, false, OMNI_LONG, OMNI_DECIMAL64>;
template class MinAggregator<false, false, true, OMNI_LONG, OMNI_DECIMAL64>;
template class MinAggregator<false, true, false, OMNI_LONG, OMNI_DECIMAL64>;
template class MinAggregator<false, true, true, OMNI_LONG, OMNI_DECIMAL64>;
template class MinAggregator<true, false, false, OMNI_LONG, OMNI_DECIMAL64>;
template class MinAggregator<true, false, true, OMNI_LONG, OMNI_DECIMAL64>;
template class MinAggregator<true, true, false, OMNI_LONG, OMNI_DECIMAL64>;
template class MinAggregator<true, true, true, OMNI_LONG, OMNI_DECIMAL64>;


template class MinAggregator<false, false, false, OMNI_DOUBLE, OMNI_BOOLEAN>;
template class MinAggregator<false, false, true, OMNI_DOUBLE, OMNI_BOOLEAN>;
template class MinAggregator<false, true, false, OMNI_DOUBLE, OMNI_BOOLEAN>;
template class MinAggregator<false, true, true, OMNI_DOUBLE, OMNI_BOOLEAN>;
template class MinAggregator<true, false, false, OMNI_DOUBLE, OMNI_BOOLEAN>;
template class MinAggregator<true, false, true, OMNI_DOUBLE, OMNI_BOOLEAN>;
template class MinAggregator<true, true, false, OMNI_DOUBLE, OMNI_BOOLEAN>;
template class MinAggregator<true, true, true, OMNI_DOUBLE, OMNI_BOOLEAN>;

template class MinAggregator<false, false, false, OMNI_DOUBLE, OMNI_SHORT>;
template class MinAggregator<false, false, true, OMNI_DOUBLE, OMNI_SHORT>;
template class MinAggregator<false, true, false, OMNI_DOUBLE, OMNI_SHORT>;
template class MinAggregator<false, true, true, OMNI_DOUBLE, OMNI_SHORT>;
template class MinAggregator<true, false, false, OMNI_DOUBLE, OMNI_SHORT>;
template class MinAggregator<true, false, true, OMNI_DOUBLE, OMNI_SHORT>;
template class MinAggregator<true, true, false, OMNI_DOUBLE, OMNI_SHORT>;
template class MinAggregator<true, true, true, OMNI_DOUBLE, OMNI_SHORT>;

template class MinAggregator<false, false, false, OMNI_DOUBLE, OMNI_INT>;
template class MinAggregator<false, false, true, OMNI_DOUBLE, OMNI_INT>;
template class MinAggregator<false, true, false, OMNI_DOUBLE, OMNI_INT>;
template class MinAggregator<false, true, true, OMNI_DOUBLE, OMNI_INT>;
template class MinAggregator<true, false, false, OMNI_DOUBLE, OMNI_INT>;
template class MinAggregator<true, false, true, OMNI_DOUBLE, OMNI_INT>;
template class MinAggregator<true, true, false, OMNI_DOUBLE, OMNI_INT>;
template class MinAggregator<true, true, true, OMNI_DOUBLE, OMNI_INT>;

template class MinAggregator<false, false, false, OMNI_DOUBLE, OMNI_LONG>;
template class MinAggregator<false, false, true, OMNI_DOUBLE, OMNI_LONG>;
template class MinAggregator<false, true, false, OMNI_DOUBLE, OMNI_LONG>;
template class MinAggregator<false, true, true, OMNI_DOUBLE, OMNI_LONG>;
template class MinAggregator<true, false, false, OMNI_DOUBLE, OMNI_LONG>;
template class MinAggregator<true, false, true, OMNI_DOUBLE, OMNI_LONG>;
template class MinAggregator<true, true, false, OMNI_DOUBLE, OMNI_LONG>;
template class MinAggregator<true, true, true, OMNI_DOUBLE, OMNI_LONG>;

template class MinAggregator<false, false, false, OMNI_DOUBLE, OMNI_DOUBLE>;
template class MinAggregator<false, false, true, OMNI_DOUBLE, OMNI_DOUBLE>;
template class MinAggregator<false, true, false, OMNI_DOUBLE, OMNI_DOUBLE>;
template class MinAggregator<false, true, true, OMNI_DOUBLE, OMNI_DOUBLE>;
template class MinAggregator<true, false, false, OMNI_DOUBLE, OMNI_DOUBLE>;
template class MinAggregator<true, false, true, OMNI_DOUBLE, OMNI_DOUBLE>;
template class MinAggregator<true, true, false, OMNI_DOUBLE, OMNI_DOUBLE>;
template class MinAggregator<true, true, true, OMNI_DOUBLE, OMNI_DOUBLE>;

template class MinAggregator<false, false, false, OMNI_DOUBLE, OMNI_DECIMAL128>;
template class MinAggregator<false, false, true, OMNI_DOUBLE, OMNI_DECIMAL128>;
template class MinAggregator<false, true, false, OMNI_DOUBLE, OMNI_DECIMAL128>;
template class MinAggregator<false, true, true, OMNI_DOUBLE, OMNI_DECIMAL128>;
template class MinAggregator<true, false, false, OMNI_DOUBLE, OMNI_DECIMAL128>;
template class MinAggregator<true, false, true, OMNI_DOUBLE, OMNI_DECIMAL128>;
template class MinAggregator<true, true, false, OMNI_DOUBLE, OMNI_DECIMAL128>;
template class MinAggregator<true, true, true, OMNI_DOUBLE, OMNI_DECIMAL128>;

template class MinAggregator<false, false, false, OMNI_DOUBLE, OMNI_DECIMAL64>;
template class MinAggregator<false, false, true, OMNI_DOUBLE, OMNI_DECIMAL64>;
template class MinAggregator<false, true, false, OMNI_DOUBLE, OMNI_DECIMAL64>;
template class MinAggregator<false, true, true, OMNI_DOUBLE, OMNI_DECIMAL64>;
template class MinAggregator<true, false, false, OMNI_DOUBLE, OMNI_DECIMAL64>;
template class MinAggregator<true, false, true, OMNI_DOUBLE, OMNI_DECIMAL64>;
template class MinAggregator<true, true, false, OMNI_DOUBLE, OMNI_DECIMAL64>;
template class MinAggregator<true, true, true, OMNI_DOUBLE, OMNI_DECIMAL64>;


template class MinAggregator<false, false, false, OMNI_DECIMAL128, OMNI_BOOLEAN>;
template class MinAggregator<false, false, true, OMNI_DECIMAL128, OMNI_BOOLEAN>;
template class MinAggregator<false, true, false, OMNI_DECIMAL128, OMNI_BOOLEAN>;
template class MinAggregator<false, true, true, OMNI_DECIMAL128, OMNI_BOOLEAN>;
template class MinAggregator<true, false, false, OMNI_DECIMAL128, OMNI_BOOLEAN>;
template class MinAggregator<true, false, true, OMNI_DECIMAL128, OMNI_BOOLEAN>;
template class MinAggregator<true, true, false, OMNI_DECIMAL128, OMNI_BOOLEAN>;
template class MinAggregator<true, true, true, OMNI_DECIMAL128, OMNI_BOOLEAN>;

template class MinAggregator<false, false, false, OMNI_DECIMAL128, OMNI_SHORT>;
template class MinAggregator<false, false, true, OMNI_DECIMAL128, OMNI_SHORT>;
template class MinAggregator<false, true, false, OMNI_DECIMAL128, OMNI_SHORT>;
template class MinAggregator<false, true, true, OMNI_DECIMAL128, OMNI_SHORT>;
template class MinAggregator<true, false, false, OMNI_DECIMAL128, OMNI_SHORT>;
template class MinAggregator<true, false, true, OMNI_DECIMAL128, OMNI_SHORT>;
template class MinAggregator<true, true, false, OMNI_DECIMAL128, OMNI_SHORT>;
template class MinAggregator<true, true, true, OMNI_DECIMAL128, OMNI_SHORT>;

template class MinAggregator<false, false, false, OMNI_DECIMAL128, OMNI_INT>;
template class MinAggregator<false, false, true, OMNI_DECIMAL128, OMNI_INT>;
template class MinAggregator<false, true, false, OMNI_DECIMAL128, OMNI_INT>;
template class MinAggregator<false, true, true, OMNI_DECIMAL128, OMNI_INT>;
template class MinAggregator<true, false, false, OMNI_DECIMAL128, OMNI_INT>;
template class MinAggregator<true, false, true, OMNI_DECIMAL128, OMNI_INT>;
template class MinAggregator<true, true, false, OMNI_DECIMAL128, OMNI_INT>;
template class MinAggregator<true, true, true, OMNI_DECIMAL128, OMNI_INT>;

template class MinAggregator<false, false, false, OMNI_DECIMAL128, OMNI_LONG>;
template class MinAggregator<false, false, true, OMNI_DECIMAL128, OMNI_LONG>;
template class MinAggregator<false, true, false, OMNI_DECIMAL128, OMNI_LONG>;
template class MinAggregator<false, true, true, OMNI_DECIMAL128, OMNI_LONG>;
template class MinAggregator<true, false, false, OMNI_DECIMAL128, OMNI_LONG>;
template class MinAggregator<true, false, true, OMNI_DECIMAL128, OMNI_LONG>;
template class MinAggregator<true, true, false, OMNI_DECIMAL128, OMNI_LONG>;
template class MinAggregator<true, true, true, OMNI_DECIMAL128, OMNI_LONG>;

template class MinAggregator<false, false, false, OMNI_DECIMAL128, OMNI_DOUBLE>;
template class MinAggregator<false, false, true, OMNI_DECIMAL128, OMNI_DOUBLE>;
template class MinAggregator<false, true, false, OMNI_DECIMAL128, OMNI_DOUBLE>;
template class MinAggregator<false, true, true, OMNI_DECIMAL128, OMNI_DOUBLE>;
template class MinAggregator<true, false, false, OMNI_DECIMAL128, OMNI_DOUBLE>;
template class MinAggregator<true, false, true, OMNI_DECIMAL128, OMNI_DOUBLE>;
template class MinAggregator<true, true, false, OMNI_DECIMAL128, OMNI_DOUBLE>;
template class MinAggregator<true, true, true, OMNI_DECIMAL128, OMNI_DOUBLE>;

template class MinAggregator<false, false, false, OMNI_DECIMAL128, OMNI_DECIMAL128>;
template class MinAggregator<false, false, true, OMNI_DECIMAL128, OMNI_DECIMAL128>;
template class MinAggregator<false, true, false, OMNI_DECIMAL128, OMNI_DECIMAL128>;
template class MinAggregator<false, true, true, OMNI_DECIMAL128, OMNI_DECIMAL128>;
template class MinAggregator<true, false, false, OMNI_DECIMAL128, OMNI_DECIMAL128>;
template class MinAggregator<true, false, true, OMNI_DECIMAL128, OMNI_DECIMAL128>;
template class MinAggregator<true, true, false, OMNI_DECIMAL128, OMNI_DECIMAL128>;
template class MinAggregator<true, true, true, OMNI_DECIMAL128, OMNI_DECIMAL128>;

template class MinAggregator<false, false, false, OMNI_DECIMAL128, OMNI_DECIMAL64>;
template class MinAggregator<false, false, true, OMNI_DECIMAL128, OMNI_DECIMAL64>;
template class MinAggregator<false, true, false, OMNI_DECIMAL128, OMNI_DECIMAL64>;
template class MinAggregator<false, true, true, OMNI_DECIMAL128, OMNI_DECIMAL64>;
template class MinAggregator<true, false, false, OMNI_DECIMAL128, OMNI_DECIMAL64>;
template class MinAggregator<true, false, true, OMNI_DECIMAL128, OMNI_DECIMAL64>;
template class MinAggregator<true, true, false, OMNI_DECIMAL128, OMNI_DECIMAL64>;
template class MinAggregator<true, true, true, OMNI_DECIMAL128, OMNI_DECIMAL64>;


template class MinAggregator<false, false, false, OMNI_DECIMAL64, OMNI_BOOLEAN>;
template class MinAggregator<false, false, true, OMNI_DECIMAL64, OMNI_BOOLEAN>;
template class MinAggregator<false, true, false, OMNI_DECIMAL64, OMNI_BOOLEAN>;
template class MinAggregator<false, true, true, OMNI_DECIMAL64, OMNI_BOOLEAN>;
template class MinAggregator<true, false, false, OMNI_DECIMAL64, OMNI_BOOLEAN>;
template class MinAggregator<true, false, true, OMNI_DECIMAL64, OMNI_BOOLEAN>;
template class MinAggregator<true, true, false, OMNI_DECIMAL64, OMNI_BOOLEAN>;
template class MinAggregator<true, true, true, OMNI_DECIMAL64, OMNI_BOOLEAN>;

template class MinAggregator<false, false, false, OMNI_DECIMAL64, OMNI_SHORT>;
template class MinAggregator<false, false, true, OMNI_DECIMAL64, OMNI_SHORT>;
template class MinAggregator<false, true, false, OMNI_DECIMAL64, OMNI_SHORT>;
template class MinAggregator<false, true, true, OMNI_DECIMAL64, OMNI_SHORT>;
template class MinAggregator<true, false, false, OMNI_DECIMAL64, OMNI_SHORT>;
template class MinAggregator<true, false, true, OMNI_DECIMAL64, OMNI_SHORT>;
template class MinAggregator<true, true, false, OMNI_DECIMAL64, OMNI_SHORT>;
template class MinAggregator<true, true, true, OMNI_DECIMAL64, OMNI_SHORT>;

template class MinAggregator<false, false, false, OMNI_DECIMAL64, OMNI_INT>;
template class MinAggregator<false, false, true, OMNI_DECIMAL64, OMNI_INT>;
template class MinAggregator<false, true, false, OMNI_DECIMAL64, OMNI_INT>;
template class MinAggregator<false, true, true, OMNI_DECIMAL64, OMNI_INT>;
template class MinAggregator<true, false, false, OMNI_DECIMAL64, OMNI_INT>;
template class MinAggregator<true, false, true, OMNI_DECIMAL64, OMNI_INT>;
template class MinAggregator<true, true, false, OMNI_DECIMAL64, OMNI_INT>;
template class MinAggregator<true, true, true, OMNI_DECIMAL64, OMNI_INT>;

template class MinAggregator<false, false, false, OMNI_DECIMAL64, OMNI_LONG>;
template class MinAggregator<false, false, true, OMNI_DECIMAL64, OMNI_LONG>;
template class MinAggregator<false, true, false, OMNI_DECIMAL64, OMNI_LONG>;
template class MinAggregator<false, true, true, OMNI_DECIMAL64, OMNI_LONG>;
template class MinAggregator<true, false, false, OMNI_DECIMAL64, OMNI_LONG>;
template class MinAggregator<true, false, true, OMNI_DECIMAL64, OMNI_LONG>;
template class MinAggregator<true, true, false, OMNI_DECIMAL64, OMNI_LONG>;
template class MinAggregator<true, true, true, OMNI_DECIMAL64, OMNI_LONG>;

template class MinAggregator<false, false, false, OMNI_DECIMAL64, OMNI_DOUBLE>;
template class MinAggregator<false, false, true, OMNI_DECIMAL64, OMNI_DOUBLE>;
template class MinAggregator<false, true, false, OMNI_DECIMAL64, OMNI_DOUBLE>;
template class MinAggregator<false, true, true, OMNI_DECIMAL64, OMNI_DOUBLE>;
template class MinAggregator<true, false, false, OMNI_DECIMAL64, OMNI_DOUBLE>;
template class MinAggregator<true, false, true, OMNI_DECIMAL64, OMNI_DOUBLE>;
template class MinAggregator<true, true, false, OMNI_DECIMAL64, OMNI_DOUBLE>;
template class MinAggregator<true, true, true, OMNI_DECIMAL64, OMNI_DOUBLE>;

template class MinAggregator<false, false, false, OMNI_DECIMAL64, OMNI_DECIMAL128>;
template class MinAggregator<false, false, true, OMNI_DECIMAL64, OMNI_DECIMAL128>;
template class MinAggregator<false, true, false, OMNI_DECIMAL64, OMNI_DECIMAL128>;
template class MinAggregator<false, true, true, OMNI_DECIMAL64, OMNI_DECIMAL128>;
template class MinAggregator<true, false, false, OMNI_DECIMAL64, OMNI_DECIMAL128>;
template class MinAggregator<true, false, true, OMNI_DECIMAL64, OMNI_DECIMAL128>;
template class MinAggregator<true, true, false, OMNI_DECIMAL64, OMNI_DECIMAL128>;
template class MinAggregator<true, true, true, OMNI_DECIMAL64, OMNI_DECIMAL128>;

template class MinAggregator<false, false, false, OMNI_DECIMAL64, OMNI_DECIMAL64>;
template class MinAggregator<false, false, true, OMNI_DECIMAL64, OMNI_DECIMAL64>;
template class MinAggregator<false, true, false, OMNI_DECIMAL64, OMNI_DECIMAL64>;
template class MinAggregator<false, true, true, OMNI_DECIMAL64, OMNI_DECIMAL64>;
template class MinAggregator<true, false, false, OMNI_DECIMAL64, OMNI_DECIMAL64>;
template class MinAggregator<true, false, true, OMNI_DECIMAL64, OMNI_DECIMAL64>;
template class MinAggregator<true, true, false, OMNI_DECIMAL64, OMNI_DECIMAL64>;
template class MinAggregator<true, true, true, OMNI_DECIMAL64, OMNI_DECIMAL64>;
}
}
