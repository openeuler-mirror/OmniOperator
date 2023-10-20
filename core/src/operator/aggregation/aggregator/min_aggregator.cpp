/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Min aggregate
 */

#include "min_aggregator.h"
#include "operator/aggregation/neon_aggregation/neon_aggregation_external.h"
#ifdef ENABLE_HMPP
#include "aggregator_util.h"
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
#ifdef ENABLE_HMPP
template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinAggregator<IN_ID, OUT_ID>::ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch)
{
    auto vector = vectorBatch->Get(this->channels[0]);

    auto vectorValues = VectorHelper::UnsafeGetValues(vector);
    auto rowCount = vector->GetSize();

    HmppResult result = HMPP_STS_NO_ERR;
    auto minVal = reinterpret_cast<ResultType *>(this->executionContext->GetArena()->Allocate(sizeof(ResultType)));
    memset_sp((void *)minVal, sizeof(ResultType), 0, sizeof(ResultType));

    if constexpr (IN_ID == OMNI_SHORT) {
        LogDebug("HMPP-Agg-min");
        result = HMPPS_Min_16s(static_cast<int16_t *>(static_cast<int16_t *>(vectorValues)), rowCount,
            reinterpret_cast<int16_t *>(minVal));
        int16_t realVal = *reinterpret_cast<int16_t *>(minVal);
        *minVal = static_cast<ResultType>(realVal);
    } else if constexpr (IN_ID == OMNI_INT || IN_ID == OMNI_DATE32) {
        LogDebug("HMPP-Agg-min");
        result = HMPPS_Min_32s(static_cast<int32_t *>(static_cast<int32_t *>(vectorValues)), rowCount,
            reinterpret_cast<int32_t *>(minVal));
        int32_t realVal = *reinterpret_cast<int32_t *>(minVal);
        *minVal = static_cast<ResultType>(realVal);
    } else if constexpr (IN_ID == OMNI_LONG || IN_ID == OMNI_DECIMAL64) {
        LogDebug("HMPP-Agg-min");
        result = HMPPS_Min_64s(static_cast<int64_t *>(static_cast<int64_t *>(vectorValues)), rowCount,
            reinterpret_cast<int64_t *>(minVal));
    } else if constexpr (IN_ID == OMNI_DOUBLE) {
        LogDebug("HMPP-Agg-min");
        result = HMPPS_Min_64f(static_cast<double *>(static_cast<double *>(vectorValues)), rowCount,
            reinterpret_cast<double *>(minVal));
    } else {
        LogDebug("HMPP-Agg-min");
        result = HMPPS_Min_decimal(static_cast<HmppDecimal128 *>(static_cast<HmppDecimal128 *>(vectorValues)), rowCount,
            reinterpret_cast<HmppDecimal128 *>(minVal));
    }

    if (result != HMPP_STS_NO_ERR) {
        throw OmniException("HMPP ERROR", "min failed for hmpp error");
    }
    if (state.val == nullptr) {
        state.val = minVal;
    } else {
        auto preMinVal = static_cast<ResultType *>(state.val);
        *static_cast<ResultType *>(state.val) = (Compare(*preMinVal, *minVal) == -1) ? *preMinVal : *minVal;
    }
    // hmpp only works on not nullable columns, so it always find min
    state.count = 1;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
bool MinAggregator<IN_ID, OUT_ID>::CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch)
{
    // just support raw input data and must no null inpout
    if (!inputRaw) {
        return false;
    } else {
        if (vectorBatch->Get(this->channels[0])->HasNull()) {
            return false;
        }
        // not accept dictionnary vector
        if (vectorBatch->Get(this->channels[0])->GetEncoding() == OMNI_DICTIONARY) {
            return false;
        }
        // type check with whitelist for min
        auto inputTypeId = this->inputTypes.GetType(0)->GetId();
        return AggregatorUtil::IsHMPPMaxMinSupportDataTypeId(inputTypeId);
    }
}
#endif

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex)
{
    auto v = static_cast<OutVector *>(vectors[0]);
    if (state.count == 0 || (state.count > 0 && state.val == nullptr)) {
        v->SetNull(rowIndex);
        return;
    }

    bool overflow = state.count < 0;
    auto result =
        this->template CastWithOverflow<ResultType, OutType>(*reinterpret_cast<ResultType *>(state.val), overflow);
    v->SetValue(rowIndex, result);
    if (overflow) {
        this->SetNullOrThrowException(v, rowIndex, "min_aggregator overflow.");
    } else if (state.count == 0 || state.val == nullptr) {
        v->SetNull(rowIndex);
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> void MinAggregator<IN_ID, OUT_ID>::InitState(AggregateState &state)
{
    state.val = this->executionContext->GetArena()->Allocate(sizeof(ResultType));
    *reinterpret_cast<ResultType *>(state.val) = GetMax<ResultType>();
    state.count = 0;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState &state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap)
{
    if (state.val == nullptr) {
        InitState(state);
    }
    auto *res = reinterpret_cast<ResultType *>(state.val);

    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            if constexpr (std::is_same_v<InType, Decimal128> || std::is_same_v<ResultType, Decimal128>) {
                Add<InType, ResultType, MinOp<InType, ResultType>>(res, state.count, ptr, rowCount);
            } else {
                simd::SIMDAdd<InType,ResultType, simd::BasicOp::Min>(res, state.count, ptr, rowCount);
            }
        } else {
            if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_same_v<ResultType, Decimal128>) {
                AddConditional<InType, ResultType, MinConditionalOp<InType, ResultType, false>>(res, state.count, ptr,
                                                                                                rowCount, nullMap);
            } else {
                simd::SIMDAddConditional<InType, ResultType, simd::BasicOp::Min>(res, state.count, ptr,
                                                                                 rowCount, nullMap);
            }
        }
    } else {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
        auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
        if (nullMap == nullptr) {
            if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_same_v<ResultType, Decimal128>) {
                AddDict<InType, ResultType, MinOp<InType, ResultType>>(res, state.count, ptr, rowCount, indexMap);
            } else {
                simd::SIMDAddDict<InType, ResultType, simd::BasicOp::Min>(res, state.count, ptr, rowCount, indexMap);
            }
        } else {
            if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_same_v<ResultType, Decimal128>) {
                AddDictConditional<InType, ResultType, MinConditionalOp<InType, ResultType, false>>(res, state.count, ptr,
                                                                                                    rowCount, nullMap, indexMap);
            } else {
                simd::SIMDAddDictConditional<InType, ResultType,simd::BasicOp::Min>(res, state.count, ptr,
                                                                                    rowCount, nullMap, indexMap);
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    BaseVector *vector, const int32_t rowOffset, const uint8_t *nullMap)
{
    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            AddUseRowIndex<InType, ResultType, MinOp<InType, ResultType>>(rowStates, aggIdx, ptr);
        } else {
            AddConditionalUseRowIndex<InType, ResultType, MinConditionalOp<InType, ResultType, false>>(rowStates,
                aggIdx, ptr, nullMap);
        }
    } else {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
        auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
        if (nullMap == nullptr) {
            AddDictUseRowIndex<InType, ResultType, MinOp<InType, ResultType>>(rowStates, aggIdx, ptr, indexMap);
        } else {
            AddDictConditionalUseRowIndex<InType, ResultType, MinConditionalOp<InType, ResultType, false>>(rowStates,
                aggIdx, ptr, nullMap, indexMap);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
MinAggregator<IN_ID, OUT_ID>::MinAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
    : TypedAggregator(OMNI_AGGREGATION_TYPE_MIN, inputTypes, outputTypes, channels, inputRaw, outputPartial,
    isOverflowAsNull)
{}

// Explicit template instantiation
// Defining templated aggregators in header file consume a lot of memory during compilation
// since, compiler needs to generate each individual template instance wherever aggregator header is include
// to reduce time and memory usage during compilation moved templated aggregator implementation into .cpp files
// and used explicit template instantiation to generate template instances
template class MinAggregator<OMNI_BOOLEAN, OMNI_BOOLEAN>;

template class MinAggregator<OMNI_BOOLEAN, OMNI_SHORT>;

template class MinAggregator<OMNI_BOOLEAN, OMNI_INT>;

template class MinAggregator<OMNI_BOOLEAN, OMNI_LONG>;

template class MinAggregator<OMNI_BOOLEAN, OMNI_DOUBLE>;

template class MinAggregator<OMNI_BOOLEAN, OMNI_DECIMAL128>;

template class MinAggregator<OMNI_BOOLEAN, OMNI_DECIMAL64>;

template class MinAggregator<OMNI_SHORT, OMNI_BOOLEAN>;

template class MinAggregator<OMNI_SHORT, OMNI_SHORT>;

template class MinAggregator<OMNI_SHORT, OMNI_INT>;

template class MinAggregator<OMNI_SHORT, OMNI_LONG>;

template class MinAggregator<OMNI_SHORT, OMNI_DOUBLE>;

template class MinAggregator<OMNI_SHORT, OMNI_DECIMAL128>;

template class MinAggregator<OMNI_SHORT, OMNI_DECIMAL64>;

template class MinAggregator<OMNI_INT, OMNI_BOOLEAN>;

template class MinAggregator<OMNI_INT, OMNI_SHORT>;

template class MinAggregator<OMNI_INT, OMNI_INT>;

template class MinAggregator<OMNI_INT, OMNI_LONG>;

template class MinAggregator<OMNI_INT, OMNI_DOUBLE>;

template class MinAggregator<OMNI_INT, OMNI_DECIMAL128>;

template class MinAggregator<OMNI_INT, OMNI_DECIMAL64>;

template class MinAggregator<OMNI_LONG, OMNI_BOOLEAN>;

template class MinAggregator<OMNI_LONG, OMNI_SHORT>;

template class MinAggregator<OMNI_LONG, OMNI_INT>;

template class MinAggregator<OMNI_LONG, OMNI_LONG>;

template class MinAggregator<OMNI_LONG, OMNI_DOUBLE>;

template class MinAggregator<OMNI_LONG, OMNI_DECIMAL128>;

template class MinAggregator<OMNI_LONG, OMNI_DECIMAL64>;

template class MinAggregator<OMNI_DOUBLE, OMNI_BOOLEAN>;

template class MinAggregator<OMNI_DOUBLE, OMNI_SHORT>;

template class MinAggregator<OMNI_DOUBLE, OMNI_INT>;

template class MinAggregator<OMNI_DOUBLE, OMNI_LONG>;

template class MinAggregator<OMNI_DOUBLE, OMNI_DOUBLE>;

template class MinAggregator<OMNI_DOUBLE, OMNI_DECIMAL128>;

template class MinAggregator<OMNI_DOUBLE, OMNI_DECIMAL64>;

template class MinAggregator<OMNI_DECIMAL128, OMNI_BOOLEAN>;

template class MinAggregator<OMNI_DECIMAL128, OMNI_SHORT>;

template class MinAggregator<OMNI_DECIMAL128, OMNI_INT>;

template class MinAggregator<OMNI_DECIMAL128, OMNI_LONG>;

template class MinAggregator<OMNI_DECIMAL128, OMNI_DOUBLE>;

template class MinAggregator<OMNI_DECIMAL128, OMNI_DECIMAL128>;

template class MinAggregator<OMNI_DECIMAL128, OMNI_DECIMAL64>;

template class MinAggregator<OMNI_DECIMAL64, OMNI_BOOLEAN>;

template class MinAggregator<OMNI_DECIMAL64, OMNI_SHORT>;

template class MinAggregator<OMNI_DECIMAL64, OMNI_INT>;

template class MinAggregator<OMNI_DECIMAL64, OMNI_LONG>;

template class MinAggregator<OMNI_DECIMAL64, OMNI_DOUBLE>;

template class MinAggregator<OMNI_DECIMAL64, OMNI_DECIMAL128>;

template class MinAggregator<OMNI_DECIMAL64, OMNI_DECIMAL64>;
}
}
