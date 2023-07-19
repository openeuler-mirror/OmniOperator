/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Min aggregate for varchar
 */

#include "min_varchar_aggregator.h"
#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif
#include "operator/aggregation/definitions.h"

namespace omniruntime {
namespace op {
#ifdef ENABLE_HMPP
template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinVarcharAggregator<IN_ID, OUT_ID>::ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch)
{
    auto vector = vectorBatch->Get(this->channels[0]);

    auto offsets = static_cast<int32_t *>(static_cast<int32_t *>(VectorHelper::UnsafeGetOffsetsAddr(vector)));
    auto width = static_cast<VarcharDataType *>(this->inputTypes.GetType(0).get())->GetWidth();
    int32_t minLen = 3 * width;
    uint8_t *minVal = this->executionContext->GetArena()->Allocate(3 * width);

    LogDebug("HMPP-Agg-min");
    auto result = HMPPS_Min_varchar(static_cast<uint8_t *>(VectorHelper::UnsafeGetValues(vector)), offsets,
        vector->GetSize(), minVal, &minLen);
    if (result != HMPP_STS_NO_ERR) {
        throw OmniException("HMPP ERROR", "min failed for hmpp error");
    }

    if (state.val == nullptr) {
        state.val = minVal;
        state.count = minLen;
    } else {
        auto preMinVal = reinterpret_cast<char *>(state.val);

        int32_t result =
            memcmp(preMinVal, reinterpret_cast<char *>(minVal), std::min(state.count, static_cast<int64_t>(minLen)));
        if (result > 0 || (result == 0 && state.count > minLen)) {
            state.val = minVal;
            state.count = minLen;
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
bool MinVarcharAggregator<IN_ID, OUT_ID>::CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch)
{
    // must no null inpout
    if (vectorBatch->Get(this->channels[0])->HasNull()) {
        return false;
    }
    // not accept dictionnary vector
    if (vectorBatch->Get(this->channels[0])->GetEncoding() == OMNI_DICTIONARY) {
        return false;
    }
    return true;
}
#endif

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinVarcharAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex)
{
    auto v = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
    if (state.val == nullptr || state.count == 0) {
        // Note: due to issue #614 we should call SetValueNull on VarcharVector vector not Vector base class
        v->SetNull(rowIndex);
    } else {
        std::string_view val(reinterpret_cast<char *>(state.val), state.count);
        v->SetValue(rowIndex, val);
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinVarcharAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState &state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap)
{
    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
        if (nullMap == nullptr) {
            AddChar<MinCharOp>(state, vector, rowOffset, rowCount);
        } else {
            AddConditionalChar<MinCharOp>(state, vector, rowOffset, rowCount, nullMap);
        }
    } else {
        if (nullMap == nullptr) {
            AddDictChar<MinDictCharOp>(state, vector, rowOffset, rowCount);
        } else {
            AddDictConditionalChar<MinDictCharOp>(state, vector, rowOffset, rowCount, nullMap);
        }
    }

    SaveState(state);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinVarcharAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates,
    const size_t aggIdx, BaseVector *vector, const int32_t rowOffset, const uint8_t *nullMap)
{
    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
        if (nullMap == nullptr) {
            AddUseRowIndexChar<MinCharOp>(rowStates, aggIdx, vector, rowOffset);
        } else {
            AddConditionalUseRowIndexChar<MinCharOp>(rowStates, aggIdx, vector, rowOffset, nullMap);
        }
    } else {
        if (nullMap == nullptr) {
            AddDictUseRowIndexChar<MinDictCharOp>(rowStates, aggIdx, rowOffset, vector);
        } else {
            AddDictConditionalUseRowIndexChar<MinDictCharOp>(rowStates, aggIdx, rowOffset, vector, nullMap);
        }
    }

    for (AggregateState *states : rowStates) {
        SaveState(states[aggIdx]);
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
ALWAYS_INLINE void MinVarcharAggregator<IN_ID, OUT_ID>::SaveState(AggregateState &state)
{
    if ((state.count & UPDATE_FLAG) == 0) {
        return;
    }

    int32_t len = static_cast<int32_t>(state.count & VALUE_FLAG);
    if (state.val == nullptr || len == 0) {
        state.val = nullptr;
        state.count = 0;
        return;
    }

    uint8_t *ptr = reinterpret_cast<uint8_t *>(this->executionContext->GetArena()->Allocate(len));
    std::copy(reinterpret_cast<uint8_t *>(state.val), reinterpret_cast<uint8_t *>(state.val) + len, ptr);
    state.val = ptr;
    state.count = len;
}

// Explicit template instantiation
// Defining templated aggregators in header file consume a lot of memory during compilation
// since, compiler needs to generate each individual template instance wherever aggregator header is include
// to reduce time and memory usage during compilation moved templated aggregator implementation into .cpp files
// and used explicit template instantiation to generate template instances

void InitialVarcharState(AggregateState &state, Vector<LargeStringContainer<std::string_view>> *rawVector,
    const char *res, int32_t idx)
{
    if (state.val == nullptr || state.count == 0) {
        auto strView = rawVector->GetValue(idx);
        res = strView.data();
        state.count = strView.size();
        state.count |= UPDATE_FLAG;
    }
}

template class MinVarcharAggregator<OMNI_CHAR, OMNI_CHAR>;

template class MinVarcharAggregator<OMNI_CHAR, OMNI_VARCHAR>;

template class MinVarcharAggregator<OMNI_VARCHAR, OMNI_CHAR>;

template class MinVarcharAggregator<OMNI_VARCHAR, OMNI_VARCHAR>;
}
}
