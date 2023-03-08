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
void MinVarcharAggregator<IN_ID, OUT_ID>::ProcessGroupWithHMPP(
    AggregateState &state, VectorBatch *vectorBatch)
{
    auto vector = vectorBatch->GetVector(this->channels[0]);

    auto offsets =
        static_cast<int32_t *>(static_cast<int32_t *>(vector->GetValueOffsets()) + vector->GetPositionOffset());
    auto width = static_cast<VarcharDataType *>(this->inputTypes.GetType(0).get())->GetWidth();
    int32_t minLen = 3 * width;
    uint8_t *minVal = this->executionContext->GetArena()->Allocate(3 * width);

    LogDebug("HMPP-Agg-min");
    auto result =
        HMPPS_Min_varchar(static_cast<uint8_t *>(vector->GetValues()), offsets, vector->GetSize(), minVal, &minLen);
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
bool MinVarcharAggregator<IN_ID, OUT_ID>::CanProcessWithHMPP(AggregateState &state,
    VectorBatch *vectorBatch)
{
    // must no null inpout
    if (vectorBatch->GetVector(this->channels[0])->MayHaveNull()) {
        return false;
    }
    // not accept dictionnary vector
    if (vectorBatch->GetVector(this->channels[0])->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
        return false;
    }
    return true;
}
#endif

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinVarcharAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState &state,
    std::vector<Vector *> &vectors, int32_t rowIndex)
{
    int32_t offset;
    auto v = static_cast<VarcharVector *>(VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset));
    if (state.val == nullptr || state.count == 0) {
        // Note: due to issue #614 we should call SetValueNull on VarcharVector vector not Vector base class
        v->SetValueNull(offset);
    } else {
        v->SetValue(offset, reinterpret_cast<uint8_t *>(state.val), state.count);
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinVarcharAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(
    AggregateState &state, Vector *v, const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap,
    const int32_t *indexMap)
{
    VarcharVector *vector = static_cast<VarcharVector *>(v);

    if (indexMap == nullptr) {
        if (nullMap == nullptr) {
            AddChar<MinCharOp>(state, vector, rowOffset, rowCount);
        } else {
            AddConditionalChar<MinCharOp>(state, vector, rowOffset, rowCount, nullMap);
        }
    } else {
        if (nullMap == nullptr) {
            AddDictChar<MinCharOp>(state, vector, rowCount, indexMap);
        } else {
            AddDictConditionalChar<MinCharOp>(state, vector, rowCount, nullMap, indexMap);
        }
    }

    SaveState(state);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinVarcharAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(
    std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *v, const int32_t rowOffset,
    const uint8_t *nullMap, const int32_t *indexMap)
{
    VarcharVector *vector = static_cast<VarcharVector *>(v);

    if (indexMap == nullptr) {
        if (nullMap == nullptr) {
            AddUseRowIndexChar<MinCharOp>(rowStates, aggIdx, vector, rowOffset);
        } else {
            AddConditionalUseRowIndexChar<MinCharOp>(rowStates, aggIdx, vector, rowOffset, nullMap);
        }
    } else {
        if (nullMap == nullptr) {
            AddDictUseRowIndexChar<MinCharOp>(rowStates, aggIdx, vector, indexMap);
        } else {
            AddDictConditionalUseRowIndexChar<MinCharOp>(rowStates, aggIdx, vector, nullMap, indexMap);
        }
    }

    for (AggregateState *states : rowStates) {
        SaveState(states[aggIdx]);
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
ALWAYS_INLINE void MinVarcharAggregator<IN_ID, OUT_ID>::SaveState(
    AggregateState &state)
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
template class MinVarcharAggregator<OMNI_CHAR, OMNI_CHAR>;

template class MinVarcharAggregator<OMNI_CHAR, OMNI_VARCHAR>;

template class MinVarcharAggregator<OMNI_VARCHAR, OMNI_CHAR>;

template class MinVarcharAggregator<OMNI_VARCHAR, OMNI_VARCHAR>;

}
}
