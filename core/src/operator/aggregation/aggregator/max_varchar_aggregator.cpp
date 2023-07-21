/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Max aggregate for varchar
 */

#include "max_varchar_aggregator.h"

#include "min_varchar_aggregator.h"
#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
#ifdef ENABLE_HMPP
template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MaxVarcharAggregator<IN_ID, OUT_ID>::ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch)
{
    auto vector = vectorBatch->Get(this->channels[0]);

    auto offsets = static_cast<int32_t *>(static_cast<int32_t *>(VectorHelper::UnsafeGetOffsetsAddr(vector)));
    auto width = static_cast<VarcharDataType *>(this->inputTypes.GetType(0).get())->GetWidth();
    int32_t maxLen = 0;
    uint8_t *maxVal = this->executionContext->GetArena()->Allocate(3 * width);

    LogDebug("HMPP-Agg-max");
    auto result = HMPPS_Max_varchar(static_cast<uint8_t *>(VectorHelper::UnsafeGetValues(vector)), offsets,
        vector->GetSize(), maxVal, &maxLen);
    if (result != HMPP_STS_NO_ERR) {
        throw OmniException("HMPP ERROR", "max failed for hmpp error");
    }

    if (state.val == nullptr) {
        state.val = maxVal;
        state.count = maxLen;
    } else {
        auto preMaxVal = reinterpret_cast<char *>(state.val);

        int32_t result =
            memcmp(preMaxVal, reinterpret_cast<char *>(maxVal), std::min(state.count, static_cast<int64_t>(maxLen)));
        if (result < 0 || (result == 0 && state.count < maxLen)) {
            state.val = maxVal;
            state.count = maxLen;
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
bool MaxVarcharAggregator<IN_ID, OUT_ID>::CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch)
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
void MaxVarcharAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors,
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
void MaxVarcharAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState &state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap)
{
    if (indexMap == nullptr) {
        if (nullMap == nullptr) {
            AddChar<MaxCharOp>(state, vector, rowOffset, rowCount);
        } else {
            AddConditionalChar<MaxCharOp>(state, vector, rowOffset, rowCount, nullMap);
        }
    } else {
        if (nullMap == nullptr) {
            AddDictChar<MaxDictCharOp>(state, vector, rowOffset, rowCount);
        } else {
            AddDictConditionalChar<MaxDictCharOp>(state, vector, rowOffset, rowCount, nullMap);
        }
    }
    SaveState(state);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MaxVarcharAggregator<IN_ID, OUT_ID>::ProcessSingleInternalFilter(AggregateState &state, BaseVector *vector,
    Vector<bool> *booleanVector, const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap,
    const int32_t *indexMap)
{
    int8_t *boolPtr = reinterpret_cast<int8_t *>(GetValuesFromVector<type::OMNI_BOOLEAN>(booleanVector));

    if (indexMap == nullptr) {
        if (nullMap == nullptr) {
            AddCharFilter<MaxCharOp>(state, vector, rowOffset, rowCount, boolPtr);
        } else {
            AddConditionalCharFilter<MaxCharOp>(state, vector, rowOffset, rowCount, nullMap, boolPtr);
        }
    } else {
        if (nullMap == nullptr) {
            AddDictCharFilter<MaxCharOp>(state, vector, rowOffset, rowCount, boolPtr);
        } else {
            AddDictConditionalCharFilter<MaxCharOp>(state, vector, rowOffset, rowCount, nullMap, boolPtr);
        }
    }
    SaveState(state);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MaxVarcharAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates,
    const size_t aggIdx, BaseVector *vector, const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap)
{
    if (indexMap == nullptr) {
        if (nullMap == nullptr) {
            AddUseRowIndexChar<MaxCharOp>(rowStates, aggIdx, vector, rowOffset);
        } else {
            AddConditionalUseRowIndexChar<MaxCharOp>(rowStates, aggIdx, vector, rowOffset, nullMap);
        }
    } else {
        if (nullMap == nullptr) {
            AddDictUseRowIndexChar<MaxDictCharOp>(rowStates, aggIdx, rowOffset, vector);
        } else {
            AddDictConditionalUseRowIndexChar<MaxDictCharOp>(rowStates, aggIdx, rowOffset, vector, nullMap);
        }
    }

    for (AggregateState *states : rowStates) {
        SaveState(states[aggIdx]);
    }
}


template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MaxVarcharAggregator<IN_ID, OUT_ID>::ProcessGroupInternalFilter(std::vector<AggregateState *> &rowStates,
    const size_t aggIdx, BaseVector *v, Vector<bool> *booleanVector, const int32_t rowOffset, const uint8_t *nullMap,
    const int32_t *indexMap)
{
    int8_t *boolPtr = reinterpret_cast<int8_t *>(GetValuesFromVector<type::OMNI_BOOLEAN>(booleanVector));

    if (indexMap == nullptr) {
        if (nullMap == nullptr) {
            AddUseRowIndexCharFilter<MaxCharOp>(rowStates, aggIdx, v, rowOffset, boolPtr);
        } else {
            AddConditionalUseRowIndexCharFilter<MaxCharOp>(rowStates, aggIdx, v, rowOffset, nullMap, boolPtr);
        }
    } else {
        if (nullMap == nullptr) {
            AddDictUseRowIndexCharFilter<MaxCharOp>(rowStates, aggIdx, rowOffset, v, boolPtr);
        } else {
            AddDictConditionalUseRowIndexCharFilter<MaxCharOp>(rowStates, aggIdx, rowOffset, v, nullMap, boolPtr);
        }
    }

    for (AggregateState *states : rowStates) {
        SaveState(states[aggIdx]);
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MaxVarcharAggregator<IN_ID, OUT_ID>::SaveState(AggregateState &state)
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
template class MaxVarcharAggregator<OMNI_CHAR, OMNI_CHAR>;

template class MaxVarcharAggregator<OMNI_CHAR, OMNI_VARCHAR>;

template class MaxVarcharAggregator<OMNI_VARCHAR, OMNI_CHAR>;

template class MaxVarcharAggregator<OMNI_VARCHAR, OMNI_VARCHAR>;
}
}
