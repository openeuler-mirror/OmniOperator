/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Max aggregate for varchar
 */

#include "max_varchar_aggregator.h"

#include "min_varchar_aggregator.h"

namespace omniruntime {
namespace op {
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
void MaxVarcharAggregator<IN_ID, OUT_ID>::ExtractSpillValues(const AggregateState &state,
    std::vector<BaseVector *> &vectors, int32_t rowIndex)
{
    this->ExtractValues(state, vectors, rowIndex);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MaxVarcharAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState &state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap)
{
    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
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
void MaxVarcharAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates,
    const size_t aggIdx, BaseVector *vector, const int32_t rowOffset, const uint8_t *nullMap)
{
    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
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
void MaxVarcharAggregator<IN_ID, OUT_ID>::ProcessGroupAfterSpill(AggregateState &state, VectorBatch *vectorBatch,
    int32_t &vectorIndex, int32_t rowIdx)
{
    auto vectorPtr = vectorBatch->Get(vectorIndex++);
    if (!vectorPtr->IsNull(rowIdx)) {
        auto varcharVec = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vectorPtr);
        if (state.val == nullptr || state.count == 0) {
            auto strView = varcharVec->GetValue(rowIdx);
            auto *res = strView.data();
            state.count = strView.size();
            state.count |= UPDATE_FLAG;
            state.val = const_cast<char *>(res);
            SaveState(state);
        } else {
            state.val = const_cast<char *>(MaxCharOp(reinterpret_cast<char *>(state.val), state.count, varcharVec,
                rowIdx));
        }
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
