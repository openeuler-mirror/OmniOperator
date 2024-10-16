/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Min aggregate for varchar
 */

#include "min_varchar_aggregator.h"
#include "operator/aggregation/definitions.h"

namespace omniruntime {
namespace op {
template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinVarcharAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex)
{
    auto v = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
    if (state.val == 0) {
        // Note: due to issue #614 we should call SetValueNull on VarcharVector vector not Vector base class
        v->SetNull(rowIndex);
    } else {
        std::string_view val(reinterpret_cast<char *>(state.val), state.count);
        v->SetValue(rowIndex, val);
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinVarcharAggregator<IN_ID, OUT_ID>::ExtractValuesBatch(std::vector<AggregateState *> &groupStates,
    const size_t aggIdx, std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount)
{
    auto v = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
    for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        auto &state = groupStates[rowIndex][aggIdx];
        if (state.val == 0) {
            // Note: due to issue #614 we should call SetValueNull on VarcharVector vector not Vector base class
            v->SetNull(rowIndex);
        } else {
            std::string_view val(reinterpret_cast<char *>(state.val), state.count);
            v->SetValue(rowIndex, val);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinVarcharAggregator<IN_ID, OUT_ID>::ExtractValuesForSpill(std::vector<AggregateState *> &groupStates,
    const size_t aggIdx, std::vector<BaseVector *> &vectors)
{
    auto v = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
    auto rowCount = static_cast<int32_t>(groupStates.size());
    for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        auto &state = groupStates[rowIndex][aggIdx];
        if (state.val == 0) {
            // Note: due to issue #614 we should call SetValueNull on VarcharVector vector not Vector base class
            v->SetNull(rowIndex);
        } else {
            std::string_view val(reinterpret_cast<char *>(state.val), state.count);
            v->SetValue(rowIndex, val);
        }
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
void MinVarcharAggregator<IN_ID, OUT_ID>::ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows,
    int32_t rowCount, const size_t aggIdx, int32_t &vectorIndex)
{
    auto firstVecIdx = vectorIndex++;
    for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
        auto &row = unspillRows[rowIdx];
        auto batch = row.batch;
        auto index = row.rowIdx;
        auto varcharVec = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(batch->Get(firstVecIdx));
        if (!varcharVec->IsNull(index)) {
            auto &state = row.state[aggIdx];
            if (state.val == 0) {
                auto strView = varcharVec->GetValue(index);
                auto *res = strView.data();
                state.count = strView.size();
                state.count |= UPDATE_FLAG;
                state.val = (int64_t)(res);
                SaveState(state);
            } else {
                state.val = (int64_t)(MinCharOp(reinterpret_cast<char *>(state.val), state.count, varcharVec, index));
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinVarcharAggregator<IN_ID, OUT_ID>::ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
                                                                const uint8_t *nullMap, const bool aggFilter)
{
    // note: type relationship matches only (IN_ID == OMNI_CHAR || IN_ID == OMNI_VARCHAR) and
    // (OUT_ID == OMNI_CHAR || OUT_ID == OMNI_VARCHAR)
    if (!aggFilter) {
        int rowCount = originVector->GetSize();
        auto *minVector = VectorHelper::SliceVector(originVector, 0, rowCount);
        result->Append(minVector);
        return;
    }

    // handle agg filter, nullMap != nullptr
    if (originVector->GetEncoding() == OMNI_DICTIONARY) {
        ProcessAlignAggSchemaInternal<Vector<DictionaryContainer<std::string_view>>>(result, originVector, nullMap);
    } else {
        ProcessAlignAggSchemaInternal<Vector<std::string_view>>(result, originVector, nullMap);
    }
}

template<DataTypeId IN_ID, DataTypeId OUT_ID>
template<typename T>
void MinVarcharAggregator<IN_ID, OUT_ID>::ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector,
                                                                        const uint8_t *nullMap)
{
    int rowCount = originVector->GetSize();
    auto minVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(
            VectorHelper::CreateFlatVector(OUT_ID, rowCount));
    auto vector = reinterpret_cast<T *>(originVector);
    for (int index = 0; index < rowCount; ++index) {
        if (nullMap[index]) {
            minVector->SetNull(index);
        } else {
            std::string_view val = vector->GetValue(index);
            minVector->SetValue(index, val);
        }
    }
    result->Append(minVector);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
ALWAYS_INLINE void MinVarcharAggregator<IN_ID, OUT_ID>::SaveState(AggregateState &state)
{
    if ((state.count & UPDATE_FLAG) == 0) {
        return;
    }

    int32_t len = static_cast<int32_t>(state.count & VALUE_FLAG);
    uint8_t *ptr = reinterpret_cast<uint8_t *>(arenaAllocator->Allocate(len));
    std::copy(reinterpret_cast<uint8_t *>(state.val), reinterpret_cast<uint8_t *>(state.val) + len, ptr);
    state.val = reinterpret_cast<int64_t>(ptr);
    state.count = len;
}

template class MinVarcharAggregator<OMNI_CHAR, OMNI_CHAR>;

template class MinVarcharAggregator<OMNI_CHAR, OMNI_VARCHAR>;

template class MinVarcharAggregator<OMNI_VARCHAR, OMNI_CHAR>;

template class MinVarcharAggregator<OMNI_VARCHAR, OMNI_VARCHAR>;
}
}
