/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Count aggregate
 */

#include "count_column_aggregator.h"

namespace omniruntime {
namespace op {
template <bool addIf>
VECTORIZE_LOOP NO_INLINE void AddConditionalCountRaw(int64_t &res, const size_t rowCount,
    const uint8_t *__restrict condition)
{
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalCountRaw]: ConditionMap pointer NOT aligned");
        }
#endif

        for (size_t i = 0; i < rowCount; ++i) {
            res += (condition[i] == addIf);
        }
    }
}


template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CountColumnAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState &state,
    std::vector<BaseVector *> &vectors, int32_t rowIndex)
{
    static_cast<Vector<int64_t> *>(vectors[0])->SetValue(rowIndex, state.count);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CountColumnAggregator<IN_ID, OUT_ID>::ExtractSpillValues(const AggregateState &state,
    std::vector<BaseVector *> &vectors, int32_t rowIndex)
{
    this->ExtractValues(state, vectors, rowIndex);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
template <bool RAW_IN>
void CountColumnAggregator<IN_ID, OUT_ID>::ProcessSingleInternalFunction(AggregateState &state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap)
{
    if constexpr (RAW_IN) {
        if (nullMap == nullptr) {
            state.count += rowCount;
        } else {
            AddConditionalCountRaw<false>(state.count, rowCount, nullMap);
        }
    } else {
        int64_t noUsed{};

        if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
            auto *ptr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(vector));
            ptr += rowOffset;
            if (nullMap == nullptr) {
                Add<int64_t, int64_t, CountAllOp>(&(state.count), noUsed, ptr, rowCount);
            } else {
                AddConditional<int64_t, int64_t, CountAllConditionalOp<false>>(&(state.count), noUsed, ptr, rowCount,
                    nullMap);
            }
        } else {
            auto *ptr = reinterpret_cast<int64_t *>(GetValuesFromDict<OMNI_LONG>(vector));
            auto *indexMap = GetIdsFromDict<OMNI_LONG>(vector) + rowOffset;
            if (nullMap == nullptr) {
                AddDict<int64_t, int64_t, CountAllOp>(&(state.count), noUsed, ptr, rowCount, indexMap);
            } else {
                AddDictConditional<int64_t, int64_t, CountAllConditionalOp<false>>(&(state.count), noUsed, ptr,
                    rowCount, nullMap, indexMap);
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CountColumnAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState &state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap)
{
    return (this->*processSingleInternalPtr)(state, vector, rowOffset, rowCount, nullMap);
}

SIMD_ALWAYS_INLINE void ProcessGroupInternalFunctionForRawInput(std::vector<AggregateState *> &rowStates,
    const size_t aggIdx, const uint8_t *nullMap)
{
    if (nullMap == nullptr) {
        for (AggregateState *states : rowStates) {
            states[aggIdx].count++;
        }
    } else {
        size_t rowCount = rowStates.size();
        for (size_t i = 0; i < rowCount; ++i) {
            if (!nullMap[i]) {
                rowStates[i][aggIdx].count++;
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
template <bool RAW_IN>
VECTORIZE_LOOP void CountColumnAggregator<IN_ID, OUT_ID>::ProcessGroupInternalFunction(
    std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *vector, const int32_t rowOffset,
    const uint8_t *nullMap)
{
    if constexpr (RAW_IN) {
        ProcessGroupInternalFunctionForRawInput(rowStates, aggIdx, nullMap);
    } else {
        size_t rowCount = rowStates.size();
        int64_t unsedFlag = 0;

        if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
            auto *ptr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(vector));
            ptr += rowOffset;
            if (nullMap == nullptr) {
                for (size_t i = 0; i < rowCount; ++i) {
                    CountAllOp(&(rowStates[i][aggIdx].count), unsedFlag, ptr[i], 0LL);
                }
            } else {
                for (size_t i = 0; i < rowCount; ++i) {
                    CountAllConditionalOp<false>(&(rowStates[i][aggIdx].count), unsedFlag, ptr[i], 0LL, nullMap[i]);
                }
            }
        } else {
            auto *ptr = reinterpret_cast<int64_t *>(GetValuesFromDict<OMNI_LONG>(vector));
            auto *indexMap = GetIdsFromDict<OMNI_LONG>(vector) + rowOffset;
            if (nullMap == nullptr) {
                for (size_t i = 0; i < rowCount; ++i) {
                    CountAllOp(&(rowStates[i][aggIdx].count), unsedFlag, ptr[indexMap[i]], 0LL);
                }
            } else {
                for (size_t i = 0; i < rowCount; ++i) {
                    CountAllConditionalOp<false>(&(rowStates[i][aggIdx].count), unsedFlag, ptr[indexMap[i]], 0LL,
                        nullMap[i]);
                }
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CountColumnAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates,
    const size_t aggIdx, BaseVector *vector, const int32_t rowOffset, const uint8_t *nullMap)
{
    return (this->*processGroupInternalPtr)(rowStates, aggIdx, vector, rowOffset, nullMap);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CountColumnAggregator<IN_ID, OUT_ID>::ProcessGroupAfterSpill(AggregateState &state,
    VectorBatch *vectorBatch, int32_t &vectorIndex, int32_t rowIdx)
{
    auto vectorPtr = vectorBatch->Get(vectorIndex++);
    auto *ptr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(vectorPtr));
    if (!vectorPtr->IsNull(rowIdx)) {
        int64_t unsedFlag = 0;
        CountAllOp(&(state.count), unsedFlag, ptr[rowIdx], 0LL);
    }
}

// Explicit template instantiation
// Defining templated aggregators in header file consume a lot of memory during compilation
// since, compiler needs to generate each individual template instance wherever aggregator header is include
// to reduce time and memory usage during compilation moved templated aggregator implementation into .cpp files
// and used explicit template instantiation to generate template instances
template class CountColumnAggregator<OMNI_NONE, OMNI_LONG>;

template class CountColumnAggregator<OMNI_BOOLEAN, OMNI_LONG>;

template class CountColumnAggregator<OMNI_SHORT, OMNI_LONG>;

template class CountColumnAggregator<OMNI_DATE32, OMNI_LONG>;

template class CountColumnAggregator<OMNI_TIME32, OMNI_LONG>;

template class CountColumnAggregator<OMNI_INT, OMNI_LONG>;

template class CountColumnAggregator<OMNI_LONG, OMNI_LONG>;

template class CountColumnAggregator<OMNI_DATE64, OMNI_LONG>;

template class CountColumnAggregator<OMNI_TIME64, OMNI_LONG>;

template class CountColumnAggregator<OMNI_TIMESTAMP, OMNI_LONG>;

template class CountColumnAggregator<OMNI_DOUBLE, OMNI_LONG>;

template class CountColumnAggregator<OMNI_DECIMAL64, OMNI_LONG>;

template class CountColumnAggregator<OMNI_DECIMAL128, OMNI_LONG>;

template class CountColumnAggregator<OMNI_CONTAINER, OMNI_LONG>;

template class CountColumnAggregator<OMNI_VARCHAR, OMNI_LONG>;

template class CountColumnAggregator<OMNI_CHAR, OMNI_LONG>;

template class CountColumnAggregator<OMNI_INVALID, OMNI_LONG>;
}
}
