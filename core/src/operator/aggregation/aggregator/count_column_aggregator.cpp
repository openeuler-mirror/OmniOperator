/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Count aggregate
 */

#include "count_column_aggregator.h"

namespace omniruntime {
namespace op {
template <bool addIf>
VECTORIZE_LOOP NO_INLINE void addConditionalCountRaw(int64_t &res, const size_t rowCount,
    const uint8_t *__restrict condition)
{
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalCountRaw]: ConditionMap pointer NOT aligned");
        }
#endif

        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            res += (condition[i] == addIf);
        }
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
void CountColumnAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::ExtractValues(
    const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex)
{
    int32_t offset;
    Vector *vector = VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset);
    static_cast<LongVector *>(vector)->SetValue(offset, state.count);
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
void CountColumnAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::ProcessSingleInternal(
    AggregateState &state, Vector *vector, const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap,
    const int32_t *indexMap)
{
    if constexpr (RAW_IN) {
        if (nullMap == nullptr) {
            state.count += rowCount;
        } else {
            addConditionalCountRaw<false>(state.count, rowCount, nullMap);
        }
    } else {
        int64_t *ptr = reinterpret_cast<int64_t *>(static_cast<LongVector *>(vector)->GetValues());
        ptr += vector->GetPositionOffset();

        int64_t noUsed{};

        if (indexMap == nullptr) {
            ptr += rowOffset;
            if (nullMap == nullptr) {
                add<int64_t, int64_t, countAllOp>(&(state.count), noUsed, ptr, rowCount);
            } else {
                addConditional<int64_t, int64_t, countAllConditionalOp<false>>(&(state.count), noUsed, ptr, rowCount,
                    nullMap);
            }
        } else {
            if (nullMap == nullptr) {
                addDict<int64_t, int64_t, countAllOp>(&(state.count), noUsed, ptr, rowCount, indexMap);
            } else {
                addDictConditional<int64_t, int64_t, countAllConditionalOp<false>>(&(state.count), noUsed, ptr,
                    rowCount, nullMap, indexMap);
            }
        }
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
void CountColumnAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::ProcessGroupInternal(
    std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector, const int32_t rowOffset,
    const uint8_t *nullMap, const int32_t *indexMap)
{
    if constexpr (RAW_IN) {
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
    } else {
        int64_t *ptr = reinterpret_cast<int64_t *>(static_cast<LongVector *>(vector)->GetValues());
        ptr += vector->GetPositionOffset();
        size_t rowCount = rowStates.size();
        int64_t unsedFlag = 0;

        if (indexMap == nullptr) {
            ptr += rowOffset;
            if (nullMap == nullptr) {
                for (size_t i = 0; i < rowCount; ++i) {
                    countAllOp(&(rowStates[i][aggIdx].count), unsedFlag, ptr[i], 0LL);
                }
            } else {
                for (size_t i = 0; i < rowCount; ++i) {
                    countAllConditionalOp<false>(&(rowStates[i][aggIdx].count), unsedFlag, ptr[i], 0LL, nullMap[i]);
                }
            }
        } else {
            if (nullMap == nullptr) {
                for (size_t i = 0; i < rowCount; ++i) {
                    countAllOp(&(rowStates[i][aggIdx].count), unsedFlag, ptr[indexMap[i]], 0LL);
                }
            } else {
                for (size_t i = 0; i < rowCount; ++i) {
                    countAllConditionalOp<false>(&(rowStates[i][aggIdx].count), unsedFlag, ptr[indexMap[i]], 0LL,
                        nullMap[i]);
                }
            }
        }
    }
}

// Explicit template instantiation
// Defining templated aggregators in header file consume a lot of memory during compilation
// since, compiler needs to generate each individual template instance wherever aggregator header is include
// to reduce time and memory usage during compilation moved templated aggregator implementation into .cpp files
// and used explicit template instantiation to generate template instances
template class CountColumnAggregator<false, false, false, OMNI_NONE, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_NONE, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_NONE, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_NONE, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_NONE, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_NONE, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_NONE, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_NONE, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_BOOLEAN, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_BOOLEAN, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_BOOLEAN, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_BOOLEAN, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_BOOLEAN, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_BOOLEAN, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_BOOLEAN, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_BOOLEAN, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_SHORT, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_SHORT, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_SHORT, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_SHORT, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_SHORT, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_SHORT, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_SHORT, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_SHORT, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_DATE32, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_DATE32, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_DATE32, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_DATE32, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_DATE32, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_DATE32, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_DATE32, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_DATE32, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_TIME32, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_TIME32, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_TIME32, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_TIME32, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_TIME32, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_TIME32, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_TIME32, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_TIME32, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_INT, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_INT, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_INT, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_INT, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_INT, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_INT, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_INT, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_INT, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_LONG, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_LONG, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_LONG, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_LONG, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_LONG, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_LONG, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_LONG, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_LONG, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_DATE64, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_DATE64, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_DATE64, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_DATE64, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_DATE64, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_DATE64, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_DATE64, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_DATE64, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_TIME64, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_TIME64, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_TIME64, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_TIME64, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_TIME64, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_TIME64, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_TIME64, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_TIME64, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_TIMESTAMP, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_TIMESTAMP, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_TIMESTAMP, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_TIMESTAMP, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_TIMESTAMP, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_TIMESTAMP, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_TIMESTAMP, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_TIMESTAMP, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_DOUBLE, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_DOUBLE, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_DOUBLE, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_DOUBLE, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_DOUBLE, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_DOUBLE, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_DOUBLE, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_DOUBLE, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_DECIMAL64, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_DECIMAL64, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_DECIMAL64, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_DECIMAL64, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_DECIMAL64, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_DECIMAL64, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_DECIMAL64, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_DECIMAL64, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_DECIMAL128, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_DECIMAL128, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_DECIMAL128, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_DECIMAL128, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_DECIMAL128, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_DECIMAL128, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_DECIMAL128, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_DECIMAL128, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_CONTAINER, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_CONTAINER, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_CONTAINER, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_CONTAINER, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_CONTAINER, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_CONTAINER, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_CONTAINER, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_CONTAINER, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_VARCHAR, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_VARCHAR, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_VARCHAR, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_VARCHAR, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_VARCHAR, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_VARCHAR, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_VARCHAR, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_VARCHAR, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_CHAR, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_CHAR, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_CHAR, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_CHAR, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_CHAR, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_CHAR, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_CHAR, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_CHAR, OMNI_LONG>;

template class CountColumnAggregator<false, false, false, OMNI_INVALID, OMNI_LONG>;
template class CountColumnAggregator<false, false, true, OMNI_INVALID, OMNI_LONG>;
template class CountColumnAggregator<false, true, false, OMNI_INVALID, OMNI_LONG>;
template class CountColumnAggregator<false, true, true, OMNI_INVALID, OMNI_LONG>;
template class CountColumnAggregator<true, false, false, OMNI_INVALID, OMNI_LONG>;
template class CountColumnAggregator<true, false, true, OMNI_INVALID, OMNI_LONG>;
template class CountColumnAggregator<true, true, false, OMNI_INVALID, OMNI_LONG>;
template class CountColumnAggregator<true, true, true, OMNI_INVALID, OMNI_LONG>;
}
}
