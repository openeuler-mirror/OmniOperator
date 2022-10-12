/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Min aggregate for varchar
 */
#ifndef OMNI_RUNTIME_MIN_VARCHAR_AGGREGATOR_H
#define OMNI_RUNTIME_MIN_VARCHAR_AGGREGATOR_H

#include "typed_aggregator.h"
#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif
#include "operator/aggregation/definitions.h"

constexpr int64_t updateFlag = 0x0000000100000000LL;
constexpr int64_t valueFlag  = 0x00000000FFFFFFFFLL;

namespace omniruntime {
namespace op {
inline uint8_t *minCharOp(uint8_t *res, int64_t &lenAndFlag, const VarcharVector *vector, const int32_t idx)
{
    uint8_t *curVal = nullptr;
    int32_t curLen = vector->GetValue(idx, &curVal);
    int32_t len = static_cast<int32_t>(lenAndFlag & valueFlag);
    auto result = memcmp(res, curVal, std::min(len, curLen));
    if (result > 0 || (result == 0 && len > curLen)) {
        lenAndFlag = curLen;
        lenAndFlag |= updateFlag;
        return curVal;
    } else {
        return res;
    }
}

template<uint8_t * (*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP
inline void addChar(AggregateState &state, const VarcharVector *vector, const int32_t rowOffset,
    const int32_t rowCount)
{
    if (rowCount > 0) {
        uint8_t *res = nullptr;
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;

        state.count = vector->GetValue(idx++, &res);
        state.count |= updateFlag;
        while (idx < end) {
            res = OP(res, state.count, vector, idx++);
        }
        state.val = res;
    }
}

template<uint8_t * (*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP
inline void addDictChar(AggregateState &state, const VarcharVector *vector, const int32_t rowCount,
    const int32_t * __restrict indexMap)
{
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictChar]: Dictionary Index Map pointer NOT aligned");
        }
#endif
        indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);

        uint8_t *res = nullptr;
        int32_t idx = 0;

        state.count = vector->GetValue(indexMap[idx++], &res);
        state.count |= updateFlag;
        while (idx < rowCount) {
            res = OP(res, state.count, vector, indexMap[idx++]);
        }
        state.val = res;
    }
}

template<uint8_t * (*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP
inline void addConditionalChar(AggregateState &state, const VarcharVector *vector, const int32_t rowOffset,
    const int32_t rowCount, const uint8_t * __restrict condition)
{
    if (rowCount > 0) {
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalChar]: ConditionMap pointer NOT aligned");
        }
        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);

        uint8_t *res = nullptr;
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;
        while (idx < end) {
            if (!(*condition)) {
                state.count = vector->GetValue(idx, &res);
                state.count |= updateFlag;
                ++condition;
                ++idx;
                break;
            }
            ++condition;
            ++idx;
        }

        while (idx < end) {
            if (!(*condition)) {
                res = OP(res, state.count, vector, idx);
            }
            ++condition;
            ++idx;
        }
        state.val = res;
    }
}

template<uint8_t * (*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP
inline void addDictConditionalChar(AggregateState &state, const VarcharVector *vector, const int32_t rowCount,
    const uint8_t * __restrict condition, const int32_t * __restrict indexMap)
{
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalChar]: ConditionMap pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalChar]: Dictionary Index Map pointer NOT aligned");
        }
#endif
        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);
        indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);

        uint8_t *res = nullptr;
        int32_t idx = 0;

        while (idx < rowCount) {
            if (!condition[idx]) {
                state.count = vector->GetValue(indexMap[idx], &res);
                state.count |= updateFlag;
                ++idx;
                break;
            }
            ++idx;
        }

        while (idx < rowCount) {
            if (!condition[idx]) {
                res = OP(res, state.count, vector, indexMap[idx]);
            }
            ++idx;
        }
        state.val = res;
    }
}

template<uint8_t * (*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP
inline void addUseRowIndexChar(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    const VarcharVector *vector, const int32_t rowOffset)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
        int32_t rowIdx = rowOffset;
        for (size_t i = 0; i < rowCount; ++i) {
            auto &state = rowStates[i][aggIdx];
            if (state.val == nullptr) {
                uint8_t *res = nullptr;
                state.count = vector->GetValue(rowIdx, &res);
                state.count |= updateFlag;
                state.val = res;
            } else {
                state.val = OP(reinterpret_cast<uint8_t *>(state.val), state.count, vector, rowIdx);
            }
            ++rowIdx;
        }
    }
}

template<uint8_t * (*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP
inline void addDictUseRowIndexChar(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    const VarcharVector *vector, const int32_t * __restrict indexMap)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictUseRowIndexChar]: Dictionary Index Map pointer NOT aligned");
        }
#endif
        indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            auto &state = rowStates[i][aggIdx];
            if (state.val == nullptr) {
                uint8_t *res = nullptr;
                state.count = vector->GetValue(indexMap[i], &res);
                state.count |= updateFlag;
                state.val = res;
            } else {
                state.val = OP(reinterpret_cast<uint8_t *>(state.val), state.count, vector, indexMap[i]);
            }
        }
    }
}

template<uint8_t * (*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP
inline void addConditionalUseRowIndexChar(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    const VarcharVector *vector, const int32_t rowOffset, const uint8_t * __restrict condition)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalUseRowIndexChar]: ConditionMap pointer NOT aligned");
        }
#endif
        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);

        int32_t rowIdx = rowOffset;
        for (size_t i = 0; i < rowCount; ++i) {
            if (!(*condition)) {
                auto &state = rowStates[i][aggIdx];
                if (state.val == nullptr) {
                    uint8_t *res = nullptr;
                    state.count = vector->GetValue(rowIdx, &res);
                    state.count |= updateFlag;
                    state.val = res;
                } else {
                    state.val = OP(reinterpret_cast<uint8_t *>(state.val), state.count, vector, rowIdx);
                }
            }
            ++rowIdx;
            ++condition;
        }
    }
}

template<uint8_t * (*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP
inline void addDictConditionalUseRowIndexChar(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    const VarcharVector *vector, const uint8_t * __restrict condition,
    const int32_t * __restrict indexMap)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalUseRowIndexChar]: ConditionMap pointer NOT aligned");
        }
        if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalUseRowIndexChar]: Dictionary Index Map pointer NOT aligned");
        }
#endif
        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);
        indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            if (!condition[i]) {
                auto &state = rowStates[i][aggIdx];
                if (state.val == nullptr) {
                    uint8_t *res = nullptr;
                    state.count = vector->GetValue(indexMap[i], &res);
                    state.count |= updateFlag;
                    state.val = res;
                } else {
                    state.val = OP(reinterpret_cast<uint8_t *>(state.val), state.count, vector, indexMap[i]);
                }
            }
        }
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
class MinVarcharAggregator : public TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW> {
public:
    MinVarcharAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
        : TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            OMNI_AGGREGATION_TYPE_MIN, inputTypes, outputTypes, channels)
    {}

    ~MinVarcharAggregator() override = default;

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        auto vector = vectorBatch->GetVector(this->channels[0]);

        auto offsets =
            static_cast<int32_t *>(static_cast<int32_t *>(vector->GetValueOffsets()) + vector->GetPositionOffset());
        auto width = static_cast<VarcharDataType *>(inputTypes.GetType(0).get())->GetWidth();
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

            int32_t result = memcmp(
                    preMinVal,
                    reinterpret_cast<char *>(minVal),
                    std::min(state.count, static_cast<int64_t>(minLen))
            );
            if (result > 0 || (result == 0 && state.count > minLen)) {
                state.val = minVal;
                state.count = minLen;
            }
        }
    }

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
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

    void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
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

protected:
    ALWAYS_INLINE void ProcessRawInput(
        AggregateState &state, Vector *v, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap) override
    {
        VarcharVector *vector = static_cast<VarcharVector *>(v);

        if (indexMap == nullptr) {
            if (nullMap == nullptr) {
                addChar<minCharOp>(state, vector, rowOffset, rowCount);
            } else {
                addConditionalChar<minCharOp>(state, vector, rowOffset, rowCount, nullMap);
            }
        } else {
            if (nullMap == nullptr) {
                addDictChar<minCharOp>(state, vector, rowCount, indexMap);
            } else {
                addDictConditionalChar<minCharOp>(state, vector, rowCount, nullMap, indexMap);
            }
        }

        SaveState(state);
    }

    ALWAYS_INLINE void ProcessGroupRawInput(std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *v,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) override
    {
        VarcharVector *vector = static_cast<VarcharVector *>(v);

        if (indexMap == nullptr) {
            if (nullMap == nullptr) {
                addUseRowIndexChar<minCharOp>(rowStates, aggIdx, vector, rowOffset);
            } else {
                addConditionalUseRowIndexChar<minCharOp>(rowStates, aggIdx, vector, rowOffset, nullMap);
            }
        } else {
            if (nullMap == nullptr) {
                addDictUseRowIndexChar<minCharOp>(rowStates, aggIdx, vector, indexMap);
            } else {
                addDictConditionalUseRowIndexChar<minCharOp>(rowStates, aggIdx, vector, nullMap, indexMap);
            }
        }

        for (AggregateState *states : rowStates) {
            SaveState(states[aggIdx]);
        }
    }

private:
    ALWAYS_INLINE void SaveState(AggregateState &state)
    {
        if ((state.count & updateFlag) == 0) {
            return;
        }

        int32_t len = static_cast<int32_t>(state.count & valueFlag);
        if (state.val == nullptr || len == 0) {
            state.val = nullptr;
            state.count = 0;
            return;
        }

        uint8_t *ptr = reinterpret_cast<uint8_t *>(this->executionContext->GetArena()->Allocate(len));
        memcpy(ptr, state.val, len);
        state.val = ptr;
        state.count = len;
    }
};
}
}
#endif // OMNI_RUNTIME_MIN_VARCHAR_AGGREGATOR_H
