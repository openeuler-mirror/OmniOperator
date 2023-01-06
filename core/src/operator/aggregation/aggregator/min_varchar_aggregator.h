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

constexpr int64_t UPDATE_FLAG = 0x0000000100000000LL;
constexpr int64_t VALUE_FLAG  = 0x00000000FFFFFFFFLL;

namespace omniruntime {
namespace op {
inline uint8_t *minCharOp(uint8_t *res, int64_t &lenAndFlag, const VarcharVector *vector, const int32_t idx)
{
    uint8_t *curVal = nullptr;
    int32_t curLen = vector->GetValue(idx, &curVal);
    int32_t len = static_cast<int32_t>(lenAndFlag & VALUE_FLAG);
    auto result = memcmp(res, curVal, std::min(len, curLen));
    if (result > 0 || (result == 0 && len > curLen)) {
        lenAndFlag = curLen;
        lenAndFlag |= UPDATE_FLAG;
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
        uint8_t *res = reinterpret_cast<uint8_t *>(state.val);
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;

        if (state.val == nullptr || state.count == 0) {
            state.count = vector->GetValue(idx++, &res);
            state.count |= UPDATE_FLAG;
        }
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

        uint8_t *res = reinterpret_cast<uint8_t *>(state.val);
        int32_t idx = 0;

        if (state.val == nullptr || state.count == 0) {
            state.count = vector->GetValue(indexMap[idx++], &res);
            state.count |= UPDATE_FLAG;
        }
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

        uint8_t *res = reinterpret_cast<uint8_t *>(state.val);
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;

        if (state.val == nullptr || state.count == 0) {
            while (idx < end) {
                if (!(*condition)) {
                    state.count = vector->GetValue(idx, &res);
                    state.count |= UPDATE_FLAG;
                    ++condition;
                    ++idx;
                    break;
                }
                ++condition;
                ++idx;
            }
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

        uint8_t *res = reinterpret_cast<uint8_t *>(state.val);
        int32_t idx = 0;

        if (state.val == nullptr || state.count == 0) {
            while (idx < rowCount) {
                if (!condition[idx]) {
                    state.count = vector->GetValue(indexMap[idx], &res);
                    state.count |= UPDATE_FLAG;
                    ++idx;
                    break;
                }
                ++idx;
            }
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
            if (state.val == nullptr || state.count == 0) {
                uint8_t *res = nullptr;
                state.count = vector->GetValue(rowIdx, &res);
                state.count |= UPDATE_FLAG;
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
            if (state.val == nullptr || state.count == 0) {
                uint8_t *res = nullptr;
                state.count = vector->GetValue(indexMap[i], &res);
                state.count |= UPDATE_FLAG;
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
                if (state.val == nullptr || state.count == 0) {
                    uint8_t *res = nullptr;
                    state.count = vector->GetValue(rowIdx, &res);
                    state.count |= UPDATE_FLAG;
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
                if (state.val == nullptr || state.count == 0) {
                    uint8_t *res = nullptr;
                    state.count = vector->GetValue(indexMap[i], &res);
                    state.count |= UPDATE_FLAG;
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
    ~MinVarcharAggregator() override = default;

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override;

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override;
#endif

    void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override;

    static std::unique_ptr<Aggregator> Create(
        const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
    {
        if constexpr (!(IN_ID == OMNI_CHAR || IN_ID == OMNI_VARCHAR)) {
            LogError("Error in min_varchar aggregator: Unsupported input type %s",
                TypeUtil::TypeToStringLog(IN_ID).c_str());
            return nullptr;
        } else if constexpr (!(OUT_ID == OMNI_CHAR || OUT_ID == OMNI_VARCHAR)) {
            LogError("Error in min_varchar aggregator: Unsupported output type %s",
                TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        } else if constexpr (IN_ID != OUT_ID) {
            LogError(
                "Error in min_varchar aggregator: Expecting same input output type. Got %s input and %s output types",
                TypeUtil::TypeToStringLog(IN_ID).c_str(), TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        } else {
            if (!TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>::CheckTypes(
                "min_varchar", inputTypes, outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }

            return std::unique_ptr<MinVarcharAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>>(
                new MinVarcharAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>(
                    inputTypes, outputTypes, channels));
        }
    }

protected:
    MinVarcharAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            OMNI_AGGREGATION_TYPE_MIN, inputTypes, outputTypes, channels)
    {}

    void ProcessSingleInternal(
        AggregateState &state, Vector *v, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *v,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) override;

private:
    ALWAYS_INLINE void SaveState(AggregateState &state);
};
}
}
#endif // OMNI_RUNTIME_MIN_VARCHAR_AGGREGATOR_H
