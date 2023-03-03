/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
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
constexpr int64_t VALUE_FLAG = 0x00000000FFFFFFFFLL;

namespace omniruntime {
namespace op {
inline const char *MinCharOp(const char *res, int64_t &lenAndFlag, BaseVector *vector, const int32_t idx)
{
    auto *rawVector = reinterpret_cast<BaseVector<LargeStringContainer<std::string_view>> *>(vector);
    auto strView = rawVector->GetValue(idx);
    const auto *curVal = strView.data();
    int32_t curLen = strView.size();
    auto len = static_cast<int32_t>(lenAndFlag & VALUE_FLAG);
    auto result = memcmp(res, curVal, std::min(len, curLen));
    if (result > 0 || (result == 0 && len > curLen)) {
        lenAndFlag = curLen;
        lenAndFlag |= UPDATE_FLAG;
        return curVal;
    } else {
        return res;
    }
}

inline const char *MinDictCharOp(const char *res, int64_t &lenAndFlag, BaseVector *vector, const int32_t idx)
{
    auto *rawVector = reinterpret_cast<BaseVector<DictionaryContainer<std::string_view>> *>(vector);
    auto strView = rawVector->GetValue(idx);
    const auto *curVal = strView.data();
    int32_t curLen = strView.size();
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

template <const char *(*OP)(const char *, int64_t &, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddChar(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
    const int32_t rowCount)
{
    auto *rawVector = reinterpret_cast<BaseVector<LargeStringContainer<std::string_view>> *>(vector);
    if (rowCount > 0) {
        const char *res = reinterpret_cast<char *>(state.val);
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;

        if (state.val == nullptr || state.count == 0) {
            auto strView = rawVector->GetValue(idx++);
            res = strView.data();
            state.count = strView.size();
            state.count |= UPDATE_FLAG;
        }
        while (idx < end) {
            res = OP(res, state.count, vector, idx++);
        }
        state.val = const_cast<char*>(res);
    }
}

template <uint8_t *(*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP inline void AddCharFilter(AggregateState &state, const VarcharVector *vector, const int32_t rowOffset,
                                         const int32_t rowCount, const int8_t *__restrict boolPtr)
{
    boolPtr = (const int8_t *)__builtin_assume_aligned(boolPtr, ARRAY_ALIGNMENT);
    if (rowCount > 0) {
        uint8_t *res = reinterpret_cast<uint8_t *>(state.val);
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;

        if (state.val == nullptr || state.count == 0) {
            if (boolPtr[idx]) {
                state.count = vector->GetValue(idx++, &res);
                state.count |= UPDATE_FLAG;
            }
        }
        while (idx < end) {
            if (boolPtr[idx]) {
                res = OP(res, state.count, vector, idx++);
            }
        }
        state.val = res;
    }
}
    
template <const char *(*OP)(const char *, int64_t &, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddDictChar(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
    const int32_t rowCount)
{
    if (rowCount > 0) {
        auto rawVector = reinterpret_cast<BaseVector<DictionaryContainer<std::string_view>>*>(vector);

        const char *res = reinterpret_cast<char *>(state.val);
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;

        if (state.val == nullptr || state.count == 0) {
            auto strView = rawVector->GetValue(idx++);
            res = strView.data();
            state.count = strView.size();
            state.count |= UPDATE_FLAG;
        }
        while (idx < end) {
            res = OP(res, state.count, vector, idx++);
        }
        state.val = const_cast<char*>(res);
    }
}

template <uint8_t *(*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP inline void AddDictCharFilter(AggregateState &state, const VarcharVector *vector, const int32_t rowCount,
    const int32_t *__restrict indexMap, const int8_t *__restrict boolPtr)
{
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictChar]: Dictionary Index Map pointer NOT aligned");
        }
#endif
        indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);
        boolPtr = (const int8_t *)__builtin_assume_aligned(boolPtr, ARRAY_ALIGNMENT);

        uint8_t *res = reinterpret_cast<uint8_t *>(state.val);
        int32_t idx = 0;

        if (boolPtr[idx]) {
            if (state.val == nullptr || state.count == 0) {
                state.count = vector->GetValue(indexMap[idx++], &res);
                state.count |= UPDATE_FLAG;
            }
        }

        while (idx < rowCount) {
            if (boolPtr[idx]) {
                res = OP(res, state.count, vector, indexMap[idx++]);
            }
        }
        state.val = res;
    }
}

template <const char *(*OP)(const char *, int64_t &, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddConditionalChar(AggregateState &state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const uint8_t *__restrict condition)
{
    if (rowCount > 0) {
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[AddConditionalChar]: ConditionMap pointer NOT aligned");
        }
        auto rawVector = reinterpret_cast<BaseVector<LargeStringContainer<std::string_view>> *>(vector);
//        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);

        const char *res = reinterpret_cast<char *>(state.val);
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;

        if (state.val == nullptr || state.count == 0) {
            while (idx < end) {
                if (!(*condition)) {
                    auto strView = rawVector->GetValue(idx);
                    res = strView.data();
                    state.count = strView.size();
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
        state.val = const_cast<char*>(res);
    }
}

template <uint8_t *(*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP inline void AddConditionalCharFilter(AggregateState &state, const VarcharVector *vector,
                                                    const int32_t rowOffset, const int32_t rowCount, const uint8_t *__restrict condition,
                                                    const int8_t *__restrict boolPtr)
{
    if (rowCount <= 0) {
        return;
    }

    if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
        LogWarn("[AddConditionalChar]: ConditionMap pointer NOT aligned");
    }
    condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);
    boolPtr = (const int8_t *)__builtin_assume_aligned(boolPtr, ARRAY_ALIGNMENT);
    uint8_t *res = reinterpret_cast<uint8_t *>(state.val);
    int32_t idx = rowOffset;
    const auto end = rowOffset + rowCount;

    if (state.val == nullptr || state.count == 0) {
        while (idx < end) {
            if (boolPtr[idx] && !(*condition)) {
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
        if (boolPtr[idx] && !(*condition)) {
            res = OP(res, state.count, vector, idx);
        }
        ++condition;
        ++idx;
    }
    state.val = res;
}
    
template <const char *(*OP)(const char *, int64_t &, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddDictConditionalChar(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
    const int32_t rowCount, const uint8_t *__restrict condition)
{
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalChar]: ConditionMap pointer NOT aligned");
        }
#endif
//        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);

        auto *rawVector = reinterpret_cast<BaseVector<DictionaryContainer<std::string_view>>*>(vector);
        const char *res = reinterpret_cast<char *>(state.val);
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;

        if (state.val == nullptr || state.count == 0) {
            while (idx < end) {
                if (!condition[idx]) {
                    auto strView = rawVector->GetValue(idx);
                    res = strView.data();
                    state.count = strView.size();
                    state.count |= UPDATE_FLAG;
                    ++idx;
                    break;
                }
                ++idx;
            }
        }

        while (idx < end) {
            if (!condition[idx]) {
                res = OP(res, state.count, vector, idx);
            }
            ++idx;
        }
        state.val = const_cast<char*>(res);
    }
}

template <uint8_t *(*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP inline void AddDictConditionalCharFilter(AggregateState &state, const VarcharVector *vector,
    const int32_t rowCount, const uint8_t *__restrict condition, const int32_t *__restrict indexMap,
    const int8_t *__restrict boolPtr)
{
    if (rowCount <= 0) {
        return;
    }
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
    boolPtr = (const int8_t *)__builtin_assume_aligned(boolPtr, ARRAY_ALIGNMENT);

    uint8_t *res = reinterpret_cast<uint8_t *>(state.val);
    int32_t idx = 0;

    if (state.val == nullptr || state.count == 0) {
        while (idx < rowCount) {
            if (boolPtr[idx] && !condition[idx]) {
                state.count = vector->GetValue(indexMap[idx], &res);
                state.count |= UPDATE_FLAG;
                ++idx;
                break;
            }
            ++idx;
        }
    }

    while (idx < rowCount) {
        if (boolPtr[idx] && !condition[idx]) {
            res = OP(res, state.count, vector, indexMap[idx]);
        }
        ++idx;
    }
    state.val = res;
}


template <const char *(*OP)(const char *, int64_t &, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddUseRowIndexChar(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    BaseVector *vector, const int32_t rowOffset)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
        auto *rawVector = reinterpret_cast<BaseVector<LargeStringContainer<std::string_view>>*>(vector);
        int32_t rowIdx = rowOffset;
        for (size_t i = 0; i < rowCount; ++i) {
            auto &state = rowStates[i][aggIdx];
            if (state.val == nullptr || state.count == 0) {
                auto strView = rawVector->GetValue(rowIdx);
                auto *res = strView.data();
                state.count = strView.size();
                state.count |= UPDATE_FLAG;
                state.val = const_cast<char *>(res);
            } else {
                state.val = const_cast<char*>(
                    OP(reinterpret_cast<char*>(state.val), state.count, vector, rowIdx));
            }
            ++rowIdx;
        }
    }
}

template <uint8_t *(*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP inline void AddUseRowIndexCharFilter(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
                                                    const VarcharVector *vector, const int32_t rowOffset, const int8_t *__restrict boolPtr)
{
    const size_t rowCount = rowStates.size();
    if (rowCount <= 0) {
        return;
    }
    boolPtr = (const int8_t *)__builtin_assume_aligned(boolPtr, ARRAY_ALIGNMENT);
    int32_t rowIdx = rowOffset;
    for (size_t i = 0; i < rowCount; ++i) {
        if (boolPtr[i]) {
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
    }
}

template <const char *(*OP)(const char *, int64_t &, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddDictUseRowIndexChar(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    const int32_t rowOffset, BaseVector *vector)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
        auto *dictVector = reinterpret_cast<BaseVector<DictionaryContainer<std::string_view>>*>(vector);
        int32_t rowIdx = rowOffset;
        for (size_t i = 0; i < rowCount; ++i) {
            auto &state = rowStates[i][aggIdx];
            if (state.val == nullptr || state.count == 0) {
                auto strView = dictVector->GetValue(rowIdx);
                auto *res = strView.data();
                state.count = strView.size();
                state.count |= UPDATE_FLAG;
                state.val = const_cast<char *>(res);
            } else {
                state.val = const_cast<char*>(
                    OP(reinterpret_cast<char *>(state.val), state.count, vector, rowIdx));
            }
            rowIdx++;
        }
    }
}

template <uint8_t *(*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP inline void AddDictUseRowIndexCharFilter(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    const VarcharVector *vector, const int32_t *__restrict indexMap, const int8_t *__restrict boolPtr)
{
    const size_t rowCount = rowStates.size();
    if (rowCount <= 0) {
        return;
    }

#ifdef DEBUG
    if (reinterpret_cast<unsigned long>(indexMap) % ARRAY_ALIGNMENT != 0) {
        LogWarn("[addDictUseRowIndexChar]: Dictionary Index Map pointer NOT aligned");
    }
#endif
    indexMap = (const int32_t *)__builtin_assume_aligned(indexMap, ARRAY_ALIGNMENT);
    boolPtr = (const int8_t *)__builtin_assume_aligned(boolPtr, ARRAY_ALIGNMENT);
    for (size_t i = 0; i < rowCount; ++i) {
        if (boolPtr[i]) {
            auto &state = rowStates[i][aggIdx];
            if (state.val == nullptr || state.count == 0) {
                uint8_t *res = nullptr;
                state.count = vector->GetValue(indexMap[i], &res);
                state.count |= UPDATE_FLAG;
                state.val = res;
                continue;
            }
            state.val = OP(reinterpret_cast<uint8_t *>(state.val), state.count, vector, indexMap[i]);
        }
    }
}


template <const char *(*OP)(const char *, int64_t &, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddConditionalUseRowIndexChar(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    BaseVector *vector, const int32_t rowOffset, const uint8_t *__restrict condition)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalUseRowIndexChar]: ConditionMap pointer NOT aligned");
        }
#endif
//        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);
        auto *rawVector = reinterpret_cast<BaseVector<LargeStringContainer<std::string_view>>*>(vector);
        int32_t rowIdx = rowOffset;
        for (size_t i = 0; i < rowCount; ++i) {
            if (!(*condition)) {
                auto &state = rowStates[i][aggIdx];
                if (state.val == nullptr || state.count == 0) {
                    auto strView = rawVector->GetValue(rowIdx);
                    auto *res = strView.data();
                    state.count = strView.size();
                    state.count |= UPDATE_FLAG;
                    state.val = const_cast<char*>(res);
                } else {
                    state.val = const_cast<char*>(OP(reinterpret_cast<char *>(state.val), state.count, vector, rowIdx));
                }
            }
            ++rowIdx;
            ++condition;
        }
    }
}

template <uint8_t *(*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP inline void AddConditionalUseRowIndexCharFilter(std::vector<AggregateState *> &rowStates,
                                                               const size_t aggIdx, const VarcharVector *vector, const int32_t rowOffset, const uint8_t *__restrict condition,
                                                               const int8_t *__restrict boolPtr)
{
    const size_t rowCount = rowStates.size();
    if (rowCount <= 0) {
        return;
    }

#ifdef DEBUG
    if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
    LogWarn("[addConditionalUseRowIndexChar]: ConditionMap pointer NOT aligned");
}
#endif
    condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);
    boolPtr = (const int8_t *)__builtin_assume_aligned(boolPtr, ARRAY_ALIGNMENT);

    int32_t rowIdx = rowOffset;
    for (size_t i = 0; i < rowCount; ++i) {
        if (boolPtr[i] && !(*condition)) {
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

template <const char *(*OP)(const char *, int64_t &, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddDictConditionalUseRowIndexChar(std::vector<AggregateState *> &rowStates,
    const int32_t rowOffset, const size_t aggIdx, BaseVector *vector, const uint8_t *__restrict condition)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalUseRowIndexChar]: ConditionMap pointer NOT aligned");
        }
#endif
//        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);
        auto rawVector = reinterpret_cast<BaseVector<DictionaryContainer<std::string_view>>*>(vector);
        int32_t rowIdx = rowOffset;
        for (size_t i = 0; i < rowCount; ++i) {
            if (!condition[i]) {
                auto &state = rowStates[i][aggIdx];
                if (state.val == nullptr || state.count == 0) {
                    auto strView = rawVector->GetValue(rowIdx);
                    auto res = strView.data();
                    state.count = strView.size();
                    state.count |= UPDATE_FLAG;
                    state.val = const_cast<char*>(res);
                } else {
                    state.val = const_cast<char*>(OP(reinterpret_cast<const char *>(state.val),
                        state.count, vector, rowIdx));
                }
            }
            rowIdx++;
        }
    }
}


template <uint8_t *(*OP)(uint8_t *, int64_t &, const VarcharVector *, const int32_t)>
VECTORIZE_LOOP inline void AddDictConditionalUseRowIndexCharFilter(std::vector<AggregateState *> &rowStates,
    const size_t aggIdx, const VarcharVector *vector, const uint8_t *__restrict condition,
    const int32_t *__restrict indexMap, const int8_t *__restrict boolPtr)
{
    const size_t rowCount = rowStates.size();
    if (rowCount <= 0) {
        return;
    }
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
    boolPtr = (const int8_t *)__builtin_assume_aligned(boolPtr, ARRAY_ALIGNMENT);

    for (size_t i = 0; i < rowCount; ++i) {
        if (boolPtr[i] && !condition[i]) {
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

template <DataTypeId IN_ID, DataTypeId OUT_ID> class MinVarcharAggregator : public TypedAggregator {
public:
    ~MinVarcharAggregator() override = default;

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override;

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override;
#endif

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull)
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
            if (!TypedAggregator::CheckTypes("min_varchar", inputTypes, outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }

            return std::unique_ptr<MinVarcharAggregator<IN_ID, OUT_ID>>(new MinVarcharAggregator<IN_ID, OUT_ID>(
                inputTypes, outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
        }
    }

protected:
    MinVarcharAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
        : TypedAggregator(OMNI_AGGREGATION_TYPE_MIN, inputTypes, outputTypes, channels, inputRaw, outputPartial,
        isOverflowAsNull)
    {}

    void ProcessSingleInternal(AggregateState &state, BaseVector *v, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessSingleInternalFilter(AggregateState &state, BaseVector *v, BooleanVector *booleanVector,
        const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *v,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessGroupInternalFilter(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *v,
        BooleanVector *booleanVector, const int32_t rowOffset, const uint8_t *nullMap,
        const int32_t *indexMap) override;

private:
    ALWAYS_INLINE void SaveState(AggregateState &state);
};
}
}
#endif // OMNI_RUNTIME_MIN_VARCHAR_AGGREGATOR_H
