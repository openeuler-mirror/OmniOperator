/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Min aggregate for varchar
 */
#ifndef OMNI_RUNTIME_MIN_VARCHAR_AGGREGATOR_H
#define OMNI_RUNTIME_MIN_VARCHAR_AGGREGATOR_H

#include "typed_aggregator.h"
#include "operator/aggregation/definitions.h"

constexpr int64_t UPDATE_FLAG = 0x0000000100000000LL;
constexpr int64_t VALUE_FLAG = 0x00000000FFFFFFFFLL;

namespace omniruntime {
namespace op {
inline const char *MinCharOp(const char *res, int64_t &lenAndFlag, BaseVector *vector, const int32_t idx)
{
    auto *rawVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
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
    auto *rawVector = reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
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
    auto *rawVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
    if (rowCount > 0) {
        const char *res = reinterpret_cast<char *>(state.val);
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;

        if (state.val == 0) {
            auto strView = rawVector->GetValue(idx++);
            res = strView.data();
            state.count = strView.size();
            state.count |= UPDATE_FLAG;
        }
        while (idx < end) {
            res = OP(res, state.count, vector, idx++);
        }
        state.val = reinterpret_cast<int64_t>(res);
    }
}

template <const char *(*OP)(const char *, int64_t &, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddDictChar(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
    const int32_t rowCount)
{
    if (rowCount > 0) {
        auto rawVector = reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);

        const char *res = reinterpret_cast<char *>(state.val);
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;

        if (state.val == 0) {
            auto strView = rawVector->GetValue(idx++);
            res = strView.data();
            state.count = strView.size();
            state.count |= UPDATE_FLAG;
        }
        while (idx < end) {
            res = OP(res, state.count, vector, idx++);
        }
        state.val = reinterpret_cast<int64_t>(res);
    }
}

template <const char *(*OP)(const char *, int64_t &, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddConditionalChar(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
    const int32_t rowCount, const uint8_t *__restrict condition)
{
    if (rowCount > 0) {
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[AddConditionalChar]: ConditionMap pointer NOT aligned");
        }
        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);
        auto rawVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);

        const char *res = reinterpret_cast<char *>(state.val);
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;

        if (state.val == 0) {
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
        state.val = reinterpret_cast<int64_t>(res);
    }
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
        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);
        auto *rawVector = reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
        const char *res = reinterpret_cast<char *>(state.val);
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;

        if (state.val == 0) {
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
        state.val = reinterpret_cast<int64_t>(res);
    }
}

template <const char *(*OP)(const char *, int64_t &, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddUseRowIndexChar(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    BaseVector *vector, const int32_t rowOffset)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
        auto *rawVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
        int32_t rowIdx = rowOffset;
        for (size_t i = 0; i < rowCount; ++i) {
            auto &state = rowStates[i][aggIdx];
            if (state.val == 0) {
                auto strView = rawVector->GetValue(rowIdx);
                auto *res = strView.data();
                state.count = strView.size();
                state.count |= UPDATE_FLAG;
                state.val = (int64_t) (res);
            } else {
                state.val = (int64_t)(OP(reinterpret_cast<char *>(state.val), state.count, vector, rowIdx));
            }
            ++rowIdx;
        }
    }
}

template <const char *(*OP)(const char *, int64_t &, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddDictUseRowIndexChar(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    const int32_t rowOffset, BaseVector *vector)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
        auto *dictVector = reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
        int32_t rowIdx = rowOffset;
        for (size_t i = 0; i < rowCount; ++i) {
            auto &state = rowStates[i][aggIdx];
            if (state.val == 0) {
                auto strView = dictVector->GetValue(rowIdx);
                auto *res = strView.data();
                state.count = strView.size();
                state.count |= UPDATE_FLAG;
                state.val = (int64_t) res;
            } else {
                state.val = (int64_t)(OP(reinterpret_cast<char *>(state.val), state.count, vector, rowIdx));
            }
            rowIdx++;
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
        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);
        auto *rawVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
        int32_t rowIdx = rowOffset;
        for (size_t i = 0; i < rowCount; ++i) {
            if (!(*condition)) {
                auto &state = rowStates[i][aggIdx];
                if (state.val == 0) {
                    auto strView = rawVector->GetValue(rowIdx);
                    auto *res = strView.data();
                    state.count = strView.size();
                    state.count |= UPDATE_FLAG;
                    state.val = (int64_t) (res);
                } else {
                    state.val = (int64_t)(OP(reinterpret_cast<char *>(state.val), state.count, vector, rowIdx));
                }
            }
            ++rowIdx;
            ++condition;
        }
    }
}

template <const char *(*OP)(const char *, int64_t &, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddDictConditionalUseRowIndexChar(std::vector<AggregateState *> &rowStates,
    const size_t aggIdx, const int32_t rowOffset, BaseVector *vector, const uint8_t *__restrict condition)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addDictConditionalUseRowIndexChar]: ConditionMap pointer NOT aligned");
        }
#endif
        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);
        auto rawVector = reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
        int32_t rowIdx = rowOffset;
        for (size_t i = 0; i < rowCount; ++i) {
            if (!condition[i]) {
                auto &state = rowStates[i][aggIdx];
                if (state.val == 0) {
                    auto strView = rawVector->GetValue(rowIdx);
                    auto res = strView.data();
                    state.count = strView.size();
                    state.count |= UPDATE_FLAG;
                    state.val = (int64_t) (res);
                } else {
                    state.val = (int64_t)(OP(reinterpret_cast<const char *>(state.val), state.count, vector, rowIdx));
                }
            }
            rowIdx++;
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> class MinVarcharAggregator : public TypedAggregator {
public:
    ~MinVarcharAggregator() override = default;

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void GetSpillType(std::vector<DataTypePtr> &spillTypes) override
    {
        spillTypes.push_back(inputTypes.GetType(0));
    }
    void ExtractSpillValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
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

    void ProcessGroupAfterSpill(AggregateState &state, VectorBatch *vectorBatch, int32_t &vectorIndex,
        int32_t rowIdx) override;

protected:
    MinVarcharAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
        : TypedAggregator(OMNI_AGGREGATION_TYPE_MIN, inputTypes, outputTypes, channels, inputRaw, outputPartial,
        isOverflowAsNull)
    {}

    void ProcessSingleInternal(AggregateState &state, BaseVector *v, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *v,
        const int32_t rowOffset, const uint8_t *nullMap) override;

private:
    ALWAYS_INLINE void SaveState(AggregateState &state);
};
}
}
#endif // OMNI_RUNTIME_MIN_VARCHAR_AGGREGATOR_H
