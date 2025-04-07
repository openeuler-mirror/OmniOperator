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
#pragma pack(push, 1)
struct VarcharState {
    int64_t realValue;
    int32_t len;
    bool needUpdate;

    static const VarcharState *ConstCastState(const AggregateState *state)
    {
        return reinterpret_cast<const VarcharState *>(state);
    }

    static VarcharState *CastState(AggregateState *state)
    {
        return reinterpret_cast<VarcharState *>(state);
    }
};
#pragma pack(pop)

inline void MinCharOp(VarcharState *state, BaseVector *vector, const int32_t idx)
{
    auto *rawVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
    auto strView = rawVector->GetValue(idx);
    const auto *curVal = strView.data();
    int32_t curLen = strView.size();
    auto len = state->len;
    auto result = memcmp((const char *)state->realValue, curVal, std::min(len, curLen));
    if (result > 0 || (result == 0 && len > curLen)) {
        state->len = curLen;
        state->needUpdate = true;
        state->realValue = reinterpret_cast<int64_t>(curVal);
    }
}

inline void MinDictCharOp(VarcharState *state, BaseVector *vector, const int32_t idx)
{
    auto *rawVector = reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
    auto strView = rawVector->GetValue(idx);
    const auto *curVal = strView.data();
    int32_t curLen = strView.size();
    int32_t len = state->len;
    auto result = memcmp((const char *)state->realValue, curVal, std::min(len, curLen));
    if (result > 0 || (result == 0 && len > curLen)) {
        state->len = curLen;
        state->needUpdate = true;
        state->realValue = reinterpret_cast<int64_t>(curVal);
        return;
    }
}

template <void (*OP)(VarcharState *, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddChar(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
    const int32_t rowCount)
{
    auto *rawVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
    if (rowCount > 0) {
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;
        auto *varcharState = VarcharState::CastState(state);
        if (varcharState->realValue == 0) {
            auto strView = rawVector->GetValue(idx++);
            varcharState->realValue = reinterpret_cast<int64_t>(strView.data());
            varcharState->len = strView.size();
            varcharState->needUpdate = true;
        }
        while (idx < end) {
            OP(varcharState, vector, idx++);
        }
    }
}

template <void (*OP)(VarcharState *state, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddDictChar(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
    const int32_t rowCount)
{
    if (rowCount > 0) {
        auto rawVector = reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
        auto *varcharState = VarcharState::CastState(state);
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;

        if (varcharState->realValue == 0) {
            auto strView = rawVector->GetValue(idx++);
            varcharState->len = strView.size();
            varcharState->realValue = reinterpret_cast<int64_t>(strView.data());
            varcharState->needUpdate = true;
        }
        while (idx < end) {
            OP(varcharState, vector, idx++);
        }
    }
}

template <void (*OP)(VarcharState *, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddConditionalChar(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
    const int32_t rowCount, const NullsHelper &condition)
{
    if (rowCount > 0) {
        auto rawVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);

        auto *varcharState = VarcharState::CastState(state);
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;

        int32_t index = 0;
        if (varcharState->realValue == 0) {
            while (idx < end) {
                if (!condition[index]) {
                    auto strView = rawVector->GetValue(idx);
                    varcharState->realValue = reinterpret_cast<int64_t>(strView.data());
                    varcharState->len = strView.size();
                    varcharState->needUpdate = true;
                    ++index;
                    ++idx;
                    break;
                }
                ++index;
                ++idx;
            }
        }

        while (idx < end) {
            if (!condition[index]) {
                OP(varcharState, vector, idx);
            }
            ++index;
            ++idx;
        }
    }
}

template <void (*OP)(VarcharState *, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddDictConditionalChar(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
    const int32_t rowCount, const NullsHelper &condition)
{
    if (rowCount > 0) {
        auto *rawVector = reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
        auto *varcharState = VarcharState::CastState(state);
        int32_t idx = rowOffset;
        const auto end = rowOffset + rowCount;

        if (varcharState->realValue == 0) {
            while (idx < end) {
                if (!condition[idx]) {
                    auto strView = rawVector->GetValue(idx);
                    varcharState->realValue = reinterpret_cast<int64_t>(strView.data());
                    varcharState->len = strView.size();
                    varcharState->needUpdate = true;
                    ++idx;
                    break;
                }
                ++idx;
            }
        }

        while (idx < end) {
            if (!condition[idx]) {
                OP(varcharState, vector, idx);
            }
            ++idx;
        }
    }
}

template <void (*OP)(VarcharState *, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddUseRowIndexChar(std::vector<AggregateState *> &rowStates, const size_t aggStateOffset,
    BaseVector *vector, const int32_t rowOffset)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
        auto *rawVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
        int32_t rowIdx = rowOffset;
        for (size_t i = 0; i < rowCount; ++i) {
            auto *state = VarcharState::CastState(rowStates[i] + aggStateOffset);
            if (state->realValue == 0) {
                auto strView = rawVector->GetValue(rowIdx);
                state->realValue = reinterpret_cast<int64_t>(strView.data());
                state->len = strView.size();
                state->needUpdate = true;
            } else {
                OP(state, vector, rowIdx);
            }
            ++rowIdx;
        }
    }
}

template <void (*OP)(VarcharState *, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddDictUseRowIndexChar(std::vector<AggregateState *> &rowStates, const size_t aggStateOffset,
    const int32_t rowOffset, BaseVector *vector)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
        auto *dictVector = reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
        int32_t rowIdx = rowOffset;
        for (size_t i = 0; i < rowCount; ++i) {
            auto *state = VarcharState::CastState(rowStates[i] + aggStateOffset);
            if (state->realValue == 0) {
                auto strView = dictVector->GetValue(rowIdx);
                auto *res = strView.data();
                state->len = strView.size();
                state->realValue = reinterpret_cast<int64_t>(res);
                state->needUpdate = true;
            } else {
                OP(state, vector, rowIdx);
            }
            rowIdx++;
        }
    }
}

template <void (*OP)(VarcharState *, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddConditionalUseRowIndexChar(std::vector<AggregateState *> &rowStates,
    const size_t aggStateOffset, BaseVector *vector, const int32_t rowOffset, const NullsHelper &condition)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
        auto *rawVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
        int32_t rowIdx = rowOffset;
        for (size_t i = 0; i < rowCount; ++i) {
            if (!condition[i]) {
                auto *state = VarcharState::CastState(rowStates[i] + aggStateOffset);
                if (state->realValue == 0) {
                    auto strView = rawVector->GetValue(rowIdx);
                    auto *res = strView.data();
                    state->len = strView.size();
                    state->realValue = reinterpret_cast<int64_t>(res);
                    state->needUpdate = true;
                } else {
                    OP(state, vector, rowIdx);
                }
            }
            ++rowIdx;
        }
    }
}

template <void (*OP)(VarcharState *, BaseVector *, const int32_t)>
VECTORIZE_LOOP inline void AddDictConditionalUseRowIndexChar(std::vector<AggregateState *> &rowStates,
    const size_t aggStateOffset, const int32_t rowOffset, BaseVector *vector, const NullsHelper &condition)
{
    const size_t rowCount = rowStates.size();
    if (rowCount > 0) {
        auto rawVector = reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
        int32_t rowIdx = rowOffset;
        for (size_t i = 0; i < rowCount; ++i) {
            if (!condition[i]) {
                auto *state = VarcharState::CastState(rowStates[i] + aggStateOffset);
                if (state->realValue == 0) {
                    auto strView = rawVector->GetValue(rowIdx);
                    state->len = strView.size();
                    state->realValue = reinterpret_cast<int64_t>(strView.data());
                    state->needUpdate = true;
                } else {
                    OP(state, vector, rowIdx);
                }
            }
            rowIdx++;
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> class MinVarcharAggregator : public TypedAggregator {
public:
    ~MinVarcharAggregator() override = default;

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override;

    std::vector<DataTypePtr> GetSpillType() override
    {
        std::vector<DataTypePtr> spillTypes;
        spillTypes.emplace_back(inputTypes.GetType(0));
        return spillTypes;
    }

    size_t GetStateSize() override
    {
        return sizeof(VarcharState);
    }

    void InitState(AggregateState *state) override
    {
        auto *varcharState = VarcharState::CastState(state + aggStateOffset);
        varcharState->realValue = 0;
        varcharState->len = 0;
        varcharState->needUpdate = false;
    }

    void InitStates(std::vector<AggregateState *> &states) override
    {
        for (auto *state : states) {
            InitState(state);
        }
    }

    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;

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

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override;

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

protected:
    MinVarcharAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
        : TypedAggregator(OMNI_AGGREGATION_TYPE_MIN, inputTypes, outputTypes, channels, inputRaw, outputPartial,
        isOverflowAsNull)
    {}

    void ProcessSingleInternal(AggregateState *state, BaseVector *v, const int32_t rowOffset, const int32_t rowCount,
        const std::shared_ptr<NullsHelper> nullMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *v, const int32_t rowOffset,
        const std::shared_ptr<NullsHelper> nullMap) override;

    template<typename T>
    void ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap);

private:
    ALWAYS_INLINE void SaveState(VarcharState *state);
    ALWAYS_INLINE void SaveState(AggregateState *state);
};
}
}
#endif // OMNI_RUNTIME_MIN_VARCHAR_AGGREGATOR_H
