/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Max aggregate for varchar
 */
#ifndef OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H
#define OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H

#include "min_varchar_aggregator.h"

namespace omniruntime {
namespace op {
inline void MaxCharOp(VarcharState *state, BaseVector *vector, const int32_t idx)
{
    auto *rawStringVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
    auto strView = rawStringVector->GetValue(idx);
    int32_t curLen = strView.size();
    auto *curVal = strView.data();
    auto result = memcmp(reinterpret_cast<const char *>(state->realValue), curVal, std::min(state->len, curLen));
    if (result < 0 || (result == 0 && state->len < curLen)) {
        state->len = curLen;
        state->needUpdate = true;
        state->realValue = reinterpret_cast<int64_t>(curVal);
    }
}

inline void MaxDictCharOp(VarcharState *state, BaseVector *vector, const int32_t idx)
{
    auto *rawStringVector = reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
    auto strView = rawStringVector->GetValue(idx);
    int32_t curLen = strView.size();
    auto *curVal = strView.data();
    auto result = memcmp(reinterpret_cast<const char *>(state->realValue), curVal, std::min(state->len, curLen));
    if (result < 0 || (result == 0 && state->len < curLen)) {
        state->len = curLen;
        state->needUpdate = true;
        state->realValue = reinterpret_cast<int64_t>(curVal);
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> class MaxVarcharAggregator : public TypedAggregator {
public:
    ~MaxVarcharAggregator() override = default;

    size_t GetStateSize() override
    {
        return sizeof(VarcharState);
    }

    void InitState(AggregateState *state) override
    {
        auto *maxState = VarcharState::CastState(state + aggStateOffset);
        maxState->realValue = 0;
        maxState->len = 0;
        maxState->needUpdate = false;
    }

    void InitStates(std::vector<AggregateState *> &groupStates) override
    {
        for (auto groupState : groupStates) {
            InitState(groupState);
        }
    }

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;

    std::vector<DataTypePtr> GetSpillType() override
    {
        std::vector<DataTypePtr> spillTypes;
        spillTypes.emplace_back(inputTypes.GetType(0));
        return spillTypes;
    }

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull)
    {
        if constexpr (!(IN_ID == OMNI_CHAR || IN_ID == OMNI_VARCHAR)) {
            LogError("Error in max_varchar aggregator: Unsupported input type %s",
                TypeUtil::TypeToStringLog(IN_ID).c_str());
            return nullptr;
        } else if constexpr (!(OUT_ID == OMNI_CHAR || OUT_ID == OMNI_VARCHAR)) {
            LogError("Error in max_varchar aggregator: Unsupported output type %s",
                TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        } else if constexpr (IN_ID != OUT_ID) {
            LogError(
                "Error in max_varchar aggregator: Expecting same input output type. Got %s input and %s output types",
                TypeUtil::TypeToStringLog(IN_ID).c_str(), TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        } else {
            if (!TypedAggregator::CheckTypes("max_varchar", inputTypes, outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }

            return std::unique_ptr<MaxVarcharAggregator<IN_ID, OUT_ID>>(new MaxVarcharAggregator<IN_ID, OUT_ID>(
                inputTypes, outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
        }
    }

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override;

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

protected:
    MaxVarcharAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
        : TypedAggregator(OMNI_AGGREGATION_TYPE_MAX, inputTypes, outputTypes, channels, inputRaw, outputPartial,
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
    void SaveState(VarcharState *state);
    void SaveState(AggregateState *state);
};
}
}
#endif // OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H
