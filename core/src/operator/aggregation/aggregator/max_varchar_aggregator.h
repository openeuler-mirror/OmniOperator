/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Max aggregate for varchar
 */
#ifndef OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H
#define OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H

#include "min_varchar_aggregator.h"

namespace omniruntime {
namespace op {
inline const char *MaxCharOp(const char *res, int64_t &lenAndFlag, BaseVector *vector, const int32_t idx)
{
    auto *rawStringVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
    auto strView = rawStringVector->GetValue(idx);
    int32_t curLen = strView.size();
    auto *curVal = strView.data();
    int32_t len = static_cast<int32_t>(lenAndFlag & VALUE_FLAG);
    auto result = memcmp(res, curVal, std::min(len, curLen));
    if (result < 0 || (result == 0 && len < curLen)) {
        lenAndFlag = curLen;
        lenAndFlag |= UPDATE_FLAG;
        return curVal;
    } else {
        return res;
    }
}

inline const char *MaxDictCharOp(const char *res, int64_t &lenAndFlag, BaseVector *vector, const int32_t idx)
{
    auto *rawStringVector = reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
    auto strView = rawStringVector->GetValue(idx);
    int32_t curLen = strView.size();
    auto *curVal = strView.data();
    int32_t len = static_cast<int32_t>(lenAndFlag & VALUE_FLAG);
    auto result = memcmp(res, curVal, std::min(len, curLen));
    if (result < 0 || (result == 0 && len < curLen)) {
        lenAndFlag = curLen;
        lenAndFlag |= UPDATE_FLAG;
        return curVal;
    } else {
        return res;
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> class MaxVarcharAggregator : public TypedAggregator {
public:
    ~MaxVarcharAggregator() override = default;

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, const size_t aggIdx,
        std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount) override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, const size_t aggIdx,
        std::vector<BaseVector *> &vectors) override;

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

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, const size_t aggIdx,
        int32_t &vectorIndex) override;

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector) override;

protected:
    MaxVarcharAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
        : TypedAggregator(OMNI_AGGREGATION_TYPE_MAX, inputTypes, outputTypes, channels, inputRaw, outputPartial,
        isOverflowAsNull)
    {}

    void ProcessSingleInternal(AggregateState &state, BaseVector *v, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *v,
        const int32_t rowOffset, const uint8_t *nullMap) override;

private:
    void SaveState(AggregateState &state);
};
}
}
#endif // OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H
