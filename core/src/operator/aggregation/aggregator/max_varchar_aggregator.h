/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Max aggregate for varchar
 */
#ifndef OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H
#define OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H

#include "min_varchar_aggregator.h"

namespace omniruntime {
namespace op {
inline uint8_t *maxCharOp(uint8_t *res, int64_t &lenAndFlag, const VarcharVector *vector, const int32_t idx)
{
    uint8_t *curVal = nullptr;
    int32_t curLen = vector->GetValue(idx, &curVal);
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

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
class MaxVarcharAggregator : public TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW> {
public:
    ~MaxVarcharAggregator() override = default;

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override;

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override;
#endif

    void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override;

    static std::unique_ptr<Aggregator> Create(
        const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
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
            if (!TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>::CheckTypes(
                "max_varchar", inputTypes, outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }

            return std::unique_ptr<MaxVarcharAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>>(
                new MaxVarcharAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>(
                    inputTypes, outputTypes, channels));
        }
    }

protected:
    MaxVarcharAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            OMNI_AGGREGATION_TYPE_MAX, inputTypes, outputTypes, channels)
    {}

    void ProcessSingleInternal(
        AggregateState &state, Vector *v, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *v,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) override;


private:
    void SaveState(AggregateState &state);
};
}
}
#endif // OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H
