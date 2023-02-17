/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Count aggregate
 */
#ifndef OMNI_RUNTIME_COUNT_ALL_AGGREGATOR_H
#define OMNI_RUNTIME_COUNT_ALL_AGGREGATOR_H

#include "count_column_aggregator.h"

namespace omniruntime {
namespace op {
template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
class CountAllAggregator : public CountColumnAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID> {
public:
    ~CountAllAggregator() override = default;

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels)
    {
        if constexpr (OUT_ID != OMNI_LONG) {
            LogError("Error in count all aggregator: Expecting long output type. Got %s",
                TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        } else if constexpr (!RAW_IN && IN_ID != OMNI_LONG) {
            LogError("Error in count all aggregator: Expecting long intput type for partial input. Got %s",
                TypeUtil::TypeToStringLog(IN_ID).c_str());
            return nullptr;
        } else {
            if (!TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>::CheckTypes("count all", inputTypes, outputTypes,
                IN_ID, OUT_ID)) {
                return nullptr;
            }
            return std::unique_ptr<CountAllAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>>(
                new CountAllAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>(outputTypes, channels));
        }
    }

protected:
    CountAllAggregator(const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : CountColumnAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>(OMNI_AGGREGATION_TYPE_COUNT_ALL,
        outputTypes, channels)
    {}

    virtual ALWAYS_INLINE Vector *GetVector(VectorBatch *vectorBatch, const int32_t rowOffset, const int32_t rowCount,
        uint8_t **nullMap, AggregatorBuffer<int32_t> &indexMap, const size_t channelIdx) override
    {
        if constexpr (RAW_IN) {
            *nullMap = nullptr;
            indexMap.Release();
            return nullptr;
        } else {
            return TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>::GetVector(vectorBatch, rowOffset, rowCount,
                nullMap, indexMap, channelIdx);
        }
    }
};
}
}
#endif // OMNI_RUNTIME_COUNT_ALL_AGGREGATOR_H
