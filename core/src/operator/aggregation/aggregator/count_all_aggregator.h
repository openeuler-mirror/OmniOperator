/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Count aggregate
 */
#ifndef OMNI_RUNTIME_COUNT_ALL_AGGREGATOR_H
#define OMNI_RUNTIME_COUNT_ALL_AGGREGATOR_H

#include "count_column_aggregator.h"

namespace omniruntime {
namespace op {
template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW>
class CountAllAggregator : public CountColumnAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW> {
public:
    CountAllAggregator(const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : CountColumnAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            OMNI_AGGREGATION_TYPE_COUNT_ALL, outputTypes, channels)
    {}

    ~CountAllAggregator() override = default;

protected:
    virtual ALWAYS_INLINE Vector *GetVector(VectorBatch *vectorBatch, const int32_t rowOffset, const int32_t rowCount,
        uint8_t **nullMap, AggregatorBuffer<int32_t> &indexMap, const size_t channelIdx) override
    {
        if constexpr (RAW_IN) {
            *nullMap = nullptr;
            indexMap.Release();
            return nullptr;
        } else {
            return TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>::GetVector(
                vectorBatch, rowOffset, rowCount, nullMap, indexMap, channelIdx);
        }
    }
};
}
}
#endif // OMNI_RUNTIME_COUNT_ALL_AGGREGATOR_H
