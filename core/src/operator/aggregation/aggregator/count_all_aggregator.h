/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Count aggregate
 */
#ifndef OMNI_RUNTIME_COUNT_ALL_AGGREGATOR_H
#define OMNI_RUNTIME_COUNT_ALL_AGGREGATOR_H

#include "count_column_aggregator.h"

namespace omniruntime {
namespace op {
template <DataTypeId IN_ID, DataTypeId OUT_ID> class CountAllAggregator : public CountColumnAggregator<IN_ID, OUT_ID> {
public:
    ~CountAllAggregator() override = default;

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inRaw, bool outPartial, bool isOverflowAsNull)
    {
        if constexpr (OUT_ID != OMNI_LONG) {
            LogError("Error in count all aggregator: Expecting long output type. Got %s",
                TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        }
        // OUT_ID == OMNI_LONG
        if (!inRaw && IN_ID != OMNI_LONG) {
            LogError("Error in count all aggregator: Expecting long intput type for partial input. Got %s",
                TypeUtil::TypeToStringLog(IN_ID).c_str());
            return nullptr;
        } else {
            if (!TypedAggregator::CheckTypes("count all", inputTypes, outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }
            return std::unique_ptr<CountAllAggregator<IN_ID, OUT_ID>>(
                new CountAllAggregator<IN_ID, OUT_ID>(outputTypes, channels, inRaw, outPartial, isOverflowAsNull));
        }
    }

protected:
    CountAllAggregator(const DataTypes &outputTypes, std::vector<int32_t> &channels, const bool inputRaw,
        const bool outputPartial, const bool isOverflowAsNull)
        : CountColumnAggregator<IN_ID, OUT_ID>(OMNI_AGGREGATION_TYPE_COUNT_ALL, outputTypes, channels, inputRaw,
        outputPartial, isOverflowAsNull)
    {}

    virtual ALWAYS_INLINE Vector *GetVector(VectorBatch *vectorBatch, const int32_t rowOffset, const int32_t rowCount,
        uint8_t **nullMap, AggregatorBuffer<int32_t> &indexMap, const size_t channelIdx) override
    {
        if (CountColumnAggregator<IN_ID, OUT_ID>::inputRaw) {
            *nullMap = nullptr;
            indexMap.Release();
            return nullptr;
        } else {
            return TypedAggregator::GetVector(vectorBatch, rowOffset, rowCount, nullMap, indexMap, channelIdx);
        }
    }
};
}
}
#endif // OMNI_RUNTIME_COUNT_ALL_AGGREGATOR_H
