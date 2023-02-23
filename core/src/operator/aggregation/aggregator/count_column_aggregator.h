/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Count aggregate
 */
#ifndef OMNI_RUNTIME_COUNT_COLUMN_AGGREGATOR_H
#define OMNI_RUNTIME_COUNT_COLUMN_AGGREGATOR_H

#include "typed_aggregator.h"
#include "operator/aggregation/definitions.h"

namespace omniruntime {
namespace op {
SIMD_ALWAYS_INLINE
void CountAllOp(int64_t *res, int64_t &noUsed1, const int64_t &in, const int64_t &noUsed2)
{
    *res += in;
}

template <bool addIf>
SIMD_ALWAYS_INLINE void CountAllConditionalOp(int64_t *res, int64_t &noUsed1, const int64_t &in, const int64_t &noUsed2,
    const uint8_t &condition)
{
    const int64_t mask = (!condition == addIf) - 1;
    *res += (in & mask);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> class CountColumnAggregator : public TypedAggregator {
public:
    ~CountColumnAggregator() override = default;

    void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override;

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inRaw, bool outPartial, bool isOverflowAsNull)
    {
        if constexpr (OUT_ID != OMNI_LONG) {
            LogError("Error in count column aggregator: Expecting long output type. Got %s",
                TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        }
        // OUT_ID == OMNI_LONG
        if (!inRaw && IN_ID != OMNI_LONG) {
            LogError("Error in count column aggregator: Expecting long intput type for partial input. Got %s",
                TypeUtil::TypeToStringLog(IN_ID).c_str());
            return nullptr;
        } else {
            if (!TypedAggregator::CheckTypes("count column", inputTypes, outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }

            return std::unique_ptr<CountColumnAggregator<IN_ID, OUT_ID>>(
                new CountColumnAggregator<IN_ID, OUT_ID>(outputTypes, channels, inRaw, outPartial, isOverflowAsNull));
        }
    }

protected:
    CountColumnAggregator(const DataTypes &outputTypes, std::vector<int32_t> &channels, const bool inputRaw,
        const bool outputPartial, const bool isOverflowAsNull)
        : TypedAggregator(OMNI_AGGREGATION_TYPE_COUNT_COLUMN, *DataTypes::NoneDataTypesInstance(), outputTypes,
        channels, inputRaw, outputPartial, isOverflowAsNull)
    {
        if (inputRaw) {
            processSingleInternalFilterPtr =
                &CountColumnAggregator<IN_ID, OUT_ID>::ProcessSingleInternalFilterFunction<true>;
            processGroupInternalFilterPtr =
                &CountColumnAggregator<IN_ID, OUT_ID>::ProcessGroupInternalFilterFunction<true>;
            processSingleInternalPtr = &CountColumnAggregator<IN_ID, OUT_ID>::ProcessSingleInternalFunction<true>;
            processGroupInternalPtr = &CountColumnAggregator<IN_ID, OUT_ID>::ProcessGroupInternalFunction<true>;
        } else {
            processSingleInternalPtr = &CountColumnAggregator<IN_ID, OUT_ID>::ProcessSingleInternalFunction<false>;
            processGroupInternalPtr = &CountColumnAggregator<IN_ID, OUT_ID>::ProcessGroupInternalFunction<false>;
        }
    }

    CountColumnAggregator(FunctionType aggregateType, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
        : TypedAggregator(aggregateType, *DataTypes::NoneDataTypesInstance(), outputTypes, channels, inputRaw,
        outputPartial, isOverflowAsNull)
    {
        if (inputRaw) {
            processSingleInternalFilterPtr =
                &CountColumnAggregator<IN_ID, OUT_ID>::ProcessSingleInternalFilterFunction<true>;
            processGroupInternalFilterPtr =
                &CountColumnAggregator<IN_ID, OUT_ID>::ProcessGroupInternalFilterFunction<true>;
            processSingleInternalPtr = &CountColumnAggregator<IN_ID, OUT_ID>::ProcessSingleInternalFunction<true>;
            processGroupInternalPtr = &CountColumnAggregator<IN_ID, OUT_ID>::ProcessGroupInternalFunction<true>;
        } else {
            processGroupInternalPtr = &CountColumnAggregator<IN_ID, OUT_ID>::ProcessGroupInternalFunction<false>;
            processSingleInternalPtr = &CountColumnAggregator<IN_ID, OUT_ID>::ProcessSingleInternalFunction<false>;
        }
    }

    void ProcessSingleInternal(AggregateState &state, Vector *vector, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessSingleInternalFilter(AggregateState &state, Vector *vector, BooleanVector *booleanVector,
        const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessGroupInternalFilter(std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector,
        BooleanVector *booleanVector, const int32_t rowOffset, const uint8_t *nullMap,
        const int32_t *indexMap) override;

    template <bool RAW_IN>
    void ProcessSingleInternalFunction(AggregateState &state, Vector *vector, const int32_t rowOffset,
        const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap);

    template <bool RAW_IN>
    void ProcessSingleInternalFilterFunction(AggregateState &state, Vector *vector, BooleanVector *booleanVector,
        const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap);

    template <bool RAW_IN>
    void ProcessGroupInternalFunction(std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap);
    template <bool RAW_IN>
    void ProcessGroupInternalFilterFunction(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
        Vector *vector, BooleanVector *booleanVector, const int32_t rowOffset, const uint8_t *nullMap,
        const int32_t *indexMap);

private:
    void (CountColumnAggregator<IN_ID, OUT_ID>::*processGroupInternalPtr)(std::vector<AggregateState *> &rowStates,
        const size_t aggIdx, Vector *vector, const int32_t rowOffset, const uint8_t *nullMap,
        const int32_t *indexMap) = nullptr;

    void (CountColumnAggregator<IN_ID, OUT_ID>::*processGroupInternalFilterPtr)(
        std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector, BooleanVector *booleanVector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) = nullptr;

    void (CountColumnAggregator<IN_ID, OUT_ID>::*processSingleInternalPtr)(AggregateState &state, Vector *vector,
        const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap) = nullptr;

    void (CountColumnAggregator<IN_ID, OUT_ID>::*processSingleInternalFilterPtr)(AggregateState &state, Vector *vector,
        BooleanVector *booleanVector, const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap,
        const int32_t *indexMap) = nullptr;
};
}
}
#endif // OMNI_RUNTIME_COUNT_COLUMN_AGGREGATOR_H
