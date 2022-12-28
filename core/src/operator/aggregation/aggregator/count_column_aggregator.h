/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Count aggregate
 */
#ifndef OMNI_RUNTIME_COUNT_COLUMN_AGGREGATOR_H
#define OMNI_RUNTIME_COUNT_COLUMN_AGGREGATOR_H

#include "typed_aggregator.h"
#include "operator/aggregation/definitions.h"

namespace omniruntime {
namespace op {
SIMD_ALWAYS_INLINE
void countAllOp(int64_t *res, int64_t &noUsed1, const int64_t &in, const int64_t &noUsed2)
{
    *res += in;
}

template<bool addIf>
SIMD_ALWAYS_INLINE
void countAllConditionalOp(
    int64_t *res, int64_t &noUsed1, const int64_t &in, const int64_t &noUsed2, const uint8_t &condition)
{
    const int64_t mask = (!condition == addIf) - 1;
    *res += (in & mask);
}

template<bool addIf>
VECTORIZE_LOOP NO_INLINE
void addConditionalCountRaw(int64_t &res, const size_t rowCount, const uint8_t * __restrict condition)
{
    if (rowCount > 0) {
#ifdef DEBUG
        if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
            LogWarn("[addConditionalCountRaw]: ConditionMap pointer NOT aligned");
        }
#endif

        condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);

        for (size_t i = 0; i < rowCount; ++i) {
            res += (condition[i] == addIf);
        }
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
class CountColumnAggregator : public TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW> {
public:
    ~CountColumnAggregator() override = default;

    void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override;

    static std::unique_ptr<Aggregator> Create(
        const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
    {
        if constexpr (OUT_ID != OMNI_LONG) {
            LogError("Error in count column aggregator: Expecting long output type. Got %s",
                TypeUtil::TypeToString(OUT_ID).c_str());
            return nullptr;
        } else if constexpr (!RAW_IN && IN_ID != OMNI_LONG) {
            LogError("Error in count column aggregator: Expecting long intput type for partial input. Got %s",
                TypeUtil::TypeToString(IN_ID).c_str());
            return nullptr;
        } else {
            if (!TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>::CheckTypes(
                "count column", inputTypes, outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }

            return std::unique_ptr<CountColumnAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>>(
                new CountColumnAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>(outputTypes, channels));
        }
    }

protected:
    CountColumnAggregator(const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            OMNI_AGGREGATION_TYPE_COUNT_COLUMN, *DataTypes::NoneDataTypesInstance(), outputTypes, channels)
    {}

    CountColumnAggregator(FunctionType aggregateType, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            aggregateType, *DataTypes::NoneDataTypesInstance(), outputTypes, channels)
    {}

    void ProcessSingleInternal(
        AggregateState &state, Vector *vector, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessGroupInternal(
        std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) override;
};
}
}
#endif // OMNI_RUNTIME_COUNT_COLUMN_AGGREGATOR_H
