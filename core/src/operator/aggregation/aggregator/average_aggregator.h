/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: Average aggregate
 */
#ifndef OMNI_RUNTIME_AVERAGE_AGGREGATOR_H
#define OMNI_RUNTIME_AVERAGE_AGGREGATOR_H

#include "sum_aggregator.h"

namespace omniruntime {
namespace op {
template <typename IN, typename MID, bool addIf>
VECTORIZE_LOOP FAST_MATH NO_INLINE void AvgConditionalFloat(MID *res, int64_t &flag, const IN *__restrict ptr,
    const int64_t *__restrict cntPtr, const size_t rowCount, const uint8_t *__restrict condition)
{
    static_assert(std::is_floating_point_v<IN>, "Not floating point input passed to AvgConditionalFloat");
#ifdef DEBUG
    if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
        LogWarn("[avgConditionalFloat] Data pointer NOT aligned");
    }
    if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
        LogWarn("[avgConditionalFloat]: Counter pointer NOT aligned");
    }
    if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
        LogWarn("[avgConditionalFloat] ConditionMap pointer NOT aligned");
    }
#endif

    ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
    cntPtr = (const int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);
    condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);

    const auto len = sizeof(IN);

    for (size_t i = 0; i < rowCount; i++) {
        const int64_t mask = (!condition[i] == addIf) - 1;

        int64_t iValue;
        // Note: using memcpy_s hugely degrades performance
        std::copy(reinterpret_cast<const uint8_t *>(&ptr[i]), reinterpret_cast<const uint8_t *>(&ptr[i]) + len,
            reinterpret_cast<uint8_t *>(&iValue));
        iValue &= mask;
        IN fValue;
        std::copy(reinterpret_cast<const uint8_t *>(&iValue), reinterpret_cast<const uint8_t *>(&iValue) + len,
            reinterpret_cast<uint8_t *>(&fValue));
        *res += fValue;

        flag += (cntPtr[i] & mask);
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
class AverageAggregator : public SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID> {
    using InVector = typename NativeAndVectorType<IN_ID>::vector;
    using InType = typename NativeAndVectorType<IN_ID>::type;
    using OutVector = typename NativeAndVectorType<OUT_ID>::vector;
    using OutType = typename NativeAndVectorType<OUT_ID>::type;
    using ResultType = typename std::conditional_t<IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG,
        int64_t, std::conditional_t<IN_ID == OMNI_DOUBLE || IN_ID == OMNI_CONTAINER, double, Decimal128>>;

public:
    ~AverageAggregator() override = default;

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override;

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override;
#endif

    void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override;

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels)
    {
        if constexpr (!(IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG || IN_ID == OMNI_DOUBLE ||
            IN_ID == OMNI_DECIMAL128 || IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CONTAINER)) {
            LogError("Error in average aggregator: Unsupported input type %s",
                TypeUtil::TypeToStringLog(IN_ID).c_str());
            return nullptr;
        } else if constexpr (!(OUT_ID == OMNI_DOUBLE || OUT_ID == OMNI_DECIMAL128 || OUT_ID == OMNI_DECIMAL64 ||
            OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CONTAINER)) {
            LogError("Error in average aggregator: Unsupported output type %s",
                TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        } else if constexpr ((RAW_IN && (IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CONTAINER)) ||
            (!RAW_IN && (IN_ID != OMNI_VARCHAR && IN_ID != OMNI_CONTAINER))) {
            LogError("Error in average aggregator: Invalid input type %s for inputRaw=%s",
                TypeUtil::TypeToStringLog(IN_ID).c_str(), (RAW_IN ? "true" : "false"));
            return nullptr;
        } else if constexpr ((!PARTIAL_OUT && (OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CONTAINER)) ||
            (PARTIAL_OUT && (OUT_ID != OMNI_VARCHAR && OUT_ID != OMNI_CONTAINER))) {
            LogError("Error in average aggregator: Invalid output type %s for outputPartial=%s",
                TypeUtil::TypeToStringLog(OUT_ID).c_str(), (PARTIAL_OUT ? "true" : "false"));
            return nullptr;
        } else if constexpr (IN_ID == OMNI_VARCHAR && OUT_ID == OMNI_CONTAINER) {
            LogError("Error in average aggregator: Invalid output type %s for partial input with varchar type",
                TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        } else if constexpr (IN_ID == OMNI_CONTAINER && OUT_ID == OMNI_VARCHAR) {
            LogError("Error in average aggregator: Invalid output type %s for partial input with container type",
                TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        } else if constexpr (OUT_ID == OMNI_VARCHAR &&
            (IN_ID != OMNI_VARCHAR && IN_ID != OMNI_DECIMAL64 && IN_ID != OMNI_DECIMAL128)) {
            LogError("Error in average aggregator: Invalid input type %s for partial output with varchar type",
                TypeUtil::TypeToStringLog(IN_ID).c_str());
            return nullptr;
        } else if constexpr (OUT_ID == OMNI_CONTAINER &&
            (IN_ID == OMNI_VARCHAR || IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_DECIMAL128)) {
            LogError("Error in average aggregator: Invalid input type %s for partial output with container type",
                TypeUtil::TypeToStringLog(IN_ID).c_str());
            return nullptr;
        } else {
            if (!SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::CheckTypes("average", inputTypes,
                outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }

            return std::unique_ptr<AverageAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>>(
                new AverageAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>(inputTypes, outputTypes,
                channels));
        }
    }

protected:
    AverageAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>(OMNI_AGGREGATION_TYPE_AVG, inputTypes,
        outputTypes, channels)
    {}

    void ProcessSingleInternal(AggregateState &state, Vector *vector, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) override;
};
}
}
#endif // OMNI_RUNTIME_AVERAGE_AGGREGATOR_H
