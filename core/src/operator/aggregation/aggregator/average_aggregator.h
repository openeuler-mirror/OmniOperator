/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * Description: Average aggregate
 */
#ifndef OMNI_RUNTIME_AVERAGE_AGGREGATOR_H
#define OMNI_RUNTIME_AVERAGE_AGGREGATOR_H

#include "sum_aggregator.h"

namespace omniruntime {
namespace op {
template <typename IN, typename MID, bool addIf>
VECTORIZE_LOOP FAST_MATH NO_INLINE void AvgConditionalFloat(MID *res, int64_t &flag, const IN *__restrict ptr,
    const int64_t *__restrict cntPtr, const size_t rowCount, const NullsHelper &condition)
{
    static_assert(std::is_floating_point_v<IN>, "Not floating point input passed to AvgConditionalFloat");
#ifdef DEBUG
    if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
        LogWarn("[avgConditionalFloat] Data pointer NOT aligned");
    }
    if (reinterpret_cast<unsigned long>(cntPtr) % ARRAY_ALIGNMENT != 0) {
        LogWarn("[avgConditionalFloat]: Counter pointer NOT aligned");
    }
#endif
    ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
    cntPtr = (const int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);

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

template <DataTypeId IN_ID, DataTypeId OUT_ID> class AverageAggregator : public SumAggregator<IN_ID, OUT_ID> {
    using InVector = typename AggNativeAndVectorType<IN_ID>::vector;
    using InType = typename AggNativeAndVectorType<IN_ID>::type;
    using OutVector = typename AggNativeAndVectorType<OUT_ID>::vector;
    using OutType = typename AggNativeAndVectorType<OUT_ID>::type;
    using ResultType = typename std::conditional_t<IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG,
        int64_t, std::conditional_t<IN_ID == OMNI_DOUBLE || IN_ID == OMNI_CONTAINER, double, Decimal128>>;

public:
    ~AverageAggregator() override = default;

    // we use sum state as avg state
    using AvgState = typename SumAggregator<IN_ID, OUT_ID>::SumState;

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override;
    std::vector<DataTypePtr> GetSpillType() override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;
    template <bool PARTIAL_OUT, bool DECIMAL_PRECISION_IMPROVEMENT>
    void ExtractValuesFunction(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex);

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inRaw, bool outPartial, bool isOverflowAsNull)
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
        }
        if ((inRaw && (IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CONTAINER)) ||
            (!inRaw && (IN_ID != OMNI_VARCHAR && IN_ID != OMNI_CONTAINER))) {
            LogError("Error in average aggregator: Invalid input type %s for inputRaw=%s",
                TypeUtil::TypeToStringLog(IN_ID).c_str(), (inRaw ? "true" : "false"));
            return nullptr;
        } else if ((!outPartial && (OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CONTAINER)) ||
            (outPartial && (OUT_ID != OMNI_VARCHAR && OUT_ID != OMNI_CONTAINER))) {
            LogError("Error in average aggregator: Invalid output type %s for outputPartial=%s",
                TypeUtil::TypeToStringLog(OUT_ID).c_str(), (outPartial ? "true" : "false"));
            return nullptr;
        }
        if constexpr (IN_ID == OMNI_VARCHAR && OUT_ID == OMNI_CONTAINER) {
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
            if (!SumAggregator<IN_ID, OUT_ID>::CheckTypes("average", inputTypes, outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }
            return std::unique_ptr<AverageAggregator<IN_ID, OUT_ID>>(new AverageAggregator<IN_ID, OUT_ID>(inputTypes,
                outputTypes, channels, inRaw, outPartial, isOverflowAsNull));
        }
    }

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override;

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

protected:
    AverageAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull);

    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
        const std::shared_ptr<NullsHelper> nullMap) override;

    template <typename T>
    void ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap);

private:
    void (AverageAggregator<IN_ID, OUT_ID>::*extractValuesFuncPointer)(const AggregateState *state,
        std::vector<BaseVector *> &vectors, int32_t rowIndex) = nullptr;

    template <bool PARTIAL_OUT, bool DECIMAL_PRECISION_IMPROVEMENT, bool IS_OVERFLOW_AS_NULL>
    void ExtractValuesBatchInternal(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount);

    template <bool DECIMAL_PRECISION_IMPROVEMENT>
    void DivideWithOverflow(const AvgState *state, OutType &result, bool &overflow);
};
}
}
#endif // OMNI_RUNTIME_AVERAGE_AGGREGATOR_H
