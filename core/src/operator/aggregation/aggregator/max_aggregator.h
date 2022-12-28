/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Max aggregate
 */
#ifndef OMNI_RUNTIME_MAX_AGGREGATOR_H
#define OMNI_RUNTIME_MAX_AGGREGATOR_H

#include <cstdint>
#include <cfloat>

#include "typed_aggregator.h"
#include "max_varchar_aggregator.h"
#ifdef ENABLE_HMPP
#include "aggregator_util.h"
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
template<typename T>
T getMin()
{
    if constexpr (std::is_same_v<T, int8_t>) {
        return 0x81;
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return 0x8001;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return 0x80000001;
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return 0x8000000000000001;
    } else if constexpr (std::is_same_v<T, float>) {
        return -FLT_MAX;
    } else if constexpr (std::is_same_v<T, double>) {
        return -DBL_MAX;
    } else if constexpr (std::is_same_v<T, Int128>) {
        return std::numeric_limits<Int128>::min();
    } else if constexpr (std::is_same_v<T, omniruntime::type::Decimal128>) {
        return omniruntime::type::Decimal128(0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF);
    } else {
        throw OmniException("LogicalError", "Unsupoorted data type");
    }
}

template<typename IN, typename OUT>
SIMD_ALWAYS_INLINE
void maxOp(OUT *res, int64_t &flag, const IN &in, const int64_t &notUsed)
{
    const OUT cur = static_cast<OUT>(in);
    if (*res < cur) {
        *res = cur;
    }
    flag |= 1;
}

template<typename IN, typename OUT, bool addIf>
SIMD_ALWAYS_INLINE
void maxConditionalOp(OUT *res, int64_t &flag, const IN &in, const int64_t &notUsed, const uint8_t &condition)
{
    if (condition == addIf) {
        const OUT cur = static_cast<OUT>(in);
        if (*res < cur) {
            *res = cur;
        }
        flag |= 1;
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
class MaxAggregator : public TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW> {
    using InVector = typename NativeAndVectorType<IN_ID>::vector;
    using InType = typename NativeAndVectorType<IN_ID>::type;
    using OutVector = typename NativeAndVectorType<OUT_ID>::vector;
    using OutType = typename NativeAndVectorType<OUT_ID>::type;
    // note: for some reason when g++ vectorizes min operation loop, it cannot correctly
    // evaluate min value for int16_t, so for that type intermediate result is promoted to int32
    // once we figure out how to resolve this issue in g++m we can set ResultType = InType
    using ResultType = std::conditional_t<IN_ID == OMNI_SHORT, int32_t, InType>;
public:
    ~MaxAggregator() override = default;

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override;

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override;
#endif

    void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override;

    void InitState(AggregateState &state) override;

    static std::unique_ptr<Aggregator> Create(
        const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
    {
        if constexpr (IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CHAR) {
            return MaxVarcharAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::Create(
                inputTypes, outputTypes, channels);
        } else if constexpr (!(IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG || IN_ID == OMNI_DOUBLE
            || IN_ID == OMNI_DECIMAL128 || IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_BOOLEAN)) {
            LogError("Error in max aggregator: Unsupported input type %s", TypeUtil::TypeToString(IN_ID).c_str());
            return nullptr;
        } else if constexpr (!(OUT_ID == OMNI_SHORT || OUT_ID == OMNI_INT || OUT_ID == OMNI_LONG || OUT_ID == OMNI_DOUBLE
            || OUT_ID == OMNI_DECIMAL128 || OUT_ID == OMNI_DECIMAL64 || OUT_ID == OMNI_BOOLEAN)) {
            LogError("Error in max aggregator: Unsupported output type %s", TypeUtil::TypeToString(OUT_ID).c_str());
            return nullptr;
        } else {
            if (!TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>::CheckTypes(
                "max", inputTypes, outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }

            return std::unique_ptr<MaxAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>>(
                new MaxAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>(inputTypes, outputTypes, channels));
        }
    }

protected:
    MaxAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            OMNI_AGGREGATION_TYPE_MAX, inputTypes, outputTypes, channels)
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
#endif // OMNI_RUNTIME_MAX_AGGREGATOR_H
