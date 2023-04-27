/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
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
template <typename T> T GetMin()
{
    if constexpr (std::is_same_v<T, int8_t>) {
        return std::numeric_limits<int8_t>::lowest();
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return std::numeric_limits<int16_t>::lowest();
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return std::numeric_limits<int32_t>::lowest();
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return std::numeric_limits<int64_t>::lowest();
    } else if constexpr (std::is_same_v<T, float>) {
        return std::numeric_limits<float>::lowest();
    } else if constexpr (std::is_same_v<T, double>) {
        return std::numeric_limits<double>::lowest();
    } else if constexpr (std::is_same_v<T, int128>) {
        return std::numeric_limits<int128>::lowest();
    } else if constexpr (std::is_same_v<T, omniruntime::type::Decimal128>) {
        return Decimal128(type::DECIMAL128_MIN_VALUE);
    } else {
        throw OmniException("LogicalError", "Unsupoorted data type");
    }
}

template <typename IN, typename OUT>
SIMD_ALWAYS_INLINE void MaxOp(OUT *res, int64_t &flag, const IN &in, const int64_t &notUsed)
{
    const OUT cur = static_cast<OUT>(in);
    if (*res < cur) {
        *res = cur;
    }
    flag |= 1;
}

template <typename IN, typename OUT, bool addIf>
SIMD_ALWAYS_INLINE void MaxConditionalOp(OUT *res, int64_t &flag, const IN &in, const int64_t &notUsed,
    const uint8_t &condition)
{
    if (condition == addIf) {
        const OUT cur = static_cast<OUT>(in);
        if (*res < cur) {
            *res = cur;
        }
        flag |= 1;
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> class MaxAggregator : public TypedAggregator {
    using InVector = typename AggNativeAndVectorType<IN_ID>::vector;
    using InType = typename AggNativeAndVectorType<IN_ID>::type;
    using OutVector = typename AggNativeAndVectorType<OUT_ID>::vector;
    using OutType = typename AggNativeAndVectorType<OUT_ID>::type;
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

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;

    void InitState(AggregateState &state) override;

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull)
    {
        if constexpr (IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CHAR) {
            return MaxVarcharAggregator<IN_ID, OUT_ID>::Create(inputTypes, outputTypes, channels, rawIn, partialOut,
                isOverflowAsNull);
        } else if constexpr (!(IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG || IN_ID == OMNI_DOUBLE ||
            IN_ID == OMNI_DECIMAL128 || IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_BOOLEAN)) {
            LogError("Error in max aggregator: Unsupported input type %s", TypeUtil::TypeToStringLog(IN_ID).c_str());
            return nullptr;
        } else if constexpr (!(OUT_ID == OMNI_SHORT || OUT_ID == OMNI_INT || OUT_ID == OMNI_LONG ||
            OUT_ID == OMNI_DOUBLE || OUT_ID == OMNI_DECIMAL128 || OUT_ID == OMNI_DECIMAL64 || OUT_ID == OMNI_BOOLEAN)) {
            LogError("Error in max aggregator: Unsupported output type %s", TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        } else {
            if (!TypedAggregator::CheckTypes("max", inputTypes, outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }

            return std::unique_ptr<MaxAggregator<IN_ID, OUT_ID>>(new MaxAggregator<IN_ID, OUT_ID>(inputTypes,
                outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
        }
    }

protected:
    MaxAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull);

    void ProcessSingleInternal(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessSingleInternalFilter(AggregateState &state, BaseVector *vector, Vector<bool> *booleanVector,
        const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessGroupInternalFilter(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *vector,
        Vector<bool> *booleanVector, const int32_t rowOffset, const uint8_t *nullMap,
        const int32_t *indexMap) override;
};
}
}
#endif // OMNI_RUNTIME_MAX_AGGREGATOR_H
