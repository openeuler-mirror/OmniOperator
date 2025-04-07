/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Min aggregate
 */
#ifndef OMNI_RUNTIME_MIN_AGGREGATOR_H
#define OMNI_RUNTIME_MIN_AGGREGATOR_H

#include <cstdint>
#include <cfloat>

#include "typed_aggregator.h"
#include "min_varchar_aggregator.h"

namespace omniruntime {
namespace op {
template <typename T> T GetMax()
{
    if constexpr (std::is_same_v<T, int8_t>) {
        return std::numeric_limits<int8_t>::max();
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return std::numeric_limits<int16_t>::max();
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return std::numeric_limits<int32_t>::max();
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return std::numeric_limits<int64_t>::max();
    } else if constexpr (std::is_same_v<T, float>) {
        return std::numeric_limits<float>::max();
    } else if constexpr (std::is_same_v<T, double>) {
        return std::numeric_limits<double>::max();
    } else if constexpr (std::is_same_v<T, int128_t>) {
        return std::numeric_limits<int128_t>::max();
    } else if constexpr (std::is_same_v<T, omniruntime::type::Decimal128>) {
        return Decimal128(type::DECIMAL128_MAX_VALUE);
    } else {
        throw OmniException("LogicalError", "Unsupoorted data type");
    }
}

template <typename IN, typename OUT>
SIMD_ALWAYS_INLINE void MinOp(OUT *res, AggValueState &flag, const IN &in, const int64_t notUsed)
{
    const OUT cur = static_cast<OUT>(in);
    *res = std::min(*res, cur);
    flag = AggValueState::NORMAL;
}

template <typename IN, typename OUT, bool addIf>
SIMD_ALWAYS_INLINE void MinConditionalOp(OUT *res, AggValueState &flag, const IN &in, const int64_t notUsed,
    const uint8_t &condition)
{
    if (condition == addIf) {
        const OUT cur = static_cast<OUT>(in);
        *res = std::min(*res, cur);
        flag = AggValueState::NORMAL;
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> class MinAggregator : public TypedAggregator {
    using InType = typename AggNativeAndVectorType<IN_ID>::type;
    using OutVector = typename AggNativeAndVectorType<OUT_ID>::vector;
    using OutType = typename AggNativeAndVectorType<OUT_ID>::type;
    // note: for some reason when g++ vectorizes min operation loop, it cannot correctly
    // evaluate min value for int16_t, so for that type intermediate result is promoted to int32
    // once we figure out how to resolve this issue in g++m we can set ResultType = InType
    using ResultType = std::conditional_t<IN_ID == OMNI_SHORT, int32_t, InType>;

    // inner class for aggregate state, the member depends on ResultType of Aggregator
#pragma pack(push, 1)
    struct MinState : BaseState<ResultType> {
        static const MinAggregator<IN_ID, OUT_ID>::MinState *ConstCastState(const AggregateState *state)
        {
            return reinterpret_cast<const MinAggregator<IN_ID, OUT_ID>::MinState *>(state);
        }

        static MinAggregator<IN_ID, OUT_ID>::MinState *CastState(AggregateState *state)
        {
            return reinterpret_cast<MinAggregator<IN_ID, OUT_ID>::MinState *>(state);
        }

        template <typename TypeIn, typename TypeOut> static void UpdateState(AggregateState *state, const TypeIn &in)
        {
            auto *maxState = CastState(state);
            MinOp<TypeIn, TypeOut>(&(maxState->value), maxState->valueState, in, 1ULL);
        }

        template <typename TypeIn, typename TypeOut, bool addIf>
        static void UpdateStateWithCondition(AggregateState *state, const TypeIn &in, const uint8_t &condition)
        {
            if (condition == addIf) {
                UpdateState<TypeIn, TypeOut>(state, in);
            }
        }
    };
#pragma pack(pop)

public:
    ~MinAggregator() override = default;

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;
    void InitState(AggregateState *state) override;
    void InitStates(std::vector<AggregateState *> &groupStates) override;
    std::vector<DataTypePtr> GetSpillType() override;

    size_t GetStateSize() override
    {
        return sizeof(MinState);
    }

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull)
    {
        if constexpr (IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CHAR) {
            return MinVarcharAggregator<IN_ID, OUT_ID>::Create(inputTypes, outputTypes, channels, rawIn, partialOut,
                isOverflowAsNull);
        } else if constexpr (!(IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG || IN_ID == OMNI_DOUBLE ||
            IN_ID == OMNI_DECIMAL128 || IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_BOOLEAN)) {
            LogError("Error in min aggregator: Unsupported input type %s", TypeUtil::TypeToStringLog(IN_ID).c_str());
            return nullptr;
        } else if constexpr (!(OUT_ID == OMNI_SHORT || OUT_ID == OMNI_INT || OUT_ID == OMNI_LONG ||
            OUT_ID == OMNI_DOUBLE || OUT_ID == OMNI_DECIMAL128 || OUT_ID == OMNI_DECIMAL64 || OUT_ID == OMNI_BOOLEAN)) {
            LogError("Error in min aggregator: Unsupported output type %s", TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        } else {
            if (!TypedAggregator::CheckTypes("min", inputTypes, outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }

            return std::unique_ptr<MinAggregator<IN_ID, OUT_ID>>(new MinAggregator<IN_ID, OUT_ID>(inputTypes,
                outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
        }
    }

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override;

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

protected:
    MinAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull);

    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
                              const std::shared_ptr<NullsHelper> nullMap) override;

    template <typename T>
    void ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap);
};
}
}
#endif // OMNI_RUNTIME_MIN_AGGREGATOR_H
