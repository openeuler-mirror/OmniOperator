/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Sum aggregator
 */
#ifndef OMNI_RUNTIME_SUM_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_AGGREGATOR_H

#include "typed_aggregator.h"

namespace omniruntime {
namespace op {
template <typename IN, typename MID>
SIMD_ALWAYS_INLINE void SumOp(MID *res, int64_t &flag, const IN &in, const int64_t &cnt)
{
    if constexpr (std::is_same_v<MID, Decimal128>) {
        if (flag >= 0) {
            Decimal128Wrapper result;

            if constexpr (std::is_same_v<IN, DecimalPartialResult>) {
                result = Decimal128Wrapper(*res).Add(in.sum);
                flag += in.count;
            } else {
                result = Decimal128Wrapper(*res).Add(in);
                flag += cnt;
            }

            *res = result.ToDecimal128();
            if (result.IsOverflow() != OpStatus::SUCCESS) {
                flag = -1;
            }
        }
    } else if constexpr (std::is_same_v<IN, int64_t>) {
        if (flag >= 0) {
            if (__builtin_add_overflow(*res, in, res)) {
                flag = -1;
            } else {
                flag += cnt;
            }
        }
    } else {
        const MID v = in;
        *res += v;
        flag += cnt;
    }
}

template <typename IN, typename MID, bool addIf>
SIMD_ALWAYS_INLINE void SumConditionalOp(MID *res, int64_t &flag, const IN &in, const int64_t &cnt,
    const uint8_t &condition)
{
    if constexpr (std::is_same_v<MID, Decimal128> || std::is_same_v<IN, int64_t> || std::is_floating_point_v<IN>) {
        if (condition == addIf) {
            SumOp<IN, MID>(res, flag, in, cnt);
        }
    } else {
        const IN mask = (!condition == addIf) - 1;
        *res += (in & mask);
        const int64_t cntMask = (!condition == addIf) - 1;
        flag += (cnt & cntMask);
    }
}

template <typename IN, typename MID, bool addIf>
FAST_MATH NO_INLINE void SumConditionalFloat(MID *res, int64_t &flag, const IN *__restrict ptr, const int32_t rowCount,
    const uint8_t *__restrict condition)
{
    static_assert(std::is_floating_point_v<IN>, "Not floating point input passed to SumConditionalFloat");
#ifdef DEBUG
    if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
        LogWarn("[sumConditionalFloat] Data pointer NOT aligned");
    }
    if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
        LogWarn("[sumConditionalFloat] ConditionMap pointer NOT aligned");
    }
#endif

//    ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
//    condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);

    const auto *endPtr = ptr + rowCount;

    using equivalent_integer = std::conditional_t<sizeof(IN) == 4, uint32_t, uint64_t>;
    const auto len = sizeof(IN);

    while (ptr < endPtr) {
        equivalent_integer iValue;
        // Note: using memcpy_s hugely degrades performance
        std::copy(reinterpret_cast<const uint8_t *>(ptr), reinterpret_cast<const uint8_t *>(ptr) + len,
            reinterpret_cast<uint8_t *>(&iValue));
        iValue &= (!*condition == addIf) - 1;
        IN fValue;
        std::copy(reinterpret_cast<const uint8_t *>(&iValue), reinterpret_cast<const uint8_t *>(&iValue) + len,
            reinterpret_cast<uint8_t *>(&fValue));
        *res += fValue;

        flag += *condition == addIf;

        ++ptr;
        ++condition;
    }
}

template <typename IN, typename MID, bool addIf>
FAST_MATH NO_INLINE void SumConditionalFloatFilter(MID *res, int64_t &flag, const IN *__restrict ptr,
    const int32_t rowCount, const uint8_t *__restrict condition, const int8_t *__restrict boolPtr)
{
    static_assert(std::is_floating_point_v<IN>, "Not floating point input passed to SumConditionalFloat");
#ifdef DEBUG
    if (reinterpret_cast<unsigned long>(ptr) % ARRAY_ALIGNMENT != 0) {
        LogWarn("[sumConditionalFloat] Data pointer NOT aligned");
    }
    if (reinterpret_cast<unsigned long>(condition) % ARRAY_ALIGNMENT != 0) {
        LogWarn("[sumConditionalFloat] ConditionMap pointer NOT aligned");
    }
#endif

    ptr = (const IN *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
    condition = (const uint8_t *)__builtin_assume_aligned(condition, ARRAY_ALIGNMENT);
    boolPtr = (const int8_t *)__builtin_assume_aligned(boolPtr, ARRAY_ALIGNMENT);

    const auto *endPtr = ptr + rowCount;

    using equivalent_integer = std::conditional_t<sizeof(IN) == 4, uint32_t, uint64_t>;
    const auto len = sizeof(IN);

    while (ptr < endPtr) {
        if (boolPtr) {
            equivalent_integer iValue;
            // Note: using memcpy_s hugely degrades performance
            std::copy(reinterpret_cast<const uint8_t *>(ptr), reinterpret_cast<const uint8_t *>(ptr) + len,
                reinterpret_cast<uint8_t *>(&iValue));
            iValue &= (!*condition == addIf) - 1;
            IN fValue;
            std::copy(reinterpret_cast<const uint8_t *>(&iValue), reinterpret_cast<const uint8_t *>(&iValue) + len,
                reinterpret_cast<uint8_t *>(&fValue));
            *res += fValue;

            flag += *condition == addIf;
        }

        ++boolPtr;
        ++ptr;
        ++condition;
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> class SumAggregator : public TypedAggregator {
    using InVector = typename AggNativeAndVectorType<IN_ID>::vector;
    using InType = typename AggNativeAndVectorType<IN_ID>::type;
    using OutVector = typename AggNativeAndVectorType<OUT_ID>::vector;
    using OutType = typename AggNativeAndVectorType<OUT_ID>::type;
    using ResultType = typename std::conditional_t<IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG,
        int64_t, std::conditional_t<IN_ID == OMNI_DOUBLE || IN_ID == OMNI_CONTAINER, double, Decimal128>>;

public:
    ~SumAggregator() override = default;

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override;

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override;
#endif

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;

    void InitState(AggregateState &state) override;

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull)
    {
        if constexpr (!(IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG || IN_ID == OMNI_DOUBLE ||
            IN_ID == OMNI_DECIMAL128 || IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CONTAINER)) {
            LogError("Error in sum aggregator: Unsupported input type %s", TypeUtil::TypeToStringLog(IN_ID).c_str());
            return nullptr;
        } else if constexpr (!(OUT_ID == OMNI_SHORT || OUT_ID == OMNI_INT || OUT_ID == OMNI_LONG ||
            OUT_ID == OMNI_DOUBLE || OUT_ID == OMNI_DECIMAL128 || OUT_ID == OMNI_DECIMAL64 || OUT_ID == OMNI_VARCHAR ||
            OUT_ID == OMNI_CONTAINER)) {
            LogError("Error in sum aggregator: Unsupported output type %s", TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        }
        if (rawIn && IN_ID == OMNI_VARCHAR) {
            LogError("Error in sum aggregator: Invalid input type %s for inputRaw=%s",
                TypeUtil::TypeToStringLog(IN_ID).c_str(), (rawIn ? "true" : "false"));
            return nullptr;
        } else if (!partialOut && OUT_ID == OMNI_VARCHAR) {
            LogError("Error in sum aggregator: Invalid output type %s for outputPartial=%s",
                TypeUtil::TypeToStringLog(OUT_ID).c_str(), (partialOut ? "true" : "false"));
            return nullptr;
        }
        if constexpr (OUT_ID == OMNI_VARCHAR &&
            (IN_ID != OMNI_VARCHAR && IN_ID != OMNI_DECIMAL64 && IN_ID != OMNI_DECIMAL128)) {
            LogError("Error in sum aggregator: Invalid input type %s for partial output with varchar type",
                TypeUtil::TypeToStringLog(IN_ID).c_str());
            return nullptr;
        } else {
            if (!SumAggregator<IN_ID, OUT_ID>::CheckTypes("sum", inputTypes, outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }
            return std::unique_ptr<SumAggregator<IN_ID, OUT_ID>>(new SumAggregator<IN_ID, OUT_ID>(inputTypes,
                outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
        }
    }

protected:
    SumAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull);

    SumAggregator(FunctionType aggregateType, const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull);

    void ProcessSingleInternal(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessSingleInternalFilter(AggregateState &state, BaseVector *vector, BooleanVector *booleanVector,
        const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) override;

    void ProcessGroupInternalFilter(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *v,
        BooleanVector *booleanVector, const int32_t rowOffset, const uint8_t *nullMap,
        const int32_t *indexMap) override;

    static bool CheckTypes(const std::string &aggName, const DataTypes &inputTypes, const DataTypes &outputTypes,
        const DataTypeId inId, const DataTypeId outId)
    {
        if (!TypedAggregator::CheckTypes(aggName, inputTypes, outputTypes, inId, outId)) {
            return false;
        }

        if constexpr (IN_ID == OMNI_VARCHAR) {
            static_cast<VarcharDataType *>(inputTypes.GetType(0).get())->SetWidth(sizeof(DecimalPartialResult));
        }
        if constexpr (OUT_ID == OMNI_VARCHAR) {
            static_cast<VarcharDataType *>(outputTypes.GetType(0).get())->SetWidth(sizeof(DecimalPartialResult));
        }

        return true;
    }
};
}
}
#endif // OMNI_RUNTIME_SUM_AGGREGATOR_H
