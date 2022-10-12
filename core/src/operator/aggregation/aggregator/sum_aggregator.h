/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Sum aggregator
 */
#ifndef OMNI_RUNTIME_SUM_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_AGGREGATOR_H

#include "typed_aggregator.h"
#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
template<typename IN, typename MID, typename OUT>
SIMD_ALWAYS_INLINE
void sumOp(MID *res, int64_t &flag, const IN &in, const int64_t &cnt)
{
    if constexpr (std::is_same_v<IN, int64_t>) {
        if (flag >= 0) {
            if (__builtin_add_overflow(*res, in, res)) {
                flag = -1;
            } else {
                flag += cnt;
            }
        }
    } else if constexpr (sizeof(MID) > 8) {
        if (flag >= 0) {
            *res += in;
            const OUT check = static_cast<OUT>(*res);
            if (static_cast<MID>(check) != *res) {
                flag = -1;
            } else {
                flag += cnt;
            }
        }
    } else {
        *res += in;
        flag += cnt;
    }
}

template<typename IN, typename MID, typename OUT, bool addIf>
SIMD_ALWAYS_INLINE
void sumConditionalOp(
    MID *res, int64_t &flag, const IN &in, const int64_t &cnt, const uint8_t &condition)
{
    if constexpr (std::is_same_v<IN, int64_t>) {
        if (condition == addIf && flag >= 0) {
            if (__builtin_add_overflow(*res, in, res)) {
                flag = -1;
            } else {
                flag += cnt;
            }
        }
    } else if constexpr (sizeof(MID) > 8) {
        if (condition == addIf && flag >= 0) {
            *res += in;
            const OUT check = static_cast<OUT>(*res);
            if (static_cast<MID>(check) != *res) {
                flag = -1;
            } else {
                flag += cnt;
            }
        }
    } else if constexpr (std::is_floating_point_v<IN>) {
        // this is called for non simd addition for floating point numbers (i.e used in addDict)
        // simd version calls sumConditionalFloat
        if (condition == addIf) {
            *res += in;
            flag += cnt;
        }
    } else {
        const IN mask = (!condition == addIf) - 1;
        *res += (in & mask);
        const int64_t cntMask = (!condition == addIf) - 1;
        flag += (cnt & cntMask);
    }
}

template<typename IN, typename MID, bool addIf>
FAST_MATH
NO_INLINE
void sumConditionalFloat(MID *res, int64_t &flag,
    const IN * __restrict ptr, const int32_t rowCount, const uint8_t * __restrict condition)
{
    static_assert(std::is_floating_point_v<IN>, "Not floating point input passed to sumConditionalFloat");
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

    const auto *endPtr = ptr + rowCount;

    using equivalent_integer = std::conditional_t<sizeof(IN) == 4, uint32_t, uint64_t>;
    const auto len = sizeof(IN);

    while (ptr < endPtr) {
        equivalent_integer iValue;
        // Note: using memcpy_s hugely degrades performance
        memcpy(&iValue, ptr, len);
        iValue &= (!*condition == addIf) - 1;
        IN fValue;
        memcpy(&fValue, &iValue, len);
        *res += fValue;

        flag += *condition == addIf;

        ++ptr;
        ++condition;
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID, typename ResultType>
class SumAggregator : public TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW> {
    using InVector = typename NativeAndVectorType<IN_ID>::vector;
    using InType = std::conditional_t<IN_ID == OMNI_VARCHAR, Int128, typename NativeAndVectorType<IN_ID>::type>;
    using OutVector = typename NativeAndVectorType<OUT_ID>::vector;
    using OutType = std::conditional_t<OUT_ID == OMNI_VARCHAR, Int128, typename NativeAndVectorType<OUT_ID>::type>;
public:
    SumAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels)
    {}

    ~SumAggregator() override = default;

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        auto vector = vectorBatch->GetVector(this->channels[0]);

        auto vectorValues = vector->GetValues();
        auto positionOffset = vector->GetPositionOffset();
        auto rowCount = vector->GetSize();
        auto nullAddr = vector->GetValueNulls();
        bool overflow = false;
        long sumVal = 0;

        auto inputTypeId = this->inputTypes.GetType(0)->GetId();
        HmppResult result = HMPP_STS_NO_ERR;
        switch (inputTypeId) {
            case OMNI_LONG: {
                LogDebug("HMPP-Agg-sum");
                result = HMPPS_Sum_64s(static_cast<int64_t *>(static_cast<int64_t *>(vectorValues) + positionOffset),
                    rowCount, static_cast<int8_t *>(static_cast<int8_t *>(nullAddr) + positionOffset), &overflow, &sumVal);
                break;
            }
            default: {
                throw OmniException("NOT SUPPORT", "Unsupported input type for sum aggregate");
                break;
            }
        }

        if (result != HMPP_STS_NO_ERR) {
            throw OmniException("HMPP ERROR", "sum failed for hmpp error");
        }

        ResultType res = static_cast<ResultType>(sumVal);
        if (state.val == nullptr) {
            auto valPtr = this->executionContext->GetArena()->Allocate(sizeof(ResultType));
            *reinterpret_cast<ResultType *>(valPtr) = res;
            state.val = valPtr;
        } else {
            *(reinterpret_cast<ResultType *>(state.val)) += res;
        }

        if (overflow) {
            state.count = -1;
        } else if (state.count >= 0) {
            state.count++;
        }
    }

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        // not accept dictionnary vector
        if (vectorBatch->GetVector(this->channels[0])->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            return false;
        }
        // only OMNI_LONG type input support
        return (inputTypes.GetType(0)->GetId() == OMNI_LONG);
    }
#endif

    virtual void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *v = VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset);
        OutVector *vector = static_cast<OutVector *>(v);

        OutType result {};
        bool overflow = state.count < 0;

        if (state.count > 0 && state.val != nullptr) {
            result = this->template CastWithOverflow<ResultType, OutType>(
                *reinterpret_cast<ResultType *>(state.val), overflow);
        }

        if constexpr (OUT_ID == OMNI_VARCHAR) {
            vector->SetValue(offset, reinterpret_cast<uint8_t *>(&result), sizeof(OutType));
        } else if constexpr (std::is_same_v<OutType, Int128>) {
            this->SetDecimal128Value(result, vector, offset);
        } else {
            vector->SetValue(offset, result);
        }

        if (overflow) {
            this->SetNullOrThrowException(v, offset, "sum_aggregator overflow.");
        } else if (state.count == 0 || state.val == nullptr) {
            v->SetValueNull(offset);
        }
    }

    void InitState(AggregateState &state) override
    {
        state.val = this->executionContext->GetArena()->Allocate(sizeof(ResultType));
        *reinterpret_cast<ResultType *>(state.val) = ResultType {};
        state.count = 0;
    }

protected:
    SumAggregator(
        FunctionType aggregateType, DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
        : TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            aggregateType, inputTypes, outputTypes, channels)
    {}

    ALWAYS_INLINE void ProcessRawInput(
        AggregateState &state, Vector *vector, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap) override
    {
        if (state.val == nullptr) {
            InitState(state);
        }
        ResultType *res = reinterpret_cast<ResultType *>(state.val);

        InType *ptr = reinterpret_cast<InType *>(static_cast<InVector *>(vector)->GetValues());
        ptr += vector->GetPositionOffset();

        if (indexMap == nullptr) {
            ptr += rowOffset;
            if (nullMap == nullptr) {
                add<InType, ResultType, sumOp<InType, ResultType, OutType>>(res, state.count, ptr, rowCount);
            } else {
                if constexpr (std::is_floating_point_v<InType>) {
                    sumConditionalFloat<InType, ResultType, false>(res, state.count, ptr, rowCount, nullMap);
                } else {
                    addConditional<InType, ResultType, sumConditionalOp<InType, ResultType, OutType, false>>(
                        res, state.count, ptr, rowCount, nullMap);
                }
            }
        } else {
            if (nullMap == nullptr) {
                addDict<InType, ResultType, sumOp<InType, ResultType, OutType>>(
                    res, state.count, ptr, rowCount, indexMap);
            } else {
                addDictConditional<InType, ResultType, sumConditionalOp<InType, ResultType, OutType, false>>(
                    res, state.count, ptr, rowCount, nullMap, indexMap);
            }
        }
    }

    ALWAYS_INLINE void ProcessGroupRawInput(
        std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) override
    {
        InType *ptr = reinterpret_cast<InType *>(static_cast<InVector *>(vector)->GetValues());
        ptr += vector->GetPositionOffset();

        if (indexMap == nullptr) {
            ptr += rowOffset;
            if (nullMap == nullptr) {
                addUseRowIndex<InType, ResultType, sumOp<InType, ResultType, OutType>>(rowStates, aggIdx, ptr);
            } else {
                // Reza: can we use customize float operation similar to sumConditionalFloat
                addConditionalUseRowIndex<InType, ResultType, sumConditionalOp<InType, ResultType, OutType, false>>(
                    rowStates, aggIdx, ptr, nullMap);
            }
        } else {
            if (nullMap == nullptr) {
                addDictUseRowIndex<InType, ResultType, sumOp<InType, ResultType, OutType>>(
                    rowStates, aggIdx, ptr, indexMap);
            } else {
                addDictConditionalUseRowIndex<InType, ResultType, sumConditionalOp<InType, ResultType, OutType, false>>(
                    rowStates, aggIdx, ptr, nullMap, indexMap);
            }
        }
    }

    void Validate() override
    {
        static_assert(
            IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG || IN_ID == OMNI_DOUBLE
            || IN_ID == OMNI_DECIMAL128 || IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_VARCHAR,
            "Unsupported input type for sum/average aggregator");

        static_assert(
            OUT_ID == OMNI_SHORT || OUT_ID == OMNI_INT || OUT_ID == OMNI_LONG || OUT_ID == OMNI_DOUBLE
            || OUT_ID == OMNI_DECIMAL128 || OUT_ID == OMNI_DECIMAL64 || OUT_ID == OMNI_VARCHAR,
            "Unsupported output type for sum/average aggregator");

        if constexpr (IN_ID == OMNI_VARCHAR) {
            static_assert((OUT_ID == OMNI_DECIMAL128 || OUT_ID == OMNI_DECIMAL64)
                && !RAW_IN && !PARTIAL_OUT,
                "Varchar input type for sum/average aggregator should have not raw input and not partial decimal output type");
        }

        if constexpr (OUT_ID == OMNI_VARCHAR) {
            static_assert((IN_ID == OMNI_DECIMAL128 || IN_ID == OMNI_DECIMAL64)
                && RAW_IN && PARTIAL_OUT,
                "Varchar output type for sum/average aggregator should have raw decimal input type and partial output");
        }
    }
};
}
}
#endif // OMNI_RUNTIME_SUM_AGGREGATOR_H
