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
template<typename IN, typename MID>
SIMD_ALWAYS_INLINE
void sumOp(MID *res, int64_t &flag, const IN &in, const int64_t &cnt)
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

template<typename IN, typename MID, bool addIf>
SIMD_ALWAYS_INLINE
void sumConditionalOp(
    MID *res, int64_t &flag, const IN &in, const int64_t &cnt, const uint8_t &condition)
{
    if constexpr (std::is_same_v<MID, Decimal128> || std::is_same_v<IN, int64_t> || std::is_floating_point_v<IN>) {
        if (condition == addIf) {
            sumOp<IN, MID>(res, flag, in, cnt);
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

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
class SumAggregator : public TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW> {
    using InVector = typename NativeAndVectorType<IN_ID>::vector;
    using InType = typename NativeAndVectorType<IN_ID>::type;
    using OutVector = typename NativeAndVectorType<OUT_ID>::vector;
    using OutType = typename NativeAndVectorType<OUT_ID>::type;
    using ResultType = typename std::conditional_t<IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG,
        int64_t, std::conditional_t<IN_ID == OMNI_DOUBLE || IN_ID == OMNI_CONTAINER, double, Decimal128>>;
public:
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

        HmppResult result = HMPP_STS_NO_ERR;
        if constexpr (IN_ID == OMNI_LONG) {
            LogDebug("HMPP-Agg-sum");
            long sumVal = 0;
            result = HMPPS_Sum_64s(static_cast<int64_t *>(static_cast<int64_t *>(vectorValues) + positionOffset),
                rowCount, static_cast<int8_t *>(static_cast<int8_t *>(nullAddr) + positionOffset), &overflow, &sumVal);

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
        } else if constexpr (IN_ID == OMNI_DECIMAL128) {
            LogDebug("HMPP-Agg-sum");
            HmppDecimal128 sumVal {};
            result = HMPPS_Sum_decimal128(
                static_cast<HmppDecimal128 *>(static_cast<HmppDecimal128 *>(vectorValues) + positionOffset),
                rowCount, static_cast<int8_t *>(static_cast<int8_t *>(nullAddr) + positionOffset), &overflow, &sumVal);

            if (result != HMPP_STS_NO_ERR) {
                throw OmniException("HMPP ERROR", "sum failed for hmpp error");
            }

            if (state.val == nullptr) {
                state.val = this->executionContext->GetArena()->Allocate(sizeof(Decimal128));
                *reinterpret_cast<Decimal128 *>(state.val) = Decimal128(sumVal.high, sumVal.low);
            } else {
                Decimal128Wrapper preSumVal(*(reinterpret_cast<Decimal128 *>(state.val)));
                preSumVal = preSumVal.Add(Decimal128Wrapper(sumVal.high, sumVal.low));
                overflow |= (preSumVal.IsOverflow() != OpStatus::SUCCESS);
            }
        } else {
            throw OmniException("NOT SUPPORT", "Unsupported input type for sum aggregate");
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

        // only OMNI_LONG or OMNI_DECIMAL128 type input support
        if (this->inputTypes.GetType(0)->GetId() == OMNI_DECIMAL128) {
            // just support row Raw data for decimal128
            return RAW_IN;
        } else if (this->inputTypes.GetType(0)->GetId() == OMNI_LONG) {
            return true;
        }
        return false;
    }
#endif

    virtual void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        int32_t offset;
        auto v = static_cast<OutVector *>(VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset));

        OutType result {};
        bool overflow = state.count < 0;

        if constexpr (OUT_ID == OMNI_VARCHAR) {
            if (state.count > 0 && state.val != nullptr) {
                result.sum = this->template CastWithOverflow<Decimal128, Decimal128>(
                    *reinterpret_cast<Decimal128 *>(state.val), overflow);
            }
            result.count = overflow ? 0 : state.count;
            v->SetValue(offset, reinterpret_cast<uint8_t *>(&result), sizeof(OutType));
            if (overflow && !this->IsOverflowAsNull()) {
                throw OmniException("OPERATOR_RUNTIME_ERROR", "sum_aggregator overflow.");
            }
        } else {
            if (state.count > 0 && state.val != nullptr) {
                result = this->template CastWithOverflow<ResultType, OutType>(
                    *reinterpret_cast<ResultType *>(state.val), overflow);
            }

            v->SetValue(offset, result);
            if (overflow) {
                this->SetNullOrThrowException(v, offset, "sum_aggregator overflow.");
            } else if (state.count == 0 || state.val == nullptr) {
                v->SetValueNull(offset);
            }
        }
    }

    void InitState(AggregateState &state) override
    {
        state.val = this->executionContext->GetArena()->Allocate(sizeof(ResultType));
        *reinterpret_cast<ResultType *>(state.val) = ResultType {};
        state.count = 0;
    }

    static std::unique_ptr<Aggregator> Create(
        const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
    {
        if constexpr (!(IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG || IN_ID == OMNI_DOUBLE
            || IN_ID == OMNI_DECIMAL128 || IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CONTAINER)) {
            LogError("Error in sum aggregator: Unsupported input type %s", TypeUtil::TypeToString(IN_ID).c_str());
            return nullptr;
        } else if constexpr (!(OUT_ID == OMNI_SHORT || OUT_ID == OMNI_INT || OUT_ID == OMNI_LONG || OUT_ID == OMNI_DOUBLE
            || OUT_ID == OMNI_DECIMAL128 || OUT_ID == OMNI_DECIMAL64 || OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CONTAINER)) {
            LogError("Error in sum aggregator: Unsupported output type %s", TypeUtil::TypeToString(OUT_ID).c_str());
            return nullptr;
        } else if constexpr (RAW_IN && IN_ID == OMNI_VARCHAR)  {
            LogError("Error in sum aggregator: Invalid input type %s for inputRaw=%s",
                TypeUtil::TypeToString(IN_ID).c_str(), (RAW_IN ? "true" : "false"));
            return nullptr;
        } else if constexpr (!PARTIAL_OUT && OUT_ID == OMNI_VARCHAR)  {
            LogError("Error in sum aggregator: Invalid output type %s for outputPartial=%s",
                TypeUtil::TypeToString(OUT_ID).c_str(), (PARTIAL_OUT ? "true" : "false"));
            return nullptr;
        } else if constexpr (OUT_ID == OMNI_VARCHAR
            && (IN_ID != OMNI_VARCHAR && IN_ID != OMNI_DECIMAL64 && IN_ID != OMNI_DECIMAL128)) {
            LogError("Error in sum aggregator: Invalid input type %s for partial output with varchar type",
                TypeUtil::TypeToString(IN_ID).c_str());
            return nullptr;
        } else {
            if (!SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::CheckTypes(
                "sum", inputTypes, outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }

            return std::unique_ptr<SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>>(
                new SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>(
                    inputTypes, outputTypes, channels));
        }
    }

protected:
    SumAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels)
    {}

    SumAggregator(
        FunctionType aggregateType, const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            aggregateType, inputTypes, outputTypes, channels)
    {}

    ALWAYS_INLINE void ProcessSingleInternal(
        AggregateState &state, Vector *vector, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap) override
    {
        if (state.val == nullptr) {
            this->InitState(state);
        }
        ResultType *res = reinterpret_cast<ResultType *>(state.val);

        InType *ptr = reinterpret_cast<InType *>(static_cast<InVector *>(vector)->GetValues());
        ptr += vector->GetPositionOffset();

        if (indexMap == nullptr) {
            ptr += rowOffset;
            if (nullMap == nullptr) {
                add<InType, ResultType, sumOp<InType, ResultType>>(res, state.count, ptr, rowCount);
            } else {
                if constexpr (std::is_floating_point_v<InType>) {
                    sumConditionalFloat<InType, ResultType, false>(res, state.count, ptr, rowCount, nullMap);
                } else {
                    addConditional<InType, ResultType, sumConditionalOp<InType, ResultType, false>>(
                        res, state.count, ptr, rowCount, nullMap);
                }
            }
        } else {
            if (nullMap == nullptr) {
                addDict<InType, ResultType, sumOp<InType, ResultType>>(res, state.count, ptr, rowCount, indexMap);
            } else {
                addDictConditional<InType, ResultType, sumConditionalOp<InType, ResultType, false>>(
                    res, state.count, ptr, rowCount, nullMap, indexMap);
            }
        }
    }

    ALWAYS_INLINE void ProcessGroupInternal(
        std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) override
    {
        InType *ptr = reinterpret_cast<InType *>(static_cast<InVector *>(vector)->GetValues());
        ptr += vector->GetPositionOffset();

        if (indexMap == nullptr) {
            ptr += rowOffset;
            if (nullMap == nullptr) {
                addUseRowIndex<InType, ResultType, sumOp<InType, ResultType>>(rowStates, aggIdx, ptr);
            } else {
                // Reza: can we use customize float operation similar to sumConditionalFloat
                addConditionalUseRowIndex<InType, ResultType, sumConditionalOp<InType, ResultType, false>>(
                    rowStates, aggIdx, ptr, nullMap);
            }
        } else {
            if (nullMap == nullptr) {
                addDictUseRowIndex<InType, ResultType, sumOp<InType, ResultType>>(rowStates, aggIdx, ptr, indexMap);
            } else {
                addDictConditionalUseRowIndex<InType, ResultType, sumConditionalOp<InType, ResultType, false>>(
                    rowStates, aggIdx, ptr, nullMap, indexMap);
            }
        }
    }

    static bool CheckTypes(const std::string &aggName,
        const DataTypes &inputTypes, const DataTypes &outputTypes, const DataTypeId inId, const DataTypeId outId)
    {
        if (!TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>::CheckTypes(
            aggName, inputTypes, outputTypes, inId, outId)) {
            return false;
        }

        constexpr int32_t partialWidth = sizeof(DecimalPartialResult);
        if constexpr (IN_ID == OMNI_VARCHAR) {
            static_cast<VarcharDataType *>(inputTypes.GetType(0).get())->SetWidth(partialWidth);
        }
        if constexpr (OUT_ID == OMNI_VARCHAR) {
            static_cast<VarcharDataType *>(outputTypes.GetType(0).get())->SetWidth(partialWidth);
        }

        return true;
    }
};
}
}
#endif // OMNI_RUNTIME_SUM_AGGREGATOR_H
