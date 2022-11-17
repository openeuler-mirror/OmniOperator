/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Average aggregate
 */
#ifndef OMNI_RUNTIME_AVERAGE_AGGREGATOR_H
#define OMNI_RUNTIME_AVERAGE_AGGREGATOR_H

#include "sum_aggregator.h"

#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
template<typename IN, typename MID, bool addIf>
VECTORIZE_LOOP FAST_MATH NO_INLINE
void avgConditionalFloat(MID *res, int64_t &flag, const IN * __restrict ptr, const int64_t * __restrict cntPtr,
    const size_t rowCount, const uint8_t * __restrict condition)
{
    static_assert(std::is_floating_point_v<IN>, "Not floating point input passed to avgConditionalFloat");
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
        memcpy(&iValue, &ptr[i], len);
        iValue &= mask;
        IN fValue;
        memcpy(&fValue, &iValue, len);
        *res += fValue;

        flag += (cntPtr[i] & mask);
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
class AverageAggregator
    : public SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID> {
    using InVector = typename NativeAndVectorType<IN_ID>::vector;
    using InType = typename NativeAndVectorType<IN_ID>::type;
    using OutVector = typename NativeAndVectorType<OUT_ID>::vector;
    using OutType = typename NativeAndVectorType<OUT_ID>::type;
    using ResultType = typename std::conditional_t<IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG,
        int64_t, std::conditional_t<IN_ID == OMNI_DOUBLE || IN_ID == OMNI_CONTAINER, double, Decimal128>>;
public:
    ~AverageAggregator() override = default;

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        auto vector = vectorBatch->GetVector(this->channels[0]);

        auto vectorValues = vector->GetValues();
        auto positionOffset = vector->GetPositionOffset();
        auto rowCount = vector->GetSize();
        auto nullAddr = vector->GetValueNulls();
        bool overflow = false;
        int32_t count = 0;

        HmppResult result = HMPP_STS_NO_ERR;
        if constexpr (IN_ID == OMNI_LONG) {
            LogDebug("HMPP-Agg-avg");
            double sumVal = 0;
            result = HMPPS_Mean_64s(static_cast<int64_t *>(static_cast<int64_t *>(vectorValues) + positionOffset),
                rowCount, static_cast<int8_t *>(static_cast<int8_t *>(nullAddr) + positionOffset), &overflow,
                &sumVal, &count);

            if (result != HMPP_STS_NO_ERR) {
                throw OmniException("HMPP ERROR", "avg failed for hmpp error");
            }

            if (state.val == nullptr) {
                auto valPtr = this->executionContext->GetArena()->Allocate(sizeof(ResultType));
                *reinterpret_cast<ResultType *>(valPtr) = static_cast<ResultType>(sumVal);
                state.val = valPtr;
                state.count = static_cast<int64_t>(count);
            } else {
                *(reinterpret_cast<ResultType *>(state.val)) += static_cast<ResultType>(sumVal);
                state.count += static_cast<int64_t>(count);
            }
        } else if constexpr (IN_ID == OMNI_DECIMAL128) {
            LogDebug("HMPP-Agg-avg");
            HmppDecimal128 sumVal {};
            result = HMPPS_Mean_decimal128(
                static_cast<HmppDecimal128 *>(static_cast<HmppDecimal128 *>(vectorValues) + positionOffset),
                rowCount, static_cast<int8_t *>(static_cast<int8_t *>(nullAddr) + positionOffset), &overflow,
                &sumVal, &count);

            if (result != HMPP_STS_NO_ERR) {
                throw OmniException("HMPP ERROR", "avg failed for hmpp error");
            }

            if (state.val == nullptr) {
                state.val = this->executionContext->GetArena()->Allocate(sizeof(Decimal128));
                *reinterpret_cast<Decimal128 *>(state.val) = Decimal128(sumVal.high, sumVal.low);
            } else {
                Decimal128Wrapper preSumVal(*(reinterpret_cast<Decimal128 *>(state.val)));
                preSumVal = preSumVal.Add(Decimal128Wrapper(sumVal.high, sumVal.low));
                overflow |= (preSumVal.IsOverflow() != OpStatus::SUCCESS);
            }
            if (overflow) {
                state.count = -1;
            } else if (state.count >= 0) {
                state.count += static_cast<int64_t>(count);
            }
        } else {
            throw OmniException("NOT SUPPORT", "Unsupported input type for avg aggregate");
        }
    }

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        // just support raw input data
        if constexpr (!RAW_IN) {
            return false;
        } else {
            // not accept dictionnary vector
            if (vectorBatch->GetVector(this->channels[0])->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
                return false;
            }
            // only long or decimal128 type input support
            return this->inputTypes.GetType(0)->GetId() == OMNI_LONG
                || this->inputTypes.GetType(0)->GetId() == OMNI_DECIMAL128;
        }
    }
#endif

    void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        if constexpr (PARTIAL_OUT) {
            if constexpr (OUT_ID == OMNI_VARCHAR) {
                SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::ExtractValues(
                state, vectors, rowIndex);
            } else if constexpr (OUT_ID == OMNI_CONTAINER) {
                ExtractPartialContainerValues(state, vectors, rowIndex);
            } else {
                throw OmniException("Unreachable code",
                    "Reached unreachable code in average aggregator extract partial");
            }
        } else {
            ExtractFinalValues(state, vectors, rowIndex);
        }
    }

    static std::unique_ptr<Aggregator> Create(
        const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
    {
        if constexpr (!(IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG || IN_ID == OMNI_DOUBLE
            || IN_ID == OMNI_DECIMAL128 || IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CONTAINER)) {
            LogError("Error in average aggregator: Unsupported input type %s", TypeUtil::TypeToString(IN_ID).c_str());
            return nullptr;
        } else if constexpr (!(OUT_ID == OMNI_DOUBLE || OUT_ID == OMNI_DECIMAL128 || OUT_ID == OMNI_DECIMAL64
            || OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CONTAINER)) {
            LogError("Error in average aggregator: Unsupported output type %s", TypeUtil::TypeToString(OUT_ID).c_str());
            return nullptr;
        } else if constexpr ((RAW_IN && (IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CONTAINER))
            || (!RAW_IN && (IN_ID != OMNI_VARCHAR && IN_ID != OMNI_CONTAINER)))  {
            LogError("Error in average aggregator: Invalid input type %s for inputRaw=%s",
                TypeUtil::TypeToString(IN_ID).c_str(), (RAW_IN ? "true" : "false"));
            return nullptr;
        } else if constexpr ((!PARTIAL_OUT && (OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CONTAINER))
            || (PARTIAL_OUT && (OUT_ID != OMNI_VARCHAR && OUT_ID != OMNI_CONTAINER)))  {
            LogError("Error in average aggregator: Invalid output type %s for outputPartial=%s",
                TypeUtil::TypeToString(OUT_ID).c_str(), (PARTIAL_OUT ? "true" : "false"));
            return nullptr;
        } else if constexpr (IN_ID == OMNI_VARCHAR && OUT_ID == OMNI_CONTAINER) {
            LogError("Error in average aggregator: Invalid output type %s for partial input with varchar type",
                TypeUtil::TypeToString(OUT_ID).c_str());
            return nullptr;
        } else if constexpr (IN_ID == OMNI_CONTAINER && OUT_ID == OMNI_VARCHAR) {
            LogError("Error in average aggregator: Invalid output type %s for partial input with container type",
                TypeUtil::TypeToString(OUT_ID).c_str());
            return nullptr;
        } else if constexpr (OUT_ID == OMNI_VARCHAR
            && (IN_ID != OMNI_VARCHAR && IN_ID != OMNI_DECIMAL64 && IN_ID != OMNI_DECIMAL128)) {
            LogError("Error in average aggregator: Invalid input type %s for partial output with varchar type",
                TypeUtil::TypeToString(IN_ID).c_str());
            return nullptr;
        } else if constexpr (OUT_ID == OMNI_CONTAINER
            && (IN_ID == OMNI_VARCHAR || IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_DECIMAL128)) {
            LogError("Error in average aggregator: Invalid input type %s for partial output with container type",
                TypeUtil::TypeToString(IN_ID).c_str());
            return nullptr;
        } else {
            if (!SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::CheckTypes(
                "average", inputTypes, outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }

            return std::unique_ptr<AverageAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>>(
                new AverageAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>(
                    inputTypes, outputTypes, channels));
        }
    }

protected:
    AverageAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>(
            OMNI_AGGREGATION_TYPE_AVG, inputTypes, outputTypes, channels)
    {}

    ALWAYS_INLINE void ProcessSingleInternal(
        AggregateState &state, Vector *vector, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap) override
    {
        if constexpr (IN_ID == OMNI_CONTAINER) {
            if (state.val == nullptr) {
                this->InitState(state);
            }
            ResultType *res = reinterpret_cast<ResultType *>(state.val);

            // when input is not raw, vector is container with <double, long> columns for <sum, count>
            auto v = static_cast<ContainerVector *>(vector);
            InVector *sumVector = reinterpret_cast<InVector *>(v->GetValue(0));
            InType *ptr = reinterpret_cast<InType *>(sumVector->GetValues());
            ptr += sumVector->GetPositionOffset();

            LongVector *cntVector = reinterpret_cast<LongVector *>(v->GetValue(1));
            int64_t *cntPtr = reinterpret_cast<int64_t *>(cntVector->GetValues());
            cntPtr += cntVector->GetPositionOffset();

            if (indexMap == nullptr) {
                ptr += rowOffset;
                cntPtr += rowOffset;
                if (nullMap == nullptr) {
                    addAvg<InType, ResultType, sumOp<InType, ResultType>>(res, state.count, ptr, cntPtr, rowCount);
                } else {
                    if constexpr (std::is_floating_point_v<InType>) {
                        avgConditionalFloat<InType, ResultType, false>(
                            res, state.count, ptr, cntPtr, rowCount, nullMap);
                    } else {
                        addConditionalAvg<InType, ResultType, sumConditionalOp<InType, ResultType, false>>(
                            res, state.count, ptr, cntPtr, rowCount, nullMap);
                    }
                }
            } else {
                if (nullMap == nullptr) {
                    addDictAvg<InType, ResultType, sumOp<InType, ResultType>>(
                        res, state.count, ptr, cntPtr, rowCount, indexMap);
                } else {
                    addDictConditionalAvg<InType, ResultType, sumConditionalOp<InType, ResultType, false>>(
                        res, state.count, ptr, cntPtr, rowCount, nullMap, indexMap);
                }
            }
        } else {
            SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::ProcessSingleInternal(
                state, vector, rowOffset, rowCount, nullMap, indexMap);
        }
    }

    ALWAYS_INLINE void ProcessGroupInternal(
        std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) override
    {
        if constexpr (IN_ID == OMNI_CONTAINER) {
            // when input is not raw, vector is container with <double, long> columns for <sum, count>
            ContainerVector *v = static_cast<ContainerVector *>(vector);
            InVector *sumVector = reinterpret_cast<InVector *>(v->GetValue(0));
            InType *ptr = reinterpret_cast<InType *>(sumVector->GetValues());
            ptr += sumVector->GetPositionOffset();

            LongVector *cntVector = reinterpret_cast<LongVector *>(v->GetValue(1));
            int64_t *cntPtr = reinterpret_cast<int64_t *>(cntVector->GetValues());
            cntPtr += cntVector->GetPositionOffset();

            if (indexMap == nullptr) {
                ptr += rowOffset;
                cntPtr += rowOffset;
                if (nullMap == nullptr) {
                    addUseRowIndexAvg<InType, ResultType, sumOp<InType, ResultType>>(rowStates, aggIdx, ptr, cntPtr);
                } else {
                    // Reza: can we use customize float operation similar to sumConditionalFloat
                    addConditionalUseRowIndexAvg<InType, ResultType, sumConditionalOp<InType, ResultType, false>>(
                        rowStates, aggIdx, ptr, cntPtr, nullMap);
                }
            } else {
                if (nullMap == nullptr) {
                    addDictUseRowIndexAvg<InType, ResultType, sumOp<InType, ResultType>>(
                        rowStates, aggIdx, ptr, cntPtr, indexMap);
                } else {
                    addDictConditionalUseRowIndexAvg<InType, ResultType, sumConditionalOp<InType, ResultType, false>>(
                        rowStates, aggIdx, ptr, cntPtr, nullMap, indexMap);
                }
            }
        } else {
            SumAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>::ProcessGroupInternal(
                rowStates, aggIdx, vector, rowOffset, nullMap, indexMap);
        }
    }

private:
    void ExtractPartialContainerValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex)
    {
        int32_t offset;
        ContainerVector *vector = static_cast<ContainerVector *>(
            VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset));
        OutVector *doubleVector = reinterpret_cast<OutVector *>(vector->GetValue(0));
        LongVector *longVector = reinterpret_cast<LongVector *>(vector->GetValue(1));

        OutType result {};
        bool overflow = state.count < 0;
        if (state.count > 0 && state.val != nullptr) {
            result = this->template CastWithOverflow<ResultType, OutType>(
                *reinterpret_cast<ResultType *>(state.val), overflow);
        }

        doubleVector->SetValue(offset, result);
        longVector->SetValue(offset, overflow ? 0 : state.count);

        if (overflow && !this->IsOverflowAsNull()) {
            throw OmniException("OPERATOR_RUNTIME_ERROR", "average_aggregator overflow.");
        }
    }

    void ExtractFinalValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex)
    {
        int32_t offset;
        auto v = static_cast<OutVector *>(VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset));

        OutType result {};
        bool overflow = state.count < 0;
        if (state.count > 0 && state.val != nullptr) {
            if constexpr (std::is_same_v<ResultType, Decimal128>) {
                Decimal128Wrapper result128 = Decimal128Wrapper(*reinterpret_cast<Decimal128 *>(state.val))
                    .Divide(Decimal128Wrapper(state.count), 0);
                result = this->template CastWithOverflow<Decimal128, OutType>(result128.ToDecimal128(), overflow);
            } else {
                // Result type is either double or int64, which for both cases we generate double avgResult;
                double avgResult = static_cast<double>(*reinterpret_cast<ResultType *>(state.val))
                    / static_cast<double>(state.count);
                result = this->template CastWithOverflow<double, OutType>(avgResult, overflow);
            }
        }

        v->SetValue(offset, result);
        if (overflow) {
            this->SetNullOrThrowException(v, offset, "average_aggregator overflow.");
        } else if (state.count == 0 || state.val == nullptr) {
            v->SetValueNull(offset);
        }
    }
};
}
}
#endif // OMNI_RUNTIME_AVERAGE_AGGREGATOR_H
