#pragma once

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Sum aggregate for short decimal
 */

#include "typed_aggregator.h"
#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
using DecimalPartialResult = struct DecimalPartialResult {
    Decimal128 sum = 0;
    int64_t count = 0;
};

SIMD_ALWAYS_INLINE
void sumDecimalOp(Decimal128 *res, int64_t &flag, const DecimalPartialResult &in, const int64_t &notUsed)
{
    if (flag >= 0) {
        Decimal128Wrapper result = Decimal128Wrapper(*res).Add(in.sum);
        *res = result.ToDecimal128();
        if (result.IsOverflow() != OpStatus::SUCCESS) {
            flag = -1;
        } else {
            flag += in.count;
        }
    }
}

template<bool addIf>
SIMD_ALWAYS_INLINE
void sumDecimalConditionalOp(
    Decimal128 *res, int64_t &flag, const DecimalPartialResult &in, const int64_t &notUsed, const uint8_t &condition)
{
    if (condition == addIf && flag >= 0) {
        Decimal128Wrapper result = Decimal128Wrapper(*res).Add(in.sum);
        *res = result.ToDecimal128();
        if (result.IsOverflow() != OpStatus::SUCCESS) {
            flag = -1;
        } else {
            flag += in.count;
        }
    }
}

template<typename IN>
SIMD_ALWAYS_INLINE
void sumDecimalOp(Decimal128 *res, int64_t &flag, const IN &in, const int64_t &cnt)
{
    static_assert(std::is_same_v<IN, int64_t> || std::is_same_v<IN, Decimal128>,
        "Input to decimal aggregator is not decimal type");
    if (flag >= 0) {
        int64_t overflow;
        if constexpr (std::is_same_v<IN, Decimal128>) {
            Decimal128Wrapper result = Decimal128Wrapper(*res).Add(in);
            *res = result.ToDecimal128();
            overflow = static_cast<int64_t>(result.IsOverflow());
        } else {
            Decimal128Wrapper curVal(in);
            Decimal128Wrapper result = curVal.Add(*res);
            *res = result.ToDecimal128();
            overflow = static_cast<int64_t>(result.IsOverflow());
        }

        if (overflow != 0) {
            flag = -1;
        } else {
            flag += cnt;
        }
    }
}

template<typename IN, bool addIf>
SIMD_ALWAYS_INLINE
void sumDecimalConditionalOp(
    Decimal128 *res, int64_t &flag, const IN &in, const int64_t &cnt, const uint8_t &condition)
{
    static_assert(std::is_same_v<IN, int64_t> || std::is_same_v<IN, Decimal128>,
        "Input to decimal aggregator is not decimal type");
    if (condition == addIf && flag >= 0) {
        int64_t overflow;
        if constexpr (std::is_same_v<IN, Decimal128>) {
            Decimal128Wrapper result = Decimal128Wrapper(in).Add(*res);
            overflow = static_cast<int64_t>(result.IsOverflow());
            *res = result.ToDecimal128();
        } else {
            Decimal128Wrapper curVal(in);
            Decimal128Wrapper result = Decimal128Wrapper(curVal).Add(*res);
            overflow = static_cast<int64_t>(result.IsOverflow());
            *res = result.ToDecimal128();
        }

        if (overflow != 0) {
            flag = -1;
        } else {
            flag += cnt;
        }
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
class SumDecimalAggregator : public TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW> {
    using InVector = typename NativeAndVectorType<IN_ID>::vector;
    using InType = std::conditional_t<IN_ID == OMNI_VARCHAR,
        DecimalPartialResult, typename NativeAndVectorType<IN_ID>::type>;
    using OutVector = typename NativeAndVectorType<OUT_ID>::vector;
    using OutType = std::conditional_t<OUT_ID == OMNI_VARCHAR,
        DecimalPartialResult, typename NativeAndVectorType<OUT_ID>::type>;
public:
    SumDecimalAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
        : TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels)
    {}

    ~SumDecimalAggregator() override = default;

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        auto vector = vectorBatch->GetVector(this->channels[0]);

        auto vectorValues = vector->GetValues();
        auto positionOffset = vector->GetPositionOffset();
        auto rowCount = vector->GetSize();
        auto nullAddr = vector->GetValueNulls();
        bool overflow = false;
        HmppDecimal128 sumVal {};

        LogDebug("HMPP-Agg-sum");
        auto result = HMPPS_Sum_decimal128(
            static_cast<HmppDecimal128 *>(static_cast<HmppDecimal128 *>(vectorValues) + positionOffset), rowCount,
            static_cast<int8_t *>(static_cast<int8_t *>(nullAddr) + positionOffset), &overflow, &sumVal);
        if (result != HMPP_STS_NO_ERR) {
            throw OmniException("HMPP ERROR", "sum failed for hmpp error");
        }

        if (state.val == nullptr) {
            state.val = this->executionContext->GetArena()->Allocate(sizeof(Decimal128));
            *reinterpret_cast<Decimal128 *>(state.val) = Decimal128(sumVal.high, sumVal.low);
        } else {
            Decimal128Wrapper preSumVal(*(reinterpret_cast<Decimal128 *>(state.val)));
            preSumVal=preSumVal.Add(Decimal128Wrapper(sumVal.high, sumVal.low));
            overflow |= (static_cast<int64_t>(preSumVal.IsOverflow()) != 0);
        }
        if (overflow) {
            state.count = -1;
        } else if (state.count >= 0) {
            state.count++;
        }
    }

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        // just support row Raw data
        if constexpr (!RAW_IN) {
            return false;
        } else {
            // not accept dictionnary vector
            if (vectorBatch->GetVector(this->channels[0])->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
                return false;
            }
            // only OMNI_DECIMAL128 type input support
            return (this->inputTypes->GetType(0)->GetId() == OMNI_DECIMAL128);
        }
    }
#endif

    virtual void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        if constexpr (OUT_ID == OMNI_VARCHAR) {
            ExtractPartialValues(state, vectors, rowIndex);
        } else {
            ExtractFinalValues(state, vectors, rowIndex);
        }
    }

    void InitState(AggregateState &state) override
    {
        state.val = this->executionContext->GetArena()->Allocate(sizeof(Decimal128));
        *reinterpret_cast<Decimal128 *>(state.val) = Decimal128 {};
        state.count = 0;
    }

protected:
    SumDecimalAggregator(
        FunctionType aggregateType, DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
        : TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            aggregateType, inputTypes, outputTypes, channels)
    {}

    ALWAYS_INLINE void ProcessRawInput(
        AggregateState &state, Vector *vector, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap) override
    {
        if constexpr (!RAW_IN) {
            throw OmniException("Unexpected function call",
                "ProcessRawInput called for not raw input in decimal aggregator");
        } else {
            if (state.val == nullptr) {
                InitState(state);
            }
            Decimal128 *res = reinterpret_cast<Decimal128 *>(state.val);

            InType *ptr = reinterpret_cast<InType *>(static_cast<InVector *>(vector)->GetValues());
            ptr += vector->GetPositionOffset();

            if (indexMap == nullptr) {
                ptr += rowOffset;
                if (nullMap == nullptr) {
                    add<InType, Decimal128, sumDecimalOp<InType>>(res, state.count, ptr, rowCount);
                } else {
                    addConditional<InType, Decimal128, sumDecimalConditionalOp<InType, false>>(
                        res, state.count, ptr, rowCount, nullMap);
                }
            } else {
                if (nullMap == nullptr) {
                    addDict<InType, Decimal128, sumDecimalOp<InType>>(res, state.count, ptr, rowCount, indexMap);
                } else {
                    addDictConditional<InType, Decimal128, sumDecimalConditionalOp<InType, false>>(
                        res, state.count, ptr, rowCount, nullMap, indexMap);
                }
            }
        }
    }

    ALWAYS_INLINE void ProcessGroupRawInput(
        std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) override
    {
        if constexpr (!RAW_IN) {
            throw OmniException("Unexpected function call",
                "ProcessGroupRawInput called for not raw input in decimal aggregator");
        } else {
            InType *ptr = reinterpret_cast<InType *>(static_cast<InVector *>(vector)->GetValues());
            ptr += vector->GetPositionOffset();

            if (indexMap == nullptr) {
                ptr += rowOffset;
                if (nullMap == nullptr) {
                    addUseRowIndex<InType, Decimal128, sumDecimalOp<InType>>(rowStates, aggIdx, ptr);
                } else {
                    addConditionalUseRowIndex<InType, Decimal128, sumDecimalConditionalOp<InType, false>>(
                        rowStates, aggIdx, ptr, nullMap);
                }
            } else {
                if (nullMap == nullptr) {
                    addDictUseRowIndex<InType, Decimal128, sumDecimalOp<InType>>(rowStates, aggIdx, ptr, indexMap);
                } else {
                    addDictConditionalUseRowIndex<InType, Decimal128, sumDecimalConditionalOp<InType, false>>(
                        rowStates, aggIdx, ptr, nullMap, indexMap);
                }
            }
        }
    }

    ALWAYS_INLINE void ProcessPartialInput(
        AggregateState &state, Vector *vector, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap) override
    {
        if constexpr (RAW_IN) {
            throw OmniException("Unexpected function call",
                "ProcessPartialInput called for raw input in decimal aggregator");
        } else {
            if (state.val == nullptr) {
                this->InitState(state);
            }
            Decimal128 *res  = reinterpret_cast<Decimal128 *>(state.val);

            // when input is not raw, vector is varchar with <Decimal128, long> tupple per row for <sum, count>
            InType *ptr = reinterpret_cast<InType *>(static_cast<InVector *>(vector)->GetValues());
            ptr += vector->GetPositionOffset();

            if (indexMap == nullptr) {
                ptr += rowOffset;
                if (nullMap == nullptr) {
                    add<InType, Decimal128, sumDecimalOp>(res, state.count, ptr, rowCount);
                } else {
                    addConditional<InType, Decimal128, sumDecimalConditionalOp<false>>(
                        res, state.count, ptr, rowCount, nullMap);
                }
            } else {
                if (nullMap == nullptr) {
                    addDict<InType, Decimal128, sumDecimalOp>(res, state.count, ptr, rowCount, indexMap);
                } else {
                    addDictConditional<InType, Decimal128, sumDecimalConditionalOp<false>>(
                        res, state.count, ptr, rowCount, nullMap, indexMap);
                }
            }
        }
    }

    ALWAYS_INLINE void ProcessGroupPartialInput(
        std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) override
    {
        if constexpr (RAW_IN) {
            throw OmniException("Unexpected function call",
                "ProcessGroupPartialInput called for raw input in decimal aggregator");
        } else {
            // when input is not raw, vector is varchar with <Decimal128, long> tupple per row for <sum, count>
            InType *ptr = reinterpret_cast<InType *>(static_cast<InVector *>(vector)->GetValues());
            ptr += vector->GetPositionOffset();

            if (indexMap == nullptr) {
                ptr += rowOffset;
                if (nullMap == nullptr) {
                    addUseRowIndex<InType, Decimal128, sumDecimalOp>(rowStates, aggIdx, ptr);
                } else {
                    addConditionalUseRowIndex<InType, Decimal128, sumDecimalConditionalOp<false>>(
                        rowStates, aggIdx, ptr, nullMap);
                }
            } else {
                if (nullMap == nullptr) {
                    addDictUseRowIndex<InType, Decimal128, sumDecimalOp>(rowStates, aggIdx, ptr, indexMap);
                } else {
                    addDictConditionalUseRowIndex<InType, Decimal128, sumDecimalConditionalOp<false>>(
                        rowStates, aggIdx, ptr, nullMap, indexMap);
                }
            }
        }
    }

    void ExtractPartialValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex)
    {
        if constexpr (!PARTIAL_OUT || OUT_ID != OMNI_VARCHAR) {
            throw OmniException("Logical Error",
                "ExtractPartialValues with outputPartial=false for sum/avg decimal aggregator");
        } else {
            int32_t offset;
            Vector *v = VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset);
            OutVector *vector = static_cast<OutVector *>(v);

            OutType result {};
            bool overflow = state.count < 0;

            if (state.count > 0 && state.val != nullptr) {
                result.sum = this->template CastWithOverflow<Decimal128, Decimal128>(
                    *reinterpret_cast<Decimal128 *>(state.val), overflow);
            }

            result.count = overflow ? 0 : state.count;
            vector->SetValue(offset, reinterpret_cast<uint8_t *>(&result), sizeof(OutType));

            if (overflow && !this->IsOverflowAsNull()) {
                throw OmniException("OPERATOR_RUNTIME_ERROR", "decimal_aggregator overflow.");
            }
        }
    }

    virtual void ExtractFinalValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex)
    {
        if constexpr (PARTIAL_OUT) {
            throw OmniException("Logical Error",
                "ExtractFinalValues with outputPartial=true for sum decimal aggregator");
        } else if constexpr (OUT_ID != OMNI_DECIMAL128 && OUT_ID != OMNI_DECIMAL64) {
            throw OmniException("Logical Error",
                "ExtractFinalValues output type is not decimal sum decimal aggregator");
        } else {
            int32_t offset;
            Vector *v = VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset);
            OutVector *vector = static_cast<OutVector *>(v);

            OutType result{};
            bool overflow = state.count < 0;

            if (state.count > 0 && state.val != nullptr) {
                result = this->template CastWithOverflow<Decimal128, OutType>(
                    *reinterpret_cast<Decimal128 *>(state.val), overflow);
            }

            vector->SetValue(offset, result);
            if (overflow) {
                this->SetNullOrThrowException(v, offset, "sum_decimal_aggregator overflow");
            } else if (state.count == 0 || state.val == nullptr) {
                v->SetValueNull(offset);
            }
        }
    }

    void Validate() override
    {
        static_assert(IN_ID == OMNI_DECIMAL128 || IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_VARCHAR,
            "Unsupported input type for sum/average decimal aggregator");

        static_assert(OUT_ID == OMNI_DECIMAL128 || OUT_ID == OMNI_DECIMAL64 || OUT_ID == OMNI_VARCHAR,
            "Unsupported output type for sum/average decimal aggregator");

        if constexpr (IN_ID == OMNI_VARCHAR) {
            static_assert((OUT_ID == OMNI_DECIMAL128 || OUT_ID == OMNI_DECIMAL64) && !RAW_IN && !PARTIAL_OUT,
                "Varchar input type for sum/average decimal aggregator should have not raw input and not partial decimal output type");
        }

        if constexpr (OUT_ID == OMNI_VARCHAR) {
            static_assert((IN_ID == OMNI_DECIMAL128 || IN_ID == OMNI_DECIMAL64) && RAW_IN && PARTIAL_OUT,
                "Varchar output type for sum/average decimal aggregator should have raw decimal input type and partial output");
        }
    }
};
}
}
