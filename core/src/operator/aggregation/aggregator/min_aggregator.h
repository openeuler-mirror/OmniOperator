/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Min aggregate
 */
#ifndef OMNI_RUNTIME_MIN_AGGREGATOR_H
#define OMNI_RUNTIME_MIN_AGGREGATOR_H

#include <cstdint>
#include <cfloat>

#include "typed_aggregator.h"
#ifdef ENABLE_HMPP
#include "aggregator_util.h"
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
template<typename T>
T getMax()
{
    if constexpr (std::is_same_v<T, int8_t>) {
        return 0x7F;
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return 0x7FFF;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return 0x7FFFFFFF;
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return 0x7FFFFFFFFFFFFFFF;
    } else if constexpr (std::is_same_v<T, float>) {
        return FLT_MAX;
    } else if constexpr (std::is_same_v<T, double>) {
        return DBL_MAX;
    } else if constexpr (std::is_same_v<T, Int128>) {
        return std::numeric_limits<Int128>::max();
    } else if constexpr (std::is_same_v<T, omniruntime::type::Decimal128>) {
        return omniruntime::type::Decimal128(0x7FFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF);
    } else {
        throw OmniException("LogicalError", "Unsupoorted data type");
    }
}

template<typename IN>
SIMD_ALWAYS_INLINE
void minOp(IN *res, int64_t &flag, const IN &in, const int64_t &notUsed)
{
    if (*res > in) {
        *res = in;
    }
    flag |= 1;
}

template<typename IN, bool addIf>
SIMD_ALWAYS_INLINE
void minConditionalOp(IN *res, int64_t &flag, const IN &in, const int64_t &notUsed, const uint8_t &condition)
{
    if (condition == addIf) {
        if (*res > in) {
            *res = in;
        }
        flag |= 1;
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
class MinAggregator : public TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW> {
    using InVector = typename NativeAndVectorType<IN_ID>::vector;
    using InType = typename NativeAndVectorType<IN_ID>::type;
    using OutVector = typename NativeAndVectorType<OUT_ID>::vector;
    using OutType = typename NativeAndVectorType<OUT_ID>::type;
public:
    MinAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            OMNI_AGGREGATION_TYPE_MIN, inputTypes, outputTypes, channels)
    {}

    ~MinAggregator() override = default;

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        auto vector = vectorBatch->GetVector(this->channels[0]);

        auto vectorValues = vector->GetValues();
        auto positionOffset = vector->GetPositionOffset();
        auto rowCount = vector->GetSize();
        auto inputTypeId = inputTypes.GetType(0)->GetId();
        auto outputTypeId = outputTypes.GetType(0)->GetId();

        HmppResult result = HMPP_STS_NO_ERR;
        auto minVal = reinterpret_cast<InType *>(this->executionContext->GetArena()->Allocate(sizeof(InType)));
        switch (inputTypeId) {
            case OMNI_SHORT: {
                LogDebug("HMPP-Agg-min");
                result = HMPPS_Min_16s(static_cast<int16_t *>(static_cast<int16_t *>(vectorValues) + positionOffset),
                    rowCount, reinterpret_cast<int16_t *>(minVal));
                if (outputTypeId == OMNI_LONG) {
                    *minVal = *reinterpret_cast<int16_t *>(minVal);
                }
                break;
            }
            case OMNI_INT:
            case OMNI_DATE32: {
                LogDebug("HMPP-Agg-min");
                result = HMPPS_Min_32s(static_cast<int32_t *>(static_cast<int32_t *>(vectorValues) + positionOffset),
                    rowCount, reinterpret_cast<int32_t *>(minVal));
                if (outputTypeId == OMNI_LONG) {
                    *minVal = *reinterpret_cast<int32_t *>(minVal);
                }
                break;
            }
            case OMNI_LONG:
            case OMNI_DECIMAL64: {
                LogDebug("HMPP-Agg-min");
                result = HMPPS_Min_64s(static_cast<int64_t *>(static_cast<int64_t *>(vectorValues) + positionOffset),
                    rowCount, reinterpret_cast<int64_t *>(minVal));
                break;
            }
            case OMNI_DOUBLE: {
                LogDebug("HMPP-Agg-min");
                result = HMPPS_Min_64f(static_cast<double *>(static_cast<double *>(vectorValues) + positionOffset),
                    rowCount, reinterpret_cast<double *>(minVal));
                break;
            }
            case OMNI_DECIMAL128: {
                LogDebug("HMPP-Agg-min");
                result = HMPPS_Min_decimal(
                    static_cast<HmppDecimal128 *>(static_cast<HmppDecimal128 *>(vectorValues) + positionOffset),
                    rowCount, reinterpret_cast<HmppDecimal128 *>(minVal));
                break;
            }
            default: {
                throw OmniException("NOT SUPPORT", "Unsupported input type for min aggregate");
                break;
            }
        }

        if (result != HMPP_STS_NO_ERR) {
            throw OmniException("HMPP ERROR", "min failed for hmpp error");
        }
        if (state.val == nullptr) {
            state.val = minVal;
        } else {
            auto preMinVal = static_cast<InType *>(state.val);
            *static_cast<InType *>(state.val) = (Compare(*preMinVal, *minVal) == -1) ? *preMinVal : *minVal;
        }
        // hmpp only works on not nullable columns, so it always find min
        state.count = 1;
    }

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        // just support raw input data and must no null inpout
        if constexpr (!RAW_IN) {
            return false;
        } else {
            if (vectorBatch->GetVector(this->channels[0])->MayHaveNull()) {
                return false;
            }
            // not accept dictionnary vector
            if (vectorBatch->GetVector(this->channels[0])->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
                return false;
            }
            // type check with whitelist for min
            auto inputTypeId = this->inputTypes->GetType(0)->GetId();
            return AggregatorUtil::IsHMPPMaxMinSupportDataTypeId(inputTypeId);
        }
    }
#endif

    void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        int32_t offset;
        auto v = static_cast<OutVector *>(VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset));
        if (state.count == 0 || (state.count > 0 && state.val == nullptr)) {
            v->SetValueNull(offset);
            return;
        }

        bool overflow = state.count < 0;
        OutType result = this->template CastWithOverflow<InType, OutType>(
            *reinterpret_cast<InType *>(state.val), overflow);
        if (overflow) {
            this->SetNullOrThrowException(v, offset, "min_aggregator overflow.");
        } else {
            if constexpr (std::is_same_v<OutType, Int128>) {
                this->SetDecimal128Value(result, v, offset);
            } else {
                v->SetValue(offset, result);
            }
        }
    }

    void InitState(AggregateState &state) override
    {
        state.val = this->executionContext->GetArena()->Allocate(sizeof(InType));
        *reinterpret_cast<InType *>(state.val) = getMax<InType>();
        state.count = 0;
    }

protected:
    ALWAYS_INLINE void ProcessRawInput(
        AggregateState &state, Vector *vector, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap) override
    {
        if (state.val == nullptr) {
            InitState(state);
        }
        InType *res = reinterpret_cast<InType *>(state.val);

        InType *ptr = reinterpret_cast<InType *>(static_cast<InVector *>(vector)->GetValues());
        ptr += vector->GetPositionOffset();

        if (indexMap == nullptr) {
            ptr += rowOffset;
            if (nullMap == nullptr) {
                add<InType, InType, minOp<InType>>(res, state.count, ptr, rowCount);
            } else {
                addConditional<InType, InType, minConditionalOp<InType, false>>(
                    res, state.count, ptr, rowCount, nullMap);
            }
        } else {
            if (nullMap == nullptr) {
                addDict<InType, InType, minOp<InType>>(res, state.count, ptr, rowCount, indexMap);
            } else {
                addDictConditional<InType, InType, minConditionalOp<InType, false>>(
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
                addUseRowIndex<InType, InType, minOp<InType>>(rowStates, aggIdx, ptr);
            } else {
                addConditionalUseRowIndex<InType, InType, minConditionalOp<InType, false>>(
                    rowStates, aggIdx, ptr, nullMap);
            }
        } else {
            if (nullMap == nullptr) {
                addDictUseRowIndex<InType, InType, minOp<InType>>(rowStates, aggIdx, ptr, indexMap);
            } else {
                addDictConditionalUseRowIndex<InType, InType, minConditionalOp<InType, false>>(
                    rowStates, aggIdx, ptr, nullMap, indexMap);
            }
        }
    }

    void Validate() override
    {
        static_assert(
            IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG || IN_ID == OMNI_DOUBLE
            || IN_ID == OMNI_DECIMAL128 || IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_BOOLEAN,
            "Unsupported input type for min aggregator");

        static_assert(
            OUT_ID == OMNI_SHORT || OUT_ID == OMNI_INT || OUT_ID == OMNI_LONG || OUT_ID == OMNI_DOUBLE
            || OUT_ID == OMNI_DECIMAL128 || OUT_ID == OMNI_DECIMAL64 || OUT_ID == OMNI_BOOLEAN,
            "Unsupported output type for min aggregator");
    }
};
}
}
#endif // OMNI_RUNTIME_MIN_AGGREGATOR_H
