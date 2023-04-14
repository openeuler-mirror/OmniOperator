/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Sum aggregator
 */

#include "sum_aggregator.h"
#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
#ifdef ENABLE_HMPP
template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch)
{
    if constexpr (IN_ID != OMNI_LONG && IN_ID != OMNI_DECIMAL128) {
        throw OmniException("NOT SUPPORT", "Unsupported input type for sum aggregate");
    } else {
        auto vector = vectorBatch->Get(this->channels[0]);

        auto vectorValues = VectorHelper::UnsafeGetValues(vector, IN_ID);
        auto rowCount = vector->GetSize();
        auto nullAddr = reinterpret_cast<void *>(unsafe::UnsafeBaseVector::GetNulls(vector));
        bool overflow = false;

        HmppResult result = HMPP_STS_NO_ERR;

        if constexpr (IN_ID == OMNI_LONG) {
            LogDebug("HMPP-Agg-sum");
            long sumVal = 0;
            result = HMPPS_Sum_64s(static_cast<int64_t *>(static_cast<int64_t *>(vectorValues)), rowCount,
                static_cast<int8_t *>(static_cast<int8_t *>(nullAddr)), &overflow, &sumVal);
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
        } else {
            // IN_ID == OMNI_DECIMAL128
            LogDebug("HMPP-Agg-sum");
            HmppDecimal128 sumVal{};
            result = HMPPS_Sum_decimal128(static_cast<HmppDecimal128 *>(static_cast<HmppDecimal128 *>(vectorValues)),
                rowCount, static_cast<int8_t *>(static_cast<int8_t *>(nullAddr)), &overflow, &sumVal);
            if (result != HMPP_STS_NO_ERR) {
                throw OmniException("HMPP ERROR", "sum failed for hmpp error");
            }

            if (state.val == nullptr) {
                state.val = this->executionContext->GetArena()->Allocate(sizeof(Decimal128));
                *reinterpret_cast<Decimal128 *>(state.val) = Decimal128(sumVal.high, sumVal.low);
            } else {
                Decimal128Wrapper preSumVal(*(reinterpret_cast<Decimal128 *>(state.val)));
                preSumVal = preSumVal.Add(Decimal128Wrapper(sumVal.high, sumVal.low));
                *reinterpret_cast<Decimal128 *>(state.val) = preSumVal.ToDecimal128();
                overflow |= (preSumVal.IsOverflow() != OpStatus::SUCCESS);
            }
        }

        if (overflow) {
            state.count = -1;
        } else if (state.count >= 0) {
            state.count++;
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
bool SumAggregator<IN_ID, OUT_ID>::CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch)
{
    // not accept dictionnary vector
    if (vectorBatch->Get(this->channels[0])->GetEncoding() == OMNI_DICTIONARY) {
        return false;
    }

    // only OMNI_LONG or OMNI_DECIMAL128 type input support
    if (this->inputTypes.GetType(0)->GetId() == OMNI_DECIMAL128) {
        // just support row Raw data for decimal128
        return inputRaw;
    } else if (this->inputTypes.GetType(0)->GetId() == OMNI_LONG) {
        return true;
    }
    return false;
}
#endif

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex)
{
    auto v = static_cast<OutVector *>(vectors[0]);

    OutType result{};
    bool overflow = state.count < 0;

    if constexpr (OUT_ID == OMNI_VARCHAR) {
        if (state.count > 0 && state.val != nullptr) {
            result.sum = this->template CastWithOverflow<Decimal128, Decimal128>(
                *reinterpret_cast<Decimal128 *>(state.val), overflow);
        }
        result.count = overflow ? 0 : state.count;
        std::string_view decimal2Str(reinterpret_cast<char *>(&result), sizeof(OutType));
        v->SetValue(rowIndex, decimal2Str);
        if (overflow && !this->IsOverflowAsNull()) {
            throw OmniException("OPERATOR_RUNTIME_ERROR", "sum_aggregator overflow.");
        }
    } else {
        if (state.count > 0 && state.val != nullptr) {
            result = this->template CastWithOverflow<ResultType, OutType>(*reinterpret_cast<ResultType *>(state.val),
                overflow);
        }
        v->SetValue(rowIndex, result);
        if (overflow) {
            this->SetNullOrThrowException(v, rowIndex, "sum_aggregator overflow.");
        } else if (state.count == 0 || state.val == nullptr) {
            v->SetNull(rowIndex);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> void SumAggregator<IN_ID, OUT_ID>::InitState(AggregateState &state)
{
    state.val = this->executionContext->GetArena()->Allocate(sizeof(ResultType));
    *reinterpret_cast<ResultType *>(state.val) = ResultType{};
    state.count = 0;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState &state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap)
{
    if (state.val == nullptr) {
        this->InitState(state);
    }
    auto *res = reinterpret_cast<ResultType *>(state.val);

    if (indexMap == nullptr) {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            Add<InType, ResultType, SumOp<InType, ResultType>>(res, state.count, ptr, rowCount);
        } else {
            if constexpr (std::is_floating_point_v<InType>) {
                SumConditionalFloat<InType, ResultType, false>(res, state.count, ptr, rowCount, nullMap);
            } else {
                AddConditional<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(res, state.count, ptr,
                    rowCount, nullMap);
            }
        }
    } else {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
        if (nullMap == nullptr) {
            AddDict<InType, ResultType, SumOp<InType, ResultType>>(res, state.count, ptr, rowCount, indexMap);
        } else {
            AddDictConditional<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(res, state.count, ptr,
                rowCount, nullMap, indexMap);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ProcessSingleInternalFilter(AggregateState &state, BaseVector *vector,
    Vector<bool> *booleanVector, const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap,
    const int32_t *indexMap)
{
    if (state.val == nullptr) {
        this->InitState(state);
    }
    auto *res = reinterpret_cast<ResultType *>(state.val);
    int8_t *boolPtr = reinterpret_cast<int8_t *>(GetValuesFromVector<type::OMNI_BOOLEAN>(booleanVector));

    if (indexMap == nullptr) {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            AddFilter<InType, ResultType, SumOp<InType, ResultType>>(res, state.count, ptr, rowCount, boolPtr);
        } else {
            if constexpr (std::is_floating_point_v<InType>) {
                SumConditionalFloat<InType, ResultType, false>(res, state.count, ptr, rowCount, nullMap);
            } else {
                AddConditionalFilter<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(res, state.count,
                    ptr, rowCount, nullMap, boolPtr);
            }
        }
    } else {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
        if (nullMap == nullptr) {
            AddDictFilter<InType, ResultType, SumOp<InType, ResultType>>(res, state.count, ptr, rowCount, indexMap,
                boolPtr);
        } else {
            AddDictConditionalFilter<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(res, state.count,
                ptr, rowCount, nullMap, indexMap, boolPtr);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    BaseVector *vector, const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap)
{
    if (indexMap == nullptr) {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            AddUseRowIndex<InType, ResultType, SumOp<InType, ResultType>>(rowStates, aggIdx, ptr);
        } else {
            // Reza: can we use customize float operation similar to sumConditionalFloat
            AddConditionalUseRowIndex<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(rowStates,
                aggIdx, ptr, nullMap);
        }
    } else {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
        if (nullMap == nullptr) {
            AddDictUseRowIndex<InType, ResultType, SumOp<InType, ResultType>>(rowStates, aggIdx, ptr, indexMap);
        } else {
            AddDictConditionalUseRowIndex<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(rowStates,
                aggIdx, ptr, nullMap, indexMap);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ProcessGroupInternalFilter(std::vector<AggregateState *> &rowStates,
    const size_t aggIdx, BaseVector *vector, Vector<bool> *booleanVector, const int32_t rowOffset,
    const uint8_t *nullMap, const int32_t *indexMap)
{
    int8_t *boolPtr = reinterpret_cast<int8_t *>(GetValuesFromVector<type::OMNI_BOOLEAN>(booleanVector));

    if (indexMap == nullptr) {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            AddUseRowIndexFilter<InType, ResultType, SumOp<InType, ResultType>>(rowStates, aggIdx, ptr, boolPtr);
        } else {
            // Reza: can we use customize float operation similar to sumConditionalFloat
            AddConditionalUseRowIndexFilter<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(rowStates,
                aggIdx, ptr, nullMap, boolPtr);
        }
    } else {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
        if (nullMap == nullptr) {
            AddDictUseRowIndexFilter<InType, ResultType, SumOp<InType, ResultType>>(rowStates, aggIdx, ptr, indexMap,
                boolPtr);
        } else {
            AddDictConditionalUseRowIndexFilter<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(
                rowStates, aggIdx, ptr, nullMap, indexMap, boolPtr);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
SumAggregator<IN_ID, OUT_ID>::SumAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
    : TypedAggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels, inputRaw, outputPartial,
    isOverflowAsNull)
{}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
SumAggregator<IN_ID, OUT_ID>::SumAggregator(FunctionType aggregateType, const DataTypes &inputTypes,
    const DataTypes &outputTypes, std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial,
    const bool isOverflowAsNull)
    : TypedAggregator(aggregateType, inputTypes, outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull)
{}

// Explicit template instantiation
// Defining templated aggregators in header file consume a lot of memory during compilation
// since, compiler needs to generate each individual template instance wherever aggregator header is include
// to reduce time and memory usage during compilation moved templated aggregator implementation into .cpp files
// and used explicit template instantiation to generate template instances
template class SumAggregator<OMNI_SHORT, OMNI_SHORT>;

template class SumAggregator<OMNI_SHORT, OMNI_INT>;

template class SumAggregator<OMNI_SHORT, OMNI_LONG>;

template class SumAggregator<OMNI_SHORT, OMNI_DOUBLE>;

template class SumAggregator<OMNI_SHORT, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_SHORT, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_SHORT, OMNI_VARCHAR>;

template class SumAggregator<OMNI_SHORT, OMNI_CONTAINER>;

template class SumAggregator<OMNI_INT, OMNI_SHORT>;

template class SumAggregator<OMNI_INT, OMNI_INT>;

template class SumAggregator<OMNI_INT, OMNI_LONG>;

template class SumAggregator<OMNI_INT, OMNI_DOUBLE>;

template class SumAggregator<OMNI_INT, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_INT, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_INT, OMNI_VARCHAR>;

template class SumAggregator<OMNI_INT, OMNI_CONTAINER>;

template class SumAggregator<OMNI_LONG, OMNI_SHORT>;

template class SumAggregator<OMNI_LONG, OMNI_INT>;

template class SumAggregator<OMNI_LONG, OMNI_LONG>;

template class SumAggregator<OMNI_LONG, OMNI_DOUBLE>;

template class SumAggregator<OMNI_LONG, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_LONG, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_LONG, OMNI_VARCHAR>;

template class SumAggregator<OMNI_LONG, OMNI_CONTAINER>;

template class SumAggregator<OMNI_DOUBLE, OMNI_SHORT>;

template class SumAggregator<OMNI_DOUBLE, OMNI_INT>;

template class SumAggregator<OMNI_DOUBLE, OMNI_LONG>;

template class SumAggregator<OMNI_DOUBLE, OMNI_DOUBLE>;

template class SumAggregator<OMNI_DOUBLE, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_DOUBLE, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_DOUBLE, OMNI_VARCHAR>;

template class SumAggregator<OMNI_DOUBLE, OMNI_CONTAINER>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_SHORT>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_INT>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_LONG>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_DOUBLE>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_VARCHAR>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_CONTAINER>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_SHORT>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_INT>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_LONG>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_DOUBLE>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_VARCHAR>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_CONTAINER>;

template class SumAggregator<OMNI_VARCHAR, OMNI_SHORT>;

template class SumAggregator<OMNI_VARCHAR, OMNI_INT>;

template class SumAggregator<OMNI_VARCHAR, OMNI_LONG>;

template class SumAggregator<OMNI_VARCHAR, OMNI_DOUBLE>;

template class SumAggregator<OMNI_VARCHAR, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_VARCHAR, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_VARCHAR, OMNI_VARCHAR>;

template class SumAggregator<OMNI_VARCHAR, OMNI_CONTAINER>;

template class SumAggregator<OMNI_CONTAINER, OMNI_SHORT>;

template class SumAggregator<OMNI_CONTAINER, OMNI_INT>;

template class SumAggregator<OMNI_CONTAINER, OMNI_LONG>;

template class SumAggregator<OMNI_CONTAINER, OMNI_DOUBLE>;

template class SumAggregator<OMNI_CONTAINER, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_CONTAINER, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_CONTAINER, OMNI_VARCHAR>;

template class SumAggregator<OMNI_CONTAINER, OMNI_CONTAINER>;
}
}
