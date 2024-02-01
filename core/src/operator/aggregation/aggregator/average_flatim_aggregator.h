/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * Description: average aggregate for intermedia data vector are multi vectors
 *
 *
 */
#ifndef OMNI_RUNTIME_AVERAGE_FLAT_IM_AGGREGATOR_H
#define OMNI_RUNTIME_AVERAGE_FLAT_IM_AGGREGATOR_H

#include "sum_flatim_aggregator.h"

namespace omniruntime {
namespace op {
template <DataTypeId IN_ID, DataTypeId OUT_ID = OMNI_DOUBLE>
class AverageFlatIMAggregator : public SumFlatIMAggregator<IN_ID, OUT_ID> {
    using RawInputType = typename AggNativeAndVectorType<IN_ID>::type;
    using ResultType = typename AggNativeAndVectorType<OUT_ID>::type;
    using RawInputVectorType = Vector<RawInputType>;
    using DicVector = Vector<DictionaryContainer<RawInputVectorType>>;

public:
    AverageFlatIMAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : SumFlatIMAggregator<IN_ID, OUT_ID>(OMNI_AGGREGATION_TYPE_AVG, inputTypes, outputTypes, channels, inputRaw,
        outputPartial, isOverflowAsNull)
    {}

    ~AverageFlatIMAggregator() override {}
    void ProcessSingleInternal(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const uint8_t *nullMap)
    {
        if (AverageFlatIMAggregator<IN_ID, OUT_ID>::inputRaw) {
            SumFlatIMAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(state, vector, rowOffset, rowCount, nullMap);
        } else {
            using InType = double;
            auto *res = reinterpret_cast<ResultType *>(state.val);
            // when input is not raw, vector is container with <double, long> columns for <sum, count>

            auto *sumVector =
                reinterpret_cast<RawInputVectorType *>(SumFlatIMAggregator<IN_ID, OUT_ID>::curVectorBatch->Get(
                    SumFlatIMAggregator<IN_ID, OUT_ID>::channels[0]));

            auto *cntVector =
                reinterpret_cast<Vector<int64_t> *>(SumFlatIMAggregator<IN_ID, OUT_ID>::curVectorBatch->Get(
                    SumFlatIMAggregator<IN_ID, OUT_ID>::channels[1]));

            // no dict in Vector<T> when input is not raw
            auto *ptr = reinterpret_cast<double *>(GetValuesFromVector<IN_ID>(sumVector));
            auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(cntVector));
            ptr += rowOffset;
            cntPtr += rowOffset;
            if (nullMap == nullptr) {
                AddAvg<InType, ResultType, SumOp<InType, ResultType, false>>(res, state.count, ptr, cntPtr, rowCount);
            } else {
                AddConditionalAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false, false>>(res,
                    state.count, ptr, cntPtr, rowCount, nullMap);
            }
        }
    }

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *vector,
        const int32_t rowOffset, const uint8_t *nullMap)
    {
        if (AverageFlatIMAggregator<IN_ID, OUT_ID>::inputRaw) {
            SumFlatIMAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(rowStates, aggIdx, vector, rowOffset, nullMap);
        } else {
            using InType = double;
            // when input is not raw, vector is <doubleVector, longVector> columns for <sum, count>
            auto *sumVector =
                reinterpret_cast<RawInputVectorType *>(SumFlatIMAggregator<IN_ID, OUT_ID>::curVectorBatch->Get(
                    SumFlatIMAggregator<IN_ID, OUT_ID>::channels[0]));

            auto *cntVector =
                reinterpret_cast<RawInputVectorType *>(SumFlatIMAggregator<IN_ID, OUT_ID>::curVectorBatch->Get(
                    SumFlatIMAggregator<IN_ID, OUT_ID>::channels[1]));

            auto *ptr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(sumVector));
            auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(cntVector));
            ptr += rowOffset;
            cntPtr += rowOffset;
            if (nullMap == nullptr) {
                AddUseRowIndexAvg<InType, ResultType, SumOp<InType, ResultType, false>>(rowStates, aggIdx, ptr, cntPtr);
            } else {
                // Reza: can we use customize float operation similar to sumConditionalFloat
                AddConditionalUseRowIndexAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false, false>>(
                    rowStates, aggIdx, ptr, cntPtr, nullMap);
            }
        }
    }

    void GetSpillType(std::vector<DataTypeId>& spillTypes) override
    {
        spillTypes.push_back(OMNI_DOUBLE);
        spillTypes.push_back(OMNI_LONG);
    }

    void ProcessGroupAfterSpill(AggregateState &state, VectorBatch *vectorBatch, int32_t &vectorIndex, int32_t rowIdx) override
    {
        if (state.count >= 0) {
            auto vectorPtr = vectorBatch->Get(vectorIndex++);
            auto *ptr = reinterpret_cast<ResultType *>(GetValuesFromVector<OUT_ID>(vectorPtr));
            auto vectorCnt = vectorBatch->Get(vectorIndex++);
            auto *cnt = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(vectorCnt));
            ptr = (ResultType *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
            cnt = (int64_t *)__builtin_assume_aligned(cnt, ARRAY_ALIGNMENT);

            int64_t sumCnt = cnt[rowIdx];
            if (sumCnt > 0 && !vectorPtr->IsNull(rowIdx)) {
                SumOp<ResultType, ResultType, false>(reinterpret_cast<ResultType *>(state.val), state.count,
                    ptr[rowIdx], sumCnt);
            } else {
                state.count = sumCnt;
            }
        } else {
            state.count = -1;
        }
    }

    void ExtractSpillValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        auto avgValVector = static_cast<Vector<ResultType> *>(vectors[0]);
        auto avgCountVector = static_cast<Vector<int64_t> *>(vectors[1]);
        if (state.count == 0) {
            avgValVector->SetNull(rowIndex);
            avgCountVector->SetValue(rowIndex, 0);
            return;
        }

        avgValVector->SetValue(rowIndex, *static_cast<ResultType *>(state.val));
        avgCountVector->SetValue(rowIndex, state.count);
    }

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        if (SumFlatIMAggregator<IN_ID, OUT_ID>::outputPartial) {
            auto avgValVector = static_cast<Vector<double> *>(vectors[0]);
            auto avgCountVector = static_cast<Vector<int64_t> *>(vectors[1]);

            if (state.count == 0) {
                // all input are nulls, return 0
                avgValVector->SetValue(rowIndex, 0);
                avgCountVector->SetValue(rowIndex, 0);
                return;
            }

            avgValVector->SetValue(rowIndex, *static_cast<ResultType *>(state.val));
            avgCountVector->SetValue(rowIndex, state.count);
        } else {
            auto avgValVector = static_cast<Vector<double> *>(vectors[0]);

            if (state.count == 0) {
                // only contains null
                avgValVector->SetNull(rowIndex);
                return;
            }

            auto currentVal = *(static_cast<ResultType *>(state.val));
            auto result = currentVal / state.count;
            avgValVector->SetValue(rowIndex, result);
        }
    }
};
}
}
#endif // OMNI_RUNTIME_AVERAGE_FLAT_IM_AGGREGATOR_H
