/*
* Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
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
        : SumFlatIMAggregator<IN_ID, OUT_ID>(OMNI_AGGREGATION_TYPE_AVG, inputTypes, outputTypes,
                                             channels, inputRaw, outputPartial, isOverflowAsNull)
{}

~AverageFlatIMAggregator() override {}

static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
                                          std::vector<int32_t> &channels, bool inRaw, bool outPartial, bool isOverflowAsNull)
{
    if constexpr (!(IN_ID == OMNI_SHORT || IN_ID == OMNI_INT
                    || IN_ID == OMNI_LONG || IN_ID == OMNI_DOUBLE
                    || IN_ID == OMNI_DECIMAL64)) {
        LogError("Error in average flatIM  aggregator: Unsupported input type %s",
                 TypeUtil::TypeToStringLog(IN_ID).c_str());
        return nullptr;
    } else if constexpr (!(OUT_ID == OMNI_DOUBLE)) {
        LogError("Error in average flatIM  aggregator: Unsupported output type %s",
                 TypeUtil::TypeToStringLog(OUT_ID).c_str());
        return nullptr;
    }

    return std::unique_ptr<AverageAggregator<IN_ID, OUT_ID>>(
            new AverageAggregator<IN_ID, OUT_ID>(inputTypes, outputTypes, channels, inRaw, outPartial, isOverflowAsNull));
}


//    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
//    {
//        BaseVector *vector = vectorBatch->Get(SumFlatIMAggregator<IN_ID, OUT_ID>::channels[0]);
//        if (vector->IsNull(rowIndex)) {
//            return;
//        }
//
//        if (state.val == nullptr) {
//            this->InitiateGroup(state, vectorBatch, rowIndex);
//            return;
//        }
//
//        if (SumFlatIMAggregator<IN_ID, OUT_ID>::inputRaw) {
//            auto currentVal = static_cast<ResultType *>(state.val);
//            if (vector->GetEncoding() == OMNI_DICTIONARY) {
//                *reinterpret_cast<ResultType *>(state.val) =
//                    (static_cast<DicVector *>(vector))->GetValue(rowIndex) + *currentVal;
//            } else {
//                *reinterpret_cast<ResultType *>(state.val) =
//                    (static_cast<RawInputVectorType *>(vector))->GetValue(rowIndex) + *currentVal;
//            }
//            ++state.count;
//        } else {
//            double avgVal;
//            avgVal = static_cast<Vector<double> *>(vector)->GetValue(rowIndex);
//            auto avgCountVector = vectorBatch->Get(SumFlatIMAggregator<IN_ID, OUT_ID>::channels[1]);
//
//            int64_t avgCnt;
//            avgCnt = static_cast<Vector<int64_t> *>(avgCountVector)->GetValue(rowIndex);
//
//            auto currentVal = static_cast<ResultType *>(state.val);
//            state.count += avgCnt;
//            *currentVal += avgVal;
//        }
//    }

void ProcessSingleInternal(AggregateState &state, BaseVector *vector,
                           const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap)
{
    if (AverageFlatIMAggregator<IN_ID,OUT_ID>::inputRaw) {
        SumFlatIMAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(state, vector, rowOffset, rowCount, nullMap, indexMap);
    } else {
        using InType = double;
        auto *res = reinterpret_cast<ResultType *>(state.val);
        // when input is not raw, vector is container with <double, long> columns for <sum, count>

        auto *sumVector = reinterpret_cast<RawInputVectorType *>(SumFlatIMAggregator<IN_ID,OUT_ID>::curVectorBatch->Get(
                SumFlatIMAggregator<IN_ID,OUT_ID>::channels[0]));

        auto *cntVector = reinterpret_cast<Vector<int64_t> *>(SumFlatIMAggregator<IN_ID,OUT_ID>::curVectorBatch->Get(
                SumFlatIMAggregator<IN_ID,OUT_ID>::channels[1]));

        // no dict in Vector<T> when input is not raw
        auto *ptr = reinterpret_cast<double *>(GetValuesFromVector<IN_ID>(sumVector));
        auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(cntVector));
        ptr += rowOffset;
        cntPtr += rowOffset;
        if (nullMap == nullptr) {
            AddAvg<InType, ResultType, SumOp<InType, ResultType>>(res, state.count, ptr, cntPtr, rowCount);
        } else {
            AddConditionalAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(res, state.count,
                                                                                               ptr, cntPtr, rowCount, nullMap);

        }
    }
}

void ProcessGroupInternal(std::vector<AggregateState *> &rowStates,
                          const size_t aggIdx, BaseVector *vector, const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap)
{
    if (AverageFlatIMAggregator<IN_ID,OUT_ID>::inputRaw) {
        SumFlatIMAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(rowStates, aggIdx, vector, rowOffset, nullMap, indexMap);
    } else {
        using InType = double;
        // when input is not raw, vector is <doubleVector, longVector> columns for <sum, count>
        auto *sumVector = reinterpret_cast<RawInputVectorType *>(SumFlatIMAggregator<IN_ID,OUT_ID>::curVectorBatch->Get(
                SumFlatIMAggregator<IN_ID,OUT_ID>::channels[0]));

        auto *cntVector = reinterpret_cast<RawInputVectorType *>(SumFlatIMAggregator<IN_ID,OUT_ID>::curVectorBatch->Get(
                SumFlatIMAggregator<IN_ID,OUT_ID>::channels[1]));

        auto *ptr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(sumVector));
        auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(cntVector));
        ptr += rowOffset;
        cntPtr += rowOffset;
        if (nullMap == nullptr) {
            AddUseRowIndexAvg<InType, ResultType, SumOp<InType , ResultType>>(rowStates, aggIdx, ptr, cntPtr);
        } else {
            // Reza: can we use customize float operation similar to sumConditionalFloat
            AddConditionalUseRowIndexAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(rowStates,
                                                                                                          aggIdx, ptr, cntPtr, nullMap);
        }
    }
}

void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
{
//        BaseVector *vector = vectorBatch->Get(SumFlatIMAggregator<IN_ID, OUT_ID>::channels[0]);
//        if (vector->IsNull(rowIndex)) {
//            return;
//        }
//
//        // for partial aggregation
//        if constexpr (SumFlatIMAggregator<IN_ID, OUT_ID>::inputRaw) {
//            auto ptr = SumFlatIMAggregator<IN_ID, OUT_ID>::executionContext->GetArena()->Allocate(sizeof(ResultType));
//            if (vector->GetEncoding() == OMNI_DICTIONARY) {
//                auto rowVal = (static_cast<DicVector *>(vector))->GetValue(rowIndex);
//                *reinterpret_cast<ResultType *>(ptr) = rowVal;
//            } else {
//                auto rowVal = (static_cast<FixedVector *>(vector))->GetValue(rowIndex);
//                *reinterpret_cast<ResultType *>(ptr) = rowVal;
//            }
//            state.val = ptr;
//            state.count = 1;
//        } else {
//            double avgVal;
//            if (vector->GetEncoding() == OMNI_DICTIONARY) {
//                avgVal = static_cast<Vector<DictionaryContainer<double>> *>(vector)->GetValue(rowIndex);
//            } else {
//                avgVal = static_cast<Vector<double> *>(vector)->GetValue(rowIndex);
//            }
//
//            auto avgCountVector = vectorBatch->Get(SumFlatIMAggregator<IN_ID, OUT_ID>::channels[1]);
//            int64_t avgCnt;
//            if (avgCountVector->GetEncoding() == OMNI_DICTIONARY) {
//                avgCnt = static_cast<Vector<DictionaryContainer<int64_t>> *>(avgCountVector)->GetValue(rowIndex);
//            } else {
//                avgCnt = static_cast<Vector<int64_t> *>(avgCountVector)->GetValue(rowIndex);
//            }
//            auto ptr = SumFlatIMAggregator<IN_ID, OUT_ID>::executionContext->GetArena()->Allocate(sizeof(ResultType));
//            *reinterpret_cast<ResultType *>(ptr) = avgVal;
//            state.val = ptr;
//            state.count = avgCnt;
//        }
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
        } else if (state.count < 0) {
            // overflow
            this->SetNullOrThrowException(avgValVector,rowIndex,"avg overflow when output partial stage");
            avgCountVector->SetValue(rowIndex, state.count);
            return;
        }

        avgValVector->SetValue(rowIndex, *static_cast<ResultType *>(state.val));
        avgCountVector->SetValue(rowIndex, state.count);
    } else {
        auto avgValVector = static_cast<Vector<double> *>(vectors[0]);
        // overflow
        if (state.count < 0) {
            avgValVector->SetNull(rowIndex);
            return;
        } else if (state.count == 0) {
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
