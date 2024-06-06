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

public:
    AverageFlatIMAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : SumFlatIMAggregator<IN_ID, OUT_ID>(OMNI_AGGREGATION_TYPE_AVG, inputTypes, outputTypes, channels, inputRaw,
        outputPartial, isOverflowAsNull)
    {}

    ~AverageFlatIMAggregator() override = default;

    void ProcessSingleInternal(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const uint8_t *nullMap)
    {
        if (this->inputRaw) {
            SumFlatIMAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(state, vector, rowOffset, rowCount, nullMap);
        } else {
            using InType = double;

            // when input is not raw, vector is container with <double, long> columns for <sum, count>
            auto *sumVector = this->curVectorBatch->Get(this->channels[0]);
            auto *cntVector = this->curVectorBatch->Get(this->channels[1]);

            // no dict in Vector<T> when input is not raw
            auto *ptr = reinterpret_cast<double *>(GetValuesFromVector<IN_ID>(sumVector));
            auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(cntVector));
            ptr += rowOffset;
            cntPtr += rowOffset;
            if (nullMap == nullptr) {
                if constexpr (std::is_floating_point_v<ResultType>) {
                    AddAvg<InType, ResultType, SumOp<InType, ResultType, false>>(
                        reinterpret_cast<ResultType *>(state.val), state.count, ptr, cntPtr, rowCount);
                } else {
                    AddAvg<InType, ResultType, SumOp<InType, ResultType, false>>(
                        reinterpret_cast<ResultType *>(&state.val), state.count, ptr, cntPtr, rowCount);
                }
            } else {
                if constexpr (std::is_floating_point_v<ResultType>) {
                    AddConditionalAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false, false>>(
                        reinterpret_cast<ResultType *>(state.val), state.count, ptr, cntPtr, rowCount, nullMap);
                } else {
                    AddConditionalAvg<InType, ResultType, SumConditionalOp<InType, ResultType, false, false>>(
                        reinterpret_cast<ResultType *>(&state.val), state.count, ptr, cntPtr, rowCount, nullMap);
                }
            }
        }
    }

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *vector,
        const int32_t rowOffset, const uint8_t *nullMap)
    {
        if (this->inputRaw) {
            SumFlatIMAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(rowStates, aggIdx, vector, rowOffset, nullMap);
        } else {
            using InType = double;
            // when input is not raw, vector is <doubleVector, longVector> columns for <sum, count>
            auto *sumVector = this->curVectorBatch->Get(this->channels[0]);
            auto *cntVector = this->curVectorBatch->Get(this->channels[1]);

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

    std::vector<DataTypePtr> GetSpillType() override
    {
        std::vector<DataTypePtr> spillTypes;
        spillTypes.emplace_back(std::make_shared<DataType>(OMNI_DOUBLE));
        spillTypes.emplace_back(std::make_shared<DataType>(OMNI_LONG));
        return spillTypes;
    }

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, const size_t aggIdx,
        int32_t &vectorIndex) override
    {
        auto firstVecIdx = vectorIndex++;
        auto secondVecIdx = vectorIndex++;
        for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            auto &row = unspillRows[rowIdx];
            auto batch = row.batch;
            auto index = row.rowIdx;
            auto sumVector = static_cast<Vector<ResultType> *>(batch->Get(firstVecIdx));
            if (!sumVector->IsNull(index)) {
                auto value = sumVector->GetValue(index);
                auto countVector = static_cast<Vector<int64_t> *>(batch->Get(secondVecIdx));
                auto count = countVector->GetValue(index);
                auto &state = row.state[aggIdx];
                if constexpr (std::is_floating_point_v<ResultType>) {
                    SumOp<ResultType, ResultType, false>(reinterpret_cast<ResultType *>(state.val), state.count, value,
                        count);
                } else {
                    SumOp<ResultType, ResultType, false>(reinterpret_cast<ResultType *>(&state.val), state.count, value,
                        count);
                }
            }
        }
    }

    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, const size_t aggIdx,
        std::vector<BaseVector *> &vectors) override
    {
        auto avgValVector = static_cast<Vector<ResultType> *>(vectors[0]);
        auto avgCountVector = static_cast<Vector<int64_t> *>(vectors[1]);

        auto rowCount = static_cast<int32_t>(groupStates.size());
        for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            auto &state = groupStates[rowIndex][aggIdx];
            auto count = state.count;
            if (count == 0) {
                // set null for empty group(all rows are NULL) when spill to ensure skip empty group when unspill
                avgValVector->SetNull(rowIndex);
            } else {
                if constexpr (std::is_floating_point_v<ResultType>) {
                    avgValVector->SetValue(rowIndex, *reinterpret_cast<ResultType *>(state.val));
                } else {
                    avgValVector->SetValue(rowIndex, static_cast<ResultType>(state.val));
                }
                avgCountVector->SetValue(rowIndex, count);
            }
        }
    }

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        if (this->outputPartial) {
            auto avgValVector = static_cast<Vector<double> *>(vectors[0]);
            auto avgCountVector = static_cast<Vector<int64_t> *>(vectors[1]);

            if (state.count == 0) {
                // all input are nulls, return 0
                avgValVector->SetValue(rowIndex, 0);
                avgCountVector->SetValue(rowIndex, 0);
                return;
            }

            if constexpr (std::is_floating_point_v<ResultType>) {
                avgValVector->SetValue(rowIndex, *reinterpret_cast<ResultType *>(state.val));
            } else {
                avgValVector->SetValue(rowIndex, static_cast<ResultType>(state.val));
            }
            avgCountVector->SetValue(rowIndex, state.count);
        } else {
            auto avgValVector = static_cast<Vector<double> *>(vectors[0]);

            if (state.count == 0) {
                // only contains null
                avgValVector->SetNull(rowIndex);
                return;
            }

            ResultType currentVal;
            if constexpr (std::is_floating_point_v<ResultType>) {
                currentVal = *(reinterpret_cast<ResultType *>(state.val));
            } else {
                currentVal = static_cast<ResultType>(state.val);
            }
            auto result = currentVal / state.count;
            avgValVector->SetValue(rowIndex, result);
        }
    }

    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, const size_t aggIdx,
        std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount) override
    {
        if (this->outputPartial) {
            auto avgValVector = static_cast<Vector<double> *>(vectors[0]);
            auto avgCountVector = static_cast<Vector<int64_t> *>(vectors[1]);
            for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                auto &state = groupStates[rowIndex][aggIdx];
                if (state.count == 0) {
                    // all input are nulls, return 0
                    avgValVector->SetValue(rowIndex, 0);
                    avgCountVector->SetValue(rowIndex, 0);
                    continue;
                }
                if constexpr (std::is_floating_point_v<ResultType>) {
                    avgValVector->SetValue(rowIndex, *reinterpret_cast<ResultType *>(state.val));
                } else {
                    avgValVector->SetValue(rowIndex, static_cast<ResultType>(state.val));
                }
                avgCountVector->SetValue(rowIndex, state.count);
            }
        } else {
            auto avgValVector = static_cast<Vector<double> *>(vectors[0]);
            for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                auto &state = groupStates[rowIndex][aggIdx];
                if (state.count == 0) {
                    // only contains null
                    avgValVector->SetNull(rowIndex);
                    continue;
                }

                ResultType currentVal;
                if constexpr (std::is_floating_point_v<ResultType>) {
                    currentVal = *(reinterpret_cast<ResultType *>(state.val));
                } else {
                    currentVal = static_cast<ResultType>(state.val);
                }
                auto result = currentVal / state.count;
                avgValVector->SetValue(rowIndex, result);
            }
        }
    }
};
}
}
#endif // OMNI_RUNTIME_AVERAGE_FLAT_IM_AGGREGATOR_H
