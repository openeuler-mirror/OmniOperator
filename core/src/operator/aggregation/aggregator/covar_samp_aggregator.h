/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: CovarSamp aggregate (Partial: 4 values compatible with Spark/Gluten/Velox)
 *   Partial order: c2, count, meanX, meanY (alphabetical, same as Velox)
 *   covar_samp = c2 / (count - 1)
 */
#ifndef OMNI_RUNTIME_COVAR_SAMP_AGGREGATOR_H
#define OMNI_RUNTIME_COVAR_SAMP_AGGREGATOR_H

#include "covar_pop_aggregator.h"

namespace omniruntime {
namespace op {

inline double CovarSampFromState(int64_t n, double, double, double c2) {
    return n < 2 ? 0.0 : (c2 / (n - 1));
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
class CovarSampAggregator : public TypedAggregator {
    using InVector = typename AggNativeAndVectorType<IN_ID>::vector;
    using InType = typename AggNativeAndVectorType<IN_ID>::type;
    using OutVector = typename AggNativeAndVectorType<OUT_ID>::vector;
    using OutType = typename AggNativeAndVectorType<OUT_ID>::type;

public:
    ~CovarSampAggregator() override = default;

    size_t GetStateSize() override { return sizeof(CovarPartialState); }

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;
    void InitState(AggregateState *state) override;
    void InitStates(std::vector<AggregateState *> &groupStates) override;
    std::vector<DataTypePtr> GetSpillType() override;

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull);

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override;
    void AlignAggSchema(VectorBatch *result, VectorBatch *inputVecBatch) override;
    void AlignAggSchemaWithFilter(VectorBatch *result, VectorBatch *inputVecBatch,
        const int32_t filterIndex) override;
    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

protected:
    CovarSampAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull);

    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
        const std::shared_ptr<NullsHelper> nullMap) override;
};
}
}
#endif // OMNI_RUNTIME_COVAR_SAMP_AGGREGATOR_H
