/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: regr_avgy(y, x) - full aggregator (raw / merge / partial / final / spill) in this pair of files.
 */
#ifndef OMNI_RUNTIME_REGR_AVGY_AGGREGATOR_H
#define OMNI_RUNTIME_REGR_AVGY_AGGREGATOR_H

#include "operator/aggregation/aggregator/typed_aggregator.h"
#include "operator/aggregation/aggregator/regr/regr_state.h"
#include "type/data_types.h"

#include <cstdint>
#include <memory>
#include <vector>

namespace omniruntime::op {

class RegrAvgYAggregator : public TypedAggregator {
public:
    RegrAvgYAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull);
    ~RegrAvgYAggregator() override = default;

    size_t GetStateSize() override;
    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override;
    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector,
        const int32_t rowOffset, const std::shared_ptr<NullsHelper> nullMap) override;
    void InitState(AggregateState *state) override;
    void InitStates(std::vector<AggregateState *> &groupStates) override;
    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors,
        const int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;
    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount,
        int32_t &vectorIndex) override;
    void AlignAggSchema(VectorBatch *result, VectorBatch *inputVecBatch) override;
    void AlignAggSchemaWithFilter(VectorBatch *result, VectorBatch *inputVecBatch,
        const int32_t filterIndex) override;
    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;
    std::vector<DataTypePtr> GetSpillType() override;
};

} // namespace omniruntime::op

#endif // OMNI_RUNTIME_REGR_AVGY_AGGREGATOR_H
