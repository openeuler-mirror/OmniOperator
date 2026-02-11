/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
 * Description: approx_count_distinct aggregate (HyperLogLog + boolean special case).
 *
 * Two-phase semantics:
 * - Partial: inputRaw=true, outputPartial=true; input is raw column, output is serialized state (VARBINARY), one aggregate.
 * - Final:   inputRaw=false, outputPartial=false; input is VARBINARY, output is merged BIGINT, another aggregate.
 * - Single-stage: inputRaw=true, outputPartial=false; input is raw column, output is BIGINT directly.
 */
#ifndef OMNI_RUNTIME_APPROX_COUNT_DISTINCT_AGGREGATOR_H
#define OMNI_RUNTIME_APPROX_COUNT_DISTINCT_AGGREGATOR_H

#include "typed_aggregator.h"
#include "codegen/hyperloglog.h"
#include "operator/hash_util.h"
#include "operator/aggregation/vector_getter.h"

namespace omniruntime {
namespace op {

constexpr int32_t kApproxCountDistinctMaxSerializedSize = 65536;

/**
 * State for one group: pointer to serialized HLL/boolean buffer and its length.
 */
#pragma pack(push, 1)
struct ApproxCountDistinctAggState {
    int64_t serializePtr;
    int32_t len;

    static const ApproxCountDistinctAggState *ConstCastState(const AggregateState *state)
    {
        return reinterpret_cast<const ApproxCountDistinctAggState *>(state);
    }

    static ApproxCountDistinctAggState *CastState(AggregateState *state)
    {
        return reinterpret_cast<ApproxCountDistinctAggState *>(state);
    }
};
#pragma pack(pop)

template <DataTypeId IN_ID, DataTypeId OUT_ID>
class ApproxCountDistinctAggregator : public TypedAggregator {
    using InType = typename AggNativeAndVectorType<IN_ID>::type;
    using OutType = typename AggNativeAndVectorType<OUT_ID>::type;

public:
    ~ApproxCountDistinctAggregator() override;

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;

    std::vector<DataTypePtr> GetSpillType() override;
    size_t GetStateSize() override { return sizeof(ApproxCountDistinctAggState); }

    void InitState(AggregateState *state) override;
    void InitStates(std::vector<AggregateState *> &groupStates) override;

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull);

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override;
    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

protected:
    ApproxCountDistinctAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull);

    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
        const std::shared_ptr<NullsHelper> nullMap) override;

private:
    void ProcessPartialRaw(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap);
    void ProcessFinalMerge(AggregateState *state, BaseVector *vector, const int32_t rowOffset, const int32_t rowCount,
        const std::shared_ptr<NullsHelper> nullMap);

    std::vector<int64_t> stateBufferPtrs_;
    bool isBooleanInput_;
    int8_t indexBitLength_;       // from kDefaultHllIndexBitLength or from 2nd column (max standard error)
    bool hasMaxErrorFromColumn_;  // true when 2nd column present and we have read it once
};

}  // namespace op
}  // namespace omniruntime

#endif  // OMNI_RUNTIME_APPROX_COUNT_DISTINCT_AGGREGATOR_H
