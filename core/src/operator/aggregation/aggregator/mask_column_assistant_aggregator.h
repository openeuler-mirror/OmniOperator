/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Count aggregate
 */
#ifndef OMNI_MASK_COLUMN_ASSISTANT_AGGREGATOR_H
#define OMNI_MASK_COLUMN_ASSISTANT_AGGREGATOR_H

#include "aggregator.h"

namespace omniruntime {
namespace op {
class MaskColAggregator : public Aggregator {
public:
    MaskColAggregator(int32_t maskColumnId, std::unique_ptr<Aggregator> realAggregator)
        : Aggregator(realAggregator->GetType(), realAggregator->GetInputType(), realAggregator->GetOutputType(),
        realAggregator->GetInputChannel(), realAggregator->IsInputRaw(), realAggregator->IsOutputPartial()),
          maskColumnId(maskColumnId),
          realAggregator(std::move(realAggregator))
    {}

    ~MaskColAggregator() override = default;

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        auto rowCount = vectorBatch->GetRowCount();
        for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            ProcessGroup(state, vectorBatch, rowIdx);
        }
    }
#endif

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *maskVector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(maskColumnId), rowIndex, offset);
        if (maskVector->IsValueNull(offset)) {
            return;
        }

        if (static_cast<BooleanVector *>(maskVector)->GetValue(offset)) {
            realAggregator->ProcessGroup(state, vectorBatch, rowIndex);
        }
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *maskVector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(maskColumnId), rowIndex, offset);
        if (maskVector->IsValueNull(offset)) {
            return;
        }

        if (static_cast<BooleanVector *>(maskVector)->GetValue(offset)) {
            realAggregator->InitiateGroup(state, vectorBatch, rowIndex);
        }
    }

    void ExtractValue(AggregateState &state, Vector *vector, int32_t rowIndex) override
    {
        realAggregator->ExtractValue(state, vector, rowIndex);
    }

    bool IsInputRaw() const override
    {
        return realAggregator->IsInputRaw();
    }

    bool IsOutputPartial() const override
    {
        return realAggregator->IsOutputPartial();
    }

    FunctionType GetType() const override
    {
        return realAggregator->GetType();
    }

    const DataTypePtr &GetInputType() const override
    {
        return realAggregator->GetInputType();
    }

    const DataTypePtr &GetOutputType() const override
    {
        return realAggregator->GetOutputType();
    }

    int32_t GetInputChannel() const override
    {
        return realAggregator->GetInputChannel();
    }

private:
    int32_t maskColumnId;
    std::unique_ptr<Aggregator> realAggregator;
};
}
}

#endif // OMNI_MASK_COLUMN_ASSISTANT_AGGREGATOR_H
