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
        : Aggregator(realAggregator->GetType(), realAggregator->GetInputTypes(), realAggregator->GetOutputTypes(),
        realAggregator->GetInputChannels(), realAggregator->IsInputRaw(), realAggregator->IsOutputPartial(),
        realAggregator->IsOverflowAsNull()),
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

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        return realAggregator->CanProcessWithHMPP(state, vectorBatch);
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

    void ExtractValues(AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        realAggregator->ExtractValues(state, vectors, rowIndex);
    }

    bool IsInputRaw() const override
    {
        return realAggregator->IsInputRaw();
    }

    bool IsOutputPartial() const override
    {
        return realAggregator->IsOutputPartial();
    }

    bool IsOverflowAsNull() const
    {
        return realAggregator->IsOverflowAsNull();
    }

    FunctionType GetType() const override
    {
        return realAggregator->GetType();
    }

    const DataTypesPtr &GetInputTypes() const override
    {
        return realAggregator->GetInputTypes();
    }

    const DataTypesPtr &GetOutputTypes() const override
    {
        return realAggregator->GetOutputTypes();
    }

    const std::vector<int32_t> &GetInputChannels() const
    {
        return realAggregator->GetInputChannels();
        ;
    }

private:
    int32_t maskColumnId;
    std::unique_ptr<Aggregator> realAggregator;
};
}
}

#endif // OMNI_MASK_COLUMN_ASSISTANT_AGGREGATOR_H
