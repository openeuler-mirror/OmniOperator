/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
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

    ~MaskColAggregator() override {}

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *maskVector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(maskColumnId), rowIndex, offset);
        bool *maskValues = static_cast<bool *>(maskVector->GetValues());
        if (maskVector->IsValueNull(offset)) {
            return;
        }

        if (maskValues[offset]) {
            realAggregator->ProcessGroup(state, vectorBatch, rowIndex);
        }
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        realAggregator->InitiateGroup(state, vectorBatch, rowIndex);
    }

    void ExtractValue(AggregateState &state, Vector *vector, int32_t rowIndex) override
    {
        realAggregator->ExtractValue(state, vector, rowIndex);
    }

    bool IsInputRaw() const
    {
        return realAggregator->IsInputRaw();
    }

    bool IsOutputPartial() const
    {
        return realAggregator->IsOutputPartial();
    }

    FunctionType GetType() const
    {
        return realAggregator->GetType();
    }

    DataType GetInputType() const
    {
        return realAggregator->GetInputType();
    }

    DataType GetOutputType() const
    {
        return realAggregator->GetOutputType();
    }

    int32_t GetInputChannel() const
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
