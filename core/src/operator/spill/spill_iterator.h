/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: spill unit iterator
 */

#ifndef OMNI_RUNTIME_SPILL_ITERATOR_H
#define OMNI_RUNTIME_SPILL_ITERATOR_H

#include <cstddef>
#include "spill_unit.h"
#include "vector/vector_batch.h"

namespace omniruntime {
namespace op {
class SpillUnitIter {
public:
    SpillUnitIter() = default;

    virtual ~SpillUnitIter() = default;

    virtual bool HasNext() = 0;

    virtual SpillUnit *Next() = 0;
};

class VectorBatchUnitIter : public SpillUnitIter {
public:
    VectorBatchUnitIter() = default;

    explicit VectorBatchUnitIter(omniruntime::vec::VectorBatch *vectorBatch) : size(1)
    {
        vectorBatches.push_back(vectorBatch);
    }

    explicit VectorBatchUnitIter(std::vector<omniruntime::vec::VectorBatch *> &vectorBatches)
        : vectorBatches(vectorBatches), size(vectorBatches.size())
    {}

    ~VectorBatchUnitIter() override
    {
        delete result;
    }

    bool HasNext() override
    {
        return (offset < size);
    }

    VectorBatchUnit *Next() override
    {
        result->SetVectorBatch(vectorBatches[offset++]);
        return result;
    }

private:
    std::vector<omniruntime::vec::VectorBatch *> vectorBatches;
    VectorBatchUnit *result = new VectorBatchUnit();
    size_t offset = 0;
    size_t size = 0;
};
} // end of namespace op
} // end of namespace omniruntime
#endif // OMNI_RUNTIME_SPILL_ITERATOR_H
