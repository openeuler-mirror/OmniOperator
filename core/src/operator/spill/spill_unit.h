/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: spill unit
 */

#ifndef OMNI_RUNTIME_SPILL_UNIT_H
#define OMNI_RUNTIME_SPILL_UNIT_H

#include "vector/vector_batch.h"

namespace omniruntime {
namespace op {
class SpillUnit {
public:
    SpillUnit() = default;

    virtual ~SpillUnit() = default;
};

class VectorBatchUnit : public SpillUnit {
public:
    VectorBatchUnit() = default;

    explicit VectorBatchUnit(omniruntime::vec::VectorBatch *vectorBatch) : vectorBatch(vectorBatch) {}

    ~VectorBatchUnit() override = default;

    void SetVectorBatch(omniruntime::vec::VectorBatch *pVectorBatch)
    {
        this->vectorBatch = pVectorBatch;
    }

    omniruntime::vec::VectorBatch *GetVectorBatch()
    {
        return vectorBatch;
    }

private:
    omniruntime::vec::VectorBatch *vectorBatch = nullptr;
};
}
}
#endif // OMNI_RUNTIME_SPILL_UNIT_H
