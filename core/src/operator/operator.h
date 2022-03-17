/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_OPERATOR_H__
#define __OMNI_OPERATOR_H__

#include "../util/compiler_util.h"
#include "../vector/vector_batch.h"
#include "../vector/vector_allocator.h"
#include "../vector/vector_allocator_factory.h"
#include "status.h"
#include <vector>

namespace omniruntime {
namespace op {
class Operator {
public:
    Operator() : vecAllocator(vec::VectorAllocatorFactory::GetGlobalAllocator()), status(0) {}

    virtual ~Operator() {}

    virtual int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) = 0;

    virtual int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &data) = 0;

    static void DeleteOperator(Operator *op)
    {
        op->Close();
        delete op;
    }

    int GetStatus()
    {
        return status;
    }

    void SetStatus(OmniStatus status)
    {
        this->status = status;
    };

    virtual OmniStatus Init()
    {
        return OMNI_STATUS_NORMAL;
    }

    virtual OmniStatus Close()
    {
        return OMNI_STATUS_NORMAL;
    }

    void SetVecAllocator(vec::VectorAllocator *vecAllocator)
    {
        this->vecAllocator = vecAllocator;
    }

    vec::VectorAllocator ALWAYS_INLINE *GetVecAllocator() const
    {
        return vecAllocator;
    }

protected:
    int32_t *sourceTypes;
    vec::VectorAllocator *vecAllocator;

private:
    int status;
};
}
}
#endif