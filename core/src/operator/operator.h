/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_OPERATOR_H__
#define __OMNI_OPERATOR_H__

#include <cstdint>
#include <vector>

#include "execution_context.h"
#include "status.h"
#include "vector/vector_batch.h"
#include "vector/vector_allocator.h"

namespace omniruntime {
namespace op {
class Operator {
public:
    Operator() : sourceTypes(nullptr), vecAllocator(vec::GetProcessGlobalVecAllocator()), context(nullptr), status(0) {}

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

    void SetStatus(OmniStatus omniStatus)
    {
        this->status = omniStatus;
    };

    virtual OmniStatus Init()
    {
        return OMNI_STATUS_NORMAL;
    }

    virtual OmniStatus Close()
    {
        return OMNI_STATUS_NORMAL;
    }

    void SetVecAllocator(vec::VectorAllocator *vectorAllocator)
    {
        this->vecAllocator = vectorAllocator;
        if (context != nullptr) {
            context->GetArena()->SetAllocator(vectorAllocator);
        }
    }

    vec::VectorAllocator ALWAYS_INLINE *GetVecAllocator() const
    {
        return vecAllocator;
    }

protected:
    int32_t *sourceTypes;
    vec::VectorAllocator *vecAllocator;
    ExecutionContext *context;

private:
    int32_t status;
};
}
}
#endif