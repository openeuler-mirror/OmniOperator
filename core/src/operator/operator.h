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
#include "vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
class Operator {
public:
    Operator()
        : sourceTypes(nullptr),
          vecAllocator(vec::GetProcessGlobalVecAllocator()),
          context(nullptr),
          status(OMNI_STATUS_NORMAL)
    {}

    virtual ~Operator() {}

    virtual int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) = 0;

    virtual int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &data)
    {}

    virtual int32_t GetOutput(omniruntime::vec::VectorBatch **result)
    {
        std::vector<omniruntime::vec::VectorBatch *> outputs;
        int32_t resultCode = GetOutput(outputs);
        if (outputs.size() == 1) {
            *result = outputs[0];
            outputs.clear();
        } else if (outputs.size() > 1) {
            VectorHelper::FreeVecBatches(outputs);
            throw OmniException("OPERATOR_RUNTIME_ERROR", "output multiple batch at once");
        }
        return resultCode;
    }

    static void DeleteOperator(Operator *op)
    {
        op->Close();
        delete op;
    }

    OmniStatus GetStatus()
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
    OmniStatus status;
};
}
}
#endif