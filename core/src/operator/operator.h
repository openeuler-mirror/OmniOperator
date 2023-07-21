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
#include "vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
class Operator {
public:
    Operator()
        : sourceTypes(nullptr),
          context(nullptr),
          status(OMNI_STATUS_NORMAL)
    {}

    virtual ~Operator() {}

    virtual int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) = 0;

    virtual int32_t GetOutput(omniruntime::vec::VectorBatch **result) = 0;

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

protected:
    int32_t *sourceTypes;
    ExecutionContext *context;

private:
    OmniStatus status;
};
}
}
#endif