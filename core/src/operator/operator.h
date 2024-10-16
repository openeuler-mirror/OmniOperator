/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2024. All rights reserved.
 */
#ifndef __OMNI_OPERATOR_H__
#define __OMNI_OPERATOR_H__

#include <cstdint>
#include <vector>

#include "execution_context.h"
#include "status.h"
#include "vector/vector_batch.h"
#include "vector/vector_helper.h"
#include "metrics/metrics.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
class Operator {
public:
    Operator()
        : sourceTypes(nullptr),
          executionContext(std::make_unique<ExecutionContext>()),
          inputVecBatch(nullptr),
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

    virtual uint64_t GetSpilledBytes()
    {
        return 0;
    }

    // Obtains the number of keys in the hashmap object.
    virtual uint64_t GetHashMapUniqueKeys()
    {
        return 0;
    }

    virtual VectorBatch *AlignSchema(VectorBatch *inputVecBatch)
    {
        return inputVecBatch;
    }

    omniruntime::vec::VectorBatch *GetInputVecBatch()
    {
        return inputVecBatch;
    }

    void SetInputVecBatch(vec::VectorBatch *inVecBatch)
    {
        inputVecBatch = inVecBatch;
    }

    // no need to clear memory when exception, so we have to reset
    void ResetInputVecBatch()
    {
        inputVecBatch = nullptr;
    }

protected:
    int32_t *sourceTypes;
    std::unique_ptr<ExecutionContext> executionContext;
    vec::VectorBatch *inputVecBatch = nullptr;

    void UpdateAddInputInfo(int32_t rowCount)
    {
        if (LIKELY(!IsDebugEnable())) {
            return;
        }
        metrics.UpdateAddInputInfo(rowCount, executionContext);
    }

    void UpdateGetOutputInfo(int32_t rowCount)
    {
        if (LIKELY(!IsDebugEnable())) {
            return;
        }
        metrics.UpdateGetOutputInfo(rowCount, executionContext);
    }

    void UpdateSpillFileInfo(uint32_t fileCount)
    {
        if (LIKELY(!IsDebugEnable())) {
            return;
        }
        metrics.UpdateSpillFileInfo(fileCount, executionContext);
    }

    void UpdateSpillTimesInfo()
    {
        if (LIKELY(!IsDebugEnable())) {
            return;
        }
        metrics.UpdateSpillTimesInfo(executionContext);
    }

    void UpdateCloseInfo()
    {
        if (LIKELY(!IsDebugEnable())) {
            return;
        }
        metrics.UpdateCloseInfo(executionContext);
    }

    void SetOperatorName(const std::string &operatorName)
    {
        if (LIKELY(!IsDebugEnable())) {
            return;
        }
        metrics.SetOperatorName(operatorName);
    }

private:
    OmniStatus status;
    Metrics metrics;
};
}
}
#endif