/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_OPERATOR_H__
#define __OMNI_OPERATOR_H__

#include "../vector/vector_batch.h"
#include "status.h"
#include <vector>

const int32_t MAX_VEC_BATCH_SIZE_IN_BYTES = 1024 * 1024;
namespace omniruntime {
namespace op {
    class Operator {
    public:
        Operator() : status(0) {}

        virtual ~Operator() {}

        virtual int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) = 0;

        virtual int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &data) = 0;

        const virtual int32_t *GetSourceTypes()
        {
            return sourceTypes;
        }

        int GetStatus()
        {
            return status;
        }

        void SetStatus(OmniStatus status)
        {
            this->status = status;
        };

        virtual void Close() {}

    protected:
        int32_t* sourceTypes;
    private:
        int status;
    };
}
}
#endif