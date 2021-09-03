/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_OPERATOR_H__
#define __OMNI_OPERATOR_H__

#include "../vector/vector_batch.h"
#include "status.h"
#include <vector>

namespace omniruntime {
namespace op {
    class Operator {
    public:
        Operator() : status(0) {}

        virtual ~Operator() {}

        virtual int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) = 0;

        virtual int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &data) = 0;

        virtual const int32_t *GetSourceTypes()
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

        virtual OmniStatus Init()
        {
            return OMNI_STATUS_NORMAL;
        }

        virtual OmniStatus Close()
        {
            return OMNI_STATUS_NORMAL;
        }

    protected:
        int32_t* sourceTypes;
    private:
        int status;
    };
}
}
#endif