/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#ifndef __OMNI_OPERATOR_FACTORY_H__
#define __OMNI_OPERATOR_FACTORY_H__

#include "operator.h"
#include "jit_context/jit_context.h"

class OperatorFactory {
public:
    OperatorFactory() {}

    virtual ~OperatorFactory() {}

    virtual omniruntime::op::Operator *CreateOperator()
    {
        return nullptr;
    }

    virtual void SetJitContext(JitContext *JitContext)
    {
        jitContext = JitContext;
    }

    virtual JitContext *GetJitContext() const
    {
        return jitContext;
    }

private:
    JitContext *jitContext = nullptr;
};

using OptModule = omniruntime::op::Operator *(*)(OperatorFactory *);
using opt_module = omniruntime::op::Operator *(*)(OperatorFactory *);

#endif // __OMNI_OPERATOR_FACTORY_H__
