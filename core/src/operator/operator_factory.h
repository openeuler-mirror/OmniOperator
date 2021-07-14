//
// Created by root on 5/31/21.
//

#ifndef __OMNI_OPERATOR_FACTORY_H__
#define __OMNI_OPERATOR_FACTORY_H__

#include "operator.h"

typedef struct JitContext {
    uintptr_t func;
} JitContext;

class OperatorFactory {
public:
    OperatorFactory() {}

    virtual ~OperatorFactory() {}

    virtual omniruntime::op::Operator *CreateOperator() { return nullptr; };

    virtual void SetJitContext(JitContext *JitContext) {
        jitContext = JitContext;
    }

    virtual JitContext *GetJitContext() {
        return jitContext;
    }

private:
    JitContext *jitContext;
};

typedef omniruntime::op::Operator *(*opt_module)(OperatorFactory *);

#endif //__OMNI_OPERATOR_FACTORY_H__
