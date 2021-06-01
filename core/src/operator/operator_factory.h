//
// Created by root on 5/31/21.
//

#ifndef __OMNI_OPERATOR_FACTORY_H__
#define __OMNI_OPERATOR_FACTORY_H__

#include "operator.h"

using namespace omni;

typedef struct JitContext {
    uintptr_t jitter;
    uintptr_t func;
} JitContext;

class OperatorFactory {
public:
    OperatorFactory() {}

    virtual ~OperatorFactory() {}

    virtual omni::Operator *createOperator() { return nullptr; };

    virtual void setJitContext(JitContext *JitContext) {
        jitContext = JitContext;
    }

    virtual JitContext *getJitContext() {
        return jitContext;
    }

private:
    JitContext *jitContext;
};

typedef omni::Operator *(*opt_module)(OperatorFactory *);

#endif //__OMNI_OPERATOR_FACTORY_H__
