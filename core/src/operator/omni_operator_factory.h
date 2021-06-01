//
// Created by root on 5/31/21.
//

#ifndef __OMNI_OPERATOR_FACTORY_H__
#define __OMNI_OPERATOR_FACTORY_H__

#include "omni_operator.h"

typedef struct JitContext
{
    uintptr_t jitter;
    uintptr_t func;
} JitContext;

class OmniOperatorFactory
{
public:
    OmniOperatorFactory() {}
    virtual ~OmniOperatorFactory(){}
    virtual OmniOperator *createOmniOperator(){ return nullptr; };
    virtual void setJitContext(JitContext* JitContext)
    {
        jitContext = JitContext;
    }
    virtual JitContext* getJitContext()
    {
        return jitContext;
    }
private:
    JitContext* jitContext;
};

typedef OmniOperator* (*opt_module) (OmniOperatorFactory*);

#endif //__OMNI_OPERATOR_FACTORY_H__
