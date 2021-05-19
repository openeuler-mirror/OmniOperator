#ifndef __NATIVE_BASE_H__
#define __NATIVE_BASE_H__

#include "../vector/table.h"
#include <vector>

typedef struct JitContext
{
    uintptr_t jitter;
    uintptr_t func;
} JitContext;

class NativeOmniOperator;

class NativeOmniOperatorFactory 
{
public:
    NativeOmniOperatorFactory() {}
    virtual ~NativeOmniOperatorFactory(){}
    virtual NativeOmniOperator *createOmniOperator(){};
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

class NativeOmniOperator
{
public:
    NativeOmniOperator() {}
    virtual ~NativeOmniOperator(){}
    // TBD addInput return ErrNo 
    virtual int32_t addInput(Table* data, int32_t rowCount) = 0;
    // orderby needs an array to sort
    virtual int32_t addInput(Table** data, int32_t* rowCount, int32_t pageCount) = 0;
    virtual int32_t getOutput(std::vector<Table*>& data) = 0;
    virtual int32_t* getSourceTypes() = 0;
    virtual void close() {}
};

typedef NativeOmniOperator* (*opt_module) (NativeOmniOperatorFactory*);
#endif