/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry  function  implementation
 */
#include "context_helper.h"

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

using namespace std;
using namespace omniruntime::op;

extern "C" DLLEXPORT {

char* ArenaAllocatorMalloc(int64_t contextPtr, int32_t size)
{
    auto context = reinterpret_cast<ExecutionContext*>(contextPtr);
    return reinterpret_cast<char *>(context->getArena()->Allocate(size));
}

void ArenaAllocatorReset(int64_t contextPtr)
{
    auto context = reinterpret_cast<ExecutionContext*>(contextPtr);
    return context->getArena()->Reset();
}
}