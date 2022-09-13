/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry  function  implementation
 */
#include "context_helper.h"

using namespace std;
using namespace omniruntime::op;

namespace omniruntime {
namespace codegen {
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT
{
    char *ArenaAllocatorMalloc(int64_t contextPtr, int32_t size)
    {
        auto context = reinterpret_cast<ExecutionContext *>(contextPtr);
        return reinterpret_cast<char *>(context->GetArena()->Allocate(size));
    }

    bool ArenaAllocatorReset(int64_t contextPtr)
    {
        auto context = reinterpret_cast<ExecutionContext *>(contextPtr);
        context->GetArena()->Reset();
        return true;
    }

    bool SetError(int64_t contextPtr, const char *errorMessage, int32_t messageLength)
    {
        auto context = reinterpret_cast<ExecutionContext *>(contextPtr);
        context->SetError(std::string(errorMessage, messageLength));
        return true;
    }
}
}
}