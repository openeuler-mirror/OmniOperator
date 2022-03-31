/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry  function  implementation
 */
#include "context_helper.h"
#include "common.h"

using namespace std;
using namespace omniruntime::op;

extern "C" {
INLINE char *ArenaAllocatorMalloc(int64_t contextPtr, int32_t size) {
    auto context = reinterpret_cast<ExecutionContext *>(contextPtr);
    return reinterpret_cast<char *>(context->GetArena()->Allocate(size));
}

INLINE bool ArenaAllocatorReset(int64_t contextPtr) {
    auto context = reinterpret_cast<ExecutionContext *>(contextPtr);
    context->GetArena()->Reset();
    return true;
}
}