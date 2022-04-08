/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry  function  implementation
 */
#ifndef OMNI_RUNTIME_CONTEXT_HELPER_H
#define OMNI_RUNTIME_CONTEXT_HELPER_H
#include "operator/execution_context.h"

extern "C" {
char *ArenaAllocatorMalloc(int64_t contextPtr, int32_t size);
bool ArenaAllocatorReset(int64_t contextPtr);
}
#endif