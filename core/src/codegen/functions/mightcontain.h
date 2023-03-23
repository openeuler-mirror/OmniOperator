/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: registry  function  implementation
 */
#ifndef OMNI_RUNTIME_MIGHTCONTAIN_H
#define OMNI_RUNTIME_MIGHTCONTAIN_H

#include <iostream>
#include "xxhash64_hash.h"

namespace omniruntime::codegen::function {
// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT bool MightContain(int64_t bloomFilterAddr, int64_t hashValue, bool isNull);
}
#endif // OMNI_RUNTIME_MIGHTCONTAIN_H