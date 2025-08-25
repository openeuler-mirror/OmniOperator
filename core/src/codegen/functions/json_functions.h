/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: String Function Registry
 */
#ifndef JSON_FUNCTIONS_H
#define JSON_FUNCTIONS_H

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

#include <iostream>
#include <string>
#include "codegen/json_util.h"
#include "codegen/context_helper.h"

namespace omniruntime::codegen::function {
extern "C" DLLEXPORT const char *GetJsonObject(int64_t contextPtr, const char *jsonStr, int32_t jsonLen,
    bool jsonIsNull, const char *pathStr, int32_t pathLen, bool pathIsNull, bool* retIsNull, int32_t *outLen);
}
#endif
