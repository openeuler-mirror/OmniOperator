/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: registry  function  implementation
 */
#ifndef OMNI_RUNTIME_CONTEXT_HELPER_H
#define OMNI_RUNTIME_CONTEXT_HELPER_H

#include <cstdint>
#include "operator/execution_context.h"
#include "type/data_type.h"
#include "type/decimal_operations.h"
#include "util/config_util.h"
#include "type/data_operations.h"

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

using namespace omniruntime::type;

namespace omniruntime::codegen {
#define CHECK_OVERFLOW_RETURN(DECIMAL, PRECISION)                   \
if (DECIMAL.IsOverflow(PRECISION) !=  OpStatus::SUCCESS) {          \
    SetError(contextPtr, DECIMAL_OVERFLOW);                         \
    return 1;                                                       \
}

#define CHECK_OVERFLOW(DECIMAL, PRECISION)                          \
if (DECIMAL.IsOverflow(PRECISION) !=  OpStatus::SUCCESS) {          \
    SetError(contextPtr, DECIMAL_OVERFLOW);                         \
    return;                                                         \
}

#define CHECK_DIVIDE_BY_ZERO_RETURN(dividend)                       \
if (dividend == 0) {                                                \
    SetError(contextPtr, DIVIDE_ZERO);                              \
    return 0;                                                       \
}

#define CHECK_DIVIDE_BY_ZERO(dividend)                              \
if (dividend == 0) {                                                \
    SetError(contextPtr, DIVIDE_ZERO);                              \
    return;                                                         \
}

#define CHECK_OVERFLOW_RETURN_NULL(DECIMAL, PRECISION)              \
if (DECIMAL.IsOverflow(PRECISION) != OpStatus::SUCCESS) {           \
    *isNull = true;                                                 \
    return 0;                                                       \
}

#define CHECK_OVERFLOW_VOID_RETURN_NULL(DECIMAL, PRECISION)         \
if (DECIMAL.IsOverflow(PRECISION) != OpStatus::SUCCESS) {           \
    *isNull = true;                                                 \
    return;                                                         \
}

static std::ostringstream errorMessage;

static void ReSetErrorMessage()
{
    errorMessage.clear();
    errorMessage.str("");
}

extern "C" DLLEXPORT
{
    char *ArenaAllocatorMalloc(int64_t contextPtr, int32_t size);
    bool ArenaAllocatorReset(int64_t contextPtr);
    bool SetError(int64_t contextPtr, std::string errorMessage);
    std::string GetDataString(DataTypeId type, int count, ...);
}

template<typename T>
std::string CastErrorMessage(DataTypeId from, DataTypeId to, T value, OpStatus reason, ...)
{
    va_list v;
    va_start(v, reason);
    ReSetErrorMessage();
    if (from == OMNI_DECIMAL128 || from == OMNI_DECIMAL64) {
        int32_t precision = va_arg(v, int32_t);
        int32_t scale = va_arg(v, int32_t);
        errorMessage << "Cannot cast " << GetDataString(from, 2, precision, scale) << " '";
        errorMessage << Decimal128Wrapper(value).SetScale(scale).ToString();
    } else {
        errorMessage << "Cannot cast " << GetDataString(from, 1) << " '";
        errorMessage << value;
    }
    if (to == OMNI_DECIMAL128 || to == OMNI_DECIMAL64) {
        int32_t precision = va_arg(v, int32_t);
        int32_t scale = va_arg(v, int32_t);
        errorMessage << "' to " << GetDataString(to, 2, precision, scale);
    } else {
        errorMessage << "' to " << GetDataString(to, 1);
    }
    if (reason == type::OpStatus::OP_OVERFLOW) {
        errorMessage << ". Value too large.";
    }
    if (reason == type::OpStatus::FAIL) {
        errorMessage << ". Value is not a number.";
    }
    va_end(v);
    return errorMessage.str();
}
}
#endif