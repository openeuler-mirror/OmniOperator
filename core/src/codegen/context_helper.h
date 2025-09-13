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

namespace omniruntime::codegen {
#define CHECK_OVERFLOW_RETURN(DECIMAL, PRECISION)                     \
    do {                                                              \
        if ((DECIMAL).IsOverflow((PRECISION)) != OpStatus::SUCCESS) { \
            SetError(contextPtr, DECIMAL_OVERFLOW);                   \
            return 1;                                                 \
        }                                                             \
    } while (false)

#define CHECK_OVERFLOW(DECIMAL, PRECISION)                            \
    do {                                                              \
        if ((DECIMAL).IsOverflow((PRECISION)) != OpStatus::SUCCESS) { \
            SetError(contextPtr, DECIMAL_OVERFLOW);                   \
            return;                                                   \
        }                                                             \
    } while (false)

#define CHECK_DIVIDE_BY_ZERO_RETURN(dividend)  \
    do {                                       \
        if ((dividend) == 0) {                 \
            SetError(contextPtr, DIVIDE_ZERO); \
            return 0;                          \
        }                                      \
    } while (false)

#define CHECK_DIVIDE_BY_ZERO(dividend)         \
    do {                                       \
        if ((dividend) == 0) {                 \
            SetError(contextPtr, DIVIDE_ZERO); \
            return;                            \
        }                                      \
    } while (false)

#define CHECK_OVERFLOW_RETURN_NULL(DECIMAL, PRECISION)                \
    do {                                                              \
        if ((DECIMAL).IsOverflow((PRECISION)) != OpStatus::SUCCESS) { \
            *isNull = true;                                           \
            return 0;                                                 \
        }                                                             \
    } while (false)

#define CHECK_OVERFLOW_VOID_RETURN_NULL(DECIMAL, PRECISION)           \
    do {                                                              \
        if ((DECIMAL).IsOverflow((PRECISION)) != OpStatus::SUCCESS) { \
            *isNull = true;                                           \
            return;                                                   \
        }                                                             \
    } while (false)

#define CHECK_OVERFLOW_CONTINUE_NULL(DECIMAL, PRECISION)              \
    do {                                                              \
        if ((DECIMAL).IsOverflow((PRECISION)) != OpStatus::SUCCESS) { \
            isNull[i] = true;                                         \
            continue;                                                 \
        }                                                             \
    } while (false)

#define CHECK_OVERFLOW_CONTINUE(DECIMAL, PRECISION)                                            \
    do {                                                                                       \
        if ((DECIMAL).IsOverflow((PRECISION)) != OpStatus::SUCCESS && !HasError(contextPtr)) { \
            SetError(contextPtr, DECIMAL_OVERFLOW);                                            \
            continue;                                                                          \
        }                                                                                      \
    } while (false)

#define CHECK_DIVIDE_BY_ZERO_CONTINUE(dividend) \
    do {                                        \
        if ((dividend) == 0) {                  \
            SetError(contextPtr, DIVIDE_ZERO);  \
            continue;                           \
        }                                       \
    } while (false)

extern "C" DLLEXPORT
{
    char *ArenaAllocatorMalloc(int64_t contextPtr, int32_t size);
    bool ArenaAllocatorReset(int64_t contextPtr);
    bool SetError(int64_t contextPtr, std::string errorMessage);
    bool HasError(int64_t contextPtr);
    std::string GetDataString(type::DataTypeId type, int count, ...);
}

template <typename T>
std::string CastErrorMessage(type::DataTypeId from, type::DataTypeId to, T value, type::OpStatus reason, ...)
{
    va_list v;
    va_start(v, reason);
    std::ostringstream errorMessage;
    if (from == type::OMNI_DECIMAL128 || from == type::OMNI_DECIMAL64) {
        int32_t precision = va_arg(v, int32_t);
        int32_t scale = va_arg(v, int32_t);
        errorMessage << "Cannot cast " << GetDataString(from, 2, precision, scale) << " '";
        errorMessage << type::Decimal128Wrapper(value).SetScale(scale).ToString();
    } else {
        errorMessage << "Cannot cast " << GetDataString(from, 1) << " '";
        if constexpr (std::is_same_v<T, __int128_t>) {
            errorMessage << omniruntime::type::ToString(value);
        } else {
            errorMessage << value;
        }
    }
    if (to == type::OMNI_DECIMAL128 || to == type::OMNI_DECIMAL64) {
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