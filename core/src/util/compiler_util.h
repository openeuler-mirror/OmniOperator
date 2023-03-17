/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: compiler util Header
 */
#ifndef OMNI_RUNTIME_COMPILER_UTIL_H
#define OMNI_RUNTIME_COMPILER_UTIL_H
#include "config.h"

#if defined(DEBUG) || defined(TRACE)
#define ALWAYS_INLINE
#else
#define ALWAYS_INLINE inline __attribute__((always_inline))
#endif

#endif // OMNI_RUNTIME_COMPILER_UTIL_H
