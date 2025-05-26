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

#define LIKELY(expr) __builtin_expect((expr), 1)
#define UNLIKELY(expr) __builtin_expect((expr), 0)
#define ALIGNMENT_SIZE  16
// for not vectorize replace "tree-vectorize" with "no-tree-vectorize"
#define VECTORIZE_LOOP __attribute__((optimize("tree-vectorize")))
#define SIMD_ALWAYS_INLINE inline __attribute__((always_inline))
#define NO_INLINE __attribute__((__noinline__))
// use __attribute__((optimize ("fast-math"))) for FAST_MATH to auto vectorize floating point loops
// note that this might generated different results compaired with not vectorized loop since floating point
// arithmetic is not associative
#define FAST_MATH __attribute__((optimize("fast-math")))
#endif // OMNI_RUNTIME_COMPILER_UTIL_H
