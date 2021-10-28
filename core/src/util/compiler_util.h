/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: JNI Vector Operations Header
 */
#ifndef OMNI_RUNTIME_COMPILER_UTIL_H
#define OMNI_RUNTIME_COMPILER_UTIL_H
#include "../../config.h"
/// Compiler hint that this branch is likely or unlikely to
/// be taken. Take from the "What all programmers should know
/// about memory" paper.
/// example: if (LIKELY(size > 0)) { ... }
/// example: if (UNLIKELY(!status.ok())) { ... }
#ifdef LIKELY
#undef LIKELY
#endif

#ifdef UNLIKELY
#undef UNLIKELY
#endif

#define LIKELY(expr) __builtin_expect(!!(expr), 1)
#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)

#define PREFETCH(addr) __builtin_prefetch(addr)

/// Force inlining. The 'inline' keyword is treated by most compilers as a hint,
/// not a command. This should be used sparingly for cases when either the function
/// needs to be inlined for a specific reason or the compiler's heuristics make a bad
/// decision, e.g. not inlining a small function on a hot path.
#if defined(DEBUG) || defined(TRACE)
#define ALWAYS_INLINE
#else
#define ALWAYS_INLINE __attribute__((always_inline))
#endif
#endif //OMNI_RUNTIME_COMPILER_UTIL_H
