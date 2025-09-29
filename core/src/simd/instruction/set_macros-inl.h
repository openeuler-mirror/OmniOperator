/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
 // Copyright 2020 Google LLC
 // Copyright 2024 Arm Limited and/or its affiliates <open-source-office@arm.com>
 // SPDX-License-Identifier: Apache-2.0
 // SPDX-License-Identifier: BSD-3-Clause
 //
 // Licensed under the Apache License, Version 2.0 (the "License");
 // you may not use this file except in compliance with the License.
 // You may obtain a copy of the License at
 //
 //      http://www.apache.org/licenses/LICENSE-2.0
 //
 // Unless required by applicable law or agreed to in writing, software
 // distributed under the License is distributed on an "AS IS" BASIS,
 // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 // See the License for the specific language governing permissions and
 // limitations under the License.
#ifndef OMNI_SET_MACROS_H
#define OMNI_SET_MACROS_H

#include "simd/targets.h"

#undef OMNI_NAMESPACE
#undef OMNI_ALIGN
#undef OMNI_MAX_BYTES
#undef OMNI_LANES

#undef OMNI_HAVE_SCALABLE
#undef OMNI_HAVE_TUPLE
#undef OMNI_HAVE_INTEGER64
#undef OMNI_HAVE_FLOAT16
#undef OMNI_HAVE_FLOAT64
#undef OMNI_MEM_OPS_MIGHT_FAULT
#undef OMNI_NATIVE_FMA
#undef OMNI_NATIVE_DOT_BF16
#undef OMNI_CAP_GE256
#undef OMNI_CAP_GE512

#undef OMNI_TARGET_IS_SVE
#if OMNI_TARGET == OMNI_SVE_256
#define OMNI_TARGET_IS_SVE 1
#else
#define OMNI_TARGET_IS_SVE 0
#endif

#undef OMNI_TARGET_IS_NEON
#if OMNI_TARGET == OMNI_NEON
#define OMNI_TARGET_IS_NEON 1
#else
#define OMNI_TARGET_IS_NEON 0
#endif

// For internal use (clamping/validating N for Simd<>)
#define OMNI_MAX_N 65536

// For OMNI_TARGET == OMNI_RVV, LMUL <= 8. Even on other targets, we want to
// support say Rebind<uint64_t, Simd<uint8_t, 1, 0>> d; whose kPow2 is also 3.
// However, those other targets do not actually support multiple vectors, and
// thus Lanes(d) must not exceed Lanes(ScalableTag<T>()).
#define OMNI_MAX_POW2 3

// User-visible. Loose lower bound that guarantees OMNI_MAX_BYTES >>
// (-OMNI_MIN_POW2) <= 1. Useful for terminating compile-time recursions.
// Tighter bound for other targets, whose vectors are smaller, to potentially
// save compile time.
#define OMNI_MIN_POW2 -8

// -----------------------------------------------------------------------------
#if OMNI_TARGET_IS_NEON
#define OMNI_ALIGN alignas(16)
#define OMNI_MAX_BYTES 16
#define OMNI_LANES(T) static_cast<int32_t>(16 / sizeof(T))

#define OMNI_HAVE_SCALABLE 0
#define OMNI_HAVE_INTEGER64 1
#define OMNI_HAVE_FLOAT16 0
#define OMNI_HAVE_FLOAT64 1

#define OMNI_MEM_OPS_MIGHT_FAULT 1
#define OMNI_NATIVE_FMA 1
#if OMNI_NEON_HAVE_F32_TO_BF16C || OMNI_TARGET == OMNI_NEON_BF16
#define OMNI_NATIVE_DOT_BF16 1
#else
#define OMNI_NATIVE_DOT_BF16 0
#endif

#define OMNI_CAP_GE256 0
#define OMNI_CAP_GE512 0

#if OMNI_TARGET == OMNI_NEON_WITHOUT_AES
#define OMNI_NAMESPACE N_NEON_WITHOUT_AES
#elif OMNI_TARGET == OMNI_NEON
#define OMNI_NAMESPACE N_NEON
#elif OMNI_TARGET == OMNI_NEON_BF16
#define OMNI_NAMESPACE N_NEON_BF16
#else
#error "Logic error, missing case"
#endif // OMNI_TARGET

// Can use pragmas instead of -march compiler flag
#if OMNI_HAVE_RUNTIME_DISPATCH
#if OMNI_ARCH_ARM_V7

// The __attribute__((target(+neon-vfpv4)) was introduced in gcc >= 8.
#if OMNI_COMPILER_GCC_ACTUAL >= 800
#define OMNI_TARGET_STR "+neon-vfpv4"
#else  // GCC < 7
// Do not define OMNI_TARGET_STR (no pragma).
#endif // OMNI_COMPILER_GCC_ACTUAL

#else // !OMNI_ARCH_ARM_V7

#if (OMNI_COMPILER_GCC_ACTUAL && OMNI_COMPILER_GCC_ACTUAL < 1300) ||
    (OMNI_COMPILER_CLANG && OMNI_COMPILER_CLANG < 1300)
// GCC 12 or earlier and Clang 12 or earlier require +crypto be added to the
// target string to enable AArch64 AES intrinsics
#define OMNI_TARGET_STR_NEON "+crypto"
#else
#define OMNI_TARGET_STR_NEON "+aes"
#endif

// Clang >= 16 requires +fullfp16 instead of fp16, but Apple Clang 15 = 1600
// fails to parse unless the string starts with armv8, whereas 1700 refuses it.
#define OMNI_TARGET_STR_FP16 "+fp16"

#if OMNI_TARGET == OMNI_NEON_WITHOUT_AES
// Do not define OMNI_TARGET_STR (no pragma).
#elif OMNI_TARGET == OMNI_NEON
#define OMNI_TARGET_STR OMNI_TARGET_STR_NEON
#elif OMNI_TARGET == OMNI_NEON_BF16
#define OMNI_TARGET_STR OMNI_TARGET_STR_FP16 "+bf16+dotprod" OMNI_TARGET_STR_NEON
#else
#error "Logic error, missing case"
#endif // OMNI_TARGET

#endif // !OMNI_ARCH_ARM_V7
#else  // !OMNI_HAVE_RUNTIME_DISPATCH
// OMNI_TARGET_STR remains undefined
#endif

// -----------------------------------------------------------------------------
// SVE[2]
#elif OMNI_TARGET_IS_SVE

// SVE only requires lane alignment, not natural alignment of the entire vector.
#define OMNI_ALIGN alignas(8)

// Value ensures MaxLanes() is the tightest possible upper bound to reduce
// overallocation.
#define OMNI_MAX_BYTES 32
#define OMNI_LANES(T) ((OMNI_MAX_BYTES) / sizeof(T))

#define OMNI_HAVE_INTEGER64 1
#define OMNI_HAVE_FLOAT16 1
#define OMNI_HAVE_FLOAT64 1
#define OMNI_MEM_OPS_MIGHT_FAULT 0
#define OMNI_NATIVE_FMA 1
#define OMNI_NATIVE_DOT_BF16 0
#endif
#define OMNI_CAP_GE256 0
#define OMNI_CAP_GE512 0

#define OMNI_MAX_BYTES 32
#define OMNI_HAVE_SCALABLE 0

#endif // OMNI_SET_MACROS_H