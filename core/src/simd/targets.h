/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

#ifndef OMNI_TARGETS_H_
#define OMNI_TARGETS_H_

// Allows opting out of C++ standard library usage, which is not available in
// some Compiler Explorer environments.

#include "base.h"

namespace simd {
#define OMNI_NEON (1LL << 28)
#define OMNI_SVE_256 (1LL << 19)

#define OMNI_TARGET OMNI_SVE_256
} // namespace omni

#endif // OMNI_TARGETS_H_
