/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNI_RUNTIME_XSIMD_H
#define OMNI_RUNTIME_XSIMD_H

#include "xsimd/xsimd.hpp"

using int64BatchType = xsimd::batch<int64_t, xsimd::default_arch>;
using int64BatchBoolType = xsimd::batch_bool<int64_t, xsimd::default_arch>;

constexpr int32_t INT64_K_WIDTH = xsimd::batch<int64_t>::size;

#endif // OMNI_RUNTIME_XSIMD_H
