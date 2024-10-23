/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef OMNI_SIMD_H
#define OMNI_SIMD_H

#include "base.h"
#ifdef ENABLE_SVE
#include "instruction/arm_sve-inl.h"
#else
#include "instruction/arm_neon-inl.h"
#endif // OMNI_TARGET

#include "instruction/generic_ops-inl.h"

#endif // OMNI_SIMD_H
