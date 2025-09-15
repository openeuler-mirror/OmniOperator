/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#ifndef OMNI_RUNTIME_DEFINITIONS_H
#define OMNI_RUNTIME_DEFINITIONS_H

#include <cstdint>

namespace omniruntime {
namespace op {
constexpr int32_t DEFAULT_HASHTABLE_SIZE = 512;
constexpr int32_t DEFAULT_TEMP_MEM_SIZE = 8192;
constexpr int32_t AVG_VECTOR_COUNT = 2;
constexpr uint64_t ARRAY_ALIGNMENT = 64;
}
}
#endif // OMNI_RUNTIME_DEFINITIONS_H
