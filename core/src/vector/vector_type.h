/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
//
// Created by root on 6/8/21.
//

#ifndef OMNI_RUNTIME_VECTOR_TYPE_H
#define OMNI_RUNTIME_VECTOR_TYPE_H

namespace omniruntime {
namespace vec {
typedef enum {
    OMNI_VEC_TYPE_INT = 1,
    OMNI_VEC_TYPE_LONG = 2,
    OMNI_VEC_TYPE_DOUBLE = 3,
    OMNI_VEC_TYPE_BOOLEAN = 4,
    OMNI_VEC_TYPE_SHORT = 5,
    OMNI_VEC_TYPE_128_DECIMAL = 6,
    OMNI_VEC_TYPE_256_DECIMAL = 7,
    OMNI_VEC_TYPE_CONTAINER = 8,
    OMNI_VEC_TYPE_VARCHAR = 100,
    OMNI_VEC_TYPE_DICTIONARY = 101,
    OMNI_VEC_TYPE_INVALID = 200
} VecType;
} // namespace vec
} // namespace omniruntime
#endif // OMNI_RUNTIME_VECTOR_TYPE_H
