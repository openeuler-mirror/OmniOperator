/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_ENCODING_H
#define OMNI_RUNTIME_VECTOR_ENCODING_H

namespace omniruntime {
namespace vec {
enum VectorEncoding {
    // ordinary vector
    OMNI_VEC_ENCODING_FLAT = 0,
    OMNI_VEC_ENCODING_DICTIONARY = 1,
    OMNI_VEC_ENCODING_CONTAINER = 2,
    OMNI_VEC_ENCODING_LAZY = 3,
    OMNI_VEC_ENCODING_INVALID
};
} // namespace vec
} // namespace omniruntime
#endif // OMNI_RUNTIME_VECTOR_ENCODING_H
