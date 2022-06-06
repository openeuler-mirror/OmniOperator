/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_LOADER_H
#define OMNI_RUNTIME_VECTOR_LOADER_H

#include "vector/vector.h"

namespace omniruntime {
namespace vec {
class VectorLoader {
public:
    VectorLoader() = default;

    virtual ~VectorLoader() = default;

    virtual Vector *Load() = 0;
};
}
}

#endif // OMNI_RUNTIME_VECTOR_LOADER_H
