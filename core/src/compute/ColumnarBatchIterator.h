/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#pragma once

#include <vector/vector_common.h>

namespace omniruntime {

class ColumnarBatchIterator {
public:
    ColumnarBatchIterator() {}

    virtual ~ColumnarBatchIterator() = default;

    virtual vec::VectorBatch* Next() = 0;
};
}