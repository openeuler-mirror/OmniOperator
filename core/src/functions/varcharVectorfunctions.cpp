/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry varcharVector functions
 */

#include "common.h"
#include "vector/variable_width_vector.h"

using namespace omniruntime::vec;
using namespace std;

extern "C" {
INLINE int32_t WrapVarcharVector(int64_t vectorAddr, int32_t index, uint8_t *data, int32_t dataLen)
{
    auto *varcharVectorPtr = reinterpret_cast<VarcharVector *>(vectorAddr);
    varcharVectorPtr->SetValue(index, reinterpret_cast<uint8_t *>(data), dataLen);
    return 0;
}
}