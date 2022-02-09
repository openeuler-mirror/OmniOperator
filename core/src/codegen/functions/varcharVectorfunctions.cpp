/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry varcharVector functions
 */

#include "varcharVectorfunctions.h"
#include "../../vector/variable_width_vector.h"

using namespace omniruntime::vec;
using namespace std;

extern "C" DLLEXPORT void WrapVarcharVector(int8_t *vectorPtr, int32_t index, int8_t *data, int32_t dataLen)
{
    auto *varcharVectorPtr = reinterpret_cast<VarcharVector *>(vectorPtr);
    varcharVectorPtr->SetValue(index, reinterpret_cast<uint8_t *>(data), dataLen);
}