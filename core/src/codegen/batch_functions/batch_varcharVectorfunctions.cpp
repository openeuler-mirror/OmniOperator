/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch varcharVector functions implementation
 */

#include "batch_varcharVectorfunctions.h"
#include "../../vector/variable_width_vector.h"

using namespace omniruntime::vec;
using namespace std;

extern DLLEXPORT int32_t BatchWrapVarcharVector(int64_t vectorAddr, uint8_t **data, int32_t *dataLen, int32_t rowCnt)
{
    auto *varcharVectorPtr = reinterpret_cast<VarcharVector *>(vectorAddr);
    for (int i = 0; i < rowCnt; ++i) {
        varcharVectorPtr->SetValue(i, reinterpret_cast<uint8_t *>(data[i]), dataLen[i]);
    }
    return 0;
}