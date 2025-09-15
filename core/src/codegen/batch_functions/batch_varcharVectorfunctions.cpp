/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch varcharVector functions implementation
 */

#include "batch_varcharVectorfunctions.h"
#include "vector/vector.h"

using namespace omniruntime::vec;

namespace omniruntime::codegen::function {
extern "C" DLLEXPORT int32_t BatchWrapVarcharVector(int64_t vectorAddr, uint8_t **data, int32_t *dataLen,
    int32_t rowCnt)
{
    auto *varcharVectorPtr = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vectorAddr);
    for (int i = 0; i < rowCnt; ++i) {
        if (data[i] == nullptr) {
            varcharVectorPtr->SetNull(i);
        } else {
            std::string_view strView(reinterpret_cast<const char *>(data[i]), dataLen[i]);
            varcharVectorPtr->SetValue(i, strView);
        }
    }
    return 0;
}

extern "C" DLLEXPORT void BatchNullArrayToBits(int32_t *dstBits, bool *srcArray, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        BitUtil::SetBit(dstBits, i, srcArray[i]);
    }
}

extern "C" DLLEXPORT void BatchBitsToNullArray(bool *dstArray, int32_t *srcBits, int32_t *rowIdxArray, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        dstArray[i] = BitUtil::IsBitSet(srcBits, rowIdxArray[i]);
    }
}
}