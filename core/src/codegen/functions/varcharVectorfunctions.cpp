/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry varcharVector functions
 */

#include "varcharVectorfunctions.h"
#include "vector/vector.h"

using namespace omniruntime::vec;
using namespace std;
namespace omniruntime::codegen::function {
extern DLLEXPORT int32_t WrapVarcharVector(int64_t vectorAddr, int32_t index, uint8_t *data, int32_t dataLen)
{
    auto vec = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vectorAddr);
    if (data == nullptr) {
        vec->SetNull(index);
    } else {
        std::string_view strView(reinterpret_cast<const char *>(data), dataLen);
        vec->SetValue(index, strView);
    }
    return 0;
}
}