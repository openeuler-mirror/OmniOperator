/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_TYPES_H
#define OMNI_RUNTIME_VECTOR_TYPES_H

#include <memory>
#include "vector_type.h"
#include "../../thirdparty/huawei_secure_c/include/securec.h"

namespace omniruntime {
namespace vec {
class VecTypes {
public:
    VecTypes(const std::vector<VecType> &vecTypes) : vecTypes(vecTypes), vecTypesSize(vecTypes.size())
    {
        InitVecTypeIds();
    }

    ~VecTypes()
    {
        delete[] vecTypeIds;
    }

    const std::vector<VecType> &Get()
    {
        return vecTypes;
    }

    const int32_t *GetIds()
    {
        // need remove here when jni operator factor finish refactor.
        int32_t *newVecTypeIds = new int32_t[vecTypesSize];
        memcpy_s(newVecTypeIds, vecTypesSize * sizeof(int32_t), vecTypeIds, vecTypesSize * sizeof(int32_t));
        return newVecTypeIds;
    }

    int32_t GetSize()
    {
        return vecTypesSize;
    }

private:
    void InitVecTypeIds()
    {
        int32_t size = vecTypes.size();
        vecTypeIds = new int32_t[size];
        for (int i = 0; i < size; ++i) {
            vecTypeIds[i] = vecTypes[i].GetId();
        }
    }

    int32_t vecTypesSize;
    const std::vector<VecType> vecTypes;
    int32_t *vecTypeIds;
};
using VecTypesPtr = std::shared_ptr<VecTypes>;
}
}

#endif // OMNI_RUNTIME_VECTOR_TYPES_H
