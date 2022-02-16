/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DATA_TYPES_H
#define OMNI_RUNTIME_DATA_TYPES_H

#include <memory>
#include "data_type.h"

namespace omniruntime {
namespace type {
class VecTypes {
public:
    VecTypes(const VecTypes &types) : VecTypes(types.vecTypes) {}

    explicit VecTypes(const std::vector<VecType> &vecTypes)
        : vecTypes(vecTypes), vecTypesSize(vecTypes.size()), vecTypeIds(nullptr)
    {
        InitVecTypeIds();
    }

    VecTypes &operator = (const VecTypes &types)
    {
        vecTypes = types.vecTypes;
        vecTypesSize = types.vecTypesSize;
        if (vecTypeIds != nullptr) {
            delete[] vecTypeIds;
        }
        InitVecTypeIds();
        return *this;
    }

    ~VecTypes()
    {
        delete[] vecTypeIds;
    }

    const std::vector<VecType> &Get() const
    {
        return vecTypes;
    }

    const int32_t *GetIds() const
    {
        return vecTypeIds;
    }

    int32_t GetSize() const
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
    std::vector<VecType> vecTypes;
    int32_t *vecTypeIds;
};
}
}

#endif // OMNI_RUNTIME_DATA_TYPES_H
