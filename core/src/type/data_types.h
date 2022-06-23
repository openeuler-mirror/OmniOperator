/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DATA_TYPES_H
#define OMNI_RUNTIME_DATA_TYPES_H

#include <memory>
#include "data_type.h"

namespace omniruntime {
namespace type {
class DataTypes {
public:
    explicit DataTypes(std::vector<DataTypePtr> &dataTypes)
        : dataTypesSize(dataTypes.size()), dataTypes(std::move(dataTypes)), dataTypeIds(nullptr)
    {
        InitDataTypeIds();
    }

    DataTypes &operator = (const DataTypes &types)
    {
        dataTypes = types.dataTypes;
        dataTypesSize = types.dataTypesSize;
        if (dataTypeIds != nullptr) {
            delete[] dataTypeIds;
        }
        InitDataTypeIds();
        return *this;
    }

    ~DataTypes()
    {
        delete[] dataTypeIds;
    }

    const std::vector<DataTypePtr> &Get() const
    {
        return dataTypes;
    }

    const int32_t *GetIds() const
    {
        return dataTypeIds;
    }

    int32_t GetSize() const
    {
        return dataTypesSize;
    }

private:
    void InitDataTypeIds()
    {
        int32_t size = dataTypes.size();
        dataTypeIds = new int32_t[size];
        for (int i = 0; i < size; ++i) {
            dataTypeIds[i] = dataTypes[i]->GetId();
        }
    }

    int32_t dataTypesSize;
    std::vector<DataTypePtr> dataTypes;
    int32_t *dataTypeIds;
};
using DataTypesPtr = std::shared_ptr<DataTypes>;
}
}

#endif // OMNI_RUNTIME_DATA_TYPES_H
