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
    DataTypes(const DataTypes &types) : DataTypes(types.dataTypes) {}

    explicit DataTypes(const std::vector<DataTypeRawPtr> &dataTypes)
        : dataTypesSize(dataTypes.size()), dataTypes(dataTypes), dataTypeIds(nullptr)
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

    const std::vector<DataTypeRawPtr> &Get() const
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
    std::vector<DataTypeRawPtr> dataTypes;
    int32_t *dataTypeIds;
};
}
}

#endif // OMNI_RUNTIME_DATA_TYPES_H
