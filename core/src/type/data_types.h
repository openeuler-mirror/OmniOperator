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
    DataTypes() : dataTypesSize(0), dataTypeIds(nullptr) {}

    DataTypes(const DataTypes &types) : DataTypes(types.dataTypes) {}

    explicit DataTypes(const std::vector<DataTypePtr> &dataTypes)
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

    const std::vector<DataTypePtr> &Get() const
    {
        return dataTypes;
    }

    const DataTypePtr &GetType(int i) const
    {
        return dataTypes[i];
    }

    const int32_t *GetIds() const
    {
        return dataTypeIds;
    }

    int32_t GetSize() const
    {
        return dataTypesSize;
    }

    std::shared_ptr<DataTypes> Instance()
    {
        return std::make_shared<DataTypes>(dataTypes);
    }

    static std::unique_ptr<DataTypes> NoneDataTypesInstance()
    {
        return std::make_unique<DataTypes>(std::vector<DataTypePtr> { NoneDataType::Instance() });
    }

    static std::unique_ptr<DataTypes> GenerateDataTypes(DataTypePtr dataTypePtr)
    {
        std::vector<DataTypePtr> singleDataType;
        singleDataType.push_back(dataTypePtr);
        return std::make_unique<DataTypes>(singleDataType);
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
