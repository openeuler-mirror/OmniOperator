/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: aggregator tool class
 */
#include "aggregator_util.h"

namespace omniruntime {
namespace op {
std::vector<int32_t> AggregatorUtil::WrapWithVector(int32_t value)
{
    std::vector<int32_t> retVector;
    retVector.push_back(value);
    return retVector;
}
//   DataTypes(LONG, INT) => vector(DataTypes(LONG),DataTypes(INT))
std::vector<DataTypes> AggregatorUtil::WrapWithVector(const DataTypes &value)
{
    std::vector<DataTypes> retVector;
    retVector.reserve(value.GetSize());
    for (int i = 0; i < value.GetSize(); ++i) {
        std::vector<DataTypePtr> vector{ value.GetType(i) };
        retVector.push_back(DataTypes(vector));
    }
    return retVector;
}

std::vector<bool> AggregatorUtil::WrapWithVector(bool value, int num)
{
    std::vector<bool> retVector;
    retVector.reserve(num);
    for (int i = 0; i < num; i++) {
        retVector.push_back(value);
    }
    return retVector;
}

std::vector<std::vector<uint32_t>> AggregatorUtil::WrapWithVector(const std::vector<uint32_t> &values)
{
    std::vector<std::vector<uint32_t>> twoLayerVectors;
    twoLayerVectors.reserve(values.size());
    for (auto value : values) {
        twoLayerVectors.push_back({ value });
    }
    return twoLayerVectors;
}

// vector(1,2,3) => vector(vector(1),vector(2),vector(3))
std::vector<std::vector<int32_t>> AggregatorUtil::WrapWithVector(std::vector<int32_t> &values)
{
    std::vector<std::vector<int32_t>> twoLayerVectors;
    twoLayerVectors.reserve(values.size());
    for (auto value : values) {
        twoLayerVectors.push_back({ value });
    }
    return twoLayerVectors;
}

std::unique_ptr<DataTypes> AggregatorUtil::WrapWithDataTypes(const DataTypePtr &value)
{
    std::vector<DataTypePtr> vector;
    vector.push_back(value);
    return std::make_unique<DataTypes>(vector);
}

bool AggregatorUtil::IsHMPPMaxMinSupportDataTypeId(DataTypeId toChkDataTypeId)
{
    static std::vector<DataTypeId> dataTypeIdsWhiteList = { OMNI_SHORT,     OMNI_INT,    OMNI_DATE32,    OMNI_LONG,
                                                            OMNI_DECIMAL64, OMNI_DOUBLE, OMNI_DECIMAL128 };
    for (auto dataTypeId : dataTypeIdsWhiteList) {
        if (dataTypeId == toChkDataTypeId) {
            return true;
        }
    }
    return false;
}
} // end of namespace op
} // end of namespace omniruntime