/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <nlohmann/json.hpp>
#include <gtest/gtest.h>
#include <type/data_type_serializer.h>

using namespace omniruntime::type;
using DataType = omniruntime::type::DataType;
using IntDataType = omniruntime::type::IntDataType;
using LongDataType = omniruntime::type::LongDataType;
using DoubleDataType = omniruntime::type::DoubleDataType;
using Decimal128DataType = omniruntime::type::Decimal128DataType;

namespace TypeSerializationTest {
TEST(DataTypeSerializer, serialization)
{
    Decimal128DataType decimal(10, 2);
    std::vector<DataType> types = { IntDataType(), DoubleDataType(), decimal, LongDataType(), Date32DataType(MILLI),
        Date64DataType(MILLI), VarcharDataType(1024), ContainerDataType(
        std::vector<DataType>{IntDataType(), DoubleDataType()})};
    std::string typesJson = omniruntime::type::Serialize(types);
    auto dataTypes = omniruntime::type::Deserialize(typesJson);
    auto check = dataTypes.Get();
    EXPECT_EQ(check.size(), types.size());
    EXPECT_EQ(check[0].GetId(), OMNI_INT);
    EXPECT_EQ(check[1].GetId(), OMNI_DOUBLE);
    EXPECT_EQ(check[2].GetId(), OMNI_DECIMAL128);
    EXPECT_EQ(static_cast<Decimal128DataType *>(&check[2])->GetScale(), decimal.GetScale());
    EXPECT_EQ(static_cast<Decimal128DataType *>(&check[2])->GetPrecision(), decimal.GetPrecision());
    EXPECT_EQ(static_cast<Decimal128DataType *>(&check[2])->GetScale(), decimal.GetScale());
    EXPECT_EQ(check[3].GetId(), OMNI_LONG);
    EXPECT_EQ(check[4].GetId(), OMNI_DATE32);
    EXPECT_EQ(static_cast<Date32DataType *>(&check[4])->GetDateUnit(), MILLI);
    EXPECT_EQ(check[5].GetId(), OMNI_DATE64);
    EXPECT_EQ(static_cast<Date32DataType *>(&check[5])->GetDateUnit(), MILLI);
    EXPECT_EQ(check[6].GetId(), OMNI_VARCHAR);
    EXPECT_EQ(static_cast<VarcharDataType *>(&check[6])->GetWidth(), 1024);
    EXPECT_EQ(check[7].GetId(), OMNI_CONTAINER);
    EXPECT_EQ(check[7].GetFieldTypes()[0].GetId(), OMNI_INT);
    EXPECT_EQ(check[7].GetFieldTypes()[1].GetId(), OMNI_DOUBLE);

    std::cout << omniruntime::type::Serialize(types) << std::endl;

    auto dataTypesCopy = dataTypes;
    check = dataTypesCopy.Get();
    EXPECT_EQ(check.size(), types.size());
    EXPECT_EQ(check[0].GetId(), OMNI_INT);
    EXPECT_EQ(check[1].GetId(), OMNI_DOUBLE);
    EXPECT_EQ(check[2].GetId(), OMNI_DECIMAL128);
    EXPECT_EQ(static_cast<Decimal128DataType *>(&check[2])->GetScale(), decimal.GetScale());
    EXPECT_EQ(static_cast<Decimal128DataType *>(&check[2])->GetPrecision(), decimal.GetPrecision());
    EXPECT_EQ(static_cast<Decimal128DataType *>(&check[2])->GetScale(), decimal.GetScale());
    EXPECT_EQ(check[3].GetId(), OMNI_LONG);
    EXPECT_EQ(check[4].GetId(), OMNI_DATE32);
    EXPECT_EQ(static_cast<Date32DataType *>(&check[4])->GetDateUnit(), MILLI);
    EXPECT_EQ(check[5].GetId(), OMNI_DATE64);
    EXPECT_EQ(static_cast<Date32DataType *>(&check[5])->GetDateUnit(), MILLI);
    EXPECT_EQ(check[6].GetId(), OMNI_VARCHAR);
    EXPECT_EQ(static_cast<VarcharDataType *>(&check[6])->GetWidth(), 1024);

    std::cout << omniruntime::type::Serialize(types) << std::endl;
    std::string typeJson = omniruntime::type::SerializeSingle(IntDataType());
    auto dataType = omniruntime::type::DeserializeSingle(typeJson);
    EXPECT_EQ(dataType.GetId(), OMNI_INT);
}
}
