/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <nlohmann/json.hpp>
#include <gtest/gtest.h>
#include <type/data_type_serializer.h>
#include "util/type_util.h"

using namespace omniruntime::type;
using DataType = omniruntime::type::DataType;
//using IntDataType = omniruntime::type::IntDataType;
//using LongDataType = omniruntime::type::LongDataType;
//using DoubleDataType = omniruntime::type::DoubleDataType;
//using Decimal128DataType = omniruntime::type::Decimal128DataType;

namespace TypeSerializationTest {
TEST(DataTypeSerializer, serialization)
{
    std::vector<DataTypePtr> fieldTypes {IntType(), Decimal128Type(20, 10), VarcharType(256) };
    std::vector<DataTypePtr> types = {IntType(),
                                      DoubleType(),
                                      BooleanType(),
                                      LongType(),
                                      Date32Type(MILLI),
                                      Date64Type(MILLI),
                                      Time32Type(SEC),
                                      Time64Type(MICROSEC),
                                      VarcharType(1024),
                                      CharType(512),
                                      Decimal128Type(30, 20),
                                      Decimal64Type(20, 10),
                                      ContainerType(fieldTypes) };
    std::string typesJson = omniruntime::type::Serialize(types);
    ContainerDataTypePtr dataTypes = omniruntime::type::Deserialize(typesJson);
    auto dataTypePtrs = dataTypes->GetFieldTypes();
    EXPECT_EQ(dataTypePtrs.size(), types.size());
    for (uint32_t i = 0; i < dataTypePtrs.size(); i++) {

        EXPECT_EQ(dataTypePtrs[i]->GetId(), types[i]->GetId());
        EXPECT_EQ(dataTypePtrs[i]->GetWidth(), types[i]->GetWidth());
        EXPECT_EQ(dataTypePtrs[i]->GetPrecision(), types[i]->GetPrecision());
        EXPECT_EQ(dataTypePtrs[i]->GetScale(), types[i]->GetScale());
        EXPECT_EQ(dataTypePtrs[i]->GetDateUnit(), types[i]->GetDateUnit());
        EXPECT_EQ(dataTypePtrs[i]->GetTimeUnit(), types[i]->GetTimeUnit());
    }
//    EXPECT_EQ(dataTypes[0]->GetId(), OMNI_INT);
//    EXPECT_EQ(dataTypes[1]->GetId(), OMNI_DOUBLE);
//    EXPECT_EQ(dataTypes[2]->GetId(), OMNI_DECIMAL128);
//    EXPECT_EQ(static_cast<Decimal128DataType *>(&dataTypes[2])->GetScale(), decimal.GetScale());
//    EXPECT_EQ(static_cast<Decimal128DataType *>(&dataTypes[2])->GetPrecision(), decimal.GetPrecision());
//    EXPECT_EQ(static_cast<Decimal128DataType *>(&dataTypes[2])->GetScale(), decimal.GetScale());
//    EXPECT_EQ(dataTypes[3].GetId(), OMNI_LONG);
//    EXPECT_EQ(dataTypes[4].GetId(), OMNI_DATE32);
//    EXPECT_EQ(static_cast<Date32DataType *>(&dataTypes[4])->GetDateUnit(), MILLI);
//    EXPECT_EQ(dataTypes[5].GetId(), OMNI_DATE64);
//    EXPECT_EQ(static_cast<Date32DataType *>(&dataTypes[5])->GetDateUnit(), MILLI);
//    EXPECT_EQ(dataTypes[6].GetId(), OMNI_VARCHAR);
//    EXPECT_EQ(static_cast<VarcharDataType *>(&dataTypes[6])->GetWidth(), 1024);
    //    EXPECT_EQ(dataTypes[7].GetId(), OMNI_CONTAINER);
    //    EXPECT_EQ(dataTypes[7].GetFieldTypes()[0].GetId(), OMNI_INT);
    //    EXPECT_EQ(dataTypes[7].GetFieldTypes()[1].GetId(), OMNI_DECIMAL128);
    //    EXPECT_EQ(dataTypes[7].GetFieldTypes()[1].GetPrecision(), 20);
    //    EXPECT_EQ(dataTypes[7].GetFieldTypes()[1].GetScale(), 10);
    //    EXPECT_EQ(dataTypes[7].GetFieldTypes()[2].GetId(), OMNI_VARCHAR);
    //    EXPECT_EQ(dataTypes[7].GetFieldTypes()[2].GetWidth(), 512);

//    auto dataTypesCopy = dataTypes;
//    dataTypes = dataTypesCopy.Get();
//    EXPECT_EQ(dataTypes.size(), types.size());
//    EXPECT_EQ(dataTypes[0].GetId(), OMNI_INT);
//    EXPECT_EQ(dataTypes[1].GetId(), OMNI_DOUBLE);
//    EXPECT_EQ(dataTypes[2].GetId(), OMNI_DECIMAL128);
//    EXPECT_EQ(static_cast<Decimal128DataType *>(&dataTypes[2])->GetScale(), decimal.GetScale());
//    EXPECT_EQ(static_cast<Decimal128DataType *>(&dataTypes[2])->GetPrecision(), decimal.GetPrecision());
//    EXPECT_EQ(static_cast<Decimal128DataType *>(&dataTypes[2])->GetScale(), decimal.GetScale());
//    EXPECT_EQ(dataTypes[3].GetId(), OMNI_LONG);
//    EXPECT_EQ(dataTypes[4].GetId(), OMNI_DATE32);
//    EXPECT_EQ(static_cast<Date32DataType *>(&dataTypes[4])->GetDateUnit(), MILLI);
//    EXPECT_EQ(dataTypes[5].GetId(), OMNI_DATE64);
//    EXPECT_EQ(static_cast<Date32DataType *>(&dataTypes[5])->GetDateUnit(), MILLI);
//    EXPECT_EQ(dataTypes[6].GetId(), OMNI_VARCHAR);
//    EXPECT_EQ(static_cast<VarcharDataType *>(&dataTypes[6])->GetWidth(), 1024);
//    std::string typeJson = omniruntime::type::SerializeSingle(IntDataType());
//    auto dataType = omniruntime::type::DeserializeSingle(typeJson);
//    EXPECT_EQ(dataType.GetId(), OMNI_INT);
}


TEST(DataTypeSerializer, DataTypeBaseOperatorTest)
{
    Date32DataType date32DataType = Date32DataType(MILLI);
    Date32DataType date32DataType2 = Date32DataType(DAY);
    ASSERT_TRUE(date32DataType != date32DataType2);
    date32DataType2 = date32DataType;
    EXPECT_EQ(date32DataType2.GetDateUnit(), MILLI);
    ASSERT_FALSE(date32DataType != date32DataType2);
    Date32DataType date32DataType3 = date32DataType2;
    EXPECT_EQ(date32DataType3.GetDateUnit(), MILLI);

    Time32DataType time32DataType = Time32DataType(MICROSEC);
    Time32DataType time32DataType2 = Time32DataType(SEC);
    ASSERT_TRUE(time32DataType != time32DataType2);
    time32DataType2 = time32DataType;
    EXPECT_EQ(time32DataType2.GetTimeUnit(), MICROSEC);
    Time32DataType time32DataType3 = time32DataType2;
    EXPECT_EQ(time32DataType3.GetTimeUnit(), MICROSEC);

    Decimal128DataType decimal128DataType = Decimal128DataType(20, 10);
    Decimal128DataType decimal128DataType2 = Decimal128DataType(30, 20);
    ASSERT_TRUE(decimal128DataType != decimal128DataType2);
    decimal128DataType2 = decimal128DataType;
    EXPECT_EQ(decimal128DataType2.GetPrecision(), decimal128DataType.GetPrecision());
    EXPECT_EQ(decimal128DataType2.GetScale(), decimal128DataType.GetScale());
    EXPECT_EQ(decimal128DataType2.GetPrecision(), 20);
    EXPECT_EQ(decimal128DataType2.GetScale(), 10);
    Decimal128DataType decimal128DataType3 = decimal128DataType2;
    EXPECT_EQ(decimal128DataType3.GetPrecision(), 20);
    EXPECT_EQ(decimal128DataType3.GetScale(), 10);

    VarcharDataType varcharDataType = VarcharDataType(1024);
    VarcharDataType varcharDataType2 = VarcharDataType(512);
    ASSERT_TRUE(varcharDataType != varcharDataType2);
    varcharDataType2 = varcharDataType;
    ASSERT_FALSE(varcharDataType != varcharDataType2);
    EXPECT_EQ(varcharDataType2.GetWidth(), varcharDataType.GetWidth());
    VarcharDataType varcharDataType3 = varcharDataType2;
    EXPECT_EQ(varcharDataType3.GetWidth(), varcharDataType.GetWidth());
    EXPECT_EQ(varcharDataType3.GetWidth(), 1024);

    std::vector<DataTypePtr> fieldTypes {IntType(), VarcharType(1024) };
    std::vector<DataTypePtr> fieldTypes2 {IntType(), Date32Type(MILLI) };
    ContainerDataType containerDataType = ContainerDataType(fieldTypes);
    ContainerDataType containerDataType2 = ContainerDataType(fieldTypes2);
//    ASSERT_FALSE()
}
}
