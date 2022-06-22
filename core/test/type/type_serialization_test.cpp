/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <nlohmann/json.hpp>
#include <gtest/gtest.h>
#include <type/data_type_serializer.h>

using namespace omniruntime::type;
using DataType = omniruntime::type::DataType;
//using IntDataType = omniruntime::type::IntDataType;
//using LongDataType = omniruntime::type::LongDataType;
//using DoubleDataType = omniruntime::type::DoubleDataType;
//using Decimal128DataType = omniruntime::type::Decimal128DataType;

namespace TypeSerializationTest {
TEST(DataTypeSerializer, serialization)
{
    std::vector<DataTypeRawPtr> types = {  new IntDataType,
        new DoubleDataType(),
        new BooleanDataType(),
        new LongDataType(),
        new Date32DataType(MILLI),
        new Date64DataType(MILLI),
        new Time32DataType(SEC),
        new Time64DataType(MICROSEC),
        new VarcharDataType(1024),
        new CharDataType(512),
        new Decimal128DataType(30, 20),
        new Decimal64DataType(20, 10),
        new ContainerDataType(std::vector<DataTypeRawPtr> { new IntDataType(), new Decimal128DataType(20, 10),
            new VarcharDataType(256) }) };
    std::string typesJson = omniruntime::type::Serialize(types);
    DataTypes dataTypes = omniruntime::type::Deserialize(typesJson);
    auto dataTypeRawPtrs = dataTypes.Get();
    EXPECT_EQ(dataTypeRawPtrs.size(), types.size());
    for (uint32_t i = 0; i < dataTypeRawPtrs.size(); i++) {
        EXPECT_EQ(dataTypeRawPtrs[i]->GetId(), types[i]->GetId());
        EXPECT_EQ(dataTypeRawPtrs[i]->GetWidth(), types[i]->GetWidth());
        EXPECT_EQ(dataTypeRawPtrs[i]->GetPrecision(), types[i]->GetPrecision());
        EXPECT_EQ(dataTypeRawPtrs[i]->GetScale(), types[i]->GetScale());
        EXPECT_EQ(dataTypeRawPtrs[i]->GetDateUnit(), types[i]->GetDateUnit());
        EXPECT_EQ(dataTypeRawPtrs[i]->GetTimeUnit(), types[i]->GetTimeUnit());
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

TEST(DataTypeSerializer, ExtendSerialization)
{
    auto *charDataType = new CharDataType(512);
    auto *varcharDataType = new VarcharDataType(1024);
    auto *intDataType = new IntDataType();
    auto *longDataType = new LongDataType();
    auto *date32DataType = new Date32DataType(MILLI);
    auto *date64DataType = new Date64DataType(MILLI);
    auto *time32DataType = new Time32DataType(SEC);
    auto *time64DataType = new Time64DataType(MICROSEC);
    auto *decimal64DataType = new Decimal64DataType(30, 20);
    auto *decimal128DataType = new Decimal128DataType(20, 10);
    auto *varcharDataType2 = new VarcharDataType(512);
    auto *containerDataType = new ContainerDataType(
        std::vector<DataType *> { intDataType, decimal128DataType, time64DataType, date64DataType, varcharDataType2 });

    std::vector<DataType *> types = { intDataType,       longDataType,       charDataType,     varcharDataType,
        date32DataType,    date64DataType,     time32DataType,   time64DataType,
        decimal64DataType, decimal128DataType, containerDataType };

    std::string typesJson = omniruntime::type::Serialize(types);
    std::string typesJson2 = omniruntime::type::SerializeSingle(varcharDataType);
    std::string typesJson3 = omniruntime::type::SerializeSingle(containerDataType);
    std::string typesJson4 = omniruntime::type::SerializeSingle(date32DataType);
    std::cout << "typesJson :" << typesJson << std::endl;
    std::cout << "varchar2 :" << typesJson2 << std::endl;
    std::cout << "varchar3 :" << typesJson3 << std::endl;
    std::cout << "varchar4 :" << typesJson4 << std::endl;
    auto dataType1 = omniruntime::type::Deserialize(typesJson);
    //        auto dataType2 = omniruntime::type::DeserializeSingle(typesJson2);
    std::cout << "" << std::endl;
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

    DataTypeRawPtr intd = new IntDataType() ;
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

    std::vector<DataTypeRawPtr> fieldTypes = std::vector<DataTypeRawPtr> {new IntDataType, new VarcharDataType(1024) };
    std::vector<DataTypeRawPtr> fieldTypes2 = std::vector<DataTypeRawPtr> {new IntDataType, new Date32DataType(MILLI) };
    ContainerDataType containerDataType = ContainerDataType(fieldTypes);
}
}
