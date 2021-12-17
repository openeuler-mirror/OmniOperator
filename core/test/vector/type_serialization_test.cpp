/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <nlohmann/json.hpp>
#include <gtest/gtest.h>
#include <vector_type_serializer.h>
using namespace omniruntime::vec;
using VecType = omniruntime::vec::VecType;
using IntVecType = omniruntime::vec::IntVecType;
using LongVecType = omniruntime::vec::LongVecType;
using DoubleVecType = omniruntime::vec::DoubleVecType;
using Decimal128VecType = omniruntime::vec::Decimal128VecType;

TEST(VecTypeSerializer, serialization)
{
    Decimal128VecType decimal(10, 2);
    std::vector<VecType> types = { IntVecType(),        DoubleVecType(),      decimal,
        LongVecType(),       Date32VecType(MILLI), Date64VecType(MILLI),
        VarcharVecType(1024) };
    std::string typesJson = omniruntime::vec::Serialize(types);
    auto vecTypes = omniruntime::vec::Deserialize(typesJson);
    auto check = vecTypes.Get();
    EXPECT_EQ(check.size(), types.size());
    EXPECT_EQ(check[0].GetId(), OMNI_VEC_TYPE_INT);
    EXPECT_EQ(check[1].GetId(), OMNI_VEC_TYPE_DOUBLE);
    EXPECT_EQ(check[2].GetId(), OMNI_VEC_TYPE_DECIMAL128);
    EXPECT_EQ(static_cast<Decimal128VecType *>(&check[2])->GetScale(), decimal.GetScale());
    EXPECT_EQ(static_cast<Decimal128VecType *>(&check[2])->GetPrecision(), decimal.GetPrecision());
    EXPECT_EQ(static_cast<Decimal128VecType *>(&check[2])->GetScale(), decimal.GetScale());
    EXPECT_EQ(check[3].GetId(), OMNI_VEC_TYPE_LONG);
    EXPECT_EQ(check[4].GetId(), OMNI_VEC_TYPE_DATE32);
    EXPECT_EQ(static_cast<Date32VecType *>(&check[4])->GetDateUnit(), MILLI);
    EXPECT_EQ(check[5].GetId(), OMNI_VEC_TYPE_DATE64);
    EXPECT_EQ(static_cast<Date32VecType *>(&check[5])->GetDateUnit(), MILLI);
    EXPECT_EQ(check[6].GetId(), OMNI_VEC_TYPE_VARCHAR);
    EXPECT_EQ(static_cast<VarcharVecType *>(&check[6])->GetWidth(), 1024);

    std::cout << omniruntime::vec::Serialize(types) << std::endl;

    auto vecTypesCopy = vecTypes;
    check = vecTypesCopy.Get();
    EXPECT_EQ(check.size(), types.size());
    EXPECT_EQ(check[0].GetId(), OMNI_VEC_TYPE_INT);
    EXPECT_EQ(check[1].GetId(), OMNI_VEC_TYPE_DOUBLE);
    EXPECT_EQ(check[2].GetId(), OMNI_VEC_TYPE_DECIMAL128);
    EXPECT_EQ(static_cast<Decimal128VecType *>(&check[2])->GetScale(), decimal.GetScale());
    EXPECT_EQ(static_cast<Decimal128VecType *>(&check[2])->GetPrecision(), decimal.GetPrecision());
    EXPECT_EQ(static_cast<Decimal128VecType *>(&check[2])->GetScale(), decimal.GetScale());
    EXPECT_EQ(check[3].GetId(), OMNI_VEC_TYPE_LONG);
    EXPECT_EQ(check[4].GetId(), OMNI_VEC_TYPE_DATE32);
    EXPECT_EQ(static_cast<Date32VecType *>(&check[4])->GetDateUnit(), MILLI);
    EXPECT_EQ(check[5].GetId(), OMNI_VEC_TYPE_DATE64);
    EXPECT_EQ(static_cast<Date32VecType *>(&check[5])->GetDateUnit(), MILLI);
    EXPECT_EQ(check[6].GetId(), OMNI_VEC_TYPE_VARCHAR);
    EXPECT_EQ(static_cast<VarcharVecType *>(&check[6])->GetWidth(), 1024);

    std::cout << omniruntime::vec::Serialize(types) << std::endl;
    std::string typeJson = omniruntime::vec::SerializeSingle(IntVecType());
    auto vecType = omniruntime::vec::DeserializeSingle(typeJson);
    EXPECT_EQ(vecType.GetId(), OMNI_VEC_TYPE_INT);
}
