/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "vector.h"
#include "vector_allocator.h"
#include "vector_allocator_manager.h"
#include "int_vector.h"
#include "long_vector.h"
#include "double_vector.h"
#include "varchar_vector.h"
#include "boolean_vector.h"
#include "decimal128.h"
#include "decimal128_vector.h"
#include "vector_helper.h"
#include "dictionary_vector.h"

using namespace omniruntime::vec;

TEST(VectorHelper, setAndGetValue)
{
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test_helper1");
    EXPECT_TRUE(allocator != nullptr);

    // int32 test
    auto *vector1 = new IntVector(allocator, 10);
    int32_t value1 = 100;
    VectorHelper::SetValue(vector1, 5, &value1);
    int32_t result1 = 0;
    VectorHelper::GetValue(vector1, 5, &result1);
    EXPECT_EQ(result1, value1);
    delete vector1;

    // int64 test
    auto *vector2 = new LongVector(allocator, 10);
    int64_t value2 = 1000;
    VectorHelper::SetValue(vector2, 5, &value2);
    int64_t result2 = 0;
    VectorHelper::GetValue(vector2, 5, &result2);
    EXPECT_EQ(result2, value2);
    delete vector2;

    // double test
    auto *vector3 = new DoubleVector(allocator, 10);
    double value3 = 33.333;
    VectorHelper::SetValue(vector3, 5, &value3);
    double result3 = 0.00;
    VectorHelper::GetValue(vector3, 5, &result3);
    EXPECT_EQ(result3, value3);
    delete vector3;

    // boolean test
    auto *vector4 = new BooleanVector(allocator, 10);
    bool value4 = true;
    VectorHelper::SetValue(vector4, 5, &value4);
    bool result4;
    VectorHelper::GetValue(vector4, 5, &result4);
    EXPECT_EQ(result4, value4);
    delete vector4;

    // varchar test
    auto *vector5 = new VarcharVector(allocator, 100, 10);
    std::string value5 = "testvectorhelper";
    VectorHelper::SetValue(vector5, 5, &value5);
    uint8_t *result5 = nullptr;
    int32_t len = VectorHelper::GetValue(vector5, 5, &result5);
    std::string expected(reinterpret_cast<char *>(result5), 0, len);
    EXPECT_EQ(expected, value5);
    delete vector5;

    // decimal test
    auto *vector6 = new Decimal128Vector(allocator, 10);
    Decimal128 value6(111, 222);
    VectorHelper::SetValue(vector6, 5, &value6);
    Decimal128 result6;
    VectorHelper::GetValue(vector6, 5, &result6);
    EXPECT_EQ(result6, value6);
    delete vector6;

    // dictionary test
    auto *dictionary = new LongVector(allocator, 10);
    int64_t value7 = 1000;
    VectorHelper::SetValue(dictionary, 5, &value7);
    int32_t ids[2] = {5, 5};
    int32_t *p = ids;
    auto *dictionaryVec = new DictionaryVector(dictionary, p, 2);
    int64_t result7 = 0;
    VectorHelper::GetValue(dictionaryVec, 0, &result7);
    EXPECT_EQ(result7, value7);
    delete dictionary;
    delete dictionaryVec;

    auto *dictionaryVarchar = new VarcharVector(allocator, 100, 10);
    std::string value8 = "testVarcharDictionary";
    VectorHelper::SetValue(dictionaryVarchar, 5, &value8);
    int32_t ids2[2] = {5, 5};
    int32_t *p2 = ids2;
    auto *dictionaryVarcharVec = new DictionaryVector(dictionaryVarchar, p2, 2);
    uint8_t *result8 = nullptr;
    int32_t len1 = VectorHelper::GetValue(dictionaryVarcharVec, 0, &result8);
    std::string expected1(reinterpret_cast<char *>(result8), len1);
    EXPECT_EQ(expected1, value8);
    delete dictionaryVarchar;
    delete dictionaryVarcharVec;

    manager.DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(VectorHelper, printVectorValue)
{
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test_helper2");
    EXPECT_TRUE(allocator != nullptr);

    auto *dictionaryVarchar = new VarcharVector(allocator, 100, 10);
    std::string value8 = "testvectorhelper";
    VectorHelper::SetValue(dictionaryVarchar, 5, &value8);
    int32_t ids2[2] = {5, 5};
    int32_t *p2 = ids2;
    auto *dictionaryVarcharVec = new DictionaryVector(dictionaryVarchar, p2, 2);
    for (int32_t i = 0; i < 2; i++) {
        VectorHelper::PrintVectorValue(dictionaryVarcharVec, i);
    }
    delete dictionaryVarchar;
    delete dictionaryVarcharVec;

    manager.DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}