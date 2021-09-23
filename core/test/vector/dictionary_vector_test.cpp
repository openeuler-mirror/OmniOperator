/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "dictionary_vector.h"
#include "long_vector.h"
#include "int_vector.h"
#include "double_vector.h"
#include "boolean_vector.h"
#include "decimal128_vector.h"
#include "decimal128.h"
#include "varchar_vector.h"
#include <vector_allocator_factory.h>

using namespace omniruntime::vec;

TEST(DictionaryVector, appendVector)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    int *ids1 = new int[5];
    int *ids2 = new int[5];
    int *ids = new int[10];
    LongVector *dictionary = new LongVector(allocator, 100);
    for (int32_t i = 0; i < 5; i++) {
        ids1[i] = i;
        ids2[i] = i + 5;
    }

    DictionaryVector *src1 = new DictionaryVector(dictionary, ids1, 5);
    DictionaryVector *src2 = new DictionaryVector(dictionary, ids2, 5);

    DictionaryVector *dest = new DictionaryVector(dictionary, ids, 10);
    dest->Append(src1, 0, 5);
    dest->Append(src2, 5, 5);
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(dest->GetIds()[i], i);
    }
    delete dictionary;
    delete src1;
    delete src2;
    delete dest;
    delete[] ids1;
    delete[] ids2;
    delete[] ids;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(DictionaryVector, CopyRegion)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    auto *dictionary = new LongVector(allocator, 10);
    for (int32_t i = 0; i < 10; i++) {
        dictionary->SetValue(i, i);
    }

    int32_t ids[] = {6, 8, 9};
    auto *dictionaryVector = new DictionaryVector(dictionary, ids, 3);
    DictionaryVector *copyRegion = dictionaryVector->CopyRegion(1, 2);
    EXPECT_EQ(copyRegion->GetLong(0), dictionary->GetValue(8));
    EXPECT_EQ(copyRegion->GetLong(1), dictionary->GetValue(9));

    delete dictionary;
    delete dictionaryVector;
    delete copyRegion;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(DictionaryVector, copyPostions)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    auto *dictionary = new LongVector(allocator, 10);
    for (int32_t i = 0; i < 10; i++) {
        dictionary->SetValue(i, i);
    }

    int32_t ids[] = {2, 3, 4, 5, 6, 8, 9};
    auto *dictionaryVector = new DictionaryVector(dictionary, ids, 7);
    int32_t positions[] = {1, 3, 5, 6};
    DictionaryVector *copyPositions = dictionaryVector->CopyPositions(positions, 1, 3);
    EXPECT_EQ(copyPositions->GetLong(0), dictionary->GetValue(5));
    EXPECT_EQ(copyPositions->GetLong(1), dictionary->GetValue(8));
    EXPECT_EQ(copyPositions->GetLong(2), dictionary->GetValue(9));

    delete dictionary;
    delete dictionaryVector;
    delete copyPositions;

    // dictionary data compress
    auto *dictionary1 = new LongVector(allocator, 2);
    dictionary1->SetValue(0, 100);
    dictionary1->SetValue(1, 200);
    int32_t ids1[] = {0, 0, 0, 1, 1, 1};
    auto *dictionaryVector1 = new DictionaryVector(dictionary1, ids1, 6);
    int32_t positions1[] = {1, 2, 3, 5};
    DictionaryVector *copyPositions1 = dictionaryVector1->CopyPositions(positions1, 0, 4);

    EXPECT_EQ(copyPositions1->GetLong(0), dictionary1->GetValue(0));
    EXPECT_EQ(copyPositions1->GetLong(1), dictionary1->GetValue(0));
    EXPECT_EQ(copyPositions1->GetLong(2), dictionary1->GetValue(1));
    EXPECT_EQ(copyPositions1->GetLong(3), dictionary1->GetValue(1));

    delete dictionary1;
    delete dictionaryVector1;
    delete copyPositions1;

    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(DictionaryVector, LongType)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    auto *dictionary = new LongVector(allocator, 10);
    for (int32_t i = 0; i < 10; i++) {
        dictionary->SetValue(i, i);
    }

    int32_t ids[] = {6, 8, 9};
    auto *dictionaryVector = new DictionaryVector(dictionary, ids, 3);
    EXPECT_EQ(dictionaryVector->GetLong(0), dictionary->GetValue(6));
    EXPECT_EQ(dictionaryVector->GetLong(1), dictionary->GetValue(8));
    EXPECT_EQ(dictionaryVector->GetLong(2), dictionary->GetValue(9));
    int32_t nestedIds[] = {1, 2};
    auto *nested = new DictionaryVector(dictionaryVector, nestedIds, 2);
    EXPECT_EQ(nested->GetLong(0), dictionary->GetValue(8));
    EXPECT_EQ(nested->GetLong(1), dictionary->GetValue(9));

    delete dictionary;
    delete dictionaryVector;
    delete nested;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(DictionaryVector, IntType)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    auto *dictionary = new IntVector(allocator, 10);
    for (int32_t i = 0; i < 10; i++) {
        dictionary->SetValue(i, i);
    }

    int32_t ids[] = {6, 8, 9};
    auto *dictionaryVector = new DictionaryVector(dictionary, ids, 3);
    EXPECT_EQ(dictionaryVector->GetInt(0), dictionary->GetValue(6));
    EXPECT_EQ(dictionaryVector->GetInt(1), dictionary->GetValue(8));
    EXPECT_EQ(dictionaryVector->GetInt(2), dictionary->GetValue(9));
    int32_t nestedIds[] = {1, 2};
    auto *nested = new DictionaryVector(dictionaryVector, nestedIds, 2);
    EXPECT_EQ(nested->GetInt(0), dictionary->GetValue(8));
    EXPECT_EQ(nested->GetInt(1), dictionary->GetValue(9));

    delete dictionary;
    delete dictionaryVector;
    delete nested;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(DictionaryVector, BooleanType)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    auto *dictionary = new BooleanVector(allocator, 10);
    for (int32_t i = 0; i < 10; i++) {
        dictionary->SetValue(i, i % 2 == 0);
    }

    int32_t ids[] = {6, 8, 9};
    auto *dictionaryVector = new DictionaryVector(dictionary, ids, 3);
    EXPECT_EQ(dictionaryVector->GetBoolean(0), dictionary->GetValue(6));
    EXPECT_EQ(dictionaryVector->GetBoolean(1), dictionary->GetValue(8));
    EXPECT_EQ(dictionaryVector->GetBoolean(2), dictionary->GetValue(9));
    int32_t nestedIds[] = {1, 2};
    auto *nested = new DictionaryVector(dictionaryVector, nestedIds, 2);
    EXPECT_EQ(nested->GetBoolean(0), dictionary->GetValue(8));
    EXPECT_EQ(nested->GetBoolean(1), dictionary->GetValue(9));

    delete dictionary;
    delete dictionaryVector;
    delete nested;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(DictionaryVector, DoubleType)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    auto *dictionary = new DoubleVector(allocator, 10);
    for (int32_t i = 0; i < 10; i++) {
        dictionary->SetValue(i, 2.3 * i);
    }

    int32_t ids[] = {6, 8, 9};
    auto *dictionaryVector = new DictionaryVector(dictionary, ids, 3);
    EXPECT_TRUE(dictionaryVector->GetDouble(0) - dictionary->GetValue(6) <= DBL_EPSILON);
    EXPECT_TRUE(dictionaryVector->GetDouble(1) - dictionary->GetValue(8) <= DBL_EPSILON);
    EXPECT_TRUE(dictionaryVector->GetDouble(2) - dictionary->GetValue(9) <= DBL_EPSILON);
    int32_t nestedIds[] = {1, 2};
    auto *nested = new DictionaryVector(dictionaryVector, nestedIds, 2);
    EXPECT_TRUE(nested->GetDouble(0) - dictionary->GetValue(8) <= DBL_EPSILON);
    EXPECT_TRUE(nested->GetDouble(1) - dictionary->GetValue(9) <= DBL_EPSILON);

    delete dictionary;
    delete dictionaryVector;
    delete nested;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(DictionaryVector, VarcharType)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    auto *dictionary = new VarcharVector(allocator, 1024, 10);
    for (int32_t i = 0; i < 10; i++) {
        std::string tmpStr = std::to_string(i);
        dictionary->SetValue(i, reinterpret_cast<const uint8_t *>(tmpStr.c_str()), tmpStr.length());
    }

    int32_t ids[] = {6, 8, 9};
    auto *dictionaryVector = new DictionaryVector(dictionary, ids, 3);
    std::vector<std::string> result;
    for (int i = 0; i < dictionaryVector->GetSize(); i++) {
        uint8_t *actual = nullptr;
        int32_t dataLen = dictionaryVector->GetVarchar(i, &actual);
        std::string tmpStr(actual, actual + dataLen);
        result.push_back(tmpStr);
    }
    EXPECT_EQ(result[0], std::to_string(6));
    EXPECT_EQ(result[1], std::to_string(8));
    EXPECT_EQ(result[2], std::to_string(9));

    int32_t nestedIds[] = {1, 2};
    auto *nested = new DictionaryVector(dictionaryVector, nestedIds, 2);
    result.clear();
    for (int i = 0; i < nested->GetSize(); i++) {
        uint8_t *actual = nullptr;
        int32_t dataLen = nested->GetVarchar(i, &actual);
        std::string tmpStr(actual, actual + dataLen);
        result.push_back(tmpStr);
    }
    EXPECT_EQ(result[0], std::to_string(8));
    EXPECT_EQ(result[1], std::to_string(9));

    delete dictionary;
    delete dictionaryVector;
    delete nested;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(DictionaryVector, Decimal128)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    auto *dictionary = new Decimal128Vector(allocator, 10);
    for (int32_t i = 0; i < 10; i++) {
        Decimal128 decimal128(i, i);
        dictionary->SetValue(i, decimal128);
    }

    int32_t ids[] = {6, 8, 9};
    auto *dictionaryVector = new DictionaryVector(dictionary, ids, 3);
    EXPECT_EQ(dictionaryVector->GetDecimal128(0), dictionary->GetValue(6));
    EXPECT_EQ(dictionaryVector->GetDecimal128(1), dictionary->GetValue(8));
    EXPECT_EQ(dictionaryVector->GetDecimal128(2), dictionary->GetValue(9));
    int32_t nestedIds[] = {1, 2};
    auto *nested = new DictionaryVector(dictionaryVector, nestedIds, 2);
    EXPECT_EQ(nested->GetDecimal128(0), dictionary->GetValue(8));
    EXPECT_EQ(nested->GetDecimal128(1), dictionary->GetValue(9));

    delete dictionary;
    delete dictionaryVector;
    delete nested;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}