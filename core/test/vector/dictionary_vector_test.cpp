/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <gtest/gtest.h>
#include "type/decimal128.h"
#include "vector_common.h"

using namespace omniruntime::vec;

namespace DictionaryVectorTest {
TEST(DictionaryVector, appendVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("appendVector");
    EXPECT_TRUE(allocator != nullptr);
    int *ids1 = new int[5];
    int *ids2 = new int[5];
    int *ids = new int[10];
    LongVector *dictionary = new LongVector(allocator, 100);
    for (int32_t i = 0; i < 5; i++) {
        ids1[i] = i;
        ids2[i] = i + 5;
    }
    for (int32_t i = 0; i < 10; i++) {
        ids[i] = 0;
    }
    DictionaryVector *src1 = new DictionaryVector(dictionary, ids1, 5);
    DictionaryVector *src2 = new DictionaryVector(dictionary, ids2, 5);

    DictionaryVector *dest = new DictionaryVector(dictionary, ids, 10);
    dest->Append(src1, 0, 5);
    dest->Append(src2, 5, 5);
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(dest->GetId(i), i);
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
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("CopyRegion");
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
    delete allocator;
}

TEST(DictionaryVector, copyPostions)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("copyPositions");
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

    delete allocator;
}

TEST(DictionaryVector, LongType)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("longType");
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
    delete allocator;
}

TEST(DictionaryVector, IntType)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("intType");
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
    delete allocator;
}

TEST(DictionaryVector, BooleanType)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("booleanType");;
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
    delete allocator;
}

TEST(DictionaryVector, DoubleType)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("doubleType");
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
    delete allocator;
}

TEST(DictionaryVector, VarcharType)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("varcharType");
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
    delete allocator;
}

TEST(DictionaryVector, Decimal128)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("decimal128");
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
    delete allocator;
}

TEST(DictionaryVector, NestedDictionaryVectorExtract)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("nestedDicVecExtract");
    EXPECT_TRUE(allocator != nullptr);
    auto *dictionary = new LongVector(allocator, 10);
    for (int32_t i = 0; i < 10; i++) {
        dictionary->SetValue(i, i);
    }

    int32_t ids[] = {1, 1, 2, 2, 3, 3, 4, 4, 5, 5};
    auto *dictionaryVector = new DictionaryVector(dictionary, ids, 10);

    int32_t nestedIds[] = {1, 2, 3, 4, 5, 6, 7, 8};
    auto *nested = new DictionaryVector(dictionaryVector, nestedIds, 8);

    Vector *checkDictionary = nested->ExtractDictionary();
    EXPECT_EQ(nested->GetLong(0), dictionary->GetValue(1));
    EXPECT_EQ(nested->GetLong(1), dictionary->GetValue(2));
    EXPECT_EQ(nested->GetLong(2), dictionary->GetValue(2));
    EXPECT_EQ(nested->GetLong(3), dictionary->GetValue(3));
    EXPECT_EQ(nested->GetLong(4), dictionary->GetValue(3));
    delete dictionary;
    delete dictionaryVector;
    delete nested;
    delete checkDictionary;
    delete allocator;
}

TEST(DictionaryVector, NestedDictionaryVectorGetId)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("nestedDicVecGetId");
    EXPECT_TRUE(allocator != nullptr);
    auto *dictionary = new LongVector(allocator, 10);
    for (int32_t i = 0; i < 10; i++) {
        dictionary->SetValue(i, i);
    }

    int32_t ids[] = {1, 1, 2, 2, 3, 3, 4, 4, 5, 5};
    auto *dictionaryVector = new DictionaryVector(dictionary, ids, 10);

    int32_t nestedIds[] = {1, 2, 3, 4, 5, 6, 7, 8};
    auto *nested = new DictionaryVector(dictionaryVector, nestedIds, 8);

    int32_t originalId;
    nested->ExtractDictionaryAndId(0, originalId);
    EXPECT_EQ(originalId, 1);
    nested->ExtractDictionaryAndId(1, originalId);
    EXPECT_EQ(originalId, 2);
    nested->ExtractDictionaryAndId(2, originalId);
    EXPECT_EQ(originalId, 2);
    nested->ExtractDictionaryAndId(3, originalId);
    EXPECT_EQ(originalId, 3);
    nested->ExtractDictionaryAndId(4, originalId);
    EXPECT_EQ(originalId, 3);
    delete dictionary;
    delete dictionaryVector;
    delete nested;
    delete allocator;
}

TEST(DictionaryVector, NestedDictionaryVectorGetIds)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("nestedDicVecGetIds");
    EXPECT_TRUE(allocator != nullptr);
    auto *dictionary = new LongVector(allocator, 10);
    for (int32_t i = 0; i < 10; i++) {
        dictionary->SetValue(i, i);
    }

    int32_t ids[] = {1, 1, 2, 2, 3, 3, 4, 4, 5, 5};
    auto *dictionaryVector = new DictionaryVector(dictionary, ids, 10);

    int32_t nestedIds[] = {1, 2, 3, 4, 5, 6, 7, 8};
    auto *nested = new DictionaryVector(dictionaryVector, nestedIds, 8);

    int32_t originalIds[5];
    nested->ExtractDictionaryAndIds(0, 5, originalIds);
    EXPECT_EQ(originalIds[0], 1);
    EXPECT_EQ(originalIds[1], 2);
    EXPECT_EQ(originalIds[2], 2);
    EXPECT_EQ(originalIds[3], 3);
    EXPECT_EQ(originalIds[4], 3);
    delete dictionary;
    delete dictionaryVector;
    delete nested;
    delete allocator;
}
}
