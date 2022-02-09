/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <sstream>
#include "gtest/gtest.h"
#include "vector_common.h"

using namespace omniruntime::vec;

TEST(VarcharVector, newVector)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);
    VarcharVector *vector = new VarcharVector(allocator, 1024, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetCapacityInBytes(), 1024);
    EXPECT_EQ(vector->GetTypeId(), OMNI_VEC_TYPE_VARCHAR);
    delete vector;

    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(VarcharVector, sliceVector)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    int size = 10;
    VarcharVector *vector = new VarcharVector(allocator, 1024, size);
    std::string s = "testvarchar";
    for (int i = 0; i < size; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        vector->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    int offset = 3;
    VarcharVector *sliceVector1 = vector->Slice(offset, 4);
    EXPECT_EQ(sliceVector1->GetPositionOffset(), offset);
    EXPECT_EQ(sliceVector1->GetSize(), 4);
    EXPECT_EQ(sliceVector1->GetReference(), 2);

    for (int i = 0; i < sliceVector1->GetSize(); i++) {
        std::string str(s, 0, i + 3);
        str.append(std::to_string(i + 3));
        uint8_t *actualChar = nullptr;
        int len = sliceVector1->GetValue(i, &actualChar);
        std::string actualStr(reinterpret_cast<char *>(actualChar), 0, len);
        EXPECT_EQ(actualStr, str);
    }

    VarcharVector *sliceVector2 = sliceVector1->Slice(1, 2);
    EXPECT_EQ(sliceVector2->GetPositionOffset(), offset + 1);
    EXPECT_EQ(sliceVector2->GetSize(), 2);
    EXPECT_EQ(sliceVector2->GetReference(), 3);

    for (int i = 0; i < sliceVector2->GetSize(); i++) {
        std::string str(s, 0, i + 4);
        str.append(std::to_string(i + 4));
        uint8_t *actualChar = nullptr;
        int len = sliceVector2->GetValue(i, &actualChar);
        std::string actualStr(reinterpret_cast<char *>(actualChar), 0, len);
        EXPECT_EQ(actualStr, str);
    }

    delete vector;
    EXPECT_EQ(sliceVector1->GetReference(), 2);

    delete sliceVector1;
    EXPECT_EQ(sliceVector2->GetReference(), 1);

    delete sliceVector2;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test set/get
TEST(VarcharVector, setAndGetValue)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator, 1024, 4);
    std::string s = "test";
    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        vector->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        uint8_t *actualChar = nullptr;
        int len = vector->GetValue(i, &actualChar);
        std::string actualStr(reinterpret_cast<char *>(actualChar), 0, len);
        EXPECT_EQ(actualStr, str);
    }

    delete vector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test is null
TEST(VarcharVector, setValueNull)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator,1024, 256);
    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            vector->SetValueNull(i);
        } else {
            std::string str = std::to_string(i);
            vector->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
        }
    }

    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            EXPECT_TRUE(vector->IsValueNull(i));
        } else {
            uint8_t *actual = nullptr;
            int len = vector->GetValue(i, &actual);
            std::string actualStr(reinterpret_cast<char *>(actual), 0, len);
            EXPECT_EQ(actualStr, std::to_string(i));
        }
    }
    delete vector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test is copyPosition
TEST(VarcharVector, copyPositions)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator, 1024, 4);
    std::string s = "test";
    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        vector->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    int *positions = new int[2];
    positions[0] = 1;
    positions[1] = 3;
    VarcharVector *copyPostionVector = vector->CopyPositions(positions, 0, 2);

    for (int i = 0; i < copyPostionVector->GetSize(); i++) {
        uint8_t *expectedChar = nullptr;
        int len = vector->GetValue(positions[i], &expectedChar);
        std::string expectedStr(reinterpret_cast<char *>(expectedChar), 0, len);

        uint8_t *actualChar = nullptr;
        int len1 = copyPostionVector->GetValue(i, &actualChar);
        std::string actualStr(reinterpret_cast<char *>(actualChar), 0, len1);
        EXPECT_EQ(actualStr, expectedStr);
    }

    delete vector;
    delete copyPostionVector;
    delete[] positions;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test is CopyRegion
TEST(VarcharVector, copyRegion)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *vector = new VarcharVector(allocator, 1024, 4);
    std::string s = "test";
    for (int i = 0; i < 4; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        vector->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    VarcharVector *copyRegionVector = vector->CopyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->GetSize(); i++) {
        uint8_t *expectedChar = nullptr;
        int len = vector->GetValue(i + 2, &expectedChar);
        std::string expectedStr(reinterpret_cast<char *>(expectedChar), 0, len);

        uint8_t *actualChar = nullptr;
        int len1 = copyRegionVector->GetValue(i, &actualChar);
        std::string actualStr(reinterpret_cast<char *>(actualChar), 0, len1);
        EXPECT_EQ(actualStr, expectedStr);
    }

    delete vector;
    delete copyRegionVector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

TEST(VarcharVector, emptyString)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    std::vector<std::string> data = {"e", "abc", "", "hg", ""};
    int size = 5;
    VarcharVector *original = new VarcharVector(allocator, 1024, size);
    for (int i = 0; i < size; i++) {
        original->SetValue(i, reinterpret_cast<const uint8_t *>(data[i].c_str()), data[i].length());
    }

    for (int i = 0; i < size; i++) {
        uint8_t *actualChar = nullptr;
        int len = original->GetValue(i, &actualChar);
        std::string actualStr(actualChar, actualChar + len);
        EXPECT_EQ(actualStr, data[i]);
    }

    delete original;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

TEST(VarcharVector, appendVector)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    auto *src1 = new VarcharVector(allocator, 1024, 5);
    auto *src2 = new VarcharVector(allocator, 1024, 5);
    for (int i = 0; i < 5; i++) {
        std::string str = std::to_string(i + 1);
        src1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
        std::string str1 = std::to_string(i + 6);
        src2->SetValue(i, reinterpret_cast<const uint8_t *>(str1.c_str()), str1.length());
    }

    auto *appended = new VarcharVector(allocator, 1024, 13);
    appended->Append(src1, 0, 5);
    appended->Append(src2, 5, 5);
    int32_t ids[3] = {1, 2, 3};
    auto *dictionaryVector = new DictionaryVector(src1, ids, 3);
    appended->Append(dictionaryVector, 10, 3);

    for (int i = 0; i < 10; i++) {
        uint8_t *actualChar = nullptr;
        int32_t len = appended->GetValue(i, &actualChar);
        std::string result(actualChar, actualChar + len);
        EXPECT_EQ(result, std::to_string(i + 1));
    }

    for (int i = 10; i < 13; i++) {
        uint8_t *actualChar = nullptr;
        int32_t len = appended->GetValue(i, &actualChar);
        uint8_t *actualChar1 = nullptr;
        int32_t len1 = dictionaryVector->GetVarchar(i % 10, &actualChar1);
        std::string result(actualChar, actualChar + len);
        std::string result1(actualChar1, actualChar1 + len1);
        EXPECT_EQ(result, result1);
    }

    delete src1;
    delete src2;
    delete dictionaryVector;
    delete appended;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(VarcharVector, setValueExpand)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);

    // specify initial capacity expansion
    int32_t rows = 4;
    auto *vector = new VarcharVector(allocator, 1, rows);
    std::string s = "test";
    for (int i = 0; i < rows; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        vector->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    const int32_t expectedExpandedCapacity = 16;
    EXPECT_EQ(vector->GetCapacityInBytes(), expectedExpandedCapacity);

    for (int i = 0; i < rows; i++) {
        std::string str(s, 0, i);
        str.append(std::to_string(i));
        uint8_t *actualChar = nullptr;
        int len = vector->GetValue(i, &actualChar);
        std::string actualStr(reinterpret_cast<char *>(actualChar), 0, len);
        EXPECT_EQ(actualStr, str);
    }

    // no capacity specified when created
    rows = 8000;
    auto *vector1 = new VarcharVector(allocator, rows);
    for (int i = 0; i < rows; i++) {
        std::string str(s, 0, s.length());
        str.append(std::to_string(i));
        vector1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    // init capacity is 32K, expansion to 64k at a time
    const int32_t expectedExpandedCapacity1 = 32 * 1024 * 2;
    EXPECT_EQ(vector1->GetCapacityInBytes(), expectedExpandedCapacity1);

    for (int i = 0; i < rows; i++) {
        std::string str(s, 0, s.length());
        str.append(std::to_string(i));
        uint8_t *actualChar = nullptr;
        int len = vector1->GetValue(i, &actualChar);
        std::string actualStr(reinterpret_cast<char *>(actualChar), 0, len);
        EXPECT_EQ(actualStr, str);
    }

    delete vector;
    delete vector1;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

TEST(VarcharVector, appendExpand)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    auto *src1 = new VarcharVector(allocator, 1024, 5);
    auto *src2 = new VarcharVector(allocator, 1024, 5);

    for (int i = 0; i < 5; i++) {
        std::string str = std::to_string(i + 1);
        src1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
        std::string str1 = std::to_string(i + 6);
        src2->SetValue(i, reinterpret_cast<const uint8_t *>(str1.c_str()), str1.length());
    }

    auto *appended = new VarcharVector(allocator, 10, 10);
    appended->Append(src1, 0, 5);
    appended->Append(src2, 5, 5);

    const int32_t expectedExpandCapacity = 20;
    EXPECT_EQ(appended->GetCapacityInBytes(), expectedExpandCapacity);
    for (int i = 0; i < 10; i++) {
        uint8_t *actualChar = nullptr;
        int32_t len = appended->GetValue(i, &actualChar);
        std::string result(actualChar, actualChar + len);
        EXPECT_EQ(result, std::to_string(i + 1));
    }

    delete src1;
    delete src2;
    delete appended;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}