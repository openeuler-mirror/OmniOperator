/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <sstream>
#include <cstring>
#include "gtest/gtest.h"
#include "vector.h"
#include "vector_allocator.h"
#include "vector_allocator_manager.h"
#include "varchar_vector.h"

using namespace omniruntime::vec;

TEST(VarcharVector, newVector)
{
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("varchar");
    EXPECT_TRUE(allocator != nullptr);
    VarcharVector *vector = new VarcharVector(allocator, 1024, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetCapacityInBytes(), 1024);
    EXPECT_EQ(vector->GetType().GetId(), OMNI_VEC_TYPE_VARCHAR);
    delete vector;

    manager.DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(VarcharVector, sliceVector)
{
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("varchar");
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
    manager.DeleteAllocator(&allocator);
}

// Test set/get
TEST(VarcharVector, setAndGetValue)
{
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("varchar");
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
    manager.DeleteAllocator(&allocator);
}

// Test is null
TEST(VarcharVector, SetValueNull)
{
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("varchar");
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
    manager.DeleteAllocator(&allocator);
}


// Test is copyPosition
TEST(VarcharVector, CopyPositions)
{
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("varchar");
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
    manager.DeleteAllocator(&allocator);
}

// Test is CopyRegion
TEST(VarcharVector, CopyRegion)
{
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("varchar");
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
    manager.DeleteAllocator(&allocator);
}

TEST(VarcharVector, jniFreeVector)
{
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    VarcharVector *oritianlVector = new VarcharVector(allocator, 1024, 256);
    Vector *vector = (Vector *)oritianlVector;
    delete vector;
}

TEST(VarcharVector, emptyString)
{
    VectorAllocatorManager manager = VectorAllocatorManager::GetInstance();
    VectorAllocator *allocator = manager.GetOrCreateAllocator("test");
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
    manager.DeleteAllocator(&allocator);
}