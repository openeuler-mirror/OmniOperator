/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <gtest/gtest.h>
#include "vector_common.h"
#include "../util/test_util.h"

using namespace omniruntime::vec;
using namespace TestUtil;

namespace ShortVectorTest {
TEST(ShortVector, newVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ShortVector_newVector");
    EXPECT_TRUE(allocator != nullptr);
    ShortVector *vector = new ShortVector(allocator, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetCapacityInBytes(), 512);
    EXPECT_EQ(vector->GetTypeId(), OMNI_SHORT);
    delete vector;

    delete allocator;
}

TEST(ShortVector, sliceVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ShortVector_sliceVector");
    EXPECT_TRUE(allocator != nullptr);

    ShortVector *originalVector = new ShortVector(allocator, 10);
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, i * 2);
    }

    int offset = 3;
    ShortVector *slice1 = originalVector->Slice(offset, 4);
    EXPECT_EQ(slice1->GetPositionOffset(), offset);
    EXPECT_EQ(slice1->GetSize(), 4);
    EXPECT_EQ(slice1->GetReference(), 2);
    for (int i = 0; i < slice1->GetSize(); i++) {
        EXPECT_EQ(slice1->GetValue(i), originalVector->GetValue(i + offset));
    }

    ShortVector *slice2 = slice1->Slice(1, 2);
    for (int i = 0; i < slice2->GetSize(); i++) {
        EXPECT_EQ(slice2->GetValue(i), originalVector->GetValue(i + offset + 1));
    }

    delete originalVector;
    EXPECT_EQ(slice1->GetReference(), 2);

    delete slice1;
    EXPECT_EQ(slice2->GetReference(), 1);
    delete slice2;

    delete allocator;
}

// Test set/get
TEST(ShortVector, setAndGetValue)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ShortVector_setAndGetValue");
    EXPECT_TRUE(allocator != nullptr);

    ShortVector *vector = new ShortVector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        vector->SetValue(i, i * 2);
    }

    for (int i = 0; i < 256; i++) {
        EXPECT_EQ(vector->GetValue(i), i * 2);
    }
    delete vector;
    delete allocator;
}

// Test SetValues
TEST(ShortVector, setValues)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ShortVector_setValues");
    EXPECT_TRUE(allocator != nullptr);

    const int size = 5;
    int16_t values[size] = {1, 3, 4, 6, 7};
    int16_t *p = values;
    ShortVector *shortVector1 = new ShortVector(allocator, size);
    shortVector1->SetValues(0, p, size);
    for (int i = 0; i < size; i++) {
        EXPECT_EQ(shortVector1->GetValue(i), values[i]);
    }

    ShortVector *shortVector2 = new ShortVector(allocator, size);
    shortVector2->SetValues(1, p + 2, 3);
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(shortVector2->GetValue(i + 1), values[i + 2]);
    }

    delete shortVector1;
    delete shortVector2;
    delete allocator;
}

// Test SetValues/get
TEST(ShortVector, setValuesWithoutOffset)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ShortVector_setValuesWithoutOffset");
    EXPECT_TRUE(allocator != nullptr);

    ShortVector *vector = new ShortVector(allocator, 256);
    int16_t *value = new int16_t[256];
    for (int i = 0; i < 256; i++) {
        value[i] = i * 2;
    }
    vector->SetValues(0, value, 256);
    for (int i = 0; i < 256; i++) {
        EXPECT_EQ(vector->GetValue(i), i * 2);
    }

    delete[] value;
    delete vector;
    delete allocator;
}

// Test SetValues/get with offset
TEST(ShortVector, setValuesWithOffset)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ShortVector_setValuesWithOffset");
    EXPECT_TRUE(allocator != nullptr);

    ShortVector *vector = new ShortVector(allocator, 256);
    int16_t *value = new int16_t[256];
    for (int i = 0; i < 256; i++) {
        value[i] = i * 2;
    }
    vector->SetValues(128, &value[128], 128);
    for (int i = 128; i < 256; i++) {
        EXPECT_EQ(vector->GetValue(i), i * 2);
    }

    delete[] value;
    delete vector;
    delete allocator;
}

// Test is null
TEST(ShortVector, setValueNull)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ShortVector_setValueNull");
    EXPECT_TRUE(allocator != nullptr);

    ShortVector *vector = new ShortVector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            vector->SetValueNull(i);
        } else {
            vector->SetValue(i, i);
        }
    }
    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            EXPECT_TRUE(vector->IsValueNull(i));
        } else {
            EXPECT_EQ(vector->GetValue(i), i);
        }
    }
    delete vector;
    delete allocator;
}

// Test is copyPosition
TEST(ShortVector, copyPositions)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ShortVector_copyPositions");
    EXPECT_TRUE(allocator != nullptr);

    ShortVector *originalVector = new ShortVector(allocator, 4);
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, i);
    }

    int *positions = new int[2];
    positions[0] = 1;
    positions[1] = 3;
    ShortVector *copyPositionVector = originalVector->CopyPositions(positions, 0, 2);

    for (int i = 0; i < copyPositionVector->GetSize(); i++) {
        EXPECT_EQ(copyPositionVector->GetValue(i), originalVector->GetValue(positions[i]));
    }

    delete[] positions;
    delete originalVector;
    delete copyPositionVector;
    delete allocator;
}

// Test is CopyRegion
TEST(ShortVector, copyRegion)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ShortVector_copyPosition");
    EXPECT_TRUE(allocator != nullptr);

    ShortVector *originalVector = new ShortVector(allocator, 4);
    for (int i = 0; i < 4; i++) {
        originalVector->SetValue(i, i * 2);
    }

    ShortVector *copyRegionVector = originalVector->CopyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->GetSize(); i++) {
        EXPECT_EQ(copyRegionVector->GetValue(i), originalVector->GetValue(i + 2));
    }

    delete originalVector;
    delete copyRegionVector;
    delete allocator;
}

TEST(ShortVector, jniFreeVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ShortVector_jniFreeVector");
    EXPECT_TRUE(allocator != nullptr);

    ShortVector *originalVector = new ShortVector(allocator, 256);
    Vector *vector = (Vector *)originalVector;
    std::cout << typeid(*vector).hash_code() << std::endl;
    delete vector;
}

TEST(ShortVector, testNullFlagWithSet)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithSet");
    // no null value
    auto *noNull = new ShortVector(allocator, 10);
    EXPECT_FALSE(noNull->MayHaveNull());
    delete noNull;

    // has null value
    auto *hasNulls = new ShortVector(allocator, 10);
    std::vector<bool> nulls = { false, true, false, true, false, true, false, true, false, true };
    SetNulls(hasNulls, nulls);
    EXPECT_TRUE(hasNulls->MayHaveNull());
    EXPECT_EQ(hasNulls->GetNullCount(), 5);
    delete hasNulls;
    delete allocator;
}

TEST(ShortVector, testNullFlagWithCopyPosition)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithCopyPosition");
    // has null value
    auto *hasNulls = new ShortVector(allocator, 10);
    std::vector<bool> nulls = { false, false, true, true, false, true, false, true, false, true };
    TestUtil::SetNulls(hasNulls, nulls);
    EXPECT_TRUE(hasNulls->MayHaveNull());
    EXPECT_EQ(hasNulls->GetNullCount(), 5);

    std::vector<int32_t> positions = { 0, 1 };
    ShortVector *copyPositionNoNull = hasNulls->CopyPositions(positions.data(), 0, 2);
    EXPECT_FALSE(copyPositionNoNull->MayHaveNull());
    EXPECT_EQ(copyPositionNoNull->GetNullCount(), 0);
    delete copyPositionNoNull;

    positions = { 1, 2, 3, 4 };
    ShortVector *copyPositionHasNull = hasNulls->CopyPositions(positions.data(), 0, 4);
    EXPECT_TRUE(copyPositionHasNull->MayHaveNull());
    EXPECT_EQ(copyPositionHasNull->GetNullCount(), 2);
    delete copyPositionHasNull;

    delete hasNulls;
    delete allocator;
}

TEST(ShortVector, testNullFlagWithSlice)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithSlice");
    // has null value
    auto *hasNulls = new ShortVector(allocator, 10);
    std::vector<bool> nulls = { false, false, true, true, false, true, false, true, false, true };
    SetNulls(hasNulls, nulls);
    EXPECT_TRUE(hasNulls->MayHaveNull());
    EXPECT_EQ(hasNulls->GetNullCount(), 5);

    ShortVector *sliceNoNull = hasNulls->Slice(0, 1);
    EXPECT_TRUE(sliceNoNull->MayHaveNull());
    EXPECT_EQ(sliceNoNull->GetNullCount(), 0);
    delete sliceNoNull;

    ShortVector *sliceHasNull = hasNulls->Slice(1, 4);
    EXPECT_TRUE(sliceHasNull->MayHaveNull());
    EXPECT_EQ(sliceHasNull->GetNullCount(), 2);
    delete sliceHasNull;

    delete hasNulls;
}

TEST(ShortVector, testNullFlagWithCopyRegion)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithCopyRegion");
    // has null value
    auto *hasNulls = new ShortVector(allocator, 10);
    std::vector<bool> nulls = { false, false, true, true, false, true, false, true, false, true };
    SetNulls(hasNulls, nulls);
    EXPECT_TRUE(hasNulls->MayHaveNull());
    EXPECT_EQ(hasNulls->GetNullCount(), 5);

    ShortVector *copyRegionNoNull = hasNulls->CopyRegion(0, 2);
    EXPECT_FALSE(copyRegionNoNull->MayHaveNull());
    delete copyRegionNoNull;

    ShortVector *copyRegionHasNull = hasNulls->CopyRegion(1, 4);
    EXPECT_TRUE(copyRegionHasNull->MayHaveNull());
    EXPECT_EQ(copyRegionHasNull->GetNullCount(), 2);
    delete copyRegionHasNull;

    delete hasNulls;
    delete allocator;
}

TEST(ShortVector, testNullFlagWithAppend)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithAppend");

    int rowCount = 5;
    auto *src = new ShortVector(allocator, rowCount);
    for (int i = 0; i < rowCount; i++) {
        src->SetValue(i, i + 1);
    }

    auto *appended = new ShortVector(allocator, 15);
    appended->Append(src, 0, rowCount);
    delete src;
    EXPECT_FALSE(appended->MayHaveNull());
    EXPECT_EQ(appended->GetNullCount(), 0);

    auto *withNull = new ShortVector(allocator, rowCount);
    std::vector<bool> nulls = { false, true, true, false, true };
    SetNulls(withNull, nulls);
    appended->Append(withNull, 5, rowCount);
    EXPECT_TRUE(appended->MayHaveNull());
    EXPECT_EQ(appended->GetNullCount(), 3);

    appended->Append(withNull, 10, rowCount);
    EXPECT_TRUE(appended->MayHaveNull());
    EXPECT_EQ(appended->GetNullCount(), 6);
    delete withNull;

    delete appended;
    delete allocator;
}
}
