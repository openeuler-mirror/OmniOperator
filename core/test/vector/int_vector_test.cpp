/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <gtest/gtest.h>
#include "vector_common.h"
#include "../util/test_util.h"

using namespace omniruntime::vec;
using namespace TestUtil;

namespace IntVectorTest {
TEST(IntVector, newVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("IntVector_newVector");
    EXPECT_TRUE(allocator != nullptr);
    IntVector *vector = new IntVector(allocator, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetCapacityInBytes(), 1024);
    EXPECT_EQ(vector->GetTypeId(), OMNI_INT);
    delete vector;

    delete allocator;
}

TEST(IntVector, sliceVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("IntVector_sliceVector");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *originalVector = new IntVector(allocator, 10);
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, i * 2);
    }

    int offset = 3;
    IntVector *slice1 = originalVector->Slice(offset, 4);
    EXPECT_EQ(slice1->GetPositionOffset(), offset);
    EXPECT_EQ(slice1->GetSize(), 4);
    EXPECT_EQ(slice1->GetReference(), 2);
    for (int i = 0; i < slice1->GetSize(); i++) {
        EXPECT_EQ(slice1->GetValue(i), originalVector->GetValue(i + offset));
    }

    IntVector *slice2 = slice1->Slice(1, 2);
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
TEST(IntVector, setAndGetValue)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("IntVector_setAndGetValue");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *vector = new IntVector(allocator, 256);
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
TEST(IntVector, setValues)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("IntVector_setValues");
    EXPECT_TRUE(allocator != nullptr);

    const int size = 5;
    int32_t values[size] = {1, 3, 4, 6, 7};
    int32_t *p = values;
    IntVector *intVector1 = new IntVector(allocator, size);
    intVector1->SetValues(0, p, size);
    for (int i = 0; i < size; i++) {
        EXPECT_EQ(intVector1->GetValue(i), values[i]);
    }

    IntVector *intVector2 = new IntVector(allocator, size);
    intVector2->SetValues(1, p + 2, 3);
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(intVector2->GetValue(i + 1), values[i + 2]);
    }

    delete intVector1;
    delete intVector2;
    delete allocator;
}

// Test SetValues/get
TEST(IntVector, setValuesWithoutOffset)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("IntVector_setValuesWithoutOffset");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *vector = new IntVector(allocator, 256);
    int32_t *value = new int32_t[256];
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
TEST(IntVector, setValuesWithOffset)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("IntVector_setValuesWithOffset");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *vector = new IntVector(allocator, 256);
    int32_t *value = new int32_t[256];
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
TEST(IntVector, setValueNull)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("IntVector_setValueNull");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *vector = new IntVector(allocator, 256);
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
TEST(IntVector, copyPositions)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("IntVector_copyPositions");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *originalVector = new IntVector(allocator, 4);
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, i);
    }

    int *positions = new int[2];
    positions[0] = 1;
    positions[1] = 3;
    IntVector *copyPositionVector = originalVector->CopyPositions(positions, 0, 2);

    for (int i = 0; i < copyPositionVector->GetSize(); i++) {
        EXPECT_EQ(copyPositionVector->GetValue(i), originalVector->GetValue(positions[i]));
    }

    delete[] positions;
    delete originalVector;
    delete copyPositionVector;
    delete allocator;
}

// Test is CopyRegion
TEST(IntVector, copyRegion)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("IntVector_copyPosition");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *originalVector = new IntVector(allocator, 4);
    for (int i = 0; i < 4; i++) {
        originalVector->SetValue(i, i * 2);
    }

    IntVector *copyRegionVector = originalVector->CopyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->GetSize(); i++) {
        EXPECT_EQ(copyRegionVector->GetValue(i), originalVector->GetValue(i + 2));
    }

    delete originalVector;
    delete copyRegionVector;
    delete allocator;
}

TEST(IntVector, jniFreeVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("IntVector_jniFreeVector");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *originalVector = new IntVector(allocator, 256);
    Vector *vector = (Vector *)originalVector;
    std::cout << typeid(*vector).hash_code() << std::endl;
    delete vector;
}

TEST(IntVector, testNullFlagWithSet)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithSet");
    // no null value
    auto *noNull = new IntVector(allocator, 10);
    EXPECT_FALSE(noNull->MayHaveNull());
    delete noNull;

    // hash null value
    auto *hashNulls = new IntVector(allocator, 10);
    std::vector<bool> nulls = { false, true, false, true, false, true, false, true, false, true };
    SetNulls(hashNulls, nulls);
    EXPECT_TRUE(hashNulls->MayHaveNull());
    EXPECT_EQ(hashNulls->GetNullCount(), 5);
    delete hashNulls;
    delete allocator;
}

TEST(IntVector, testNullFlagWithCopyPosition)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithCopyPosition");
    // hash null value
    auto *hashNulls = new IntVector(allocator, 10);
    std::vector<bool> nulls = { false, false, true, true, false, true, false, true, false, true };
    TestUtil::SetNulls(hashNulls, nulls);
    EXPECT_TRUE(hashNulls->MayHaveNull());
    EXPECT_EQ(hashNulls->GetNullCount(), 5);

    std::vector<int32_t> positions = { 0, 1 };
    IntVector *copyPositionNoNull = hashNulls->CopyPositions(positions.data(), 0, 2);
    EXPECT_FALSE(copyPositionNoNull->MayHaveNull());
    EXPECT_EQ(copyPositionNoNull->GetNullCount(), 0);
    delete copyPositionNoNull;

    positions = { 1, 2, 3, 4 };
    IntVector *copyPositionHasNull = hashNulls->CopyPositions(positions.data(), 0, 4);
    EXPECT_TRUE(copyPositionHasNull->MayHaveNull());
    EXPECT_EQ(copyPositionHasNull->GetNullCount(), 2);
    delete copyPositionHasNull;

    delete hashNulls;
    delete allocator;
}

TEST(IntVector, testNullFlagWithSlice)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithSlice");
    // hash null value
    auto *hashNulls = new IntVector(allocator, 10);
    std::vector<bool> nulls = { false, false, true, true, false, true, false, true, false, true };
    SetNulls(hashNulls, nulls);
    EXPECT_TRUE(hashNulls->MayHaveNull());
    EXPECT_EQ(hashNulls->GetNullCount(), 5);

    IntVector *sliceNoNull = hashNulls->Slice(0, 1);
    EXPECT_TRUE(sliceNoNull->MayHaveNull());
    EXPECT_EQ(sliceNoNull->GetNullCount(), 0);
    delete sliceNoNull;

    IntVector *sliceHasNull = hashNulls->Slice(1, 4);
    EXPECT_TRUE(sliceHasNull->MayHaveNull());
    EXPECT_EQ(sliceHasNull->GetNullCount(), 2);
    delete sliceHasNull;

    delete hashNulls;
}

TEST(IntVector, testNullFlagWithCopyRegion)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithCopyRegion");
    // hash null value
    auto *hashNulls = new IntVector(allocator, 10);
    std::vector<bool> nulls = { false, false, true, true, false, true, false, true, false, true };
    SetNulls(hashNulls, nulls);
    EXPECT_TRUE(hashNulls->MayHaveNull());
    EXPECT_EQ(hashNulls->GetNullCount(), 5);

    IntVector *copyRegionNoNull = hashNulls->CopyRegion(0, 2);
    EXPECT_FALSE(copyRegionNoNull->MayHaveNull());
    delete copyRegionNoNull;

    IntVector *copyRegionHasNull = hashNulls->CopyRegion(1, 4);
    EXPECT_TRUE(copyRegionHasNull->MayHaveNull());
    EXPECT_EQ(copyRegionHasNull->GetNullCount(), 2);
    delete copyRegionHasNull;

    delete hashNulls;
    delete allocator;
}

TEST(IntVector, testNullFlagWithAppend)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithAppend");

    int rowCount = 5;
    auto *src = new IntVector(allocator, rowCount);
    for (int i = 0; i < rowCount; i++) {
        src->SetValue(i, i + 1);
    }

    auto *appended = new IntVector(allocator, 15);
    appended->Append(src, 0, rowCount);
    delete src;
    EXPECT_FALSE(appended->MayHaveNull());
    EXPECT_EQ(appended->GetNullCount(), 0);

    auto *withNull = new IntVector(allocator, rowCount);
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
