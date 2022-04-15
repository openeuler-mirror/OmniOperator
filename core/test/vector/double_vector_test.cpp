/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <gtest/gtest.h>
#include "vector_common.h"

using namespace omniruntime::vec;

namespace DoubleVectorTest {
TEST(DoubleVector, newVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("DoubleVector_newVector");
    EXPECT_TRUE(allocator != nullptr);
    DoubleVector *vector = new DoubleVector(allocator, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetCapacityInBytes(), 2048);
    EXPECT_EQ(vector->GetTypeId(), OMNI_DOUBLE);
    delete vector;

    delete allocator;
}

TEST(DoubleVector, sliceVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("DoubleVector_sliceVector");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *originalVector = new DoubleVector(allocator, 10);
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, (double)i / 3);
    }

    int offset = 3;
    DoubleVector *slice1 = originalVector->Slice(offset, 4);
    EXPECT_EQ(slice1->GetSize(), 4);
    EXPECT_EQ(slice1->GetReference(), 2);
    for (int i = 0; i < slice1->GetSize(); i++) {
        EXPECT_EQ(slice1->GetValue(i), originalVector->GetValue(i + offset));
    }

    DoubleVector *slice2 = slice1->Slice(1, 2);
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
TEST(DoubleVector, setAndGetValue)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
            "DoubleVector_setAndGetValue");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        vector->SetValue(i, i * 2.3);
    }

    for (int i = 0; i < 256; i++) {
        EXPECT_EQ(vector->GetValue(i), i * 2.3);
    }
    delete vector;
    delete allocator;
}

// Test SetValues
TEST(DoubleVector, SetValues)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("DoubleVector_setValues");
    EXPECT_TRUE(allocator != NULL);

    const int size = 5;
    double values[size] = {1.1, 3.3, 4.5, 6.6, 7.7};
    double *p = values;
    DoubleVector *doubleVector1 = new DoubleVector(allocator, size);
    doubleVector1->SetValues(0, p, size);
    for (int i = 0; i < size; i++) {
        EXPECT_EQ(doubleVector1->GetValue(i), values[i]);
    }

    DoubleVector *doubleVector2 = new DoubleVector(allocator, size);
    doubleVector2->SetValues(1, p + 2, 3);
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(doubleVector2->GetValue(i + 1), values[i + 2]);
    }

    delete doubleVector1;
    delete doubleVector2;
    delete allocator;
}

// Test SetValues/get
TEST(DoubleVector, SetValuesWithoutOffset)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
            "DoubleVector_SetValuesWithoutOffset");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    double *value = new double[256];
    for (int i = 0; i < 256; i++) {
        value[i] = i * 2.3;
    }
    vector->SetValues(0, value, 256);
    for (int i = 0; i < 256; i++) {
        EXPECT_EQ(vector->GetValue(i), i * 2.3);
    }

    delete[] value;
    delete vector;
    delete allocator;
}

// Test SetValues/get with offset
TEST(DoubleVector, SetValuesWithOffset)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
            "DoubleVector_SetValuesWithOffset");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    double *value = new double[256];
    for (int i = 0; i < 256; i++) {
        value[i] = i * 2.3;
    }
    vector->SetValues(128, &value[128], 128);
    for (int i = 128; i < 256; i++) {
        EXPECT_EQ(vector->GetValue(i), i * 2.3);
    }

    delete[] value;
    delete vector;
    delete allocator;
}

// Test is null
TEST(DoubleVector, SetValueNull)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
            "DoubleVector_SetValueNull");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *vector = new DoubleVector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            vector->SetValueNull(i);
        } else {
            vector->SetValue(i, i * 2.3);
        }
    }
    for (int i = 0; i < 256; i++) {
        if (i % 5 == 0) {
            EXPECT_TRUE(vector->IsValueNull(i));
        } else {
            EXPECT_EQ(vector->GetValue(i), i * 2.3);
        }
    }
    delete vector;
    delete allocator;
}

// Test is copyPosition
TEST(DoubleVector, copyPositions)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
            "DoubleVector_copyPositions");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *originalVector = new DoubleVector(allocator, 4);
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, i * 2.3);
    }

    int *possions = new int[2];
    possions[0] = 1;
    possions[1] = 3;
    DoubleVector *copyPostionVector = originalVector->CopyPositions(possions, 0, 2);

    for (int i = 0; i < copyPostionVector->GetSize(); i++) {
        EXPECT_EQ(copyPostionVector->GetValue(i), originalVector->GetValue(possions[i]));
    }

    delete originalVector;
    delete copyPostionVector;
    delete allocator;
}

// Test is copyRegion
TEST(DoubleVector, copyRegion)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
            "DoubleVector_copyPosition");
    EXPECT_TRUE(allocator != NULL);

    DoubleVector *originalVector = new DoubleVector(allocator, 4);
    for (int i = 0; i < 4; i++) {
        originalVector->SetValue(i, i * 3.3);
    }

    DoubleVector *copyRegionVector = originalVector->CopyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->GetSize(); i++) {
        EXPECT_EQ(copyRegionVector->GetValue(i), originalVector->GetValue(i + 2));
    }

    delete originalVector;
    delete copyRegionVector;
    delete allocator;
}
}
