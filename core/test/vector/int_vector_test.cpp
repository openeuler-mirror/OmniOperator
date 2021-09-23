/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "vector.h"
#include "vector_allocator.h"
#include "vector_allocator_factory.h"
#include "int_vector.h"

using namespace omniruntime::vec;

TEST(IntVector, newVector)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    IntVector *vector = new IntVector(allocator, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetCapacityInBytes(), 1024);
    EXPECT_EQ(vector->GetTypeId(), OMNI_VEC_TYPE_INT);
    delete vector;

    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(IntVector, sliceVector)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
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

    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test set/get
TEST(IntVector, setAndGetValue)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *vector = new IntVector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        vector->SetValue(i, i * 2);
    }

    for (int i = 0; i < 256; i++) {
        EXPECT_EQ(vector->GetValue(i), i * 2);
    }
    delete vector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test SetValues
TEST(IntVector, setValues)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != NULL);

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
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test out of bounds
#ifdef DEBUG
TEST(IntVector, SetValueOutOfBounds1)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *vector = new IntVector(allocator, 256);
    EXPECT_THROW(vector->SetValue(256, 256), std::runtime_error);

    delete vector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}
#endif

// Test out of bounds
#ifdef DEBUG
TEST(IntVector, SetValueOutOfBounds2)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *vector = new IntVector(allocator, 256);
    EXPECT_THROW(vector->SetValue(-1, 256), std::runtime_error);

    delete vector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}
#endif

// Test SetValues/get
TEST(IntVector, setValuesWithoutOffset)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
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
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test SetValues/get with offset
TEST(IntVector, setValuesWithOffset)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
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
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test out of bounds
#ifdef DEBUG
TEST(IntVector, SetValuesWithoutOffsetOutOfBounds)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *vector = new IntVector(allocator, 256);
    int32_t *value = new int32_t[257];
    for (int i = 0; i < 257; i++) {
        value[i] = i * 2;
    }

    EXPECT_THROW(vector->SetValues(0, value, 257), std::runtime_error);

    delete[] value;
    delete vector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}
#endif

// Test is null
TEST(IntVector, setValueNull)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
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
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test is copyPosition
TEST(IntVector, copyPositions)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("intVector");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *originalVector = new IntVector(allocator, 4);
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, i);
    }

    int *possions = new int[2];
    possions[0] = 1;
    possions[1] = 3;
    IntVector *copyPostionVector = originalVector->CopyPositions(possions, 0, 2);

    for (int i = 0; i < copyPostionVector->GetSize(); i++) {
        EXPECT_EQ(copyPostionVector->GetValue(i), originalVector->GetValue(possions[i]));
    }

    delete originalVector;
    delete copyPostionVector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test is CopyRegion
TEST(IntVector, copyRegion)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("intVector");
    EXPECT_TRUE(allocator != NULL);

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
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

TEST(IntVector, jniFreeVector)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    IntVector *originalVector = new IntVector(allocator, 256);
    Vector *vector = (Vector *)originalVector;
    std::cout << typeid(*vector).hash_code() << std::endl;
    delete vector;
}
