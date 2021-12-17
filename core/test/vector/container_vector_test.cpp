/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "vector_common.h"

using namespace omniruntime::vec;

const int32_t POSITION_COUNT = 100;
const int32_t VECTOR_COUNT = 2;
const VecType VECTOR_TYPES[] = {DoubleVecType::Instance(), LongVecType::Instance()};

TEST(ContainerVector, sliceVector)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *doubleVector = new DoubleVector(allocator, POSITION_COUNT);
    LongVector *longVector = new LongVector(allocator, POSITION_COUNT);
    Vector **vectorAddresses = new Vector *[VECTOR_COUNT];
    vectorAddresses[0] = doubleVector;
    vectorAddresses[1] = longVector;
    ContainerVector *originalVector = new ContainerVector(allocator, POSITION_COUNT, vectorAddresses, VECTOR_COUNT,
        const_cast<VecType *>(VECTOR_TYPES));
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, i * 2);
    }

    int offset = 0;
    ContainerVector *slice1 = originalVector->Slice(offset, 2);
    EXPECT_EQ(slice1->GetPositionOffset(), offset);
    EXPECT_EQ(slice1->GetSize(), 2);
    EXPECT_EQ(slice1->GetReference(), 2);
    for (int i = 0; i < slice1->GetSize(); i++) {
        EXPECT_EQ(slice1->GetValue(i), originalVector->GetValue(i + offset));
    }

    delete originalVector;
    EXPECT_EQ(slice1->GetReference(), 1);

    delete slice1;
    delete doubleVector;
    delete longVector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test set/get
TEST(ContainerVector, setAndGetValue)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *doubleVector = new DoubleVector(allocator, POSITION_COUNT);
    LongVector *longVector = new LongVector(allocator, POSITION_COUNT);
    Vector **vectorAddresses = new Vector *[VECTOR_COUNT];
    vectorAddresses[0] = doubleVector;
    vectorAddresses[1] = longVector;
    ContainerVector *vector = new ContainerVector(allocator, POSITION_COUNT, vectorAddresses, VECTOR_COUNT,
        const_cast<VecType *>(VECTOR_TYPES));
    for (int i = 0; i < VECTOR_COUNT; i++) {
        vector->SetValue(i, i * 2);
    }

    for (int i = 0; i < VECTOR_COUNT; i++) {
        EXPECT_EQ(vector->GetValue(i), i * 2);
    }
    delete vector;
    delete doubleVector;
    delete longVector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test is copyPosition
TEST(ContainerVector, copyPositions)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *doubleVector = new DoubleVector(allocator, POSITION_COUNT);
    LongVector *longVector = new LongVector(allocator, POSITION_COUNT);
    Vector **vectorAddresses = new Vector *[VECTOR_COUNT];
    vectorAddresses[0] = doubleVector;
    vectorAddresses[1] = longVector;
    ContainerVector *vector = new ContainerVector(allocator, POSITION_COUNT, vectorAddresses, VECTOR_COUNT,
        const_cast<VecType *>(VECTOR_TYPES));

    int *positions = new int[1];
    positions[0] = 1;
    ContainerVector *copyPostionVector = vector->CopyPositions(positions, 0, 1);

    for (int i = 0; i < copyPostionVector->GetVectorCount(); i++) {
        EXPECT_EQ(copyPostionVector->GetValue(i), vector->GetValue(positions[i]));
    }

    delete vector;
    delete doubleVector;
    delete longVector;
    delete copyPostionVector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test is copyRegion
TEST(ContainerVector, copyRegion)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != NULL);

    DoubleVector *doubleVector = new DoubleVector(allocator, POSITION_COUNT);
    LongVector *longVector = new LongVector(allocator, POSITION_COUNT);
    Vector **vectorAddresses = new Vector *[VECTOR_COUNT];
    vectorAddresses[0] = doubleVector;
    vectorAddresses[1] = longVector;
    ContainerVector *vector = new ContainerVector(allocator, POSITION_COUNT, vectorAddresses, VECTOR_COUNT,
        const_cast<VecType *>(VECTOR_TYPES));

    ContainerVector *copyRegionVector = vector->CopyRegion(0, 2);

    for (int i = 0; i < copyRegionVector->GetVectorCount(); i++) {
        EXPECT_EQ(copyRegionVector->GetValue(i), vector->GetValue(i));
    }

    delete vector;
    delete doubleVector;
    delete longVector;
    delete copyRegionVector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

TEST(ContainerVector, jniFreeVector)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *doubleVector = new DoubleVector(allocator, POSITION_COUNT);
    LongVector *longVector = new LongVector(allocator, POSITION_COUNT);
    Vector **vectorAddresses = new Vector *[VECTOR_COUNT];
    vectorAddresses[0] = doubleVector;
    vectorAddresses[1] = longVector;
    ContainerVector *vector = new ContainerVector(allocator, POSITION_COUNT, vectorAddresses, VECTOR_COUNT,
        const_cast<VecType *>(VECTOR_TYPES));
    Vector *vec = (Vector *)vector;
    delete vec;
    delete doubleVector;
    delete longVector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

TEST(ContainerVector, getVectorAllocator)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    DoubleVector *doubleVector = new DoubleVector(allocator, POSITION_COUNT);
    LongVector *longVector = new LongVector(allocator, POSITION_COUNT);
    Vector **vectorAddresses = new Vector *[VECTOR_COUNT];
    vectorAddresses[0] = doubleVector;
    vectorAddresses[1] = longVector;
    ContainerVector *vector = new ContainerVector(allocator, POSITION_COUNT, vectorAddresses, VECTOR_COUNT,
        const_cast<VecType *>(VECTOR_TYPES));

    int64_t doubleVecAddr = vector->GetValue(0);
    auto doubleVec = reinterpret_cast<Vector *>(doubleVecAddr);

    delete vector;
    delete doubleVector;
    delete longVector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}
// Test is not writable

// Test multi thread
