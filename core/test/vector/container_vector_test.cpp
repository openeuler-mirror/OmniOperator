/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <gtest/gtest.h>
#include "vector_common.h"
#include "util/test_util.h"

using namespace omniruntime::vec;
using namespace TestUtil;

namespace ContainerVectorTest {
const int32_t POSITION_COUNT = 100;
const int32_t VECTOR_COUNT = 2;
TEST(ContainerVector, sliceVector)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ContainerVector_SliceVector");
    EXPECT_TRUE(allocator != nullptr);

    int32_t rows = 10;
    auto *doubleVector = new DoubleVector(allocator, rows);
    double data1[] = {0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9};
    doubleVector->SetValues(0, data1, rows);
    auto *longVector = new LongVector(allocator, rows);
    int64_t data2[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    longVector->SetValues(0, data2, rows);
    std::vector<uintptr_t> vecAddr = { reinterpret_cast<uintptr_t>(doubleVector),
        reinterpret_cast<uintptr_t>(longVector) };
    std::vector<DataTypePtr> dataTypes = { DoubleType(), LongType() };
    auto *originalVector = new ContainerVector(allocator, rows, vecAddr, 2, dataTypes);

    int offset = 1;
    ContainerVector *slice1 = originalVector->Slice(offset, 5);
    AssertDoubleVectorEquals(reinterpret_cast<DoubleVector *>(slice1->GetValue(0)), data1 + offset);
    auto *result = reinterpret_cast<LongVector *>(slice1->GetValue(1));
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(result->GetValue(i), data2[offset + i]);
    }

    delete originalVector;
    delete slice1;
    delete allocator;
}

// Test set/get
TEST(ContainerVector, setAndGetValue)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ContainerVector_SetAndGetValue");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *doubleVector = new DoubleVector(allocator, POSITION_COUNT);
    LongVector *longVector = new LongVector(allocator, POSITION_COUNT);
    std::vector<uintptr_t> vectorAddresses(2);
    vectorAddresses[0] = reinterpret_cast<uintptr_t>(doubleVector);
    vectorAddresses[1] = reinterpret_cast<uintptr_t>(longVector);
    std::vector<DataTypePtr> VECTOR_TYPES = { DoubleType(), LongType() };
    ContainerVector *vector =
        new ContainerVector(allocator, POSITION_COUNT, vectorAddresses, VECTOR_COUNT, VECTOR_TYPES);
    Vector *values[] = {new DoubleVector(allocator, POSITION_COUNT), new LongVector(allocator, POSITION_COUNT)};
    for (int i = 0; i < VECTOR_COUNT; i++) {
        vector->SetValue(i, reinterpret_cast<uintptr_t>(values[i]));
    }

    for (int i = 0; i < VECTOR_COUNT; i++) {
        EXPECT_EQ(vector->GetValue(i), reinterpret_cast<uintptr_t>(values[i]));
    }

    delete vector;
    delete doubleVector;
    delete longVector;
    delete allocator;
}

// Test is copyPosition
TEST(ContainerVector, copyPositions)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ContainerVector_CopyPositions");
    EXPECT_TRUE(allocator != nullptr);

    int32_t rows = 10;
    auto *doubleVector = new DoubleVector(allocator, rows);
    double data1[] = {0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9};
    doubleVector->SetValues(0, data1, rows);
    auto *longVector = new LongVector(allocator, rows);
    int64_t data2[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    longVector->SetValues(0, data2, rows);
    std::vector<uintptr_t> vecAddr = { reinterpret_cast<uintptr_t>(doubleVector),
        reinterpret_cast<uintptr_t>(longVector) };
    std::vector<DataTypePtr> dataTypes = { DoubleType(), LongType() };
    auto *originalVector = new ContainerVector(allocator, rows * 2, vecAddr, 2, dataTypes);

    int32_t positions[] = {1, 3, 5, 7, 9};
    ContainerVector *copyPositioned = originalVector->CopyPositions(positions, 0, 5);
    std::vector<double> expected(5);
    for (int32_t i = 0; i < 5; i++) {
        expected[i] = data1[positions[i]];
    }

    AssertDoubleVectorEquals(reinterpret_cast<DoubleVector *>(copyPositioned->GetValue(0)), expected.data());
    auto *result = reinterpret_cast<LongVector *>(copyPositioned->GetValue(1));
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(result->GetValue(i), data2[positions[i]]);
    }

    delete originalVector;
    delete copyPositioned;
    delete allocator;
}

// Test is copyRegion
TEST(ContainerVector, copyRegion)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ContainerVector_CopyRegion");
    EXPECT_TRUE(allocator != nullptr);

    int32_t rows = 10;
    auto *doubleVector = new DoubleVector(allocator, rows);
    double data1[] = {0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9};
    doubleVector->SetValues(0, data1, rows);
    auto *longVector = new LongVector(allocator, rows);
    int64_t data2[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    longVector->SetValues(0, data2, rows);
    std::vector<uintptr_t> vecAddr = { reinterpret_cast<uintptr_t>(doubleVector),
        reinterpret_cast<uintptr_t>(longVector) };
    std::vector<DataTypePtr> dataTypes = { DoubleType(), LongType() };
    auto *originalVector = new ContainerVector(allocator, rows * 2, vecAddr, 2, dataTypes);

    int offset = 1;
    ContainerVector *copyRegionedVec = originalVector->CopyRegion(offset, 5);
    AssertDoubleVectorEquals(reinterpret_cast<DoubleVector *>(copyRegionedVec->GetValue(0)), data1 + offset);
    auto *result = reinterpret_cast<LongVector *>(copyRegionedVec->GetValue(1));
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(result->GetValue(i), data2[offset + i]);
    }

    delete originalVector;
    delete copyRegionedVec;
    delete allocator;
}

TEST(ContainerVector, jniFreeVector)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ContainerVector_jniFreeVector");
    EXPECT_TRUE(allocator != nullptr);

    DoubleVector *doubleVector = new DoubleVector(allocator, POSITION_COUNT);
    LongVector *longVector = new LongVector(allocator, POSITION_COUNT);

    std::vector<uintptr_t> vectorAddresses(2);
    vectorAddresses[0] = reinterpret_cast<uintptr_t>(doubleVector);
    vectorAddresses[1] = reinterpret_cast<uintptr_t>(longVector);
    std::vector<DataTypePtr> VECTOR_TYPES = { DoubleType(), LongType() };
    ContainerVector *vector =
        new ContainerVector(allocator, POSITION_COUNT, vectorAddresses, VECTOR_COUNT, VECTOR_TYPES);
    Vector *vec = (Vector *)vector;
    delete vec;
    delete allocator;
}

TEST(ContainerVector, getVectorAllocator)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ContainerVector_getVectorAllocator");
    EXPECT_TRUE(allocator != nullptr);
    DoubleVector *doubleVector = new DoubleVector(allocator, POSITION_COUNT);
    LongVector *longVector = new LongVector(allocator, POSITION_COUNT);

    std::vector<uintptr_t> vectorAddresses(2);
    vectorAddresses[0] = reinterpret_cast<uintptr_t>(doubleVector);
    vectorAddresses[1] = reinterpret_cast<uintptr_t>(longVector);
    std::vector<DataTypePtr> VECTOR_TYPES = { DoubleType(), LongType() };
    ContainerVector *vector =
        new ContainerVector(allocator, POSITION_COUNT, vectorAddresses, VECTOR_COUNT, VECTOR_TYPES);

    int64_t doubleVecAddr = vector->GetValue(0);
    auto doubleVec = reinterpret_cast<Vector *>(doubleVecAddr);
    EXPECT_TRUE(doubleVec->GetValueNulls() != nullptr);

    delete vector;
    delete allocator;
}

TEST(ContainerVector, appendVector)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("ContainerVector_appendVector");
    EXPECT_TRUE(allocator != nullptr);

    int32_t rows = 5;
    int64_t data[5] = {1, 2, 3, 4, 5};
    auto *src1 = new LongVector(allocator, 5);
    src1->SetValues(0, data, 5);

    double data1[5] = {1.1, 2.2, 3.3, 4.4, 5.5};
    auto *doubleVector = new DoubleVector(allocator, rows);
    doubleVector->SetValues(0, data1, 5);

    const int32_t columnCount = 2;
    std::vector<uintptr_t> vectorAddresses = { reinterpret_cast<uintptr_t>(doubleVector),
        reinterpret_cast<uintptr_t>(src1) };
    std::vector<DataTypePtr> vectorTypes = { DoubleType(), LongType() };
    auto *vector = new ContainerVector(allocator, rows, vectorAddresses, columnCount, vectorTypes);

    auto *src2 = new LongVector(allocator, rows);
    int64_t data2[5] = {6, 7, 8, 9, 10};
    src2->SetValues(0, data2, rows);
    double data22[5] = {6.6, 7.7, 8.8, 9.9, 10.1};
    auto *doubleVector1 = new DoubleVector(allocator, rows);
    doubleVector1->SetValues(0, data22, rows);

    std::vector<uintptr_t> vecAddr2 = { reinterpret_cast<uintptr_t>(doubleVector1), reinterpret_cast<uintptr_t>(src2) };
    std::vector<DataTypePtr> dataTypes2 = { DoubleType(), LongType() };
    auto *vector2 = new ContainerVector(allocator, rows, vecAddr2, columnCount, dataTypes2);

    auto *appendedDouble = new DoubleVector(allocator, 10);
    auto *appendedLong = new LongVector(allocator, 10);
    std::vector<uintptr_t> appendedAddr = { reinterpret_cast<uintptr_t>(appendedDouble),
        reinterpret_cast<uintptr_t>(appendedLong) };
    std::vector<DataTypePtr> appendedDataType = { DoubleType(), LongType() };
    auto *appended = new ContainerVector(allocator, rows * 2, appendedAddr, columnCount, appendedDataType);
    appended->Append(vector, 0, 5);
    appended->Append(vector2, 5, 5);

    double expected[] = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.1};
    AssertDoubleVectorEquals(reinterpret_cast<DoubleVector *>(appended->GetValue(0)), expected);
    for (int i = 0; i < 10; i++) {
        EXPECT_EQ(reinterpret_cast<LongVector *>(appended->GetValue(1))->GetValue(i), i + 1);
    }

    delete vector;
    delete vector2;
    delete appended;
    delete allocator;
}

TEST(ContainerVector, testNullFlagWithSet)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testNullFlagWithSet");
    int rows = 10;
    auto *sub1 = new IntVector(allocator, rows);
    auto *sub2 = new LongVector(allocator, rows);

    std::vector<uintptr_t> subAddrs = { reinterpret_cast<uintptr_t>(sub1), reinterpret_cast<uintptr_t>(sub2) };
    std::vector<DataTypePtr> subTypes = { IntType(), LongType() };
    auto *hasNulls = new ContainerVector(allocator, rows, subAddrs, 2, subTypes);

    std::vector<bool> nulls = { true, false, true, false, true, false, true, false, true, false };
    TestUtil::SetNulls(hasNulls, nulls);
    EXPECT_TRUE(hasNulls->MayHaveNull());
    EXPECT_EQ(hasNulls->GetNullCount(), 5);
    delete hasNulls;
    delete allocator;
}
}