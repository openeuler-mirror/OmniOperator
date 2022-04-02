/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <gtest/gtest.h>
#include "../util/test_util.h"
#include "vector_common.h"

using namespace omniruntime::vec;
using namespace TestUtil;

namespace LongVectorTest {
TEST(LongVector, sliceVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("LongVector_sliceVector");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *originalVector = new LongVector(allocator, 10);
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, i * 2);
    }

    int offset = 3;
    LongVector *slice1 = originalVector->Slice(offset, 4);
    EXPECT_EQ(slice1->GetPositionOffset(), offset);
    EXPECT_EQ(slice1->GetSize(), 4);
    EXPECT_EQ(slice1->GetReference(), 2);
    for (int i = 0; i < slice1->GetSize(); i++) {
        EXPECT_EQ(slice1->GetValue(i), originalVector->GetValue(i + offset));
    }

    LongVector *slice2 = slice1->Slice(1, 2);
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
TEST(LongVector, setAndGetValue)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("LongVector_setAndGetValue");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);
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
TEST(LongVector, SetValues)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("LongVector_SetValues");
    EXPECT_TRUE(allocator != NULL);

    const int size = 5;
    int64_t values[size] = {1, 3, 4, 6, 7};
    int64_t *p = values;
    LongVector *longVector1 = new LongVector(allocator, size);
    longVector1->SetValues(0, p, size);
    for (int i = 0; i < size; i++) {
        EXPECT_EQ(longVector1->GetValue(i), values[i]);
    }

    LongVector *longVector2 = new LongVector(allocator, size);
    longVector2->SetValues(1, p + 2, 3);
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(longVector2->GetValue(i + 1), values[i + 2]);
    }

    delete longVector1;
    delete longVector2;
    delete allocator;
}

// Test SetValues/get
TEST(LongVector, SetValuesWithoutOffset)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
            "LongVector_SetValuesWithoutOffset");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);
    long *value = new long[256];
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
TEST(LongVector, SetValuesWithOffset)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
            "LongVector_SetValuesWithOffset");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);
    long *value = new long[256];
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
TEST(LongVector, setValueNull)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("LongVector_setValueNull");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);
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
TEST(LongVector, copyPositions)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("LongVector_copyPositions");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *originalVector = new LongVector(allocator, 4);
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, i);
    }

    int *possions = new int[2];
    possions[0] = 1;
    possions[1] = 3;
    LongVector *copyPostionVector = originalVector->CopyPositions(possions, 0, 2);

    for (int i = 0; i < copyPostionVector->GetSize(); i++) {
        EXPECT_EQ(copyPostionVector->GetValue(i), originalVector->GetValue(possions[i]));
    }

    delete originalVector;
    delete copyPostionVector;
    delete allocator;
}

// Test is copyRegion
TEST(LongVector, copyRegion)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("LongVector_copyPosition");
    EXPECT_TRUE(allocator != NULL);

    LongVector *originalVector = new LongVector(allocator, 4);
    for (int i = 0; i < 4; i++) {
        originalVector->SetValue(i, i * 2);
    }

    LongVector *copyRegionVector = originalVector->CopyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->GetSize(); i++) {
        EXPECT_EQ(copyRegionVector->GetValue(i), originalVector->GetValue(i + 2));
    }

    delete originalVector;
    delete copyRegionVector;
    delete allocator;
}

TEST(Vector, jniFreeVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("LongVector_jniFreeVector");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *longVector = new LongVector(allocator, 256);
    Vector *vector = (Vector *)longVector;
    delete vector;
}

class LongVectorTest {
public:
    LongVectorTest() : values(new long[100000000]) {}

    void SetValue(int index, int64_t value)
    {
        ((int64_t *)values)[index] = value;
    }

    int64_t GetValue(int index)
    {
        return ((int64_t *)values)[index];
    }

private:
    void *values;
};

// Performance test
TEST(LongVector, performanceCompare)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("LongVector_performanceCompare");
    int rowCount = 100000000;

    Timer timer;

    // test long vector set value
    auto *vectorTest2 = new LongVectorTest();
    timer.Start("point test vector set value");
    for (int i = 0; i < rowCount; ++i) {
        vectorTest2->SetValue(i, i);
    }
    timer.End();

    // test long vector set value
    LongVectorTest vectorTest1;
    timer.Start("stack test vector set value");
    for (int i = 0; i < rowCount; ++i) {
        vectorTest1.SetValue(i, i);
    }
    timer.End();

    // test long vector get value
    timer.Start("point test vector get value");
    for (int i = 0; i < rowCount; ++i) {
        vectorTest2->GetValue(i);
    }
    timer.End();

    // test long vector get value
    timer.Start("stack test vector get value");
    for (int i = 0; i < rowCount; ++i) {
        vectorTest1.GetValue(i);
    }
    timer.End();

    // vector set value
    LongVector longVector(allocator, rowCount);
    timer.Start("vector set value");
    for (int i = 0; i < rowCount; ++i) {
        longVector.SetValue(i, i);
    }
    timer.End();

    // original set value
    void *longVector2 = new long[rowCount];
    timer.Start("original set value");
    for (int i = 0; i < rowCount; ++i) {
        ((long *)longVector2)[i] = i;
    }
    timer.End();

    // vector get value
    long value = 0;
    timer.Start("vector get value");
    for (int i = 0; i < rowCount; ++i) {
        value = longVector.GetValue(i);
    }
    timer.End();
    value = value + 1;
    // original get value
    timer.Start("original get value");
    for (int i = 0; i < rowCount; ++i) {
        value = *((int64_t *)(longVector2) + i);
    }
    timer.End();

    delete[](long *) longVector2;
    delete vectorTest2;
}

TEST(LongVector, appendVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("LongVector_appendVector");
    EXPECT_TRUE(allocator != nullptr);

    int64_t data[5] = {1, 2, 3, 4, 5};
    auto *src1 = new LongVector(allocator, 5);
    src1->SetValues(0, data, 5);
    auto *src2 = new LongVector(allocator, 5);
    int64_t data2[5] = {6, 7, 8, 9, 10};
    src2->SetValues(0, data2, 5);

    auto *appended = new LongVector(allocator, 13);
    appended->Append(src1, 0, 5);
    appended->Append(src2, 5, 5);
    for (int i = 0; i < 10; i++) {
        EXPECT_EQ(appended->GetValue(i), i + 1);
    }

    int32_t ids[3] = {1, 2, 3};
    auto *dictionaryVector = new DictionaryVector(src1, ids, 3);
    appended->Append(dictionaryVector, 10, 3);
    for (int i = 10; i < 13; i++) {
        EXPECT_EQ(appended->GetValue(i), dictionaryVector->GetLong(i % 10));
    }

    delete src1;
    delete src2;
    delete dictionaryVector;
    delete appended;
    delete allocator;
}
}
// Test is not writable

// Test multi thread
