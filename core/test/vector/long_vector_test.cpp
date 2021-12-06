/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "../util/test_util.h"
#include "vector_common.h"

using namespace omniruntime::vec;

TEST(LongVector, sliceVector)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
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

    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test set/get
TEST(LongVector, setAndGetValue)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);
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
TEST(LongVector, SetValues)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
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
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test out of bounds
#ifdef DEBUG
TEST(LongVector, SetValueOutOfBounds1)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);
    EXPECT_THROW(vector->SetValue(256, 256), std::runtime_error);

    delete vector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}
#endif

// Test out of bounds
#ifdef DEBUG
TEST(LongVector, SetValueOutOfBounds2)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);
    EXPECT_THROW(vector->SetValue(-1, 256), std::runtime_error);

    delete vector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}
#endif

// Test SetValues/get
TEST(LongVector, SetValuesWithoutOffset)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
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
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test SetValues/get with offset
TEST(LongVector, SetValuesWithOffset)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
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
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test out of bounds
#ifdef DEBUG
TEST(LongVector, SetValuesWithoutOffsetOutOfBounds)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *vector = new LongVector(allocator, 256);
    long *value = new long[257];
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
TEST(LongVector, setValueNull)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
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
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test is copyPosition
TEST(LongVector, copyPositions)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("longVector");
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
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test is copyRegion
TEST(LongVector, copyRegion)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("longVector");
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
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

TEST(Vector, jniFreeVector)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    LongVector *longVector = new LongVector(allocator, 256);
    Vector *vector = (Vector *)longVector;
    delete vector;
}

class LongVectorTest {
public:
    LongVectorTest()
    {
        values = new long[100000000];
    }

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
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    int ROW_COUNT = 100000000;

    Timer timer;

    // test long vector set value
    auto *vectorTest2 = new LongVectorTest();
    timer.start("point test vector set value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        vectorTest2->SetValue(i, i);
    }
    timer.end();

    // test long vector set value
    LongVectorTest vectorTest1;
    timer.start("stack test vector set value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        vectorTest1.SetValue(i, i);
    }
    timer.end();

    // test long vector get value
    timer.start("point test vector get value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        vectorTest2->GetValue(i);
    }
    timer.end();

    // test long vector get value
    timer.start("stack test vector get value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        vectorTest1.GetValue(i);
    }
    timer.end();

    // vector set value
    LongVector longVector(allocator, ROW_COUNT);
    timer.start("vector set value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        longVector.SetValue(i, i);
    }
    timer.end();

    // original set value
    void *longVector2 = new long[ROW_COUNT];
    timer.start("original set value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        ((long *)longVector2)[i] = i;
    }
    timer.end();

    // vector get value
    timer.start("vector get value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        long value = longVector.GetValue(i);
    }
    timer.end();

    // original get value
    timer.start("original get value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        long value = *((int64_t *)(longVector2) + i);
    }
    timer.end();

    //    delete longVector;
    delete[](long *) longVector2;
    delete vectorTest2;
}

// Test is not writable

// Test multi thread
