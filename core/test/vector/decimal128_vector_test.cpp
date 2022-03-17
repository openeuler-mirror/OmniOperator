/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "vector_common.h"
#include "../util/test_util.h"

using namespace omniruntime::vec;

TEST(Decimal128Vector, SliceVector)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    Decimal128Vector *originalVector = new Decimal128Vector(allocator, 10);
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, i * 2);
    }

    int offset = 3;
    Decimal128Vector *slice1 = originalVector->Slice(offset, 4);
    EXPECT_EQ(slice1->GetPositionOffset(), offset);
    EXPECT_EQ(slice1->GetSize(), 4);
    EXPECT_EQ(slice1->GetReference(), 2);
    for (int i = 0; i < slice1->GetSize(); i++) {
        EXPECT_TRUE(slice1->GetValue(i) == originalVector->GetValue(i + offset));
    }

    Decimal128Vector *slice2 = slice1->Slice(1, 2);
    for (int i = 0; i < slice2->GetSize(); i++) {
        EXPECT_TRUE(slice2->GetValue(i) == originalVector->GetValue(i + offset + 1));
    }

    delete originalVector;
    EXPECT_EQ(slice1->GetReference(), 2);

    delete slice1;
    EXPECT_EQ(slice2->GetReference(), 1);
    delete slice2;

    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test set/get
TEST(Decimal128Vector, SetAndGetValue)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    Decimal128Vector *vector = new Decimal128Vector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        vector->SetValue(i, i * 2);
    }

    for (int i = 0; i < 256; i++) {
        EXPECT_TRUE(vector->GetValue(i) == i * 2);
    }
    delete vector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test SetValues
TEST(Decimal128Vector, SetValues)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != NULL);

    const int size = 5;
    int64_t values[size * 2] = {0, 1, 0, 3, 0, 4, 0, 6, 0, 7};
    int64_t *p = values;
    Decimal128Vector *Decimal128Vector1 = new Decimal128Vector(allocator, size);
    Decimal128Vector1->SetValues(0, p, size);
    for (int i = 0; i < size; i++) {
        Decimal128 decimal128 = Decimal128Vector1->GetValue(i);
        EXPECT_TRUE(decimal128.LowBits() == values[i * 2]);
        EXPECT_TRUE(decimal128.HighBits() == values[i * 2 + 1]);
    }

    Decimal128Vector *Decimal128Vector2 = new Decimal128Vector(allocator, size);
    Decimal128Vector2->SetValues(1, p + 2 * 2, 3);
    for (int i = 0; i < 3; i++) {
        Decimal128 decimal128 = Decimal128Vector2->GetValue(i + 1);
        EXPECT_TRUE(decimal128.LowBits() == values[(i + 2) * 2]);
        EXPECT_TRUE(decimal128.HighBits() == values[(i + 2) * 2 + 1]);
    }

    delete Decimal128Vector1;
    delete Decimal128Vector2;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test SetValues/get
TEST(Decimal128Vector, SetValuesWithoutOffset)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    Decimal128Vector *vector = new Decimal128Vector(allocator, 256);
    long *value = new long[256 * 2];
    for (int i = 0; i < 256; i++) {
        value[i * 2] = 12;
        value[i * 2 + 1] = i * 2;
    }
    vector->SetValues(0, value, 256);
    for (int i = 0; i < 256; i++) {
        Decimal128 decimal128 = vector->GetValue(i);
        EXPECT_TRUE(decimal128.LowBits() == value[i * 2]);
        EXPECT_TRUE(decimal128.HighBits() == value[i * 2 + 1]);
    }

    delete[] value;
    delete vector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test SetValues/get with offset
TEST(Decimal128Vector, SetValuesWithOffset)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    Decimal128Vector *vector = new Decimal128Vector(allocator, 256);
    long *value = new long[256 * 2];
    for (int i = 0; i < 256; i++) {
        value[i * 2] = i + 1;
        value[i * 2 + 1] = i * 2;
    }
    vector->SetValues(128, &value[128 * 2], 128);
    for (int i = 128; i < 256; i++) {
        Decimal128 decimal128 = vector->GetValue(i);
        EXPECT_TRUE(decimal128.LowBits() == value[i * 2]);
        EXPECT_TRUE(decimal128.HighBits() == value[i * 2 + 1]);
    }

    delete[] value;
    delete vector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test is null
TEST(Decimal128Vector, SetValueNull)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);

    Decimal128Vector *vector = new Decimal128Vector(allocator, 256);
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
            EXPECT_FALSE(vector->IsValueNull(i));
        }
    }
    delete vector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test is copyPosition
TEST(Decimal128Vector, CopyPositions)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("Decimal128Vector");
    EXPECT_TRUE(allocator != nullptr);

    Decimal128Vector *originalVector = new Decimal128Vector(allocator, 4);
    for (int i = 0; i < originalVector->GetSize(); i++) {
        originalVector->SetValue(i, i);
    }

    int *possions = new int[2];
    possions[0] = 1;
    possions[1] = 3;
    Decimal128Vector *copyPostionVector = originalVector->CopyPositions(possions, 0, 2);

    for (int i = 0; i < copyPostionVector->GetSize(); i++) {
        EXPECT_TRUE(copyPostionVector->GetValue(i) == originalVector->GetValue(possions[i]));
    }

    delete originalVector;
    delete copyPostionVector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

// Test is copyRegion
TEST(Decimal128Vector, CopyRegion)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("Decimal128Vector");
    EXPECT_TRUE(allocator != NULL);

    Decimal128Vector *originalVector = new Decimal128Vector(allocator, 4);
    for (int i = 0; i < 4; i++) {
        originalVector->SetValue(i, i * 2);
    }

    Decimal128Vector *copyRegionVector = originalVector->CopyRegion(2, 2);

    for (int i = 0; i < copyRegionVector->GetSize(); i++) {
        EXPECT_TRUE(copyRegionVector->GetValue(i) == originalVector->GetValue(i + 2));
    }

    delete originalVector;
    delete copyRegionVector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

class Decimal128VectorTest {
public:
    Decimal128VectorTest()
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
TEST(Decimal128Vector, PerformanceCompare)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    int ROW_COUNT = 100000000;

    Timer timer;

    // test long vector set value
    auto *vectorTest2 = new Decimal128VectorTest();
    timer.start("point test vector set value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        vectorTest2->SetValue(i, i);
    }
    timer.end();

    // test long vector set value
    Decimal128VectorTest vectorTest1;
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
    Decimal128Vector decimal128Vector(allocator, ROW_COUNT);
    timer.start("vector set value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        decimal128Vector.SetValue(i, i);
    }
    timer.end();

    // original set value
    void *Decimal128Vector2 = new long[ROW_COUNT];
    timer.start("original set value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        ((long *)Decimal128Vector2)[i] = i;
    }
    timer.end();

    // vector get value
    timer.start("vector get value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        Decimal128 value = decimal128Vector.GetValue(i);
    }
    timer.end();

    // original get value
    timer.start("original get value");
    for (int i = 0; i < ROW_COUNT; ++i) {
        long value = *((int64_t *)(Decimal128Vector2) + i);
    }
    timer.end();

    //    delete Decimal128Vector;
    delete[](long *) Decimal128Vector2;
    delete vectorTest2;
}

// Test is not writable
