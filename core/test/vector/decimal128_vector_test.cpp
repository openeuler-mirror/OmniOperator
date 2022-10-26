/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <gtest/gtest.h>
#include "vector_common.h"
#include "util/test_util.h"

using namespace omniruntime::vec;
using namespace TestUtil;

namespace Decimal128VectorTest {
TEST(Decimal128Vector, SliceVector)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Decimal128Vector_SliceVector");
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

    delete allocator;
}

// Test set/get
TEST(Decimal128Vector, SetAndGetValue)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Decimal128Vector_SetAndGetValue");
    EXPECT_TRUE(allocator != nullptr);

    Decimal128Vector *vector = new Decimal128Vector(allocator, 256);
    for (int i = 0; i < 256; i++) {
        vector->SetValue(i, i * 2);
    }

    for (int i = 0; i < 256; i++) {
        EXPECT_TRUE(vector->GetValue(i) == i * 2);
    }
    delete vector;
    delete allocator;
}

// Test SetValues
TEST(Decimal128Vector, SetValues)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Decimal128Vector_SetValues");
    EXPECT_TRUE(allocator != nullptr);

    const int size = 5;
    uint64_t values[size * 2] = {0, 1, 0, 3, 0, 4, 0, 6, 0, 7};
    uint64_t *p = values;
    auto *Decimal128Vector1 = new Decimal128Vector(allocator, size);
    Decimal128Vector1->SetValues(0, p, size);
    for (int i = 0; i < size; i++) {
        Decimal128 decimal128 = Decimal128Vector1->GetValue(i);
        EXPECT_TRUE(decimal128.LowBits() == values[i * 2]);
        EXPECT_TRUE(static_cast<uint64_t>(decimal128.HighBits()) == values[i * 2 + 1]);
    }

    auto *Decimal128Vector2 = new Decimal128Vector(allocator, size);
    Decimal128Vector2->SetValues(1, p + 2 * 2, 3);
    for (int i = 0; i < 3; i++) {
        Decimal128 decimal128 = Decimal128Vector2->GetValue(i + 1);
        EXPECT_TRUE(decimal128.LowBits() == values[(i + 2) * 2]);
        EXPECT_TRUE(static_cast<uint64_t>(decimal128.HighBits()) == values[(i + 2) * 2 + 1]);
    }

    delete Decimal128Vector1;
    delete Decimal128Vector2;
    delete allocator;
}

// Test SetValues/get
TEST(Decimal128Vector, SetValuesWithoutOffset)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Decimal128Vector_SetValuesWithoutOffset");
    EXPECT_TRUE(allocator != nullptr);

    auto *vector = new Decimal128Vector(allocator, 256);
    auto *value = new uint64_t[256 * 2];
    for (int i = 0; i < 256; i++) {
        value[i * 2] = 12;
        value[i * 2 + 1] = i * 2;
    }
    vector->SetValues(0, value, 256);
    for (int i = 0; i < 256; i++) {
        Decimal128 decimal128 = vector->GetValue(i);
        EXPECT_TRUE(decimal128.LowBits() == value[i * 2]);
        EXPECT_TRUE(static_cast<uint64_t>(decimal128.HighBits()) == value[i * 2 + 1]);
    }

    delete[] value;
    delete vector;
    delete allocator;
}

// Test SetValues/get with offset
TEST(Decimal128Vector, SetValuesWithOffset)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Decimal128Vector_SetValuesWithOffset");
    EXPECT_TRUE(allocator != nullptr);

    Decimal128Vector *vector = new Decimal128Vector(allocator, 256);
    auto *value = new uint64_t[256 * 2];
    for (int i = 0; i < 256; i++) {
        value[i * 2] = i + 1;
        value[i * 2 + 1] = i * 2;
    }
    vector->SetValues(128, &value[128 * 2], 128);
    for (int i = 128; i < 256; i++) {
        Decimal128 decimal128 = vector->GetValue(i);
        EXPECT_TRUE(decimal128.LowBits() == value[i * 2]);
        EXPECT_TRUE(static_cast<uint64_t>(decimal128.HighBits()) == value[i * 2 + 1]);
    }

    delete[] value;
    delete vector;
    delete allocator;
}

// Test is null
TEST(Decimal128Vector, SetValueNull)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Decimal128Vector_SetValueNull");
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
    delete allocator;
}

// Test is copyPosition
TEST(Decimal128Vector, CopyPositions)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Decimal128Vector_CopyPositions");
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

    delete[] possions;
    delete originalVector;
    delete copyPostionVector;
    delete allocator;
}

// Test is copyRegion
TEST(Decimal128Vector, CopyRegion)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Decimal128Vector_CopyRegion");
    EXPECT_TRUE(allocator != nullptr);

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
    delete allocator;
}

class Decimal128VectorTest {
public:
    Decimal128VectorTest() : values(new long[100000000]) {}

    ~Decimal128VectorTest()
    {
        delete[](long *) values;
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
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Decimal128Vector_PerformanceCompare");
    int rowCount = 100000000;

    Timer timer;

    // test long vector set value
    auto *vectorTest2 = new Decimal128VectorTest();
    timer.Start("point test vector set value");
    for (int i = 0; i < rowCount; ++i) {
        vectorTest2->SetValue(i, i);
    }
    timer.End();

    // test long vector set value
    Decimal128VectorTest vectorTest1;
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
    auto *decimal128Vector = new Decimal128Vector(allocator, rowCount);
    timer.Start("vector set value");
    for (int i = 0; i < rowCount; ++i) {
        decimal128Vector->SetValue(i, i);
    }
    timer.End();

    // original set value
    void *decimal128Vector2 = new long[rowCount];
    timer.Start("original set value");
    for (int i = 0; i < rowCount; ++i) {
        ((long *)decimal128Vector2)[i] = i;
    }
    timer.End();

    // vector get value
    timer.Start("vector get value");
    for (int i = 0; i < rowCount; ++i) {
        Decimal128 value = decimal128Vector->GetValue(i);
    }
    timer.End();

    // original get value
    long value = 0;
    timer.Start("original get value");
    for (int i = 0; i < rowCount; ++i) {
        value = *((int64_t *)(decimal128Vector2) + i);
    }
    timer.End();
    value = value + 1;

    delete decimal128Vector;
    delete[](long *) decimal128Vector2;
    delete vectorTest2;
    delete allocator;
}
}
// Test is not writable
