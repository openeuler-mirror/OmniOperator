/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "gtest/gtest.h"
#include "vector/vector.h"
#include "allocator.h"
#include "vector/vector_test_util.h"

namespace omniruntime::mem::test {
using namespace vec;
using namespace vec::test;
template <typename T> void createVector(int vec_size)
{
    auto vector = std::make_unique<Vector<T>>(vec_size);
    for (int i = 0; i < vec_size; i++) {
        T value;
        if constexpr (std::is_same_v<std::string, T>) {
            value = "string " + std::to_string(i);
        } else {
            value = static_cast<T>(i) * 2 / 3;
        }
        vector->SetValue(i, value);
    }

    for (int i = 0; i < vec_size; i++) {
        T value;
        if constexpr (std::is_same_v<std::string, T>) {
            value = "string " + std::to_string(i);
        } else {
            value = static_cast<T>(i) * 2 / 3;
        }
        EXPECT_EQ(value, vector->GetValue(i));
    }
}

TEST(Allocator, testLoopAllocZeroSize)
{
    Allocator *allocator = Allocator::GetAllocator();
    for (int i = 0; i < 1000; ++i) {
        void *firstPtr = allocator->Alloc(0);
        void *lastPtr = allocator->Alloc(0);
        EXPECT_TRUE(firstPtr != lastPtr);
        allocator->Free(firstPtr, 0);
        allocator->Free(lastPtr, 0);
    }
}

// test: create and free vector
TEST(Allocator, testCreateAndFreeVector)
{
    int vecSize = 100;
    createVector<int32_t>(vecSize);
    createVector<int64_t>(vecSize);
    createVector<double>(vecSize);
    createVector<test::boost_dec64>(vecSize);
    createVector<test::boost_dec128>(vecSize);
    createVector<std::string>(vecSize);
}

TEST(Allocator, testCreateZeroVector)
{
    int vecSize = 0;
    createVector<int32_t>(vecSize);
    createVector<int64_t>(vecSize);
    createVector<double>(vecSize);
    createVector<test::boost_dec64>(vecSize);
    createVector<test::boost_dec128>(vecSize);
    createVector<std::string>(vecSize);
}

// test: the alloc method works properly.
TEST(Allocator, testAllocateRoundSize)
{
    // Avoid interference between UTs.
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    Allocator *allocator = Allocator::GetAllocator();
    for (int size = 1; size < 100000; ++size) {
        void *p = allocator->Alloc(static_cast<int64_t>(size));
        EXPECT_TRUE(p != nullptr);
        int64_t untrackedMemory = threadMemoryManager->GetUntrackedMemory();
        EXPECT_EQ(untrackedMemory, size);
        allocator->Free(p, size);
        untrackedMemory = threadMemoryManager->GetUntrackedMemory();
        EXPECT_EQ(untrackedMemory, 0);
    }
}

// test: allocator allocate zero size.
TEST(Allocator, testAllocateZeroSize)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    mem::Allocator *allocator = mem::Allocator::GetAllocator();
    const int64_t constexpr size = 0;
    void *p = nullptr;
    ASSERT_NO_THROW(p = allocator->Alloc(size));
    EXPECT_TRUE(p != nullptr);
    int64_t untrackedMemory = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(untrackedMemory, 0);
    allocator->Free(p, size);
}

// test: allocator allocate alignment size.
TEST(Allocator, testAllocateAlignmentSize)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    mem::Allocator *allocator = mem::Allocator::GetAllocator();
    const int constexpr size = sizeof(int64_t) * 8;
    void *p = allocator->Alloc(static_cast<int64_t>(size));
    EXPECT_TRUE(p != nullptr);
    int64_t untrackedMemory = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(untrackedMemory, 64);
    allocator->Free(p, size);
    untrackedMemory = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(untrackedMemory, 0);
}

TEST(Allocator, testSlicedVectorSize)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();
    int32_t vecSize = 100;
    auto vector = std::make_unique<Vector<int32_t>>(vecSize).release();
    for (int i = 0; i < vecSize; i++) {
        int32_t value = static_cast<int32_t>(i);
        vector->SetValue(i, value);
    }
    int64_t accountedMemory = threadMemoryManager->GetUntrackedMemory();
    // 628 = 128(vector, nullsBuffer, valuesBuffer class) + 100(nulls capacity) + 400(values capacity)
    EXPECT_EQ(accountedMemory, 628);

    auto sliceVector = vector->Slice(0, vecSize);
    int64_t accountedMemory2  = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(accountedMemory2, accountedMemory + 80);

    delete vector;
    int64_t accountedMemory3 = threadMemoryManager->GetUntrackedMemory();
    // 580 = 80(vector class) + 100(nulls capacity) + 400(values capacity)
    EXPECT_EQ(accountedMemory3, 580);

    delete sliceVector;
    EXPECT_EQ(threadMemoryManager->GetUntrackedMemory(), 0);
    threadMemoryManager->Clear();
}
}
