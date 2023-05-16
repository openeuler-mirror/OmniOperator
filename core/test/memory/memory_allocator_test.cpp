/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "gtest/gtest.h"
#include "vector/vector.h"
#include "allocator.h"
#include "../vector/test.h"

namespace omniruntime::vec {
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

    mem::Allocator *allocator = mem::Allocator::GetAllocator();
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
    const int constexpr size = 0;
    EXPECT_ANY_THROW(allocator->Alloc(static_cast<int64_t>(size)));
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
    int64_t accountedMemory  = threadMemoryManager->GetUntrackedMemory();

    auto sliceVector = vector->Slice(0, vecSize).release();
    int64_t accountedMemory2  = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(accountedMemory2, accountedMemory + 64);

    delete vector;
    int64_t accountedMemory3  = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(accountedMemory3, 64);

    delete sliceVector;
    EXPECT_EQ(threadMemoryManager->GetUntrackedMemory(), 0);
    threadMemoryManager->Clear();
}
}
