/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "gtest/gtest.h"
#include "vector/vector.h"
#include "allocator.h"
#include "../vector/test.h"

namespace omniruntime::vec {
template <typename T> void CreateVector(int vec_size)
{
    auto vector = std::make_unique<Vector<T>>(vec_size);
    for (int i = 0; i < vec_size; i++) {
        T value;
        if constexpr (std::is_same_v<std::string, T>) {
            value = "string " + std::to_string(i);
        } else {
            value = (T)i * 2 / 3;
        }
        vector->SetValue(i, value);
    }

    for (int i = 0; i < vec_size; i++) {
        T value;
        if constexpr (std::is_same_v<std::string, T>) {
            value = "string " + std::to_string(i);
        } else {
            value = (T)i * 2 / 3;
        }
        EXPECT_EQ(value, vector->GetValue(i));
    }
}

// test: create and free vector
TEST(Allocator, testCreateAndFreeVector)
{
    int vecSize = 100;
    CreateVector<int32_t>(vecSize);
    CreateVector<int64_t>(vecSize);
    CreateVector<double>(vecSize);
    CreateVector<test::boost_dec64>(vecSize);
    CreateVector<test::boost_dec128>(vecSize);
    CreateVector<std::string>(vecSize);
}

TEST(Allocator, testCreateZeroVector)
{
    int vecSize = 0;
    CreateVector<int32_t>(vecSize);
    CreateVector<int64_t>(vecSize);
    CreateVector<double>(vecSize);
    CreateVector<test::boost_dec64>(vecSize);
    CreateVector<test::boost_dec128>(vecSize);
    CreateVector<std::string>(vecSize);
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
}
