/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#include <gtest/gtest.h>
#include "memory/memory_trace.h"
#include "vector/vector.h"
#include "vector/vector_helper.h"
#include "allocator.h"

namespace omniruntime::mem::test {
using namespace omniruntime::vec;

template <typename T> static BaseVector *CreateVector()
{
    int size = 100;
    if constexpr (std::is_same_v<T, std::string_view>) {
        BaseVector *varcharVector = new Vector<LargeStringContainer<std::string_view>>(size);
        return varcharVector;
    } else {
        BaseVector *fixedVector = new Vector<T>(size);
        return fixedVector;
    }
}

template <typename T> static BaseVector *CreateDictionaryVector()
{
    int32_t dicSize = 100;
    int32_t valueSize = 10;
    auto *values = new int32_t[valueSize];
    for (int32_t i = 0; i < valueSize; ++i) {
        values[i] = i % dicSize;
    }

    auto originVec = std::make_unique<Vector<int64_t>>(dicSize);
    for (int32_t i = 0; i < dicSize; ++i) {
        if (i % 2 == 0) {
            originVec->SetNull(i);
            continue;
        }
        originVec->SetValue(i, i * i);
    }

    auto dicVec = VectorHelper::CreateDictionary(values, valueSize, originVec.get());
    return dicVec;
}

#ifdef TRACE
// scenario test
TEST(MemoryTrace, testNoMemoryLeak)
{
    auto trace = MemoryTrace::GetMemoryTrace();
    auto intVector = CreateVector<int32_t>();
    auto varcharVector = CreateVector<std::string_view>();
    auto longDicVector = CreateDictionaryVector<int64_t>();
    delete intVector;
    delete varcharVector;
    delete longDicVector;

    int32_t arenaSize = 100;
    Allocator *allocator = Allocator::GetAllocator();
    void *arena = allocator->Alloc(arenaSize);
    allocator->Free(arena, arenaSize);

    int64_t vectorAllocated = trace->GetVectorAllocated();
    EXPECT_EQ(vectorAllocated, 0);
    int64_t arenaAllocated = trace->GetArenaAllocated();
    EXPECT_EQ(arenaAllocated, 0);
    trace->Clear();
}

TEST(MemoryTrace, testNewVectorLeak)
{
    auto trace = MemoryTrace::GetMemoryTrace();
    // new vector leak
    auto intVector = CreateVector<int32_t>();
    auto varcharVector = CreateVector<std::string_view>();
    auto longDicVector = CreateDictionaryVector<int64_t>();
    EXPECT_TRUE(trace->HasMemoryLeak());
    EXPECT_EQ(trace->GetVectorPtrAllocated().size(), 3);

    delete intVector;
    delete varcharVector;
    delete longDicVector;
    trace->Clear();
}

TEST(MemoryTrace, testArenaMemoryLeak)
{
    auto trace = MemoryTrace::GetMemoryTrace();
    int32_t arenaSize = 100;
    Allocator *allocator = Allocator::GetAllocator();
    void *arena = allocator->Alloc(arenaSize);

    EXPECT_TRUE(trace->HasMemoryLeak());
    EXPECT_EQ(trace->GetArenaPtrAllocated().size(), 1);

    allocator->Free(arena, arenaSize);
    trace->Clear();
}
#endif

// function test
TEST(MemoryTrace, testAddVectorMemory)
{
    int size = 100;
    auto trace = MemoryTrace::GetMemoryTrace();
    auto vector = new BaseVector();
    trace->AddVectorMemory(reinterpret_cast<uintptr_t>(vector), size);
    int64_t allocated = trace->GetVectorAllocated();
    EXPECT_EQ(allocated, size);
    trace->SubVectorMemory(reinterpret_cast<uintptr_t>(vector), size);
    allocated = trace->GetVectorAllocated();
    EXPECT_EQ(allocated, 0);
    delete vector;
    trace->Clear();
}

TEST(MemoryTrace, testTAddVectorMemoryWithException)
{
    int size = 100;
    auto trace = MemoryTrace::GetMemoryTrace();
    auto vector1 = new BaseVector();
    trace->AddVectorMemory(reinterpret_cast<uintptr_t>(vector1), size);
    int64_t allocated = trace->GetVectorAllocated();
    EXPECT_EQ(allocated, size);

    // test exception: vector ptr is wrong
    auto vector2 = new BaseVector();
    // match error info
    EXPECT_ANY_THROW(trace->SubVectorMemory(reinterpret_cast<uintptr_t>(vector2), size));

    // test exception: wrong vector size
    // match error info
    EXPECT_ANY_THROW(trace->SubVectorMemory(reinterpret_cast<uintptr_t>(vector1), 0));

    delete vector1;
    delete vector2;
    trace->Clear();
}

TEST(MemoryTrace, testReplaceVectorPtrAllocated)
{
    int size = 100;
    auto trace = MemoryTrace::GetMemoryTrace();
    auto vector = new BaseVector();
    trace->AddVectorMemory(reinterpret_cast<uintptr_t>(vector), size);

    const std::string &stack = TraceUtil::GetStack();
    trace->ReplaceVectorPtrAllocated(reinterpret_cast<uintptr_t>(vector), stack);
    PtrMap vectorPtrAllocated = trace->GetVectorPtrAllocated();
    PtrMap::iterator iter;
    if ((iter = vectorPtrAllocated.find(reinterpret_cast<uintptr_t>(vector))) != vectorPtrAllocated.end()) {
        EXPECT_EQ(iter->second.first, size);
        EXPECT_EQ(iter->second.second, stack);
    }
    delete vector;
    trace->Clear();
}

TEST(MemoryTrace, testReplaceVectorPtrAllocatedWithException)
{
    int size = 100;
    auto trace = MemoryTrace::GetMemoryTrace();
    auto vector1 = new BaseVector();
    trace->AddVectorMemory(reinterpret_cast<uintptr_t>(vector1), size);

    auto vector2 = new BaseVector();
    const std::string &stack = TraceUtil::GetStack();
    EXPECT_ANY_THROW(trace->ReplaceVectorPtrAllocated(reinterpret_cast<uintptr_t>(vector2), stack));

    delete vector1;
    delete vector2;
    trace->Clear();
}

TEST(MemoryTrace, testAddArenaMemory)
{
    int size = 100;
    auto trace = MemoryTrace::GetMemoryTrace();
    MemoryPool *pool = GetMemoryPool();
    uint8_t *arena = nullptr;
    pool->Allocate(size, &arena);
    trace->AddArenaMemory(reinterpret_cast<uintptr_t>(arena), size);
    int64_t allocated = trace->GetArenaAllocated();
    EXPECT_EQ(allocated, size);
    trace->SubArenaMemory(reinterpret_cast<uintptr_t>(arena), size);
    pool->Release(arena);
    allocated = trace->GetArenaAllocated();
    EXPECT_EQ(allocated, 0);
    trace->Clear();
}

TEST(MemoryTrace, testTAddArenaMemoryWithException)
{
    int size = 100;
    auto trace = MemoryTrace::GetMemoryTrace();
    MemoryPool *pool = GetMemoryPool();
    uint8_t *arena1 = nullptr;
    pool->Allocate(size, &arena1);
    trace->AddArenaMemory(reinterpret_cast<uintptr_t>(arena1), size);
    int64_t allocated = trace->GetArenaAllocated();
    EXPECT_EQ(allocated, size);

    // test exception: arena ptr is wrong
    uint8_t *arena2 = nullptr;
    pool->Allocate(size, &arena2);
    // match error info
    EXPECT_ANY_THROW(trace->SubArenaMemory(reinterpret_cast<uintptr_t>(arena2), size));

    // test exception: wrong arena size
    // match error info
    EXPECT_ANY_THROW(trace->SubArenaMemory(reinterpret_cast<uintptr_t>(arena1), 0));

    pool->Release(arena1);
    pool->Release(arena2);
    trace->Clear();
}

TEST(MemoryTrace, testHasMemoryLeak)
{
    int size = 100;
    auto trace = MemoryTrace::GetMemoryTrace();
    MemoryPool *pool = GetMemoryPool();

    auto vector = new BaseVector();
    trace->AddVectorMemory(reinterpret_cast<uintptr_t>(vector), size);

    uint8_t *arena = nullptr;
    pool->Allocate(size, &arena);
    trace->AddArenaMemory(reinterpret_cast<uintptr_t>(arena), size);

    EXPECT_TRUE(trace->HasMemoryLeak());
    EXPECT_EQ(trace->GetVectorAllocated(), size);
    EXPECT_EQ(trace->GetArenaAllocated(), size);
    EXPECT_EQ(trace->GetVectorPtrAllocated().size(), 1);
    EXPECT_EQ(trace->GetArenaPtrAllocated().size(), 1);

    delete vector;
    pool->Release(arena);
    trace->Clear();
}
}
