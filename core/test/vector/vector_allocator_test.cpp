/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <thread>
#include "gtest/gtest.h"
#include "long_vector.h"
#include "vector_batch.h"
#include "vector_allocator.h"
#include "vector_allocator_factory.h"

using namespace omniruntime::vec;

#ifdef VECTOR_DEBUG

TEST(VectorAllocatorManager, getOrCreateAllocator)
{
    // create one scope vecAllocator
    VectorAllocator *allocator1 = VectorAllocatorFactory::GetOrCreateAllocator("test1");
    EXPECT_TRUE(allocator1 != nullptr);
    // get the same scope vecAllocator
    VectorAllocator *allocator2 = VectorAllocatorFactory::GetOrCreateAllocator("test1");
    EXPECT_TRUE(allocator2 != nullptr);
    EXPECT_TRUE(allocator2 == allocator1);
    // get different scope vecAllocator
    VectorAllocator *allocator3 = VectorAllocatorFactory::GetOrCreateAllocator("test3");
    EXPECT_TRUE(allocator3 != nullptr);
    EXPECT_TRUE(allocator2 != allocator3);
    // free allocator2
    VectorAllocatorFactory::DeleteAllocator(&allocator1);
    EXPECT_TRUE(allocator1 == nullptr);
    VectorAllocatorFactory::DeleteAllocator(&allocator2);
    EXPECT_TRUE(allocator2 == nullptr);
    VectorAllocatorFactory::DeleteAllocator(&allocator3);
    EXPECT_TRUE(allocator3 == nullptr);
}

TEST(VectorAllocatorManager, getGlobalAllocator)
{
    VectorAllocator *globalAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    VectorAllocator *globalAllocator1 = VectorAllocatorFactory::GetOrCreateAllocator(GLOBAL_SCOPE_NAME);

    EXPECT_TRUE(globalAllocator == globalAllocator1);
}

TEST(VectorAllocator, newVector)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    LongVector *vector = new LongVector(allocator, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetCapacityInBytes(), 2048);
    EXPECT_EQ(vector->GetTypeId(), OMNI_VEC_TYPE_LONG);
    delete vector;

    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

void AddInput(VectorAllocator *allocator)
{
    LongVector *vector1 = new LongVector(allocator, 1024);
    LongVector *vector2 = new LongVector(allocator, 1024);
    VectorBatch vectorBatch(2, 1024);
    vectorBatch.SetVector(0, vector1);
    vectorBatch.SetVector(1, vector2);

    vectorBatch.TraceRecord(allocator->GetLeakDetector(), PROJECT, GET_OUTPUT);
    vectorBatch.TraceRecord(allocator->GetLeakDetector(), HASH_AGG, ADD_INPUT);
    vectorBatch.TraceRecord(allocator->GetLeakDetector(), HASH_AGG, GET_OUTPUT);
    vectorBatch.TraceRecord(allocator->GetLeakDetector(), ORDER_BY, ADD_INPUT);
    vectorBatch.TraceRecord(allocator->GetLeakDetector(), ORDER_BY, GET_OUTPUT);

    delete vector1;
    delete vector2;
}

TEST(VectorAllocator, multiAddInput)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");

    LongVector *vector1 = new LongVector(allocator, 1024);
    LongVector *vector2 = new LongVector(allocator, 1024);
    VectorBatch vectorBatch(2, 1024);
    vectorBatch.SetVector(0, vector1);
    vectorBatch.SetVector(1, vector2);

    for (int i = 0; i < 1000; ++i) {
        vectorBatch.TraceRecord(allocator->GetLeakDetector(), HASH_AGG, ADD_INPUT);
    }

    delete vector1;
    delete vector2;

    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(VectorAllocator, multiThreadAddInput)
{
    const static int32_t THREAD_NUM = 1000;

    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");

    LongVector *vector1 = new LongVector(allocator, 1024);
    LongVector *vector2 = new LongVector(allocator, 1024);
    VectorBatch vectorBatch(2, 1024);
    vectorBatch.SetVector(0, vector1);
    vectorBatch.SetVector(1, vector2);

    std::vector<std::thread> threads;
    for (int i = 0; i < THREAD_NUM; ++i) {
        std::thread th(AddInput, allocator);
        threads.push_back(std::move(th));
    }

    for (int i = 0; i < THREAD_NUM; ++i) {
        if (threads[i].joinable()) {
            threads[i].join();
        }
    }

    delete vector1;
    delete vector2;

    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(VectorAllocator, memoryLeak)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    LongVector *vector = new LongVector(allocator, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetCapacityInBytes(), 2048);
    EXPECT_EQ(vector->GetTypeId(), OMNI_VEC_TYPE_LONG);

    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(VectorAllocator, doubleFree)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    LongVector *vector = new LongVector(allocator, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetCapacityInBytes(), 2048);
    EXPECT_EQ(vector->GetTypeId(), OMNI_VEC_TYPE_LONG);

    delete vector;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(VectorAllocator, usedAfterReleased)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    EXPECT_TRUE(allocator != nullptr);
    LongVector *vector = new LongVector(allocator, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetCapacityInBytes(), 2048);
    EXPECT_EQ(vector->GetTypeId(), OMNI_VEC_TYPE_LONG);
    delete vector;
    allocator->GetLeakDetector().Record(vector, HASH_AGG, ADD_INPUT);
    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}

TEST(VectorAllocator, recycleDeletedTracer)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("test");
    int32_t vectorCount = 1025;
    std::vector<LongVector *> vecs;
    vecs.reserve(vectorCount);
    for (int i = 0; i < vectorCount; i++) {
        vecs.push_back(new LongVector(allocator, 10));
    }
    for (int i = 0; i < vectorCount; i++) {
        delete vecs[i];
    }

    auto *vec = new LongVector(allocator, 10);
    delete vec;
    EXPECT_TRUE(allocator->GetLeakDetector().FindTracer(vec)->Closed());

    for (int i = 0; i < vectorCount; i++) {
        if (vecs[i] == vec) {
            // vector address reused
            EXPECT_TRUE(allocator->GetLeakDetector().FindTracer(vecs[i])->Closed());
        } else {
            EXPECT_TRUE(allocator->GetLeakDetector().FindTracer(vecs[i]) == nullptr);
        }
    }
    vecs.clear();

    // partially deleted
    vecs.reserve(vectorCount * 2);
    for (int i = 0; i < vectorCount * 2; i++) {
        vecs.push_back(new LongVector(allocator, 10));
    }

    for (int i = 0; i < vectorCount * 2; i++) {
        if (i % 2 == 0) {
            delete vecs[i];
        }
    }

    for (int i = 0; i < vectorCount * 2; i++) {
        if (i % 2 == 0 && i != 2048) {
            EXPECT_TRUE(allocator->GetLeakDetector().FindTracer(vecs[i]) == nullptr);
        } else if (i == 2048) {
            EXPECT_TRUE(allocator->GetLeakDetector().FindTracer(vecs[i])->Closed());
        } else {
            EXPECT_TRUE(!allocator->GetLeakDetector().FindTracer(vecs[i])->Closed());
        }
    }

    for (int i = 0; i < vectorCount * 2; i++) {
        if (i % 2 != 0) {
            delete vecs[i];
        }
    }

    vecs.clear();

    VectorAllocatorFactory::DeleteAllocator(&allocator);
    EXPECT_TRUE(allocator == nullptr);
}
#endif
