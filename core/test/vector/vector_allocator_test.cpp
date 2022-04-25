/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <thread>
#include <gtest/gtest.h>
#include "../../config.h"
#include "vector_common.h"
#include "memory/base_allocator.h"

using namespace omniruntime::vec;
using namespace omniruntime::mem;

namespace VectorAllocatorTest {
#ifdef DEBUG_VECTOR

TEST(VectorAllocator, newVector)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VectorAllocator_newVector");
    EXPECT_TRUE(allocator != nullptr);
    LongVector *vector = new LongVector(allocator, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetCapacityInBytes(), 2048);
    EXPECT_EQ(vector->GetTypeId(), OMNI_LONG);
    delete vector;
    delete allocator;
}

void TraceRecord(VectorBatch &vecBatch, std::string opName, VecOpType opType)
{
    for (int i = 0; i < vecBatch.GetVectorCount(); ++i) {
        Vector *vector = vecBatch.GetVector(i);
        vector->RecordStack(opName, opType);
    }
}

void AddInput(VectorAllocator *allocator)
{
    int vectorCount = 2;
    int size = 1024;
    LongVector *vector1 = new LongVector(allocator, size);
    LongVector *vector2 = new LongVector(allocator, size);
    VectorBatch vectorBatch(vectorCount, size);
    vectorBatch.SetVector(0, vector1);
    vectorBatch.SetVector(1, vector2);

    TraceRecord(vectorBatch, "PROJECT", JNI_GET_OUTPUT);
    TraceRecord(vectorBatch, "HASH_AGG", JNI_ADD_INPUT);
    TraceRecord(vectorBatch, "HASH_AGG", JNI_GET_OUTPUT);
    TraceRecord(vectorBatch, "ORDER_BY", JNI_ADD_INPUT);
    TraceRecord(vectorBatch, "ORDER_BY", JNI_GET_OUTPUT);

    delete vector1;
    delete vector2;
}

TEST(VectorAllocator, multiAddInput)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VectorAllocator_multiAddInput");

    LongVector *vector1 = new LongVector(allocator, 1024);
    LongVector *vector2 = new LongVector(allocator, 1024);
    VectorBatch vectorBatch(2, 1024);
    vectorBatch.SetVector(0, vector1);
    vectorBatch.SetVector(1, vector2);

    for (int i = 0; i < 1000; ++i) {
        TraceRecord(vectorBatch, "HASH_AGG", JNI_ADD_INPUT);
    }

    delete vector1;
    delete vector2;
    VectorHelper::FreeVecBatch(&vectorBatch);

    delete allocator;
}

TEST(VectorAllocator, multiThreadAddInput)
{
    const static int32_t THREAD_NUM = 1000;

    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VectorAllocator_multiThreadAddInput");

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

    delete allocator;
}

TEST(VectorAllocator, memoryLeak)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VectorAllocator_memoryLeak");
    EXPECT_TRUE(allocator != nullptr);
    LongVector *vector = new LongVector(allocator, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetCapacityInBytes(), 2048);
    EXPECT_EQ(vector->GetTypeId(), OMNI_LONG);

    delete vector;
    delete allocator;
}

TEST(VectorAllocator, doubleFree)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VectorAllocator_doubleFree");
    EXPECT_TRUE(allocator != nullptr);
    LongVector *vector = new LongVector(allocator, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetCapacityInBytes(), 2048);
    EXPECT_EQ(vector->GetTypeId(), OMNI_LONG);

    delete vector;
    delete allocator;
}

TEST(VectorAllocator, usedAfterReleased)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VectorAllocator_usedAfterReleased");
    EXPECT_TRUE(allocator != nullptr);
    LongVector *vector = new LongVector(allocator, 256);
    EXPECT_EQ(vector->GetSize(), 256);
    EXPECT_EQ(vector->GetPositionOffset(), 0);
    EXPECT_EQ(vector->GetCapacityInBytes(), 2048);
    EXPECT_EQ(vector->GetTypeId(), OMNI_LONG);
    delete vector;
    std::string stack = "HASH_AGG";
    vector->RecordStack(stack, JNI_ADD_INPUT);

    delete allocator;
}

TEST(VectorAllocator, recycleDeletedTracer)
{
    VectorAllocator *allocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VectorAllocator_recycleDeletedTracer");
    int32_t vectorCount = 1025;
    std::vector<LongVector *> vecs;
    vecs.reserve(vectorCount);
    for (int i = 0; i < vectorCount; i++) {
        vecs.push_back(new LongVector(allocator, 10));
    }
    std::vector<VectorTracer *> tracers;
    for (int i = 0; i < vectorCount; i++) {
        tracers.push_back(vecs[i]->GetVectorTracer());
        delete vecs[i];
    }

    auto *vec = new LongVector(allocator, 10);
    auto *tracer = vec->GetVectorTracer();
    delete vec;
    EXPECT_TRUE(tracer->IsClosed());

    for (int i = 0; i < vectorCount; i++) {
        EXPECT_TRUE(tracers[i]->IsClosed());
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
            EXPECT_TRUE(allocator->GetLeakDetector().FindTracer(vecs[i])->IsClosed());
        } else {
            EXPECT_TRUE(!allocator->GetLeakDetector().FindTracer(vecs[i])->IsClosed());
        }
    }

    for (int i = 0; i < vectorCount * 2; i++) {
        if (i % 2 != 0) {
            delete vecs[i];
        }
    }

    vecs.clear();

    delete allocator;
}
#endif

TEST(VectorAllocator, basic)
{
    VectorAllocator *vectorAllocator = GetProcessGlobalVecAllocator()->NewChildAllocator("VectorAllocator_basic");
    int64_t limit = 4096;
    int64_t subLimit = 2048;
    int size = 8;
    vectorAllocator->SetLimit(limit);
    VectorAllocator *subAllocator1 = vectorAllocator->NewChildAllocator("subVectorAllocator", subLimit, 0);
    VectorAllocator *subAllocator2 = vectorAllocator->NewChildAllocator("subVectorAllocator", subLimit, 0);

    EXPECT_EQ(vectorAllocator->GetScope(), "VectorAllocator_basic");
    EXPECT_EQ(vectorAllocator->GetLimit(), limit);
    std::vector<BaseAllocator *> subAllocators = vectorAllocator->GetChildAllocators();
    for (auto &subAllocator : subAllocators) {
        EXPECT_EQ(subAllocator->GetParentAllocator(), vectorAllocator);
        EXPECT_EQ(subAllocator->GetLimit(), subLimit);
        EXPECT_EQ(subAllocator->GetScope(), "subVectorAllocator");
    }

    auto *longVector = new LongVector(subAllocator1, size);
    EXPECT_EQ(subAllocator1->GetAllocatedMemory(), 72); // size * 8 + size
    auto *intVector = new IntVector(subAllocator2, size);
    EXPECT_EQ(subAllocator2->GetAllocatedMemory(), 40);           // size * 4 + size
    auto *doubleVector = new DoubleVector(vectorAllocator, size); // size * 8 + size
    EXPECT_EQ(vectorAllocator->GetAllocatedMemory(), 184);        // 72 + 40 + 72
    EXPECT_EQ(vectorAllocator->GetPeakAllocated(), vectorAllocator->GetAllocatedMemory());

    delete longVector;
    EXPECT_EQ(subAllocator1->GetAllocatedMemory(), 0);
    delete intVector;
    EXPECT_EQ(subAllocator2->GetAllocatedMemory(), 0);
    delete doubleVector;
    EXPECT_EQ(vectorAllocator->GetAllocatedMemory(), 0);
    delete subAllocator1;
    delete subAllocator2;
    delete vectorAllocator;
}

TEST(VectorAllocator, beyondLimit)
{
    int64_t limit = 2048;
    int64_t subLimit = 1024;
    int32_t size = 300;
    VectorAllocator *vecAllocator = GetProcessGlobalVecAllocator()->NewChildAllocator("parent", limit, 0);
    vecAllocator->SetLimit(limit);
    VectorAllocator *subVecAllocator = vecAllocator->NewChildAllocator("operator", subLimit, 0);

    LongVector *longVector = nullptr;
    LongVector *subLongVector = nullptr;
    ASSERT_THROW(subLongVector = new LongVector(subVecAllocator, size), OmniException);
    ASSERT_THROW(longVector = new LongVector(vecAllocator, size), OmniException);

    if (longVector != nullptr) {
        delete longVector;
    }
    if (subLongVector != nullptr) {
        delete subLongVector;
    }
    delete subVecAllocator;
    delete vecAllocator;
}
}
